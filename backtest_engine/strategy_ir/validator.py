"""
Semantic Validator for Strategy IR.

Two-layer validation:
1. Schema validation — handled by Pydantic at model parse time
2. Semantic validation — checks cross-field consistency, registry references,
   date range feasibility, graph cycle detection, etc.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional, Set

from backtest_engine.strategy_ir.models import (
    NodeGraph,
    StrategyIR,
    SleeveConfig,
    SelectionMethod,
    AllocatorType,
    SleeveMixMethod,
    RunMode,
)


# ============================================================
# Validation issue
# ============================================================

@dataclass
class ValidationIssue:
    severity: str   # "error" | "warning" | "info"
    code: str
    message: str
    location: str = ""

    def __str__(self) -> str:
        loc = f" [{self.location}]" if self.location else ""
        return f"[{self.severity.upper()}] {self.code}{loc}: {self.message}"


@dataclass
class ValidationResult:
    issues: List[ValidationIssue] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(i.severity == "error" for i in self.issues)

    @property
    def errors(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    def add_error(self, code: str, message: str, location: str = "") -> None:
        self.issues.append(ValidationIssue("error", code, message, location))

    def add_warning(self, code: str, message: str, location: str = "") -> None:
        self.issues.append(ValidationIssue("warning", code, message, location))

    def add_info(self, code: str, message: str, location: str = "") -> None:
        self.issues.append(ValidationIssue("info", code, message, location))

    def __str__(self) -> str:
        if not self.issues:
            return "Validation passed with no issues."
        lines = [str(i) for i in self.issues]
        summary = f"{len(self.errors)} error(s), {len(self.warnings)} warning(s)"
        return "\n".join([summary] + lines)


# ============================================================
# Semantic Validator
# ============================================================

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class SemanticValidator:
    """
    Validates a StrategyIR for semantic correctness.

    Usage:
        result = SemanticValidator().validate(ir)
        if result.has_errors:
            print(result)

    Optionally inject a field_registry to validate field references.
    """

    def __init__(self, field_registry: Optional[dict] = None):
        self._field_registry = field_registry or {}

    def validate(self, ir: StrategyIR) -> ValidationResult:
        result = ValidationResult()
        self._check_dates(ir, result)
        self._check_sleeves(ir, result)
        self._check_portfolio_aggregation(ir, result)
        self._check_execution(ir, result)
        self._check_contest_mode(ir, result)
        return result

    # ----------------------------------------------------------
    # Date checks
    # ----------------------------------------------------------

    def _check_dates(self, ir: StrategyIR, r: ValidationResult) -> None:
        dr = ir.effective_date_range()
        for name, val in [("start", dr.start), ("end", dr.end)]:
            if not DATE_RE.match(val):
                r.add_error("INVALID_DATE_FORMAT", f"date_range.{name}='{val}' is not YYYY-MM-DD")
        if DATE_RE.match(dr.start) and DATE_RE.match(dr.end):
            if dr.start >= dr.end:
                r.add_error("DATE_RANGE_INVALID", f"start ({dr.start}) must be before end ({dr.end})")
            # Warn about very short periods
            from datetime import date
            try:
                d0 = date.fromisoformat(dr.start)
                d1 = date.fromisoformat(dr.end)
                days = (d1 - d0).days
                if days < 90:
                    r.add_warning("SHORT_DATE_RANGE", f"Date range is only {days} calendar days; metrics may be unreliable")
            except ValueError:
                pass

    # ----------------------------------------------------------
    # Sleeve checks
    # ----------------------------------------------------------

    def _check_sleeves(self, ir: StrategyIR, r: ValidationResult) -> None:
        for sleeve in ir.sleeves:
            loc = f"sleeve:{sleeve.sleeve_id}"
            self._check_node_graph(sleeve.node_graph, r, loc)
            self._check_selection(sleeve, r, loc)
            self._check_allocator(sleeve, r, loc)
            self._check_constraints(sleeve, ir, r, loc)
            self._check_score_ref(sleeve, r, loc)

    def _check_node_graph(self, graph: NodeGraph, r: ValidationResult, loc: str) -> None:
        """Validate node graph: no cycles, all references resolve."""
        if not graph.nodes:
            return
        # Build adjacency for cycle detection
        deps: dict[str, Set[str]] = {}
        for nid, node in graph.nodes.items():
            inputs = set()
            node_type = getattr(node, "type", None)
            if node_type in ("ts_op", "cs_op"):
                inputs.add(node.input)  # type: ignore[attr-defined]
            elif node_type in ("combine", "predicate"):
                inputs.update(node.inputs)  # type: ignore[attr-defined]
            elif node_type == "condition":
                inputs.add(node.condition)  # type: ignore[attr-defined]
                inputs.add(node.true_branch)  # type: ignore[attr-defined]
                inputs.add(node.false_branch)  # type: ignore[attr-defined]
            deps[nid] = inputs

        # Check all referenced node_ids exist
        all_ids = set(graph.nodes.keys())
        for nid, inputs in deps.items():
            for inp in inputs:
                if inp not in all_ids:
                    r.add_error(
                        "GRAPH_MISSING_NODE",
                        f"Node '{nid}' references unknown node '{inp}'",
                        loc
                    )

        # Check for cycles using DFS
        if self._has_cycle(deps):
            r.add_error("GRAPH_CYCLE", "Node graph contains a cycle", loc)

        # Validate field references
        if self._field_registry:
            for nid, node in graph.nodes.items():
                if getattr(node, "type", None) == "field":
                    fid = node.field_id  # type: ignore[attr-defined]
                    if fid not in self._field_registry:
                        r.add_warning(
                            "UNKNOWN_FIELD_ID",
                            f"Field '{fid}' not found in field registry",
                            f"{loc}.node:{nid}"
                        )

    def _has_cycle(self, deps: dict[str, Set[str]]) -> bool:
        visited: Set[str] = set()
        in_stack: Set[str] = set()

        def dfs(node: str) -> bool:
            visited.add(node)
            in_stack.add(node)
            for neighbor in deps.get(node, set()):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in in_stack:
                    return True
            in_stack.discard(node)
            return False

        for n in deps:
            if n not in visited:
                if dfs(n):
                    return True
        return False

    def _check_selection(self, sleeve: SleeveConfig, r: ValidationResult, loc: str) -> None:
        sel = sleeve.selection
        if sel.method == SelectionMethod.TOP_N and (sel.n is None or sel.n < 1):
            r.add_error("SELECTION_MISSING_N", "top_n requires n >= 1", loc)
        if sel.method == SelectionMethod.TOP_PCT and (sel.pct is None or not (0 < sel.pct <= 1)):
            r.add_error("SELECTION_MISSING_PCT", "top_pct requires 0 < pct <= 1", loc)
        if sel.method == SelectionMethod.THRESHOLD and sel.threshold is None:
            r.add_error("SELECTION_MISSING_THRESHOLD", "threshold selection requires threshold value", loc)

        # Cross-check: min_names in constraints cannot exceed the number of stocks selection will produce
        c = sleeve.constraints
        if sel.method == SelectionMethod.TOP_N and sel.n is not None and c.min_names > sel.n:
            r.add_error(
                "CONSTRAINT_NAMES_VS_SELECTION",
                f"constraints.min_names ({c.min_names}) > selection.n ({sel.n}): "
                "can never satisfy — reduce min_names or increase selection.n",
                loc,
            )

        # Warn if no score for ranked selections
        ranked = {SelectionMethod.TOP_N, SelectionMethod.TOP_PCT, SelectionMethod.THRESHOLD, SelectionMethod.ALL_POSITIVE}
        if sel.method in ranked:
            score_source = sleeve.score_ref or (sleeve.selection.score_ref)
            if not score_source and not sleeve.node_graph.output:
                r.add_warning(
                    "NO_SCORE_REF",
                    "Selection method requires a score but no score_ref or node_graph.output is set. "
                    "Will fall back to equal-weight arbitrary selection.",
                    loc
                )

    def _check_allocator(self, sleeve: SleeveConfig, r: ValidationResult, loc: str) -> None:
        alloc = sleeve.allocator
        alloc_type = alloc.type

        if alloc_type == "enhanced_index":
            if not alloc.alpha_ref and not sleeve.node_graph.output:  # type: ignore[attr-defined]
                r.add_warning(
                    "ENHANCED_INDEX_NO_ALPHA",
                    "enhanced_index allocator has no alpha signal; will optimize to min-TE only",
                    loc
                )
        if alloc_type == "benchmark_tracking":
            idx = alloc.benchmark_index  # type: ignore[attr-defined]
            valid_indices = {"KOSPI", "KOSPI200", "KOSDAQ", "KRX300"}
            if idx not in valid_indices:
                r.add_warning(
                    "UNKNOWN_BENCHMARK_INDEX",
                    f"Benchmark index '{idx}' not in known set {valid_indices}",
                    loc
                )

    def _check_constraints(self, sleeve: SleeveConfig, ir: StrategyIR, r: ValidationResult, loc: str) -> None:
        c = sleeve.constraints
        if c.max_weight < c.min_weight:
            r.add_error("CONSTRAINT_WEIGHT_INVALID", f"max_weight ({c.max_weight}) < min_weight ({c.min_weight})", loc)
        if c.min_names > (c.max_names or 99999):
            r.add_error("CONSTRAINT_NAMES_INVALID", f"min_names ({c.min_names}) > max_names ({c.max_names})", loc)
        # Contest-specific
        if ir.mode == RunMode.CONTEST:
            if c.max_weight > 0.15:
                r.add_warning(
                    "CONTEST_WEIGHT_CAP",
                    f"Contest mode: max_weight={c.max_weight} exceeds 15% per-stock rule",
                    loc
                )

    def _check_score_ref(self, sleeve: SleeveConfig, r: ValidationResult, loc: str) -> None:
        score_ref = sleeve.score_ref or (sleeve.node_graph.output if sleeve.node_graph else None)
        if score_ref and sleeve.node_graph.nodes:
            if score_ref not in sleeve.node_graph.nodes:
                r.add_error(
                    "SCORE_REF_MISSING",
                    f"score_ref '{score_ref}' not found in node_graph.nodes",
                    loc
                )

    # ----------------------------------------------------------
    # Portfolio aggregation
    # ----------------------------------------------------------

    def _check_portfolio_aggregation(self, ir: StrategyIR, r: ValidationResult) -> None:
        pa = ir.portfolio_aggregation
        sleeve_ids = {s.sleeve_id for s in ir.sleeves}

        if pa.method == SleeveMixMethod.FIXED_MIX:
            if not pa.sleeve_weights and len(ir.sleeves) > 1:
                r.add_warning(
                    "FIXED_MIX_NO_WEIGHTS",
                    "fixed_mix with multiple sleeves but no sleeve_weights — will use equal weights",
                    "portfolio_aggregation"
                )
            if pa.sleeve_weights:
                total = sum(pa.sleeve_weights.values())
                if abs(total - 1.0) > 0.01 and pa.normalize is False:
                    r.add_warning(
                        "WEIGHTS_DONT_SUM",
                        f"sleeve_weights sum to {total:.3f} (not 1.0) and normalize=False",
                        "portfolio_aggregation"
                    )

        if pa.method == SleeveMixMethod.REGIME_SWITCH:
            if not pa.regime_branches:
                r.add_error(
                    "REGIME_SWITCH_NO_BRANCHES",
                    "regime_switch requires at least one regime_branch",
                    "portfolio_aggregation"
                )
            for i, branch in enumerate(pa.regime_branches):
                unknown = set(branch.weights.keys()) - sleeve_ids
                if unknown:
                    r.add_error(
                        "REGIME_BRANCH_UNKNOWN_SLEEVE",
                        f"regime_branch[{i}] references unknown sleeve_ids: {unknown}",
                        "portfolio_aggregation"
                    )
            # Validate regime condition nodes in global_node_graph
            global_ids = set(pa.global_node_graph.nodes.keys()) if pa.global_node_graph else set()
            for i, branch in enumerate(pa.regime_branches):
                if branch.condition_node not in global_ids:
                    r.add_error(
                        "REGIME_CONDITION_MISSING",
                        f"regime_branch[{i}].condition_node '{branch.condition_node}' "
                        f"not in portfolio_aggregation.global_node_graph",
                        "portfolio_aggregation"
                    )

    # ----------------------------------------------------------
    # Execution checks
    # ----------------------------------------------------------

    def _check_execution(self, ir: StrategyIR, r: ValidationResult) -> None:
        ex = ir.effective_execution()
        total_cost = ex.commission_bps + ex.sell_tax_bps + ex.slippage_bps
        if total_cost > 100:
            r.add_warning(
                "HIGH_TRANSACTION_COST",
                f"Total transaction cost per trade is {total_cost} bps — unusually high",
                "execution"
            )
        if ex.initial_capital < 1_000_000:
            r.add_warning(
                "LOW_CAPITAL",
                f"initial_capital={ex.initial_capital:,.0f} KRW is very low for realistic simulation",
                "execution"
            )

    # ----------------------------------------------------------
    # Contest mode checks
    # ----------------------------------------------------------

    def _check_contest_mode(self, ir: StrategyIR, r: ValidationResult) -> None:
        if ir.mode != RunMode.CONTEST:
            return
        # Check weekly turnover requirement
        ex = ir.effective_execution()
        fa = ir.portfolio_aggregation.final_constraints
        if fa.max_turnover_weekly is not None and fa.max_turnover_weekly < 0.05:
            r.add_warning(
                "CONTEST_TURNOVER_LOW",
                "Contest requires >= 5% weekly turnover; constraint may be too restrictive",
                "contest"
            )
        # Warn if Samsung cap not set to contest level
        any_sleeve_high_cap = any(
            s.constraints.contest_samsung_cap < 0.40 for s in ir.sleeves
        )
        if any_sleeve_high_cap:
            r.add_info(
                "CONTEST_SAMSUNG_CAP",
                "One or more sleeves have contest_samsung_cap < 40%; adjust if Samsung exceeds 15%",
                "contest"
            )
