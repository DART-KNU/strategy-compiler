"""
Normalizer — converts a draft strategy dict into a valid StrategyIR dict.

Handles:
- Default value injection
- Field alias resolution (synonym -> field_id)
- Benchmark alias resolution
- Single-sleeve shorthand expansion
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

from backtest_engine.registry.field_registry import SYNONYM_MAP
from backtest_engine.registry.benchmark_registry import BENCHMARK_ALIASES


class Normalizer:
    """
    Takes a raw dict (possibly from LLM or manual input) and normalizes it
    into a format that Pydantic can parse as StrategyIR.
    """

    # Default values (see spec §17)
    DEFAULTS = {
        "version": "1.0",
        "mode": "research",
        "objective": "maximize_return",
        "benchmark": {"index_code": "KOSPI200", "proxy_type": "return_proxy"},
        "rebalancing": {
            "frequency": "monthly",
            "day_of_month": 1,
            "look_ahead_buffer": 1,
        },
        "execution": {
            "fill_rule": "next_open",
            "commission_bps": 10.0,
            "sell_tax_bps": 20.0,
            "slippage_bps": 10.0,
            "initial_capital": 1_000_000_000,
            "round_lot": 1,
        },
        "base_universe": {
            "base": "is_eligible",
            "markets": ["코스피", "코스닥"],
            "include_blocked": False,
        },
        "portfolio_aggregation": {
            "method": "fixed_mix",
            "normalize": True,
        },
        "reporting": {
            "charts": ["nav", "drawdown", "turnover", "sector_exposure", "sleeve_attribution"],
            "tables": ["summary_metrics", "monthly_returns", "top_holdings", "top_trades", "constraint_violations"],
        },
    }

    def normalize(self, draft: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a draft strategy dict.

        Returns a dict suitable for StrategyIR.model_validate().
        """
        d = deepcopy(draft)

        # Normalize rebalance_frequency shorthand → rebalancing.frequency
        # Must run BEFORE defaults injection so frequency isn't overwritten
        if "rebalance_frequency" in d:
            freq_val = d.pop("rebalance_frequency")
            if "rebalancing" not in d:
                d["rebalancing"] = {"frequency": freq_val}
            # else: rebalancing already present, ignore shorthand

        # Inject top-level defaults
        for k, v in self.DEFAULTS.items():
            if k not in d:
                if isinstance(v, dict):
                    d[k] = dict(v)
                else:
                    d[k] = v
            elif isinstance(v, dict) and isinstance(d[k], dict):
                for subk, subv in v.items():
                    if subk not in d[k]:
                        d[k][subk] = subv

        # Resolve benchmark alias
        bm = d.get("benchmark", {})
        idx = bm.get("index_code", "KOSPI200")
        resolved_idx = BENCHMARK_ALIASES.get(idx.lower(), idx)
        d["benchmark"]["index_code"] = resolved_idx

        # Normalize sleeves
        if "sleeves" not in d and "sleeve" in d:
            d["sleeves"] = [d.pop("sleeve")]

        if "sleeves" not in d:
            # Create a default single sleeve
            d["sleeves"] = [self._build_default_sleeve(d)]

        sleeves = d["sleeves"]
        for i, sleeve in enumerate(sleeves):
            sleeves[i] = self._normalize_sleeve(sleeve, i)

        # Normalize portfolio_aggregation
        pa = d.get("portfolio_aggregation", {})
        if "sleeve_weights" not in pa and len(sleeves) == 1:
            pa["sleeve_weights"] = {sleeves[0].get("sleeve_id", "main"): 1.0}
        d["portfolio_aggregation"] = pa

        # Resolve field synonyms in node graphs
        for sleeve in d["sleeves"]:
            if "node_graph" in sleeve and sleeve["node_graph"].get("nodes"):
                self._resolve_field_synonyms(sleeve["node_graph"]["nodes"])

        return d

    def _normalize_sleeve(self, sleeve: Dict, idx: int) -> Dict:
        """Normalize a single sleeve definition."""
        if "sleeve_id" not in sleeve:
            sleeve["sleeve_id"] = f"sleeve_{idx}"

        # Default selection
        if "selection" not in sleeve:
            sleeve["selection"] = {
                "method": "top_n",
                "n": 20,
                "min_names": 5,
            }

        # Default allocator
        if "allocator" not in sleeve:
            # If benchmark-objective, use enhanced_index; otherwise equal_weight
            alloc_hint = sleeve.get("_allocator_hint", "")
            if "benchmark" in alloc_hint or "tracking" in alloc_hint:
                sleeve["allocator"] = {"type": "enhanced_index", "benchmark_index": "KOSPI200"}
            else:
                sleeve["allocator"] = {"type": "equal_weight"}

        # Default constraints
        if "constraints" not in sleeve:
            sleeve["constraints"] = {
                "long_only": True,
                "fully_invested": True,
                "max_weight": 0.15,
                "min_weight": 0.0,
                "min_names": 5,
                "target_cash_weight": 0.005,
            }

        # Default node_graph
        if "node_graph" not in sleeve:
            sleeve["node_graph"] = {"nodes": {}, "output": None}

        return sleeve

    def _build_default_sleeve(self, draft: Dict) -> Dict:
        """Build a default single-sleeve when no sleeve is specified."""
        return {
            "sleeve_id": "main",
            "description": "Default single sleeve",
            "selection": {"method": "top_n", "n": 20, "min_names": 5},
            "allocator": {"type": "equal_weight"},
            "constraints": {
                "long_only": True,
                "max_weight": 0.15,
                "min_names": 5,
            },
            "node_graph": {"nodes": {}, "output": None},
        }

    def _resolve_field_synonyms(self, nodes: Dict[str, Any]) -> None:
        """Resolve field synonyms in FieldNode definitions."""
        for node_id, node in nodes.items():
            if node.get("type") == "field":
                fid = node.get("field_id", "")
                resolved = SYNONYM_MAP.get(fid.lower())
                if resolved and resolved != fid:
                    node["field_id"] = resolved
