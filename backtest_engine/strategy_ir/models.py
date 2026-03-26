"""
Strategy IR — typed Pydantic models for a single unified strategy representation.

Design principles:
- One IR for all strategy types (momentum, value, enhanced index, etc.)
- Sleeve-based architecture: portfolio = mix of sleeves
- Node graph per sleeve: DAG of feature/signal computations
- Allocators are separate from signals
- Fully serializable to/from JSON
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field, model_validator


# ============================================================
# Enums
# ============================================================

class RunMode(str, Enum):
    RESEARCH = "research"
    CONTEST = "contest"


class FillRule(str, Enum):
    NEXT_OPEN = "next_open"
    NEXT_CLOSE = "next_close"
    SAME_CLOSE = "same_close"


class RebalanceFrequency(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    CUSTOM = "custom"


class SelectionMethod(str, Enum):
    TOP_N = "top_n"
    TOP_PCT = "top_pct"
    THRESHOLD = "threshold"
    ALL_POSITIVE = "all_positive"
    OPTIMIZER_ONLY = "optimizer_only"


class AllocatorType(str, Enum):
    EQUAL_WEIGHT = "equal_weight"
    SCORE_WEIGHTED = "score_weighted"
    INVERSE_VOL = "inverse_vol"
    MEAN_VARIANCE = "mean_variance"
    BENCHMARK_TRACKING = "benchmark_tracking"
    ENHANCED_INDEX = "enhanced_index"
    RISK_BUDGET = "risk_budget"


class CovarianceModel(str, Enum):
    DIAGONAL_VOL = "diagonal_vol"
    SAMPLE_COV = "sample_cov"
    SHRINKAGE_COV = "shrinkage_cov"


class SleeveMixMethod(str, Enum):
    FIXED_MIX = "fixed_mix"
    REGIME_SWITCH = "regime_switch"
    SCORE_BASED_MIX = "score_based_mix"


class NullPolicy(str, Enum):
    DROP = "drop"
    ZERO = "zero"
    FFILL = "ffill"
    BFILL = "bfill"
    KEEP_NULL = "keep_null"


class BenchmarkProxyType(str, Enum):
    BENCHMARK_TRACKING = "benchmark_tracking"
    ENHANCED_INDEX = "enhanced_index"
    SECTOR_PROXY = "sector_proxy"
    RETURN_PROXY = "return_proxy"


# ============================================================
# Node Graph — DAG of computations
# ============================================================

class NodeBase(BaseModel):
    """Base class for all node graph nodes."""
    node_id: str = Field(..., description="Unique node identifier within this graph")
    null_policy: NullPolicy = NullPolicy.DROP
    description: Optional[str] = None


class FieldNode(NodeBase):
    """Load a raw field from the DB registry."""
    type: Literal["field"] = "field"
    field_id: str = Field(..., description="Field ID from the field registry")
    lag: int = Field(0, ge=0, description="Additional lag in business days")


class ConstantNode(NodeBase):
    """A scalar constant."""
    type: Literal["constant"] = "constant"
    value: float


class BenchmarkRefNode(NodeBase):
    """Load benchmark index level or return."""
    type: Literal["benchmark_ref"] = "benchmark_ref"
    index_code: str = Field(..., description="e.g. KOSPI200")
    field: str = Field("close", description="open/high/low/close")
    lag: int = Field(0, ge=0)


class TsOpNode(NodeBase):
    """Time-series operation on a single input."""
    type: Literal["ts_op"] = "ts_op"
    op: str = Field(..., description="lag/sma/ema/std/mean/zscore/rank/percentile")
    input: str = Field(..., description="node_id of input")
    window: Optional[int] = Field(None, ge=1)
    params: Dict[str, Any] = Field(default_factory=dict)


class CsOpNode(NodeBase):
    """Cross-sectional operation (rank/zscore/percentile/winsorize/sector_neutralize)."""
    type: Literal["cs_op"] = "cs_op"
    op: str = Field(...)
    input: str = Field(...)
    params: Dict[str, Any] = Field(default_factory=dict)


class CombineNode(NodeBase):
    """Arithmetic or weighted combination of inputs."""
    type: Literal["combine"] = "combine"
    op: str = Field(..., description="add/sub/mul/div/weighted_sum/negate/abs/clip/winsorize/if_else/vol_scale")
    inputs: List[str] = Field(..., description="List of input node_ids")
    params: Dict[str, Any] = Field(default_factory=dict)


class PredicateNode(NodeBase):
    """Boolean predicate: gt/gte/lt/lte/eq/ne/logical_and/logical_or/logical_not."""
    type: Literal["predicate"] = "predicate"
    op: str
    inputs: List[str]
    params: Dict[str, Any] = Field(default_factory=dict)


class ConditionNode(NodeBase):
    """if_else conditional selection based on a boolean mask."""
    type: Literal["condition"] = "condition"
    condition: str = Field(..., description="node_id of boolean mask")
    true_branch: str = Field(..., description="node_id used when condition is True")
    false_branch: str = Field(..., description="node_id used when condition is False")


# Union of all node types
NodeDef = Union[
    FieldNode,
    ConstantNode,
    BenchmarkRefNode,
    TsOpNode,
    CsOpNode,
    CombineNode,
    PredicateNode,
    ConditionNode,
]


class NodeGraph(BaseModel):
    """
    Directed acyclic graph of computations.
    Keys in 'nodes' dict are node_ids. 'output' is the final score node_id.
    """
    nodes: Dict[str, NodeDef] = Field(default_factory=dict)
    output: Optional[str] = Field(None, description="node_id of the final score")

    @model_validator(mode="after")
    def _validate_output_exists(self) -> "NodeGraph":
        if self.output and self.output not in self.nodes:
            raise ValueError(f"NodeGraph output '{self.output}' not found in nodes")
        return self


# ============================================================
# Universe
# ============================================================

class UniverseConfig(BaseModel):
    """Universe selection configuration."""
    base: str = Field("is_eligible", description="Base universe filter")
    markets: List[str] = Field(
        default_factory=lambda: ["코스피", "코스닥"],
        description="Allowed market types"
    )
    min_mcap_bn: Optional[float] = Field(None, description="Min market cap in billions KRW (overrides base)")
    min_adv5_bn: Optional[float] = Field(None, description="Min 5-day ADV in billions KRW (overrides base)")
    include_blocked: bool = Field(False, description="Include non-eligible stocks (research only)")
    custom_filter_node: Optional[str] = Field(None, description="node_id of boolean filter in sleeve graph")
    extra_fields: List[str] = Field(default_factory=list, description="Additional DB fields to load")


# ============================================================
# Selection
# ============================================================

class SelectionConfig(BaseModel):
    """How stocks are selected from scored universe."""
    method: SelectionMethod = SelectionMethod.TOP_N
    n: Optional[int] = Field(None, ge=1, description="Number of stocks for top_n")
    pct: Optional[float] = Field(None, gt=0, le=1, description="Fraction for top_pct")
    threshold: Optional[float] = Field(None, description="Score threshold")
    min_names: int = Field(5, ge=1)
    max_names: Optional[int] = None
    score_ref: Optional[str] = Field(None, description="node_id of score in node_graph")

    @model_validator(mode="after")
    def _validate_params(self) -> "SelectionConfig":
        if self.method == SelectionMethod.TOP_N and self.n is None:
            raise ValueError("SelectionConfig: top_n requires 'n'")
        if self.method == SelectionMethod.TOP_PCT and self.pct is None:
            raise ValueError("SelectionConfig: top_pct requires 'pct'")
        if self.method == SelectionMethod.THRESHOLD and self.threshold is None:
            raise ValueError("SelectionConfig: threshold requires 'threshold'")
        return self


# ============================================================
# Allocators
# ============================================================

class EqualWeightConfig(BaseModel):
    type: Literal["equal_weight"] = "equal_weight"


class ScoreWeightedConfig(BaseModel):
    type: Literal["score_weighted"] = "score_weighted"
    power: float = Field(1.0, gt=0, description="Score raised to this power before normalizing")
    clip_negative: bool = True


class InverseVolConfig(BaseModel):
    type: Literal["inverse_vol"] = "inverse_vol"
    vol_field: str = Field("vol_20d", description="Field ID for volatility")
    floor_vol: float = Field(0.005, gt=0, description="Floor to avoid zero division")


class MeanVarianceConfig(BaseModel):
    type: Literal["mean_variance"] = "mean_variance"
    cov_model: CovarianceModel = CovarianceModel.SHRINKAGE_COV
    cov_lookback: int = Field(60, ge=20)
    alpha_ref: Optional[str] = Field(None, description="node_id of expected return signal")
    risk_aversion: float = Field(1.0, gt=0)
    long_only: bool = True
    fully_invested: bool = True


class BenchmarkTrackingConfig(BaseModel):
    type: Literal["benchmark_tracking"] = "benchmark_tracking"
    benchmark_index: str = Field("KOSPI200")
    proxy_type: BenchmarkProxyType = BenchmarkProxyType.BENCHMARK_TRACKING
    te_target: Optional[float] = Field(None, description="Max tracking error (annualized)")
    turnover_penalty: float = Field(0.001)
    sector_match: bool = True
    top_k: Optional[int] = Field(None, description="Max number of names in proxy")
    cov_model: CovarianceModel = CovarianceModel.SHRINKAGE_COV


class EnhancedIndexConfig(BaseModel):
    type: Literal["enhanced_index"] = "enhanced_index"
    benchmark_index: str = Field("KOSPI200")
    alpha_ref: Optional[str] = Field(None, description="node_id of alpha signal")
    alpha_weight: float = Field(1.0, gt=0, description="gamma in maximize(alpha - gamma*TE - eta*turnover)")
    te_penalty: float = Field(1.0, gt=0, description="TE penalty")
    turnover_penalty: float = Field(0.001, ge=0)
    te_target: Optional[float] = None
    cov_model: CovarianceModel = CovarianceModel.SHRINKAGE_COV


class RiskBudgetConfig(BaseModel):
    type: Literal["risk_budget"] = "risk_budget"
    budgets: Dict[str, float] = Field(..., description="ticker or group -> risk budget fraction")
    cov_model: CovarianceModel = CovarianceModel.SHRINKAGE_COV


AllocatorConfig = Union[
    EqualWeightConfig,
    ScoreWeightedConfig,
    InverseVolConfig,
    MeanVarianceConfig,
    BenchmarkTrackingConfig,
    EnhancedIndexConfig,
    RiskBudgetConfig,
]


# ============================================================
# Constraints
# ============================================================

class ConstraintSet(BaseModel):
    """Portfolio-level constraints applied after allocation."""
    long_only: bool = True
    fully_invested: bool = True
    max_weight: float = Field(0.15, gt=0, le=1.0)
    min_weight: float = Field(0.0, ge=0)
    max_names: Optional[int] = None
    min_names: int = Field(5, ge=1)
    max_sector_weight: Optional[float] = Field(None, description="Absolute sector cap")
    max_sector_multiplier: Optional[float] = Field(
        None, description="Sector weight <= multiplier * benchmark_weight"
    )
    small_mcap_threshold_bn: float = Field(1000.0, description="Mcap threshold for 'small' in billions KRW")
    max_small_mcap_weight: Optional[float] = Field(None, description="Max aggregate weight in small-cap names")
    max_adv_participation: float = Field(0.10, description="Max fraction of ADV5 in any single trade")
    max_turnover_weekly: Optional[float] = None
    target_cash_weight: float = Field(0.005, description="Cash buffer fraction")
    # Contest special overrides
    contest_samsung_cap: float = Field(0.40, description="Cap for 005930 in contest mode")
    contest_sector_small_cap: float = Field(0.10, description="Sector cap when benchmark weight <=5% in contest mode")
    contest_sector_large_multiplier: float = Field(2.0, description="Sector cap multiplier in contest mode")


# ============================================================
# Sleeve
# ============================================================

class SleeveConfig(BaseModel):
    """
    A sleeve is a self-contained sub-strategy.
    Final portfolio = weighted sum of sleeves.
    """
    sleeve_id: str = Field(..., description="Unique sleeve ID within the strategy")
    description: Optional[str] = None
    # Optional universe override (inherits strategy universe if not set)
    universe_override: Optional[UniverseConfig] = None
    # Node graph defines all features, signals, and filters for this sleeve
    node_graph: NodeGraph = Field(default_factory=NodeGraph)
    # Reference to the score node in the node_graph
    score_ref: Optional[str] = Field(None, description="node_id of the final score in node_graph")
    # Selection step
    selection: SelectionConfig = Field(default_factory=lambda: SelectionConfig(method=SelectionMethod.TOP_N, n=20))
    # Allocator
    allocator: AllocatorConfig = Field(default_factory=EqualWeightConfig)
    # Constraints applied to this sleeve's output
    constraints: ConstraintSet = Field(default_factory=ConstraintSet)
    # For regime-driven sleeve mixing
    active_condition: Optional[str] = Field(
        None, description="node_id of boolean node; sleeve active only when True"
    )


# ============================================================
# Portfolio Aggregation
# ============================================================

class RegimeBranch(BaseModel):
    """A condition-weight pair for regime switching."""
    condition_node: str = Field(..., description="node_id of boolean regime predicate in global graph")
    weights: Dict[str, float] = Field(..., description="sleeve_id -> weight")


class PortfolioAggregation(BaseModel):
    """How sleeves are combined into a final portfolio."""
    method: SleeveMixMethod = SleeveMixMethod.FIXED_MIX
    sleeve_weights: Dict[str, float] = Field(
        default_factory=dict,
        description="sleeve_id -> weight for fixed_mix (must sum ~1.0)"
    )
    regime_branches: List[RegimeBranch] = Field(
        default_factory=list,
        description="For regime_switch: ordered branches, first True wins"
    )
    default_weights: Optional[Dict[str, float]] = Field(
        None,
        description="Default weights when no regime branch is True"
    )
    normalize: bool = True
    # Global constraints applied to the final merged portfolio
    final_constraints: ConstraintSet = Field(default_factory=ConstraintSet)
    # Global node graph for regime predicates (benchmark MA, etc.)
    global_node_graph: NodeGraph = Field(default_factory=NodeGraph)


# ============================================================
# Date range, Rebalancing, Execution, Benchmark, Reporting
# ============================================================

class DateRange(BaseModel):
    start: str = Field(..., description="YYYY-MM-DD")
    end: str = Field(..., description="YYYY-MM-DD")


class RebalancingConfig(BaseModel):
    frequency: RebalanceFrequency = RebalanceFrequency.MONTHLY
    day_of_week: int = Field(0, ge=0, le=4, description="0=Mon (for weekly)")
    day_of_month: int = Field(1, ge=1, le=28, description="For monthly: rebalance on N-th trading day")
    custom_dates: List[str] = Field(default_factory=list, description="For custom frequency")
    look_ahead_buffer: int = Field(1, ge=0, description="Extra buffer days after rebalance signal")
    # Dual-cadence: compute signals at 'frequency', execute trades at 'execution_cadence'
    execution_cadence: Optional[RebalanceFrequency] = Field(
        None,
        description="If set, trade at this frequency while signals fire at 'frequency'. "
                    "E.g. monthly signal + weekly execution for contest turnover rules.",
    )
    min_turnover_per_rebalance: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Floor on one-way turnover per execution step (0.05 = 5%). "
                    "Ensures at least this much is traded each step when gap allows. "
                    "Used with execution_cadence to meet contest minimum turnover rules.",
    )
    max_turnover_per_rebalance: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Cap on one-way turnover per execution step (0.05 = 5%). "
                    "Used with execution_cadence to pace trades gradually.",
    )


class ExecutionConfig(BaseModel):
    fill_rule: FillRule = FillRule.NEXT_OPEN
    commission_bps: float = Field(10.0, ge=0)
    sell_tax_bps: float = Field(20.0, ge=0)
    slippage_bps: float = Field(10.0, ge=0)
    round_lot: int = Field(1, ge=1, description="Minimum share lot size")
    partial_fill: bool = False
    initial_capital: float = Field(1_000_000_000.0, gt=0, description="Initial capital in KRW")


class BenchmarkConfig(BaseModel):
    index_code: str = Field("KOSPI200", description="Benchmark index code")
    proxy_type: BenchmarkProxyType = BenchmarkProxyType.RETURN_PROXY


class ReportingConfig(BaseModel):
    charts: List[str] = Field(
        default_factory=lambda: ["nav", "drawdown", "turnover", "sector_exposure", "sleeve_attribution"]
    )
    tables: List[str] = Field(
        default_factory=lambda: ["summary_metrics", "monthly_returns", "top_holdings", "top_trades", "constraint_violations"]
    )
    top_n_holdings: int = Field(20, ge=1)
    top_n_trades: int = Field(20, ge=1)
    annualization_factor: int = Field(252)


class RunOverrides(BaseModel):
    """Ad-hoc overrides for a specific backtest run (not saved to strategy definition)."""
    initial_capital: Optional[float] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    fill_rule: Optional[FillRule] = None
    commission_bps: Optional[float] = None
    run_label: Optional[str] = None


# ============================================================
# Top-level Strategy IR
# ============================================================

class StrategyIR(BaseModel):
    """
    Unified Strategy IR — the single representation of any quant strategy.

    This IR supports:
    - cross-sectional ranking / momentum / value / multi-factor strategies
    - benchmark tracking and enhanced indexing
    - regime switching
    - multi-sleeve portfolio construction
    - contest-mode constraint enforcement

    The IR is intentionally strategy-type-agnostic.
    Strategy 'type' is determined by the combination of allocator and node graph,
    not by a type field.
    """

    version: str = Field("1.0", description="IR schema version")
    strategy_id: str = Field(..., description="Unique strategy identifier")
    strategy_name: Optional[str] = None
    mode: RunMode = RunMode.RESEARCH
    description: Optional[str] = None

    objective: str = Field(
        "maximize_return",
        description="Human-readable objective: maximize_return / minimize_te / enhanced_index / etc."
    )

    date_range: DateRange

    base_universe: UniverseConfig = Field(default_factory=UniverseConfig)

    benchmark: BenchmarkConfig = Field(default_factory=BenchmarkConfig)

    sleeves: List[SleeveConfig] = Field(
        ..., min_length=1, description="At least one sleeve required"
    )

    portfolio_aggregation: PortfolioAggregation = Field(
        default_factory=PortfolioAggregation
    )

    rebalancing: RebalancingConfig = Field(default_factory=RebalancingConfig)

    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)

    reporting: ReportingConfig = Field(default_factory=ReportingConfig)

    run_overrides: Optional[RunOverrides] = None

    tags: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_sleeve_ids_unique(self) -> "StrategyIR":
        ids = [s.sleeve_id for s in self.sleeves]
        if len(ids) != len(set(ids)):
            raise ValueError("Sleeve IDs must be unique")
        return self

    @model_validator(mode="after")
    def _validate_portfolio_aggregation(self) -> "StrategyIR":
        pa = self.portfolio_aggregation
        sleeve_ids = {s.sleeve_id for s in self.sleeves}
        if pa.method == SleeveMixMethod.FIXED_MIX and pa.sleeve_weights:
            unknown = set(pa.sleeve_weights.keys()) - sleeve_ids
            if unknown:
                raise ValueError(f"Unknown sleeve_ids in portfolio_aggregation.sleeve_weights: {unknown}")
        return self

    def effective_execution(self) -> ExecutionConfig:
        """Return ExecutionConfig with run_overrides applied."""
        if self.run_overrides is None:
            return self.execution
        cfg = self.execution.model_copy()
        ov = self.run_overrides
        if ov.initial_capital is not None:
            cfg.initial_capital = ov.initial_capital
        if ov.fill_rule is not None:
            cfg.fill_rule = ov.fill_rule
        if ov.commission_bps is not None:
            cfg.commission_bps = ov.commission_bps
        return cfg

    def effective_date_range(self) -> DateRange:
        """Return DateRange with run_overrides applied."""
        if self.run_overrides is None:
            return self.date_range
        dr = self.date_range.model_copy()
        ov = self.run_overrides
        if ov.start_date is not None:
            dr.start = ov.start_date
        if ov.end_date is not None:
            dr.end = ov.end_date
        return dr
