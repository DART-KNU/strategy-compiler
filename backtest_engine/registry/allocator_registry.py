"""Allocator registry — describes available allocator types and their parameters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass
class AllocatorMeta:
    allocator_id: str
    description: str
    required_params: List[str]
    optional_params: Dict[str, object]  # param -> default
    needs_score: bool
    needs_covariance: bool
    notes: str = ""


ALLOCATOR_REGISTRY: dict[str, AllocatorMeta] = {m.allocator_id: m for m in [
    AllocatorMeta(
        "equal_weight",
        "Equal weight across selected stocks",
        [], {}, False, False
    ),
    AllocatorMeta(
        "score_weighted",
        "Weight proportional to score (optionally raised to a power)",
        [], {"power": 1.0, "clip_negative": True}, True, False
    ),
    AllocatorMeta(
        "inverse_vol",
        "Weight inversely proportional to realized volatility",
        [], {"vol_field": "vol_20d", "floor_vol": 0.005}, False, False
    ),
    AllocatorMeta(
        "mean_variance",
        "Markowitz mean-variance optimization (alpha + covariance)",
        [], {"cov_model": "shrinkage_cov", "risk_aversion": 1.0}, True, True,
        notes="Uses SciPy SLSQP; interface compatible with cvxpy/osqp"
    ),
    AllocatorMeta(
        "benchmark_tracking",
        "Minimize tracking error to benchmark using proxy holdings",
        ["benchmark_index"], {"te_target": None, "top_k": None}, False, True,
        notes="Exact constituent replication not supported; uses proxy approach"
    ),
    AllocatorMeta(
        "enhanced_index",
        "Benchmark tracking + alpha tilt: maximize(alpha - te_penalty*TE - turnover_penalty*T)",
        ["benchmark_index"], {"te_target": None, "alpha_weight": 1.0}, True, True,
        notes="Requires alpha signal in node_graph"
    ),
    AllocatorMeta(
        "risk_budget",
        "Risk budgeting: each stock contributes target fraction of portfolio risk",
        ["budgets"], {}, False, True,
        notes="Placeholder — uses iterative shrinkage"
    ),
]}
