"""
Constraint enforcement — applies portfolio constraints after initial allocation.

Handles:
- Max/min weight per stock
- Sector weight caps
- Small-cap aggregate weight
- Contest-specific rules (Samsung cap, sector multiplier)
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import warnings

import numpy as np
import pandas as pd

from backtest_engine.strategy_ir.models import ConstraintSet, RunMode


def apply_constraints(
    weights: pd.Series,
    snapshot: pd.DataFrame,
    constraints: ConstraintSet,
    mode: RunMode = RunMode.RESEARCH,
    benchmark_sector_weights: Optional[Dict[str, float]] = None,
    prev_weights: Optional[pd.Series] = None,
    max_iter: int = 10,
) -> Tuple[pd.Series, List[str]]:
    """
    Apply constraint set to a weight vector. Returns (adjusted_weights, violations).

    Uses iterative capping and re-normalization.

    Parameters
    ----------
    weights : pd.Series
        ticker -> weight (should sum to 1 - cash_buffer).
    snapshot : pd.DataFrame
        Indexed by ticker; must have 'sector_name' and 'market_cap'.
    constraints : ConstraintSet
    mode : RunMode
    benchmark_sector_weights : dict, optional
        sector_name -> benchmark sector weight (fraction).
    prev_weights : pd.Series, optional
        Previous weights (for turnover monitoring).
    max_iter : int
        Iterations for the iterative constraint enforcement loop.

    Returns
    -------
    (adjusted_weights, list_of_violation_messages)
    """
    violations: List[str] = []
    w = weights.copy().reindex(snapshot.index).fillna(0.0)

    target_sum = weights.sum()

    for iteration in range(max_iter):
        changed = False

        # 1. Max weight per stock — iterative capping
        if constraints.max_weight < 1.0:
            # Determine per-stock caps
            per_stock_cap = pd.Series(constraints.max_weight, index=w.index)
            if mode == RunMode.CONTEST and "005930" in w.index:
                per_stock_cap["005930"] = constraints.contest_samsung_cap

            # Find stocks exceeding their cap
            over_cap = w > per_stock_cap + 1e-8
            if over_cap.any():
                excess = (w[over_cap] - per_stock_cap[over_cap]).sum()
                w[over_cap] = per_stock_cap[over_cap]
                # Redistribute to stocks with room below their cap
                room = (per_stock_cap - w).clip(lower=0.0)
                room[over_cap] = 0.0  # already capped — no more room
                total_room = room.sum()
                if total_room > 1e-8:
                    w += room / total_room * excess
                    # Clip to cap again after redistribution
                    w = w.clip(upper=per_stock_cap)
                changed = True

        # 2. Min weight per stock (below min = set to 0)
        if constraints.min_weight > 0:
            below_min = (w > 0) & (w < constraints.min_weight)
            if below_min.any():
                w[below_min] = 0.0
                changed = True

        # 3. Sector weight constraints
        if "sector_name" in snapshot.columns:
            sector = snapshot["sector_name"].reindex(w.index).fillna("Unknown")
            changed_sector = _apply_sector_constraints(
                w, sector, constraints, mode, benchmark_sector_weights, violations
            )
            if changed_sector:
                changed = True

        # 4. Small-cap aggregate cap
        if constraints.max_small_mcap_weight is not None and "market_cap" in snapshot.columns:
            mcap = snapshot["market_cap"].reindex(w.index).fillna(0.0)
            threshold_krw = constraints.small_mcap_threshold_bn * 1e9
            small = mcap < threshold_krw
            small_weight = w[small].sum()
            if small_weight > constraints.max_small_mcap_weight:
                scale = constraints.max_small_mcap_weight / small_weight
                w[small] *= scale
                # Redistribute freed weight
                large = ~small & (w > 0)
                freed = small_weight * (1 - scale)
                if large.any() and w[large].sum() > 1e-8:
                    w[large] += freed * (w[large] / w[large].sum())
                violations.append(
                    f"Small-cap weight {small_weight:.1%} exceeded {constraints.max_small_mcap_weight:.1%} cap; adjusted"
                )
                changed = True

        # Re-normalize to target sum
        current_sum = w.sum()
        if current_sum > 1e-8:
            w = w / current_sum * target_sum

        if not changed:
            break

    # Final clip to ensure non-negative
    w = w.clip(lower=0.0)

    # Record final violations
    if constraints.max_names is not None and (w > 0).sum() > constraints.max_names:
        violations.append(f"Names {(w > 0).sum()} exceeds max_names {constraints.max_names}")

    if (w > 0).sum() < constraints.min_names:
        violations.append(f"Names {(w > 0).sum()} below min_names {constraints.min_names}")

    return w, violations


def _apply_sector_constraints(
    w: pd.Series,
    sector: pd.Series,
    constraints: ConstraintSet,
    mode: RunMode,
    benchmark_sector_weights: Optional[Dict[str, float]],
    violations: List[str],
) -> bool:
    """Apply sector caps. Returns True if weights were modified."""
    changed = False
    sector_weights = {}
    for sec in sector.unique():
        mask = sector == sec
        sector_weights[sec] = w[mask].sum()

    for sec, sec_w in sector_weights.items():
        cap = None

        # Absolute cap
        if constraints.max_sector_weight is not None:
            cap = constraints.max_sector_weight

        # Multiplier-based cap (contest mode)
        if constraints.max_sector_multiplier is not None and benchmark_sector_weights:
            bw = benchmark_sector_weights.get(sec, 0.0)
            if mode == RunMode.CONTEST:
                if bw <= 0.05:
                    multiplier_cap = constraints.contest_sector_small_cap
                else:
                    multiplier_cap = bw * constraints.contest_sector_large_multiplier
            else:
                multiplier_cap = bw * constraints.max_sector_multiplier
            cap = min(cap, multiplier_cap) if cap is not None else multiplier_cap

        if cap is not None and sec_w > cap + 1e-6:
            mask = sector == sec
            scale = cap / sec_w
            freed = sec_w * (1 - scale)
            w[mask] *= scale
            # Redistribute to other sectors
            other = ~mask & (w > 0)
            if other.any() and w[other].sum() > 1e-8:
                w[other] += freed * (w[other] / w[other].sum())
            violations.append(
                f"Sector '{sec}' weight {sec_w:.1%} exceeded cap {cap:.1%}; adjusted"
            )
            changed = True

    return changed


def check_turnover_constraint(
    new_weights: pd.Series,
    prev_weights: pd.Series,
    max_turnover: Optional[float],
) -> Optional[str]:
    """Check if turnover exceeds the allowed maximum."""
    if max_turnover is None:
        return None
    all_tickers = new_weights.index.union(prev_weights.index)
    nw = new_weights.reindex(all_tickers).fillna(0.0)
    pw = prev_weights.reindex(all_tickers).fillna(0.0)
    turnover = (nw - pw).abs().sum() / 2
    if turnover > max_turnover:
        return f"Turnover {turnover:.2%} exceeds max {max_turnover:.2%}"
    return None


def compute_weekly_turnover(
    weights_history: Dict[str, pd.Series],
    nav_history: Dict[str, float],
) -> float:
    """
    Compute weekly turnover as:
    (total buys + total sells) / (2 * average NAV in the period)
    """
    if len(weights_history) < 2:
        return 0.0

    dates = sorted(weights_history.keys())
    total_trades = 0.0
    avg_nav = np.mean(list(nav_history.values())) if nav_history else 1.0

    for i in range(1, len(dates)):
        d_prev, d_cur = dates[i - 1], dates[i]
        all_t = weights_history[d_prev].index.union(weights_history[d_cur].index)
        prev_w = weights_history[d_prev].reindex(all_t).fillna(0.0)
        cur_w = weights_history[d_cur].reindex(all_t).fillna(0.0)
        total_trades += (cur_w - prev_w).abs().sum()

    return total_trades / (2 * max(avg_nav, 1e-6)) if total_trades > 0 else 0.0
