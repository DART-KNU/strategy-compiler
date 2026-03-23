"""
Portfolio allocators — convert a selected universe + optional signals to target weights.

Each allocator returns a pd.Series (ticker -> weight), summing to ~1.0 (less cash buffer).
"""

from __future__ import annotations

import warnings
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from backtest_engine.portfolio.risk import estimate_covariance
from backtest_engine.strategy_ir.models import (
    AllocatorConfig,
    EqualWeightConfig,
    ScoreWeightedConfig,
    InverseVolConfig,
    MeanVarianceConfig,
    BenchmarkTrackingConfig,
    EnhancedIndexConfig,
    RiskBudgetConfig,
    CovarianceModel,
    ConstraintSet,
)


def allocate(
    tickers: list[str],
    scores: Optional[pd.Series],
    snapshot: pd.DataFrame,
    config: AllocatorConfig,
    constraints: ConstraintSet,
    returns_history: Optional[pd.DataFrame] = None,
    prev_weights: Optional[pd.Series] = None,
    benchmark_weights: Optional[pd.Series] = None,
) -> pd.Series:
    """
    Dispatch to the appropriate allocator.

    Parameters
    ----------
    tickers : list[str]
        Selected tickers to allocate over.
    scores : pd.Series or None
        Signal scores indexed by ticker (optional for some allocators).
    snapshot : pd.DataFrame
        Cross-sectional data indexed by ticker.
    config : AllocatorConfig
        Allocator configuration.
    constraints : ConstraintSet
        Portfolio constraints.
    returns_history : pd.DataFrame or None
        (date x ticker) returns DataFrame for covariance estimation.
    prev_weights : pd.Series or None
        Previous portfolio weights (for turnover-penalized allocators).
    benchmark_weights : pd.Series or None
        Benchmark constituent weights (for tracking/enhanced allocators).

    Returns
    -------
    pd.Series : ticker -> weight, summing to (1 - target_cash_weight).
    """
    if not tickers:
        return pd.Series(dtype=float)

    # Align all inputs to selected tickers
    tickers = [t for t in tickers if t in snapshot.index]
    if not tickers:
        return pd.Series(dtype=float)

    alloc_type = config.type

    if alloc_type == "equal_weight":
        raw = _equal_weight(tickers)
    elif alloc_type == "score_weighted":
        raw = _score_weighted(tickers, scores, config)
    elif alloc_type == "inverse_vol":
        raw = _inverse_vol(tickers, snapshot, config)
    elif alloc_type == "mean_variance":
        raw = _mean_variance(tickers, scores, snapshot, config, constraints, returns_history)
    elif alloc_type == "benchmark_tracking":
        raw = _benchmark_tracking(tickers, snapshot, config, constraints, returns_history, benchmark_weights, prev_weights)
    elif alloc_type == "enhanced_index":
        raw = _enhanced_index(tickers, scores, snapshot, config, constraints, returns_history, benchmark_weights, prev_weights)
    elif alloc_type == "risk_budget":
        raw = _risk_budget(tickers, snapshot, config, returns_history)
    else:
        raise ValueError(f"Unknown allocator type: {alloc_type}")

    # Apply max/min weight clipping
    raw = _apply_weight_bounds(raw, constraints)

    # Normalize to (1 - cash_buffer)
    total = raw.sum()
    target_invested = 1.0 - constraints.target_cash_weight
    if total > 1e-6:
        raw = raw / total * target_invested
    else:
        # Optimizer returned all-zero weights — fall back to equal weight
        raw = pd.Series(target_invested / len(tickers), index=tickers)
    return raw


# ============================================================
# Allocator implementations
# ============================================================

def _equal_weight(tickers: list[str]) -> pd.Series:
    n = len(tickers)
    return pd.Series(1.0 / n, index=tickers)


def _score_weighted(
    tickers: list[str],
    scores: Optional[pd.Series],
    config: ScoreWeightedConfig,
) -> pd.Series:
    if scores is None or scores.empty:
        return _equal_weight(tickers)

    s = scores.reindex(tickers).fillna(0.0)
    if config.clip_negative:
        s = s.clip(lower=0.0)

    s = s ** config.power
    total = s.sum()
    if total < 1e-10:
        return _equal_weight(tickers)
    return s / total


def _inverse_vol(
    tickers: list[str],
    snapshot: pd.DataFrame,
    config: InverseVolConfig,
) -> pd.Series:
    vol_col = config.vol_field
    floor = config.floor_vol

    if vol_col in snapshot.columns:
        vol = snapshot[vol_col].reindex(tickers).fillna(0.15)
    else:
        vol = pd.Series(0.15, index=tickers)

    vol = vol.clip(lower=floor)
    inv_vol = 1.0 / vol
    return inv_vol / inv_vol.sum()


def _mean_variance(
    tickers: list[str],
    scores: Optional[pd.Series],
    snapshot: pd.DataFrame,
    config: MeanVarianceConfig,
    constraints: ConstraintSet,
    returns_history: Optional[pd.DataFrame],
) -> pd.Series:
    n = len(tickers)

    # Build alpha vector
    if scores is not None and not scores.empty:
        alpha = scores.reindex(tickers).fillna(0.0).values
    else:
        alpha = np.ones(n) / n

    # Normalize alpha
    alpha_std = alpha.std()
    if alpha_std > 1e-8:
        alpha = (alpha - alpha.mean()) / alpha_std

    # Covariance
    cov = _build_cov(tickers, returns_history, config.cov_model, config.cov_lookback)

    # Objective: minimize -alpha'w + 0.5 * gamma * w'Cov*w
    gamma = config.risk_aversion

    def objective(w: np.ndarray) -> float:
        return -alpha @ w + 0.5 * gamma * w @ cov @ w

    def grad(w: np.ndarray) -> np.ndarray:
        return -alpha + gamma * cov @ w

    # Constraints
    bounds = [(constraints.min_weight, constraints.max_weight)] * n
    scipy_constraints = []
    if config.fully_invested:
        target = 1.0 - constraints.target_cash_weight
        scipy_constraints.append({"type": "eq", "fun": lambda w: w.sum() - target})
    if config.long_only:
        bounds = [(max(0.0, constraints.min_weight), constraints.max_weight)] * n

    w0 = np.ones(n) / n * (1.0 - constraints.target_cash_weight)
    result = _run_optimizer(objective, grad, w0, bounds, scipy_constraints)
    return pd.Series(result, index=tickers)


def _benchmark_tracking(
    tickers: list[str],
    snapshot: pd.DataFrame,
    config: BenchmarkTrackingConfig,
    constraints: ConstraintSet,
    returns_history: Optional[pd.DataFrame],
    benchmark_weights: Optional[pd.Series],
    prev_weights: Optional[pd.Series],
) -> pd.Series:
    """Minimize tracking error to benchmark using available tickers."""
    n = len(tickers)

    # Build benchmark weight vector over selected tickers
    bw = _get_benchmark_proxy(tickers, benchmark_weights, snapshot)

    # Covariance
    cov = _build_cov(tickers, returns_history, config.cov_model)

    # Turnover penalty
    prev_w = (prev_weights or pd.Series(dtype=float)).reindex(tickers).fillna(0.0).values

    def objective(w: np.ndarray) -> float:
        diff = w - bw
        te = diff @ cov @ diff
        turnover = np.sum(np.abs(w - prev_w))
        return te + config.turnover_penalty * turnover

    def grad(w: np.ndarray) -> np.ndarray:
        diff = w - bw
        return 2 * cov @ diff + config.turnover_penalty * np.sign(w - prev_w)

    target_invested = 1.0 - constraints.target_cash_weight
    bounds = [(0.0, constraints.max_weight)] * n
    scipy_constraints = [{"type": "eq", "fun": lambda w: w.sum() - target_invested}]

    if config.te_target is not None:
        te_limit = config.te_target ** 2
        _bw = bw  # capture for closure
        _cov = cov
        scipy_constraints.append({
            "type": "ineq",
            "fun": lambda w, _tl=te_limit, _b=_bw, _c=_cov: _tl - (w - _b) @ _c @ (w - _b),
        })

    w0 = bw.copy()
    w0 = w0 / (w0.sum() + 1e-10) * target_invested
    result = _run_optimizer(objective, grad, w0, bounds, scipy_constraints)
    return pd.Series(result, index=tickers)


def _enhanced_index(
    tickers: list[str],
    scores: Optional[pd.Series],
    snapshot: pd.DataFrame,
    config: EnhancedIndexConfig,
    constraints: ConstraintSet,
    returns_history: Optional[pd.DataFrame],
    benchmark_weights: Optional[pd.Series],
    prev_weights: Optional[pd.Series],
) -> pd.Series:
    """
    maximize alpha'w - te_penalty * (w-bw)' * Cov * (w-bw) - turnover_penalty * |w - w_prev|
    """
    n = len(tickers)

    # Alpha
    if scores is not None and not scores.empty:
        alpha = scores.reindex(tickers).fillna(0.0).values
    else:
        alpha = np.zeros(n)

    # Normalize alpha
    alpha_std = alpha.std()
    if alpha_std > 1e-8:
        alpha = (alpha - alpha.mean()) / alpha_std * config.alpha_weight

    # Benchmark proxy
    bw = _get_benchmark_proxy(tickers, benchmark_weights, snapshot)

    # Covariance
    cov = _build_cov(tickers, returns_history, config.cov_model)

    prev_w = (prev_weights or pd.Series(dtype=float)).reindex(tickers).fillna(0.0).values
    te_pen = config.te_penalty
    to_pen = config.turnover_penalty

    def objective(w: np.ndarray) -> float:
        diff = w - bw
        te = diff @ cov @ diff
        turnover = np.sum(np.abs(w - prev_w))
        return -alpha @ w + te_pen * te + to_pen * turnover

    def grad(w: np.ndarray) -> np.ndarray:
        diff = w - bw
        return -alpha + 2 * te_pen * cov @ diff + to_pen * np.sign(w - prev_w)

    target_invested = 1.0 - constraints.target_cash_weight
    bounds = [(0.0, constraints.max_weight)] * n
    scipy_constraints = [{"type": "eq", "fun": lambda w: w.sum() - target_invested}]

    # Add TE constraint if specified
    if config.te_target is not None:
        te_limit = config.te_target ** 2
        scipy_constraints.append({
            "type": "ineq",
            "fun": lambda w: te_limit - (w - bw) @ cov @ (w - bw)
        })

    w0 = bw.copy()
    w0 = w0 / (w0.sum() + 1e-10) * target_invested
    result = _run_optimizer(objective, grad, w0, bounds, scipy_constraints)
    return pd.Series(result, index=tickers)


def _risk_budget(
    tickers: list[str],
    snapshot: pd.DataFrame,
    config: RiskBudgetConfig,
    returns_history: Optional[pd.DataFrame],
) -> pd.Series:
    """Risk budgeting via iterative algorithm."""
    n = len(tickers)
    budgets_raw = [config.budgets.get(t, 1.0 / n) for t in tickers]
    budgets = np.array(budgets_raw)
    budgets = budgets / budgets.sum()

    cov = _build_cov(tickers, returns_history, config.cov_model)

    # Newton-based risk parity
    w = np.ones(n) / n
    for _ in range(200):
        sigma = np.sqrt(w @ cov @ w)
        mrc = cov @ w / sigma  # marginal risk contribution
        rc = w * mrc
        grad = rc - budgets * sigma
        w = w - 0.1 * grad
        w = np.maximum(w, 1e-6)
        w = w / w.sum()

    return pd.Series(w, index=tickers)


# ============================================================
# Helpers
# ============================================================

def _build_cov(
    tickers: list[str],
    returns_history: Optional[pd.DataFrame],
    model: CovarianceModel,
    lookback: int = 60,
) -> np.ndarray:
    n = len(tickers)
    if returns_history is None or returns_history.empty:
        return np.eye(n) * (0.02 ** 2)   # fallback: 2% daily vol

    r = returns_history[
        [t for t in tickers if t in returns_history.columns]
    ].iloc[-lookback:]

    if r.empty or r.shape[0] < 5:
        return np.eye(n) * (0.02 ** 2)

    cov_small = estimate_covariance(r, model=model, annualize=True)

    # Expand to full n x n
    present_tickers = [t for t in tickers if t in returns_history.columns]
    cov_full = np.eye(n) * (0.20 ** 2)   # default 20% annual vol for missing tickers

    for i, ti in enumerate(tickers):
        for j, tj in enumerate(tickers):
            if ti in present_tickers and tj in present_tickers:
                pi = present_tickers.index(ti)
                pj = present_tickers.index(tj)
                cov_full[i, j] = cov_small[pi, pj]

    return _nearestPD(cov_full)


def _nearestPD(A: np.ndarray) -> np.ndarray:
    B = (A + A.T) / 2
    eigvals, eigvecs = np.linalg.eigh(B)
    eigvals = np.maximum(eigvals, 1e-8)
    return eigvecs @ np.diag(eigvals) @ eigvecs.T


def _get_benchmark_proxy(
    tickers: list[str],
    benchmark_weights: Optional[pd.Series],
    snapshot: pd.DataFrame,
) -> np.ndarray:
    """
    Build a benchmark proxy weight vector over selected tickers.

    If benchmark_weights provided: use as-is (normalized).
    Otherwise: approximate using market-cap weights of selected tickers.
    """
    if benchmark_weights is not None and not benchmark_weights.empty:
        bw = benchmark_weights.reindex(tickers).fillna(0.0)
    elif "market_cap" in snapshot.columns:
        mcap = snapshot["market_cap"].reindex(tickers).fillna(0.0)
        bw = mcap
    else:
        bw = pd.Series(1.0, index=tickers)

    total = bw.sum()
    if total < 1e-10:
        return np.ones(len(tickers)) / len(tickers)
    return (bw / total).values


def _apply_weight_bounds(w: pd.Series, constraints: ConstraintSet) -> pd.Series:
    """Apply min/max weight bounds."""
    w = w.clip(lower=constraints.min_weight, upper=constraints.max_weight)
    # Drop below minimum (set to 0 if < min_weight after clipping ensures they're at min)
    return w


def _run_optimizer(
    objective,
    grad,
    w0: np.ndarray,
    bounds,
    scipy_constraints,
    max_iter: int = 500,
) -> np.ndarray:
    """Run SciPy SLSQP optimizer with fallback to equal weight."""
    import sys
    n = len(w0)
    try:
        result = minimize(
            objective,
            w0,
            jac=grad,
            method="SLSQP",
            bounds=bounds,
            constraints=scipy_constraints,
            options={"maxiter": max_iter, "ftol": 1e-8},
        )
        if result.success or result.status in (0, 1):
            w = np.maximum(result.x, 0.0)
            total = w.sum()
            if total > 1e-6:
                return w / total  # normalize so weights sum to 1 before cash adjustment
            print(f"  [경고] 옵티마이저 결과 합계가 0에 가까움 — 동일 비중으로 대체합니다.", file=sys.stderr)
        else:
            print(
                f"  [경고] 옵티마이저 수렴 실패 (status={result.status}, msg={result.message}) "
                "— 동일 비중으로 대체합니다.",
                file=sys.stderr,
            )
    except Exception as e:
        warnings.warn(f"Optimizer failed: {e}. Falling back to equal weight.")
        print(f"  [경고] 옵티마이저 예외 발생: {e} — 동일 비중으로 대체합니다.", file=sys.stderr)

    # Fallback: equal weight
    return np.ones(n) / n
