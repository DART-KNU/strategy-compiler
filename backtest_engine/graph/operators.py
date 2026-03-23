"""
Primitive operators for the node graph executor.

All operators take pandas Series/DataFrames indexed by ticker
and return a Series indexed by ticker (cross-sectional ops)
or modify a (date x ticker) DataFrame (time-series ops).

Convention:
- Cross-sectional operators (cs_*): input is a Series[ticker], output is Series[ticker]
- Time-series operators (ts_*): input is a (date x ticker) DataFrame, applied per ticker
- Combine operators: element-wise operations on Series
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional


# ============================================================
# Time-series operators
# ============================================================

def ts_lag(s: pd.Series, window: int = 1) -> pd.Series:
    """Shift series by n periods (for use inside rolling windows)."""
    return s.shift(window)


def ts_sma(s: pd.Series, window: int) -> pd.Series:
    """Simple moving average."""
    return s.rolling(window=window, min_periods=max(1, window // 2)).mean()


def ts_ema(s: pd.Series, window: int) -> pd.Series:
    """Exponential moving average."""
    return s.ewm(span=window, min_periods=max(1, window // 2), adjust=False).mean()


def ts_std(s: pd.Series, window: int) -> pd.Series:
    """Rolling standard deviation."""
    return s.rolling(window=window, min_periods=max(2, window // 2)).std()


def ts_mean(s: pd.Series, window: int) -> pd.Series:
    """Rolling mean (alias for sma)."""
    return ts_sma(s, window)


def ts_rank(s: pd.Series, window: int) -> pd.Series:
    """Rolling rank (0..1) of current value within the window."""
    def _rank_last(x: pd.Series) -> float:
        if len(x) < 2:
            return np.nan
        rank = pd.Series(x).rank(pct=True).iloc[-1]
        return rank
    return s.rolling(window=window, min_periods=max(2, window // 2)).apply(_rank_last, raw=False)


def ts_zscore(s: pd.Series, window: int) -> pd.Series:
    """Rolling z-score of current value."""
    roll = s.rolling(window=window, min_periods=max(2, window // 2))
    return (s - roll.mean()) / (roll.std() + 1e-8)


def ts_percentile(s: pd.Series, window: int) -> pd.Series:
    """Rolling percentile rank (same as ts_rank but explicit name)."""
    return ts_rank(s, window)


# ============================================================
# Cross-sectional operators
# ============================================================

def cs_rank(s: pd.Series) -> pd.Series:
    """Cross-sectional rank normalized to [0, 1]."""
    return s.rank(pct=True, na_option="keep")


def cs_zscore(s: pd.Series) -> pd.Series:
    """Cross-sectional z-score."""
    mu = s.mean()
    sigma = s.std()
    if sigma < 1e-10:
        return pd.Series(0.0, index=s.index)
    return (s - mu) / sigma


def cs_percentile(s: pd.Series) -> pd.Series:
    """Cross-sectional percentile rank (same as cs_rank)."""
    return cs_rank(s)


def cs_winsorize(s: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    """Winsorize at given quantiles."""
    lo = s.quantile(lower)
    hi = s.quantile(upper)
    return s.clip(lower=lo, upper=hi)


def cs_sector_neutralize(
    s: pd.Series,
    sector: pd.Series,
    method: str = "demean",
) -> pd.Series:
    """
    Sector neutralization.

    method='demean': subtract sector mean
    method='zscore': subtract sector mean, divide by sector std
    """
    result = s.copy()
    for sec, grp_idx in s.groupby(sector).groups.items():
        grp = s.loc[grp_idx]
        if grp.isna().all():
            continue
        if method == "zscore":
            mu, sigma = grp.mean(), grp.std()
            if sigma < 1e-10:
                result.loc[grp_idx] = 0.0
            else:
                result.loc[grp_idx] = (grp - mu) / sigma
        else:  # demean
            result.loc[grp_idx] = grp - grp.mean()
    return result


def cs_vol_scale(s: pd.Series, vol: pd.Series, target_vol: float = 0.15) -> pd.Series:
    """Scale scores by inverse volatility, targeting a given volatility level."""
    scale = target_vol / (vol.clip(lower=1e-6) * np.sqrt(252))
    return s * scale


# ============================================================
# Combine operators
# ============================================================

def add(a: pd.Series, b: pd.Series) -> pd.Series:
    return a + b


def sub(a: pd.Series, b: pd.Series) -> pd.Series:
    return a - b


def mul(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * b


def div(a: pd.Series, b: pd.Series, fill_inf: float = np.nan) -> pd.Series:
    result = a / b.replace(0, np.nan)
    return result.replace([np.inf, -np.inf], fill_inf)


def negate(a: pd.Series) -> pd.Series:
    return -a


def abs_op(a: pd.Series) -> pd.Series:
    return a.abs()


def clip(a: pd.Series, lower: Optional[float] = None, upper: Optional[float] = None) -> pd.Series:
    return a.clip(lower=lower, upper=upper)


def winsorize(a: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    return cs_winsorize(a, lower, upper)


def weighted_sum(inputs: list[pd.Series], weights: list[float]) -> pd.Series:
    """Weighted sum of series. Series are aligned by index."""
    if len(inputs) != len(weights):
        raise ValueError(f"weighted_sum: {len(inputs)} inputs but {len(weights)} weights")
    total = sum(w * s for w, s in zip(weights, inputs))
    return total


def if_else(condition: pd.Series, true_val: pd.Series, false_val: pd.Series) -> pd.Series:
    """Element-wise conditional selection."""
    return pd.Series(
        np.where(condition.fillna(False).astype(bool), true_val, false_val),
        index=condition.index,
    )


# ============================================================
# Predicate operators
# ============================================================

def gt(a: pd.Series, b: pd.Series) -> pd.Series:
    return a > b


def gte(a: pd.Series, b: pd.Series) -> pd.Series:
    return a >= b


def lt(a: pd.Series, b: pd.Series) -> pd.Series:
    return a < b


def lte(a: pd.Series, b: pd.Series) -> pd.Series:
    return a <= b


def eq(a: pd.Series, b: pd.Series) -> pd.Series:
    return a == b


def ne(a: pd.Series, b: pd.Series) -> pd.Series:
    return a != b


def logical_and(*args: pd.Series) -> pd.Series:
    result = args[0].astype(bool)
    for a in args[1:]:
        result = result & a.astype(bool)
    return result


def logical_or(*args: pd.Series) -> pd.Series:
    result = args[0].astype(bool)
    for a in args[1:]:
        result = result | a.astype(bool)
    return result


def logical_not(a: pd.Series) -> pd.Series:
    return ~a.astype(bool)


# ============================================================
# Operator dispatch tables
# ============================================================

TS_OPS = {
    "lag": ts_lag,
    "sma": ts_sma,
    "ema": ts_ema,
    "std": ts_std,
    "mean": ts_mean,
    "zscore": ts_zscore,
    "rank": ts_rank,
    "percentile": ts_percentile,
}

CS_OPS = {
    "rank": cs_rank,
    "zscore": cs_zscore,
    "percentile": cs_percentile,
    "winsorize": cs_winsorize,
    "sector_neutralize": cs_sector_neutralize,
    "vol_scale": cs_vol_scale,
}

COMBINE_OPS = {
    "add": add,
    "sub": sub,
    "mul": mul,
    "div": div,
    "negate": negate,
    "abs": abs_op,
    "clip": clip,
    "winsorize": winsorize,
    "weighted_sum": weighted_sum,
    "if_else": if_else,
    "vol_scale": cs_vol_scale,
}

PREDICATE_OPS = {
    "gt": gt,
    "gte": gte,
    "lt": lt,
    "lte": lte,
    "eq": eq,
    "ne": ne,
    "logical_and": logical_and,
    "logical_or": logical_or,
    "logical_not": logical_not,
}
