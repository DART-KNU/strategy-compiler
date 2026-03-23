"""
Universe selector — converts scored universe into a selected set of tickers.

Takes a scored Series (ticker -> score) and returns a filtered Series.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from backtest_engine.strategy_ir.models import SelectionConfig, SelectionMethod


def select_universe(
    scores: pd.Series,
    config: SelectionConfig,
    min_names: int = 5,
) -> pd.Series:
    """
    Apply selection rules to a scored Series and return the selected subset.

    Parameters
    ----------
    scores : pd.Series
        Indexed by ticker, values are scores (higher = better).
        NaN scores are excluded.
    config : SelectionConfig
    min_names : int
        Minimum number of names; if selection yields fewer, return top-N anyway.

    Returns
    -------
    pd.Series : Selected subset of scores (same values, filtered index).
    """
    # Drop NaN
    clean = scores.dropna()
    if clean.empty:
        return clean

    method = config.method

    if method == SelectionMethod.OPTIMIZER_ONLY:
        # All non-NaN tickers passed to optimizer
        return clean

    if method == SelectionMethod.ALL_POSITIVE:
        selected = clean[clean > 0]
        if len(selected) < min_names:
            selected = clean.nlargest(min(min_names, len(clean)))
        return selected

    if method == SelectionMethod.THRESHOLD:
        thresh = config.threshold or 0.0
        selected = clean[clean >= thresh]
        if len(selected) < min_names:
            selected = clean.nlargest(min(min_names, len(clean)))
        return selected

    if method == SelectionMethod.TOP_N:
        n = config.n or min_names
        return clean.nlargest(min(n, len(clean)))

    if method == SelectionMethod.TOP_PCT:
        pct = config.pct or 0.1
        n = max(min_names, int(len(clean) * pct))
        return clean.nlargest(min(n, len(clean)))

    # Fallback
    return clean.nlargest(min(min_names, len(clean)))


def apply_max_names_filter(selected: pd.Series, max_names: Optional[int]) -> pd.Series:
    """Trim selected to max_names by keeping highest scores."""
    if max_names is None or len(selected) <= max_names:
        return selected
    return selected.nlargest(max_names)
