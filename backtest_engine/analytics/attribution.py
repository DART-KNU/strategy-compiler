"""
Performance attribution — Brinson-style sleeve and sector attribution.

Provides:
- Sleeve attribution: per-sleeve contribution to total return
- Sector attribution: allocation + selection effects
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np


def compute_sleeve_attribution(
    weights_history: Dict[str, Dict[str, Dict[str, float]]],
    nav_series: pd.Series,
    returns_history: Optional[pd.DataFrame] = None,
) -> List[Dict]:
    """
    Compute sleeve-level return attribution.

    Parameters
    ----------
    weights_history : dict
        {date -> {sleeve_id -> {ticker -> weight}}}
        (Currently: {date -> {ticker -> weight}} — single sleeve)
    nav_series : pd.Series
    returns_history : pd.DataFrame, optional
        (date x ticker) daily returns.

    Returns
    -------
    list of dicts with attribution by sleeve.
    """
    # Placeholder: single portfolio attribution
    if nav_series.empty:
        return []

    port_ret = nav_series.pct_change().dropna()

    if returns_history is not None and not returns_history.empty:
        # Compute holdings-weighted return per date
        contrib = []
        dates = sorted(weights_history.keys())
        for i, date in enumerate(dates):
            if i == 0:
                continue
            prev_date = dates[i - 1]
            w = pd.Series(weights_history.get(prev_date, {}))
            ret_row = returns_history.loc[date] if date in returns_history.index else pd.Series()
            if w.empty or ret_row.empty:
                continue
            common = w.index.intersection(ret_row.index)
            contribution = (w.reindex(common) * ret_row.reindex(common)).sum()
            contrib.append({"date": date, "portfolio_contribution": round(float(contribution), 6)})
        return contrib

    return [{"date": d, "portfolio_nav": float(v)} for d, v in nav_series.items()]


def compute_brinson_attribution(
    portfolio_weights: pd.Series,
    benchmark_weights: pd.Series,
    portfolio_returns: pd.Series,
    benchmark_returns: pd.Series,
    sector: pd.Series,
) -> pd.DataFrame:
    """
    Brinson-Hood-Beebower attribution.

    Returns a DataFrame with columns:
    - allocation_effect
    - selection_effect
    - interaction_effect
    - total_effect
    Indexed by sector.
    """
    secs = sector.unique()
    results = []

    for sec in secs:
        mask = sector == sec

        wp = portfolio_weights.reindex(sector[mask].index).fillna(0.0).sum()
        wb = benchmark_weights.reindex(sector[mask].index).fillna(0.0).sum()

        rp_sec = (portfolio_returns.reindex(sector[mask].index).fillna(0.0) *
                  portfolio_weights.reindex(sector[mask].index).fillna(0.0)).sum() / max(wp, 1e-10)
        rb_sec = (benchmark_returns.reindex(sector[mask].index).fillna(0.0) *
                  benchmark_weights.reindex(sector[mask].index).fillna(0.0)).sum() / max(wb, 1e-10)
        rb_total = (benchmark_returns * benchmark_weights.reindex(benchmark_returns.index).fillna(0.0)).sum()

        alloc = (wp - wb) * (rb_sec - rb_total)
        select = wb * (rp_sec - rb_sec)
        interact = (wp - wb) * (rp_sec - rb_sec)

        results.append({
            "sector": sec,
            "portfolio_weight": round(wp, 4),
            "benchmark_weight": round(wb, 4),
            "portfolio_return": round(rp_sec, 4),
            "benchmark_return": round(rb_sec, 4),
            "allocation_effect": round(alloc, 6),
            "selection_effect": round(select, 6),
            "interaction_effect": round(interact, 6),
            "total_effect": round(alloc + select + interact, 6),
        })

    return pd.DataFrame(results)
