"""
MetricsEngine — computes all performance statistics from a result bundle.

All calculations are deterministic and engine-driven.
LLM receives these numbers — it never computes them itself.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd


class MetricsEngine:
    """Compute performance metrics from nav_series and trade_history."""

    def __init__(self, annualization_factor: int = 252):
        self.af = annualization_factor

    def compute_all(self, bundle: Dict[str, Any]) -> Dict[str, Any]:
        """Compute all metrics and return as a dict."""
        nav_series = pd.Series(bundle["nav_series"]).sort_index()
        bm_nav_series = pd.Series(bundle.get("benchmark_nav_series", {})).sort_index()
        initial_capital = bundle.get("initial_capital", 1_000_000_000)

        if nav_series.empty:
            return {"error": "Empty NAV series"}

        # Daily returns
        port_returns = nav_series.pct_change().dropna()
        bm_returns = bm_nav_series.pct_change().dropna() if not bm_nav_series.empty else pd.Series(dtype=float)

        # Align
        common_dates = port_returns.index.intersection(bm_returns.index)

        metrics: Dict[str, Any] = {}

        # Basic return metrics
        total_ret = (nav_series.iloc[-1] / nav_series.iloc[0]) - 1
        n_years = len(nav_series) / self.af
        cagr = (1 + total_ret) ** (1 / max(n_years, 1e-3)) - 1 if n_years > 0 else 0.0

        metrics["total_return"] = round(total_ret, 6)
        metrics["cagr"] = round(cagr, 6)
        metrics["start_nav"] = round(nav_series.iloc[0], 2)
        metrics["end_nav"] = round(nav_series.iloc[-1], 2)
        metrics["start_date"] = nav_series.index[0]
        metrics["end_date"] = nav_series.index[-1]
        metrics["n_trading_days"] = len(nav_series)

        # Volatility and risk
        ann_vol = port_returns.std() * np.sqrt(self.af)
        metrics["annualized_vol"] = round(ann_vol, 6)

        # Sharpe ratio (assuming risk-free rate = 0 for simplicity)
        sharpe = port_returns.mean() / (port_returns.std() + 1e-10) * np.sqrt(self.af)
        metrics["sharpe"] = round(sharpe, 4)

        # Sortino ratio
        downside = port_returns[port_returns < 0]
        downside_std = downside.std() * np.sqrt(self.af) if len(downside) > 1 else 1e-10
        sortino = (port_returns.mean() * self.af) / (downside_std + 1e-10)
        metrics["sortino"] = round(sortino, 4)

        # Max drawdown
        max_dd, peak_date, trough_date, dd_duration = self._max_drawdown(nav_series)
        metrics["max_drawdown"] = round(max_dd, 6)
        metrics["max_dd_peak_date"] = peak_date
        metrics["max_dd_trough_date"] = trough_date
        metrics["max_dd_duration_days"] = dd_duration  # trading days peak→trough

        # Win rate
        win_rate = (port_returns > 0).sum() / max(len(port_returns), 1)
        metrics["win_rate"] = round(win_rate, 4)

        # Calmar ratio
        calmar = cagr / abs(max_dd) if abs(max_dd) > 1e-6 else 0.0
        metrics["calmar"] = round(calmar, 4)

        # Benchmark-relative metrics
        if not bm_returns.empty and len(common_dates) > 20:
            port_aligned = port_returns.reindex(common_dates)
            bm_aligned = bm_returns.reindex(common_dates)

            # Tracking error
            excess = port_aligned - bm_aligned
            te = excess.std() * np.sqrt(self.af)
            metrics["tracking_error"] = round(te, 6)

            # Information ratio
            ir = (excess.mean() * self.af) / (te + 1e-10)
            metrics["information_ratio"] = round(ir, 4)

            # Benchmark return
            bm_total = (bm_nav_series.iloc[-1] / bm_nav_series.iloc[0]) - 1
            bm_cagr = (1 + bm_total) ** (1 / max(n_years, 1e-3)) - 1
            metrics["benchmark_total_return"] = round(bm_total, 6)
            metrics["benchmark_cagr"] = round(bm_cagr, 6)
            metrics["excess_return"] = round(total_ret - bm_total, 6)
            metrics["excess_cagr"] = round(cagr - bm_cagr, 6)

            # Beta
            if bm_aligned.std() > 1e-8:
                beta = np.cov(port_aligned, bm_aligned)[0, 1] / bm_aligned.var()
                metrics["beta"] = round(beta, 4)
            else:
                metrics["beta"] = None

        # Turnover (monthly average, one-way)
        trade_history = bundle.get("trade_history", [])
        metrics["average_monthly_turnover"] = self._compute_average_turnover(trade_history, nav_series)
        # Keep legacy key for backward compat
        metrics["average_turnover"] = metrics["average_monthly_turnover"]

        # Monthly returns table
        metrics["monthly_returns"] = self._monthly_returns(nav_series)

        # Drawdown series (for chart)
        metrics["drawdown_series"] = self._drawdown_series(nav_series)

        return metrics

    def _max_drawdown(self, nav: pd.Series) -> Tuple[float, str, str, int]:
        """Compute maximum drawdown, peak/trough dates, and duration (trading days)."""
        cum_max = nav.cummax()
        dd = (nav - cum_max) / cum_max
        min_idx = dd.idxmin()
        trough_val = dd[min_idx]
        # Find the peak before the trough
        peak_region = nav[:min_idx]
        peak_idx = peak_region.idxmax() if not peak_region.empty else min_idx
        # Duration: number of trading days from peak to trough (inclusive)
        dates = nav.index.tolist()
        try:
            peak_pos = dates.index(peak_idx)
            trough_pos = dates.index(min_idx)
            duration = trough_pos - peak_pos
        except ValueError:
            duration = 0
        return float(trough_val), str(peak_idx), str(min_idx), duration

    def _drawdown_series(self, nav: pd.Series) -> Dict[str, float]:
        """Return drawdown series as dict."""
        cum_max = nav.cummax()
        dd = (nav - cum_max) / cum_max
        return {str(k): round(float(v), 6) for k, v in dd.items()}

    def _monthly_returns(self, nav: pd.Series) -> List[Dict]:
        """Compute monthly return table."""
        nav_dt = nav.copy()
        nav_dt.index = pd.to_datetime(nav_dt.index)
        monthly = nav_dt.resample("ME").last()
        monthly_ret = monthly.pct_change().dropna()

        rows = []
        for date, ret in monthly_ret.items():
            rows.append({
                "year": date.year,
                "month": date.month,
                "return": round(float(ret), 6),
            })
        return rows

    def _compute_average_turnover(
        self,
        trade_history: List[Dict],
        nav_series: pd.Series,
    ) -> float:
        """Compute average monthly turnover."""
        if not trade_history:
            return 0.0
        trades_df = pd.DataFrame(trade_history)
        if trades_df.empty:
            return 0.0

        avg_nav = nav_series.mean()
        if avg_nav <= 0:
            return 0.0

        total_buys = trades_df[trades_df["direction"] == "buy"]["notional"].sum()
        total_sells = trades_df[trades_df["direction"] == "sell"]["notional"].sum()
        n_days = len(nav_series)
        n_months = max(1, n_days / 21)

        monthly_to = (total_buys + total_sells) / (2 * avg_nav) / n_months
        return round(monthly_to, 4)

    def compute_sector_exposure(
        self,
        weights_history: Dict[str, Dict[str, float]],
        snapshot_loader,
        universe_config,
    ) -> Dict[str, Dict[str, float]]:
        """Compute sector exposure over time."""
        result = {}
        from backtest_engine.strategy_ir.models import UniverseConfig
        uc = universe_config or UniverseConfig()

        for date in sorted(weights_history.keys()):
            w = weights_history[date]
            if not w:
                result[date] = {}
                continue

            try:
                snap = snapshot_loader.load_snapshot(date, uc, [])
                sector_exp: Dict[str, float] = {}
                for ticker, weight in w.items():
                    if ticker in snap.index:
                        sec = snap.loc[ticker, "sector_name"] if "sector_name" in snap.columns else "Unknown"
                        sector_exp[str(sec)] = sector_exp.get(str(sec), 0.0) + weight
                result[date] = sector_exp
            except Exception:
                result[date] = {}

        return result

    def compute_top_holdings(
        self,
        weights_history: Dict[str, Dict[str, float]],
        n: int = 20,
    ) -> List[Dict]:
        """Compute average holdings over the backtest period."""
        if not weights_history:
            return []
        all_weights = pd.DataFrame(weights_history).T.fillna(0.0)
        avg_weights = all_weights.mean().sort_values(ascending=False)
        return [
            {"ticker": t, "avg_weight": round(float(w), 6)}
            for t, w in avg_weights.head(n).items()
        ]

    def compute_top_trades(
        self,
        trade_history: List[Dict],
        n: int = 20,
    ) -> List[Dict]:
        """Return top trades by notional."""
        if not trade_history:
            return []
        df = pd.DataFrame(trade_history).sort_values("notional", ascending=False)
        return df.head(n).to_dict("records")
