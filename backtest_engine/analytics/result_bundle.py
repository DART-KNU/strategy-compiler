"""
ReportBundleBuilder — assembles the final narrative-ready report bundle.

Takes a raw result bundle + computed metrics and produces a structured
JSON-serializable output that an LLM can interpret for narrative generation.

The LLM should NEVER compute numbers — it only interprets numbers from this bundle.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np

from backtest_engine.analytics.metrics import MetricsEngine
from backtest_engine.analytics.attribution import compute_sleeve_attribution


class ReportBundleBuilder:
    """
    Builds a complete, narrative-ready report bundle from a raw result bundle.

    Usage:
        builder = ReportBundleBuilder()
        report = builder.build(raw_bundle, strategy_ir, conn)
    """

    def __init__(self, annualization_factor: int = 252):
        self._metrics_engine = MetricsEngine(annualization_factor)

    def build(
        self,
        raw_bundle: Dict[str, Any],
        strategy_ir=None,
        conn=None,
    ) -> Dict[str, Any]:
        """
        Assemble the full report bundle.

        Parameters
        ----------
        raw_bundle : dict
            Output from ExecutionSimulator.run().
        strategy_ir : StrategyIR, optional
            Strategy definition for metadata.
        conn : sqlite3.Connection, optional
            Used for sector exposure computation.

        Returns
        -------
        dict : Complete report bundle ready for serialization/LLM consumption.
        """
        # Compute metrics
        metrics = self._metrics_engine.compute_all(raw_bundle)

        # NAV series
        nav_series = pd.Series(raw_bundle["nav_series"]).sort_index()
        bm_nav_series = pd.Series(raw_bundle.get("benchmark_nav_series", {})).sort_index()

        # Weights history
        weights_history = raw_bundle.get("weights_history", {})

        # Sector exposure
        sector_exposure = {}
        if conn is not None and weights_history:
            from backtest_engine.data.loaders import SnapshotLoader
            from backtest_engine.strategy_ir.models import UniverseConfig
            sl = SnapshotLoader(conn)
            uc = UniverseConfig(include_blocked=True)
            # Sample sector exposure at monthly dates to avoid excessive queries
            dates = sorted(weights_history.keys())
            monthly_dates = self._sample_monthly(dates)
            for d in monthly_dates:
                try:
                    snap = sl.load_snapshot(d, uc, [])
                    w = weights_history.get(d, {})
                    sec_exp: Dict[str, float] = {}
                    for ticker, weight in w.items():
                        if ticker in snap.index and "sector_name" in snap.columns:
                            sec = str(snap.loc[ticker, "sector_name"] or "Unknown")
                            sec_exp[sec] = sec_exp.get(sec, 0.0) + weight
                    sector_exposure[d] = sec_exp
                except Exception:
                    pass

        # Sleeve attribution (simple: single portfolio)
        sleeve_attr = compute_sleeve_attribution(weights_history, nav_series)

        # Top holdings (average)
        top_holdings = self._metrics_engine.compute_top_holdings(
            weights_history,
            n=20
        )

        # Top trades
        top_trades = self._metrics_engine.compute_top_trades(
            raw_bundle.get("trade_history", []),
            n=20
        )

        # Monthly returns as a pivot table
        monthly_ret_rows = metrics.pop("monthly_returns", [])
        monthly_ret_table = self._build_monthly_table(monthly_ret_rows)

        # Turnover history
        turnover_history = self._compute_turnover_history(
            raw_bundle.get("trade_history", []),
            nav_series,
        )

        # Build bundle
        report = {
            "run_id": raw_bundle.get("run_id"),
            "run_timestamp": raw_bundle.get("run_timestamp"),
            "strategy_id": raw_bundle.get("strategy_id"),
            "strategy_name": raw_bundle.get("strategy_name"),
            "mode": raw_bundle.get("mode"),
            "ir_version": raw_bundle.get("ir_version"),
            "ir_hash": raw_bundle.get("ir_hash"),
            "date_range": raw_bundle.get("date_range"),
            "initial_capital": raw_bundle.get("initial_capital"),
            "benchmark_index": raw_bundle.get("benchmark_index"),

            # Core performance data
            "summary_metrics": self._format_summary_metrics(metrics),

            # Time series (truncated for readability)
            "nav_series": {k: round(v, 2) for k, v in nav_series.items()},
            "benchmark_nav_series": {k: round(v, 2) for k, v in bm_nav_series.items()},
            "drawdown_series": metrics.pop("drawdown_series", {}),

            # Tables
            "monthly_returns_table": monthly_ret_table,
            "top_holdings": top_holdings,
            "top_trades": top_trades,
            "sector_exposure_history": sector_exposure,
            "turnover_history": turnover_history,

            # Attribution
            "sleeve_attribution": sleeve_attr,

            # Risk / constraints
            "constraint_violations": raw_bundle.get("constraint_violations", []),

            # Narration hints (populated by LLM, engine provides the data)
            "narration_hints": self._build_narration_hints(metrics, raw_bundle),
        }

        return report

    def _format_summary_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Format summary metrics for human readability."""
        fmt = {}
        pct_keys = {
            "total_return", "cagr", "annualized_vol", "max_drawdown",
            "win_rate", "tracking_error", "benchmark_total_return",
            "benchmark_cagr", "excess_return", "excess_cagr", "average_turnover"
        }
        ratio_keys = {"sharpe", "sortino", "information_ratio", "calmar", "beta"}

        for k, v in metrics.items():
            if k in ("monthly_returns", "drawdown_series"):
                continue
            if v is None:
                fmt[k] = None
            elif k in pct_keys and isinstance(v, float):
                fmt[k] = f"{v:.2%}"
            elif k in ratio_keys and isinstance(v, float):
                fmt[k] = round(v, 3)
            elif k in ("start_nav", "end_nav", "initial_capital") and isinstance(v, float):
                fmt[k] = f"{v:,.0f}"
            else:
                fmt[k] = v
        return fmt

    def _build_monthly_table(self, rows: List[Dict]) -> Dict[str, Dict[int, float]]:
        """Convert monthly return rows to year -> month -> return table."""
        table: Dict[int, Dict[int, float]] = {}
        for row in rows:
            year = row["year"]
            month = row["month"]
            ret = row["return"]
            if year not in table:
                table[year] = {}
            table[year][month] = round(ret * 100, 2)  # as percentage
        return {str(year): monthly for year, monthly in table.items()}

    def _compute_turnover_history(
        self,
        trade_history: List[Dict],
        nav_series: pd.Series,
    ) -> Dict[str, float]:
        """Monthly turnover history."""
        if not trade_history:
            return {}
        df = pd.DataFrame(trade_history)
        if df.empty:
            return {}

        df["fill_date"] = pd.to_datetime(df["fill_date"])
        df["month"] = df["fill_date"].dt.to_period("M").astype(str)

        avg_nav = nav_series.mean()
        if avg_nav <= 0:
            return {}

        monthly = df.groupby("month")["notional"].sum() / (2 * avg_nav)
        return {str(k): round(float(v), 4) for k, v in monthly.items()}

    def _sample_monthly(self, dates: List[str]) -> List[str]:
        """Sample approximately monthly dates from a list."""
        if len(dates) <= 12:
            return dates
        result = []
        seen_months = set()
        for d in sorted(dates):
            m = d[:7]
            if m not in seen_months:
                seen_months.add(m)
                result.append(d)
        return result

    def _build_narration_hints(
        self,
        metrics: Dict[str, Any],
        raw_bundle: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build narrative hints for LLM interpretation.

        The LLM uses these facts to generate text commentary.
        It does NOT recompute any values.
        """
        hints: Dict[str, Any] = {}

        # Performance classification
        cagr = metrics.get("cagr", 0.0) or 0.0
        sharpe = metrics.get("sharpe", 0.0) or 0.0
        max_dd = metrics.get("max_drawdown", 0.0) or 0.0
        excess = metrics.get("excess_return", None)
        te = metrics.get("tracking_error", None)
        ir = metrics.get("information_ratio", None)

        hints["performance_tier"] = (
            "strong" if cagr > 0.20 else
            "good" if cagr > 0.10 else
            "moderate" if cagr > 0.05 else
            "weak"
        )

        hints["risk_tier"] = (
            "high_risk" if abs(max_dd) > 0.30 else
            "moderate_risk" if abs(max_dd) > 0.15 else
            "low_risk"
        )

        if excess is not None:
            hints["benchmark_relative"] = (
                "outperformed" if excess > 0.02 else
                "roughly_matched" if abs(excess) <= 0.02 else
                "underperformed"
            )

        if ir is not None:
            hints["ir_quality"] = (
                "excellent" if ir > 0.5 else
                "good" if ir > 0.3 else
                "marginal" if ir > 0.0 else
                "negative"
            )

        violations = raw_bundle.get("constraint_violations", [])
        hints["constraint_violation_count"] = len(violations)
        if violations:
            hints["notable_violations"] = violations[:3]

        return hints
