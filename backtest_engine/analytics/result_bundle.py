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

        # Compute period length for notes
        dr = raw_bundle.get("date_range", {})
        nav_len = len(nav_series)
        n_years = nav_len / self._metrics_engine.af
        benchmark_index = raw_bundle.get("benchmark_index", "")

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
            "benchmark_index": benchmark_index,

            # Core performance data
            "summary_metrics": self._format_summary_metrics(
                metrics, benchmark=benchmark_index, n_years=n_years
            ),

            # Time series (truncated for readability)
            "nav_series": {k: round(v, 2) for k, v in nav_series.items()},
            "benchmark_nav_series": {k: round(v, 2) for k, v in bm_nav_series.items()},
            "drawdown_series": metrics.pop("drawdown_series", {}),

            # Tables
            "monthly_returns_table": monthly_ret_table,
            "top_holdings": top_holdings,
            "top_trades": top_trades,
            "sector_exposure_history": sector_exposure,
            "turnover_history": {
                "_meta": "월별 편도 회전율 (매수+매도) / (2 × 평균NAV). 키 형식: YYYY-MM",
                "data": turnover_history,
            },

            # Attribution
            "sleeve_attribution": sleeve_attr,

            # Risk / constraints
            "constraint_violations": raw_bundle.get("constraint_violations", []),

            # Narration hints (populated by LLM, engine provides the data)
            "narration_hints": self._build_narration_hints(metrics, raw_bundle),
        }

        return report

    # Descriptions shown alongside each metric key in the JSON output
    _METRIC_NOTES: Dict[str, str] = {
        "total_return":             "전체 백테스트 기간 누적 수익률",
        "cagr":                     "연환산 복리 수익률 (CAGR), 기간={period}",
        "annualized_vol":           "연환산 변동성 — 일간 수익률 표준편차 × √{af}",
        "sharpe":                   "샤프 비율 — (연환산 초과수익) / 연환산 변동성, 무위험수익률=0%",
        "sortino":                  "소르티노 비율 — (연환산 초과수익) / 하방 표준편차, 무위험수익률=0%",
        "calmar":                   "칼마 비율 — CAGR / |MDD|",
        "max_drawdown":             "최대 낙폭(MDD) — 고점 대비 최대 손실률",
        "max_dd_duration_days":     "MDD 지속 거래일 수 (고점 → 저점까지 거래일 수)",
        "win_rate":                 "일별 승률 — 상승 거래일 수 / 전체 거래일 수",
        "tracking_error":           "추적오차(TE) — 전략 vs {benchmark} 초과수익의 연환산 표준편차",
        "information_ratio":        "정보 비율(IR) — 연환산 초과수익 / TE, vs {benchmark}",
        "beta":                     "베타 — {benchmark} 대비 시장 민감도",
        "benchmark_total_return":   "벤치마크({benchmark}) 동기간 누적 수익률 (전략과 동일 기간)",
        "benchmark_cagr":           "벤치마크({benchmark}) 동기간 CAGR",
        "excess_return":            "초과 수익률 — 전략 누적 수익률 − {benchmark} 누적 수익률 (동기간)",
        "excess_cagr":              "초과 CAGR — 전략 CAGR − {benchmark} CAGR (동기간)",
        "average_monthly_turnover": "평균 월간 회전율 (편도) — 전체 기간 월평균, (매수 + 매도) / (2 × 평균 NAV) / 월수",
        "average_turnover":         "평균 월간 회전율 (편도) — average_monthly_turnover와 동일",
        "n_trading_days":           "백테스트 기간 총 거래일 수",
    }

    def _format_summary_metrics(self, metrics: Dict[str, Any], benchmark: str = "", n_years: float = 0.0) -> Dict[str, Any]:
        """Format summary metrics for human readability, with computation notes."""
        fmt = {}
        pct_keys = {
            "total_return", "cagr", "annualized_vol", "max_drawdown",
            "win_rate", "tracking_error", "benchmark_total_return",
            "benchmark_cagr", "excess_return", "excess_cagr",
            "average_turnover", "average_monthly_turnover",
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

        # Attach human-readable notes for each metric
        af = self._metrics_engine.af
        period_str = f"{n_years:.1f}년" if n_years > 0 else "전체 기간"
        notes: Dict[str, str] = {}
        for k, template in self._METRIC_NOTES.items():
            if k in fmt:
                notes[k] = (
                    template
                    .replace("{benchmark}", benchmark or "벤치마크")
                    .replace("{af}", str(af))
                    .replace("{period}", period_str)
                )
        fmt["_notes"] = notes
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
        return {str(year): {str(m): v for m, v in monthly.items()} for year, monthly in table.items()}

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
