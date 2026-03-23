"""Tests for metric calculations sanity."""

import numpy as np
import pandas as pd
import pytest

from backtest_engine.analytics.metrics import MetricsEngine


def make_nav_series(n: int = 252, annual_return: float = 0.10) -> pd.Series:
    """Generate a simple NAV series with known return."""
    daily_ret = (1 + annual_return) ** (1 / 252) - 1
    dates = pd.date_range("2024-01-02", periods=n, freq="B")
    dates_str = [d.strftime("%Y-%m-%d") for d in dates]
    nav = 1_000_000_000 * np.cumprod(1 + np.full(n, daily_ret))
    return pd.Series(nav, index=dates_str)


def make_bundle(nav: pd.Series, bm_nav: pd.Series = None) -> dict:
    return {
        "nav_series": nav.to_dict(),
        "benchmark_nav_series": (bm_nav.to_dict() if bm_nav is not None else {}),
        "initial_capital": 1_000_000_000,
        "trade_history": [],
    }


class TestMetricsEngine:
    def test_total_return_approx(self):
        nav = make_nav_series(252, annual_return=0.10)
        engine = MetricsEngine()
        bundle = make_bundle(nav)
        metrics = engine.compute_all(bundle)
        total_ret = metrics["total_return"]
        # 10% annual return for 1 year ≈ 10% total
        assert abs(total_ret - 0.10) < 0.01, f"Expected ~10%, got {total_ret:.2%}"

    def test_cagr_equals_annual_return(self):
        nav = make_nav_series(252, annual_return=0.15)
        engine = MetricsEngine()
        metrics = engine.compute_all(make_bundle(nav))
        assert abs(metrics["cagr"] - 0.15) < 0.02

    def test_max_drawdown_flat_market(self):
        """Zero-return portfolio has zero max drawdown."""
        nav = pd.Series(1_000_000_000.0, index=[f"2024-{d:02d}-01" for d in range(1, 13)])
        engine = MetricsEngine()
        metrics = engine.compute_all(make_bundle(nav))
        assert abs(metrics["max_drawdown"]) < 1e-8

    def test_max_drawdown_known_crash(self):
        """50% crash followed by recovery."""
        nav = pd.Series(
            [100.0, 100.0, 50.0, 50.0, 100.0],
            index=["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
        ) * 1_000_000
        engine = MetricsEngine()
        metrics = engine.compute_all(make_bundle(nav))
        assert abs(metrics["max_drawdown"] - (-0.5)) < 0.01, \
            f"Expected -50% drawdown, got {metrics['max_drawdown']:.2%}"

    def test_sharpe_positive_return(self):
        nav = make_nav_series(252, annual_return=0.20)
        engine = MetricsEngine()
        metrics = engine.compute_all(make_bundle(nav))
        assert metrics["sharpe"] > 0, "Positive return should give positive Sharpe"

    def test_win_rate_range(self):
        nav = make_nav_series(252, annual_return=0.10)
        engine = MetricsEngine()
        metrics = engine.compute_all(make_bundle(nav))
        wr = metrics["win_rate"]
        assert 0 <= wr <= 1

    def test_tracking_error_same_portfolio(self):
        """TE of identical portfolio vs benchmark should be ~0."""
        nav = make_nav_series(252, annual_return=0.10)
        engine = MetricsEngine()
        metrics = engine.compute_all(make_bundle(nav, bm_nav=nav.copy()))
        if "tracking_error" in metrics:
            assert metrics["tracking_error"] < 0.01

    def test_monthly_returns_table(self):
        nav = make_nav_series(252, annual_return=0.10)
        engine = MetricsEngine()
        metrics = engine.compute_all(make_bundle(nav))
        monthly = metrics["monthly_returns"]
        assert len(monthly) > 0
        assert all("year" in row and "month" in row and "return" in row for row in monthly)

    def test_information_ratio_with_alpha(self):
        """Portfolio with consistent alpha should have positive IR."""
        nav = make_nav_series(252, annual_return=0.20)
        bm = make_nav_series(252, annual_return=0.10)
        engine = MetricsEngine()
        metrics = engine.compute_all(make_bundle(nav, bm_nav=bm))
        if "information_ratio" in metrics:
            assert metrics["information_ratio"] > 0
