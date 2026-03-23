"""Tests for contest-mode constraint checks."""

import pandas as pd
import pytest

from backtest_engine.portfolio.constraints import apply_constraints
from backtest_engine.execution.contest_profile import TurnoverMonitor
from backtest_engine.strategy_ir.models import ConstraintSet, RunMode


class TestContestConstraints:
    def _make_snap(self, tickers, sectors, mcaps):
        return pd.DataFrame({
            "sector_name": sectors,
            "market_cap": mcaps,
        }, index=tickers)

    def test_samsung_cap_respected(self):
        """005930 (Samsung) should be capped at 40% in contest mode.
        Uses enough stocks to make the constraint feasible (sum<=1.0 with caps).
        """
        # 005930=40%, 7 others=8.57% each (total ~100%), max_weight=15% for others
        # feasible: 0.40 + 7*0.15 = 1.45 >= 1.0
        others = [f"{i:06d}" for i in range(1, 8)]
        tickers = ["005930"] + others
        sectors = ["IT"] * 4 + ["Finance"] * 4
        mcaps = [4e13] + [1e12] * 7
        snap = self._make_snap(tickers, sectors, mcaps)
        w_vals = [0.50] + [0.50 / 7] * 7
        weights = pd.Series(dict(zip(tickers, w_vals)))
        constraints = ConstraintSet(max_weight=0.15, contest_samsung_cap=0.40)
        adjusted, _ = apply_constraints(weights, snap, constraints, mode=RunMode.CONTEST)
        assert adjusted["005930"] <= 0.40 + 1e-6, \
            f"Samsung weight {adjusted['005930']:.2%} exceeds 40% cap"

    def test_normal_stock_cap(self):
        """Non-Samsung stocks should be capped at 15% in contest mode.
        Uses enough stocks to make the constraint feasible.
        """
        # 7 stocks, each at 15% cap → max 105% weight → feasible
        tickers = [f"{i:06d}" for i in range(1, 8)]
        snap = self._make_snap(tickers, ["IT"] * 7, [1e12] * 7)
        w_vals = [1.0 / 7] * 7
        weights = pd.Series(dict(zip(tickers, w_vals)))
        # Now manually inflate one stock
        weights[tickers[0]] = 0.50
        remaining = (1.0 - 0.50) / 6
        for t in tickers[1:]:
            weights[t] = remaining
        constraints = ConstraintSet(max_weight=0.15, contest_samsung_cap=0.40)
        adjusted, viols = apply_constraints(weights, snap, constraints, mode=RunMode.CONTEST)
        for t in tickers:
            assert adjusted[t] <= 0.15 + 1e-6, \
                f"Stock {t} weight {adjusted[t]:.2%} exceeds 15% cap"

    def test_small_mcap_aggregate_cap(self):
        """Aggregate weight in small-cap stocks should not exceed 30%."""
        tickers = ["A", "B", "C", "D"]
        snap = self._make_snap(
            tickers,
            sectors=["IT"] * 4,
            mcaps=[5e11, 5e11, 1e13, 2e13]  # A,B are small (< 1T), C,D are large
        )
        weights = pd.Series({"A": 0.20, "B": 0.20, "C": 0.30, "D": 0.30})
        constraints = ConstraintSet(
            max_weight=0.40,
            small_mcap_threshold_bn=1000.0,
            max_small_mcap_weight=0.30,
        )
        adjusted, viols = apply_constraints(weights, snap, constraints, mode=RunMode.CONTEST)
        small_total = adjusted[["A", "B"]].sum()
        assert small_total <= 0.30 + 0.01, \
            f"Small-cap aggregate {small_total:.2%} exceeds 30% cap"


class TestTurnoverMonitor:
    def test_week_below_minimum_flagged(self):
        monitor = TurnoverMonitor(min_weekly_turnover=0.05)
        nav = 1_000_000_000
        # Record a week with only 1% turnover
        monitor.record("2024-W01", buy_amount=nav * 0.005, sell_amount=nav * 0.005, nav=nav)
        violations = monitor.check_violations()
        assert len(violations) == 1
        assert violations[0][0] == "2024-W01"

    def test_week_above_minimum_not_flagged(self):
        monitor = TurnoverMonitor(min_weekly_turnover=0.05)
        nav = 1_000_000_000
        # Record a week with 10% turnover
        monitor.record("2024-W01", buy_amount=nav * 0.05, sell_amount=nav * 0.05, nav=nav)
        violations = monitor.check_violations()
        assert len(violations) == 0

    def test_weekly_turnover_calculation(self):
        monitor = TurnoverMonitor()
        nav = 1_000_000_000
        monitor.record("2024-W01", 50_000_000, 50_000_000, nav)
        to = monitor.get_weekly_turnover("2024-W01")
        # (50M + 50M) / (2 * 1B) = 5%
        assert abs(to - 0.05) < 0.001
