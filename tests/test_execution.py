"""Tests for execution simulator cash/shares consistency."""

import pytest
import pandas as pd
import numpy as np

from backtest_engine.execution.research_profile import execute_rebalance_research
from backtest_engine.strategy_ir.models import ExecutionConfig, FillRule


def make_config(**overrides) -> ExecutionConfig:
    cfg = ExecutionConfig(
        fill_rule=FillRule.NEXT_OPEN,
        commission_bps=10.0,
        sell_tax_bps=20.0,
        slippage_bps=10.0,
        initial_capital=1_000_000_000,
        round_lot=1,
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


class TestResearchExecution:
    def test_initial_buy(self):
        """Buying into an empty portfolio should allocate cash correctly."""
        target_weights = pd.Series({"A": 0.5, "B": 0.3, "C": 0.2})
        prices = pd.Series({"A": 10000.0, "B": 5000.0, "C": 20000.0})
        config = make_config()

        result = execute_rebalance_research(
            signal_date="2025-01-02",
            fill_date="2025-01-03",
            target_weights=target_weights,
            current_holdings={},
            current_cash=1_000_000_000,
            prices=prices,
            config=config,
        )

        # After buying: new_cash should be much less than initial
        assert result.new_cash >= 0, "Cash should not go negative"
        assert len(result.new_holdings) >= 2, "Should hold multiple stocks"
        assert all(result.new_holdings.get(t, 0) > 0 for t in ["A", "B"])

    def test_nav_conservation(self):
        """NAV before ≈ NAV after + transaction costs."""
        target_weights = pd.Series({"A": 0.6, "B": 0.4})
        prices = pd.Series({"A": 50000.0, "B": 30000.0})
        config = make_config()

        result = execute_rebalance_research(
            signal_date="2025-01-02",
            fill_date="2025-01-03",
            target_weights=target_weights,
            current_holdings={},
            current_cash=1_000_000_000,
            prices=prices,
            config=config,
        )

        nav_after = result.new_cash + sum(
            result.new_holdings.get(t, 0) * prices.get(t, 0)
            for t in result.new_holdings
        )
        nav_before = result.nav_before
        costs = result.total_costs
        # NAV after = NAV before - costs (approximately)
        assert abs((nav_before - costs) - nav_after) / nav_before < 0.01, \
            f"NAV not conserved: before={nav_before:,.0f}, after={nav_after:,.0f}, costs={costs:,.0f}"

    def test_sell_reduces_holdings(self):
        """Selling a position should reduce shares."""
        initial_holdings = {"A": 1000.0, "B": 500.0}
        prices = pd.Series({"A": 50000.0, "B": 30000.0})
        initial_nav = 1000 * 50000 + 500 * 30000  # 65,000,000
        target_weights = pd.Series({"A": 1.0})  # sell B

        config = make_config()
        result = execute_rebalance_research(
            signal_date="2025-01-02",
            fill_date="2025-01-03",
            target_weights=target_weights,
            current_holdings=initial_holdings,
            current_cash=0,
            prices=prices,
            config=config,
        )

        assert result.new_holdings.get("B", 0) == 0, "B should be fully sold"
        assert result.new_holdings.get("A", 0) > 0, "A should still be held"

    def test_no_trade_if_zero_weights(self):
        """Empty target weights should liquidate everything."""
        initial_holdings = {"A": 100.0}
        prices = pd.Series({"A": 10000.0})
        config = make_config()

        result = execute_rebalance_research(
            signal_date="2025-01-02",
            fill_date="2025-01-03",
            target_weights=pd.Series(dtype=float),
            current_holdings=initial_holdings,
            current_cash=0,
            prices=prices,
            config=config,
        )

        assert result.new_holdings.get("A", 0) == 0 or len(result.new_holdings) == 0
        assert result.new_cash > 0

    def test_round_lot_respected(self):
        """Shares purchased should be in integer lots."""
        target_weights = pd.Series({"A": 1.0})
        prices = pd.Series({"A": 33333.0})  # odd price to test rounding
        config = make_config()

        result = execute_rebalance_research(
            signal_date="2025-01-02",
            fill_date="2025-01-03",
            target_weights=target_weights,
            current_holdings={},
            current_cash=1_000_000_000,
            prices=prices,
            config=config,
            round_lot=1,
        )

        shares = result.new_holdings.get("A", 0)
        assert shares == round(shares), f"Shares should be integer: {shares}"

    def test_trade_costs_are_positive(self):
        """Transaction costs should be non-negative."""
        target_weights = pd.Series({"A": 0.5, "B": 0.5})
        prices = pd.Series({"A": 10000.0, "B": 20000.0})
        config = make_config()

        result = execute_rebalance_research(
            signal_date="2025-01-02",
            fill_date="2025-01-03",
            target_weights=target_weights,
            current_holdings={},
            current_cash=1_000_000_000,
            prices=prices,
            config=config,
        )

        for t in result.trades:
            assert t.total_cost >= 0
            assert t.commission >= 0
            assert t.slippage >= 0
