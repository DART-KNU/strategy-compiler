"""
Contest execution profile — approximate model for the competition's execution rules.

Key differences from research profile:
- Market impact proxy: large orders relative to ADV receive extra slippage
- Liquidity participation cap: max 10% of ADV5 per trade
- Aggressive order penalty for large positions
- Turnover rule monitor (min 5% weekly)

Note: Exact order book simulation is not possible without tick-level data.
This is a deliberate approximation documented in design_decisions.md.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from backtest_engine.execution.research_profile import (
    TradeRecord,
    RebalanceResult,
    execute_rebalance_research,
)
from backtest_engine.strategy_ir.models import ExecutionConfig, ConstraintSet


@dataclass
class ContestExecutionConfig:
    """Contest-specific execution parameters."""
    commission_bps: float = 10.0
    sell_tax_bps: float = 20.0
    base_slippage_bps: float = 10.0
    market_impact_bps_per_adv_pct: float = 5.0   # extra bps per 1% of ADV
    max_adv_participation: float = 0.10           # max 10% of ADV5 per trade
    aggressive_penalty_bps: float = 15.0          # for orders > 5% ADV
    min_weekly_turnover: float = 0.05             # 5% minimum weekly
    round_lot: int = 1


def execute_rebalance_contest(
    signal_date: str,
    fill_date: str,
    target_weights: pd.Series,
    current_holdings: Dict[str, float],
    current_cash: float,
    prices: pd.Series,
    adv5: pd.Series,
    config: ExecutionConfig,
    contest_config: Optional[ContestExecutionConfig] = None,
    round_lot: int = 1,
) -> RebalanceResult:
    """
    Execute a rebalance in contest mode with market impact approximation.

    Differences from research:
    1. Market impact scales with order size / ADV5
    2. Orders capped at max_adv_participation * ADV5
    3. Large orders receive aggressive_penalty
    """
    cc = contest_config or ContestExecutionConfig()

    # Compute NAV
    nav = current_cash
    for ticker, shares in current_holdings.items():
        price = prices.get(ticker, np.nan)
        if not np.isnan(price) and price > 0:
            nav += shares * price

    if nav <= 0:
        nav = config.initial_capital

    # Compute target shares
    target_shares: Dict[str, float] = {}
    for ticker, weight in target_weights.items():
        price = prices.get(ticker, np.nan)
        if np.isnan(price) or price <= 0:
            continue
        notional = nav * weight
        raw_shares = notional / price
        # Cap by ADV participation
        adv = adv5.get(ticker, np.nan)
        if not np.isnan(adv) and adv > 0:
            max_notional = adv * cc.max_adv_participation
            if notional > max_notional:
                notional = max_notional
                raw_shares = notional / price
        target_shares[ticker] = max(0.0, round(raw_shares / round_lot) * round_lot)

    # Execute using research profile as base, then adjust costs for market impact
    research_result = execute_rebalance_research(
        signal_date=signal_date,
        fill_date=fill_date,
        target_weights=target_weights,
        current_holdings=current_holdings,
        current_cash=current_cash,
        prices=prices,
        config=config,
        round_lot=round_lot,
    )

    # Augment slippage for large orders
    augmented_trades = []
    for trade in research_result.trades:
        adv = adv5.get(trade.ticker, np.nan)
        extra_slippage = 0.0

        if not np.isnan(adv) and adv > 0:
            adv_fraction = trade.notional / adv
            if adv_fraction > 0.01:
                # Market impact proxy
                extra_bps = adv_fraction * cc.market_impact_bps_per_adv_pct * 100
                extra_slippage = trade.notional * extra_bps / 10000

            if adv_fraction > 0.05:
                # Aggressive order penalty
                extra_slippage += trade.notional * cc.aggressive_penalty_bps / 10000

        # Create augmented record
        new_total_cost = trade.total_cost + extra_slippage
        net_adj = trade.net_notional - extra_slippage if trade.direction == "sell" else trade.net_notional + extra_slippage

        augmented_trades.append(TradeRecord(
            trade_date=trade.trade_date,
            fill_date=trade.fill_date,
            ticker=trade.ticker,
            direction=trade.direction,
            shares=trade.shares,
            fill_price=trade.fill_price,
            notional=trade.notional,
            commission=trade.commission,
            sell_tax=trade.sell_tax,
            slippage=trade.slippage + extra_slippage,
            total_cost=new_total_cost,
            net_notional=net_adj,
        ))

    total_costs = sum(t.total_cost for t in augmented_trades)

    return RebalanceResult(
        signal_date=research_result.signal_date,
        fill_date=research_result.fill_date,
        trades=augmented_trades,
        prev_holdings=research_result.prev_holdings,
        new_holdings=research_result.new_holdings,
        prev_cash=research_result.prev_cash,
        new_cash=research_result.new_cash,
        total_buys=research_result.total_buys,
        total_sells=research_result.total_sells,
        total_costs=total_costs,
        nav_before=research_result.nav_before,
        nav_after=research_result.nav_after,
    )


class TurnoverMonitor:
    """Monitors weekly portfolio turnover and flags violations."""

    def __init__(self, min_weekly_turnover: float = 0.05):
        self.min_weekly = min_weekly_turnover
        self._weekly_buys: Dict[str, float] = {}
        self._weekly_sells: Dict[str, float] = {}
        self._weekly_nav: Dict[str, float] = {}

    def record(self, week_id: str, buy_amount: float, sell_amount: float, nav: float) -> None:
        self._weekly_buys[week_id] = self._weekly_buys.get(week_id, 0.0) + buy_amount
        self._weekly_sells[week_id] = self._weekly_sells.get(week_id, 0.0) + sell_amount
        self._weekly_nav[week_id] = max(self._weekly_nav.get(week_id, 0.0), nav)

    def get_weekly_turnover(self, week_id: str) -> float:
        buys = self._weekly_buys.get(week_id, 0.0)
        sells = self._weekly_sells.get(week_id, 0.0)
        nav = self._weekly_nav.get(week_id, 1.0)
        return (buys + sells) / (2 * max(nav, 1.0))

    def check_violations(self) -> List[Tuple[str, float]]:
        """Return list of (week_id, turnover) for weeks below minimum."""
        violations = []
        for week_id in sorted(self._weekly_buys.keys()):
            to = self.get_weekly_turnover(week_id)
            if to < self.min_weekly:
                violations.append((week_id, to))
        return violations

    def summary(self) -> Dict[str, float]:
        return {
            week_id: self.get_weekly_turnover(week_id)
            for week_id in self._weekly_buys
        }
