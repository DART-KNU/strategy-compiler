"""
Research execution profile — deterministic, flat-cost simulation.

Fill rules:
- next_open: execute at next trading day's open price
- next_close: execute at next trading day's close price
- same_close: execute at current day's close (look-ahead warning issued)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd
import numpy as np

from backtest_engine.strategy_ir.models import ExecutionConfig, FillRule


@dataclass
class TradeRecord:
    """Single trade record."""
    trade_date: str       # date of signal / rebalance decision
    fill_date: str        # date of actual execution
    ticker: str
    direction: str        # "buy" | "sell"
    shares: float
    fill_price: float
    notional: float       # shares * fill_price (always positive)
    commission: float     # in KRW
    sell_tax: float       # in KRW (only for sells)
    slippage: float       # in KRW
    total_cost: float     # commission + sell_tax + slippage
    net_notional: float   # notional +/- total_cost (what actually moves cash)


@dataclass
class RebalanceResult:
    """Result of a single rebalance execution."""
    signal_date: str
    fill_date: str
    trades: list[TradeRecord]
    prev_holdings: Dict[str, float]  # ticker -> shares
    new_holdings: Dict[str, float]   # ticker -> shares
    prev_cash: float
    new_cash: float
    total_buys: float
    total_sells: float
    total_costs: float
    nav_before: float
    nav_after: float


def execute_rebalance_research(
    signal_date: str,
    fill_date: str,
    target_weights: pd.Series,       # ticker -> target weight
    current_holdings: Dict[str, float],  # ticker -> shares
    current_cash: float,
    prices: pd.Series,               # ticker -> fill price (open or close)
    config: ExecutionConfig,
    round_lot: int = 1,
) -> RebalanceResult:
    """
    Execute a rebalance in research mode.

    Steps:
    1. Compute current portfolio value (NAV)
    2. Compute target notional per ticker
    3. Compute required trades (shares to buy/sell)
    4. Apply costs and compute new cash position
    5. Return RebalanceResult

    Parameters
    ----------
    signal_date : str
        Date the rebalance signal was generated.
    fill_date : str
        Date of actual execution.
    target_weights : pd.Series
        Desired weights (ticker -> weight, summing to ~1.0).
    current_holdings : dict
        Current position in shares (ticker -> shares).
    current_cash : float
        Available cash in KRW.
    prices : pd.Series
        Fill prices (ticker -> price in KRW).
    config : ExecutionConfig
    round_lot : int
        Minimum lot size (default 1 share).
    """
    # --- Step 1: Compute current NAV ---
    nav = current_cash
    for ticker, shares in current_holdings.items():
        price = prices.get(ticker, np.nan)
        if not np.isnan(price) and price > 0:
            nav += shares * price

    if nav <= 0:
        nav = config.initial_capital

    # --- Step 2: Compute target notionals ---
    target_notionals: Dict[str, float] = {}
    for ticker, weight in target_weights.items():
        price = prices.get(ticker, np.nan)
        if np.isnan(price) or price <= 0:
            continue
        target_notionals[ticker] = nav * weight

    # --- Step 3: Compute target shares ---
    target_shares: Dict[str, float] = {}
    for ticker, notional in target_notionals.items():
        price = prices.get(ticker, 0.0)
        if price <= 0:
            continue
        raw_shares = notional / price
        # Round to lot size
        target_shares[ticker] = max(0.0, round(raw_shares / round_lot) * round_lot)

    # All tickers involved (current + target)
    all_tickers = set(current_holdings.keys()) | set(target_shares.keys())

    # --- Step 4: Generate trades ---
    trades: list[TradeRecord] = []
    new_holdings = dict(current_holdings)  # start from current
    cash = current_cash

    # First: execute sells (to free cash)
    for ticker in all_tickers:
        cur_shares = current_holdings.get(ticker, 0.0)
        tgt_shares = target_shares.get(ticker, 0.0)
        diff = tgt_shares - cur_shares

        if diff < -0.5:  # selling
            sell_shares = abs(diff)
            price = prices.get(ticker, np.nan)
            if np.isnan(price) or price <= 0:
                continue

            notional = sell_shares * price
            commission = notional * config.commission_bps / 10000
            sell_tax = notional * config.sell_tax_bps / 10000
            slippage = notional * config.slippage_bps / 10000
            total_cost = commission + sell_tax + slippage
            net_proceeds = notional - total_cost

            trades.append(TradeRecord(
                trade_date=signal_date,
                fill_date=fill_date,
                ticker=ticker,
                direction="sell",
                shares=sell_shares,
                fill_price=price,
                notional=notional,
                commission=commission,
                sell_tax=sell_tax,
                slippage=slippage,
                total_cost=total_cost,
                net_notional=net_proceeds,
            ))

            new_holdings[ticker] = max(0.0, cur_shares - sell_shares)
            if new_holdings[ticker] < round_lot:
                new_holdings[ticker] = 0.0
            cash += net_proceeds

    # Second: execute buys
    for ticker in all_tickers:
        cur_shares = new_holdings.get(ticker, 0.0)
        tgt_shares = target_shares.get(ticker, 0.0)
        diff = tgt_shares - cur_shares

        if diff > 0.5:  # buying
            price = prices.get(ticker, np.nan)
            if np.isnan(price) or price <= 0:
                continue

            # Check if we have enough cash
            buy_notional = diff * price
            commission = buy_notional * config.commission_bps / 10000
            slippage = buy_notional * config.slippage_bps / 10000
            total_cost = commission + slippage
            total_needed = buy_notional + total_cost

            if total_needed > cash:
                # Scale back the buy (floor to avoid overspending)
                import math
                cost_rate = (config.commission_bps + config.slippage_bps) / 10000
                affordable = cash / (price * (1 + cost_rate))
                diff = max(0.0, math.floor(affordable / round_lot) * round_lot)
                if diff < 0.5:
                    continue
                buy_notional = diff * price
                commission = buy_notional * config.commission_bps / 10000
                slippage = buy_notional * config.slippage_bps / 10000
                total_cost = commission + slippage
                total_needed = buy_notional + total_cost

            trades.append(TradeRecord(
                trade_date=signal_date,
                fill_date=fill_date,
                ticker=ticker,
                direction="buy",
                shares=diff,
                fill_price=price,
                notional=buy_notional,
                commission=commission,
                sell_tax=0.0,
                slippage=slippage,
                total_cost=total_cost,
                net_notional=buy_notional + total_cost,
            ))

            new_holdings[ticker] = cur_shares + diff
            cash -= total_needed

    # Remove zero positions
    new_holdings = {k: v for k, v in new_holdings.items() if v >= round_lot}

    total_buys = sum(t.notional for t in trades if t.direction == "buy")
    total_sells = sum(t.notional for t in trades if t.direction == "sell")
    total_costs = sum(t.total_cost for t in trades)

    nav_after = cash + sum(
        new_holdings.get(t, 0.0) * prices.get(t, 0.0)
        for t in new_holdings
    )

    return RebalanceResult(
        signal_date=signal_date,
        fill_date=fill_date,
        trades=trades,
        prev_holdings=current_holdings,
        new_holdings=new_holdings,
        prev_cash=current_cash,
        new_cash=cash,
        total_buys=total_buys,
        total_sells=total_sells,
        total_costs=total_costs,
        nav_before=nav,
        nav_after=nav_after,
    )


def get_fill_date_and_prices(
    signal_date: str,
    fill_rule: FillRule,
    snapshot_loader,
    calendar,
    universe_config,
) -> tuple[str, pd.Series]:
    """
    Determine fill_date and prices based on fill_rule.

    Returns (fill_date, prices_series).
    """
    if fill_rule == FillRule.SAME_CLOSE:
        fill_date = signal_date
        snap = snapshot_loader.load_snapshot(signal_date, universe_config)
        prices = snap["close"] if "close" in snap.columns else snap.get("adj_close", pd.Series(dtype=float))

    elif fill_rule == FillRule.NEXT_OPEN:
        fill_date = calendar.next_trading_day(signal_date, 1) or signal_date
        snap = snapshot_loader.load_snapshot(fill_date, universe_config, ["open"])
        prices = snap["open"] if "open" in snap.columns else snap.get("adj_close", pd.Series(dtype=float))

    elif fill_rule == FillRule.NEXT_CLOSE:
        fill_date = calendar.next_trading_day(signal_date, 1) or signal_date
        snap = snapshot_loader.load_snapshot(fill_date, universe_config)
        prices = snap["close"] if "close" in snap.columns else snap.get("adj_close", pd.Series(dtype=float))

    else:
        fill_date = signal_date
        snap = snapshot_loader.load_snapshot(signal_date, universe_config)
        prices = snap.get("adj_close", pd.Series(dtype=float))

    return fill_date, prices
