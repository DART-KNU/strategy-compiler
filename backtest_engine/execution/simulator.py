"""
ExecutionSimulator — the main backtest loop.

Orchestrates:
1. Calendar generation
2. For each rebalance date: snapshot -> signal -> selection -> allocation -> execution
3. On non-rebalance dates: mark-to-market only
4. Collects NAV, holdings, trades, turnover

Returns a raw result bundle (dict) used by analytics.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from backtest_engine.data.calendar import CalendarProvider
from backtest_engine.data.loaders import (
    SnapshotLoader,
    IndexHistoryLoader,
    PriceHistoryLoader,
    CovarianceLoader,
)
from backtest_engine.execution.research_profile import (
    execute_rebalance_research,
    get_fill_date_and_prices,
    RebalanceResult,
)
from backtest_engine.execution.contest_profile import (
    execute_rebalance_contest,
    ContestExecutionConfig,
    TurnoverMonitor,
)
from backtest_engine.graph.node_executor import NodeGraphExecutor
from backtest_engine.portfolio.allocators import allocate
from backtest_engine.portfolio.constraints import apply_constraints
from backtest_engine.portfolio.selector import select_universe, apply_max_names_filter
from backtest_engine.portfolio.sleeve_mixer import SleeveMixer
from backtest_engine.strategy_ir.models import (
    StrategyIR,
    SleeveConfig,
    RunMode,
    FillRule,
    SleeveMixMethod,
)


def _current_weights(
    holdings: Dict[str, float],
    cash: float,
    prices: Dict[str, float],
) -> pd.Series:
    """Compute current portfolio weight vector from holdings."""
    values: Dict[str, float] = {}
    nav = cash
    for ticker, shares in holdings.items():
        price = prices.get(ticker, 0.0)
        if price > 0:
            v = shares * price
            values[ticker] = v
            nav += v
    if nav <= 0 or not values:
        return pd.Series(dtype=float)
    return pd.Series({t: v / nav for t, v in values.items()})


def _partial_target(
    current_weights: pd.Series,
    target_weights: pd.Series,
    max_turnover: Optional[float] = None,
    min_turnover: Optional[float] = None,
) -> pd.Series:
    """
    Return an intermediate target that moves from current toward target,
    constrained by [min_turnover, max_turnover] per step (one-way).

    Rules:
    - step ≤ max_turnover          (cap: prevents over-trading)
    - step ≥ min(needed, min_turnover)  (floor: enforces minimum when gap allows)
    - step ≤ needed                (never overshoot the target)

    If neither bound is set, returns target unchanged.
    """
    all_tickers = current_weights.index.union(target_weights.index)
    cur = current_weights.reindex(all_tickers).fillna(0.0)
    tgt = target_weights.reindex(all_tickers).fillna(0.0)
    delta = tgt - cur
    needed = delta.abs().sum() / 2  # one-way turnover

    if needed <= 1e-9:
        return tgt  # already at target

    step = needed  # default: close gap completely

    # Apply cap (max)
    if max_turnover is not None:
        step = min(step, max_turnover)

    # Apply floor (min), but never exceed remaining gap
    if min_turnover is not None:
        step = max(step, min(needed, min_turnover))

    if abs(step - needed) <= 1e-9:
        return tgt  # step covers full gap — return exact target

    scale = step / needed
    intermediate = (cur + delta * scale).clip(lower=0.0)
    total = intermediate.sum()
    if total > 1e-9:
        intermediate /= total
    return intermediate[intermediate > 1e-9]


class ExecutionSimulator:
    """
    Main backtest simulation engine.

    Usage:
        sim = ExecutionSimulator(conn, strategy_ir)
        result = sim.run()
    """

    def __init__(
        self,
        conn,
        strategy: StrategyIR,
        history_window: int = 252,
        verbose: bool = False,
    ):
        self._conn = conn
        self._strategy = strategy
        self._history_window = history_window
        self._verbose = verbose

        # Data providers
        self._calendar = CalendarProvider(conn)
        self._snapshot_loader = SnapshotLoader(conn)
        self._index_loader = IndexHistoryLoader(conn)
        self._price_loader = PriceHistoryLoader(conn)
        self._cov_loader = CovarianceLoader(conn)
        self._mixer = SleeveMixer()

        # Run state
        self._run_id = str(uuid.uuid4())[:8]

    def run(self) -> Dict[str, Any]:
        """
        Execute the full backtest. Returns a result bundle dict.

        The result bundle is consumed by MetricsEngine and ReportBundleBuilder.
        """
        strategy = self._strategy
        dr = strategy.effective_date_range()
        ex_cfg = strategy.effective_execution()

        # Validate date range against calendar
        start = max(dr.start, self._calendar.min_date)
        end = min(dr.end, self._calendar.max_date)
        trading_days = self._calendar.trading_days_in_range(start, end)
        if not trading_days:
            raise ValueError(f"No trading days in range [{start}, {end}]")

        # Compute signal dates (when to recompute target portfolio)
        reb_dates = self._calendar.get_rebalance_dates(start, end, strategy.rebalancing)
        signal_set = set(reb_dates)

        # Compute execution dates (when to actually trade)
        exec_cadence = strategy.rebalancing.execution_cadence
        if exec_cadence is not None:
            from copy import deepcopy
            exec_cal_cfg = deepcopy(strategy.rebalancing)
            exec_cal_cfg.frequency = exec_cadence
            exec_cal_cfg.look_ahead_buffer = 0  # no buffer on execution dates
            exec_dates = set(self._calendar.get_rebalance_dates(start, end, exec_cal_cfg))
        else:
            exec_dates = signal_set

        max_turnover_step = strategy.rebalancing.max_turnover_per_rebalance
        min_turnover_step = strategy.rebalancing.min_turnover_per_rebalance
        all_action_dates = signal_set | exec_dates

        if self._verbose:
            to_range = ""
            if min_turnover_step is not None or max_turnover_step is not None:
                lo = f"{min_turnover_step:.0%}" if min_turnover_step is not None else "0%"
                hi = f"{max_turnover_step:.0%}" if max_turnover_step is not None else "∞"
                to_range = f" [{lo}~{hi}/step]"
            print(f"[{self._run_id}] Running backtest: {start} -> {end}, "
                  f"{len(trading_days)} trading days, {len(reb_dates)} signal dates"
                  + (f", {len(exec_dates)} execution dates{to_range}"
                     if exec_cadence else f", {len(reb_dates)} rebalances"))

        # Initialize state
        initial_capital = ex_cfg.initial_capital
        holdings: Dict[str, float] = {}    # ticker -> shares
        cash = initial_capital

        # History collectors
        nav_series: Dict[str, float] = {}
        holdings_history: Dict[str, Dict[str, float]] = {}
        weights_history: Dict[str, pd.Series] = {}
        trade_history: List[Dict] = []
        constraint_violations: List[Dict] = []

        # Benchmark NAV (normalized to initial_capital)
        bm_index = strategy.benchmark.index_code
        bm_levels = self._index_loader.load_levels(bm_index, start, end)
        bm_nav_series: Dict[str, float] = {}
        bm_start_level = None

        # Contest: turnover monitor
        turnover_monitor: Optional[TurnoverMonitor] = None
        if strategy.mode == RunMode.CONTEST:
            turnover_monitor = TurnoverMonitor(min_weekly_turnover=0.05)

        # Dual-cadence state
        prev_reb_weights: Optional[pd.Series] = None
        pending_target_weights: Optional[pd.Series] = None  # latest signal target

        for trade_date in trading_days:
            # Mark to market
            prices_today = self._get_closing_prices(trade_date)
            nav = cash + sum(
                holdings.get(t, 0.0) * prices_today.get(t, 0.0)
                for t in holdings
            )
            nav_series[trade_date] = nav

            # Benchmark NAV
            bm_level = bm_levels.get(trade_date, np.nan)
            if not pd.isna(bm_level):
                if bm_start_level is None:
                    bm_start_level = bm_level
                bm_nav = initial_capital * (bm_level / bm_start_level)
                bm_nav_series[trade_date] = bm_nav

            # Record holdings
            holdings_history[trade_date] = dict(holdings)

            if trade_date not in all_action_dates:
                continue

            # Step 1: if this is a signal date, compute new target (no trades yet)
            if trade_date in signal_set:
                try:
                    new_target = self._compute_target_weights(
                        signal_date=trade_date,
                        holdings=holdings,
                        cash=cash,
                        prev_weights=prev_reb_weights,
                        prices_today=prices_today,
                        ex_cfg=ex_cfg,
                    )
                    if new_target is not None:
                        pending_target_weights = new_target
                        if self._verbose and exec_cadence is not None:
                            print(f"  [{trade_date}] New target computed ({len(new_target)} tickers)")
                except Exception as e:
                    if self._verbose:
                        print(f"  [WARN] Signal computation failed on {trade_date}: {e}")

            # Step 2: if this is an execution date, trade toward pending target
            if trade_date not in exec_dates:
                continue

            # Determine which target to use
            if exec_cadence is not None:
                # Dual-cadence: use pending target (computed above or from a prior signal date)
                if pending_target_weights is None:
                    continue
                effective_target = pending_target_weights
                if max_turnover_step is not None or min_turnover_step is not None:
                    current_weights = _current_weights(holdings, cash, prices_today)
                    effective_target = _partial_target(
                        current_weights, pending_target_weights,
                        max_turnover=max_turnover_step,
                        min_turnover=min_turnover_step,
                    )
            else:
                # Single-cadence: pending_target was just set above (signal_set == exec_dates)
                if pending_target_weights is None:
                    continue
                effective_target = pending_target_weights

            try:
                result = self._execute_with_target(
                    signal_date=trade_date,
                    target_weights=effective_target,
                    holdings=holdings,
                    cash=cash,
                    prices_today=prices_today,
                    ex_cfg=ex_cfg,
                )

                if result is not None:
                    holdings = result.new_holdings
                    cash = result.new_cash

                    # Record trades
                    for t in result.trades:
                        trade_history.append({
                            "signal_date": t.trade_date,
                            "fill_date": t.fill_date,
                            "ticker": t.ticker,
                            "direction": t.direction,
                            "shares": t.shares,
                            "fill_price": t.fill_price,
                            "notional": t.notional,
                            "total_cost": t.total_cost,
                        })

                    # Record weights
                    prices_fill = self._get_prices(result.fill_date)
                    nav_after = result.nav_after
                    if nav_after > 0:
                        cur_weights = pd.Series({
                            t: (s * prices_fill.get(t, 0.0)) / nav_after
                            for t, s in holdings.items()
                        })
                    else:
                        cur_weights = pd.Series(dtype=float)

                    weights_history[trade_date] = cur_weights
                    prev_reb_weights = cur_weights

                    # Update NAV after rebalance
                    nav_after_calc = result.new_cash + sum(
                        result.new_holdings.get(t, 0.0) * prices_today.get(t, 0.0)
                        for t in result.new_holdings
                    )
                    nav_series[trade_date] = nav_after_calc

                    # Contest turnover tracking
                    if turnover_monitor:
                        week_id = self._calendar.get_week_id(trade_date)
                        turnover_monitor.record(
                            week_id, result.total_buys, result.total_sells, nav_after_calc
                        )
            except Exception as e:
                if self._verbose:
                    print(f"  [WARN] Execution failed on {trade_date}: {e}")

        # Final contest turnover check
        contest_to_violations = []
        if turnover_monitor:
            contest_to_violations = [
                {"week": w, "turnover": f"{to:.2%}", "issue": "below_5pct_minimum"}
                for w, to in turnover_monitor.check_violations()
            ]

        # Build result bundle
        result_bundle = self._build_result_bundle(
            nav_series=nav_series,
            bm_nav_series=bm_nav_series,
            holdings_history=holdings_history,
            weights_history=weights_history,
            trade_history=trade_history,
            constraint_violations=constraint_violations + contest_to_violations,
            initial_capital=initial_capital,
            start=start,
            end=end,
        )

        return result_bundle

    def _compute_target_weights(
        self,
        signal_date: str,
        holdings: Dict[str, float],
        cash: float,
        prev_weights: Optional[pd.Series],
        prices_today,
        ex_cfg,
    ) -> Optional[pd.Series]:
        """
        Run the sleeve/node-graph pipeline and return final target weights.
        Does NOT execute any trades.
        """
        strategy = self._strategy

        snap = self._snapshot_loader.load_snapshot(
            signal_date,
            strategy.base_universe,
            self._get_required_fields(),
        )
        if snap.empty:
            if self._verbose:
                print(f"  [{signal_date}] Empty universe — skip signal")
            return None

        sleeve_weights_map: Dict[str, pd.Series] = {}
        for sleeve in strategy.sleeves:
            sw = self._evaluate_sleeve(sleeve, signal_date, snap, prev_weights)
            if sw is not None and not sw.empty:
                sleeve_weights_map[sleeve.sleeve_id] = sw

        if not sleeve_weights_map:
            return None

        regime_predicates = self._evaluate_regime_predicates(signal_date, snap)
        combined_weights = self._mixer.mix(
            sleeve_weights_map,
            strategy.portfolio_aggregation,
            regime_predicates,
        )
        if combined_weights.empty:
            return None

        benchmark_sector_weights = self._get_benchmark_sector_weights(signal_date)
        final_weights, _ = apply_constraints(
            combined_weights,
            snap,
            strategy.portfolio_aggregation.final_constraints,
            mode=strategy.mode,
            benchmark_sector_weights=benchmark_sector_weights,
            prev_weights=prev_weights,
        )
        return final_weights if not final_weights.empty else None

    def _execute_with_target(
        self,
        signal_date: str,
        target_weights: pd.Series,
        holdings: Dict[str, float],
        cash: float,
        prices_today,
        ex_cfg,
    ) -> Optional[RebalanceResult]:
        """Execute trades toward pre-computed target weights."""
        strategy = self._strategy

        # Resolve fill date and prices
        fill_rule = ex_cfg.fill_rule
        if fill_rule == FillRule.NEXT_OPEN:
            fill_date = self._calendar.next_trading_day(signal_date, 1) or signal_date
            prices = self._get_open_prices(fill_date)
        elif fill_rule == FillRule.NEXT_CLOSE:
            fill_date = self._calendar.next_trading_day(signal_date, 1) or signal_date
            prices = self._get_closing_prices(fill_date)
        else:
            fill_date = signal_date
            prices = prices_today

        if strategy.mode == RunMode.CONTEST:
            # Load adv5 from snapshot for market-impact calc
            snap = self._snapshot_loader.load_snapshot(
                signal_date, strategy.base_universe, ["adv5"]
            )
            adv5 = snap.get("adv5", pd.Series(dtype=float)) if not snap.empty else pd.Series(dtype=float)
            return execute_rebalance_contest(
                signal_date=signal_date,
                fill_date=fill_date,
                target_weights=target_weights,
                current_holdings=holdings,
                current_cash=cash,
                prices=prices,
                adv5=adv5.reindex(target_weights.index).fillna(0.0),
                config=ex_cfg,
            )
        else:
            return execute_rebalance_research(
                signal_date=signal_date,
                fill_date=fill_date,
                target_weights=target_weights,
                current_holdings=holdings,
                current_cash=cash,
                prices=prices,
                config=ex_cfg,
                round_lot=ex_cfg.round_lot,
            )

    def _evaluate_sleeve(
        self,
        sleeve: SleeveConfig,
        signal_date: str,
        base_snap: pd.DataFrame,
        prev_weights: Optional[pd.Series],
    ) -> Optional[pd.Series]:
        """Generate target weights for a single sleeve."""
        # Use sleeve universe override if specified, else base snapshot
        if sleeve.universe_override:
            snap = self._snapshot_loader.load_snapshot(
                signal_date,
                sleeve.universe_override,
                self._get_required_fields(),
            )
        else:
            snap = base_snap

        if snap.empty:
            return None

        # Evaluate node graph
        scores = None
        if sleeve.node_graph.nodes:
            executor = NodeGraphExecutor(
                conn=self._conn,
                trade_date=signal_date,
                snapshot=snap,
                history_window=self._history_window,
            )
            scores = executor.evaluate(sleeve.node_graph)

        # Use score_ref from node_graph output or sleeve.score_ref
        if scores is None and sleeve.score_ref:
            scores = snap.get(sleeve.score_ref)

        # Selection
        effective_scores = scores if scores is not None else pd.Series(1.0, index=snap.index)
        selected = select_universe(effective_scores, sleeve.selection, min_names=sleeve.constraints.min_names)
        selected = apply_max_names_filter(selected, sleeve.constraints.max_names)

        if selected.empty:
            if self._verbose:
                import sys
                all_nan = scores is not None and scores.isna().all()
                reason = "모든 점수가 NaN — node_graph 노드 참조 오류 가능성" if all_nan else "선택된 종목 없음"
                print(f"  [{signal_date}] sleeve '{sleeve.sleeve_id}' 건너뜀: {reason}", file=sys.stderr)
            return None

        # Covariance history (for MV/TE optimizers)
        returns_history = None
        alloc_type = sleeve.allocator.type
        if alloc_type in ("mean_variance", "benchmark_tracking", "enhanced_index", "risk_budget", "inverse_vol"):
            lookback_start = self._calendar.prev_trading_day(signal_date, self._history_window) or "2020-12-30"
            returns_history = self._cov_loader.load_returns_for_cov(
                list(selected.index),
                lookback_start,
                signal_date,
            )

        # Sleeve-level prev weights
        sleeve_prev = None
        if prev_weights is not None:
            sleeve_prev = prev_weights.reindex(selected.index).fillna(0.0)

        # Allocate
        weights = allocate(
            tickers=list(selected.index),
            scores=selected,
            snapshot=snap,
            config=sleeve.allocator,
            constraints=sleeve.constraints,
            returns_history=returns_history,
            prev_weights=sleeve_prev,
            benchmark_weights=None,  # TODO: load if benchmark tracking
        )

        if weights.empty:
            return None

        # Apply sleeve constraints
        weights, _ = apply_constraints(
            weights,
            snap,
            sleeve.constraints,
            mode=self._strategy.mode,
        )

        return weights

    def _evaluate_regime_predicates(
        self,
        signal_date: str,
        snap: pd.DataFrame,
    ) -> Dict[str, bool]:
        """Evaluate global node graph for regime predicates."""
        pa = self._strategy.portfolio_aggregation
        if pa.method != SleeveMixMethod.REGIME_SWITCH:
            return {}
        if not pa.global_node_graph.nodes:
            return {}
        if not pa.regime_branches:
            return {}

        executor = NodeGraphExecutor(
            conn=self._conn,
            trade_date=signal_date,
            snapshot=snap,
            history_window=self._history_window,
        )
        all_values = executor.evaluate_all(pa.global_node_graph)

        results = {}
        for branch in pa.regime_branches:
            cond_id = branch.condition_node
            if cond_id in all_values:
                # Take the mode (True/False) across all tickers
                val = all_values[cond_id]
                results[cond_id] = bool(val.mode().iloc[0]) if not val.empty else False
        return results

    def _get_required_fields(self) -> List[str]:
        """Collect all field IDs needed across all sleeves."""
        fields = set()
        fields.update(["ret_1d", "ret_5d", "ret_20d", "ret_60d", "vol_20d",
                       "market_cap", "adv5", "sector_name"])
        # Fields referenced by node graph field nodes
        for sleeve in self._strategy.sleeves:
            for node in sleeve.node_graph.nodes.values():
                if node.type == "field":
                    fields.add(node.field_id)
        # extra_fields declared in universe configs
        if self._strategy.base_universe:
            fields.update(self._strategy.base_universe.extra_fields or [])
        for sleeve in self._strategy.sleeves:
            if sleeve.universe_override:
                fields.update(sleeve.universe_override.extra_fields or [])
        return list(fields)

    def _get_closing_prices(self, trade_date: str) -> Dict[str, float]:
        """Get closing prices for all tickers on a date."""
        sql = "SELECT ticker, close FROM core_price_daily WHERE trade_date = ?"
        cur = self._conn.execute(sql, [trade_date])
        return {row[0]: row[1] for row in cur.fetchall() if row[1] is not None}

    def _get_open_prices(self, trade_date: str) -> Dict[str, float]:
        """Get open prices for all tickers on a date."""
        sql = "SELECT ticker, open FROM core_price_daily WHERE trade_date = ?"
        cur = self._conn.execute(sql, [trade_date])
        prices = {row[0]: row[1] for row in cur.fetchall() if row[1] is not None}
        # Fallback to close for tickers without open
        if not prices:
            prices = self._get_closing_prices(trade_date)
        return prices

    def _get_prices(self, trade_date: str) -> Dict[str, float]:
        """Get prices using the configured fill rule."""
        fill_rule = self._strategy.effective_execution().fill_rule
        if fill_rule == FillRule.NEXT_OPEN:
            return self._get_open_prices(trade_date)
        return self._get_closing_prices(trade_date)

    def _get_benchmark_sector_weights(self, trade_date: str) -> Dict[str, float]:
        """Get benchmark sector weights from mart_sector_weight_snapshot."""
        sql = """
        SELECT sector_name, sector_weight
        FROM mart_sector_weight_snapshot
        WHERE trade_date = ?
        """
        cur = self._conn.execute(sql, [trade_date])
        rows = cur.fetchall()
        if rows:
            return {row[0]: row[1] for row in rows}

        # Fallback: compute from eligible universe market caps
        sql = """
        SELECT sec.sector_name, SUM(p.market_cap) as total_mcap
        FROM mart_universe_eligibility_daily e
        JOIN core_price_daily p ON e.trade_date = p.trade_date AND e.ticker = p.ticker
        LEFT JOIN core_sector_map sec ON e.ticker = sec.ticker
        WHERE e.trade_date = ? AND e.is_eligible = 1 AND p.market_cap IS NOT NULL
        GROUP BY sec.sector_name
        """
        cur = self._conn.execute(sql, [trade_date])
        rows = cur.fetchall()
        if not rows:
            return {}
        total = sum(r[1] or 0 for r in rows)
        if total <= 0:
            return {}
        return {r[0]: (r[1] or 0) / total for r in rows}

    def _build_result_bundle(
        self,
        nav_series: Dict[str, float],
        bm_nav_series: Dict[str, float],
        holdings_history: Dict[str, Dict[str, float]],
        weights_history: Dict[str, pd.Series],
        trade_history: List[Dict],
        constraint_violations: List[Dict],
        initial_capital: float,
        start: str,
        end: str,
    ) -> Dict[str, Any]:
        """Build the raw result bundle for analytics."""
        strategy = self._strategy

        # Compute IR hash
        ir_json = strategy.model_dump_json()
        ir_hash = hashlib.sha256(ir_json.encode()).hexdigest()[:16]

        return {
            "run_id": self._run_id,
            "run_timestamp": datetime.now().isoformat(),
            "strategy_id": strategy.strategy_id,
            "strategy_name": strategy.strategy_name or strategy.strategy_id,
            "ir_version": strategy.version,
            "ir_hash": ir_hash,
            "mode": strategy.mode.value,
            "date_range": {"start": start, "end": end},
            "initial_capital": initial_capital,
            "benchmark_index": strategy.benchmark.index_code,
            "nav_series": nav_series,
            "benchmark_nav_series": bm_nav_series,
            "holdings_history": holdings_history,
            "weights_history": {d: w.to_dict() for d, w in weights_history.items()},
            "trade_history": trade_history,
            "constraint_violations": constraint_violations,
        }
