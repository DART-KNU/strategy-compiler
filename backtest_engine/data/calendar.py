"""
CalendarProvider — wraps core_calendar table.

Provides:
- List of trading days in a range
- Previous / next trading day lookups
- Rebalance date generation by frequency
- Week/month/quarter boundary detection
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from functools import lru_cache
from typing import List, Optional, Tuple

from backtest_engine.strategy_ir.models import RebalancingConfig, RebalanceFrequency


class CalendarProvider:
    """
    Thin wrapper around the core_calendar table.

    All dates are returned as strings in YYYY-MM-DD format.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._dates: List[str] = self._load_all_dates()
        self._date_set: set[str] = set(self._dates)
        self._idx: dict[str, int] = {d: i for i, d in enumerate(self._dates)}

    def _load_all_dates(self) -> List[str]:
        cur = self._conn.execute("SELECT trade_date FROM core_calendar ORDER BY trade_date")
        return [row[0] for row in cur.fetchall()]

    @property
    def all_dates(self) -> List[str]:
        return self._dates

    @property
    def min_date(self) -> str:
        return self._dates[0]

    @property
    def max_date(self) -> str:
        return self._dates[-1]

    def is_trading_day(self, d: str) -> bool:
        return d in self._date_set

    def trading_days_in_range(self, start: str, end: str) -> List[str]:
        """Return all trading days in [start, end] (inclusive)."""
        return [d for d in self._dates if start <= d <= end]

    def prev_trading_day(self, d: str, n: int = 1) -> Optional[str]:
        """Return the N-th trading day before d (exclusive of d)."""
        idx = self._idx.get(d)
        if idx is None:
            # d is not a trading day — find the nearest before
            idx = self._find_insertion_idx(d)
        target = idx - n
        if target < 0:
            return None
        return self._dates[target]

    def next_trading_day(self, d: str, n: int = 1) -> Optional[str]:
        """Return the N-th trading day after d (exclusive of d)."""
        idx = self._idx.get(d)
        if idx is None:
            idx = self._find_insertion_idx(d) - 1
        target = idx + n
        if target >= len(self._dates):
            return None
        return self._dates[target]

    def offset(self, d: str, n: int) -> Optional[str]:
        """Return the date n business days from d (positive = forward, negative = backward)."""
        if n >= 0:
            return self.next_trading_day(d, n) if n > 0 else (d if self.is_trading_day(d) else None)
        return self.prev_trading_day(d, -n)

    def _find_insertion_idx(self, d: str) -> int:
        """Binary search for the insertion point of d in the sorted date list."""
        lo, hi = 0, len(self._dates)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._dates[mid] < d:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def business_days_between(self, start: str, end: str) -> int:
        """Number of trading days strictly between start and end (exclusive on both ends)."""
        return len([d for d in self._dates if start < d < end])

    def get_rebalance_dates(self, start: str, end: str, cfg: RebalancingConfig) -> List[str]:
        """
        Return list of rebalance dates in [start, end].

        Rebalance occurs at the START of the trading day after the calendar event,
        offset by look_ahead_buffer.
        """
        trading = self.trading_days_in_range(start, end)
        if not trading:
            return []

        if cfg.frequency == RebalanceFrequency.CUSTOM:
            return [d for d in cfg.custom_dates if start <= d <= end and d in self._date_set]

        candidates: List[str] = []

        if cfg.frequency == RebalanceFrequency.DAILY:
            candidates = list(trading)

        elif cfg.frequency == RebalanceFrequency.WEEKLY:
            # Rebalance on the first trading day of each ISO week (Mon=0 by default)
            seen_weeks: set[str] = set()
            for d in trading:
                week_id = self._get_week_id(d)
                if week_id not in seen_weeks:
                    seen_weeks.add(week_id)
                    candidates.append(d)

        elif cfg.frequency == RebalanceFrequency.MONTHLY:
            # Rebalance on the N-th trading day of each month
            seen_months: set[str] = set()
            month_counts: dict[str, int] = {}
            for d in trading:
                m = d[:7]  # YYYY-MM
                month_counts[m] = month_counts.get(m, 0) + 1
                if month_counts[m] == cfg.day_of_month and m not in seen_months:
                    seen_months.add(m)
                    candidates.append(d)

        elif cfg.frequency == RebalanceFrequency.QUARTERLY:
            # First trading day of each quarter
            seen_quarters: set[str] = set()
            for d in trading:
                q = self._get_quarter_id(d)
                if q not in seen_quarters:
                    seen_quarters.add(q)
                    candidates.append(d)

        # Apply look-ahead buffer
        if cfg.look_ahead_buffer > 0:
            result = []
            for c in candidates:
                shifted = self.next_trading_day(c, cfg.look_ahead_buffer)
                if shifted and shifted <= end:
                    result.append(shifted)
                elif c <= end:
                    result.append(c)
            return result
        return candidates

    def _get_week_id(self, d: str) -> str:
        """Return ISO week identifier like '2025-W01'."""
        dt = date.fromisoformat(d)
        return dt.strftime("%G-W%V")

    def _get_quarter_id(self, d: str) -> str:
        """Return quarter identifier like '2025-Q1'."""
        dt = date.fromisoformat(d)
        q = (dt.month - 1) // 3 + 1
        return f"{dt.year}-Q{q}"

    def get_week_id(self, d: str) -> str:
        return self._get_week_id(d)

    def get_prev_n_dates(self, d: str, n: int) -> List[str]:
        """Return the n trading dates ending at d (inclusive)."""
        idx = self._idx.get(d)
        if idx is None:
            idx = self._find_insertion_idx(d)
        start_idx = max(0, idx - n + 1)
        return self._dates[start_idx: idx + 1]
