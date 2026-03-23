"""
Data loaders — fetch DataFrames from SQLite for the backtest engine.

All loaders return pandas DataFrames with consistent column naming.
Snapshot loading includes an LRU cache for repeated date lookups.
"""

from __future__ import annotations

import functools
import sqlite3
from typing import Dict, List, Optional, Tuple

import pandas as pd

from backtest_engine.data.queries import (
    build_snapshot_query,
    build_price_history_query,
    build_feature_history_query,
    build_index_history_query,
    build_covariance_data_query,
)
from backtest_engine.strategy_ir.models import RebalancingConfig, UniverseConfig


class SnapshotLoader:
    """
    Loads cross-sectional snapshots for a single trade_date.

    Caches up to `cache_size` snapshots to avoid repeated DB hits
    when the backtest loop evaluates the same date multiple times.
    """

    def __init__(self, conn: sqlite3.Connection, cache_size: int = 64):
        self._conn = conn
        self._cache: dict[tuple, pd.DataFrame] = {}
        self._cache_size = cache_size

    def load_snapshot(
        self,
        trade_date: str,
        universe_config: Optional[UniverseConfig] = None,
        requested_fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Load the full cross-sectional snapshot for a single trade_date.

        Returns DataFrame indexed by 'ticker' with all requested fields.
        Only eligible stocks are included unless include_blocked=True.

        Columns always present:
            ticker, trade_date, is_eligible, close, adj_close,
            market_cap, traded_value, adv5, adv20, listing_age_bd, sector_name
        """
        if universe_config is None:
            from backtest_engine.strategy_ir.models import UniverseConfig
            universe_config = UniverseConfig()
        if requested_fields is None:
            requested_fields = []

        cache_key = (
            trade_date,
            universe_config.include_blocked,
            tuple(sorted(requested_fields)),
            tuple(sorted(universe_config.markets or [])),
        )
        if cache_key in self._cache:
            return self._cache[cache_key]

        sql, params = build_snapshot_query(
            trade_date=trade_date,
            requested_fields=requested_fields,
            include_blocked=universe_config.include_blocked,
            markets=universe_config.markets if not universe_config.include_blocked else None,
        )

        df = pd.read_sql_query(sql, self._conn, params=params)

        if df.empty:
            self._evict_if_full(cache_key)
            self._cache[cache_key] = df
            return df

        df = df.set_index("ticker")

        # Post-filter: additional numeric thresholds from universe_config
        if universe_config.min_mcap_bn is not None:
            df = df[df["market_cap"] >= universe_config.min_mcap_bn * 1e9]
        if universe_config.min_adv5_bn is not None:
            df = df[df["adv5"] >= universe_config.min_adv5_bn * 1e9]

        self._evict_if_full(cache_key)
        self._cache[cache_key] = df
        return df

    def _evict_if_full(self, new_key: tuple) -> None:
        if len(self._cache) >= self._cache_size and new_key not in self._cache:
            # Evict oldest entry (insertion-order dict in Python 3.7+)
            oldest = next(iter(self._cache))
            del self._cache[oldest]

    def clear_cache(self) -> None:
        self._cache.clear()


class PriceHistoryLoader:
    """Load price history for a list of tickers over a date range."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def load(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Returns wide-format DataFrame: rows = dates, columns = tickers (for a single field)
        or long format if multiple fields requested.

        If fields contains a single field, returns a (date x ticker) pivot.
        Otherwise returns long format with columns [trade_date, ticker, ...fields].
        """
        if not tickers:
            return pd.DataFrame()
        sql, params = build_price_history_query(tickers, start_date, end_date, fields)
        df = pd.read_sql_query(sql, self._conn, params=params)
        return df

    def load_returns(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Returns wide-format (date x ticker) DataFrame of adj_close prices.
        """
        df = self.load(tickers, start_date, end_date, ["adj_close"])
        if df.empty:
            return df
        pivot = df.pivot(index="trade_date", columns="ticker", values="adj_close")
        return pivot

    def load_return_series(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Return wide-format (date x ticker) DataFrame of daily returns from mart_feature_daily."""
        if not tickers:
            return pd.DataFrame()
        from backtest_engine.data.queries import build_feature_history_query
        sql, params = build_feature_history_query(tickers, start_date, end_date, ["ret_1d"])
        df = pd.read_sql_query(sql, self._conn, params=params)
        if df.empty:
            return df
        return df.pivot(index="trade_date", columns="ticker", values="ret_1d")


class FeatureHistoryLoader:
    """Load pre-computed features for a list of tickers over a date range."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def load(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Returns long-format DataFrame [trade_date, ticker, ...fields]."""
        if not tickers:
            return pd.DataFrame()
        sql, params = build_feature_history_query(tickers, start_date, end_date, fields)
        return pd.read_sql_query(sql, self._conn, params=params)

    def load_wide(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
        field: str,
    ) -> pd.DataFrame:
        """Returns (date x ticker) wide DataFrame for a single feature field."""
        df = self.load(tickers, start_date, end_date, [field])
        if df.empty:
            return df
        return df.pivot(index="trade_date", columns="ticker", values=field)


class IndexHistoryLoader:
    """Load index level data."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._cache: dict[tuple, pd.DataFrame] = {}

    def load(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        key = (index_code, start_date, end_date)
        if key in self._cache:
            return self._cache[key]
        sql, params = build_index_history_query(index_code, start_date, end_date)
        df = pd.read_sql_query(sql, self._conn, params=params)
        df = df.set_index("trade_date")
        self._cache[key] = df
        return df

    def load_returns(self, index_code: str, start_date: str, end_date: str) -> pd.Series:
        """Return Series of daily returns for the index."""
        df = self.load(index_code, start_date, end_date)
        if df.empty:
            return pd.Series(dtype=float, name=index_code)
        returns = df["close"].pct_change()
        returns.name = index_code
        return returns

    def load_levels(self, index_code: str, start_date: str, end_date: str) -> pd.Series:
        """Return Series of closing levels."""
        df = self.load(index_code, start_date, end_date)
        if df.empty:
            return pd.Series(dtype=float, name=index_code)
        return df["close"].rename(index_code)


class CovarianceLoader:
    """Load return data for covariance estimation."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def load_returns_for_cov(
        self,
        tickers: List[str],
        lookback_start: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Returns (date x ticker) DataFrame of daily returns for covariance estimation.
        Missing values are dropped at the ticker level.
        """
        if not tickers:
            return pd.DataFrame()
        sql, params = build_covariance_data_query(tickers, lookback_start, end_date)
        df = pd.read_sql_query(sql, self._conn, params=params)
        if df.empty:
            return df
        wide = df.pivot(index="trade_date", columns="ticker", values="ret_1d")
        # Drop tickers with > 50% missing
        threshold = len(wide) * 0.5
        wide = wide.dropna(axis=1, thresh=int(threshold))
        return wide


class RebalanceCalendarLoader:
    """Generate rebalance dates from a config."""

    def __init__(self, calendar: "CalendarProvider"):
        self._calendar = calendar

    def load(self, start: str, end: str, cfg: RebalancingConfig) -> List[str]:
        return self._calendar.get_rebalance_dates(start, end, cfg)
