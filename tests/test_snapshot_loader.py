"""Tests for snapshot loader correctness."""

import pytest
from backtest_engine.data.db import get_connection
from backtest_engine.data.calendar import CalendarProvider
from backtest_engine.data.loaders import SnapshotLoader
from backtest_engine.strategy_ir.models import UniverseConfig


DB_PATH = "database/db/data/db/backtest.db"


@pytest.fixture(scope="module")
def conn():
    return get_connection(DB_PATH)


@pytest.fixture(scope="module")
def calendar(conn):
    return CalendarProvider(conn)


@pytest.fixture(scope="module")
def loader(conn):
    return SnapshotLoader(conn)


class TestSnapshotLoader:
    def test_eligible_universe_nonempty(self, loader):
        """Should return non-empty universe on a normal trading day."""
        snap = loader.load_snapshot("2025-06-30")
        assert len(snap) > 100, f"Expected > 100 eligible stocks, got {len(snap)}"

    def test_all_returned_are_eligible(self, loader):
        """Default loader should only return is_eligible=1 stocks."""
        snap = loader.load_snapshot("2025-06-30")
        if "is_eligible" in snap.columns:
            assert (snap["is_eligible"] == 1).all()

    def test_has_required_columns(self, loader):
        """Should always have core columns."""
        snap = loader.load_snapshot("2025-06-30")
        for col in ["close", "adj_close", "market_cap", "adv5", "sector_name"]:
            assert col in snap.columns, f"Missing column: {col}"

    def test_feature_fields_loaded(self, loader):
        """Requesting feature fields should include them in the snapshot."""
        snap = loader.load_snapshot("2025-06-30", requested_fields=["ret_60d", "vol_20d"])
        assert "ret_60d" in snap.columns
        assert "vol_20d" in snap.columns

    def test_include_blocked_returns_more_tickers(self, loader):
        """include_blocked=True should return more tickers."""
        snap_eligible = loader.load_snapshot("2025-06-30")
        uc_blocked = UniverseConfig(include_blocked=True)
        snap_all = loader.load_snapshot("2025-06-30", universe_config=uc_blocked)
        assert len(snap_all) >= len(snap_eligible)

    def test_cache_works(self, loader):
        """Calling load_snapshot twice returns the same result."""
        snap1 = loader.load_snapshot("2025-03-31")
        snap2 = loader.load_snapshot("2025-03-31")
        assert snap1.equals(snap2)

    def test_min_mcap_filter(self, loader):
        """min_mcap_bn filter should reduce universe."""
        uc = UniverseConfig(min_mcap_bn=1000.0)  # 1 trillion KRW
        snap = loader.load_snapshot("2025-06-30", universe_config=uc)
        if "market_cap" in snap.columns:
            assert (snap["market_cap"] >= 1e12).all(), "Some stocks below 1T KRW after filter"


class TestCalendarProvider:
    def test_range_correct(self, calendar):
        dates = calendar.trading_days_in_range("2025-01-01", "2025-01-31")
        assert len(dates) > 15, "Should have ~20 trading days in January"

    def test_prev_next_trading_day(self, calendar):
        d = "2025-06-30"
        prev = calendar.prev_trading_day(d, 1)
        nxt = calendar.next_trading_day(d, 1)
        assert prev < d
        assert nxt > d

    def test_monthly_rebalance_dates(self, calendar):
        from backtest_engine.strategy_ir.models import RebalancingConfig, RebalanceFrequency
        cfg = RebalancingConfig(frequency=RebalanceFrequency.MONTHLY, day_of_month=1)
        dates = calendar.get_rebalance_dates("2025-01-01", "2025-12-31", cfg)
        assert len(dates) == 12, f"Expected 12 monthly dates, got {len(dates)}"
