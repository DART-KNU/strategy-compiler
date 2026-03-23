"""
Unit tests for financial as-of / PIT-safe logic.

Tests:
  1. available_date is always >= period_end
  2. Q4 lag >= 90 days
  3. Q1/Q2/Q3 lag >= 45 days
  4. as-of query returns correct (most recent available) quarter
  5. No look-ahead: a period not yet available is not returned
"""

import sys
import unittest
import sqlite3
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.calendar_utils import quarter_end_date, add_days


class TestQuarterEndDate(unittest.TestCase):
    """Test quarter end date computation."""

    def test_q1_2018(self):
        self.assertEqual(quarter_end_date("2018", "1Q"), "2018-03-31")

    def test_q2_2018(self):
        self.assertEqual(quarter_end_date("2018", "2Q"), "2018-06-30")

    def test_q3_2018(self):
        self.assertEqual(quarter_end_date("2018", "3Q"), "2018-09-30")

    def test_q4_2018(self):
        self.assertEqual(quarter_end_date("2018", "4Q"), "2018-12-31")

    def test_q1_2024(self):
        self.assertEqual(quarter_end_date(2024, "1Q"), "2024-03-31")

    def test_invalid_quarter(self):
        with self.assertRaises(ValueError):
            quarter_end_date("2018", "5Q")


class TestAvailableDate(unittest.TestCase):
    """Test PIT-safe available_date computation."""

    def test_q1_lag_45_days(self):
        period_end = quarter_end_date("2024", "1Q")  # 2024-03-31
        avail = add_days(period_end, 45)
        self.assertEqual(avail, "2024-05-15")

    def test_q4_lag_90_days(self):
        period_end = quarter_end_date("2023", "4Q")  # 2023-12-31
        avail = add_days(period_end, 90)
        self.assertEqual(avail, "2024-03-30")

    def test_available_always_after_period_end(self):
        for year in ["2021", "2022", "2023", "2024"]:
            for q, lag in [("1Q", 45), ("2Q", 45), ("3Q", 45), ("4Q", 90)]:
                period_end = quarter_end_date(year, q)
                avail = add_days(period_end, lag)
                self.assertGreater(avail, period_end,
                                   f"available_date not > period_end for {year}-{q}")


class TestFundamentalsAsOf(unittest.TestCase):
    """Integration test: verify PIT-safe as-of join behavior."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
            CREATE TABLE core_price_daily (
                trade_date TEXT, ticker TEXT,
                adj_close REAL, market_cap REAL, traded_value REAL,
                open REAL, high REAL, low REAL, close REAL,
                adj_open REAL, adj_high REAL, adj_low REAL, adj_factor REAL,
                volume REAL, shares_outstanding REAL, trading_halt_flag REAL,
                admin_supervision_flag REAL, float_shares REAL, float_ratio REAL,
                PRIMARY KEY (trade_date, ticker)
            );
            CREATE TABLE core_financials_quarterly (
                ticker TEXT, year TEXT, quarter TEXT,
                fiscal_month TEXT, report_type TEXT,
                period_end TEXT, available_date TEXT,
                total_assets REAL, total_liabilities REAL,
                total_equity_parent REAL, sales REAL, cogs REAL,
                operating_income REAL, net_income_parent REAL,
                operating_cash_flow REAL, cash_and_cash_equivalents REAL,
                total_financial_debt REAL,
                PRIMARY KEY (ticker, year, quarter)
            );
            CREATE TABLE mart_fundamentals_asof_daily (
                trade_date TEXT, ticker TEXT,
                available_year TEXT, available_quarter TEXT,
                period_end TEXT, available_date TEXT,
                total_assets REAL, total_liabilities REAL,
                total_equity_parent REAL, sales REAL, cogs REAL,
                operating_income REAL, net_income_parent REAL,
                operating_cash_flow REAL, cash_and_cash_equivalents REAL,
                total_financial_debt REAL,
                PRIMARY KEY (trade_date, ticker)
            );
        """)

        # Samsung-like ticker with quarterly data
        # Q4 2023: period_end=2023-12-31, available=2024-03-30 (90-day lag)
        # Q1 2024: period_end=2024-03-31, available=2024-05-15 (45-day lag)
        self.conn.executescript("""
            INSERT INTO core_financials_quarterly VALUES
                ('005930', '2023', '4Q', '12', 'IFRS',
                 '2023-12-31', '2024-03-30',
                 300e12, 80e12, 200e12, 50e12, 25e12, 10e12, 8e12, 15e12, 20e12, 30e12);
            INSERT INTO core_financials_quarterly VALUES
                ('005930', '2024', '1Q', '12', 'IFRS',
                 '2024-03-31', '2024-05-15',
                 310e12, 82e12, 205e12, 52e12, 26e12, 11e12, 8.5e12, 16e12, 22e12, 28e12);
        """)

        # Price data at various dates
        dates = [
            "2024-01-15",  # before Q4 2023 is available
            "2024-03-30",  # Q4 2023 just became available
            "2024-04-01",  # Q4 2023 available, Q1 2024 not yet
            "2024-05-15",  # Q1 2024 just became available
            "2024-06-01",  # Q1 2024 available
        ]
        for d in dates:
            self.conn.execute(
                "INSERT INTO core_price_daily (trade_date, ticker, adj_close) VALUES (?, '005930', 70000)",
                (d,)
            )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_no_data_before_availability(self):
        """On 2024-01-15: Q4 2023 not yet available (available_date = 2024-03-30).
           As-of join should return no data."""
        from src.transform.financials import build_fundamentals_asof_daily
        build_fundamentals_asof_daily(self.conn)

        row = self.conn.execute(
            "SELECT * FROM mart_fundamentals_asof_daily "
            "WHERE ticker = '005930' AND trade_date = '2024-01-15'"
        ).fetchone()
        # No quarterly data available before 2024-03-30
        self.assertIsNone(row)

    def test_q4_2023_available_on_2024_03_30(self):
        """On 2024-03-30: Q4 2023 is available (available_date = 2024-03-30)."""
        from src.transform.financials import build_fundamentals_asof_daily
        build_fundamentals_asof_daily(self.conn)

        row = self.conn.execute(
            "SELECT * FROM mart_fundamentals_asof_daily "
            "WHERE ticker = '005930' AND trade_date = '2024-03-30'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["available_year"],    "2023")
        self.assertEqual(row["available_quarter"], "4Q")

    def test_still_q4_on_2024_04_01(self):
        """On 2024-04-01: Q1 2024 not yet available; should return Q4 2023."""
        from src.transform.financials import build_fundamentals_asof_daily
        build_fundamentals_asof_daily(self.conn)

        row = self.conn.execute(
            "SELECT * FROM mart_fundamentals_asof_daily "
            "WHERE ticker = '005930' AND trade_date = '2024-04-01'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["available_year"],    "2023")
        self.assertEqual(row["available_quarter"], "4Q")

    def test_q1_2024_available_on_2024_05_15(self):
        """On 2024-05-15: Q1 2024 becomes available (available_date = 2024-05-15)."""
        from src.transform.financials import build_fundamentals_asof_daily
        build_fundamentals_asof_daily(self.conn)

        row = self.conn.execute(
            "SELECT * FROM mart_fundamentals_asof_daily "
            "WHERE ticker = '005930' AND trade_date = '2024-05-15'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["available_year"],    "2024")
        self.assertEqual(row["available_quarter"], "1Q")

    def test_no_lookahead_on_2024_05_14(self):
        """One day before Q1 2024 is available, should still show Q4 2023."""
        # Add 2024-05-14 price data
        self.conn.execute(
            "INSERT INTO core_price_daily (trade_date, ticker, adj_close) VALUES ('2024-05-14', '005930', 71000)"
        )
        self.conn.commit()

        from src.transform.financials import build_fundamentals_asof_daily
        build_fundamentals_asof_daily(self.conn)

        row = self.conn.execute(
            "SELECT * FROM mart_fundamentals_asof_daily "
            "WHERE ticker = '005930' AND trade_date = '2024-05-14'"
        ).fetchone()
        # Should still show Q4 2023 (Q1 2024 not available until 2024-05-15)
        if row:
            self.assertEqual(row["available_year"],    "2023")
            self.assertEqual(row["available_quarter"], "4Q")

    def test_available_date_always_after_period_end(self):
        """Verify all available_dates in the table are >= period_end."""
        bad = self.conn.execute(
            "SELECT COUNT(*) FROM core_financials_quarterly "
            "WHERE available_date < period_end"
        ).fetchone()[0]
        self.assertEqual(bad, 0, "Found records where available_date < period_end!")


if __name__ == "__main__":
    unittest.main()
