"""
Unit tests for eligibility logic.

Tests the eligibility computation in isolation using a mock SQLite database.
"""

import sys
import unittest
import sqlite3
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.transform.eligibility import (
    BIT_NOT_LISTED, BIT_NOT_COMMON_EQUITY, BIT_WRONG_MARKET,
    BIT_TOO_NEW, BIT_LOW_LIQUIDITY, BIT_SMALL_MCAP,
    BIT_CAUTION, BIT_WARNING, BIT_RISK, BIT_ADMIN, BIT_HALT,
    BIT_LABELS,
)


class TestEligibilityBitmask(unittest.TestCase):
    """Verify bitmask values are distinct and cover all expected flags."""

    def test_all_bits_distinct(self):
        bits = [
            BIT_NOT_LISTED, BIT_NOT_COMMON_EQUITY, BIT_WRONG_MARKET,
            BIT_TOO_NEW, BIT_LOW_LIQUIDITY, BIT_SMALL_MCAP,
            BIT_CAUTION, BIT_WARNING, BIT_RISK, BIT_ADMIN, BIT_HALT,
        ]
        self.assertEqual(len(bits), len(set(bits)))

    def test_all_bits_powers_of_two(self):
        bits = [
            BIT_NOT_LISTED, BIT_NOT_COMMON_EQUITY, BIT_WRONG_MARKET,
            BIT_TOO_NEW, BIT_LOW_LIQUIDITY, BIT_SMALL_MCAP,
            BIT_CAUTION, BIT_WARNING, BIT_RISK, BIT_ADMIN, BIT_HALT,
        ]
        for b in bits:
            self.assertTrue(b > 0 and (b & (b - 1)) == 0, f"{b} is not a power of 2")

    def test_all_bits_have_labels(self):
        bits = [
            BIT_NOT_LISTED, BIT_NOT_COMMON_EQUITY, BIT_WRONG_MARKET,
            BIT_TOO_NEW, BIT_LOW_LIQUIDITY, BIT_SMALL_MCAP,
            BIT_CAUTION, BIT_WARNING, BIT_RISK, BIT_ADMIN, BIT_HALT,
        ]
        for b in bits:
            self.assertIn(b, BIT_LABELS, f"BIT {b} has no label")

    def test_combined_mask(self):
        """Combining masks with OR should be reversible via AND."""
        mask = BIT_NOT_LISTED | BIT_SMALL_MCAP | BIT_LOW_LIQUIDITY
        self.assertTrue(mask & BIT_NOT_LISTED)
        self.assertTrue(mask & BIT_SMALL_MCAP)
        self.assertFalse(mask & BIT_CAUTION)


class TestEligibilityDatabase(unittest.TestCase):
    """Integration test: build a minimal in-memory DB and test eligibility."""

    def setUp(self):
        """Create a minimal in-memory DB with required tables."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
            CREATE TABLE core_price_daily (
                trade_date TEXT, ticker TEXT,
                open REAL, high REAL, low REAL, close REAL,
                adj_open REAL, adj_high REAL, adj_low REAL, adj_close REAL,
                adj_factor REAL, volume REAL, traded_value REAL,
                shares_outstanding REAL, market_cap REAL,
                trading_halt_flag REAL, admin_supervision_flag REAL,
                float_shares REAL, float_ratio REAL,
                PRIMARY KEY (trade_date, ticker)
            );
            CREATE TABLE core_security_master (
                ticker TEXT PRIMARY KEY,
                corp_name TEXT, market_type TEXT,
                security_type TEXT, is_common_equity INTEGER,
                listing_date TEXT, delisting_date TEXT,
                is_active_current INTEGER, listing_type TEXT,
                fiscal_month TEXT, industry TEXT, source_notes TEXT
            );
            CREATE TABLE mart_liquidity_daily (
                trade_date TEXT, ticker TEXT,
                adv5 REAL, adv20 REAL, market_cap REAL,
                listing_age_bd INTEGER,
                is_above_3bn_adv5 INTEGER, is_above_100bn_mcap INTEGER,
                PRIMARY KEY (trade_date, ticker)
            );
            CREATE TABLE core_regulatory_status_interval (
                ticker TEXT, status_type TEXT,
                interval_start TEXT, interval_end TEXT,
                source_detail TEXT,
                PRIMARY KEY (ticker, status_type, interval_start)
            );
            CREATE TABLE core_calendar (
                trade_date TEXT PRIMARY KEY,
                is_open INTEGER,
                prev_open_date TEXT, next_open_date TEXT,
                week_id TEXT, month_id TEXT
            );
            CREATE TABLE mart_universe_eligibility_daily (
                trade_date TEXT, ticker TEXT,
                is_listed INTEGER, is_common_equity INTEGER,
                is_market_ok INTEGER, is_listing_age_ok INTEGER,
                is_liquidity_ok INTEGER, is_mcap_ok INTEGER,
                is_not_caution INTEGER, is_not_warning INTEGER,
                is_not_risk INTEGER, is_not_admin INTEGER, is_not_halt INTEGER,
                is_eligible INTEGER, block_reason_mask INTEGER,
                block_reason_json TEXT,
                PRIMARY KEY (trade_date, ticker)
            );
        """)

        # Insert test data
        self.conn.executescript("""
            -- Trading calendar
            INSERT INTO core_calendar VALUES ('2024-01-15', 1, '2024-01-12', '2024-01-16', '2024-W03', '2024-01');
            INSERT INTO core_calendar VALUES ('2024-01-16', 1, '2024-01-15', '2024-01-17', '2024-W03', '2024-01');

            -- Security master: eligible stock
            INSERT INTO core_security_master VALUES (
                '005930', '삼성전자', '코스피', '주권', 1,
                '2000-01-01', NULL, 1, '신규상장', '12', 'semiconductor', ''
            );
            -- Security master: non-common-equity stock
            INSERT INTO core_security_master VALUES (
                '005935', '삼성전자우', '코스피', '우선주', 0,
                '2000-01-01', NULL, 1, '신규상장', '12', 'semiconductor', ''
            );
            -- Security master: wrong market stock
            INSERT INTO core_security_master VALUES (
                '999999', 'KONEX주', 'KONEX', '주권', 1,
                '2020-01-01', NULL, 1, '신규상장', '12', 'other', ''
            );
            -- Security master: brand new listing (2 business days old)
            INSERT INTO core_security_master VALUES (
                '123456', '신규주', '코스닥', '주권', 1,
                '2024-01-12', NULL, 1, '신규상장', '12', 'tech', ''
            );

            -- Price data
            INSERT INTO core_price_daily (trade_date, ticker, close, adj_close, market_cap, traded_value, trading_halt_flag, admin_supervision_flag)
                VALUES ('2024-01-15', '005930', 70000, 70000, 4e14, 1e12, 0, 0);
            INSERT INTO core_price_daily (trade_date, ticker, close, adj_close, market_cap, traded_value, trading_halt_flag, admin_supervision_flag)
                VALUES ('2024-01-15', '005935', 60000, 60000, 1e12, 5e9, 0, 0);
            INSERT INTO core_price_daily (trade_date, ticker, close, adj_close, market_cap, traded_value, trading_halt_flag, admin_supervision_flag)
                VALUES ('2024-01-15', '999999', 1000, 1000, 5e10, 1e8, 0, 0);
            INSERT INTO core_price_daily (trade_date, ticker, close, adj_close, market_cap, traded_value, trading_halt_flag, admin_supervision_flag)
                VALUES ('2024-01-15', '123456', 5000, 5000, 2e11, 2e9, 0, 0);

            -- Liquidity
            INSERT INTO mart_liquidity_daily VALUES ('2024-01-15', '005930', 1e12, 9e11, 4e14, 100, 1, 1);
            INSERT INTO mart_liquidity_daily VALUES ('2024-01-15', '005935', 5e9,  4e9,  1e12, 200, 1, 1);
            INSERT INTO mart_liquidity_daily VALUES ('2024-01-15', '999999', 1e8,  9e7,  5e10, 300, 0, 0);
            INSERT INTO mart_liquidity_daily VALUES ('2024-01-15', '123456', 2e9,  1.8e9,2e11, 2, 0, 1);

            -- Regulatory: 005935 is warned on 2024-01-15
            INSERT INTO core_regulatory_status_interval VALUES
                ('005935', 'warning', '2024-01-14', '2024-01-20', 'test warning');
        """)

    def tearDown(self):
        self.conn.close()

    def test_eligible_stock_flags(self):
        """A stock meeting all criteria should be fully eligible."""
        from src.transform.eligibility import build_universe_eligibility
        build_universe_eligibility(
            self.conn,
            min_adv5=3e9, min_mcap=1e11, min_listing_age_bd=6,
            eligible_markets={"코스피", "코스닥"},
        )

        row = self.conn.execute(
            "SELECT * FROM mart_universe_eligibility_daily WHERE ticker = '005930'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["is_listed"],         1)
        self.assertEqual(row["is_common_equity"],  1)
        self.assertEqual(row["is_market_ok"],      1)
        self.assertEqual(row["is_listing_age_ok"], 1)
        self.assertEqual(row["is_liquidity_ok"],   1)
        self.assertEqual(row["is_mcap_ok"],        1)
        self.assertEqual(row["is_not_warning"],    1)
        self.assertEqual(row["is_eligible"],       1)
        self.assertEqual(row["block_reason_mask"], 0)

    def test_non_common_equity_blocked(self):
        """Preferred shares should be blocked: is_common_equity = 0."""
        from src.transform.eligibility import build_universe_eligibility
        build_universe_eligibility(
            self.conn,
            min_adv5=3e9, min_mcap=1e11, min_listing_age_bd=6,
            eligible_markets={"코스피", "코스닥"},
        )
        row = self.conn.execute(
            "SELECT * FROM mart_universe_eligibility_daily WHERE ticker = '005935'"
        ).fetchone()
        self.assertEqual(row["is_common_equity"], 0)
        self.assertEqual(row["is_eligible"], 0)
        self.assertTrue(row["block_reason_mask"] & BIT_NOT_COMMON_EQUITY)

    def test_wrong_market_blocked(self):
        """KONEX stock should be blocked by wrong_market."""
        from src.transform.eligibility import build_universe_eligibility
        build_universe_eligibility(
            self.conn,
            min_adv5=3e9, min_mcap=1e11, min_listing_age_bd=6,
            eligible_markets={"코스피", "코스닥"},
        )
        row = self.conn.execute(
            "SELECT * FROM mart_universe_eligibility_daily WHERE ticker = '999999'"
        ).fetchone()
        self.assertEqual(row["is_market_ok"], 0)
        self.assertEqual(row["is_eligible"], 0)
        self.assertTrue(row["block_reason_mask"] & BIT_WRONG_MARKET)

    def test_new_listing_blocked(self):
        """Stock with listing_age_bd=2 (< 6) should be blocked."""
        from src.transform.eligibility import build_universe_eligibility
        build_universe_eligibility(
            self.conn,
            min_adv5=3e9, min_mcap=1e11, min_listing_age_bd=6,
            eligible_markets={"코스피", "코스닥"},
        )
        row = self.conn.execute(
            "SELECT * FROM mart_universe_eligibility_daily WHERE ticker = '123456'"
        ).fetchone()
        self.assertEqual(row["is_listing_age_ok"], 0)
        self.assertEqual(row["is_eligible"], 0)
        self.assertTrue(row["block_reason_mask"] & BIT_TOO_NEW)

    def test_warned_stock_blocked(self):
        """005935 is warned; should have is_not_warning = 0."""
        from src.transform.eligibility import build_universe_eligibility
        build_universe_eligibility(
            self.conn,
            min_adv5=3e9, min_mcap=1e11, min_listing_age_bd=6,
            eligible_markets={"코스피", "코스닥"},
        )
        row = self.conn.execute(
            "SELECT * FROM mart_universe_eligibility_daily WHERE ticker = '005935'"
        ).fetchone()
        self.assertEqual(row["is_not_warning"], 0)
        self.assertTrue(row["block_reason_mask"] & BIT_WARNING)

    def test_block_reason_json_populated(self):
        """Blocked stocks should have non-null block_reason_json."""
        from src.transform.eligibility import build_universe_eligibility
        import json
        build_universe_eligibility(
            self.conn,
            min_adv5=3e9, min_mcap=1e11, min_listing_age_bd=6,
            eligible_markets={"코스피", "코스닥"},
        )
        row = self.conn.execute(
            "SELECT block_reason_json FROM mart_universe_eligibility_daily "
            "WHERE ticker = '999999'"
        ).fetchone()
        self.assertIsNotNone(row["block_reason_json"])
        d = json.loads(row["block_reason_json"])
        self.assertIn("blocks", d)
        self.assertIn("wrong_market", d["blocks"])


if __name__ == "__main__":
    unittest.main()
