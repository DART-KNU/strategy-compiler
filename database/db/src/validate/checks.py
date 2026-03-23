"""
Validation checks for the built database.

Returns a list of ValidationResult objects.
Each result has: name, status ('PASS'/'WARN'/'FAIL'), value, message.
"""

import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Literal

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    status: Literal["PASS", "WARN", "FAIL"]
    value: str
    message: str


def run_all_checks(conn: sqlite3.Connection) -> list[CheckResult]:
    """Run all validation checks. Returns list of CheckResult."""
    results = []
    results.extend(_row_count_checks(conn))
    results.extend(_date_range_checks(conn))
    results.extend(_missingness_checks(conn))
    results.extend(_eligibility_count_checks(conn))
    results.extend(_pit_sanity_checks(conn))
    results.extend(_duplicate_pk_checks(conn))
    results.extend(_financial_lag_checks(conn))
    results.extend(_sector_quality_checks(conn))
    results.extend(_manifest_checks(conn))
    return results


# ---------------------------------------------------------------------------
# Row count checks
# ---------------------------------------------------------------------------

def _row_count_checks(conn: sqlite3.Connection) -> list[CheckResult]:
    tables = [
        ("raw_kind_listed_companies_current", 100, None),
        ("raw_kind_delistings",               500, None),
        ("raw_kind_ipos",                     1000, None),
        ("raw_kind_investment_caution",       100, None),
        ("raw_kind_investment_warning",       100, None),
        ("raw_kind_investment_risk",          10, None),
        ("raw_dg_index_daily",                1000, None),
        ("raw_dg_stock_daily",                100000, None),
        ("raw_dg_financials_quarterly",       10000, None),
        ("raw_sector_map",                    1000, None),
        ("core_security_master",              500, None),
        ("core_calendar",                     200, None),
        ("core_price_daily",                  100000, None),
        ("core_index_daily",                  100, None),
        ("core_financials_quarterly",         1000, None),
        ("core_regulatory_status_interval",   10, None),
        ("core_sector_map",                   500, None),
        ("mart_liquidity_daily",              10000, None),
        ("mart_universe_eligibility_daily",   10000, None),
        ("mart_fundamentals_asof_daily",      1000, None),
        ("mart_feature_daily",                10000, None),
        ("meta_field_catalog",                20, None),
        ("meta_dataset_coverage",             5, None),
    ]

    results = []
    for table, min_rows, _ in tables:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if n < min_rows:
                status = "WARN" if n > 0 else "FAIL"
                msg = f"Expected >= {min_rows:,} rows, found {n:,}"
            else:
                status = "PASS"
                msg = f"{n:,} rows"
            results.append(CheckResult(f"row_count.{table}", status, str(n), msg))
        except Exception as e:
            results.append(CheckResult(f"row_count.{table}", "FAIL", "ERROR", str(e)))

    return results


# ---------------------------------------------------------------------------
# Date range checks
# ---------------------------------------------------------------------------

def _date_range_checks(conn: sqlite3.Connection) -> list[CheckResult]:
    results = []

    checks = [
        ("core_price_daily",                  "trade_date", "2021-01-01", "2026-03-20"),
        ("core_index_daily",                  "trade_date", "2021-01-01", "2026-03-20"),
        ("core_calendar",                     "trade_date", "2021-01-01", "2026-03-20"),
        ("core_financials_quarterly",         "period_end", "2018-03-31", "2025-12-31"),
        ("mart_universe_eligibility_daily",   "trade_date", "2021-01-01", "2026-03-20"),
    ]

    for table, col, expected_min, expected_max in checks:
        try:
            row = conn.execute(f"SELECT MIN({col}), MAX({col}) FROM {table}").fetchone()
            actual_min, actual_max = row[0], row[1]
            if actual_min is None:
                results.append(CheckResult(
                    f"date_range.{table}.{col}", "FAIL", "NULL",
                    "No data found"))
                continue

            ok_min = actual_min <= expected_min
            ok_max = actual_max >= expected_max
            if ok_min and ok_max:
                status = "PASS"
                msg = f"{actual_min} to {actual_max}"
            else:
                status = "WARN"
                msg = (f"Expected [{expected_min}, {expected_max}]; "
                       f"got [{actual_min}, {actual_max}]")
            results.append(CheckResult(
                f"date_range.{table}.{col}", status,
                f"{actual_min} to {actual_max}", msg))
        except Exception as e:
            results.append(CheckResult(f"date_range.{table}.{col}", "FAIL", "ERROR", str(e)))

    return results


# ---------------------------------------------------------------------------
# Missingness checks
# ---------------------------------------------------------------------------

def _missingness_checks(conn: sqlite3.Connection) -> list[CheckResult]:
    results = []

    col_checks = [
        ("core_price_daily",   "close",        0.20),
        ("core_price_daily",   "adj_close",    0.20),
        ("core_price_daily",   "market_cap",   0.20),
        ("core_price_daily",   "traded_value", 0.20),
        ("core_security_master", "listing_date", 0.10),
        ("core_sector_map",    "sector_name",  0.01),
    ]

    for table, col, max_missing_frac in col_checks:
        try:
            row = conn.execute(
                f"SELECT COUNT(*), COUNT({col}) FROM {table}"
            ).fetchone()
            total, non_null = row[0], row[1]
            if total == 0:
                results.append(CheckResult(
                    f"missingness.{table}.{col}", "WARN", "N/A", "Table is empty"))
                continue
            missing_frac = (total - non_null) / total
            pct = missing_frac * 100
            if missing_frac > max_missing_frac:
                status = "WARN"
                msg = f"{pct:.1f}% missing (limit {max_missing_frac*100:.0f}%)"
            else:
                status = "PASS"
                msg = f"{pct:.1f}% missing"
            results.append(CheckResult(
                f"missingness.{table}.{col}", status, f"{pct:.1f}%", msg))
        except Exception as e:
            results.append(CheckResult(
                f"missingness.{table}.{col}", "FAIL", "ERROR", str(e)))

    return results


# ---------------------------------------------------------------------------
# Eligibility count checks
# ---------------------------------------------------------------------------

def _eligibility_count_checks(conn: sqlite3.Connection) -> list[CheckResult]:
    """Check that eligible universe has reasonable size on sample dates."""
    results = []

    try:
        # Get a recent date with data
        row = conn.execute(
            "SELECT trade_date FROM mart_universe_eligibility_daily "
            "WHERE trade_date >= '2024-01-01' ORDER BY trade_date DESC LIMIT 1"
        ).fetchone()
        if row is None:
            results.append(CheckResult(
                "eligibility.recent_date", "WARN", "N/A",
                "No eligibility data from 2024 onwards"))
            return results

        sample_date = row[0]
        eligible_count = conn.execute(
            "SELECT COUNT(*) FROM mart_universe_eligibility_daily "
            "WHERE trade_date = ? AND is_eligible = 1", (sample_date,)
        ).fetchone()[0]

        total_count = conn.execute(
            "SELECT COUNT(*) FROM mart_universe_eligibility_daily "
            "WHERE trade_date = ?", (sample_date,)
        ).fetchone()[0]

        msg = (f"On {sample_date}: {eligible_count} eligible / {total_count} total "
               f"({eligible_count/total_count*100:.1f}%)" if total_count > 0 else "no data")

        # Expect at least 100 eligible stocks on any given trading day
        if eligible_count < 100:
            status = "WARN"
        else:
            status = "PASS"

        results.append(CheckResult("eligibility.eligible_count", status,
                                   str(eligible_count), msg))

    except Exception as e:
        results.append(CheckResult("eligibility.eligible_count", "FAIL", "ERROR", str(e)))

    return results


# ---------------------------------------------------------------------------
# PIT sanity checks
# ---------------------------------------------------------------------------

def _pit_sanity_checks(conn: sqlite3.Connection) -> list[CheckResult]:
    """Verify PIT rules are correctly applied."""
    results = []

    # 1. Newly listed stocks should be blocked before 6 business days
    try:
        # Find any stock that was marked eligible within 5 business days of listing
        bad = conn.execute(
            """
            SELECT COUNT(*)
            FROM mart_universe_eligibility_daily e
            JOIN core_security_master s ON e.ticker = s.ticker
            JOIN mart_liquidity_daily l ON e.trade_date = l.trade_date AND e.ticker = l.ticker
            WHERE e.is_eligible = 1
              AND s.listing_date IS NOT NULL
              AND l.listing_age_bd < 6
            """
        ).fetchone()[0]
        if bad > 0:
            results.append(CheckResult(
                "pit.new_listing_block", "FAIL", str(bad),
                f"{bad} eligible rows found with listing_age_bd < 6 (should be 0)"))
        else:
            results.append(CheckResult(
                "pit.new_listing_block", "PASS", "0",
                "No eligible stocks with listing_age_bd < 6"))
    except Exception as e:
        results.append(CheckResult("pit.new_listing_block", "FAIL", "ERROR", str(e)))

    # 2. Warned/risk stocks should be blocked on designated dates
    for status_type, flag_col in [
        ("warning", "is_not_warning"),
        ("risk",    "is_not_risk"),
    ]:
        try:
            bad = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM mart_universe_eligibility_daily e
                JOIN core_regulatory_status_interval i
                    ON e.ticker = i.ticker
                   AND e.trade_date BETWEEN i.interval_start AND i.interval_end
                WHERE i.status_type = '{status_type}'
                  AND e.{flag_col} = 1
                """
            ).fetchone()[0]
            if bad > 0:
                results.append(CheckResult(
                    f"pit.{status_type}_flag", "FAIL", str(bad),
                    f"{bad} rows with {status_type} interval but {flag_col}=1 (should be 0)"))
            else:
                results.append(CheckResult(
                    f"pit.{status_type}_flag", "PASS", "0",
                    f"All {status_type} intervals correctly blocked"))
        except Exception as e:
            results.append(CheckResult(f"pit.{status_type}_flag", "FAIL", "ERROR", str(e)))

    # 3. Small mcap stocks blocked
    try:
        bad = conn.execute(
            """
            SELECT COUNT(*)
            FROM mart_universe_eligibility_daily e
            JOIN core_price_daily p ON e.trade_date = p.trade_date AND e.ticker = p.ticker
            WHERE e.is_eligible = 1
              AND p.market_cap IS NOT NULL
              AND p.market_cap < 100000000000
            """
        ).fetchone()[0]
        if bad > 0:
            results.append(CheckResult(
                "pit.mcap_filter", "FAIL", str(bad),
                f"{bad} eligible rows with market_cap < 100bn KRW"))
        else:
            results.append(CheckResult(
                "pit.mcap_filter", "PASS", "0",
                "No eligible stocks below 100bn KRW market cap"))
    except Exception as e:
        results.append(CheckResult("pit.mcap_filter", "FAIL", "ERROR", str(e)))

    # 4. Financial available_date is always >= period_end
    try:
        bad = conn.execute(
            """
            SELECT COUNT(*) FROM core_financials_quarterly
            WHERE available_date < period_end
            """
        ).fetchone()[0]
        if bad > 0:
            results.append(CheckResult(
                "pit.financial_lag", "FAIL", str(bad),
                f"{bad} rows where available_date < period_end (look-ahead bias!)"))
        else:
            results.append(CheckResult(
                "pit.financial_lag", "PASS", "0",
                "All available_date >= period_end"))
    except Exception as e:
        results.append(CheckResult("pit.financial_lag", "FAIL", "ERROR", str(e)))

    return results


# ---------------------------------------------------------------------------
# Duplicate PK checks
# ---------------------------------------------------------------------------

def _duplicate_pk_checks(conn: sqlite3.Connection) -> list[CheckResult]:
    results = []

    pk_checks = [
        ("core_security_master",               "ticker"),
        ("core_price_daily",                   "trade_date, ticker"),
        ("core_financials_quarterly",          "ticker, year, quarter"),
        ("mart_universe_eligibility_daily",    "trade_date, ticker"),
        ("mart_liquidity_daily",               "trade_date, ticker"),
        ("mart_fundamentals_asof_daily",       "trade_date, ticker"),
        ("mart_feature_daily",                 "trade_date, ticker"),
    ]

    for table, pk_cols in pk_checks:
        try:
            dups = conn.execute(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT {pk_cols}, COUNT(*) AS cnt FROM {table} "
                f"  GROUP BY {pk_cols} HAVING cnt > 1"
                f")"
            ).fetchone()[0]
            if dups > 0:
                results.append(CheckResult(
                    f"dup_pk.{table}", "FAIL", str(dups),
                    f"{dups} duplicate PK groups in {table}"))
            else:
                results.append(CheckResult(
                    f"dup_pk.{table}", "PASS", "0", "No duplicate PKs"))
        except Exception as e:
            results.append(CheckResult(f"dup_pk.{table}", "FAIL", "ERROR", str(e)))

    return results


# ---------------------------------------------------------------------------
# Financial lag sanity checks
# ---------------------------------------------------------------------------

def _financial_lag_checks(conn: sqlite3.Connection) -> list[CheckResult]:
    results = []

    try:
        # Q4 lag should be >= 90 days
        bad_q4 = conn.execute(
            """
            SELECT COUNT(*)
            FROM core_financials_quarterly
            WHERE quarter = '4Q'
              AND CAST(
                JULIANDAY(available_date) - JULIANDAY(period_end)
              AS INTEGER) < 90
            """
        ).fetchone()[0]
        if bad_q4 > 0:
            results.append(CheckResult(
                "financial_lag.q4_90d", "WARN", str(bad_q4),
                f"{bad_q4} Q4 records with lag < 90 days"))
        else:
            results.append(CheckResult(
                "financial_lag.q4_90d", "PASS", "0",
                "All Q4 available_date >= period_end + 90 days"))

        # Q1-Q3 lag should be >= 45 days
        bad_q123 = conn.execute(
            """
            SELECT COUNT(*)
            FROM core_financials_quarterly
            WHERE quarter IN ('1Q','2Q','3Q')
              AND CAST(
                JULIANDAY(available_date) - JULIANDAY(period_end)
              AS INTEGER) < 45
            """
        ).fetchone()[0]
        if bad_q123 > 0:
            results.append(CheckResult(
                "financial_lag.q123_45d", "WARN", str(bad_q123),
                f"{bad_q123} Q1/Q2/Q3 records with lag < 45 days"))
        else:
            results.append(CheckResult(
                "financial_lag.q123_45d", "PASS", "0",
                "All Q1-Q3 available_date >= period_end + 45 days"))

    except Exception as e:
        results.append(CheckResult("financial_lag", "FAIL", "ERROR", str(e)))

    return results


# ---------------------------------------------------------------------------
# Sector quality checks
# ---------------------------------------------------------------------------

def _sector_quality_checks(conn: sqlite3.Connection) -> list[CheckResult]:
    results = []

    try:
        # How many price tickers have no sector mapping?
        no_sector = conn.execute(
            """
            SELECT COUNT(DISTINCT p.ticker)
            FROM core_price_daily p
            LEFT JOIN core_sector_map s ON p.ticker = s.ticker
            WHERE s.ticker IS NULL
            """
        ).fetchone()[0]
        total_tickers = conn.execute(
            "SELECT COUNT(DISTINCT ticker) FROM core_price_daily"
        ).fetchone()[0]
        pct = no_sector / total_tickers * 100 if total_tickers > 0 else 0
        status = "WARN" if pct > 20 else "PASS"
        results.append(CheckResult(
            "sector.coverage", status, f"{100-pct:.1f}%",
            f"{total_tickers - no_sector} of {total_tickers} tickers have sector mapping "
            f"({no_sector} without)"))

        # Sector distribution
        sector_counts = conn.execute(
            "SELECT sector_name, COUNT(*) as n FROM core_sector_map GROUP BY sector_name"
        ).fetchall()
        sector_str = ", ".join(f"{r[0]}:{r[1]}" for r in sector_counts)
        results.append(CheckResult(
            "sector.distribution", "PASS", str(len(sector_counts)),
            f"Sectors: {sector_str}"))

    except Exception as e:
        results.append(CheckResult("sector.quality", "FAIL", "ERROR", str(e)))

    return results


# ---------------------------------------------------------------------------
# Manifest checks
# ---------------------------------------------------------------------------

def _manifest_checks(conn: sqlite3.Connection) -> list[CheckResult]:
    results = []

    try:
        # Latest build run
        run_row = conn.execute(
            "SELECT build_run_id, COUNT(*) AS n FROM raw_build_manifest "
            "GROUP BY build_run_id ORDER BY build_run_id DESC LIMIT 1"
        ).fetchone()

        if run_row is None:
            results.append(CheckResult("manifest.exists", "FAIL", "0",
                                       "No build manifest found"))
            return results

        build_run_id = run_row["build_run_id"]
        n_files = run_row["n"]
        results.append(CheckResult("manifest.latest_run", "PASS", build_run_id,
                                   f"{n_files} files recorded in latest build"))

        # All mandatory files should have checksums
        missing_sha = conn.execute(
            "SELECT COUNT(*) FROM raw_build_manifest "
            "WHERE build_run_id = ? AND sha256 IS NULL",
            (build_run_id,)
        ).fetchone()[0]
        if missing_sha > 0:
            results.append(CheckResult(
                "manifest.checksums", "WARN", str(missing_sha),
                f"{missing_sha} files in manifest have no checksum (files missing?)"))
        else:
            results.append(CheckResult(
                "manifest.checksums", "PASS", "0",
                "All manifest files have checksums"))

    except Exception as e:
        results.append(CheckResult("manifest.checks", "FAIL", "ERROR", str(e)))

    return results
