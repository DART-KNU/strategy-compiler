"""
Build core_regulatory_status_interval and core_sector_map.

core_regulatory_status_interval:
  Converts event-based regulatory flags to [interval_start, interval_end] rows
  for efficient PIT lookup.

  status_type values:
    caution  - 투자주의종목 (single-day: no removal date in source)
    warning  - 투자경고종목 (has removal date or still active)
    risk     - 투자위험종목 (has removal date or still active)
    admin    - 관리감리 flag from DataGuide price data
    halt     - 거래정지 flag from DataGuide price data

  For halt/admin: derived from daily flags in core_price_daily.
  A run of consecutive days with flag > 0 is merged into one interval.

  For caution (no removal_date in source):
    interval_start = designation_date
    interval_end   = designation_date  (single-day designation)
    Rationale: Korean investment caution designations are typically 1 trading-day events.

  For warning/risk (has removal_date or NULL for still-active):
    interval_start = designation_date
    interval_end   = removal_date if present, else '9999-12-31' (still active)

core_sector_map:
  Normalize tickers from raw_sector_map and populate core_sector_map.
"""

import logging
import sqlite3

from src.db import truncate_table, insert_batch
from src.utils.ticker import normalize_ticker

logger = logging.getLogger(__name__)

_ACTIVE_END = "9999-12-31"  # sentinel for still-active status


def build_regulatory_status_intervals(conn: sqlite3.Connection) -> int:
    """
    Build core_regulatory_status_interval from raw KIND regulatory tables
    and from DataGuide halt/admin flags in core_price_daily.

    Returns total rows inserted.
    """
    truncate_table(conn, "core_regulatory_status_interval")
    total = 0

    total += _build_caution_intervals(conn)
    total += _build_warning_intervals(conn)
    total += _build_risk_intervals(conn)
    total += _build_halt_admin_intervals(conn)

    logger.info("Built core_regulatory_status_interval: %d total rows", total)
    return total


def _build_caution_intervals(conn: sqlite3.Connection) -> int:
    """Investment caution: single-day interval (no removal date in source)."""
    rows_raw = conn.execute(
        "SELECT raw_ticker, designation_date, caution_type FROM raw_kind_investment_caution"
    ).fetchall()

    cols = ["ticker", "status_type", "interval_start", "interval_end", "source_detail"]
    rows = []
    for r in rows_raw:
        ticker = normalize_ticker(r["raw_ticker"])
        if ticker is None:
            continue
        ddate = r["designation_date"]
        if not ddate:
            continue
        rows.append((ticker, "caution", ddate, ddate, r["caution_type"] or ""))

    n = insert_batch(conn, "core_regulatory_status_interval", rows, cols, mode="INSERT OR REPLACE")
    logger.info("  Caution intervals: %d rows", n)
    return n


def _build_warning_intervals(conn: sqlite3.Connection) -> int:
    """Investment warning: interval from designation_date to removal_date (or still active)."""
    rows_raw = conn.execute(
        "SELECT raw_ticker, designation_date, removal_date FROM raw_kind_investment_warning"
    ).fetchall()

    cols = ["ticker", "status_type", "interval_start", "interval_end", "source_detail"]
    rows = []
    for r in rows_raw:
        ticker = normalize_ticker(r["raw_ticker"])
        if ticker is None:
            continue
        ddate = r["designation_date"]
        if not ddate:
            continue
        end = r["removal_date"] if r["removal_date"] else _ACTIVE_END
        rows.append((ticker, "warning", ddate, end, "KIND warning designation"))

    n = insert_batch(conn, "core_regulatory_status_interval", rows, cols, mode="INSERT OR REPLACE")
    logger.info("  Warning intervals: %d rows", n)
    return n


def _build_risk_intervals(conn: sqlite3.Connection) -> int:
    """Investment risk: interval from designation_date to removal_date (or still active)."""
    rows_raw = conn.execute(
        "SELECT raw_ticker, designation_date, removal_date FROM raw_kind_investment_risk"
    ).fetchall()

    cols = ["ticker", "status_type", "interval_start", "interval_end", "source_detail"]
    rows = []
    for r in rows_raw:
        ticker = normalize_ticker(r["raw_ticker"])
        if ticker is None:
            continue
        ddate = r["designation_date"]
        if not ddate:
            continue
        end = r["removal_date"] if r["removal_date"] else _ACTIVE_END
        rows.append((ticker, "risk", ddate, end, "KIND risk designation"))

    n = insert_batch(conn, "core_regulatory_status_interval", rows, cols, mode="INSERT OR REPLACE")
    logger.info("  Risk intervals: %d rows", n)
    return n


def _build_halt_admin_intervals(conn: sqlite3.Connection) -> int:
    """
    Derive trading halt and admin supervision intervals from core_price_daily flags.

    Consecutive days with flag != 0 (and not NULL) are merged into one interval.
    This is a run-length-encoding approach.
    """
    total = 0

    for flag_col, status_type in [
        ("trading_halt_flag",       "halt"),
        ("admin_supervision_flag",  "admin"),
    ]:
        flagged_rows = conn.execute(
            f"""
            SELECT ticker, trade_date, {flag_col} AS flag_val
            FROM core_price_daily
            WHERE {flag_col} IS NOT NULL AND {flag_col} != 0
            ORDER BY ticker, trade_date
            """
        ).fetchall()

        if not flagged_rows:
            logger.info("  %s intervals: 0 rows (no flagged days in price data)", status_type)
            continue

        # Merge consecutive days into intervals
        intervals = _merge_consecutive_intervals(flagged_rows)

        cols = ["ticker", "status_type", "interval_start", "interval_end", "source_detail"]
        rows = [
            (ticker, status_type, start, end, f"DataGuide {flag_col}={flag_val}")
            for ticker, start, end, flag_val in intervals
        ]

        n = insert_batch(conn, "core_regulatory_status_interval", rows, cols,
                         mode="INSERT OR REPLACE")
        logger.info("  %s intervals: %d rows (merged from %d flagged days)",
                    status_type, n, len(flagged_rows))
        total += n

    return total


def _merge_consecutive_intervals(rows) -> list[tuple]:
    """
    Merge consecutive (ticker, date) flag rows into (ticker, start, end, flag_val) intervals.
    Gaps in trading days break the interval (we use calendar-day adjacency as a proxy).
    """
    # rows is sorted by ticker, trade_date
    intervals = []
    if not rows:
        return intervals

    import datetime

    cur_ticker     = rows[0]["ticker"]
    cur_start      = rows[0]["trade_date"]
    cur_end        = rows[0]["trade_date"]
    cur_flag       = rows[0]["flag_val"]

    for r in rows[1:]:
        ticker = r["ticker"]
        date   = r["trade_date"]
        flag   = r["flag_val"]

        if ticker != cur_ticker:
            intervals.append((cur_ticker, cur_start, cur_end, cur_flag))
            cur_ticker = ticker
            cur_start  = date
            cur_end    = date
            cur_flag   = flag
            continue

        # Check if date is within 7 calendar days of cur_end
        # (allows weekends and 1-2 holiday gaps without breaking the interval)
        prev = datetime.date.fromisoformat(cur_end)
        curr = datetime.date.fromisoformat(date)
        gap_days = (curr - prev).days

        if gap_days <= 7:
            cur_end  = date
            cur_flag = flag  # update flag value (usually same, but use latest)
        else:
            intervals.append((cur_ticker, cur_start, cur_end, cur_flag))
            cur_start = date
            cur_end   = date
            cur_flag  = flag

    intervals.append((cur_ticker, cur_start, cur_end, cur_flag))
    return intervals


def build_sector_map(conn: sqlite3.Connection) -> int:
    """
    Build core_sector_map by normalizing tickers from raw_sector_map.

    Uses canonical 6-digit ticker as the join key.
    Company name (코드명) is NOT used as a join key.
    """
    rows_raw = conn.execute(
        "SELECT raw_ticker, sector_name, sector_code, confidence, fill_method "
        "FROM raw_sector_map"
    ).fetchall()

    truncate_table(conn, "core_sector_map")
    cols = ["ticker", "sector_name", "sector_code", "confidence", "source"]

    rows = []
    rejected = 0
    for r in rows_raw:
        ticker = normalize_ticker(r["raw_ticker"])
        if ticker is None:
            rejected += 1
            continue
        rows.append((
            ticker,
            r["sector_name"],
            r["sector_code"],
            r["confidence"],
            r["fill_method"] or "sector_file",
        ))

    n = insert_batch(conn, "core_sector_map", rows, cols)
    logger.info("Built core_sector_map: %d rows (rejected %d non-normalizable tickers)",
                n, rejected)
    return n
