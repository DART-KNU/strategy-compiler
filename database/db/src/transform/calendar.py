"""
Build core_calendar.

The trading calendar is derived from dates present in raw_dg_stock_daily.
DataGuide only outputs data for trading days (비영업일 제외 = excluding non-business days),
so any date in that table is by definition a Korean market trading day.

Coverage: whatever date range appears in raw_dg_stock_daily
  (expected: 2020-12-30 to 2026-03-20 based on DataGuide extract).

Each row records:
  - trade_date (ISO-8601)
  - is_open (always 1 for dates derived this way)
  - prev_open_date
  - next_open_date
  - week_id (ISO week: YYYY-Www)
  - month_id (YYYY-MM)
"""

import logging
import sqlite3

from src.db import truncate_table, insert_batch
from src.utils.calendar_utils import iso_week_id, month_id, build_prev_next_maps

logger = logging.getLogger(__name__)


def build_calendar(conn: sqlite3.Connection) -> int:
    """
    Build core_calendar from the distinct dates in raw_dg_stock_daily.
    Returns number of rows inserted.
    """
    logger.info("Extracting distinct trade dates from raw_dg_stock_daily ...")

    # Get all distinct trade dates (sorted ascending)
    cur = conn.execute(
        "SELECT DISTINCT trade_date FROM raw_dg_stock_daily ORDER BY trade_date"
    )
    all_dates = [row[0] for row in cur.fetchall()]

    if not all_dates:
        logger.warning("No trade dates found in raw_dg_stock_daily - calendar will be empty")
        return 0

    logger.info("Found %d distinct trading days: %s to %s",
                len(all_dates), all_dates[0], all_dates[-1])

    prev_map, next_map = build_prev_next_maps(all_dates)

    truncate_table(conn, "core_calendar")

    cols = ["trade_date", "is_open", "prev_open_date", "next_open_date", "week_id", "month_id"]
    rows = []

    for d in all_dates:
        rows.append((
            d,
            1,
            prev_map[d],
            next_map[d],
            iso_week_id(d),
            month_id(d),
        ))

    n = insert_batch(conn, "core_calendar", rows, cols)
    logger.info("Built core_calendar: %d trading days", n)
    return n
