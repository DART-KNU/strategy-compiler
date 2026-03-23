"""
Build core_security_master.

Combines:
  1. raw_kind_ipos          - listing history (listing_date, listing_type, security_type)
  2. raw_kind_delistings    - delisting history (delisting_date)
  3. raw_kind_listed_companies_current - current market_type, fiscal_month

Logic:
  - Primary source for ticker universe: raw_kind_ipos (most complete history)
  - Supplemented by raw_kind_listed_companies_current for market_type
  - Supplemented by raw_kind_delistings for delisting_date
  - For market_type of delisted stocks not in current list: UNKNOWN

Ticker normalization:
  All source tickers are stored as-is in raw tables.
  core_security_master uses canonical 6-digit tickers.
  Tickers that cannot be normalized are logged and excluded.

is_common_equity:
  True (1) if security_type = '주권' in IPO file.
  For stocks in current list but not in IPO history, assume common equity
  if market_type is 코스피/코스닥 (conservative: may overcount).

market_type:
  Preferred source: raw_kind_listed_companies_current (current snapshot).
  For delisted tickers: UNKNOWN if not found in current list.
  This is a known limitation documented in source_notes.

NOTE on stock_issuance:
  v1 stores raw_kind_stock_issuance but does NOT transform it into
  corporate action adjustments. Full corporate action chain reconstruction
  is deferred to a future version. See source_notes.
"""

import logging
import sqlite3

from src.db import truncate_table, insert_batch
from src.utils.ticker import normalize_ticker

logger = logging.getLogger(__name__)


def build_security_master(conn: sqlite3.Connection) -> int:
    """
    Build core_security_master from raw KIND tables.
    Returns number of rows inserted.
    """
    # ----------------------------------------------------------------
    # Step 1: Build a base universe from IPO history
    # ----------------------------------------------------------------
    ipos_sql = """
        SELECT
            raw_ticker,
            corp_name,
            listing_date,
            listing_type,
            security_type,
            industry,
            MIN(listing_date) AS earliest_listing
        FROM raw_kind_ipos
        WHERE raw_ticker IS NOT NULL
          AND listing_date IS NOT NULL
        GROUP BY raw_ticker
    """
    ipos_rows = conn.execute(ipos_sql).fetchall()
    logger.info("IPO history rows (grouped by ticker): %d", len(ipos_rows))

    # ----------------------------------------------------------------
    # Step 2: Build lookup: ticker -> market_type from current list
    # ----------------------------------------------------------------
    market_type_map = {}  # raw_ticker -> market_type
    fiscal_month_map = {}
    for row in conn.execute(
        "SELECT raw_ticker, market_type, fiscal_month FROM raw_kind_listed_companies_current"
    ):
        market_type_map[row["raw_ticker"]] = row["market_type"]
        fiscal_month_map[row["raw_ticker"]] = row["fiscal_month"]

    # ----------------------------------------------------------------
    # Step 3: Build lookup: ticker -> delisting_date
    # ----------------------------------------------------------------
    delisting_map = {}  # raw_ticker -> (delisting_date, delisting_reason)
    for row in conn.execute(
        "SELECT raw_ticker, delisting_date, delisting_reason FROM raw_kind_delistings"
    ):
        # If multiple delisting events, keep most recent
        raw_t = row["raw_ticker"]
        if raw_t not in delisting_map or row["delisting_date"] > delisting_map[raw_t][0]:
            delisting_map[raw_t] = (row["delisting_date"], row["delisting_reason"])

    # ----------------------------------------------------------------
    # Step 4: Build set of currently active tickers
    # ----------------------------------------------------------------
    active_tickers = set(market_type_map.keys())

    # ----------------------------------------------------------------
    # Step 5: Merge and normalize
    # ----------------------------------------------------------------
    # Also include tickers in current list that have no IPO record
    ipos_raw_tickers = {r["raw_ticker"] for r in ipos_rows}
    current_only = active_tickers - ipos_raw_tickers

    all_raw_tickers = {}  # raw_ticker -> record dict
    for row in ipos_rows:
        all_raw_tickers[row["raw_ticker"]] = {
            "raw_ticker":     row["raw_ticker"],
            "corp_name":      row["corp_name"],
            "listing_date":   row["listing_date"],
            "listing_type":   row["listing_type"],
            "security_type":  row["security_type"],
            "industry":       row["industry"],
            "source_notes":   "ipo_history",
        }

    # Add tickers only in current list
    for raw_t in current_only:
        cur_row = conn.execute(
            "SELECT corp_name, listing_date, fiscal_month, industry, market_type "
            "FROM raw_kind_listed_companies_current WHERE raw_ticker=?",
            (raw_t,)
        ).fetchone()
        if cur_row:
            all_raw_tickers[raw_t] = {
                "raw_ticker":    raw_t,
                "corp_name":     cur_row["corp_name"],
                "listing_date":  cur_row["listing_date"],
                "listing_type":  None,
                "security_type": "주권",  # assume common equity for current listed
                "industry":      cur_row["industry"],
                "source_notes":  "current_list_only",
            }

    # ----------------------------------------------------------------
    # Step 6: Build final rows
    # ----------------------------------------------------------------
    truncate_table(conn, "core_security_master")

    cols = ["ticker", "corp_name", "market_type", "security_type", "is_common_equity",
            "listing_date", "delisting_date", "is_active_current", "listing_type",
            "fiscal_month", "industry", "source_notes"]

    rows = []
    rejected = 0

    for raw_t, rec in all_raw_tickers.items():
        ticker = normalize_ticker(raw_t)
        if ticker is None:
            logger.warning("Cannot normalize ticker %r (company=%r) - excluded from security_master",
                           raw_t, rec.get("corp_name"))
            rejected += 1
            continue

        # Market type: prefer current list, then UNKNOWN
        mt = market_type_map.get(raw_t, "UNKNOWN")

        # Delisting
        delisting_info = delisting_map.get(raw_t)
        delisting_date = delisting_info[0] if delisting_info else None

        # Is active (in current listed companies snapshot)
        is_active = 1 if raw_t in active_tickers else 0

        # Is common equity
        sec_type = rec.get("security_type") or ""
        is_common = 1 if sec_type.strip() == "주권" else 0

        # Fiscal month from current list (preferred) or from IPO
        fmon = fiscal_month_map.get(raw_t) or ""

        source_notes = rec.get("source_notes", "")
        if mt == "UNKNOWN":
            source_notes += "; market_type=UNKNOWN (not in current list)"
        if rec.get("listing_type") is None and raw_t in current_only:
            source_notes += "; listing_type unavailable (no IPO record)"
        # NOTE v1 limitation: corporate action history from stock_issuance not applied
        source_notes += "; v1: stock_issuance not fully processed for corporate actions"

        rows.append((
            ticker,
            rec.get("corp_name"),
            mt,
            sec_type,
            is_common,
            rec.get("listing_date"),
            delisting_date,
            is_active,
            rec.get("listing_type"),
            fmon,
            rec.get("industry"),
            source_notes.strip("; "),
        ))

    n = insert_batch(conn, "core_security_master", rows, cols)
    logger.info("Built core_security_master: %d rows (rejected %d non-normalizable tickers)",
                n, rejected)
    return n
