"""
Build core_price_daily and core_index_daily.

Transforms raw long-format DataGuide data into wide-format tables.

core_price_daily:
  Source: raw_dg_stock_daily
  Pivot: (raw_ticker, item_name, trade_date, value) -> (ticker, trade_date, open, high, ...)
  Ticker normalization: remove 'A' prefix

core_index_daily:
  Source: raw_dg_index_daily
  Map index codes: I.001->KOSPI, I.101->KOSPI200, I.201->KOSDAQ, I.405->KRX300
"""

import logging
import sqlite3

from src.db import truncate_table, insert_batch
from src.utils.ticker import normalize_ticker

logger = logging.getLogger(__name__)

# DataGuide item names to column names mapping
STOCK_ITEM_TO_COL = {
    "시가(원)":           "open",
    "고가(원)":           "high",
    "저가(원)":           "low",
    "종가(원)":           "close",
    "수정시가(원)":       "adj_open",
    "수정고가(원)":       "adj_high",
    "수정저가(원)":       "adj_low",
    "수정주가(원)":       "adj_close",
    "수정계수":           "adj_factor",
    "거래량(주)":         "volume",
    "거래대금(원)":       "traded_value",
    "상장주식수(주)":     "shares_outstanding",
    "시가총액(원)":       "market_cap",
    "거래정지구분":       "trading_halt_flag",
    "관리감리구분":       "admin_supervision_flag",
    "유동주식수(주)":     "float_shares",
    "유동주식비율(%)":    "float_ratio",
}

INDEX_ITEM_TO_COL = {
    "시가지수(포인트)":   "open",
    "고가지수(포인트)":   "high",
    "저가지수(포인트)":   "low",
    "종가지수(포인트)":   "close",
}

INDEX_CODE_MAP = {
    "I.001": "KOSPI",
    "I.101": "KOSPI200",
    "I.201": "KOSDAQ",
    "I.405": "KRX300",
}

_PRICE_COLS = ["trade_date", "ticker", "open", "high", "low", "close",
               "adj_open", "adj_high", "adj_low", "adj_close", "adj_factor",
               "volume", "traded_value", "shares_outstanding", "market_cap",
               "trading_halt_flag", "admin_supervision_flag", "float_shares", "float_ratio"]


def build_price_daily(conn: sqlite3.Connection, batch_size: int = 5000) -> int:
    """
    Build core_price_daily by pivoting raw_dg_stock_daily.

    Uses SQL pivot (one pass per item) to keep memory footprint low.
    """
    logger.info("Building core_price_daily from raw_dg_stock_daily ...")
    truncate_table(conn, "core_price_daily")

    # Get all distinct (raw_ticker, trade_date) pairs with their item values
    # We do this via a SQL pivot using CASE WHEN MAX() pattern

    # Map item names to column names (filter to those in STOCK_ITEM_TO_COL)
    item_clauses = ", ".join(
        f"MAX(CASE WHEN item_name = '{ko}' THEN value END) AS {en}"
        for ko, en in STOCK_ITEM_TO_COL.items()
    )

    pivot_sql = f"""
        INSERT OR REPLACE INTO core_price_daily
            (trade_date, ticker, {', '.join(list(STOCK_ITEM_TO_COL.values()))})
        SELECT
            trade_date,
            raw_ticker AS ticker,
            {item_clauses}
        FROM raw_dg_stock_daily
        WHERE item_name IN ({','.join('?' * len(STOCK_ITEM_TO_COL))})
        GROUP BY raw_ticker, trade_date
        HAVING MAX(CASE WHEN item_name = '종가(원)' THEN value END) IS NOT NULL
    """

    item_filter_values = list(STOCK_ITEM_TO_COL.keys())

    logger.info("  Running SQL pivot for core_price_daily (this may take a few minutes) ...")
    with conn:
        conn.execute(pivot_sql, item_filter_values)

    # Normalize tickers: update raw tickers (A-prefixed) to canonical form
    # The raw table has A005930 style; we stored it as-is in the pivot
    # Now we need to normalize: remove leading 'A'
    logger.info("  Normalizing ticker codes in core_price_daily ...")
    _normalize_tickers_in_price_table(conn)

    n = conn.execute("SELECT COUNT(*) FROM core_price_daily").fetchone()[0]
    logger.info("Built core_price_daily: %d rows", n)
    return n


def _normalize_tickers_in_price_table(conn: sqlite3.Connection) -> None:
    """
    Remove 'A' prefix from tickers in core_price_daily.
    e.g., 'A005930' -> '005930'
    """
    # Check how many rows need updating
    cur = conn.execute(
        "SELECT COUNT(*) FROM core_price_daily WHERE ticker LIKE 'A%'"
    )
    count_a = cur.fetchone()[0]

    if count_a == 0:
        return

    logger.info("  Normalizing %d A-prefixed tickers in core_price_daily ...", count_a)

    # Update: strip leading 'A' where ticker starts with 'A' and is 7 chars
    with conn:
        conn.execute(
            """
            UPDATE core_price_daily
            SET ticker = SUBSTR(ticker, 2)
            WHERE ticker GLOB 'A[0-9][0-9][0-9][0-9][0-9][0-9]'
            """
        )

    # Verify
    remaining = conn.execute(
        "SELECT COUNT(*) FROM core_price_daily WHERE ticker LIKE 'A%'"
    ).fetchone()[0]

    if remaining > 0:
        logger.warning(
            "  %d tickers still have 'A' prefix after normalization "
            "(may be non-standard codes like A000000 for special securities)",
            remaining
        )


def build_index_daily(conn: sqlite3.Connection) -> int:
    """Build core_index_daily from raw_dg_index_daily."""
    logger.info("Building core_index_daily ...")
    truncate_table(conn, "core_index_daily")

    item_clauses = ", ".join(
        f"MAX(CASE WHEN item_name = '{ko}' THEN value END) AS {en}"
        for ko, en in INDEX_ITEM_TO_COL.items()
    )

    # Build CASE WHEN for index_code mapping
    code_case = " ".join(
        f"WHEN raw_code = '{raw}' THEN '{canon}'"
        for raw, canon in INDEX_CODE_MAP.items()
    )

    pivot_sql = f"""
        INSERT OR REPLACE INTO core_index_daily
            (trade_date, index_code, open, high, low, close)
        SELECT
            trade_date,
            CASE {code_case} ELSE raw_code END AS index_code,
            {item_clauses}
        FROM raw_dg_index_daily
        GROUP BY raw_code, trade_date
    """

    with conn:
        conn.execute(pivot_sql)

    n = conn.execute("SELECT COUNT(*) FROM core_index_daily").fetchone()[0]
    logger.info("Built core_index_daily: %d rows", n)
    return n
