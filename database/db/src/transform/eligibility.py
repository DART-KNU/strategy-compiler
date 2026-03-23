"""
Build mart_liquidity_daily and mart_universe_eligibility_daily.

Eligibility rules (from contest spec):
  Base universe: KOSPI + KOSDAQ common equity only

  BUY-INELIGIBLE if ANY of:
    1. 5-day average traded value <= 3,000,000,000 KRW
    2. Newly listed or relisted with < 6 business days since listing
    3. Investment caution designation (from KIND)
    4. Investment warning designation (from KIND)
    5. Investment risk designation (from KIND)
    6. Admin/management issue (관리감리 flag from DataGuide)
    7. Trading halt (거래정지 flag from DataGuide)
    8. Market cap < 100,000,000,000 KRW

  EXCLUDED RULE (missing data):
    투자주의환기종목 (investment caution watchlist) is NOT implemented.
    TODO: Add is_not_caution_watchlist flag when dataset becomes available.

  block_reason_mask bit assignments:
    bit 0:  not_listed         (0x001)
    bit 1:  not_common_equity  (0x002)
    bit 2:  wrong_market       (0x004)
    bit 3:  too_new            (0x008)
    bit 4:  low_liquidity      (0x010)
    bit 5:  small_mcap         (0x020)
    bit 6:  caution            (0x040)
    bit 7:  warning            (0x080)
    bit 8:  risk               (0x100)
    bit 9:  admin              (0x200)
    bit 10: halt               (0x400)
"""

import bisect
import json
import logging
import sqlite3

from src.db import truncate_table, insert_batch

logger = logging.getLogger(__name__)

# Block reason bitmasks
BIT_NOT_LISTED        = 0x001
BIT_NOT_COMMON_EQUITY = 0x002
BIT_WRONG_MARKET      = 0x004
BIT_TOO_NEW           = 0x008
BIT_LOW_LIQUIDITY     = 0x010
BIT_SMALL_MCAP        = 0x020
BIT_CAUTION           = 0x040
BIT_WARNING           = 0x080
BIT_RISK              = 0x100
BIT_ADMIN             = 0x200
BIT_HALT              = 0x400

BIT_LABELS = {
    BIT_NOT_LISTED:        "not_listed",
    BIT_NOT_COMMON_EQUITY: "not_common_equity",
    BIT_WRONG_MARKET:      "wrong_market",
    BIT_TOO_NEW:           "too_new_listing",
    BIT_LOW_LIQUIDITY:     "low_adv5",
    BIT_SMALL_MCAP:        "small_mcap",
    BIT_CAUTION:           "investment_caution",
    BIT_WARNING:           "investment_warning",
    BIT_RISK:              "investment_risk",
    BIT_ADMIN:             "admin_supervision",
    BIT_HALT:              "trading_halt",
}

ELIGIBLE_MARKETS = {"코스피", "코스닥"}


def build_liquidity_daily(
    conn: sqlite3.Connection,
    min_adv5: float = 3e9,
    min_mcap: float = 1e11,
) -> int:
    """
    Build mart_liquidity_daily with ADV5, ADV20, listing age, and thresholds.

    Uses SQLite window functions (ROWS BETWEEN N PRECEDING AND CURRENT ROW)
    for rolling averages. Requires SQLite >= 3.25 (released 2018).

    Listing age is computed in Python for memory efficiency: for each ticker,
    count the number of trading days from listing_date up to and including
    trade_date. This avoids a massive cross-join in SQL.
    """
    logger.info("Building mart_liquidity_daily ...")
    truncate_table(conn, "mart_liquidity_daily")

    # ----------------------------------------------------------------
    # Step 1: Compute ADV5, ADV20 using SQL window functions
    # ----------------------------------------------------------------
    logger.info("  Computing rolling traded-value averages via window functions ...")

    conn.execute("DROP TABLE IF EXISTS _tmp_rolling")
    conn.execute("""
        CREATE TEMP TABLE _tmp_rolling AS
        SELECT
            trade_date,
            ticker,
            market_cap,
            AVG(traded_value) OVER (
                PARTITION BY ticker ORDER BY trade_date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS adv5,
            AVG(traded_value) OVER (
                PARTITION BY ticker ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS adv20
        FROM core_price_daily
    """)

    # ----------------------------------------------------------------
    # Step 2: Compute listing_age_bd in Python
    # ----------------------------------------------------------------
    logger.info("  Computing listing age in business days ...")

    # Get all trading dates as an ordered set
    cal_dates = [
        row[0] for row in conn.execute(
            "SELECT trade_date FROM core_calendar WHERE is_open = 1 ORDER BY trade_date"
        )
    ]
    cal_date_rank = {d: i for i, d in enumerate(cal_dates)}  # date -> 0-based rank

    # Get listing dates for all tickers
    listing_dates = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT ticker, listing_date FROM core_security_master "
            "WHERE listing_date IS NOT NULL"
        )
    }

    # Build a lookup: (ticker, trade_date) -> listing_age_bd
    # listing_age_bd = rank(trade_date) - rank(first_trading_day_on_or_after_listing_date)
    # If listing_date is before our calendar starts, age = rank(trade_date) (effectively old stock)

    def listing_rank(listing_date_str: str) -> int:
        """Find index of first trading day on or after listing_date."""
        idx = bisect.bisect_left(cal_dates, listing_date_str)
        return idx  # index into cal_dates

    # Build listing rank map
    listing_rank_map = {}
    for ticker, ldate in listing_dates.items():
        listing_rank_map[ticker] = listing_rank(ldate)

    # ----------------------------------------------------------------
    # Step 3: Fetch rolling data and compute listing age, then insert
    # ----------------------------------------------------------------
    logger.info("  Building final liquidity table ...")
    cols = ["trade_date", "ticker", "adv5", "adv20", "market_cap",
            "listing_age_bd", "is_above_3bn_adv5", "is_above_100bn_mcap"]

    rows_raw = conn.execute(
        "SELECT trade_date, ticker, adv5, adv20, market_cap FROM _tmp_rolling"
    ).fetchall()

    batch = []
    batch_size = 50000
    total = 0

    for r in rows_raw:
        trade_date = r[0]
        ticker     = r[1]
        adv5       = r[2]
        adv20      = r[3]
        mcap       = r[4]

        # Listing age in business days
        trade_rank = cal_date_rank.get(trade_date)
        if trade_rank is not None and ticker in listing_rank_map:
            age = max(0, trade_rank - listing_rank_map[ticker])
        elif trade_rank is not None and ticker not in listing_rank_map:
            # No listing date known -> treat as established stock (very old)
            age = 9999
        else:
            age = 9999

        is_adv5 = 1 if (adv5 is not None and adv5 > min_adv5) else 0
        is_mcap = 1 if (mcap is not None and mcap >= min_mcap) else 0

        batch.append((trade_date, ticker, adv5, adv20, mcap, age, is_adv5, is_mcap))

        if len(batch) >= batch_size:
            insert_batch(conn, "mart_liquidity_daily", batch, cols)
            total += len(batch)
            batch = []

    if batch:
        insert_batch(conn, "mart_liquidity_daily", batch, cols)
        total += len(batch)

    conn.execute("DROP TABLE IF EXISTS _tmp_rolling")

    n = conn.execute("SELECT COUNT(*) FROM mart_liquidity_daily").fetchone()[0]
    logger.info("Built mart_liquidity_daily: %d rows", n)
    return n


def build_universe_eligibility(
    conn: sqlite3.Connection,
    min_adv5: float = 3e9,
    min_mcap: float = 1e11,
    min_listing_age_bd: int = 6,
    eligible_markets: set | None = None,
) -> int:
    """
    Build mart_universe_eligibility_daily.

    For each (trade_date, ticker) in core_price_daily:
    - Compute each eligibility flag
    - Aggregate to is_eligible
    - Produce block_reason_mask and block_reason_json
    """
    if eligible_markets is None:
        eligible_markets = ELIGIBLE_MARKETS

    logger.info("Building mart_universe_eligibility_daily ...")
    truncate_table(conn, "mart_universe_eligibility_daily")

    # ----------------------------------------------------------------
    # Pull all the data we need for eligibility computation
    # ----------------------------------------------------------------
    logger.info("  Loading price + security + liquidity + regulatory data ...")

    # Get all ticker × date combinations (from price data)
    price_rows = conn.execute(
        "SELECT trade_date, ticker FROM core_price_daily ORDER BY trade_date, ticker"
    ).fetchall()

    # Security master lookup
    sec_map = {}
    for r in conn.execute(
        "SELECT ticker, is_common_equity, market_type, listing_date, delisting_date "
        "FROM core_security_master"
    ):
        sec_map[r["ticker"]] = {
            "is_common_equity": r["is_common_equity"],
            "market_type":      r["market_type"],
            "listing_date":     r["listing_date"],
            "delisting_date":   r["delisting_date"],
        }

    # Liquidity lookup
    liq_map = {}
    for r in conn.execute(
        "SELECT trade_date, ticker, adv5, market_cap, listing_age_bd, "
        "is_above_3bn_adv5, is_above_100bn_mcap FROM mart_liquidity_daily"
    ):
        liq_map[(r["trade_date"], r["ticker"])] = {
            "adv5":               r["adv5"],
            "market_cap":         r["market_cap"],
            "listing_age_bd":     r["listing_age_bd"],
            "is_above_3bn_adv5":  r["is_above_3bn_adv5"],
            "is_above_100bn_mcap": r["is_above_100bn_mcap"],
        }

    # Regulatory status lookup: set of (ticker, date) for each status type
    logger.info("  Building regulatory status lookup ...")
    caution_set = _build_status_set(conn, "caution")
    warning_set = _build_status_set(conn, "warning")
    risk_set    = _build_status_set(conn, "risk")
    admin_set   = _build_status_set(conn, "admin")
    halt_set    = _build_status_set(conn, "halt")

    logger.info("  Regulatory sets: caution=%d, warning=%d, risk=%d, admin=%d, halt=%d",
                len(caution_set), len(warning_set), len(risk_set), len(admin_set), len(halt_set))

    # ----------------------------------------------------------------
    # Compute eligibility for each (trade_date, ticker)
    # ----------------------------------------------------------------
    cols = [
        "trade_date", "ticker",
        "is_listed", "is_common_equity", "is_market_ok",
        "is_listing_age_ok", "is_liquidity_ok", "is_mcap_ok",
        "is_not_caution", "is_not_warning", "is_not_risk",
        "is_not_admin", "is_not_halt",
        "is_eligible", "block_reason_mask", "block_reason_json",
    ]

    batch = []
    total = 0
    batch_size = 50000

    for r in price_rows:
        trade_date = r["trade_date"]
        ticker     = r["ticker"]

        sec = sec_map.get(ticker, {})
        liq = liq_map.get((trade_date, ticker), {})

        # --- is_listed ---
        listing_date   = sec.get("listing_date")
        delisting_date = sec.get("delisting_date")
        if listing_date and trade_date < listing_date:
            is_listed = 0
        elif delisting_date and trade_date >= delisting_date:
            is_listed = 0
        elif not sec:
            # In price data but not in security master (e.g., non-standard ticker)
            is_listed = 0
        else:
            is_listed = 1

        # --- is_common_equity ---
        is_common_equity = sec.get("is_common_equity", 0)

        # --- is_market_ok ---
        mt = sec.get("market_type", "UNKNOWN")
        is_market_ok = 1 if mt in eligible_markets else 0

        # --- is_listing_age_ok ---
        age = liq.get("listing_age_bd", 0)
        is_listing_age_ok = 1 if (age is not None and age >= min_listing_age_bd) else 0

        # --- is_liquidity_ok ---
        is_liquidity_ok = liq.get("is_above_3bn_adv5", 0) or 0

        # --- is_mcap_ok ---
        is_mcap_ok = liq.get("is_above_100bn_mcap", 0) or 0

        # --- regulatory flags ---
        is_not_caution = 0 if (ticker, trade_date) in caution_set else 1
        is_not_warning = 0 if (ticker, trade_date) in warning_set else 1
        is_not_risk    = 0 if (ticker, trade_date) in risk_set    else 1
        is_not_admin   = 0 if (ticker, trade_date) in admin_set   else 1
        is_not_halt    = 0 if (ticker, trade_date) in halt_set    else 1

        # --- aggregate ---
        is_eligible = int(
            is_listed and is_common_equity and is_market_ok and
            is_listing_age_ok and is_liquidity_ok and is_mcap_ok and
            is_not_caution and is_not_warning and is_not_risk and
            is_not_admin and is_not_halt
        )

        # --- block reason mask & JSON ---
        mask = 0
        blocks = []
        if not is_listed:
            mask |= BIT_NOT_LISTED;        blocks.append(BIT_LABELS[BIT_NOT_LISTED])
        if not is_common_equity:
            mask |= BIT_NOT_COMMON_EQUITY; blocks.append(BIT_LABELS[BIT_NOT_COMMON_EQUITY])
        if not is_market_ok:
            mask |= BIT_WRONG_MARKET;      blocks.append(BIT_LABELS[BIT_WRONG_MARKET])
        if not is_listing_age_ok:
            mask |= BIT_TOO_NEW;           blocks.append(BIT_LABELS[BIT_TOO_NEW])
        if not is_liquidity_ok:
            mask |= BIT_LOW_LIQUIDITY;     blocks.append(BIT_LABELS[BIT_LOW_LIQUIDITY])
        if not is_mcap_ok:
            mask |= BIT_SMALL_MCAP;        blocks.append(BIT_LABELS[BIT_SMALL_MCAP])
        if not is_not_caution:
            mask |= BIT_CAUTION;           blocks.append(BIT_LABELS[BIT_CAUTION])
        if not is_not_warning:
            mask |= BIT_WARNING;           blocks.append(BIT_LABELS[BIT_WARNING])
        if not is_not_risk:
            mask |= BIT_RISK;              blocks.append(BIT_LABELS[BIT_RISK])
        if not is_not_admin:
            mask |= BIT_ADMIN;             blocks.append(BIT_LABELS[BIT_ADMIN])
        if not is_not_halt:
            mask |= BIT_HALT;              blocks.append(BIT_LABELS[BIT_HALT])

        block_json = json.dumps({"blocks": blocks}, ensure_ascii=False) if blocks else None

        batch.append((
            trade_date, ticker,
            is_listed, is_common_equity, is_market_ok,
            is_listing_age_ok, is_liquidity_ok, is_mcap_ok,
            is_not_caution, is_not_warning, is_not_risk,
            is_not_admin, is_not_halt,
            is_eligible, mask, block_json,
        ))

        if len(batch) >= batch_size:
            insert_batch(conn, "mart_universe_eligibility_daily", batch, cols)
            total += len(batch)
            if total % 500000 == 0:
                logger.info("  ... %d eligibility rows computed", total)
            batch = []

    if batch:
        insert_batch(conn, "mart_universe_eligibility_daily", batch, cols)
        total += len(batch)

    n = conn.execute("SELECT COUNT(*) FROM mart_universe_eligibility_daily").fetchone()[0]
    logger.info("Built mart_universe_eligibility_daily: %d rows", n)
    return n


def build_sector_weight_snapshot(conn: sqlite3.Connection) -> int:
    """
    Build mart_sector_weight_snapshot.

    Approximated from aggregate market_cap of eligible stocks per sector.
    is_approximated = 1 indicates this is derived, not from an official source.
    """
    logger.info("Building mart_sector_weight_snapshot ...")
    truncate_table(conn, "mart_sector_weight_snapshot")

    insert_sql = """
        INSERT OR REPLACE INTO mart_sector_weight_snapshot
        (trade_date, sector_name, total_market_cap, constituent_count, sector_weight, is_approximated)
        WITH eligible_mcap AS (
            SELECT
                e.trade_date,
                sm.sector_name,
                SUM(p.market_cap) AS sector_mcap,
                COUNT(*) AS cnt
            FROM mart_universe_eligibility_daily e
            JOIN core_price_daily p ON e.trade_date = p.trade_date AND e.ticker = p.ticker
            JOIN core_sector_map sm ON e.ticker = sm.ticker
            WHERE e.is_eligible = 1
              AND p.market_cap IS NOT NULL
            GROUP BY e.trade_date, sm.sector_name
        ),
        total_mcap AS (
            SELECT trade_date, SUM(sector_mcap) AS total
            FROM eligible_mcap
            GROUP BY trade_date
        )
        SELECT
            em.trade_date,
            em.sector_name,
            em.sector_mcap AS total_market_cap,
            em.cnt AS constituent_count,
            ROUND(em.sector_mcap * 1.0 / NULLIF(tm.total, 0), 6) AS sector_weight,
            1 AS is_approximated
        FROM eligible_mcap em
        JOIN total_mcap tm ON em.trade_date = tm.trade_date
    """

    with conn:
        conn.execute(insert_sql)

    n = conn.execute("SELECT COUNT(*) FROM mart_sector_weight_snapshot").fetchone()[0]
    logger.info("Built mart_sector_weight_snapshot: %d rows", n)
    return n


def _build_status_set(conn: sqlite3.Connection, status_type: str) -> set:
    """
    Build a set of (ticker, trade_date) pairs where the stock has the given status.

    Expands intervals to individual dates using core_calendar for efficiency.
    For large datasets, this set can be large (but typically manageable).
    """
    rows = conn.execute(
        """
        SELECT i.ticker, c.trade_date
        FROM core_regulatory_status_interval i
        JOIN core_calendar c
            ON c.trade_date >= i.interval_start
           AND c.trade_date <= i.interval_end
        WHERE i.status_type = ?
          AND c.is_open = 1
        """,
        (status_type,)
    ).fetchall()

    return {(r["ticker"], r["trade_date"]) for r in rows}
