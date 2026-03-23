"""
Build core_financials_quarterly and mart_fundamentals_asof_daily.

core_financials_quarterly:
  Pivot raw_dg_financials_quarterly from long to wide format.
  Compute period_end and available_date (PIT-safe lag policy).

mart_fundamentals_asof_daily:
  For each (ticker, trade_date), look up the most recently AVAILABLE
  quarterly report (where available_date <= trade_date).
  This is the key PIT-safety mechanism for financial data.

Financial lag policy (configurable):
  Q1/Q2/Q3: period_end + 45 calendar days
  Q4:       period_end + 90 calendar days

  Rationale: Korean DART filing deadlines:
    - Quarterly reports (분기보고서): within 45 days of quarter end
    - Annual reports (사업보고서): within 90 days of fiscal year end
  We use calendar days as a conservative approximation
  (actual filing may happen earlier, but we never read data before these dates).

Financial units:
  All values are in 천원 (thousands KRW) as sourced from DataGuide.
  This is documented in column descriptions and meta_field_catalog.
"""

import logging
import sqlite3

from src.db import truncate_table, insert_batch
from src.utils.calendar_utils import quarter_end_date, add_days
from src.utils.ticker import normalize_ticker

logger = logging.getLogger(__name__)

FINANCIAL_ITEM_TO_COL = {
    "자산총계(천원)":                     "total_assets",
    "부채총계(천원)":                     "total_liabilities",
    "자본총계(지배)(천원)":               "total_equity_parent",
    "매출액(천원)":                       "sales",
    "매출원가(천원)":                     "cogs",
    "영업이익(천원)":                     "operating_income",
    "당기순이익(지배)(천원)":             "net_income_parent",
    "영업활동으로인한현금흐름(천원)":     "operating_cash_flow",
    "현금및현금성자산(천원)":             "cash_and_cash_equivalents",
    "*총금융부채(천원)":                  "total_financial_debt",
}


def build_financials_quarterly(
    conn: sqlite3.Connection,
    lag_q1: int = 45,
    lag_q2: int = 45,
    lag_q3: int = 45,
    lag_q4: int = 90,
) -> int:
    """
    Build core_financials_quarterly by pivoting raw_dg_financials_quarterly.

    Computes period_end and available_date for each (ticker, year, quarter).
    Returns number of rows inserted.
    """
    logger.info("Building core_financials_quarterly ...")
    truncate_table(conn, "core_financials_quarterly")

    # Build SQL pivot
    item_clauses = ", ".join(
        f"MAX(CASE WHEN item_name = '{ko}' THEN value END) AS {en}"
        for ko, en in FINANCIAL_ITEM_TO_COL.items()
    )

    pivot_sql = f"""
        SELECT
            raw_ticker,
            fiscal_month,
            report_type,
            year,
            quarter,
            {item_clauses}
        FROM raw_dg_financials_quarterly
        GROUP BY raw_ticker, year, quarter
    """

    rows_raw = conn.execute(pivot_sql).fetchall()
    logger.info("  Raw pivot rows: %d", len(rows_raw))

    cols = ["ticker", "year", "quarter", "fiscal_month", "report_type",
            "period_end", "available_date",
            "total_assets", "total_liabilities", "total_equity_parent",
            "sales", "cogs", "operating_income", "net_income_parent",
            "operating_cash_flow", "cash_and_cash_equivalents", "total_financial_debt"]

    lag_map = {"1Q": lag_q1, "2Q": lag_q2, "3Q": lag_q3, "4Q": lag_q4}

    rows = []
    rejected = 0

    for r in rows_raw:
        ticker = normalize_ticker(r["raw_ticker"])
        if ticker is None:
            rejected += 1
            continue

        year    = r["year"]
        quarter = r["quarter"]

        try:
            period_end = quarter_end_date(year, quarter)
        except (ValueError, TypeError) as e:
            logger.warning("Cannot compute period_end for (%r, %r, %r): %s",
                           r["raw_ticker"], year, quarter, e)
            continue

        lag = lag_map.get(quarter, 45)
        available_date = add_days(period_end, lag)

        rows.append((
            ticker,
            year,
            quarter,
            r["fiscal_month"],
            r["report_type"],
            period_end,
            available_date,
            r["total_assets"],
            r["total_liabilities"],
            r["total_equity_parent"],
            r["sales"],
            r["cogs"],
            r["operating_income"],
            r["net_income_parent"],
            r["operating_cash_flow"],
            r["cash_and_cash_equivalents"],
            r["total_financial_debt"],
        ))

    n = insert_batch(conn, "core_financials_quarterly", rows, cols)
    logger.info("Built core_financials_quarterly: %d rows (rejected %d bad tickers)",
                n, rejected)
    return n


def build_fundamentals_asof_daily(conn: sqlite3.Connection, batch_size: int = 50000) -> int:
    """
    Build mart_fundamentals_asof_daily.

    For each (ticker, trade_date), find the most recent quarterly report
    where available_date <= trade_date. This is PIT-safe.

    Uses a LATERAL / correlated subquery approach in SQLite.
    """
    logger.info("Building mart_fundamentals_asof_daily ...")
    truncate_table(conn, "mart_fundamentals_asof_daily")

    # For each ticker × trade_date, find max available_date <= trade_date
    # Then join to get the financial values
    insert_sql = """
        INSERT OR REPLACE INTO mart_fundamentals_asof_daily
        (
            trade_date, ticker,
            available_year, available_quarter,
            period_end, available_date,
            total_assets, total_liabilities, total_equity_parent,
            sales, cogs, operating_income, net_income_parent,
            operating_cash_flow, cash_and_cash_equivalents, total_financial_debt
        )
        SELECT
            p.trade_date,
            p.ticker,
            f.year              AS available_year,
            f.quarter           AS available_quarter,
            f.period_end,
            f.available_date,
            f.total_assets,
            f.total_liabilities,
            f.total_equity_parent,
            f.sales,
            f.cogs,
            f.operating_income,
            f.net_income_parent,
            f.operating_cash_flow,
            f.cash_and_cash_equivalents,
            f.total_financial_debt
        FROM (
            SELECT DISTINCT trade_date, ticker FROM core_price_daily
        ) p
        JOIN core_financials_quarterly f
            ON f.ticker = p.ticker
           AND f.available_date <= p.trade_date
        WHERE f.available_date = (
            SELECT MAX(f2.available_date)
            FROM core_financials_quarterly f2
            WHERE f2.ticker = p.ticker
              AND f2.available_date <= p.trade_date
        )
    """

    logger.info("  Running as-of join (may take several minutes for full dataset) ...")
    with conn:
        conn.execute(insert_sql)

    n = conn.execute("SELECT COUNT(*) FROM mart_fundamentals_asof_daily").fetchone()[0]
    logger.info("Built mart_fundamentals_asof_daily: %d rows", n)
    return n
