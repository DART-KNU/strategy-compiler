"""
Build mart_feature_daily using SQLite window functions (no self-joins).
"""

import logging
import sqlite3

from src.db import truncate_table

logger = logging.getLogger(__name__)


def build_features(conn: sqlite3.Connection) -> int:
    """Build mart_feature_daily from core_price_daily and mart_fundamentals_asof_daily."""
    logger.info("Building mart_feature_daily ...")
    truncate_table(conn, "mart_feature_daily")

    # ------------------------------------------------------------------
    # Step 1: returns + 52w-high via LAG/MAX window functions
    # Materialise into a temp table so step 2 can reuse cheaply.
    # ------------------------------------------------------------------
    logger.info("  Step 1/3: Computing price returns and 52w high (window scan) ...")
    conn.execute("DROP TABLE IF EXISTS _tmp_feat_ret")
    with conn:
        conn.execute("""
            CREATE TEMP TABLE _tmp_feat_ret AS
            SELECT
                trade_date,
                ticker,
                close,
                traded_value,
                market_cap,
                CASE WHEN LAG(adj_close, 1)  OVER (PARTITION BY ticker ORDER BY trade_date) > 0
                     THEN adj_close / LAG(adj_close, 1)  OVER (PARTITION BY ticker ORDER BY trade_date) - 1
                END AS ret_1d,
                CASE WHEN LAG(adj_close, 5)  OVER (PARTITION BY ticker ORDER BY trade_date) > 0
                     THEN adj_close / LAG(adj_close, 5)  OVER (PARTITION BY ticker ORDER BY trade_date) - 1
                END AS ret_5d,
                CASE WHEN LAG(adj_close, 20) OVER (PARTITION BY ticker ORDER BY trade_date) > 0
                     THEN adj_close / LAG(adj_close, 20) OVER (PARTITION BY ticker ORDER BY trade_date) - 1
                END AS ret_20d,
                CASE WHEN LAG(adj_close, 60) OVER (PARTITION BY ticker ORDER BY trade_date) > 0
                     THEN adj_close / LAG(adj_close, 60) OVER (PARTITION BY ticker ORDER BY trade_date) - 1
                END AS ret_60d,
                CASE WHEN market_cap > 0 THEN traded_value / market_cap END AS turnover_ratio,
                MAX(close) OVER (
                    PARTITION BY ticker ORDER BY trade_date
                    ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING
                ) AS max_close_252d
            FROM core_price_daily
        """)
    n1 = conn.execute("SELECT COUNT(*) FROM _tmp_feat_ret").fetchone()[0]
    logger.info("  Step 1 complete: %d rows", n1)

    # ------------------------------------------------------------------
    # Step 2: rolling vol-20d via AVG window on materialised returns
    # Variance = E[x^2] - E[x]^2, std = SQRT(variance).
    # ------------------------------------------------------------------
    logger.info("  Step 2/3: Computing rolling 20-day volatility ...")
    conn.execute("DROP TABLE IF EXISTS _tmp_feat_vol")
    with conn:
        conn.execute("""
            CREATE TEMP TABLE _tmp_feat_vol AS
            WITH rolling AS (
                SELECT
                    trade_date, ticker,
                    ret_1d, ret_5d, ret_20d, ret_60d,
                    turnover_ratio, max_close_252d, close,
                    COUNT(ret_1d) OVER (
                        PARTITION BY ticker ORDER BY trade_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS cnt20,
                    AVG(ret_1d * ret_1d) OVER (
                        PARTITION BY ticker ORDER BY trade_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS avg_sq20,
                    AVG(ret_1d) OVER (
                        PARTITION BY ticker ORDER BY trade_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS avg20
                FROM _tmp_feat_ret
            )
            SELECT
                trade_date, ticker,
                ret_1d, ret_5d, ret_20d, ret_60d,
                CASE
                    WHEN cnt20 >= 10 AND (avg_sq20 - avg20 * avg20) >= 0
                    THEN SQRT(avg_sq20 - avg20 * avg20)
                END AS vol_20d,
                turnover_ratio,
                CASE WHEN max_close_252d > 0 THEN close / max_close_252d END AS price_to_52w_high
            FROM rolling
        """)
    n2 = conn.execute("SELECT COUNT(*) FROM _tmp_feat_vol").fetchone()[0]
    logger.info("  Step 2 complete: %d rows", n2)

    # ------------------------------------------------------------------
    # Step 3: compact fundamentals lookup (distinct periods, ~70K rows)
    # This avoids a correlated subquery over 3.7M rows for YoY growth.
    # ------------------------------------------------------------------
    logger.info("  Step 3/3: Joining fundamentals and inserting final rows ...")
    conn.execute("DROP TABLE IF EXISTS _tmp_fund_periods")
    with conn:
        conn.execute("""
            CREATE TEMP TABLE _tmp_fund_periods AS
            SELECT
                ticker,
                CAST(available_year AS INTEGER)  AS avail_year,
                available_quarter                AS avail_q,
                MAX(sales)                       AS sales,
                MAX(operating_income)            AS operating_income
            FROM mart_fundamentals_asof_daily
            GROUP BY ticker, available_year, available_quarter
        """)

    conn.execute(
        "CREATE INDEX IF NOT EXISTS _idx_fund_periods ON _tmp_fund_periods(ticker, avail_year, avail_q)"
    )

    with conn:
        conn.execute("""
            INSERT OR REPLACE INTO mart_feature_daily (
                trade_date, ticker,
                ret_1d, ret_5d, ret_20d, ret_60d,
                vol_20d, turnover_ratio, price_to_52w_high,
                sales_growth_yoy, op_income_growth_yoy,
                net_debt_to_equity, cash_to_assets
            )
            SELECT
                f.trade_date,
                f.ticker,
                f.ret_1d, f.ret_5d, f.ret_20d, f.ret_60d,
                f.vol_20d,
                f.turnover_ratio,
                f.price_to_52w_high,
                -- YoY sales growth
                CASE WHEN fp_prev.sales IS NOT NULL AND fp_prev.sales != 0
                     THEN fa.sales / fp_prev.sales - 1 END AS sales_growth_yoy,
                -- YoY operating income growth
                CASE WHEN fp_prev.operating_income IS NOT NULL AND fp_prev.operating_income != 0
                     THEN fa.operating_income / fp_prev.operating_income - 1 END AS op_income_growth_yoy,
                -- Net debt to equity
                CASE WHEN fa.total_equity_parent IS NOT NULL AND fa.total_equity_parent != 0
                     THEN (COALESCE(fa.total_financial_debt, 0) -
                           COALESCE(fa.cash_and_cash_equivalents, 0)) / fa.total_equity_parent
                END AS net_debt_to_equity,
                -- Cash to assets
                CASE WHEN fa.total_assets IS NOT NULL AND fa.total_assets != 0
                     THEN COALESCE(fa.cash_and_cash_equivalents, 0) / fa.total_assets
                END AS cash_to_assets
            FROM _tmp_feat_vol f
            -- current PIT-safe financials for this trade_date
            LEFT JOIN mart_fundamentals_asof_daily fa
                ON f.ticker = fa.ticker AND f.trade_date = fa.trade_date
            -- prior-year same quarter (compact lookup, ~70K rows)
            LEFT JOIN _tmp_fund_periods fp_prev
                ON fa.ticker = fp_prev.ticker
               AND fp_prev.avail_year  = CAST(fa.available_year AS INTEGER) - 1
               AND fp_prev.avail_q     = fa.available_quarter
        """)

    # cleanup temp tables
    for t in ("_tmp_feat_ret", "_tmp_feat_vol", "_tmp_fund_periods"):
        conn.execute(f"DROP TABLE IF EXISTS {t}")

    n = conn.execute("SELECT COUNT(*) FROM mart_feature_daily").fetchone()[0]
    logger.info("Built mart_feature_daily: %d rows", n)
    return n
