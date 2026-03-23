"""
SQL query builders for the backtest engine.

All queries are PIT-safe:
- Price/feature data is only available after the close of the trade_date
- Financial data uses available_date (not period_end)
- Regulatory status uses interval_start <= trade_date <= interval_end

Query building is field-selective: only requested columns are fetched.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from backtest_engine.registry.field_registry import FIELD_REGISTRY, FieldMeta


# Table aliases used in the main snapshot join
_TABLE_ALIASES = {
    "mart_universe_eligibility_daily": "e",
    "core_price_daily": "p",
    "mart_liquidity_daily": "l",
    "mart_feature_daily": "ft",
    "mart_fundamentals_asof_daily": "fa",
    "core_sector_map": "sec",
    "mart_sector_weight_snapshot": "sw",
}

# Tables that join on (trade_date, ticker)
_DAILY_TABLES = {
    "mart_universe_eligibility_daily",
    "core_price_daily",
    "mart_liquidity_daily",
    "mart_feature_daily",
    "mart_fundamentals_asof_daily",
    "mart_sector_weight_snapshot",
}

# Tables that join on ticker only (static)
_STATIC_TABLES = {"core_sector_map"}


def build_snapshot_query(
    trade_date: str,
    requested_fields: List[str],
    include_blocked: bool = False,
    markets: Optional[List[str]] = None,
) -> Tuple[str, list]:
    """
    Build the main snapshot SQL query for a single trade_date.

    Returns (sql, params) ready for cursor.execute(sql, params).

    Always includes: trade_date, ticker, is_eligible, sector_name.
    """
    # Determine which tables are needed
    tables_needed: set[str] = {
        "mart_universe_eligibility_daily",   # always — universe gate
        "core_price_daily",                   # always — price
        "mart_liquidity_daily",               # always — adv5, mcap
    }

    # Collect field metadata
    field_metas: dict[str, FieldMeta] = {}
    for fid in requested_fields:
        if fid in FIELD_REGISTRY:
            fm = FIELD_REGISTRY[fid]
            field_metas[fid] = fm
            tables_needed.add(fm.table_name)

    include_sector = "sector_name" in requested_fields or True  # always join sector
    include_features = "mart_feature_daily" in tables_needed
    include_fundamentals = "mart_fundamentals_asof_daily" in tables_needed
    include_sector_weight = "mart_sector_weight_snapshot" in tables_needed

    # Build SELECT list
    select_cols = ["e.trade_date", "e.ticker", "e.is_eligible"]

    # Always include core price fields needed for execution
    base_price_cols = ["p.close", "p.adj_close", "p.market_cap", "p.traded_value"]
    select_cols.extend(base_price_cols)

    # Add liquidity
    select_cols.extend(["l.adv5", "l.adv20", "l.listing_age_bd"])

    # Add sector
    select_cols.append("sec.sector_name")

    # Add requested fields (deduplicated)
    already_added = {
        "trade_date", "ticker", "is_eligible",
        "close", "adj_close", "market_cap", "traded_value",
        "adv5", "adv20", "listing_age_bd", "sector_name"
    }
    for fid in requested_fields:
        if fid in field_metas and fid not in already_added:
            fm = field_metas[fid]
            alias = _TABLE_ALIASES.get(fm.table_name, fm.table_name)
            select_cols.append(f"{alias}.{fm.column_name} AS {fid}")
            already_added.add(fid)

    sql = f"""
SELECT
    {',\n    '.join(select_cols)}
FROM mart_universe_eligibility_daily e
JOIN core_price_daily p
    ON e.trade_date = p.trade_date AND e.ticker = p.ticker
JOIN mart_liquidity_daily l
    ON e.trade_date = l.trade_date AND e.ticker = l.ticker
LEFT JOIN core_sector_map sec
    ON e.ticker = sec.ticker
"""

    if include_features:
        sql += "LEFT JOIN mart_feature_daily ft ON e.trade_date = ft.trade_date AND e.ticker = ft.ticker\n"
    if include_fundamentals:
        sql += "LEFT JOIN mart_fundamentals_asof_daily fa ON e.trade_date = fa.trade_date AND e.ticker = fa.ticker\n"
    if include_sector_weight:
        sql += "LEFT JOIN mart_sector_weight_snapshot sw ON e.trade_date = sw.trade_date AND sec.sector_name = sw.sector_name\n"

    params = [trade_date]
    where_clauses = ["e.trade_date = ?"]

    if not include_blocked:
        where_clauses.append("e.is_eligible = 1")

    if markets:
        placeholders = ",".join("?" * len(markets))
        where_clauses.append(
            f"e.ticker IN (SELECT ticker FROM core_security_master WHERE market_type IN ({placeholders}))"
        )
        params.extend(markets)

    sql += "WHERE " + " AND ".join(where_clauses)
    return sql, params


def build_price_history_query(
    tickers: List[str],
    start_date: str,
    end_date: str,
    fields: Optional[List[str]] = None,
) -> Tuple[str, list]:
    """Build query for price history across a date range."""
    default_fields = ["trade_date", "ticker", "adj_close", "close", "volume", "traded_value", "market_cap"]
    requested = fields or default_fields
    valid_price_cols = {
        "open", "high", "low", "close", "adj_open", "adj_high", "adj_low", "adj_close",
        "adj_factor", "volume", "traded_value", "shares_outstanding", "market_cap",
        "float_shares", "float_ratio", "trading_halt_flag", "admin_supervision_flag"
    }
    select = ["trade_date", "ticker"] + [f for f in requested if f in valid_price_cols and f not in ("trade_date", "ticker")]
    ticker_placeholders = ",".join("?" * len(tickers))
    sql = f"""
SELECT {', '.join(select)}
FROM core_price_daily
WHERE ticker IN ({ticker_placeholders})
  AND trade_date BETWEEN ? AND ?
ORDER BY ticker, trade_date
"""
    return sql, tickers + [start_date, end_date]


def build_feature_history_query(
    tickers: List[str],
    start_date: str,
    end_date: str,
    fields: Optional[List[str]] = None,
) -> Tuple[str, list]:
    """Build query for feature history."""
    default_fields = ["trade_date", "ticker", "ret_1d", "ret_5d", "ret_20d", "ret_60d", "vol_20d"]
    all_feature_cols = {
        "ret_1d", "ret_5d", "ret_20d", "ret_60d", "vol_20d",
        "turnover_ratio", "price_to_52w_high",
        "sales_growth_yoy", "op_income_growth_yoy", "net_debt_to_equity", "cash_to_assets"
    }
    requested = fields or default_fields
    select = ["trade_date", "ticker"] + [f for f in requested if f in all_feature_cols and f not in ("trade_date", "ticker")]
    ticker_ph = ",".join("?" * len(tickers))
    sql = f"""
SELECT {', '.join(select)}
FROM mart_feature_daily
WHERE ticker IN ({ticker_ph})
  AND trade_date BETWEEN ? AND ?
ORDER BY ticker, trade_date
"""
    return sql, tickers + [start_date, end_date]


def build_index_history_query(
    index_code: str,
    start_date: str,
    end_date: str,
) -> Tuple[str, list]:
    """Build query for index daily data."""
    sql = """
SELECT trade_date, index_code, open, high, low, close
FROM core_index_daily
WHERE index_code = ? AND trade_date BETWEEN ? AND ?
ORDER BY trade_date
"""
    return sql, [index_code, start_date, end_date]


def build_covariance_data_query(
    tickers: List[str],
    lookback_start: str,
    end_date: str,
) -> Tuple[str, list]:
    """Build query for return data used in covariance estimation."""
    ph = ",".join("?" * len(tickers))
    sql = f"""
SELECT trade_date, ticker, ret_1d
FROM mart_feature_daily
WHERE ticker IN ({ph})
  AND trade_date BETWEEN ? AND ?
  AND ret_1d IS NOT NULL
ORDER BY trade_date, ticker
"""
    return sql, tickers + [lookback_start, end_date]


def build_rebalance_universe_query(dates: List[str]) -> Tuple[str, list]:
    """Build query to get eligible tickers across multiple rebalance dates."""
    ph = ",".join("?" * len(dates))
    sql = f"""
SELECT DISTINCT trade_date, ticker
FROM mart_universe_eligibility_daily
WHERE trade_date IN ({ph}) AND is_eligible = 1
ORDER BY trade_date, ticker
"""
    return sql, dates
