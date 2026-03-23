"""
Build meta_field_catalog and meta_dataset_coverage.

meta_field_catalog:
  A machine-readable catalog of all important fields in the database,
  with Korean/English synonyms to support future natural-language strategy parsing.
  Each entry includes lookahead_safe flag and default lag information.

meta_dataset_coverage:
  Coverage summary for each major dataset.
"""

import json
import logging
import sqlite3

from src.db import truncate_table, insert_batch

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Field catalog entries
# ---------------------------------------------------------------------------
# Format: (field_id, field_name_en, field_name_ko, table_name, column_name,
#          dtype, frequency, unit, source, lookahead_safe, default_lag,
#          description, synonyms_json)

_FIELD_CATALOG = [
    # --- Price fields ---
    ("close",         "close",              "종가",           "core_price_daily", "close",
     "REAL", "daily", "KRW", "DataGuide", 1, 0,
     "Unadjusted closing price (원)",
     ["종가", "클로징", "close price", "closing price"]),

    ("adj_close",     "adjusted close",     "수정주가",        "core_price_daily", "adj_close",
     "REAL", "daily", "KRW", "DataGuide", 1, 0,
     "Dividend and split adjusted closing price (원)",
     ["수정주가", "수정종가", "adj_close", "adjusted closing price"]),

    ("open",          "open",               "시가",           "core_price_daily", "open",
     "REAL", "daily", "KRW", "DataGuide", 1, 0,
     "Opening price (원)",
     ["시가", "오픈", "open price"]),

    ("high",          "high",               "고가",           "core_price_daily", "high",
     "REAL", "daily", "KRW", "DataGuide", 1, 0,
     "Intraday high price (원)",
     ["고가", "하이"]),

    ("low",           "low",                "저가",           "core_price_daily", "low",
     "REAL", "daily", "KRW", "DataGuide", 1, 0,
     "Intraday low price (원)",
     ["저가", "로우"]),

    ("volume",        "volume",             "거래량",          "core_price_daily", "volume",
     "REAL", "daily", "shares", "DataGuide", 1, 0,
     "Trading volume in shares",
     ["거래량", "volume", "vol"]),

    ("traded_value",  "traded value",       "거래대금",        "core_price_daily", "traded_value",
     "REAL", "daily", "KRW", "DataGuide", 1, 0,
     "Daily traded value in KRW (원). Also called 거래대금 or turnover value.",
     ["거래대금", "거래금액", "turnover value", "traded_value", "adv", "일거래대금", "turnover"]),

    ("market_cap",    "market cap",         "시가총액",        "core_price_daily", "market_cap",
     "REAL", "daily", "KRW", "DataGuide", 1, 0,
     "Market capitalization in KRW (원)",
     ["시가총액", "시총", "mcap", "market cap", "market capitalization", "시가 총액"]),

    ("adj_factor",    "adjustment factor",  "수정계수",        "core_price_daily", "adj_factor",
     "REAL", "daily", "ratio", "DataGuide", 1, 0,
     "Cumulative price adjustment factor for dividends and splits",
     ["수정계수", "adj_factor", "adjustment factor"]),

    ("shares_outstanding", "shares outstanding", "상장주식수", "core_price_daily", "shares_outstanding",
     "REAL", "daily", "shares", "DataGuide", 1, 0,
     "Total listed shares (주)",
     ["상장주식수", "발행주식수", "shares outstanding", "total shares"]),

    ("float_shares",  "float shares",       "유동주식수",      "core_price_daily", "float_shares",
     "REAL", "daily", "shares", "DataGuide", 1, 0,
     "Float shares (유동주식수): shares available for public trading",
     ["유동주식수", "float shares", "float"]),

    ("float_ratio",   "float ratio",        "유동주식비율",    "core_price_daily", "float_ratio",
     "REAL", "daily", "percent", "DataGuide", 1, 0,
     "Float ratio as percentage (%)",
     ["유동주식비율", "float ratio", "float_ratio"]),

    ("trading_halt",  "trading halt",       "거래정지",        "core_price_daily", "trading_halt_flag",
     "REAL", "daily", "flag", "DataGuide", 1, 0,
     "Trading halt flag. Non-zero means halt. Used in eligibility filter.",
     ["거래정지", "거래정지구분", "halt", "trading halt"]),

    ("admin_flag",    "admin supervision",  "관리감리",        "core_price_daily", "admin_supervision_flag",
     "REAL", "daily", "flag", "DataGuide", 1, 0,
     "Administrative supervision / management issue flag (관리종목/감리종목). Non-zero = flagged.",
     ["관리감리", "관리종목", "감리", "admin", "management issue"]),

    # --- Liquidity / eligibility mart fields ---
    ("adv5",          "5-day ADV",          "5일평균거래대금", "mart_liquidity_daily", "adv5",
     "REAL", "daily", "KRW", "derived", 1, 0,
     "5-day average daily traded value (KRW). Must exceed 3bn for eligibility.",
     ["5일평균거래대금", "adv5", "5day adv", "5-day average volume", "5일거래대금평균"]),

    ("adv20",         "20-day ADV",         "20일평균거래대금","mart_liquidity_daily", "adv20",
     "REAL", "daily", "KRW", "derived", 1, 0,
     "20-day average daily traded value (KRW)",
     ["20일평균거래대금", "adv20", "20day adv"]),

    ("listing_age_bd","listing age",        "상장일수",        "mart_liquidity_daily", "listing_age_bd",
     "INTEGER", "daily", "days", "derived", 1, 0,
     "Number of trading days (business days) since listing. < 6 = too new for eligibility.",
     ["상장일수", "상장경과일", "listing age", "days since ipo"]),

    ("is_eligible",   "eligibility flag",   "편입가능여부",    "mart_universe_eligibility_daily", "is_eligible",
     "INTEGER", "daily", "flag", "derived", 1, 0,
     "1 if stock is eligible for the contest universe on this date, 0 otherwise.",
     ["편입가능", "유니버스포함", "eligible", "investable", "is_eligible"]),

    # --- Feature mart fields ---
    ("ret_1d",        "1-day return",       "1일수익률",       "mart_feature_daily", "ret_1d",
     "REAL", "daily", "ratio", "derived", 1, 1,
     "1-day price return (adj_close based). Lag=1 day (PIT-safe).",
     ["1일수익률", "일간수익률", "ret_1d", "1d return", "daily return"]),

    ("ret_5d",        "5-day return",       "5일수익률",       "mart_feature_daily", "ret_5d",
     "REAL", "daily", "ratio", "derived", 1, 5,
     "5-day cumulative return (adj_close based)",
     ["5일수익률", "주간수익률", "ret_5d", "1w return", "weekly return"]),

    ("ret_20d",       "20-day return",      "20일수익률",      "mart_feature_daily", "ret_20d",
     "REAL", "daily", "ratio", "derived", 1, 20,
     "20-day cumulative return (adj_close based)",
     ["20일수익률", "월간수익률", "ret_20d", "1m return", "monthly return"]),

    ("ret_60d",       "60-day return",      "60일수익률",      "mart_feature_daily", "ret_60d",
     "REAL", "daily", "ratio", "derived", 1, 60,
     "60-day cumulative return (adj_close based)",
     ["60일수익률", "분기수익률", "ret_60d", "3m return", "quarterly return", "모멘텀", "momentum"]),

    ("vol_20d",       "20-day volatility",  "20일변동성",      "mart_feature_daily", "vol_20d",
     "REAL", "daily", "ratio", "derived", 1, 20,
     "20-day realized volatility (std of daily returns)",
     ["20일변동성", "변동성", "vol", "volatility", "vol_20d", "risk"]),

    ("turnover_ratio","turnover ratio",     "회전율",          "mart_feature_daily", "turnover_ratio",
     "REAL", "daily", "ratio", "derived", 1, 0,
     "Turnover ratio = traded_value / market_cap",
     ["회전율", "turnover", "turnover ratio"]),

    ("price_52w_hi",  "52-week high ratio", "52주고점비율",    "mart_feature_daily", "price_to_52w_high",
     "REAL", "daily", "ratio", "derived", 1, 0,
     "Current price as fraction of prior 252-day high (52-week high proxy)",
     ["52주고점", "52주고가대비", "52w high", "price to high"]),

    # --- Fundamental features ---
    ("sales_yoy",     "sales growth YoY",   "매출증가율",      "mart_feature_daily", "sales_growth_yoy",
     "REAL", "daily", "ratio", "derived", 1, 45,
     "Year-over-year sales growth. PIT-safe via mart_fundamentals_asof_daily.",
     ["매출증가율", "매출성장률", "revenue growth", "sales growth", "top-line growth"]),

    ("opinc_yoy",     "op income growth",   "영업이익증가율",  "mart_feature_daily", "op_income_growth_yoy",
     "REAL", "daily", "ratio", "derived", 1, 45,
     "Year-over-year operating income growth",
     ["영업이익증가율", "영업이익성장률", "operating income growth", "op income growth"]),

    ("net_debt_eq",   "net debt/equity",    "순부채비율",      "mart_feature_daily", "net_debt_to_equity",
     "REAL", "daily", "ratio", "derived", 1, 45,
     "Net debt to equity = (total_financial_debt - cash) / total_equity_parent",
     ["순부채비율", "부채비율", "leverage", "net debt equity", "net leverage"]),

    ("cash_assets",   "cash to assets",     "현금비율",        "mart_feature_daily", "cash_to_assets",
     "REAL", "daily", "ratio", "derived", 1, 45,
     "Cash / total assets ratio",
     ["현금비율", "현금자산비율", "cash ratio", "cash to assets"]),

    # --- Fundamental quarterly ---
    ("total_assets",  "total assets",       "자산총계",        "core_financials_quarterly", "total_assets",
     "REAL", "quarterly", "천원", "DataGuide", 0, 45,
     "Total assets (천원, thousands KRW). Not PIT-safe in raw form; use mart_fundamentals_asof_daily.",
     ["자산총계", "총자산", "total assets", "assets"]),

    ("total_eq",      "total equity",       "자본총계",        "core_financials_quarterly", "total_equity_parent",
     "REAL", "quarterly", "천원", "DataGuide", 0, 45,
     "Total equity attributable to parent (천원)",
     ["자본총계", "지배자본", "equity", "shareholders equity", "book value"]),

    ("sales_q",       "sales",              "매출액",          "core_financials_quarterly", "sales",
     "REAL", "quarterly", "천원", "DataGuide", 0, 45,
     "Net sales / revenue (천원)",
     ["매출액", "매출", "revenue", "sales", "top line"]),

    ("op_income",     "operating income",   "영업이익",        "core_financials_quarterly", "operating_income",
     "REAL", "quarterly", "천원", "DataGuide", 0, 45,
     "Operating income / profit (천원)",
     ["영업이익", "영업이익", "operating income", "operating profit", "EBIT proxy"]),

    ("net_income",    "net income",         "당기순이익",      "core_financials_quarterly", "net_income_parent",
     "REAL", "quarterly", "천원", "DataGuide", 0, 45,
     "Net income attributable to parent (천원)",
     ["당기순이익", "순이익", "net income", "bottom line", "profit"]),

    ("ocf",           "operating cash flow","영업현금흐름",    "core_financials_quarterly", "operating_cash_flow",
     "REAL", "quarterly", "천원", "DataGuide", 0, 45,
     "Cash flow from operations (천원)",
     ["영업현금흐름", "영업활동현금흐름", "operating cash flow", "OCF", "CFO"]),

    ("fin_debt",      "total financial debt","금융부채",       "core_financials_quarterly", "total_financial_debt",
     "REAL", "quarterly", "천원", "DataGuide", 0, 45,
     "Total financial debt (천원). Includes short-term and long-term borrowings.",
     ["금융부채", "부채", "financial debt", "total debt", "debt"]),

    # --- Sector ---
    ("sector",        "sector",             "섹터",            "core_sector_map", "sector_name",
     "TEXT", "static", "", "sector_file", 1, 0,
     "Sector classification (11-sector RFM contest taxonomy)",
     ["섹터", "산업", "섹터분류", "sector", "industry", "산업군", "업종분류"]),
]


def build_field_catalog(conn: sqlite3.Connection) -> int:
    """Populate meta_field_catalog."""
    truncate_table(conn, "meta_field_catalog")

    cols = ["field_id", "field_name_en", "field_name_ko", "table_name", "column_name",
            "dtype", "frequency", "unit", "source", "lookahead_safe", "default_lag",
            "description", "synonyms_json"]

    rows = []
    for entry in _FIELD_CATALOG:
        (field_id, fname_en, fname_ko, table_name, col_name,
         dtype, freq, unit, source, pit_safe, lag, desc, synonyms) = entry
        rows.append((
            field_id, fname_en, fname_ko, table_name, col_name,
            dtype, freq, unit, source, pit_safe, lag, desc,
            json.dumps(synonyms, ensure_ascii=False),
        ))

    n = insert_batch(conn, "meta_field_catalog", rows, cols)
    logger.info("Built meta_field_catalog: %d entries", n)
    return n


def build_dataset_coverage(conn: sqlite3.Connection) -> int:
    """Populate meta_dataset_coverage with coverage ranges and caveats."""
    truncate_table(conn, "meta_dataset_coverage")

    def get_range(table, date_col):
        try:
            r = conn.execute(
                f"SELECT MIN({date_col}), MAX({date_col}), COUNT(*) FROM {table}"
            ).fetchone()
            return r[0], r[1], r[2]
        except Exception:
            return None, None, 0

    price_min, price_max, price_cnt = get_range("core_price_daily", "trade_date")
    idx_min, idx_max, idx_cnt       = get_range("core_index_daily",  "trade_date")
    fin_min, fin_max, fin_cnt       = get_range("core_financials_quarterly", "period_end")
    ipo_min, ipo_max, ipo_cnt       = get_range("raw_kind_ipos",     "listing_date")
    delist_min, delist_max, delist_cnt = get_range("raw_kind_delistings", "delisting_date")
    caution_min, caution_max, caution_cnt = get_range("raw_kind_investment_caution", "designation_date")
    warning_min, warning_max, warning_cnt = get_range("raw_kind_investment_warning", "designation_date")
    risk_min, risk_max, risk_cnt    = get_range("raw_kind_investment_risk", "designation_date")

    datasets = [
        ("dg_stock_daily", "DataGuide Stock Daily",
         "raw/dataguide.xlsx (sheet: type1)",
         price_min, price_max, "daily", price_cnt,
         "~4097 KOSPI+KOSDAQ stocks. adj_close based on DataGuide adjustment. "
         "Financial units: KRW (원). trading_halt_flag and admin_supervision_flag "
         "are DataGuide proprietary codes (non-zero = flagged)."),

        ("dg_index_daily", "DataGuide Index Daily",
         "raw/dataguide.xlsx (sheet: bm)",
         idx_min, idx_max, "daily", idx_cnt,
         "KOSPI, KOSPI200, KOSDAQ, KRX300 daily OHLC. Units: index points."),

        ("dg_financials", "DataGuide Quarterly Financials",
         "raw/dataguide.xlsx (sheet: type2)",
         fin_min, fin_max, "quarterly", fin_cnt,
         "Financial values in 천원 (thousands KRW). "
         "period_end is computed as calendar quarter end (approximation for non-Dec FY). "
         "available_date = period_end + lag (45d for Q1-Q3, 90d for Q4). "
         "See financial_lag config for details."),

        ("kind_ipos", "KIND IPO/Listing History",
         "raw/kind/신규상장기업현황.xls",
         ipo_min, ipo_max, "event", ipo_cnt,
         "Listing events from 2001 to present. No market type (KOSPI/KOSDAQ) in source. "
         "Market type is joined from current listed companies file."),

        ("kind_delistings", "KIND Delisting History",
         "raw/kind/상장폐지현황.xls",
         delist_min, delist_max, "event", delist_cnt,
         "Full delisting history from 1999 to present."),

        ("kind_current_list", "KIND Currently Listed Companies",
         "raw/kind/상장법인목록.xls",
         None, None, "snapshot", None,
         "Current snapshot only. Used for market_type and fiscal_month. "
         "NOT used as sole historical truth for PIT safety."),

        ("kind_caution", "KIND Investment Caution",
         "raw/kind/투자주의종목.xls",
         caution_min, caution_max, "event", caution_cnt,
         "Recent 3 years. No removal date in source - treated as 1-day designation. "
         "EXCLUDED RULE: 투자주의환기종목 (investment caution watchlist) data is NOT available "
         "and therefore NOT implemented. TODO: add when data is obtained."),

        ("kind_warning", "KIND Investment Warning",
         "raw/kind/투자경고종목.xls",
         warning_min, warning_max, "event", warning_cnt,
         "Recent 3 years. Has removal date. '-' = still active."),

        ("kind_risk", "KIND Investment Risk",
         "raw/kind/투자위험종목.xls",
         risk_min, risk_max, "event", risk_cnt,
         "Recent 3 years. Has removal date. '-' = still active."),

        ("sector_map", "Sector Mapping",
         "raw/sector/sector allocation_filled_reprocessed.xlsx",
         None, None, "static", None,
         "11-sector RFM contest taxonomy. Filled using OpenAI for missing entries. "
         "Ticker code is the join key (not company name). "
         "Taxonomy level: 11-sector-RFM-contest. See sector.allowed_values in config."),

        ("kind_stock_issuance", "KIND Stock Issuance",
         "raw/kind/주식발행내역.xls",
         None, None, "event", None,
         "v1 LIMITATION: stored in raw_kind_stock_issuance but NOT fully transformed "
         "for corporate action adjustments. DataGuide adj_factor is used for price adjustment instead."),
    ]

    cols = ["dataset_id", "dataset_name", "source_file", "coverage_start", "coverage_end",
            "frequency", "record_count", "caveats"]

    rows = [(d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7]) for d in datasets]
    n = insert_batch(conn, "meta_dataset_coverage", rows, cols)
    logger.info("Built meta_dataset_coverage: %d entries", n)
    return n
