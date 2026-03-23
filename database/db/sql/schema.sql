-- ============================================================
-- DART Backtest Database Schema
-- SQLite (WAL mode, foreign_keys ON)
-- All dates stored as ISO-8601 TEXT (YYYY-MM-DD)
-- Financial values in 천원 (thousands KRW) as sourced from DataGuide
-- ============================================================

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;

-- ============================================================
-- LAYER 1: RAW TABLES
-- Mirror source files as closely as practical.
-- All raw tickers are stored exactly as they appear in the source.
-- ============================================================

-- Raw: KIND currently listed companies (current snapshot only)
CREATE TABLE IF NOT EXISTS raw_kind_listed_companies_current (
    raw_ticker          TEXT NOT NULL,
    corp_name           TEXT,
    market_type         TEXT,   -- 코스피 / 코스닥 etc.
    industry            TEXT,
    main_products       TEXT,
    listing_date        TEXT,
    fiscal_month        TEXT,
    representative      TEXT,
    website             TEXT,
    region              TEXT,
    ingested_at         TEXT NOT NULL,
    PRIMARY KEY (raw_ticker)
);

-- Raw: KIND delisting history
CREATE TABLE IF NOT EXISTS raw_kind_delistings (
    seq                 INTEGER,
    raw_ticker          TEXT NOT NULL,
    corp_name           TEXT,
    delisting_date      TEXT,
    delisting_reason    TEXT,
    notes               TEXT,
    ingested_at         TEXT NOT NULL,
    PRIMARY KEY (raw_ticker, delisting_date)
);

-- Raw: KIND IPO / new listing history
CREATE TABLE IF NOT EXISTS raw_kind_ipos (
    raw_ticker          TEXT NOT NULL,
    corp_name           TEXT,
    listing_date        TEXT,
    listing_type        TEXT,   -- 신규상장/이전상장/재상장 etc.
    security_type       TEXT,   -- 주권/신주인수권 etc.
    industry            TEXT,
    nationality         TEXT,
    underwriter         TEXT,
    ingested_at         TEXT NOT NULL,
    PRIMARY KEY (raw_ticker, listing_date, listing_type)
);

-- Raw: KIND stock issuance history
-- NOTE v1 limitation: stored as-is, not fully transformed for corporate actions.
CREATE TABLE IF NOT EXISTS raw_kind_stock_issuance (
    raw_ticker          TEXT NOT NULL,
    corp_name           TEXT,
    listing_date        TEXT,   -- 상장(예정)일
    issuance_type       TEXT,   -- 추가상장/변경상장/신규상장 etc.
    shares_issued       REAL,   -- can be negative for cancellations
    par_value           REAL,
    issuance_reason     TEXT,
    ingested_at         TEXT NOT NULL,
    PRIMARY KEY (raw_ticker, listing_date, issuance_type, issuance_reason)
);

-- Raw: KIND investment caution (투자주의종목)
-- NOTE: No removal date in source data. Treated as 1-day designation.
CREATE TABLE IF NOT EXISTS raw_kind_investment_caution (
    seq                 INTEGER,
    raw_ticker          TEXT NOT NULL,
    stock_name          TEXT,
    caution_type        TEXT,
    announcement_date   TEXT,
    designation_date    TEXT NOT NULL,
    ingested_at         TEXT NOT NULL,
    PRIMARY KEY (raw_ticker, designation_date, caution_type)
);

-- Raw: KIND investment warning (투자경고종목)
CREATE TABLE IF NOT EXISTS raw_kind_investment_warning (
    seq                 INTEGER,
    raw_ticker          TEXT NOT NULL,
    stock_name          TEXT,
    announcement_date   TEXT,
    designation_date    TEXT NOT NULL,
    removal_date        TEXT,   -- NULL means still active; '-' in source mapped to NULL
    ingested_at         TEXT NOT NULL,
    PRIMARY KEY (raw_ticker, designation_date)
);

-- Raw: KIND investment risk (투자위험종목)
CREATE TABLE IF NOT EXISTS raw_kind_investment_risk (
    seq                 INTEGER,
    raw_ticker          TEXT NOT NULL,
    stock_name          TEXT,
    announcement_date   TEXT,
    designation_date    TEXT NOT NULL,
    removal_date        TEXT,   -- NULL means still active
    ingested_at         TEXT NOT NULL,
    PRIMARY KEY (raw_ticker, designation_date)
);

-- Raw: DataGuide index daily (long format: one row per index × item × date)
CREATE TABLE IF NOT EXISTS raw_dg_index_daily (
    raw_code            TEXT NOT NULL,  -- e.g. I.001
    code_name           TEXT,           -- e.g. 코스피
    index_type          TEXT,           -- IDX
    item_code           TEXT NOT NULL,
    item_name           TEXT NOT NULL,
    trade_date          TEXT NOT NULL,
    value               REAL,
    ingested_at         TEXT NOT NULL,
    PRIMARY KEY (raw_code, item_code, trade_date)
);

-- Raw: DataGuide stock daily (long format: one row per ticker × item × date)
-- ~4097 tickers × 17 items × ~1300 dates ≈ 90M rows
-- This is the largest table; build may take significant time.
CREATE TABLE IF NOT EXISTS raw_dg_stock_daily (
    raw_ticker          TEXT NOT NULL,
    corp_name           TEXT,
    security_type       TEXT,   -- SSC etc.
    item_code           TEXT NOT NULL,
    item_name           TEXT NOT NULL,
    trade_date          TEXT NOT NULL,
    value               REAL,
    ingested_at         TEXT NOT NULL,
    PRIMARY KEY (raw_ticker, item_code, trade_date)
);

-- Raw: DataGuide quarterly financials (long format: one row per ticker × item × period)
CREATE TABLE IF NOT EXISTS raw_dg_financials_quarterly (
    raw_ticker          TEXT NOT NULL,
    corp_name           TEXT,
    fiscal_month        TEXT,   -- e.g. '12' for December year-end
    report_type         TEXT,   -- e.g. 'NFS-IFRS(M)'
    item_code           TEXT NOT NULL,
    item_name           TEXT NOT NULL,
    year                TEXT NOT NULL,  -- e.g. '2018'
    quarter             TEXT NOT NULL,  -- '1Q', '2Q', '3Q', '4Q'
    value               REAL,
    ingested_at         TEXT NOT NULL,
    PRIMARY KEY (raw_ticker, item_code, year, quarter)
);

-- Raw: Sector mapping
CREATE TABLE IF NOT EXISTS raw_sector_map (
    raw_ticker          TEXT NOT NULL,   -- original code from file (may have 'A' prefix)
    corp_name           TEXT,
    sector_code         TEXT,
    sector_name         TEXT,
    fill_method         TEXT,
    confidence          REAL,
    needs_review        INTEGER,
    notes               TEXT,
    ingested_at         TEXT NOT NULL,
    PRIMARY KEY (raw_ticker)
);

-- Raw: Build manifest - one row per input file per build run
CREATE TABLE IF NOT EXISTS raw_build_manifest (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    build_run_id        TEXT NOT NULL,
    source_name         TEXT NOT NULL,
    absolute_path       TEXT NOT NULL,
    file_size_bytes     INTEGER,
    modified_time       TEXT,
    sha256              TEXT,
    ingested_at         TEXT NOT NULL,
    UNIQUE (build_run_id, source_name)
);

-- ============================================================
-- LAYER 2: CORE TABLES
-- Normalized, canonical, PIT-aware tables.
-- All tickers are in canonical 6-digit format (leading zeros preserved, 'A' prefix removed).
-- ============================================================

-- Core: Security master
-- Built from KIND IPO history + delisting history + listed companies snapshot.
-- Interval-aware: each row covers the period [listing_date, delisting_date).
CREATE TABLE IF NOT EXISTS core_security_master (
    ticker              TEXT NOT NULL,  -- canonical 6-digit ticker
    corp_name           TEXT,
    market_type         TEXT,           -- 코스피 / 코스닥 / UNKNOWN
    security_type       TEXT,           -- 주권 / other
    is_common_equity    INTEGER NOT NULL DEFAULT 0,  -- 1 if security_type = '주권'
    listing_date        TEXT,           -- YYYY-MM-DD; NULL if unknown
    delisting_date      TEXT,           -- YYYY-MM-DD; NULL if still listed
    is_active_current   INTEGER NOT NULL DEFAULT 0,  -- 1 if in current listed snapshot
    listing_type        TEXT,           -- 신규상장 / 이전상장 / 재상장 etc.
    fiscal_month        TEXT,
    industry            TEXT,
    source_notes        TEXT,           -- documentation of data sources used
    PRIMARY KEY (ticker)
);

-- Core: Trading calendar
-- Derived from dates present in raw_dg_stock_daily (only trading days are in DataGuide).
-- Coverage: 2020-12-30 to 2026-03-20 (as per DataGuide extract).
CREATE TABLE IF NOT EXISTS core_calendar (
    trade_date          TEXT NOT NULL,
    is_open             INTEGER NOT NULL DEFAULT 1,
    prev_open_date      TEXT,   -- previous trading day
    next_open_date      TEXT,   -- next trading day
    week_id             TEXT,   -- ISO week: YYYY-Www
    month_id            TEXT,   -- YYYY-MM
    PRIMARY KEY (trade_date)
) WITHOUT ROWID;

-- Core: Daily price data (one row per ticker × trade_date)
-- Pivoted from raw_dg_stock_daily.
-- Financial units: KRW (won) as sourced.
CREATE TABLE IF NOT EXISTS core_price_daily (
    trade_date              TEXT NOT NULL,
    ticker                  TEXT NOT NULL,
    open                    REAL,
    high                    REAL,
    low                     REAL,
    close                   REAL,
    adj_open                REAL,
    adj_high                REAL,
    adj_low                 REAL,
    adj_close               REAL,
    adj_factor              REAL,
    volume                  REAL,
    traded_value            REAL,   -- KRW
    shares_outstanding      REAL,
    market_cap              REAL,   -- KRW
    trading_halt_flag       REAL,   -- 0/1 or code from DataGuide
    admin_supervision_flag  REAL,   -- 0/1 or code from DataGuide
    float_shares            REAL,
    float_ratio             REAL,   -- percent
    PRIMARY KEY (trade_date, ticker)
) WITHOUT ROWID;

-- Core: Index daily (KOSPI, KOSPI200, KOSDAQ, KRX300)
CREATE TABLE IF NOT EXISTS core_index_daily (
    trade_date          TEXT NOT NULL,
    index_code          TEXT NOT NULL,  -- canonical: KOSPI / KOSPI200 / KOSDAQ / KRX300
    open                REAL,
    high                REAL,
    low                 REAL,
    close               REAL,
    PRIMARY KEY (trade_date, index_code)
) WITHOUT ROWID;

-- Core: Quarterly financials
-- Values in 천원 (thousands KRW) as sourced from DataGuide.
-- available_date is the earliest date the data can be used (PIT-safe).
CREATE TABLE IF NOT EXISTS core_financials_quarterly (
    ticker                  TEXT NOT NULL,
    year                    TEXT NOT NULL,
    quarter                 TEXT NOT NULL,  -- '1Q','2Q','3Q','4Q'
    fiscal_month            TEXT,
    report_type             TEXT,
    period_end              TEXT NOT NULL,  -- YYYY-MM-DD (last day of quarter)
    available_date          TEXT NOT NULL,  -- period_end + lag (PIT-safe release date)
    total_assets            REAL,   -- 천원
    total_liabilities       REAL,   -- 천원
    total_equity_parent     REAL,   -- 천원
    sales                   REAL,   -- 천원
    cogs                    REAL,   -- 천원
    operating_income        REAL,   -- 천원
    net_income_parent       REAL,   -- 천원
    operating_cash_flow     REAL,   -- 천원
    cash_and_cash_equivalents REAL, -- 천원
    total_financial_debt    REAL,   -- 천원
    PRIMARY KEY (ticker, year, quarter)
);

-- Core: Regulatory status intervals
-- Converts event-based regulatory flags to intervals for efficient PIT lookup.
-- status_type: caution / warning / risk / admin / halt
-- For caution (no removal date): interval_end = interval_start (single day).
CREATE TABLE IF NOT EXISTS core_regulatory_status_interval (
    ticker              TEXT NOT NULL,
    status_type         TEXT NOT NULL,  -- caution/warning/risk/admin/halt
    interval_start      TEXT NOT NULL,  -- YYYY-MM-DD (inclusive)
    interval_end        TEXT NOT NULL,  -- YYYY-MM-DD (inclusive); '9999-12-31' if still active
    source_detail       TEXT,           -- caution_type, reason, etc.
    PRIMARY KEY (ticker, status_type, interval_start)
);

-- Core: Sector map (canonical tickers)
CREATE TABLE IF NOT EXISTS core_sector_map (
    ticker              TEXT NOT NULL,
    sector_name         TEXT NOT NULL,
    sector_code         TEXT,
    confidence          REAL,
    source              TEXT,
    PRIMARY KEY (ticker)
);

-- ============================================================
-- LAYER 3: MART TABLES
-- Backtest-ready, pre-computed tables.
-- ============================================================

-- Mart: Daily liquidity metrics
CREATE TABLE IF NOT EXISTS mart_liquidity_daily (
    trade_date              TEXT NOT NULL,
    ticker                  TEXT NOT NULL,
    adv5                    REAL,   -- 5-day avg traded value (KRW)
    adv20                   REAL,   -- 20-day avg traded value (KRW)
    market_cap              REAL,   -- KRW
    listing_age_bd          INTEGER, -- business days since listing (from core_calendar)
    is_above_3bn_adv5       INTEGER NOT NULL DEFAULT 0,
    is_above_100bn_mcap     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (trade_date, ticker)
) WITHOUT ROWID;

-- Mart: Universe eligibility (the key backtest filter table)
-- One row per trade_date × ticker for all tickers with price data.
-- Flags combine to produce is_eligible.
--
-- EXCLUDED RULE (missing data):
-- is_not_caution_watchlist (투자주의환기종목) is NOT implemented in v1.
-- TODO: Add when 투자주의환기종목 dataset is available.
CREATE TABLE IF NOT EXISTS mart_universe_eligibility_daily (
    trade_date              TEXT NOT NULL,
    ticker                  TEXT NOT NULL,
    is_listed               INTEGER NOT NULL DEFAULT 0,
    is_common_equity        INTEGER NOT NULL DEFAULT 0,
    is_market_ok            INTEGER NOT NULL DEFAULT 0,
    is_listing_age_ok       INTEGER NOT NULL DEFAULT 0,
    is_liquidity_ok         INTEGER NOT NULL DEFAULT 0,
    is_mcap_ok              INTEGER NOT NULL DEFAULT 0,
    is_not_caution          INTEGER NOT NULL DEFAULT 1,
    is_not_warning          INTEGER NOT NULL DEFAULT 1,
    is_not_risk             INTEGER NOT NULL DEFAULT 1,
    is_not_admin            INTEGER NOT NULL DEFAULT 1,
    is_not_halt             INTEGER NOT NULL DEFAULT 1,
    is_eligible             INTEGER NOT NULL DEFAULT 0,
    -- Bitmask of block reasons (bit 0=not_listed, 1=not_common_equity,
    -- 2=wrong_market, 3=too_new, 4=low_liquidity, 5=small_mcap,
    -- 6=caution, 7=warning, 8=risk, 9=admin, 10=halt)
    block_reason_mask       INTEGER NOT NULL DEFAULT 0,
    block_reason_json       TEXT,   -- human-readable JSON {"blocks": ["..."]}
    PRIMARY KEY (trade_date, ticker)
) WITHOUT ROWID;

-- Mart: Daily as-of fundamentals (PIT-safe: uses available_date, not period_end)
-- Forward-fills the most recently available quarterly report for each trade_date.
CREATE TABLE IF NOT EXISTS mart_fundamentals_asof_daily (
    trade_date              TEXT NOT NULL,
    ticker                  TEXT NOT NULL,
    -- Most recent available period info
    available_year          TEXT,
    available_quarter       TEXT,
    period_end              TEXT,
    available_date          TEXT,
    -- Financial fields (천원 = thousands KRW)
    total_assets            REAL,
    total_liabilities       REAL,
    total_equity_parent     REAL,
    sales                   REAL,
    cogs                    REAL,
    operating_income        REAL,
    net_income_parent       REAL,
    operating_cash_flow     REAL,
    cash_and_cash_equivalents REAL,
    total_financial_debt    REAL,
    PRIMARY KEY (trade_date, ticker)
) WITHOUT ROWID;

-- Mart: Sector weight snapshot
-- Approximated from aggregate market_cap by sector unless an official snapshot is supplied.
-- is_approximated = 1 means derived from market cap aggregation, not official data.
CREATE TABLE IF NOT EXISTS mart_sector_weight_snapshot (
    trade_date          TEXT NOT NULL,
    sector_name         TEXT NOT NULL,
    total_market_cap    REAL,   -- KRW, sum of market_cap of eligible stocks in sector
    constituent_count   INTEGER,
    sector_weight       REAL,   -- fraction of total market cap across all sectors
    is_approximated     INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (trade_date, sector_name)
) WITHOUT ROWID;

-- Mart: Daily feature set (starter set for strategy backtesting)
CREATE TABLE IF NOT EXISTS mart_feature_daily (
    trade_date          TEXT NOT NULL,
    ticker              TEXT NOT NULL,
    -- Price-based features
    ret_1d              REAL,   -- 1-day return (adj_close to adj_close)
    ret_5d              REAL,   -- 5-day return
    ret_20d             REAL,   -- 20-day return
    ret_60d             REAL,   -- 60-day return
    vol_20d             REAL,   -- 20-day realized volatility (std of ret_1d)
    turnover_ratio      REAL,   -- traded_value / market_cap
    price_to_52w_high   REAL,   -- close / max(close, 252 bdays)
    -- Fundamental features (PIT-safe from mart_fundamentals_asof_daily)
    sales_growth_yoy    REAL,   -- (sales_q / sales_q_1y_ago) - 1
    op_income_growth_yoy REAL,  -- (op_income_q / op_income_q_1y_ago) - 1
    net_debt_to_equity  REAL,   -- (total_financial_debt - cash) / total_equity_parent
    cash_to_assets      REAL,   -- cash_and_cash_equivalents / total_assets
    PRIMARY KEY (trade_date, ticker)
) WITHOUT ROWID;

-- ============================================================
-- LAYER 4: META TABLES
-- Metadata for LLM-based strategy compilation.
-- ============================================================

-- Meta: Field catalog
-- Maps field names to tables/columns, with LLM-friendly synonyms.
CREATE TABLE IF NOT EXISTS meta_field_catalog (
    field_id            TEXT NOT NULL,
    field_name_en       TEXT NOT NULL,
    field_name_ko       TEXT,
    table_name          TEXT NOT NULL,
    column_name         TEXT NOT NULL,
    dtype               TEXT,   -- REAL, INTEGER, TEXT
    frequency           TEXT,   -- daily, quarterly, static
    unit                TEXT,   -- KRW, percent, ratio, points, count
    source              TEXT,   -- DataGuide/KIND/sector
    lookahead_safe      INTEGER NOT NULL DEFAULT 1,  -- 1 if PIT-safe
    default_lag         INTEGER NOT NULL DEFAULT 0,  -- days of built-in lag
    description         TEXT,
    synonyms_json       TEXT,   -- JSON array of alternative names / Korean aliases
    PRIMARY KEY (field_id)
);

-- Meta: Dataset coverage
-- Coverage summary for each major dataset, including caveats.
CREATE TABLE IF NOT EXISTS meta_dataset_coverage (
    dataset_id          TEXT NOT NULL,
    dataset_name        TEXT NOT NULL,
    source_file         TEXT,
    coverage_start      TEXT,
    coverage_end        TEXT,
    frequency           TEXT,
    record_count        INTEGER,
    caveats             TEXT,
    PRIMARY KEY (dataset_id)
);

-- ============================================================
-- INDEXES
-- ============================================================

-- Performance indexes for common backtest queries
CREATE INDEX IF NOT EXISTS idx_price_ticker          ON core_price_daily (ticker, trade_date);
CREATE INDEX IF NOT EXISTS idx_eligibility_date      ON mart_universe_eligibility_daily (trade_date, is_eligible);
CREATE INDEX IF NOT EXISTS idx_eligibility_ticker    ON mart_universe_eligibility_daily (ticker, trade_date);
CREATE INDEX IF NOT EXISTS idx_liquidity_ticker      ON mart_liquidity_daily (ticker, trade_date);
CREATE INDEX IF NOT EXISTS idx_financials_ticker     ON core_financials_quarterly (ticker, year, quarter);
CREATE INDEX IF NOT EXISTS idx_fundamentals_ticker   ON mart_fundamentals_asof_daily (ticker, trade_date);
CREATE INDEX IF NOT EXISTS idx_features_ticker       ON mart_feature_daily (ticker, trade_date);
CREATE INDEX IF NOT EXISTS idx_regulatory_interval   ON core_regulatory_status_interval (ticker, status_type, interval_start, interval_end);
CREATE INDEX IF NOT EXISTS idx_manifest_run          ON raw_build_manifest (build_run_id);

-- Partial index: fast retrieval of eligible universe on any date
CREATE INDEX IF NOT EXISTS idx_eligible_universe
    ON mart_universe_eligibility_daily (trade_date, ticker)
    WHERE is_eligible = 1;
