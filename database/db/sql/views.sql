-- ============================================================
-- Convenience Views
-- ============================================================

-- View: Eligible universe on any date (join price + eligibility + sector)
CREATE VIEW IF NOT EXISTS v_eligible_universe AS
SELECT
    e.trade_date,
    e.ticker,
    s.corp_name,
    s.market_type,
    sec.sector_name,
    p.close,
    p.market_cap,
    p.traded_value,
    l.adv5,
    l.adv20,
    l.listing_age_bd
FROM mart_universe_eligibility_daily e
JOIN core_security_master s  ON e.ticker = s.ticker
JOIN core_price_daily p      ON e.trade_date = p.trade_date AND e.ticker = p.ticker
JOIN mart_liquidity_daily l  ON e.trade_date = l.trade_date AND e.ticker = l.ticker
LEFT JOIN core_sector_map sec ON e.ticker = sec.ticker
WHERE e.is_eligible = 1;

-- View: Full eligibility breakdown with reasons
CREATE VIEW IF NOT EXISTS v_eligibility_detail AS
SELECT
    e.trade_date,
    e.ticker,
    s.corp_name,
    s.market_type,
    e.is_listed,
    e.is_common_equity,
    e.is_market_ok,
    e.is_listing_age_ok,
    e.is_liquidity_ok,
    e.is_mcap_ok,
    e.is_not_caution,
    e.is_not_warning,
    e.is_not_risk,
    e.is_not_admin,
    e.is_not_halt,
    e.is_eligible,
    e.block_reason_json
FROM mart_universe_eligibility_daily e
LEFT JOIN core_security_master s ON e.ticker = s.ticker;

-- View: Latest available fundamentals per ticker (most recent trade date)
CREATE VIEW IF NOT EXISTS v_latest_fundamentals AS
SELECT f.*
FROM mart_fundamentals_asof_daily f
WHERE f.trade_date = (
    SELECT MAX(f2.trade_date) FROM mart_fundamentals_asof_daily f2
    WHERE f2.ticker = f.ticker
);

-- View: Feature set for eligible stocks only
CREATE VIEW IF NOT EXISTS v_features_eligible AS
SELECT
    ft.trade_date,
    ft.ticker,
    sec.sector_name,
    ft.ret_1d,
    ft.ret_5d,
    ft.ret_20d,
    ft.ret_60d,
    ft.vol_20d,
    ft.turnover_ratio,
    ft.price_to_52w_high,
    ft.sales_growth_yoy,
    ft.op_income_growth_yoy,
    ft.net_debt_to_equity,
    ft.cash_to_assets
FROM mart_feature_daily ft
JOIN mart_universe_eligibility_daily e
    ON ft.trade_date = e.trade_date AND ft.ticker = e.ticker
LEFT JOIN core_sector_map sec ON ft.ticker = sec.ticker
WHERE e.is_eligible = 1;
