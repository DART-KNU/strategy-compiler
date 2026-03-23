# DART Backtest DB - Validation Report

Generated: 2026-03-23 03:08:51 UTC

**Summary:** 53 PASS | 0 WARN | 0 FAIL

## Check Results

| Status | Check | Value | Message |
|--------|-------|-------|---------|
| ✅ PASS | `row_count.raw_kind_listed_companies_current` | 2765 | 2,765 rows |
| ✅ PASS | `row_count.raw_kind_delistings` | 1734 | 1,734 rows |
| ✅ PASS | `row_count.raw_kind_ipos` | 3000 | 3,000 rows |
| ✅ PASS | `row_count.raw_kind_investment_caution` | 3000 | 3,000 rows |
| ✅ PASS | `row_count.raw_kind_investment_warning` | 981 | 981 rows |
| ✅ PASS | `row_count.raw_kind_investment_risk` | 80 | 80 rows |
| ✅ PASS | `row_count.raw_dg_index_daily` | 20448 | 20,448 rows |
| ✅ PASS | `row_count.raw_dg_stock_daily` | 50040552 | 50,040,552 rows |
| ✅ PASS | `row_count.raw_dg_financials_quarterly` | 768836 | 768,836 rows |
| ✅ PASS | `row_count.raw_sector_map` | 3835 | 3,835 rows |
| ✅ PASS | `row_count.core_security_master` | 3566 | 3,566 rows |
| ✅ PASS | `row_count.core_calendar` | 1278 | 1,278 rows |
| ✅ PASS | `row_count.core_price_daily` | 3209299 | 3,209,299 rows |
| ✅ PASS | `row_count.core_index_daily` | 5112 | 5,112 rows |
| ✅ PASS | `row_count.core_financials_quarterly` | 81309 | 81,309 rows |
| ✅ PASS | `row_count.core_regulatory_status_interval` | 3733 | 3,733 rows |
| ✅ PASS | `row_count.core_sector_map` | 3835 | 3,835 rows |
| ✅ PASS | `row_count.mart_liquidity_daily` | 3209299 | 3,209,299 rows |
| ✅ PASS | `row_count.mart_universe_eligibility_daily` | 3209299 | 3,209,299 rows |
| ✅ PASS | `row_count.mart_fundamentals_asof_daily` | 3203102 | 3,203,102 rows |
| ✅ PASS | `row_count.mart_feature_daily` | 3209299 | 3,209,299 rows |
| ✅ PASS | `row_count.meta_field_catalog` | 37 | 37 rows |
| ✅ PASS | `row_count.meta_dataset_coverage` | 11 | 11 rows |
| ✅ PASS | `date_range.core_price_daily.trade_date` | 2020-12-30 to 2026-03-20 | 2020-12-30 to 2026-03-20 |
| ✅ PASS | `date_range.core_index_daily.trade_date` | 2020-12-30 to 2026-03-20 | 2020-12-30 to 2026-03-20 |
| ✅ PASS | `date_range.core_calendar.trade_date` | 2020-12-30 to 2026-03-20 | 2020-12-30 to 2026-03-20 |
| ✅ PASS | `date_range.core_financials_quarterly.period_end` | 2018-03-31 to 2026-12-31 | 2018-03-31 to 2026-12-31 |
| ✅ PASS | `date_range.mart_universe_eligibility_daily.trade_date` | 2020-12-30 to 2026-03-20 | 2020-12-30 to 2026-03-20 |
| ✅ PASS | `missingness.core_price_daily.close` | 0.0% | 0.0% missing |
| ✅ PASS | `missingness.core_price_daily.adj_close` | 0.0% | 0.0% missing |
| ✅ PASS | `missingness.core_price_daily.market_cap` | 0.0% | 0.0% missing |
| ✅ PASS | `missingness.core_price_daily.traded_value` | 0.2% | 0.2% missing |
| ✅ PASS | `missingness.core_security_master.listing_date` | 0.0% | 0.0% missing |
| ✅ PASS | `missingness.core_sector_map.sector_name` | 0.0% | 0.0% missing |
| ✅ PASS | `eligibility.eligible_count` | 366 | On 2026-03-20: 366 eligible / 2667 total (13.7%) |
| ✅ PASS | `pit.new_listing_block` | 0 | No eligible stocks with listing_age_bd < 6 |
| ✅ PASS | `pit.warning_flag` | 0 | All warning intervals correctly blocked |
| ✅ PASS | `pit.risk_flag` | 0 | All risk intervals correctly blocked |
| ✅ PASS | `pit.mcap_filter` | 0 | No eligible stocks below 100bn KRW market cap |
| ✅ PASS | `pit.financial_lag` | 0 | All available_date >= period_end |
| ✅ PASS | `dup_pk.core_security_master` | 0 | No duplicate PKs |
| ✅ PASS | `dup_pk.core_price_daily` | 0 | No duplicate PKs |
| ✅ PASS | `dup_pk.core_financials_quarterly` | 0 | No duplicate PKs |
| ✅ PASS | `dup_pk.mart_universe_eligibility_daily` | 0 | No duplicate PKs |
| ✅ PASS | `dup_pk.mart_liquidity_daily` | 0 | No duplicate PKs |
| ✅ PASS | `dup_pk.mart_fundamentals_asof_daily` | 0 | No duplicate PKs |
| ✅ PASS | `dup_pk.mart_feature_daily` | 0 | No duplicate PKs |
| ✅ PASS | `financial_lag.q4_90d` | 0 | All Q4 available_date >= period_end + 90 days |
| ✅ PASS | `financial_lag.q123_45d` | 0 | All Q1-Q3 available_date >= period_end + 45 days |
| ✅ PASS | `sector.coverage` | 93.0% | 2737 of 2942 tickers have sector mapping (205 without) |
| ✅ PASS | `sector.distribution` | 11 | Sectors: 금융:261, 부동산:18, 산업:786, 소재:471, 에너지:33, 유틸리티:22, 임의소비재:483, 정보기술:943, 커뮤니케이션:210, 필수소비재:200, 헬스케어:408 |
| ✅ PASS | `manifest.latest_run` | 20260322T175441_6473769e | 9 files recorded in latest build |
| ✅ PASS | `manifest.checksums` | 0 | All manifest files have checksums |

## Row Counts by Table

| Table | Row Count |
|-------|-----------|
| `raw_kind_listed_companies_current` | 2,765 |
| `raw_kind_delistings` | 1,734 |
| `raw_kind_ipos` | 3,000 |
| `raw_kind_stock_issuance` | 2,662 |
| `raw_kind_investment_caution` | 3,000 |
| `raw_kind_investment_warning` | 981 |
| `raw_kind_investment_risk` | 80 |
| `raw_dg_index_daily` | 20,448 |
| `raw_dg_stock_daily` | 50,040,552 |
| `raw_dg_financials_quarterly` | 768,836 |
| `raw_sector_map` | 3,835 |
| `raw_build_manifest` | 9 |
| `core_security_master` | 3,566 |
| `core_calendar` | 1,278 |
| `core_price_daily` | 3,209,299 |
| `core_index_daily` | 5,112 |
| `core_financials_quarterly` | 81,309 |
| `core_regulatory_status_interval` | 3,733 |
| `core_sector_map` | 3,835 |
| `mart_liquidity_daily` | 3,209,299 |
| `mart_universe_eligibility_daily` | 3,209,299 |
| `mart_fundamentals_asof_daily` | 3,203,102 |
| `mart_feature_daily` | 3,209,299 |
| `mart_sector_weight_snapshot` | 0 |
| `meta_field_catalog` | 37 |
| `meta_dataset_coverage` | 11 |

## Date Coverage

| Table | Column | Min Date | Max Date |
|-------|--------|----------|----------|
| `core_price_daily` | `trade_date` | 2020-12-30 | 2026-03-20 |
| `core_index_daily` | `trade_date` | 2020-12-30 | 2026-03-20 |
| `core_calendar` | `trade_date` | 2020-12-30 | 2026-03-20 |
| `core_financials_quarterly` | `period_end` | 2018-03-31 | 2026-12-31 |
| `core_financials_quarterly` | `available_date` | 2018-05-15 | 2027-03-31 |
| `mart_universe_eligibility_daily` | `trade_date` | 2020-12-30 | 2026-03-20 |
| `mart_feature_daily` | `trade_date` | 2020-12-30 | 2026-03-20 |

## Eligible Universe Size (Sample Dates)

| Date | Eligible | Total | Pct |
|------|----------|-------|-----|
| 2026-03-20 | 366 | 2,667 | 13.7% |
| 2026-03-19 | 357 | 2,666 | 13.4% |
| 2026-03-18 | 358 | 2,666 | 13.4% |
| 2026-03-17 | 356 | 2,667 | 13.3% |
| 2026-03-16 | 358 | 2,668 | 13.4% |
| 2026-03-13 | 359 | 2,669 | 13.5% |
| 2026-03-12 | 350 | 2,669 | 13.1% |
| 2026-03-11 | 369 | 2,669 | 13.8% |
| 2026-03-10 | 394 | 2,669 | 14.8% |
| 2026-03-09 | 418 | 2,669 | 15.7% |

## Sample Queries

### Was ticker X eligible on date T?
```sql
SELECT is_eligible, block_reason_json
FROM mart_universe_eligibility_daily
WHERE ticker = '005930' AND trade_date = '2024-01-15';
```

### Why was ticker X blocked on date T?
```sql
SELECT
    ticker, trade_date,
    is_listed, is_common_equity, is_market_ok,
    is_listing_age_ok, is_liquidity_ok, is_mcap_ok,
    is_not_caution, is_not_warning, is_not_risk,
    is_not_admin, is_not_halt,
    block_reason_json
FROM mart_universe_eligibility_daily
WHERE ticker = '005930' AND trade_date = '2024-01-15';
```

### Show eligible universe on date T
```sql
SELECT
    e.ticker,
    s.corp_name,
    s.market_type,
    sec.sector_name,
    p.market_cap / 1e8 AS mcap_100m_krw,
    l.adv5 / 1e9 AS adv5_bn_krw
FROM mart_universe_eligibility_daily e
JOIN core_security_master s ON e.ticker = s.ticker
JOIN core_price_daily p ON e.trade_date = p.trade_date AND e.ticker = p.ticker
JOIN mart_liquidity_daily l ON e.trade_date = l.trade_date AND e.ticker = l.ticker
LEFT JOIN core_sector_map sec ON e.ticker = sec.ticker
WHERE e.trade_date = '2024-01-15' AND e.is_eligible = 1
ORDER BY p.market_cap DESC;
```

### PIT-safe financials for ticker X as of date T
```sql
SELECT *
FROM mart_fundamentals_asof_daily
WHERE ticker = '005930' AND trade_date = '2024-01-15';
```

### Sector weights on date T
```sql
SELECT sector_name, constituent_count, sector_weight, is_approximated
FROM mart_sector_weight_snapshot
WHERE trade_date = '2024-01-15'
ORDER BY sector_weight DESC;
```

## Live Sample Query Results

### Eligible universe on 2026-03-20 (top 10 by market cap)

| Ticker | Corp Name | Market | Sector | McapBnKRW | ADV5BnKRW |
|--------|-----------|--------|--------|-----------|-----------|
| 000250 | 삼천당제약 | 코스닥 | 헬스케어 | 212.76 | 238.7 |
| 086520 | 에코프로 | 코스닥 | 산업 | 204.89 | 159.1 |
| 196170 | 알테오젠 | 코스닥 | 헬스케어 | 189.24 | 82.6 |
| 247540 | 에코프로비엠 | 코스닥 | 산업 | 187.74 | 86.1 |
| 277810 | 레인보우로보틱스 | 코스닥 | 산업 | 127.85 | 138.1 |
| 298380 | 에이비엘바이오 | 코스닥 | 헬스케어 | 106.66 | 105.7 |
| 058470 | 리노공업 | 코스닥 | 정보기술 | 85.13 | 86.3 |
| 087010 | 펩트론 | 코스닥 | 헬스케어 | 80.68 | 77.3 |
| 141080 | 레고켐바이오 | 코스닥 | 헬스케어 | 77.98 | 95.4 |
| 028300 | HLB | 코스닥 | 헬스케어 | 68.82 | 29.4 |

---
*Report generated by DART Backtest DB validation pipeline*