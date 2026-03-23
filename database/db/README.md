# DART Backtest DB

A point-in-time-safe SQLite database for Korean equity backtesting.

## Quick Start

```bash
cd C:\Users\cmsch\Desktop\DART-backtest-NL\database\db

# 1. Install dependencies
pip install -r requirements.txt

# 2. Preview what will happen (no DB created)
python -m src.main dry-run --config configs/config.example.yaml

# 3. Build the full database
python -m src.main build --config configs/config.example.yaml

# 4. Validate an existing database
python -m src.main validate --config configs/config.example.yaml

# 5. Inspect counts and a specific ticker/date
python -m src.main inspect --config configs/config.example.yaml
python -m src.main inspect --config configs/config.example.yaml --ticker 005930 --date 2024-01-15

# 6. Run unit tests
python -m pytest tests/ -v
```

## Data File Placement

All input files must exist before running `build`. Place them exactly here:

```
database/
  raw/
    kind/
      상장법인목록.xls          ← Currently listed companies (current snapshot)
      상장폐지현황.xls           ← Delisting history (1999–2026)
      신규상장기업현황.xls        ← IPO / new listing history (2001–2026)
      주식발행내역.xls           ← Stock issuance history
      투자경고종목.xls           ← Investment warning (recent 3 years)
      투자위험종목.xls           ← Investment risk (recent 3 years)
      투자주의종목.xls           ← Investment caution (recent 3 years)
    sector/
      sector allocation_filled_reprocessed.xlsx   ← EXACT filename required
    dataguide.xlsx             ← DataGuide export (412 MB; ~3 sheets)
```

**Important:**
- KIND files are HTML served as `.xls` by KRX. The code handles this automatically.
- The sector file path is fixed and hardcoded in config. Do not rename it.
- Other files in `raw/sector/` are ignored.
- `dataguide.xlsx` has 3 sheets: `bm` (index), `type1` (stock daily), `type2` (financials).

## Project Structure

```
db/
  README.md
  requirements.txt
  pyproject.toml
  configs/
    config.example.yaml      ← Copy and edit for custom paths
  artifacts/                 ← Build logs, validation reports, manifest
    build_YYYYMMDD.log
    validation_report.md
    manifest.csv
  data/db/
    backtest.db              ← Output SQLite database
  sql/
    schema.sql               ← All CREATE TABLE / INDEX statements
    views.sql                ← Convenience views
  src/
    main.py                  ← CLI entry point
    config.py                ← Config loader
    db.py                    ← SQLite connection / schema helpers
    utils/
      ticker.py              ← Ticker normalization (A-prefix removal)
      hashing.py             ← SHA-256 file checksums
      paths.py               ← Path resolution
      io.py                  ← File reading utilities
      calendar_utils.py      ← Date / calendar utilities
    ingest/
      kind.py                ← KIND HTML-as-XLS ingestion
      dataguide.py           ← DataGuide Excel ingestion (wide→long)
      sectors.py             ← Sector file ingestion
    transform/
      manifest.py            ← Build manifest / checksums
      security_master.py     ← core_security_master
      calendar.py            ← core_calendar
      prices.py              ← core_price_daily, core_index_daily
      financials.py          ← core_financials_quarterly, mart_fundamentals_asof_daily
      regulatory.py          ← core_regulatory_status_interval, core_sector_map
      eligibility.py         ← mart_liquidity_daily, mart_universe_eligibility_daily
      features.py            ← mart_feature_daily
      metadata.py            ← meta_field_catalog, meta_dataset_coverage
    validate/
      checks.py              ← Validation checks
      report.py              ← Report generation
  tests/
    test_ticker.py           ← Ticker normalization unit tests
    test_eligibility.py      ← Eligibility logic unit tests
    test_financials_asof.py  ← PIT-safe financial lag tests
```

## Database Layers

### Layer 1: raw_*
Raw ingestion, mirrors source files. Long format for DataGuide price/financial data.

| Table | Description |
|-------|-------------|
| `raw_kind_listed_companies_current` | Current KRX listed companies snapshot |
| `raw_kind_delistings` | Full delisting history |
| `raw_kind_ipos` | IPO / new listing history |
| `raw_kind_stock_issuance` | Stock issuance history (v1: stored only) |
| `raw_kind_investment_caution` | Investment caution designations |
| `raw_kind_investment_warning` | Investment warning designations |
| `raw_kind_investment_risk` | Investment risk designations |
| `raw_dg_index_daily` | Index daily OHLC (long format) |
| `raw_dg_stock_daily` | Stock daily fields (long format, ~90M rows) |
| `raw_dg_financials_quarterly` | Quarterly financials (long format) |
| `raw_sector_map` | Raw sector mapping with original tickers |
| `raw_build_manifest` | File checksums and build metadata |

### Layer 2: core_*
Normalized, canonical, point-in-time-aware.

| Table | Description |
|-------|-------------|
| `core_security_master` | All tickers with listing/delisting dates and flags |
| `core_calendar` | Trading calendar (derived from DataGuide dates) |
| `core_price_daily` | Wide-format daily prices (one row per ticker × date) |
| `core_index_daily` | KOSPI/KOSPI200/KOSDAQ/KRX300 daily OHLC |
| `core_financials_quarterly` | Quarterly financials with `available_date` |
| `core_regulatory_status_interval` | Regulatory flags as [start, end] intervals |
| `core_sector_map` | Canonical ticker → sector mapping |

### Layer 3: mart_*
Backtest-ready pre-computed tables.

| Table | Description |
|-------|-------------|
| `mart_liquidity_daily` | ADV5, ADV20, listing age, threshold flags |
| `mart_universe_eligibility_daily` | **KEY TABLE**: Is stock eligible today? Why not? |
| `mart_fundamentals_asof_daily` | PIT-safe as-of financials for each trade date |
| `mart_feature_daily` | Price + fundamental feature set |
| `mart_sector_weight_snapshot` | Sector market cap weights (approximated) |

### Layer 4: meta_*
For LLM / natural-language strategy compilation.

| Table | Description |
|-------|-------------|
| `meta_field_catalog` | Field dictionary with Korean/English synonyms |
| `meta_dataset_coverage` | Coverage ranges and caveats per dataset |

## Key Design Decisions

### Point-in-Time Safety

1. **Security master**: built from IPO history (interval-aware), not just current snapshot.
2. **Financials**: `available_date` = `period_end` + conservative lag (45d/90d).
   `mart_fundamentals_asof_daily` uses `available_date <= trade_date` for the join.
3. **Regulatory flags**: expanded to intervals in `core_regulatory_status_interval`.
4. **Listing age**: computed from listing_date to each trade_date using trading days only.
5. **Calendar**: derived from DataGuide dates (only trading days present in DataGuide).

### Ticker Normalization

All tickers are normalized to canonical **6-digit numeric strings**:
- `A005930` → `005930` (remove DataGuide 'A' prefix)
- Leading zeros are preserved: `000010` stays `000010`
- Raw original tickers are kept in `raw_*` tables

### Eligibility Rules

```
is_eligible = 1 iff ALL of:
  1. is_listed              — listed on KRX on this date
  2. is_common_equity       — security_type = '주권' (common stock)
  3. is_market_ok           — market_type in {코스피, 코스닥}
  4. is_listing_age_ok      — >= 6 business days since listing
  5. is_liquidity_ok        — ADV5 > 3,000,000,000 KRW
  6. is_mcap_ok             — market_cap >= 100,000,000,000 KRW
  7. is_not_caution         — not in investment caution on this date
  8. is_not_warning         — not in investment warning interval
  9. is_not_risk            — not in investment risk interval
  10. is_not_admin          — no admin supervision flag
  11. is_not_halt           — no trading halt flag

EXCLUDED (missing data):
  투자주의환기종목 (investment caution watchlist) — TODO
```

### Financial Units

All financial values from DataGuide are in **천원 (thousands KRW)** as sourced.
Do NOT multiply by 1000 before comparing; check `meta_field_catalog.unit`.

### Sector Mapping

The frozen contest sector taxonomy uses the file:
`raw/sector/sector allocation_filled_reprocessed.xlsx`

11 sectors: 정보기술, 산업, 임의소비재, 헬스케어, 소재, 필수소비재,
커뮤니케이션, 금융, 에너지, 유틸리티, 부동산

Sector weights in `mart_sector_weight_snapshot` are **approximated** from
aggregate market cap of eligible constituents (`is_approximated = 1`).
An official fixed snapshot would replace this.

## Performance Notes

### Build Time

| Step | Expected time |
|------|--------------|
| KIND files (7 × HTML-XLS) | < 5 min |
| Sector file | < 1 min |
| DataGuide bm sheet | < 1 min |
| DataGuide type1 sheet (stock daily) | **10-30 min** (412 MB file, streaming) |
| DataGuide type2 sheet (financials) | 2-5 min |
| Core table builds | 10-20 min |
| Mart table builds | 20-60 min |
| **Total first build** | **~1-2 hours** |

Subsequent builds skip unchanged files (checksum-based). If the source files
haven't changed, the build completes in minutes.

### Database Size

| Table | Approximate size |
|-------|-----------------|
| raw_dg_stock_daily | ~2-4 GB |
| core_price_daily | ~500 MB |
| mart_universe_eligibility_daily | ~500 MB |
| Other tables | < 100 MB each |
| **Total** | **~5-8 GB** |

## Sample Queries

### Was stock 005930 eligible on 2024-01-15?
```sql
SELECT is_eligible, block_reason_json
FROM mart_universe_eligibility_daily
WHERE ticker = '005930' AND trade_date = '2024-01-15';
```

### Show eligible universe on 2024-01-15
```sql
SELECT e.ticker, s.corp_name, s.market_type, sec.sector_name,
       p.market_cap / 1e11 AS mcap_100bn_krw
FROM mart_universe_eligibility_daily e
JOIN core_security_master s  ON e.ticker = s.ticker
JOIN core_price_daily p      ON e.trade_date = p.trade_date AND e.ticker = p.ticker
LEFT JOIN core_sector_map sec ON e.ticker = sec.ticker
WHERE e.trade_date = '2024-01-15' AND e.is_eligible = 1
ORDER BY p.market_cap DESC;
```

### PIT-safe financials as of 2024-04-01
```sql
SELECT ticker, available_year, available_quarter, period_end, available_date,
       sales, operating_income
FROM mart_fundamentals_asof_daily
WHERE trade_date = '2024-04-01'
ORDER BY sales DESC LIMIT 10;
```

### Sector breakdown of eligible universe
```sql
SELECT sec.sector_name, COUNT(*) as n,
       SUM(p.market_cap) / 1e12 as total_mcap_tn
FROM mart_universe_eligibility_daily e
JOIN core_price_daily p ON e.trade_date = p.trade_date AND e.ticker = p.ticker
JOIN core_sector_map sec ON e.ticker = sec.ticker
WHERE e.trade_date = '2024-01-15' AND e.is_eligible = 1
GROUP BY sec.sector_name ORDER BY total_mcap_tn DESC;
```

## Current Limitations (v1)

1. **Stock issuance**: `raw_kind_stock_issuance` is stored but not transformed.
   Corporate action history is not reconstructed; DataGuide `adj_factor` is used instead.

2. **Investment caution watchlist (투자주의환기종목)**: NOT implemented.
   Data not available. TODO placeholder in eligibility code.

3. **Market type for historical stocks**: Delisted stocks not in the current listed
   companies file may have `market_type = 'UNKNOWN'`. These are excluded from the
   KOSPI+KOSDAQ eligible universe.

4. **Non-December fiscal year**: `period_end` uses calendar quarter ends for all
   companies. For companies with non-December fiscal year ends, this is an
   approximation. The conservative lag policy mitigates look-ahead risk.

5. **Regulatory data coverage**: KIND caution/warning/risk files cover recent 3 years
   only. Pre-2022 regulatory status is not available.

6. **Sector weights**: Derived from market cap aggregation, not official index data.
   Use `mart_sector_weight_snapshot.is_approximated = 1` to filter these.

7. **Financial values unit**: All in 천원 (thousands KRW). Check `meta_field_catalog.unit`
   before comparisons.

## Configuration

Copy `configs/config.example.yaml` to `configs/config.yaml` and adjust:
- `paths.raw_root`: path to the raw data directory
- `financial_lag.q4_days`: Q4 filing lag (default: 90 days)
- `eligibility.min_adv5_krw`: ADV5 threshold (default: 3,000,000,000)
- `eligibility.min_mcap_krw`: market cap threshold (default: 100,000,000,000)
- `eligibility.min_listing_age_bd`: listing age threshold (default: 6 business days)
