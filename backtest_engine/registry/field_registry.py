"""
Field Registry — maps field_id strings to DB source tables/columns with metadata.

Used by:
- FieldNode in node graphs (resolves field_id -> table.column)
- SQL loaders (builds SELECT lists)
- Semantic validator (checks field existence)
- describe_dataset API
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class FieldMeta:
    """Metadata for a single loadable field."""
    field_id: str
    table_name: str
    column_name: str
    dtype: str                  # REAL | INTEGER | TEXT
    frequency: str              # daily | quarterly | static
    unit: str                   # KRW | percent | ratio | count | bool | index_points
    lookahead_safe: bool        # True if can be used without additional PIT lag
    default_lag: int            # Built-in lag in business days (0 = no extra lag)
    description: str
    allowed_ops: List[str]      # ops that make sense on this field
    notes: Optional[str] = None
    synonyms: List[str] = field(default_factory=list)


# ============================================================
# Universe / eligibility fields
# ============================================================
_ELIGIBILITY = [
    FieldMeta("is_eligible", "mart_universe_eligibility_daily", "is_eligible",
              "INTEGER", "daily", "bool", True, 0,
              "1 if stock passes all eligibility filters (the main investable universe gate)",
              ["predicate"], synonyms=["투자가능", "eligible"]),
    FieldMeta("is_listed", "mart_universe_eligibility_daily", "is_listed",
              "INTEGER", "daily", "bool", True, 0,
              "1 if stock is currently listed", ["predicate"]),
    FieldMeta("is_common_equity", "mart_universe_eligibility_daily", "is_common_equity",
              "INTEGER", "daily", "bool", True, 0,
              "1 if security type is 주권 (common equity)", ["predicate"]),
    FieldMeta("is_market_ok", "mart_universe_eligibility_daily", "is_market_ok",
              "INTEGER", "daily", "bool", True, 0,
              "1 if market type is KOSPI or KOSDAQ", ["predicate"]),
    FieldMeta("is_listing_age_ok", "mart_universe_eligibility_daily", "is_listing_age_ok",
              "INTEGER", "daily", "bool", True, 0,
              "1 if listed for >= 6 business days", ["predicate"]),
    FieldMeta("is_liquidity_ok", "mart_universe_eligibility_daily", "is_liquidity_ok",
              "INTEGER", "daily", "bool", True, 0,
              "1 if 5-day ADV >= 3 billion KRW", ["predicate"]),
    FieldMeta("is_mcap_ok", "mart_universe_eligibility_daily", "is_mcap_ok",
              "INTEGER", "daily", "bool", True, 0,
              "1 if market cap >= 100 billion KRW", ["predicate"]),
    FieldMeta("is_not_caution", "mart_universe_eligibility_daily", "is_not_caution",
              "INTEGER", "daily", "bool", True, 0,
              "1 if not designated as investment caution (투자주의)", ["predicate"]),
    FieldMeta("is_not_warning", "mart_universe_eligibility_daily", "is_not_warning",
              "INTEGER", "daily", "bool", True, 0,
              "1 if not investment warning (투자경고)", ["predicate"]),
    FieldMeta("is_not_risk", "mart_universe_eligibility_daily", "is_not_risk",
              "INTEGER", "daily", "bool", True, 0,
              "1 if not investment risk (투자위험)", ["predicate"]),
    FieldMeta("is_not_admin", "mart_universe_eligibility_daily", "is_not_admin",
              "INTEGER", "daily", "bool", True, 0,
              "1 if not under administrative supervision (관리종목)", ["predicate"]),
    FieldMeta("is_not_halt", "mart_universe_eligibility_daily", "is_not_halt",
              "INTEGER", "daily", "bool", True, 0,
              "1 if not trading halted (거래정지)", ["predicate"]),
    FieldMeta("block_reason_mask", "mart_universe_eligibility_daily", "block_reason_mask",
              "INTEGER", "daily", "count", True, 0,
              "Bitmask of eligibility block reasons", ["predicate"]),
]

# ============================================================
# Price / liquidity fields
# ============================================================
_PRICE = [
    FieldMeta("open", "core_price_daily", "open",
              "REAL", "daily", "KRW", True, 0,
              "Opening price (unadjusted)", ["ts_op", "cs_op", "combine"]),
    FieldMeta("high", "core_price_daily", "high",
              "REAL", "daily", "KRW", True, 0,
              "Daily high (unadjusted)", ["ts_op", "cs_op"]),
    FieldMeta("low", "core_price_daily", "low",
              "REAL", "daily", "KRW", True, 0,
              "Daily low (unadjusted)", ["ts_op", "cs_op"]),
    FieldMeta("close", "core_price_daily", "close",
              "REAL", "daily", "KRW", True, 0,
              "Closing price (unadjusted)", ["ts_op", "cs_op", "combine"]),
    FieldMeta("adj_open", "core_price_daily", "adj_open",
              "REAL", "daily", "KRW", True, 0,
              "Adjustment-factor-corrected open", ["ts_op", "cs_op", "combine"]),
    FieldMeta("adj_high", "core_price_daily", "adj_high",
              "REAL", "daily", "KRW", True, 0,
              "Adjustment-factor-corrected high", ["ts_op"]),
    FieldMeta("adj_low", "core_price_daily", "adj_low",
              "REAL", "daily", "KRW", True, 0,
              "Adjustment-factor-corrected low", ["ts_op"]),
    FieldMeta("adj_close", "core_price_daily", "adj_close",
              "REAL", "daily", "KRW", True, 0,
              "Adjustment-factor-corrected close (use for returns)", ["ts_op", "cs_op", "combine"],
              synonyms=["수정주가", "adjusted_close"]),
    FieldMeta("volume", "core_price_daily", "volume",
              "REAL", "daily", "shares", True, 0,
              "Trading volume in shares", ["ts_op", "cs_op"]),
    FieldMeta("traded_value", "core_price_daily", "traded_value",
              "REAL", "daily", "KRW", True, 0,
              "Daily traded value (KRW)", ["ts_op", "cs_op", "combine"],
              synonyms=["거래대금"]),
    FieldMeta("market_cap", "core_price_daily", "market_cap",
              "REAL", "daily", "KRW", True, 0,
              "Market capitalization (KRW)", ["ts_op", "cs_op", "combine"],
              synonyms=["시가총액", "mcap"]),
    FieldMeta("float_shares", "core_price_daily", "float_shares",
              "REAL", "daily", "shares", True, 0,
              "Free-float shares outstanding", ["ts_op"]),
    FieldMeta("float_ratio", "core_price_daily", "float_ratio",
              "REAL", "daily", "percent", True, 0,
              "Free-float ratio (%)", ["ts_op", "cs_op"]),
    FieldMeta("shares_outstanding", "core_price_daily", "shares_outstanding",
              "REAL", "daily", "shares", True, 0,
              "Total shares outstanding", ["ts_op"]),
    FieldMeta("adj_factor", "core_price_daily", "adj_factor",
              "REAL", "daily", "ratio", True, 0,
              "Price adjustment factor (for return calculation)", ["ts_op"]),
]

_LIQUIDITY = [
    FieldMeta("adv5", "mart_liquidity_daily", "adv5",
              "REAL", "daily", "KRW", True, 0,
              "5-day average daily traded value (KRW)", ["ts_op", "cs_op", "predicate"],
              synonyms=["5일평균거래대금"]),
    FieldMeta("adv20", "mart_liquidity_daily", "adv20",
              "REAL", "daily", "KRW", True, 0,
              "20-day average daily traded value (KRW)", ["ts_op", "cs_op", "predicate"]),
    FieldMeta("listing_age_bd", "mart_liquidity_daily", "listing_age_bd",
              "INTEGER", "daily", "count", True, 0,
              "Number of business days since listing", ["predicate", "ts_op"]),
]

# ============================================================
# Starter features (mart_feature_daily — already computed)
# ============================================================
_FEATURES = [
    FieldMeta("ret_1d", "mart_feature_daily", "ret_1d",
              "REAL", "daily", "ratio", True, 0,
              "1-day return (adj_close to adj_close)", ["ts_op", "cs_op", "combine"],
              synonyms=["1일수익률"]),
    FieldMeta("ret_5d", "mart_feature_daily", "ret_5d",
              "REAL", "daily", "ratio", True, 0,
              "5-day return", ["ts_op", "cs_op", "combine"],
              synonyms=["5일수익률", "1주일수익률"]),
    FieldMeta("ret_20d", "mart_feature_daily", "ret_20d",
              "REAL", "daily", "ratio", True, 0,
              "20-day return (~1 month)", ["ts_op", "cs_op", "combine"],
              synonyms=["20일수익률", "1개월수익률"]),
    FieldMeta("ret_60d", "mart_feature_daily", "ret_60d",
              "REAL", "daily", "ratio", True, 0,
              "60-day return (~3 months)", ["ts_op", "cs_op", "combine"],
              synonyms=["60일수익률", "3개월수익률"]),
    FieldMeta("vol_20d", "mart_feature_daily", "vol_20d",
              "REAL", "daily", "ratio", True, 0,
              "20-day realized volatility (annualized std of daily returns)",
              ["ts_op", "cs_op", "combine"],
              synonyms=["변동성", "volatility"]),
    FieldMeta("turnover_ratio", "mart_feature_daily", "turnover_ratio",
              "REAL", "daily", "ratio", True, 0,
              "Traded value / market cap", ["ts_op", "cs_op", "combine"]),
    FieldMeta("price_to_52w_high", "mart_feature_daily", "price_to_52w_high",
              "REAL", "daily", "ratio", True, 0,
              "Close / 52-week high — proximity to 52w high", ["ts_op", "cs_op", "combine"],
              synonyms=["52주최고비율"]),
    FieldMeta("sales_growth_yoy", "mart_feature_daily", "sales_growth_yoy",
              "REAL", "daily", "ratio", True, 0,
              "Sales year-over-year growth (PIT-safe from available_date)",
              ["cs_op", "combine", "predicate"],
              synonyms=["매출성장률", "revenue_growth"]),
    FieldMeta("op_income_growth_yoy", "mart_feature_daily", "op_income_growth_yoy",
              "REAL", "daily", "ratio", True, 0,
              "Operating income YoY growth (PIT-safe)", ["cs_op", "combine"],
              synonyms=["영업이익성장률"]),
    FieldMeta("net_debt_to_equity", "mart_feature_daily", "net_debt_to_equity",
              "REAL", "daily", "ratio", True, 0,
              "Net debt / equity (financial leverage, PIT-safe)", ["cs_op", "combine"],
              synonyms=["순부채비율"]),
    FieldMeta("cash_to_assets", "mart_feature_daily", "cash_to_assets",
              "REAL", "daily", "ratio", True, 0,
              "Cash & equivalents / total assets (PIT-safe)", ["cs_op", "combine"],
              synonyms=["현금비율"]),
]

# ============================================================
# Fundamentals (mart_fundamentals_asof_daily — PIT-safe)
# ============================================================
_FUNDAMENTALS = [
    FieldMeta("total_assets", "mart_fundamentals_asof_daily", "total_assets",
              "REAL", "daily", "KRW_thousands", True, 0,
              "Total assets (천원, PIT-safe as-of)", ["cs_op", "combine"]),
    FieldMeta("total_liabilities", "mart_fundamentals_asof_daily", "total_liabilities",
              "REAL", "daily", "KRW_thousands", True, 0,
              "Total liabilities (천원, PIT-safe)", ["cs_op", "combine"]),
    FieldMeta("total_equity_parent", "mart_fundamentals_asof_daily", "total_equity_parent",
              "REAL", "daily", "KRW_thousands", True, 0,
              "Parent equity (천원, PIT-safe)", ["cs_op", "combine"],
              synonyms=["자본총계", "equity"]),
    FieldMeta("sales", "mart_fundamentals_asof_daily", "sales",
              "REAL", "daily", "KRW_thousands", True, 0,
              "Sales/Revenue (천원, PIT-safe)", ["cs_op", "combine"],
              synonyms=["매출액", "revenue"]),
    FieldMeta("cogs", "mart_fundamentals_asof_daily", "cogs",
              "REAL", "daily", "KRW_thousands", True, 0,
              "Cost of goods sold (천원, PIT-safe)", ["cs_op"]),
    FieldMeta("operating_income", "mart_fundamentals_asof_daily", "operating_income",
              "REAL", "daily", "KRW_thousands", True, 0,
              "Operating income (천원, PIT-safe)", ["cs_op", "combine"],
              synonyms=["영업이익"]),
    FieldMeta("net_income_parent", "mart_fundamentals_asof_daily", "net_income_parent",
              "REAL", "daily", "KRW_thousands", True, 0,
              "Net income attributable to parent (천원, PIT-safe)", ["cs_op"],
              synonyms=["순이익", "net_income"]),
    FieldMeta("operating_cash_flow", "mart_fundamentals_asof_daily", "operating_cash_flow",
              "REAL", "daily", "KRW_thousands", True, 0,
              "Operating cash flow (천원, PIT-safe)", ["cs_op"],
              synonyms=["영업현금흐름", "OCF"]),
    FieldMeta("cash_and_cash_equivalents", "mart_fundamentals_asof_daily", "cash_and_cash_equivalents",
              "REAL", "daily", "KRW_thousands", True, 0,
              "Cash and equivalents (천원, PIT-safe)", ["cs_op"],
              synonyms=["현금", "cash"]),
    FieldMeta("total_financial_debt", "mart_fundamentals_asof_daily", "total_financial_debt",
              "REAL", "daily", "KRW_thousands", True, 0,
              "Total financial debt (천원, PIT-safe)", ["cs_op"],
              synonyms=["금융부채"]),
]

# ============================================================
# Sector
# ============================================================
_SECTOR = [
    FieldMeta("sector_name", "core_sector_map", "sector_name",
              "TEXT", "static", "category", True, 0,
              "GICS-like sector name", [],
              synonyms=["섹터", "sector"]),
    FieldMeta("sector_weight", "mart_sector_weight_snapshot", "sector_weight",
              "REAL", "daily", "ratio", True, 0,
              "Approximate sector weight in eligible universe (derived from market cap)",
              ["predicate", "combine"]),
]

# ============================================================
# Build registry dict
# ============================================================
def _build_registry(*groups: list) -> dict[str, FieldMeta]:
    reg: dict[str, FieldMeta] = {}
    for grp in groups:
        for f in grp:
            reg[f.field_id] = f
    return reg


FIELD_REGISTRY: dict[str, FieldMeta] = _build_registry(
    _ELIGIBILITY,
    _PRICE,
    _LIQUIDITY,
    _FEATURES,
    _FUNDAMENTALS,
    _SECTOR,
)

# Reverse lookup: synonym -> field_id
SYNONYM_MAP: dict[str, str] = {}
for _fm in FIELD_REGISTRY.values():
    for _syn in _fm.synonyms:
        SYNONYM_MAP[_syn.lower()] = _fm.field_id


def resolve_field(name: str) -> Optional[FieldMeta]:
    """Resolve a field name or synonym to a FieldMeta. Returns None if not found."""
    if name in FIELD_REGISTRY:
        return FIELD_REGISTRY[name]
    return FIELD_REGISTRY.get(SYNONYM_MAP.get(name.lower(), ""))


# Table -> list of field_ids for efficient selective loading
TABLE_FIELDS: dict[str, list[str]] = {}
for _fm in FIELD_REGISTRY.values():
    TABLE_FIELDS.setdefault(_fm.table_name, []).append(_fm.field_id)
