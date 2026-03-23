"""
Feature Registry — describes computed/derived features and their dependencies.

Differs from FieldMeta in that features may combine multiple raw fields
or require multi-step computation. Used for documentation and LLM resolution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class FeatureMeta:
    feature_id: str
    description: str
    source_fields: List[str]       # underlying field_ids
    computation: str               # human-readable computation description
    typical_ops: List[str]         # typical node graph ops
    lookahead_safe: bool = True
    notes: Optional[str] = None
    synonyms: List[str] = None     # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.synonyms is None:
            self.synonyms = []


FEATURE_REGISTRY: dict[str, FeatureMeta] = {f.feature_id: f for f in [
    FeatureMeta(
        "momentum_1m",
        "1-month price momentum",
        ["ret_20d"],
        "20-day return (already in mart_feature_daily)",
        ["cs_op:rank", "cs_op:zscore"],
        synonyms=["모멘텀", "1개월모멘텀"]
    ),
    FeatureMeta(
        "momentum_3m",
        "3-month price momentum",
        ["ret_60d"],
        "60-day return (already in mart_feature_daily)",
        ["cs_op:rank", "cs_op:zscore"],
        synonyms=["3개월모멘텀"]
    ),
    FeatureMeta(
        "quality_roe",
        "Return on Equity (net income / equity)",
        ["net_income_parent", "total_equity_parent"],
        "net_income_parent / total_equity_parent",
        ["combine:div", "cs_op:zscore"],
        synonyms=["ROE", "자기자본수익률"]
    ),
    FeatureMeta(
        "quality_roa",
        "Return on Assets",
        ["net_income_parent", "total_assets"],
        "net_income_parent / total_assets",
        ["combine:div", "cs_op:zscore"],
        synonyms=["ROA", "총자산수익률"]
    ),
    FeatureMeta(
        "quality_gross_margin",
        "Gross margin = (sales - cogs) / sales",
        ["sales", "cogs"],
        "(sales - cogs) / sales",
        ["combine:sub", "combine:div", "cs_op:zscore"],
        synonyms=["매출총이익률", "gross_margin"]
    ),
    FeatureMeta(
        "quality_op_margin",
        "Operating margin = operating_income / sales",
        ["operating_income", "sales"],
        "operating_income / sales",
        ["combine:div", "cs_op:zscore"],
        synonyms=["영업이익률", "op_margin"]
    ),
    FeatureMeta(
        "value_pb",
        "Price-to-Book ratio (lower = cheaper)",
        ["market_cap", "total_equity_parent"],
        "market_cap / (total_equity_parent * 1000)",
        ["combine:div", "cs_op:rank"],
        notes="market_cap in KRW, total_equity_parent in 천원 — multiply by 1000",
        synonyms=["PB", "주가순자산비율"]
    ),
    FeatureMeta(
        "value_pe",
        "Price-to-Earnings ratio",
        ["market_cap", "net_income_parent"],
        "market_cap / (net_income_parent * 1000)",
        ["combine:div", "cs_op:rank"],
        notes="Requires positive earnings; handle divide-by-zero",
        synonyms=["PE", "주가수익비율"]
    ),
    FeatureMeta(
        "value_ev_sales",
        "Enterprise value to sales",
        ["market_cap", "total_financial_debt", "cash_and_cash_equivalents", "sales"],
        "(market_cap + total_financial_debt*1000 - cash_and_cash_equivalents*1000) / (sales*1000)",
        ["combine", "cs_op:rank"],
        synonyms=["EV/Sales"]
    ),
    FeatureMeta(
        "low_vol",
        "Low-volatility factor (lower vol = higher score)",
        ["vol_20d"],
        "-vol_20d (negate so lower vol scores higher)",
        ["combine:negate", "cs_op:rank"],
        synonyms=["저변동성", "low_volatility"]
    ),
    FeatureMeta(
        "reversal_1w",
        "Short-term reversal (negate 5d return)",
        ["ret_5d"],
        "-ret_5d",
        ["combine:negate", "cs_op:rank"],
        synonyms=["단기반전", "short_reversal"]
    ),
    FeatureMeta(
        "growth_composite",
        "Composite growth score",
        ["sales_growth_yoy", "op_income_growth_yoy"],
        "0.5*rank(sales_growth_yoy) + 0.5*rank(op_income_growth_yoy)",
        ["cs_op:rank", "combine:weighted_sum"],
        synonyms=["성장복합", "growth"]
    ),
]}
