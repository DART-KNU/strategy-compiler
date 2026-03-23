"""Constraint registry — catalog of supported portfolio constraints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class ConstraintMeta:
    constraint_id: str
    description: str
    field_name: str             # ConstraintSet attribute name
    default_value: object
    notes: Optional[str] = None


CONSTRAINT_REGISTRY: dict[str, ConstraintMeta] = {c.constraint_id: c for c in [
    ConstraintMeta("long_only", "No short positions", "long_only", True),
    ConstraintMeta("fully_invested", "Cash weight = target_cash_weight", "fully_invested", True),
    ConstraintMeta("max_weight", "Maximum weight per stock", "max_weight", 0.15),
    ConstraintMeta("min_weight", "Minimum weight per stock (0 = no minimum)", "min_weight", 0.0),
    ConstraintMeta("max_names", "Maximum number of holdings", "max_names", None),
    ConstraintMeta("min_names", "Minimum number of holdings", "min_names", 5),
    ConstraintMeta("max_sector_weight", "Absolute sector weight cap", "max_sector_weight", None),
    ConstraintMeta("max_sector_multiplier",
                   "Sector weight <= N * benchmark sector weight",
                   "max_sector_multiplier", 2.0,
                   notes="Contest default: 2x; benchmark sectors <=5% get 10% absolute cap"),
    ConstraintMeta("max_small_mcap_weight",
                   "Max aggregate weight in mcap < threshold stocks",
                   "max_small_mcap_weight", 0.30,
                   notes="Contest: 30% for mcap < 1 trillion KRW"),
    ConstraintMeta("max_adv_participation",
                   "Max fraction of ADV5 consumed by a single trade",
                   "max_adv_participation", 0.10),
    ConstraintMeta("max_turnover_weekly",
                   "Max weekly portfolio turnover (contest: min 5%)",
                   "max_turnover_weekly", None),
    ConstraintMeta("target_cash_weight",
                   "Target cash buffer",
                   "target_cash_weight", 0.005),
]}
