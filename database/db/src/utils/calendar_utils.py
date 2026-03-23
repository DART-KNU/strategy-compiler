"""
Calendar utilities.

The trading calendar is derived from dates present in the DataGuide price data:
DataGuide only includes trading days (비영업일 제외 = excluding non-business days).
So any date present in raw_dg_stock_daily is a trading day by definition.

After building core_calendar, we can count business days between dates using
the calendar table (simpler and more reliable than computing holidays manually).
"""

import datetime
import logging
from typing import Sequence

logger = logging.getLogger(__name__)


def quarter_end_date(year: str | int, quarter: str) -> str:
    """
    Return the last calendar day of a fiscal quarter as ISO-8601 string.

    Assumption: uses calendar quarters (Q1=Mar31, Q2=Jun30, Q3=Sep30, Q4=Dec31).
    This is correct for December year-end companies (most Korean listed firms).
    For non-December fiscal year companies, the period_end is an approximation
    (see financial_lag config for the conservative availability window).

    Args:
        year: e.g. '2018' or 2018
        quarter: '1Q', '2Q', '3Q', '4Q'
    """
    year = int(year)
    quarter_map = {
        "1Q": (year,  3, 31),
        "2Q": (year,  6, 30),
        "3Q": (year,  9, 30),
        "4Q": (year, 12, 31),
    }
    if quarter not in quarter_map:
        raise ValueError(f"Unknown quarter: {quarter!r}. Expected 1Q/2Q/3Q/4Q.")
    y, m, d = quarter_map[quarter]
    return datetime.date(y, m, d).strftime("%Y-%m-%d")


def add_days(date_str: str, days: int) -> str:
    """Add `days` calendar days to an ISO-8601 date string. Returns ISO-8601."""
    d = datetime.date.fromisoformat(date_str)
    return (d + datetime.timedelta(days=days)).strftime("%Y-%m-%d")


def iso_week_id(date_str: str) -> str:
    """Return ISO week identifier 'YYYY-Www' for a date string."""
    d = datetime.date.fromisoformat(date_str)
    iso = d.isocalendar()
    return f"{iso[0]:04d}-W{iso[1]:02d}"


def month_id(date_str: str) -> str:
    """Return 'YYYY-MM' for a date string."""
    return date_str[:7]


def count_business_days_between(
    start_date: str,
    end_date: str,
    trading_day_set: set,
) -> int:
    """
    Count trading days in [start_date, end_date] using the provided set.

    This is used to compute listing_age_bd for the eligibility filter.
    Both start and end dates are inclusive.
    """
    if start_date > end_date:
        return 0

    # Iterate from start to end; count days in the trading set
    d = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)
    count = 0
    while d <= end:
        if d.strftime("%Y-%m-%d") in trading_day_set:
            count += 1
        d += datetime.timedelta(days=1)
    return count


def build_prev_next_maps(sorted_dates: Sequence[str]) -> tuple[dict, dict]:
    """
    Given a sorted list of trading dates (ISO-8601 strings),
    return (prev_date_map, next_date_map) dicts mapping each date to
    its previous and next trading day.

    First date has prev=None; last date has next=None.
    """
    prev_map: dict[str, str | None] = {}
    next_map: dict[str, str | None] = {}

    dates = list(sorted_dates)
    for i, d in enumerate(dates):
        prev_map[d] = dates[i - 1] if i > 0 else None
        next_map[d] = dates[i + 1] if i < len(dates) - 1 else None

    return prev_map, next_map
