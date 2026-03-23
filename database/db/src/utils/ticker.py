"""
Ticker normalization utilities.

Canonical ticker format: 6-digit zero-padded numeric string.

Rules:
  1. Strip whitespace.
  2. Remove leading 'A' if present (DataGuide/sector files prefix tickers with 'A').
  3. Preserve leading zeros (do NOT int-cast and re-stringify without zero-padding).
  4. Must be exactly 6 characters after normalization.
  5. Tickers with non-digit characters after stripping 'A' are logged and rejected.

Examples:
  A005930 -> 005930
  A000660 -> 000660
  005930  -> 005930
  '  A000010 ' -> 000010
  '0082N0' -> returns None (contains letter after 'A' strip, log as warning)
  '' -> returns None
"""

import re
import logging

logger = logging.getLogger(__name__)

# Pattern: exactly 6 digits after normalization
_VALID_TICKER_RE = re.compile(r'^\d{6}$')


def normalize_ticker(raw: str | None) -> str | None:
    """
    Normalize a raw ticker string to canonical 6-digit form.

    Returns None if the ticker cannot be normalized (malformed, blank, etc.).
    Logs a warning for rejected tickers.
    """
    if raw is None:
        return None

    # Step 1: stringify and strip whitespace
    s = str(raw).strip()

    if not s:
        return None

    # Step 2: remove leading 'A' (DataGuide convention)
    if s.startswith('A') or s.startswith('a'):
        s = s[1:]

    # Step 3: must now be exactly 6 digits
    if not _VALID_TICKER_RE.match(s):
        logger.debug("Rejected non-canonical ticker: %r -> %r", raw, s)
        return None

    return s


def normalize_ticker_strict(raw: str | None) -> str:
    """
    Like normalize_ticker but raises ValueError on failure.
    Use only in contexts where a valid ticker is mandatory.
    """
    result = normalize_ticker(raw)
    if result is None:
        raise ValueError(f"Cannot normalize ticker: {raw!r}")
    return result


def is_valid_canonical_ticker(ticker: str) -> bool:
    """Return True if ticker is already in canonical 6-digit form."""
    return bool(_VALID_TICKER_RE.match(str(ticker).strip()))


def normalize_ticker_series(series) -> "pd.Series":
    """
    Vectorized normalization for a pandas Series of raw tickers.

    Returns a Series of canonical tickers (None/NaN for rejected entries).
    """
    import pandas as pd
    return series.apply(normalize_ticker)


def normalize_ticker_list(raw_list: list) -> list:
    """
    Normalize a list of raw tickers.
    Returns list of (raw, canonical) tuples.
    canonical is None if normalization failed.
    """
    return [(r, normalize_ticker(r)) for r in raw_list]
