"""
Sector file ingest.

Source: raw/sector/sector allocation_filled_reprocessed.xlsx
ONLY this exact file is used. No other files in the sector/ folder are read.

Source columns: 코드 코드명 섹터코드 섹터명 채움방식 신뢰도 검토필요 근거/비고

Rules:
  - Use 코드 (ticker code) as the join key, NOT 코드명 (company name).
  - Strip leading 'A' from ticker codes (DataGuide convention).
  - Drop rows with missing or invalid sector name.
  - Validate sector names against allowed list; log and exclude invalid values.
  - Resolve duplicate normalized tickers: prefer rows with non-empty sector,
    then keep last valid row, logging conflicts.
"""

import datetime
import logging
import sqlite3
from pathlib import Path

import pandas as pd

from src.db import insert_batch, truncate_table
from src.utils.ticker import normalize_ticker

logger = logging.getLogger(__name__)

_NOW = lambda: datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def ingest_sectors(
    conn: sqlite3.Connection,
    path: Path,
    allowed_sectors: list[str],
) -> tuple[int, dict]:
    """
    Ingest the sector mapping file into raw_sector_map.

    Returns:
        (rows_ingested, quality_stats) where quality_stats is a dict with
        counts for: total_rows, missing_sector_dropped, invalid_sector_dropped,
        duplicate_conflicts, ingested.
    """
    df = pd.read_excel(str(path), engine="openpyxl")
    ingested_at = _NOW()

    # Rename columns
    col_map = {
        "코드":     "raw_ticker",
        "코드명":   "corp_name",
        "섹터코드": "sector_code",
        "섹터명":   "sector_name",
        "채움방식": "fill_method",
        "신뢰도":   "confidence",
        "검토필요": "needs_review",
        "근거/비고": "notes",
    }
    df = df.rename(columns={c: v for c, v in col_map.items() if c in df.columns})

    stats = {
        "total_rows": len(df),
        "missing_sector_dropped": 0,
        "invalid_sector_dropped": 0,
        "duplicate_conflicts": 0,
        "ingested": 0,
    }

    # Drop rows with missing sector
    before = len(df)
    df = df[df["sector_name"].notna() & (df["sector_name"].astype(str).str.strip() != "")]
    stats["missing_sector_dropped"] = before - len(df)
    if stats["missing_sector_dropped"] > 0:
        logger.info("Dropped %d rows with missing sector_name", stats["missing_sector_dropped"])

    # Validate sector names against allowed list
    allowed_set = set(allowed_sectors)
    invalid_mask = ~df["sector_name"].isin(allowed_set)
    invalid_rows = df[invalid_mask]
    if len(invalid_rows) > 0:
        for _, r in invalid_rows.iterrows():
            logger.warning(
                "INVALID sector value [%r] for ticker [%r] - row excluded from core sector map",
                r.get("sector_name"), r.get("raw_ticker")
            )
        stats["invalid_sector_dropped"] = len(invalid_rows)
        df = df[~invalid_mask]

    # Normalize tickers
    df["norm_ticker"] = df["raw_ticker"].astype(str).apply(normalize_ticker)

    # Log tickers that could not be normalized
    bad_norm = df[df["norm_ticker"].isna()]
    if len(bad_norm) > 0:
        for _, r in bad_norm.iterrows():
            logger.warning("Cannot normalize ticker: %r (company: %r) - skipped",
                           r.get("raw_ticker"), r.get("corp_name"))
    df = df[df["norm_ticker"].notna()]

    # Resolve duplicates: prefer non-empty sector (already guaranteed),
    # then keep last row for same normalized ticker
    dup_mask = df.duplicated(subset=["norm_ticker"], keep=False)
    if dup_mask.any():
        dups = df[dup_mask]["norm_ticker"].unique()
        stats["duplicate_conflicts"] = len(dups)
        for t in dups:
            sub = df[df["norm_ticker"] == t]
            logger.warning(
                "Duplicate normalized ticker [%s]: %d rows, keeping last. "
                "raw_tickers=%s sectors=%s",
                t, len(sub),
                sub["raw_ticker"].tolist(),
                sub["sector_name"].tolist(),
            )
        # Keep last occurrence
        df = df.drop_duplicates(subset=["norm_ticker"], keep="last")

    truncate_table(conn, "raw_sector_map")

    cols = ["raw_ticker", "corp_name", "sector_code", "sector_name",
            "fill_method", "confidence", "needs_review", "notes", "ingested_at"]

    rows = []
    for _, row in df.iterrows():
        rows.append((
            str(row.get("raw_ticker", "") or ""),
            row.get("corp_name"),
            row.get("sector_code"),
            row["sector_name"],
            row.get("fill_method"),
            _safe_float(row.get("confidence")),
            int(bool(row.get("needs_review", False))),
            _safe_str(row.get("notes")),
            ingested_at,
        ))

    n = insert_batch(conn, "raw_sector_map", rows, cols)
    stats["ingested"] = n
    logger.info("Ingested %d rows -> raw_sector_map (invalid=%d, dups=%d)",
                n, stats["invalid_sector_dropped"], stats["duplicate_conflicts"])
    return n, stats


def _safe_float(val) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_str(val) -> str | None:
    if val is None:
        return None
    import pandas as pd
    if isinstance(val, float) and pd.isna(val):
        return None
    s = str(val).strip()
    return s if s else None
