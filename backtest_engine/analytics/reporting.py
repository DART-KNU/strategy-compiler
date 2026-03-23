"""
Reporting utilities — save/load report bundles and describe datasets.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


def run_output_dir(bundle: Dict[str, Any], base_dir: str | Path) -> Path:
    """Return the per-run subdirectory path (does not create it)."""
    run_id = bundle.get("run_id", "unknown")
    strategy_id = bundle.get("strategy_id", "strategy").replace(" ", "_")
    return Path(base_dir) / f"{strategy_id}__{run_id}"


def save_report_bundle(bundle: Dict[str, Any], output_dir: str | Path) -> Path:
    """Save a report bundle JSON into a per-run subdirectory under output_dir.

    Creates: {output_dir}/{strategy_id}__{run_id}/{strategy_id}__{run_id}.json
    Returns the path to the saved JSON file.
    """
    run_dir = run_output_dir(bundle, output_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    run_id = bundle.get("run_id", "unknown")
    strategy_id = bundle.get("strategy_id", "strategy").replace(" ", "_")
    filename = f"{strategy_id}__{run_id}.json"
    path = run_dir / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2, ensure_ascii=False, default=str)
    return path


def load_report_bundle(path: str | Path) -> Dict[str, Any]:
    """Load a report bundle from JSON."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def describe_dataset(conn: sqlite3.Connection) -> Dict[str, Any]:
    """
    Return a description of the dataset in the DB.

    Used by the describe_dataset API and LLM context building.
    """
    result: Dict[str, Any] = {"tables": {}, "coverage": {}, "field_catalog": []}

    # Table row counts
    tables = [
        "core_calendar", "core_price_daily", "core_index_daily",
        "mart_universe_eligibility_daily", "mart_liquidity_daily",
        "mart_feature_daily", "mart_fundamentals_asof_daily",
        "core_sector_map", "mart_sector_weight_snapshot",
        "meta_field_catalog",
    ]
    for t in tables:
        try:
            cur = conn.execute(f"SELECT COUNT(*) FROM {t}")
            result["tables"][t] = cur.fetchone()[0]
        except Exception:
            result["tables"][t] = 0

    # Date coverage
    try:
        cur = conn.execute("SELECT MIN(trade_date), MAX(trade_date) FROM core_calendar")
        row = cur.fetchone()
        result["coverage"]["calendar"] = {"start": row[0], "end": row[1]}
    except Exception:
        pass

    try:
        cur = conn.execute("SELECT MIN(trade_date), MAX(trade_date) FROM core_price_daily")
        row = cur.fetchone()
        result["coverage"]["prices"] = {"start": row[0], "end": row[1]}
    except Exception:
        pass

    try:
        cur = conn.execute(
            "SELECT MIN(trade_date), MAX(trade_date) FROM mart_feature_daily WHERE ret_1d IS NOT NULL"
        )
        row = cur.fetchone()
        result["coverage"]["features"] = {"start": row[0], "end": row[1]}
    except Exception:
        pass

    # Sample eligible universe size
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM mart_universe_eligibility_daily "
            "WHERE trade_date = (SELECT MAX(trade_date) FROM mart_universe_eligibility_daily "
            "WHERE trade_date < '2026-01-01') AND is_eligible = 1"
        )
        result["coverage"]["eligible_universe_size"] = cur.fetchone()[0]
    except Exception:
        pass

    # Index codes
    try:
        cur = conn.execute("SELECT DISTINCT index_code FROM core_index_daily ORDER BY index_code")
        result["coverage"]["index_codes"] = [row[0] for row in cur.fetchall()]
    except Exception:
        pass

    # Sectors
    try:
        cur = conn.execute(
            "SELECT sector_name, COUNT(*) as cnt FROM core_sector_map "
            "GROUP BY sector_name ORDER BY cnt DESC"
        )
        result["coverage"]["sectors"] = {row[0]: row[1] for row in cur.fetchall()}
    except Exception:
        pass

    # Field catalog
    try:
        cur = conn.execute(
            "SELECT field_id, field_name_en, table_name, dtype, frequency, lookahead_safe "
            "FROM meta_field_catalog ORDER BY table_name, field_id"
        )
        result["field_catalog"] = [
            {"field_id": r[0], "name": r[1], "table": r[2], "dtype": r[3],
             "frequency": r[4], "lookahead_safe": bool(r[5])}
            for r in cur.fetchall()
        ]
    except Exception:
        pass

    return result
