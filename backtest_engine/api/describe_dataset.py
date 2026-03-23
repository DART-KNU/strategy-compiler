"""
describe_dataset — returns metadata about the database contents.

Usage (CLI):
    python -m backtest_engine.api.describe_dataset --db database/db/data/db/backtest.db
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

from backtest_engine.analytics.reporting import describe_dataset
from backtest_engine.data.db import get_connection

_DEFAULT_DB = str(Path(__file__).parent.parent.parent / "database" / "db" / "data" / "db" / "backtest.db")


def get_dataset_description(db_path: Optional[str] = None) -> Dict[str, Any]:
    """Return a full dataset description dict."""
    conn = get_connection(db_path or _DEFAULT_DB)
    return describe_dataset(conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Describe the backtest dataset")
    parser.add_argument("--db", default=_DEFAULT_DB, help="Path to SQLite database")
    args = parser.parse_args()

    desc = get_dataset_description(args.db)
    print(json.dumps(desc, indent=2, ensure_ascii=False, default=str))
