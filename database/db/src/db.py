"""
Database connection management and schema initialization.
"""

import logging
import math
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

_PRAGMAS = [
    "PRAGMA journal_mode = WAL;",
    "PRAGMA foreign_keys = ON;",
    "PRAGMA synchronous = NORMAL;",
    "PRAGMA cache_size = -65536;",   # ~64 MB page cache
    "PRAGMA temp_store = MEMORY;",
]


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """
    Open (or create) the SQLite database and apply standard pragmas.
    Returns a connection with row_factory = sqlite3.Row for dict-like access.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row

    for pragma in _PRAGMAS:
        conn.execute(pragma)

    # Register math functions not available natively in SQLite
    conn.create_function("SQRT", 1, lambda x: math.sqrt(x) if x is not None and x >= 0 else None)

    logger.debug("Opened database: %s", db_path)
    return conn


def apply_schema(conn: sqlite3.Connection, schema_sql_path: str | Path) -> None:
    """
    Execute the schema SQL file against the connection.
    Idempotent: uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.

    Uses executescript() which handles multi-statement SQL, comments,
    and Korean characters in comments correctly.
    Note: executescript() issues an implicit COMMIT before executing.
    """
    schema_path = Path(schema_sql_path)
    sql_text = schema_path.read_text(encoding="utf-8")
    try:
        conn.executescript(sql_text)
    except sqlite3.Error as e:
        logger.error("Schema error: %s", e)
        raise
    logger.info("Schema applied from %s", schema_path.name)


def apply_views(conn: sqlite3.Connection, views_sql_path: str | Path) -> None:
    """Execute the views SQL file. Idempotent."""
    views_path = Path(views_sql_path)
    sql_text = views_path.read_text(encoding="utf-8")

    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    with conn:
        for stmt in statements:
            if stmt.upper().startswith("--"):
                continue
            if not stmt:
                continue
            try:
                conn.execute(stmt)
            except sqlite3.Error as e:
                # Views might fail if underlying tables don't exist yet; warn only
                logger.warning("View creation warning: %s", e)

    logger.info("Views applied from %s", views_path.name)


def insert_batch(
    conn: sqlite3.Connection,
    table: str,
    rows: list[tuple],
    columns: list[str],
    mode: str = "INSERT OR REPLACE",
) -> int:
    """
    Bulk-insert a list of tuples into a table.

    mode: 'INSERT OR REPLACE' (default), 'INSERT OR IGNORE', 'INSERT'
    Returns number of rows inserted.
    """
    if not rows:
        return 0

    placeholders = ", ".join("?" * len(columns))
    col_list = ", ".join(columns)
    sql = f"{mode} INTO {table} ({col_list}) VALUES ({placeholders})"

    with conn:
        conn.executemany(sql, rows)

    return len(rows)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Return True if a table exists in the database."""
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cur.fetchone() is not None


def get_row_count(conn: sqlite3.Connection, table_name: str) -> int:
    """Return row count for a table."""
    cur = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]


def truncate_table(conn: sqlite3.Connection, table_name: str) -> None:
    """Delete all rows from a table (for idempotent rebuilds)."""
    with conn:
        conn.execute(f"DELETE FROM {table_name}")
    logger.debug("Truncated table: %s", table_name)


def run_script_lines(conn: sqlite3.Connection, sql: str) -> None:
    """Execute a multi-statement SQL script."""
    conn.executescript(sql)
