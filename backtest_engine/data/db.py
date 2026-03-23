"""
SQLite connection manager with WAL mode and foreign keys enabled.

Provides a simple context manager and a module-level cached connection
for the default DB path.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

# Default DB path relative to project root
_DEFAULT_DB = Path(__file__).parent.parent.parent / "database" / "db" / "data" / "db" / "backtest.db"

_cached_connections: dict[str, sqlite3.Connection] = {}


def get_connection(db_path: Optional[str | Path] = None, *, read_only: bool = False) -> sqlite3.Connection:
    """
    Return a SQLite connection (cached by path).

    The connection uses:
    - WAL journal mode for concurrent reads
    - foreign_keys = ON
    - detect_types = PARSE_DECLTYPES for date handling
    - row_factory = sqlite3.Row for named column access
    """
    path = str(Path(db_path) if db_path else _DEFAULT_DB)

    if path in _cached_connections:
        try:
            _cached_connections[path].execute("SELECT 1")
            return _cached_connections[path]
        except sqlite3.ProgrammingError:
            del _cached_connections[path]

    if read_only:
        uri = f"file:{path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, detect_types=sqlite3.PARSE_DECLTYPES)
    else:
        conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)

    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -32768")   # 32 MB page cache

    _cached_connections[path] = conn
    return conn


@contextmanager
def open_connection(db_path: Optional[str | Path] = None) -> Generator[sqlite3.Connection, None, None]:
    """Context manager that yields a fresh (non-cached) connection and closes on exit."""
    path = Path(db_path) if db_path else _DEFAULT_DB
    conn = sqlite3.connect(str(path), detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -32768")
    try:
        yield conn
    finally:
        conn.close()


def close_all() -> None:
    """Close all cached connections (useful in tests)."""
    for conn in _cached_connections.values():
        try:
            conn.close()
        except Exception:
            pass
    _cached_connections.clear()
