"""
Build manifest: records file checksums for all input files.

The manifest serves two purposes:
  1. Audit trail: know exactly which source files were used to build the DB.
  2. Idempotency: skip re-ingestion of files whose checksums haven't changed.
"""

import datetime
import logging
import sqlite3
import uuid
from pathlib import Path

from src.db import insert_batch
from src.utils.hashing import file_stat

logger = logging.getLogger(__name__)


def create_build_run_id() -> str:
    """Generate a unique build run ID (timestamp + uuid4 fragment)."""
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    uid = str(uuid.uuid4()).replace("-", "")[:8]
    return f"{ts}_{uid}"


def record_manifest(
    conn: sqlite3.Connection,
    build_run_id: str,
    input_files: dict[str, Path],
) -> dict[str, dict]:
    """
    Compute and record SHA-256 checksums for all input files.

    Returns a dict mapping source_name -> {sha256, file_size_bytes, modified_time}.
    """
    ingested_at = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    cols = ["build_run_id", "source_name", "absolute_path", "file_size_bytes",
            "modified_time", "sha256", "ingested_at"]

    rows = []
    checksums = {}

    for name, path in input_files.items():
        if not path.exists():
            logger.warning("Manifest: file not found, skipping checksum: %s -> %s", name, path)
            checksums[name] = None
            rows.append((build_run_id, name, str(path.resolve()),
                         None, None, None, ingested_at))
            continue

        logger.debug("Computing checksum for %s ...", path.name)
        stat = file_stat(path)
        checksums[name] = stat
        rows.append((
            build_run_id,
            name,
            str(path.resolve()),
            stat["file_size_bytes"],
            stat["modified_time"],
            stat["sha256"],
            ingested_at,
        ))
        logger.info("  [manifest] %-30s  sha256=%s...", name, stat["sha256"][:12])

    insert_batch(conn, "raw_build_manifest", rows, cols)
    logger.info("Recorded manifest: %d files for build_run_id=%s", len(rows), build_run_id)
    return checksums


def get_last_checksum(conn: sqlite3.Connection, source_name: str) -> str | None:
    """
    Retrieve the SHA-256 of the most recently recorded build for a source file.
    Returns None if the file has never been recorded.
    """
    cur = conn.execute(
        """
        SELECT sha256
        FROM raw_build_manifest
        WHERE source_name = ?
        ORDER BY ingested_at DESC
        LIMIT 1
        """,
        (source_name,),
    )
    row = cur.fetchone()
    return row["sha256"] if row else None


def is_file_unchanged(
    conn: sqlite3.Connection,
    source_name: str,
    current_sha256: str | None,
) -> bool:
    """
    Return True if the current SHA-256 matches the last recorded value.
    If current_sha256 is None (file not found), return False.
    """
    if current_sha256 is None:
        return False
    last = get_last_checksum(conn, source_name)
    return last is not None and last == current_sha256
