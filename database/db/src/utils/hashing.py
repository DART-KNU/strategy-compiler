"""
File hashing utilities for build manifest.
"""

import hashlib
import os
from pathlib import Path


def sha256_file(path: str | Path, chunk_size: int = 1 << 20) -> str:
    """
    Compute SHA-256 hex digest of a file.
    Reads in chunks to handle large files without loading fully into memory.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def file_stat(path: str | Path) -> dict:
    """
    Return a dict with file metadata: size, mtime, sha256.
    """
    p = Path(path)
    stat = p.stat()
    return {
        "file_size_bytes": stat.st_size,
        "modified_time": _mtime_iso(stat.st_mtime),
        "sha256": sha256_file(p),
    }


def _mtime_iso(mtime_float: float) -> str:
    """Convert os.stat st_mtime to ISO-8601 string (UTC)."""
    import datetime
    dt = datetime.datetime.utcfromtimestamp(mtime_float)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
