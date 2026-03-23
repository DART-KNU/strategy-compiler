"""
Configuration loader.

Loads YAML config and resolves all paths to absolute Paths.
"""

import logging
import os
from pathlib import Path

import yaml

from src.utils.paths import resolve_project_root, resolve_raw_root, build_input_file_map

logger = logging.getLogger(__name__)


def load_config(config_path: str | Path) -> dict:
    """
    Load YAML config file and resolve all paths.

    Returns a dict with an extra key '_resolved' containing:
      - project_root: Path
      - raw_root: Path
      - db_path: Path
      - artifacts_dir: Path
      - input_files: dict[name -> Path]
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    project_root = resolve_project_root()
    raw_root = resolve_raw_root(cfg["paths"]["raw_root"], project_root)

    db_path_raw = cfg["paths"]["db_file"]
    db_path = Path(db_path_raw)
    if not db_path.is_absolute():
        db_path = (project_root / db_path_raw).resolve()

    artifacts_dir_raw = cfg["paths"]["artifacts_dir"]
    artifacts_dir = Path(artifacts_dir_raw)
    if not artifacts_dir.is_absolute():
        artifacts_dir = (project_root / artifacts_dir_raw).resolve()

    input_files = build_input_file_map(cfg, raw_root)

    cfg["_resolved"] = {
        "project_root":  project_root,
        "raw_root":      raw_root,
        "db_path":       db_path,
        "artifacts_dir": artifacts_dir,
        "input_files":   input_files,
        "config_path":   config_path.resolve(),
    }

    return cfg


def get_resolved(cfg: dict, key: str):
    """Shortcut to access cfg['_resolved'][key]."""
    return cfg["_resolved"][key]
