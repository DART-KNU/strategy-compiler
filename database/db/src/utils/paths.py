"""
Path management for the project.
All output paths are under the db/ working directory.
All input paths are under the raw/ sibling directory.
"""

import os
from pathlib import Path


def resolve_project_root() -> Path:
    """
    Returns the absolute path of the db/ project root.
    This file lives at db/src/utils/paths.py, so project root = 3 parents up.
    """
    return Path(__file__).resolve().parent.parent.parent


def resolve_raw_root(cfg_raw_root: str, project_root: Path | None = None) -> Path:
    """
    Resolve the raw data root path.
    cfg_raw_root may be absolute or relative (relative to project_root).
    """
    if project_root is None:
        project_root = resolve_project_root()
    p = Path(cfg_raw_root)
    if not p.is_absolute():
        p = (project_root / p).resolve()
    return p


def validate_mandatory_files(file_map: dict[str, Path]) -> list[str]:
    """
    Check that all mandatory input files exist.
    Returns list of error messages (empty if all OK).
    """
    errors = []
    for name, path in file_map.items():
        if not path.exists():
            errors.append(f"MISSING mandatory file [{name}]: {path}")
        elif not path.is_file():
            errors.append(f"NOT A FILE [{name}]: {path}")
    return errors


def ensure_dir(path: str | Path) -> Path:
    """Create directory (and parents) if it doesn't exist. Returns Path."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def build_input_file_map(cfg: dict, raw_root: Path) -> dict[str, Path]:
    """
    Build a dict mapping logical source names to absolute Paths.
    Uses the paths section from the loaded config dict.
    Fails fast (returns errors via validate_mandatory_files) if files missing.
    """
    kind_cfg = cfg["paths"]["kind"]
    file_map = {
        "kind_listed_companies":   raw_root / kind_cfg["listed_companies"],
        "kind_delistings":         raw_root / kind_cfg["delistings"],
        "kind_ipos":               raw_root / kind_cfg["ipos"],
        "kind_stock_issuance":     raw_root / kind_cfg["stock_issuance"],
        "kind_investment_caution": raw_root / kind_cfg["investment_caution"],
        "kind_investment_warning": raw_root / kind_cfg["investment_warning"],
        "kind_investment_risk":    raw_root / kind_cfg["investment_risk"],
        "sector_file":             raw_root / cfg["paths"]["sector_file"],
        "dataguide_file":          raw_root / cfg["paths"]["dataguide_file"],
    }
    return file_map
