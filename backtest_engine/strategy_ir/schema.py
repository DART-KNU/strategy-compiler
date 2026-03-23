"""Export Strategy IR as a JSON Schema file."""

from __future__ import annotations

import json
from pathlib import Path

from backtest_engine.strategy_ir.models import StrategyIR


def get_json_schema() -> dict:
    """Return the JSON Schema for StrategyIR."""
    return StrategyIR.model_json_schema()


def export_schema(path: str | Path) -> None:
    """Write JSON schema to file."""
    schema = get_json_schema()
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)
    print(f"Schema exported to {path}")


if __name__ == "__main__":
    export_schema(Path(__file__).parent.parent.parent / "strategy_ir.schema.json")
