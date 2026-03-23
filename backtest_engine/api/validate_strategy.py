"""
validate_strategy — standalone validation of a Strategy IR dict or file.
"""

from __future__ import annotations

import json
import sys
import argparse
from pathlib import Path
from typing import Any, Dict, List

from backtest_engine.strategy_ir.models import StrategyIR
from backtest_engine.strategy_ir.validator import SemanticValidator, ValidationResult
from backtest_engine.registry.field_registry import FIELD_REGISTRY


def validate_strategy(ir_dict: Dict[str, Any]) -> ValidationResult:
    """
    Validate a strategy IR dict.

    Returns a ValidationResult with all errors and warnings.
    Does not raise — caller checks result.has_errors.
    """
    try:
        ir = StrategyIR.model_validate(ir_dict)
    except Exception as e:
        from backtest_engine.strategy_ir.validator import ValidationResult, ValidationIssue
        result = ValidationResult()
        result.add_error("SCHEMA_ERROR", str(e), "top_level")
        return result

    validator = SemanticValidator(field_registry=FIELD_REGISTRY)
    return validator.validate(ir)


def validate_strategy_from_json(path: str | Path) -> ValidationResult:
    """Load a strategy JSON file and validate it."""
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)
    return validate_strategy(d)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate a Strategy IR JSON file")
    parser.add_argument("--input", required=True, help="Path to strategy JSON file")
    args = parser.parse_args()

    result = validate_strategy_from_json(args.input)
    print(str(result))

    if result.has_errors:
        print("\nValidation FAILED.")
        sys.exit(1)
    else:
        print("\nValidation PASSED.")
        sys.exit(0)
