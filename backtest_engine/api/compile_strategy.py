"""
compile_strategy — normalizes and validates a draft strategy dict into a Strategy IR.

This is the main entry point for converting user input (programmatic or LLM-generated)
into a validated StrategyIR.

Future OpenAI integration:
- IntentParser.parse(nl_text) generates the draft
- This function validates and finalizes it
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from backtest_engine.compiler.normalizer import Normalizer
from backtest_engine.compiler.registry_resolver import RegistryResolver
from backtest_engine.strategy_ir.models import StrategyIR
from backtest_engine.strategy_ir.validator import SemanticValidator
from backtest_engine.registry.field_registry import FIELD_REGISTRY


def compile_strategy(
    draft: Dict[str, Any],
    strict: bool = False,
) -> Tuple[StrategyIR, List[str]]:
    """
    Compile a draft strategy dict into a validated StrategyIR.

    Pipeline:
    1. Normalize (inject defaults, resolve synonyms)
    2. Resolve registry references
    3. Parse Pydantic model (schema validation)
    4. Semantic validation
    5. Return (StrategyIR, warnings)

    Parameters
    ----------
    draft : dict
        Raw strategy dict (from LLM or user).
    strict : bool
        If True, raise on semantic errors. Otherwise, return them as warnings.

    Returns
    -------
    (StrategyIR, warnings: list[str])
    """
    warnings: List[str] = []

    # Step 1: Normalize
    normalizer = Normalizer()
    normalized = normalizer.normalize(draft)

    # Step 2: Registry resolution
    resolver = RegistryResolver()
    resolved, resolve_warns = resolver.resolve_all(normalized)
    warnings.extend(resolve_warns)

    # Step 3: Pydantic parse
    try:
        ir = StrategyIR.model_validate(resolved)
    except Exception as e:
        raise ValueError(f"Strategy IR parsing failed: {e}") from e

    # Step 4: Semantic validation
    validator = SemanticValidator(field_registry=FIELD_REGISTRY)
    result = validator.validate(ir)

    for issue in result.issues:
        if issue.severity == "error":
            if strict:
                raise ValueError(f"Semantic validation error: {issue}")
            warnings.append(f"[ERROR] {issue.code}: {issue.message}")
        elif issue.severity == "warning":
            warnings.append(f"[WARN] {issue.code}: {issue.message}")
        else:
            warnings.append(f"[INFO] {issue.code}: {issue.message}")

    return ir, warnings


def compile_strategy_from_json(
    json_path: str,
    strict: bool = False,
) -> Tuple[StrategyIR, List[str]]:
    """Load a strategy from a JSON file and compile it."""
    with open(json_path, "r", encoding="utf-8") as f:
        draft = json.load(f)
    return compile_strategy(draft, strict=strict)


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="Compile a strategy IR from JSON")
    parser.add_argument("--input", required=True, help="Path to strategy JSON")
    parser.add_argument("--strict", action="store_true", help="Raise on errors")
    args = parser.parse_args()

    ir, warns = compile_strategy_from_json(args.input, strict=args.strict)
    print(f"Strategy '{ir.strategy_id}' compiled successfully.")
    if warns:
        print(f"{len(warns)} warning(s):")
        for w in warns:
            print(f"  {w}")
    print(ir.model_dump_json(indent=2))
