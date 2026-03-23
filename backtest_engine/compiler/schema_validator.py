"""
SchemaValidator — validates a dict against the StrategyIR JSON schema
using jsonschema, before Pydantic parsing.
"""

from __future__ import annotations

from typing import Any, Dict, List


class SchemaValidator:
    """Validates a strategy dict against the exported JSON schema."""

    def __init__(self):
        self._schema = None

    def _get_schema(self) -> Dict:
        if self._schema is None:
            from backtest_engine.strategy_ir.schema import get_json_schema
            self._schema = get_json_schema()
        return self._schema

    def validate(self, draft: Dict[str, Any]) -> List[str]:
        """
        Validate draft against JSON schema.

        Returns a list of error messages (empty = valid).
        """
        try:
            import jsonschema
        except ImportError:
            return ["jsonschema not installed — schema validation skipped"]

        errors = []
        schema = self._get_schema()
        validator = jsonschema.Draft7Validator(schema)
        for err in sorted(validator.iter_errors(draft), key=lambda e: list(e.absolute_path)):
            path = ".".join(str(p) for p in err.absolute_path)
            errors.append(f"Schema error at '{path}': {err.message}")
        return errors
