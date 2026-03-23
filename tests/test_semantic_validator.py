"""Tests for semantic validation."""

import pytest
from backtest_engine.strategy_ir.models import (
    StrategyIR, RunMode,
)
from backtest_engine.strategy_ir.validator import SemanticValidator


def _build_ir(**overrides) -> StrategyIR:
    base = {
        "strategy_id": "test",
        "date_range": {"start": "2022-01-01", "end": "2024-12-31"},
        "sleeves": [{
            "sleeve_id": "main",
            "node_graph": {"nodes": {}, "output": None},
            "selection": {"method": "top_n", "n": 20},
            "allocator": {"type": "equal_weight"},
            "constraints": {"max_weight": 0.15},
        }],
    }
    base.update(overrides)
    return StrategyIR.model_validate(base)


class TestSemanticValidator:
    def test_valid_strategy_no_errors(self):
        ir = _build_ir()
        result = SemanticValidator().validate(ir)
        assert not result.has_errors, str(result)

    def test_start_after_end_fails(self):
        ir = _build_ir()
        ir.date_range.start = "2025-01-01"
        ir.date_range.end = "2022-01-01"
        result = SemanticValidator().validate(ir)
        assert result.has_errors
        assert any("DATE_RANGE_INVALID" in i.code for i in result.errors)

    def test_short_period_warning(self):
        ir = _build_ir()
        ir.date_range.start = "2023-01-01"
        ir.date_range.end = "2023-02-28"
        result = SemanticValidator().validate(ir)
        assert any("SHORT_DATE_RANGE" in i.code for i in result.warnings)

    def test_graph_cycle_detected(self):
        """A node graph with a cycle should produce a GRAPH_CYCLE error."""
        ir = _build_ir()
        # Create a cycle: a -> b -> a
        ir.sleeves[0].node_graph.nodes = {
            "a": type("TsOpNode", (), {
                "node_id": "a",
                "type": "ts_op",
                "op": "sma",
                "input": "b",
                "window": 5,
                "null_policy": "drop",
            })(),
            "b": type("TsOpNode", (), {
                "node_id": "b",
                "type": "ts_op",
                "op": "sma",
                "input": "a",
                "window": 5,
                "null_policy": "drop",
            })(),
        }
        result = SemanticValidator().validate(ir)
        assert any("GRAPH_CYCLE" in i.code for i in result.errors)

    def test_weight_constraint_invalid(self):
        ir = _build_ir()
        ir.sleeves[0].constraints.max_weight = 0.05
        ir.sleeves[0].constraints.min_weight = 0.10  # > max
        result = SemanticValidator().validate(ir)
        assert any("CONSTRAINT_WEIGHT_INVALID" in i.code for i in result.errors)

    def test_contest_weight_cap_warning(self):
        ir = _build_ir(mode="contest")
        ir.sleeves[0].constraints.max_weight = 0.20  # exceeds 15% contest rule
        result = SemanticValidator().validate(ir)
        assert any("CONTEST_WEIGHT_CAP" in i.code for i in result.warnings)

    def test_regime_switch_no_branches_fails(self):
        base = {
            "strategy_id": "regime_test",
            "date_range": {"start": "2022-01-01", "end": "2024-12-31"},
            "sleeves": [
                {"sleeve_id": "s1", "node_graph": {"nodes": {}, "output": None},
                 "selection": {"method": "top_n", "n": 10}, "allocator": {"type": "equal_weight"},
                 "constraints": {}},
            ],
            "portfolio_aggregation": {
                "method": "regime_switch",
                "regime_branches": [],   # empty!
                "global_node_graph": {"nodes": {}, "output": None},
            }
        }
        ir = StrategyIR.model_validate(base)
        result = SemanticValidator().validate(ir)
        assert any("REGIME_SWITCH_NO_BRANCHES" in i.code for i in result.errors)
