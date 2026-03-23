"""Tests for Strategy IR schema validation."""

import json
import pytest
from pathlib import Path

from backtest_engine.strategy_ir.models import (
    StrategyIR, SleeveConfig, SelectionConfig, SelectionMethod,
    EqualWeightConfig, NodeGraph, DateRange, ConstraintSet,
    PortfolioAggregation, RebalancingConfig, ExecutionConfig, BenchmarkConfig,
    ReportingConfig,
)


def make_minimal_ir(**overrides) -> dict:
    """Build a minimal valid StrategyIR dict."""
    base = {
        "strategy_id": "test_strategy",
        "date_range": {"start": "2022-01-01", "end": "2024-12-31"},
        "sleeves": [
            {
                "sleeve_id": "main",
                "node_graph": {"nodes": {}, "output": None},
                "selection": {"method": "top_n", "n": 10},
                "allocator": {"type": "equal_weight"},
                "constraints": {"long_only": True, "max_weight": 0.15},
            }
        ],
    }
    base.update(overrides)
    return base


class TestStrategyIRSchema:
    def test_minimal_ir_parses(self):
        """Minimal IR with required fields should parse without error."""
        d = make_minimal_ir()
        ir = StrategyIR.model_validate(d)
        assert ir.strategy_id == "test_strategy"
        assert len(ir.sleeves) == 1
        assert ir.sleeves[0].sleeve_id == "main"

    def test_date_range(self):
        ir = StrategyIR.model_validate(make_minimal_ir())
        assert ir.date_range.start == "2022-01-01"
        assert ir.date_range.end == "2024-12-31"

    def test_duplicate_sleeve_ids_rejected(self):
        d = make_minimal_ir()
        d["sleeves"].append({
            "sleeve_id": "main",  # duplicate!
            "node_graph": {"nodes": {}, "output": None},
            "selection": {"method": "top_n", "n": 5},
            "allocator": {"type": "equal_weight"},
            "constraints": {},
        })
        with pytest.raises(Exception):
            StrategyIR.model_validate(d)

    def test_defaults_injected(self):
        ir = StrategyIR.model_validate(make_minimal_ir())
        # Defaults from field defaults
        assert ir.mode.value == "research"
        assert ir.version == "1.0"
        assert ir.execution.commission_bps == 10.0
        assert ir.execution.sell_tax_bps == 20.0

    def test_selection_top_n_requires_n(self):
        with pytest.raises(Exception):
            SelectionConfig(method=SelectionMethod.TOP_N)  # missing n

    def test_selection_top_pct_requires_pct(self):
        with pytest.raises(Exception):
            SelectionConfig(method=SelectionMethod.TOP_PCT)

    def test_node_graph_missing_output_warns(self):
        """Node graph with output pointing to non-existent node should raise."""
        with pytest.raises(Exception):
            NodeGraph(
                nodes={"n1": {"node_id": "n1", "type": "field", "field_id": "ret_60d"}},
                output="nonexistent"
            )

    def test_sample_strategy_files_parse(self):
        """All sample strategy JSON files should parse without error."""
        examples_dir = Path(__file__).parent.parent / "backtest_engine" / "strategy_ir" / "examples"
        json_files = list(examples_dir.glob("*.json"))
        assert len(json_files) >= 3, f"Expected at least 3 sample strategies, found {len(json_files)}"

        for fp in json_files:
            with open(fp, encoding="utf-8") as f:
                d = json.load(f)
            ir = StrategyIR.model_validate(d)
            assert ir.strategy_id is not None, f"strategy_id missing in {fp.name}"

    def test_effective_date_range_override(self):
        d = make_minimal_ir()
        d["run_overrides"] = {"start_date": "2023-01-01", "end_date": "2023-12-31"}
        ir = StrategyIR.model_validate(d)
        dr = ir.effective_date_range()
        assert dr.start == "2023-01-01"
        assert dr.end == "2023-12-31"

    def test_portfolio_aggregation_unknown_sleeve_rejected(self):
        d = make_minimal_ir()
        d["portfolio_aggregation"] = {
            "method": "fixed_mix",
            "sleeve_weights": {"nonexistent_sleeve": 1.0},
        }
        with pytest.raises(Exception):
            StrategyIR.model_validate(d)
