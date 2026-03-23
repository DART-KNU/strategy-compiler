"""
Regression test — end-to-end backtest with the momentum strategy example.

This test verifies that:
1. The strategy compiles successfully
2. The backtest runs without error
3. Key metrics are within reasonable bounds
4. The result bundle is well-formed
"""

import json
import pytest
from pathlib import Path

DB_PATH = "database/db/data/db/backtest.db"
STRATEGY_PATH = (
    Path(__file__).parent.parent
    / "backtest_engine"
    / "strategy_ir"
    / "examples"
    / "momentum_strategy.json"
)


@pytest.fixture(scope="module")
def conn():
    from backtest_engine.data.db import get_connection
    return get_connection(DB_PATH)


@pytest.fixture(scope="module")
def compiled_ir():
    from backtest_engine.api.compile_strategy import compile_strategy_from_json
    ir, warns = compile_strategy_from_json(str(STRATEGY_PATH))
    return ir, warns


class TestEndToEndMomentum:
    def test_strategy_compiles(self, compiled_ir):
        ir, warns = compiled_ir
        assert ir is not None
        assert ir.strategy_id == "momentum_12_1"
        # Compilation warnings are OK; errors would raise
        errors = [w for w in warns if w.startswith("[ERROR]")]
        assert not errors, f"Compile errors: {errors}"

    def test_backtest_runs(self, conn, compiled_ir):
        """Backtest should complete without exception."""
        ir, _ = compiled_ir

        # Use a short date range for speed
        from copy import deepcopy
        ir_dict = json.loads(ir.model_dump_json())
        ir_dict["run_overrides"] = {
            "start_date": "2024-01-01",
            "end_date": "2024-06-30",
        }

        from backtest_engine.api.run_backtest import run_backtest
        report = run_backtest(ir_dict, db_path=DB_PATH, verbose=False)

        assert report is not None
        assert "summary_metrics" in report

    def test_nav_is_positive(self, conn, compiled_ir):
        ir, _ = compiled_ir
        ir_dict = json.loads(ir.model_dump_json())
        ir_dict["run_overrides"] = {
            "start_date": "2024-01-01",
            "end_date": "2024-06-30",
        }

        from backtest_engine.api.run_backtest import run_backtest
        report = run_backtest(ir_dict, db_path=DB_PATH)

        nav = report.get("nav_series", {})
        assert len(nav) > 0
        assert all(v > 0 for v in nav.values()), "NAV should always be positive"

    def test_summary_metrics_present(self, conn, compiled_ir):
        ir, _ = compiled_ir
        ir_dict = json.loads(ir.model_dump_json())
        ir_dict["run_overrides"] = {
            "start_date": "2024-01-01",
            "end_date": "2024-06-30",
        }

        from backtest_engine.api.run_backtest import run_backtest
        report = run_backtest(ir_dict, db_path=DB_PATH)

        metrics = report.get("summary_metrics", {})
        required_keys = ["total_return", "cagr", "annualized_vol", "sharpe", "max_drawdown"]
        for k in required_keys:
            assert k in metrics, f"Missing metric: {k}"

    def test_report_bundle_serializable(self, conn, compiled_ir):
        """Report bundle should be JSON-serializable."""
        ir, _ = compiled_ir
        ir_dict = json.loads(ir.model_dump_json())
        ir_dict["run_overrides"] = {
            "start_date": "2024-01-01",
            "end_date": "2024-03-31",
        }

        from backtest_engine.api.run_backtest import run_backtest
        report = run_backtest(ir_dict, db_path=DB_PATH)

        # Should serialize without error
        json_str = json.dumps(report, default=str)
        assert len(json_str) > 100
