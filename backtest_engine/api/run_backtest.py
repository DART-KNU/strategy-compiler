"""
run_backtest — main backtest execution entry point.

Usage (Python):
    result = run_backtest(ir_dict, db_path="database/db/data/db/backtest.db")

Usage (CLI):
    python -m backtest_engine.api.run_backtest --input strategy.json --db ... --out runs/
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from backtest_engine.api.compile_strategy import compile_strategy
from backtest_engine.analytics.reporting import save_report_bundle
from backtest_engine.analytics.result_bundle import ReportBundleBuilder
from backtest_engine.data.db import get_connection
from backtest_engine.execution.simulator import ExecutionSimulator


_DEFAULT_DB = str(Path(__file__).parent.parent.parent / "database" / "db" / "data" / "db" / "backtest.db")


def run_backtest(
    ir_dict: Dict[str, Any],
    db_path: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    verbose: bool = False,
    save_to: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run a backtest from a strategy IR dict.

    Parameters
    ----------
    ir_dict : dict
        Strategy IR dict (will be compiled if not already a StrategyIR).
    db_path : str, optional
        Path to SQLite database. Defaults to project default.
    config : dict, optional
        Runtime overrides (merged into run_overrides).
    verbose : bool
    save_to : str, optional
        Directory to save the report bundle JSON.

    Returns
    -------
    dict : Complete report bundle.
    """
    # Compile
    if config:
        ir_dict = dict(ir_dict)
        if "run_overrides" not in ir_dict:
            ir_dict["run_overrides"] = {}
        ir_dict["run_overrides"].update(config)

    ir, warnings = compile_strategy(ir_dict)
    if warnings and verbose:
        for w in warnings:
            print(f"  [compile] {w}")

    # Connect
    db_path = db_path or _DEFAULT_DB
    conn = get_connection(db_path)

    # Run simulation
    sim = ExecutionSimulator(conn, ir, verbose=verbose)
    raw_bundle = sim.run()

    # Build report
    builder = ReportBundleBuilder()
    report = builder.build(raw_bundle, strategy_ir=ir, conn=conn)

    # Save if requested
    if save_to:
        path = save_report_bundle(report, save_to)
        if verbose:
            print(f"Report saved to {path}")

    return report


def run_backtest_from_json(
    json_path: str,
    db_path: Optional[str] = None,
    verbose: bool = False,
    save_to: Optional[str] = None,
) -> Dict[str, Any]:
    """Load strategy from JSON file and run backtest."""
    with open(json_path, "r", encoding="utf-8") as f:
        ir_dict = json.load(f)
    return run_backtest(ir_dict, db_path=db_path, verbose=verbose, save_to=save_to)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a backtest from a Strategy IR JSON file")
    parser.add_argument("--input", required=True, help="Path to strategy IR JSON file")
    parser.add_argument("--db", default=_DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument("--out", default="runs", help="Output directory for report bundle")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    report = run_backtest_from_json(
        args.input,
        db_path=args.db,
        verbose=args.verbose,
        save_to=args.out,
    )

    metrics = report.get("summary_metrics", {})
    print("\n=== Backtest Results ===")
    for k, v in metrics.items():
        if not isinstance(v, dict):
            print(f"  {k}: {v}")
    print(f"\nReport bundle saved to: {args.out}/")
