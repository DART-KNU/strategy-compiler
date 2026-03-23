"""
compare_runs — compare multiple backtest result bundles side by side.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from backtest_engine.analytics.reporting import load_report_bundle


def compare_runs(
    run_paths: List[str],
    metrics_to_compare: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Compare multiple backtest runs.

    Parameters
    ----------
    run_paths : list[str]
        Paths to report bundle JSON files.
    metrics_to_compare : list[str], optional
        Subset of metrics to include in comparison.

    Returns
    -------
    dict : Side-by-side comparison table.
    """
    default_metrics = [
        "total_return", "cagr", "annualized_vol", "sharpe", "sortino",
        "max_drawdown", "tracking_error", "information_ratio",
        "win_rate", "average_turnover", "beta",
    ]
    compare_keys = metrics_to_compare or default_metrics

    bundles = []
    for path in run_paths:
        bundle = load_report_bundle(path)
        bundles.append(bundle)

    comparison: Dict[str, Any] = {
        "runs": [],
        "comparison_table": {},
    }

    for bundle in bundles:
        meta = {
            "run_id": bundle.get("run_id"),
            "strategy_id": bundle.get("strategy_id"),
            "strategy_name": bundle.get("strategy_name"),
            "mode": bundle.get("mode"),
            "date_range": bundle.get("date_range"),
        }
        comparison["runs"].append(meta)

    # Build comparison table
    for key in compare_keys:
        row = {}
        for i, bundle in enumerate(bundles):
            metrics = bundle.get("summary_metrics", {})
            val = metrics.get(key)
            run_id = bundle.get("run_id", f"run_{i}")
            row[run_id] = val
        comparison["comparison_table"][key] = row

    return comparison


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Compare multiple backtest runs")
    parser.add_argument("runs", nargs="+", help="Paths to report bundle JSON files")
    parser.add_argument("--metrics", nargs="*", help="Metrics to compare")
    args = parser.parse_args()

    result = compare_runs(args.runs, metrics_to_compare=args.metrics)
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
