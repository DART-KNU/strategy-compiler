"""Tests for allocator correctness."""

import numpy as np
import pandas as pd
import pytest

from backtest_engine.portfolio.allocators import allocate, _equal_weight, _score_weighted, _inverse_vol
from backtest_engine.strategy_ir.models import (
    EqualWeightConfig, ScoreWeightedConfig, InverseVolConfig, ConstraintSet,
)


def make_snapshot(n: int = 10) -> pd.DataFrame:
    tickers = [f"T{i:03d}" for i in range(n)]
    sectors = (["IT", "Finance"] * ((n + 1) // 2))[:n]
    return pd.DataFrame({
        "market_cap": np.random.uniform(1e11, 1e12, n),
        "vol_20d": np.random.uniform(0.1, 0.3, n),
        "sector_name": sectors,
    }, index=tickers)


class TestEqualWeight:
    def test_weights_sum_to_target(self):
        tickers = ["A", "B", "C", "D"]
        snap = make_snapshot(4)
        snap.index = tickers
        constraints = ConstraintSet(target_cash_weight=0.005)
        w = allocate(tickers, None, snap, EqualWeightConfig(), constraints)
        assert abs(w.sum() - 0.995) < 0.01

    def test_weights_equal(self):
        tickers = ["A", "B", "C", "D"]
        snap = make_snapshot(4)
        snap.index = tickers
        constraints = ConstraintSet()
        w = allocate(tickers, None, snap, EqualWeightConfig(), constraints)
        assert w.std() < 1e-8   # all equal

    def test_single_ticker(self):
        snap = pd.DataFrame({"market_cap": [1e12], "vol_20d": [0.15], "sector_name": ["IT"]}, index=["A"])
        constraints = ConstraintSet(target_cash_weight=0.005)
        w = allocate(["A"], None, snap, EqualWeightConfig(), constraints)
        assert abs(w["A"] - 0.995) < 0.01


class TestScoreWeighted:
    def test_higher_score_gets_more_weight(self):
        tickers = ["A", "B", "C"]
        snap = pd.DataFrame({
            "market_cap": [1e12, 1e12, 1e12],
            "vol_20d": [0.15, 0.15, 0.15],
            "sector_name": ["IT", "Finance", "IT"],
        }, index=tickers)
        scores = pd.Series({"A": 3.0, "B": 2.0, "C": 1.0})
        constraints = ConstraintSet(max_weight=1.0, target_cash_weight=0.0)
        w = allocate(tickers, scores, snap, ScoreWeightedConfig(power=1.0, clip_negative=True), constraints)
        assert w["A"] > w["B"] > w["C"]

    def test_negative_scores_clipped_to_zero(self):
        tickers = ["A", "B"]
        snap = make_snapshot(2)
        snap.index = tickers
        scores = pd.Series({"A": -1.0, "B": -2.0})
        constraints = ConstraintSet(target_cash_weight=0.0)
        config = ScoreWeightedConfig(clip_negative=True)
        w = allocate(tickers, scores, snap, config, constraints)
        # All negative -> fallback to equal weight
        assert abs(w["A"] - w["B"]) < 0.01

    def test_power_effect(self):
        """Higher power should concentrate weight in top scorer."""
        tickers = ["A", "B", "C"]
        snap = pd.DataFrame({
            "market_cap": [1e12, 1e12, 1e12],
            "vol_20d": [0.15, 0.15, 0.15],
            "sector_name": ["IT", "Finance", "IT"],
        }, index=tickers)
        scores = pd.Series({"A": 3.0, "B": 2.0, "C": 1.0})
        constraints = ConstraintSet(max_weight=1.0, target_cash_weight=0.0)

        w1 = allocate(tickers, scores, snap, ScoreWeightedConfig(power=1.0), constraints)
        w2 = allocate(tickers, scores, snap, ScoreWeightedConfig(power=2.0), constraints)
        # Higher power -> A gets even more weight
        assert w2["A"] > w1["A"]


class TestInverseVol:
    def test_lower_vol_gets_more_weight(self):
        tickers = ["A", "B"]
        snap = pd.DataFrame({
            "vol_20d": [0.10, 0.20],  # A is less volatile
            "market_cap": [1e12, 1e12],
            "sector_name": ["IT", "IT"],
        }, index=tickers)
        constraints = ConstraintSet(max_weight=1.0, target_cash_weight=0.0)
        w = allocate(tickers, None, snap, InverseVolConfig(vol_field="vol_20d"), constraints)
        assert w["A"] > w["B"]

    def test_weights_sum_to_target(self):
        tickers = ["A", "B", "C"]
        snap = pd.DataFrame({
            "vol_20d": [0.15, 0.20, 0.10],
            "market_cap": [1e12] * 3,
            "sector_name": ["IT"] * 3,
        }, index=tickers)
        constraints = ConstraintSet(target_cash_weight=0.005)
        w = allocate(tickers, None, snap, InverseVolConfig(), constraints)
        assert abs(w.sum() - 0.995) < 0.01


class TestConstraintApplication:
    def test_max_weight_enforced(self):
        from backtest_engine.portfolio.constraints import apply_constraints
        # 5 stocks, max_weight=0.25 => feasible (5 * 0.25 = 1.25 >= 1.0)
        tickers = list("ABCDE")
        w = pd.Series({"A": 0.5, "B": 0.3, "C": 0.1, "D": 0.06, "E": 0.04})
        snap = pd.DataFrame({"sector_name": ["IT"] * 5, "market_cap": [1e12] * 5}, index=tickers)
        constraints = ConstraintSet(max_weight=0.25)
        adjusted, viols = apply_constraints(w, snap, constraints)
        assert (adjusted <= 0.25 + 1e-6).all(), f"Max weight violated: {adjusted.max()}"

    def test_weights_sum_preserved(self):
        from backtest_engine.portfolio.constraints import apply_constraints
        w = pd.Series({"A": 0.6, "B": 0.3, "C": 0.1})
        target = w.sum()
        snap = pd.DataFrame({"sector_name": ["IT"] * 3, "market_cap": [1e12] * 3}, index=["A", "B", "C"])
        constraints = ConstraintSet(max_weight=0.40)
        adjusted, _ = apply_constraints(w, snap, constraints)
        assert abs(adjusted.sum() - target) < 0.01
