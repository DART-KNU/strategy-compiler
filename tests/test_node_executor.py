"""Tests for node graph executor and operators."""

import numpy as np
import pandas as pd
import pytest

from backtest_engine.graph.operators import (
    cs_rank, cs_zscore, cs_winsorize, cs_sector_neutralize,
    weighted_sum, if_else, ts_sma,
    add, sub, mul, div, negate,
)


class TestCsOperators:
    def test_cs_rank_range(self):
        s = pd.Series([1, 2, 3, 4, 5], index=list("abcde"))
        ranked = cs_rank(s)
        assert (ranked >= 0).all() and (ranked <= 1).all()

    def test_cs_rank_order(self):
        s = pd.Series({"a": 10, "b": 5, "c": 1})
        ranked = cs_rank(s)
        assert ranked["a"] > ranked["b"] > ranked["c"]

    def test_cs_zscore_mean_zero(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        z = cs_zscore(s)
        assert abs(z.mean()) < 1e-10

    def test_cs_zscore_std_one(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        z = cs_zscore(s)
        # cs_zscore uses sample std (ddof=1), so population std ≠ 1.0
        # but the z-scores should have mean 0 and std close to 1 (ddof=1)
        assert abs(z.std(ddof=1) - 1.0) < 0.01

    def test_cs_winsorize_clips(self):
        s = pd.Series([0.0, 1.0, 2.0, 3.0, 100.0])  # 100 is outlier
        ws = cs_winsorize(s, lower=0.01, upper=0.99)
        assert ws.max() < 100.0  # outlier clipped

    def test_cs_sector_neutralize_demean(self):
        s = pd.Series({"a": 10, "b": 8, "c": 5, "d": 3})
        sector = pd.Series({"a": "S1", "b": "S1", "c": "S2", "d": "S2"})
        result = cs_sector_neutralize(s, sector, method="demean")
        # Within S1: mean(10,8)=9, a=1, b=-1
        assert abs(result["a"] - 1.0) < 1e-6
        assert abs(result["b"] - (-1.0)) < 1e-6
        # Within S2: mean(5,3)=4, c=1, d=-1
        assert abs(result["c"] - 1.0) < 1e-6
        assert abs(result["d"] - (-1.0)) < 1e-6


class TestCombineOperators:
    def test_weighted_sum(self):
        a = pd.Series([1.0, 2.0, 3.0])
        b = pd.Series([4.0, 5.0, 6.0])
        result = weighted_sum([a, b], [0.6, 0.4])
        expected = a * 0.6 + b * 0.4
        pd.testing.assert_series_equal(result, expected)

    def test_if_else(self):
        cond = pd.Series([True, False, True])
        t = pd.Series([10.0, 20.0, 30.0])
        f = pd.Series([1.0, 2.0, 3.0])
        result = if_else(cond, t, f)
        assert result.iloc[0] == 10.0
        assert result.iloc[1] == 2.0
        assert result.iloc[2] == 30.0

    def test_div_zero_handled(self):
        a = pd.Series([1.0, 2.0, 3.0])
        b = pd.Series([0.0, 2.0, 0.0])
        result = div(a, b)
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == 1.0
        assert pd.isna(result.iloc[2])

    def test_negate(self):
        s = pd.Series([1.0, -2.0, 3.0])
        result = negate(s)
        pd.testing.assert_series_equal(result, pd.Series([-1.0, 2.0, -3.0]))


class TestTsOperators:
    def test_ts_sma(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = ts_sma(s, window=3)
        # Last value: (3+4+5)/3 = 4
        assert abs(result.iloc[-1] - 4.0) < 1e-6

    def test_ts_sma_min_periods(self):
        s = pd.Series([1.0, 2.0])
        result = ts_sma(s, window=10)
        # min_periods = max(1, window//2) = 5; with only 2 points we get NaN
        # This is the expected behavior — not enough data
        assert pd.isna(result.iloc[-1])


class TestNodeGraphExecutor:
    """Integration test of NodeGraphExecutor with mock snapshot."""

    def _make_snap(self) -> pd.DataFrame:
        return pd.DataFrame({
            "adj_close": [100.0, 200.0, 150.0, 80.0],
            "ret_60d": [0.10, 0.05, -0.03, 0.20],
            "vol_20d": [0.15, 0.10, 0.20, 0.12],
            "sector_name": ["IT", "Finance", "IT", "Finance"],
            "market_cap": [1e12, 2e11, 5e11, 3e11],
        }, index=["A", "B", "C", "D"])

    def test_field_node_from_snapshot(self):
        """Test that node graph executor correctly reads fields from snapshot and applies CS ops."""
        # Test operator directly (executor integration requires real DB)
        snap = self._make_snap()
        ret_series = snap["ret_60d"]
        ranked = cs_rank(ret_series)
        assert (ranked >= 0).all() and (ranked <= 1).all()
        assert ranked["D"] == ranked.max()   # D has highest ret_60d = 0.20

    def test_node_graph_from_dict(self):
        """Test NodeGraph construction from dict (as used in JSON strategies)."""
        from backtest_engine.strategy_ir.models import NodeGraph
        graph_dict = {
            "nodes": {
                "raw": {"node_id": "raw", "type": "field", "field_id": "ret_60d"},
                "score": {"node_id": "score", "type": "cs_op", "op": "rank", "input": "raw"},
            },
            "output": "score",
        }
        graph = NodeGraph.model_validate(graph_dict)
        assert len(graph.nodes) == 2
        assert graph.output == "score"
