"""
NodeGraphExecutor — evaluates a NodeGraph on a snapshot DataFrame.

Execution model:
1. Topological sort of the DAG
2. Evaluate each node in order, storing results in a `values` dict
3. Final output is the Series at `graph.output`

For time-series ops, we need historical data. The executor accepts an
optional `history_loader` that provides (date x ticker) matrices.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from backtest_engine.graph.operators import (
    TS_OPS, CS_OPS, COMBINE_OPS, PREDICATE_OPS,
    if_else, logical_and, logical_or, logical_not, weighted_sum,
)
from backtest_engine.strategy_ir.models import (
    NodeGraph, NodeDef, FieldNode, ConstantNode, BenchmarkRefNode,
    TsOpNode, CsOpNode, CombineNode, PredicateNode, ConditionNode,
    NullPolicy,
)


class NodeGraphExecutor:
    """
    Evaluates a NodeGraph given a cross-sectional snapshot.

    Parameters
    ----------
    conn : sqlite3.Connection
        Used for time-series lookups (ts_op nodes).
    trade_date : str
        Current evaluation date (YYYY-MM-DD).
    snapshot : pd.DataFrame
        Cross-sectional data indexed by ticker, columns = field names.
    history_window : int
        How many days of history to load for ts_op nodes.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        trade_date: str,
        snapshot: pd.DataFrame,
        history_window: int = 252,
    ):
        self._conn = conn
        self._trade_date = trade_date
        self._snapshot = snapshot
        self._history_window = history_window
        self._ts_cache: Dict[str, pd.DataFrame] = {}   # field_id -> (date x ticker)
        self._index_cache: Dict[str, pd.DataFrame] = {}
        self._current_nodes: Dict[str, Any] = {}  # node_id -> NodeDef, set during evaluate

    def evaluate(self, graph: NodeGraph) -> Optional[pd.Series]:
        """
        Evaluate all nodes in topological order and return the output Series.

        Returns None if graph is empty or has no output.
        """
        if not graph.nodes:
            return None

        self._current_nodes = graph.nodes
        order = self._topo_sort(graph)
        values: Dict[str, pd.Series] = {}

        for node_id in order:
            node = graph.nodes[node_id]
            result = self._eval_node(node, values)
            result = self._apply_null_policy(result, node.null_policy)
            values[node_id] = result

        if graph.output and graph.output in values:
            return values[graph.output]
        return None

    def evaluate_all(self, graph: NodeGraph) -> Dict[str, pd.Series]:
        """Evaluate graph and return dict of all node outputs."""
        if not graph.nodes:
            return {}
        self._current_nodes = graph.nodes
        order = self._topo_sort(graph)
        values: Dict[str, pd.Series] = {}
        for node_id in order:
            node = graph.nodes[node_id]
            result = self._eval_node(node, values)
            result = self._apply_null_policy(result, node.null_policy)
            values[node_id] = result
        return values

    # ----------------------------------------------------------
    # Node evaluation dispatch
    # ----------------------------------------------------------

    def _eval_node(self, node: NodeDef, values: Dict[str, pd.Series]) -> pd.Series:
        node_type = node.type

        if node_type == "field":
            return self._eval_field(node)
        elif node_type == "constant":
            return self._eval_constant(node)
        elif node_type == "benchmark_ref":
            return self._eval_benchmark_ref(node)
        elif node_type == "ts_op":
            return self._eval_ts_op(node, values)
        elif node_type == "cs_op":
            return self._eval_cs_op(node, values)
        elif node_type == "combine":
            return self._eval_combine(node, values)
        elif node_type == "predicate":
            return self._eval_predicate(node, values)
        elif node_type == "condition":
            return self._eval_condition(node, values)
        else:
            raise ValueError(f"Unknown node type: {node_type}")

    def _eval_field(self, node: FieldNode) -> pd.Series:
        """Load a field from the snapshot, optionally with extra lag."""
        fid = node.field_id
        lag = node.lag

        if lag == 0:
            if fid in self._snapshot.columns:
                return self._snapshot[fid].copy()
            else:
                # Try to load from field registry table
                return self._load_field_from_db(fid, lag=0)
        else:
            return self._load_field_from_db(fid, lag=lag)

    def _load_field_from_db(self, field_id: str, lag: int) -> pd.Series:
        """Load a field from DB for the current trade_date minus lag days."""
        from backtest_engine.registry.field_registry import FIELD_REGISTRY
        if field_id not in FIELD_REGISTRY:
            raise ValueError(f"Field '{field_id}' not found in field registry")
        fm = FIELD_REGISTRY[field_id]

        # Determine the effective date for this field
        if lag > 0:
            from backtest_engine.data.calendar import CalendarProvider
            cal = CalendarProvider(self._conn)
            effective_date = cal.prev_trading_day(self._trade_date, lag) or self._trade_date
        else:
            effective_date = self._trade_date

        if fm.table_name == "core_sector_map":
            sql = f"SELECT ticker, {fm.column_name} AS val FROM core_sector_map"
            df = pd.read_sql_query(sql, self._conn)
            return df.set_index("ticker")["val"]

        if fm.table_name == "mart_sector_weight_snapshot":
            # Join through sector_name
            sql = f"""
            SELECT e.ticker, sw.{fm.column_name} AS val
            FROM mart_universe_eligibility_daily e
            LEFT JOIN core_sector_map sec ON e.ticker = sec.ticker
            LEFT JOIN mart_sector_weight_snapshot sw
                ON e.trade_date = sw.trade_date AND sec.sector_name = sw.sector_name
            WHERE e.trade_date = ?
            """
            df = pd.read_sql_query(sql, self._conn, params=[effective_date])
            return df.set_index("ticker")["val"]

        sql = f"""
        SELECT ticker, {fm.column_name} AS val
        FROM {fm.table_name}
        WHERE trade_date = ?
        """
        df = pd.read_sql_query(sql, self._conn, params=[effective_date])
        if df.empty:
            return pd.Series(dtype=float)
        return df.set_index("ticker")["val"]

    def _eval_constant(self, node: ConstantNode) -> pd.Series:
        """Broadcast a constant value to all tickers in snapshot."""
        return pd.Series(node.value, index=self._snapshot.index)

    def _eval_benchmark_ref(self, node: BenchmarkRefNode) -> pd.Series:
        """Return benchmark index value broadcast to all tickers."""
        cache_key = (node.index_code, node.field)
        if cache_key not in self._index_cache:
            from backtest_engine.data.loaders import IndexHistoryLoader
            loader = IndexHistoryLoader(self._conn)
            hist = loader.load(node.index_code, "2020-12-30", self._trade_date)
            self._index_cache[cache_key] = hist

        hist = self._index_cache[cache_key]
        lag = node.lag
        dates = [d for d in hist.index if d <= self._trade_date]
        if not dates:
            return pd.Series(np.nan, index=self._snapshot.index)

        target_date = dates[max(0, len(dates) - 1 - lag)]
        val = float(hist.loc[target_date, node.field]) if target_date in hist.index else np.nan
        return pd.Series(val, index=self._snapshot.index, name=node.node_id)

    def _eval_ts_op(self, node: TsOpNode, values: Dict[str, pd.Series]) -> pd.Series:
        """
        Time-series operation.
        Loads historical data for the input field and applies the operator.
        Returns the cross-section at trade_date.
        """
        op_name = node.op
        if op_name not in TS_OPS:
            raise ValueError(f"Unknown ts_op: '{op_name}'. Available: {list(TS_OPS)}")

        window = node.window or 20
        hist = self._get_ts_history(node.input, values)
        if hist.empty:
            return pd.Series(np.nan, index=self._snapshot.index)

        op_fn = TS_OPS[op_name]
        # Apply op to each ticker column
        if op_name == "lag":
            result_df = hist.shift(window)
        elif op_name == "downside_std":
            mar = node.params.get("mar", 0.0)
            result_df = hist.apply(lambda col: op_fn(col, window, mar=mar))
        else:
            result_df = hist.apply(lambda col: op_fn(col, window))

        # Get last row (trade_date)
        if self._trade_date in result_df.index:
            row = result_df.loc[self._trade_date]
        elif len(result_df) > 0:
            row = result_df.iloc[-1]
        else:
            return pd.Series(np.nan, index=self._snapshot.index)

        # Align to snapshot index
        return row.reindex(self._snapshot.index)

    def _get_ts_history(self, node_id_or_field: str, values: Dict[str, pd.Series]) -> pd.DataFrame:
        """
        Get time-series history for a node.

        If node_id corresponds to a raw field, load from DB.
        If it's an already-evaluated node, we can't get full history — fallback to DB field.
        """
        # Check if it's a raw field we can load directly
        from backtest_engine.registry.field_registry import FIELD_REGISTRY
        if node_id_or_field in self._ts_cache:
            return self._ts_cache[node_id_or_field]

        # Determine the field to load.
        # node_id_or_field may be a raw field_id OR a node_id that wraps a field node.
        field_id = None
        if node_id_or_field in FIELD_REGISTRY:
            field_id = node_id_or_field
        elif node_id_or_field in self._current_nodes:
            # Resolve through the graph: if the input node is a field node, use its field_id
            input_node = self._current_nodes[node_id_or_field]
            if input_node.type == "field" and input_node.field_id in FIELD_REGISTRY:
                field_id = input_node.field_id

        if field_id is None:
            # Can't get history for computed nodes — return snapshot value repeated
            if node_id_or_field in values:
                s = values[node_id_or_field]
                # Create a single-row DataFrame for the trade_date
                return pd.DataFrame([s], index=[self._trade_date])
            return pd.DataFrame()

        # Load from DB
        fm = FIELD_REGISTRY[field_id]
        from backtest_engine.data.calendar import CalendarProvider
        cal = CalendarProvider(self._conn)
        lookback_start = cal.prev_trading_day(self._trade_date, self._history_window) or "2020-12-30"
        tickers = list(self._snapshot.index)

        if fm.table_name in ("mart_feature_daily", "core_price_daily"):
            sql = f"""
            SELECT trade_date, ticker, {fm.column_name} AS val
            FROM {fm.table_name}
            WHERE ticker IN ({','.join('?'*len(tickers))})
              AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date
            """
            df = pd.read_sql_query(sql, self._conn, params=tickers + [lookback_start, self._trade_date])
            if df.empty:
                return pd.DataFrame()
            hist = df.pivot(index="trade_date", columns="ticker", values="val")
        else:
            return pd.DataFrame()

        self._ts_cache[field_id] = hist
        return hist

    def _eval_cs_op(self, node: CsOpNode, values: Dict[str, pd.Series]) -> pd.Series:
        """Cross-sectional operation on the current snapshot."""
        op_name = node.op
        if op_name not in CS_OPS:
            raise ValueError(f"Unknown cs_op: '{op_name}'. Available: {list(CS_OPS)}")

        inp = values[node.input]
        op_fn = CS_OPS[op_name]

        if op_name == "sector_neutralize":
            sector = self._snapshot.get("sector_name", pd.Series("Unknown", index=inp.index))
            sector = sector.reindex(inp.index).fillna("Unknown")
            method = node.params.get("method", "demean")
            return op_fn(inp, sector, method=method)
        elif op_name == "winsorize":
            lower = node.params.get("lower", 0.01)
            upper = node.params.get("upper", 0.99)
            return op_fn(inp, lower=lower, upper=upper)
        elif op_name == "vol_scale":
            vol_col = node.params.get("vol_field", "vol_20d")
            vol = self._snapshot.get(vol_col, pd.Series(0.15, index=inp.index))
            target_vol = node.params.get("target_vol", 0.15)
            return op_fn(inp, vol.reindex(inp.index).fillna(0.15), target_vol=target_vol)
        else:
            return op_fn(inp)

    def _eval_combine(self, node: CombineNode, values: Dict[str, pd.Series]) -> pd.Series:
        """Arithmetic combination of multiple inputs."""
        op_name = node.op
        inputs = [values[inp] for inp in node.inputs]

        if op_name in COMBINE_OPS:
            fn = COMBINE_OPS[op_name]
            if op_name == "weighted_sum":
                weights = node.params.get("weights", [1.0 / len(inputs)] * len(inputs))
                return fn(inputs, weights)
            elif op_name in ("add", "sub", "mul", "div"):
                if len(inputs) != 2:
                    raise ValueError(f"'{op_name}' requires exactly 2 inputs, got {len(inputs)}")
                return fn(inputs[0], inputs[1])
            elif op_name in ("negate", "abs"):
                if len(inputs) != 1:
                    raise ValueError(f"'{op_name}' requires exactly 1 input")
                return fn(inputs[0])
            elif op_name == "clip":
                lower = node.params.get("lower")
                upper = node.params.get("upper")
                return fn(inputs[0], lower=lower, upper=upper)
            elif op_name == "winsorize":
                lower = node.params.get("lower", 0.01)
                upper = node.params.get("upper", 0.99)
                return fn(inputs[0], lower=lower, upper=upper)
            elif op_name == "if_else":
                if len(inputs) != 3:
                    raise ValueError("if_else requires 3 inputs: [condition, true, false]")
                return if_else(inputs[0], inputs[1], inputs[2])
            elif op_name == "vol_scale":
                if len(inputs) < 2:
                    raise ValueError("vol_scale requires 2 inputs: [signal, vol]")
                target_vol = node.params.get("target_vol", 0.15)
                from backtest_engine.graph.operators import cs_vol_scale
                return cs_vol_scale(inputs[0], inputs[1], target_vol=target_vol)
            else:
                return fn(*inputs)
        elif op_name in PREDICATE_OPS:
            fn = PREDICATE_OPS[op_name]
            return fn(*inputs)
        else:
            raise ValueError(f"Unknown combine op: '{op_name}'")

    def _eval_predicate(self, node: PredicateNode, values: Dict[str, pd.Series]) -> pd.Series:
        """Boolean predicate."""
        op_name = node.op
        inputs = [values[inp] for inp in node.inputs]

        if op_name not in PREDICATE_OPS:
            raise ValueError(f"Unknown predicate op: '{op_name}'. Available: {list(PREDICATE_OPS)}")

        fn = PREDICATE_OPS[op_name]
        if op_name == "logical_not":
            return fn(inputs[0])
        return fn(*inputs)

    def _eval_condition(self, node: ConditionNode, values: Dict[str, pd.Series]) -> pd.Series:
        """if_else condition node."""
        condition = values[node.condition]
        true_val = values[node.true_branch]
        false_val = values[node.false_branch]
        return if_else(condition, true_val, false_val)

    # ----------------------------------------------------------
    # Null policy application
    # ----------------------------------------------------------

    def _apply_null_policy(self, s: pd.Series, policy: NullPolicy) -> pd.Series:
        if policy == NullPolicy.DROP:
            return s.dropna()
        elif policy == NullPolicy.ZERO:
            return s.fillna(0.0)
        elif policy == NullPolicy.FFILL:
            return s.ffill()
        elif policy == NullPolicy.BFILL:
            return s.bfill()
        else:  # keep_null
            return s

    # ----------------------------------------------------------
    # Topological sort
    # ----------------------------------------------------------

    def _topo_sort(self, graph: NodeGraph) -> List[str]:
        """Return node_ids in topological order (dependencies first)."""
        deps: Dict[str, set] = {}
        for nid, node in graph.nodes.items():
            node_type = node.type
            inputs: set = set()
            if node_type in ("ts_op", "cs_op"):
                inputs.add(node.input)
            elif node_type in ("combine", "predicate"):
                inputs.update(node.inputs)
            elif node_type == "condition":
                inputs.update([node.condition, node.true_branch, node.false_branch])
            deps[nid] = inputs & set(graph.nodes.keys())

        # Kahn's algorithm
        in_degree = {nid: 0 for nid in deps}
        for nid, d in deps.items():
            for dep in d:
                in_degree[dep] = in_degree.get(dep, 0)
            for dep in d:
                in_degree[nid] = in_degree.get(nid, 0)

        # Recompute properly
        in_degree = {nid: 0 for nid in deps}
        rev_deps: Dict[str, List[str]] = {nid: [] for nid in deps}
        for nid, d in deps.items():
            for dep in d:
                in_degree[nid] = in_degree.get(nid, 0) + 1
                rev_deps[dep].append(nid)

        queue = [nid for nid, deg in in_degree.items() if deg == 0]
        result = []
        while queue:
            nid = queue.pop(0)
            result.append(nid)
            for dependent in rev_deps.get(nid, []):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(result) != len(deps):
            raise RuntimeError("Node graph has a cycle — cannot evaluate")
        return result
