# Design Decisions — DART-backtest-NL

This document records the key architectural decisions made during the implementation of the DART backtest engine, explaining the trade-offs chosen and the rationale behind them.

---

## 1. Why Exact Index Replication Is Not Supported

### Decision
The engine supports **proxy benchmark tracking** and **enhanced index** strategies but does not attempt to exactly replicate an index (e.g., KOSPI200 constituent-by-constituent matching).

### Rationale

**Data availability constraint.** Exact replication requires a complete, time-stamped history of index constituent lists with their official weights. The local SQLite DB (`backtest.db`) provides a `mart_sector_weight_snapshot` view and market-cap data, but does not include daily constituent lists for KOSPI200 or any other index. At the time of implementation, that table was empty in the test DB.

**PIT-safety conflict.** Official index rebalancing events (additions, deletions) are announced and effective on specific dates. Ingesting these accurately without lookahead bias requires a separate, carefully curated event feed. Without it, any simulation of exact replication would have subtle lookahead contamination.

**Proxy is sufficient for the contest use case.** The contest rules require that the portfolio track a benchmark and stay within 2× sector multiplier bands — this is a *soft tracking* constraint, not exact replication. The `benchmark_tracking` allocator minimizes tracking error to a proxy constructed from top-market-cap stocks weighted by their relative market cap (`_get_benchmark_proxy()`). This is sufficient for the stated purpose.

**Complexity vs. value.** Exact replication would require:
- An index constituent event feed (not in scope)
- Corporate action adjustment logic (splits, rights issues)
- Float-adjusted weight calculations

These are distinct engineering workstreams that exceed the scope of this phase. The proxy approach delivers 95%+ of the strategic value with ~10% of the complexity.

### Implementation
The `BenchmarkTrackingConfig` and `EnhancedIndexConfig` allocators (`backtest_engine/portfolio/allocators.py`) use `_get_benchmark_proxy()`, which:
1. Looks for tickers in `benchmark_sector_weights` (if provided externally)
2. Falls back to the top-N stocks by market cap, weighted proportionally
3. The `enhanced_index` allocator adds an alpha tilt via `alpha_weight` blended with the benchmark proxy

---

## 2. Why Contest Execution Is Approximated

### Decision
Contest mode uses an **ADV5-based market impact approximation** rather than a full microstructure simulation or order-book model.

### Rationale

**No tick data.** The DB contains daily OHLCV and a precomputed `adv5` (5-day average daily value traded). There is no intraday data, order book, or bid-ask spread history. A full market impact model (Kyle lambda, Almgren-Chriss, etc.) requires at minimum spread and intraday volume profiles.

**The contest rules describe impact qualitatively, not quantitatively.** The contest specification states "market impact based on ADV5 fraction" and an "aggressive order penalty" without giving exact formulas. The approximation in `contest_profile.py` is:

```python
adv_fraction = trade_value / adv5
impact_bps = base_impact_bps * (1 + adv_fraction * adv_scale_factor)
if adv_fraction > aggressive_threshold:
    impact_bps *= aggressive_penalty_multiplier
```

This is a parsimonious model that captures the two qualitative properties:
- Impact grows with order size relative to liquidity
- Large orders (> threshold of ADV) face an extra penalty

**Determinism requirement.** The research profile must be fully deterministic (same input → same output). The contest profile adds realistic slippage but preserves determinism: all inputs (prices, ADV5) come from the DB, and parameters are fixed in `ContestExecutionConfig`.

**Turnover monitoring is soft.** The minimum 5% weekly turnover is tracked and flagged but not enforced as a hard constraint (rebalancing is forced). Enforcing it as a hard constraint would require forward-looking logic (knowing future rebalance dates), which would compromise the PIT-safe architecture.

### Implementation
- `backtest_engine/execution/contest_profile.py`: `execute_rebalance_contest()` calls `execute_rebalance_research()` first for the baseline execution, then computes and adds ADV-based impact on top of the flat slippage.
- `TurnoverMonitor`: records weekly buy/sell amounts and NAV; `check_violations()` returns weeks below the minimum threshold.

---

## 3. DB Table Mapping

### Overview

The SQLite database (`database/db/data/db/backtest.db`) contains the following tables, organized into **core** (raw) and **mart** (pre-computed, PIT-safe) schemas.

### Core Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `core_price_daily` | Daily OHLCV + market cap | `ticker`, `trade_date`, `open`, `high`, `low`, `close`, `adj_close`, `volume`, `traded_value`, `market_cap` |
| `core_calendar` | Korean trading day calendar | `trade_date`, `is_trading_day`, `week_num`, `month`, `quarter` |
| `core_company_info` | Static company metadata | `ticker`, `company_name`, `sector_name`, `market_type` |

### Mart Tables (Pre-computed, PIT-safe)

| Table | Purpose | PIT Key | Key Columns |
|-------|---------|---------|-------------|
| `mart_universe_eligibility_daily` | Daily eligibility flags | `trade_date` | `ticker`, `trade_date`, `is_eligible`, `is_listed`, `is_common_equity`, `is_not_caution`, `is_not_warning`, `is_not_risk`, `is_not_admin`, `is_not_halt` |
| `mart_feature_daily` | Pre-computed return/vol/liquidity features | `trade_date` | `ticker`, `trade_date`, `ret_1d`, `ret_5d`, `ret_20d`, `ret_60d`, `vol_20d`, `adv5`, `adv20`, `turnover_ratio`, `listing_age_bd`, `price_to_52w_high` |
| `mart_fundamentals_asof_daily` | Point-in-time fundamental data | `available_date` | `ticker`, `available_date`, `period_end`, `total_assets`, `sales`, `operating_income`, `net_income_parent`, `net_debt`, `cash_and_equivalents`, and growth/ratio fields |

### Data Coverage Summary

| 데이터 소스 | 기간 | 비고 |
|------------|------|------|
| DataGuide 가격·거래량 | 2021-01-01 ~ 2026-03-20 | |
| DataGuide 지수 (KOSPI 등 4개) | 2021-01-01 ~ 2026-03-20 | |
| DataGuide 재무 (분기) | 2018 ~ 2026 | available_date 기준 PIT-safe |
| KIND 투자경고·위험·주의 | **2023-03-18 ~ 2026-03-18** | 최근 3년만 제공 |
| KIND 상장폐지·신규상장 이력 | 1999 ~ 2026-03-18 | 생존 편향 방지 |

**실질적인 완전 신뢰 백테스트 시작일: 2023-03-18**

이 날짜 이전 구간(2021-01~2023-03)은 투자경고/위험/주의 규제 데이터가 없어 `is_eligible` 판단이 덜 보수적입니다. 가격 팩터(모멘텀 등)는 사용 가능하나 유니버스 편향이 존재합니다.

### PIT-Safety Design

The critical distinction is how **fundamental data** is handled:

- `period_end`: when the fiscal period ended (e.g., 2023-Q3 ends on 2023-09-30)
- `available_date`: when the data was actually disclosed/available to investors

The `mart_fundamentals_asof_daily` table is pre-exploded so that for every trading day, it exposes the most recent fundamental data whose `available_date <= trade_date`. This means a snapshot query never has lookahead bias on fundamentals.

**Price and feature tables** use `trade_date` directly since prices are known at market close.

### Snapshot Query Design

The `build_snapshot_query()` function (`backtest_engine/data/queries.py`) performs selective LEFT JOINs:

1. **Base**: `mart_universe_eligibility_daily` filtered to `trade_date = ?` and `is_eligible = 1`
2. **Price fields** (if requested): JOIN `core_price_daily`
3. **Liquidity/feature fields** (if requested): JOIN `mart_feature_daily`
4. **Fundamental fields** (if requested): JOIN `mart_fundamentals_asof_daily` on `available_date = ?`
5. **Sector** (if requested): JOIN `core_company_info`

Selective loading (passing only needed fields) avoids scanning large tables unnecessarily and keeps snapshot latency low (~5ms per snapshot in testing).

### Benchmark Table

`mart_sector_weight_snapshot` is designed to hold pre-computed index sector weights but was empty in the test DB. The engine falls back to computing sector weights from `core_price_daily.market_cap` aggregation:

```sql
SELECT sector_name, SUM(market_cap) / total_mcap AS weight
FROM core_price_daily JOIN core_company_info USING (ticker)
WHERE trade_date = ? AND ticker IN (...)
```

---

## 4. Future OpenAI Integration

### Architecture

The compiler layer (`backtest_engine/compiler/`) is designed as a **pluggable frontend**. The NL → IR pipeline currently uses a stub (`IntentParser._call_llm()` raises `NotImplementedError`). Swapping in the OpenAI Responses API requires changes only to `intent_parser.py`.

### Integration Pattern

```python
# backtest_engine/compiler/intent_parser.py

from openai import OpenAI
from backtest_engine.strategy_ir.models import StrategyIR

class IntentParser:
    def __init__(self, llm_client: OpenAI, model: str = "gpt-5.4", tools: list = None):
        self._client = llm_client
        self._model = model
        self._tools = tools or []

    def parse(self, text: str, conversation_id: str = None) -> dict:
        response = self._client.responses.create(
            model=self._model,
            input=[{"role": "user", "content": text}],
            text={
                "format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "StrategyIR",
                        "schema": StrategyIR.model_json_schema(),
                        "strict": True,
                    }
                }
            },
            tools=self._tools,
            previous_response_id=conversation_id,  # multi-turn support
        )
        return response.output_text  # already parsed to dict via Structured Outputs
```

### Tool Definitions

Two tools should be provided to the LLM to allow it to query the dataset before generating the IR:

**`describe_dataset`** — Let the LLM understand what fields and date ranges are available:
```python
describe_dataset_tool = {
    "type": "function",
    "name": "describe_dataset",
    "description": "Returns available date range, field list, and universe statistics from the backtest DB",
    "parameters": {"type": "object", "properties": {}, "required": []},
}
```

**`resolve_field`** — Let the LLM resolve a Korean or English field name to a registry field ID:
```python
resolve_field_tool = {
    "type": "function",
    "name": "resolve_field",
    "description": "Resolves a field name or synonym to a canonical field_id in the registry",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Field name or Korean synonym (e.g. '시가총액', 'momentum')"}
        },
        "required": ["query"],
    },
}
```

### Tool Execution Loop

The `IntentParser` should implement a tool execution loop:

```python
while response.stop_reason == "tool_use":
    tool_results = []
    for tool_call in response.output:
        if tool_call.type == "tool_use":
            result = self._execute_tool(tool_call.name, tool_call.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tool_call.id,
                "content": json.dumps(result),
            })
    response = self._client.responses.create(
        model=self._model,
        input=tool_results,
        previous_response_id=response.id,  # continue conversation
        tools=self._tools,
        text={"format": ...},
    )
```

### Multi-turn Clarification

The `previous_response_id` parameter enables conversation continuity. The `SlotPlanner` stub (`intent_parser.py`) identifies underspecified slots (missing date range, ambiguous field references) and can generate clarification questions. In a full implementation:

```python
planner = SlotPlanner()
slots = planner.extract_slots(draft_ir)
missing = planner.find_missing_required(slots)
if missing:
    question = planner.generate_clarification(missing[0])
    # Return question to user instead of draft IR
    return {"status": "needs_clarification", "question": question}
```

### Background Mode

The `run_backtest()` function (`backtest_engine/api/run_backtest.py`) is stateless and I/O-bound only on the SQLite DB, making it compatible with OpenAI's background mode:

```python
# Future: kick off backtest as a background task
response = client.responses.create(
    model="gpt-5.4",
    background=True,
    input=[...],
)
# Poll or webhook for completion
```

The LLM compiles the strategy (fast), then `run_backtest()` executes independently (slower), and the result bundle is returned as structured output — consistent with the "LLM = compiler frontend only" principle.

### Key Invariant

**The LLM never computes numbers.** It only:
1. Interprets natural language into structured Strategy IR (JSON)
2. Calls tools to understand the dataset
3. Optionally narrates the result bundle for human readers

All quantitative computation (scores, weights, returns, metrics) is performed by the Python engine on deterministic, PIT-safe DB data. This separation ensures reproducibility regardless of which LLM model or version is used.

---

## 5. Other Notable Decisions

### Single Strategy IR (No Subclasses)

Early designs considered separate `MomentumStrategy`, `MultifactorStrategy`, `EnhancedIndexStrategy` models. We rejected this because:
- The sleeve + node graph abstraction is expressive enough to represent all strategy types
- A single IR reduces the compiler's output space (one JSON schema to target)
- Validators, CLI tools, and report builders only need one code path

### Iterative Constraint Enforcement

`apply_constraints()` uses up to 10 iterations of capping + re-normalization rather than a one-shot projection. This is because:
- Sector and per-stock constraints interact (capping one stock redistributes to others, potentially violating another constraint)
- A one-shot solution would require solving a constrained optimization at every rebalance date, adding ~10ms per rebalance vs ~0.1ms for the iterative approach
- In practice, constraints converge in 2–3 iterations for typical portfolios

### Room-Based Redistribution for Samsung Cap

When Samsung (005930) is capped at 40% and regular stocks at 15%, naive redistribution could push a regular stock over 15%, which then redistributes excess back to... Samsung (which was excluded from the cap). The fix uses a `per_stock_cap` Series and a `room` vector:

```python
room = (per_stock_cap - w).clip(lower=0.0)
room[over_cap] = 0.0  # don't redistribute back to capped stocks
w += room / room.sum() * excess
```

### `math.floor()` for Buy Share Rounding

Using `round()` for buy shares can produce shares that cost fractionally more than available cash (e.g., 100.5 shares at ₩10,000 = ₩1,005,000, but `round()` gives 101 shares = ₩1,010,000). `math.floor()` guarantees the trade stays within cash:

```python
diff = math.floor(affordable / round_lot) * round_lot
```

### Monthly Returns via DatetimeIndex

`pd.Series.resample("ME")` requires a `DatetimeIndex`. The NAV series uses string dates (YYYY-MM-DD) for serialization compatibility. The fix converts inline for resampling only:

```python
nav_dt = nav_series.copy()
nav_dt.index = pd.to_datetime(nav_dt.index)
monthly = nav_dt.resample("ME").last()
```

This avoids storing DatetimeIndex objects in the JSON-serializable result bundle.
