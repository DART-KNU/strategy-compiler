# DART Backtest Engine — NL-Driven Quant Strategy Compiler & Backtester

A Python backtesting system for Korean equities that compiles natural-language strategy
descriptions into typed Strategy IR and runs deterministic backtests against a local SQLite DB.

---

## System Overview

```
Natural Language  →  Draft IR  →  Validated IR  →  Backtest  →  Report Bundle
      [LLM]          [Normalizer]  [Semantic       [Engine]      [MetricsEngine]
                                   Validator]
```

### Key Principles

- **Single Strategy IR**: All strategies (momentum, multi-factor, enhanced index) use one unified JSON representation
- **Sleeve architecture**: Portfolio = weighted combination of sleeves (sub-strategies)
- **LLM = compiler frontend only**: All numbers computed by the engine; LLM only interprets
- **PIT-safe data**: Financial data loaded via `available_date`, never `period_end`
- **Deterministic engine**: Same input → same output every time
- **Research vs Contest mode**: Separate execution profiles

---

## 설치 및 실행

### 1. 저장소 클론

```bash
git clone https://github.com/DART-KNU/strategy-compiler.git
cd strategy-compiler
```

### 2. DB 다운로드

백테스트 DB(9.6 GB)는 별도로 공유됩니다.

**[📥 backtest.db 다운로드 (Google Drive)](https://drive.google.com/file/d/1KY1NYZKzGbODoa8XxkKS7VCJ7Bf6RPz1/view?usp=sharing)**

다운로드 후 아래 경로에 배치하세요:

```
strategy-compiler/
└── database/
    └── db/
        └── data/
            └── db/
                └── backtest.db   ← 여기
```

### 3. 가상환경 생성 및 패키지 설치

```bash
# Windows (PowerShell / Git Bash)
python -m venv .venv
source .venv/Scripts/activate   # Git Bash
# .venv\Scripts\activate        # PowerShell

pip install -r requirements.txt
```

### 4. OpenAI API 키 설정

```bash
cp .env.example .env
# .env 파일을 열어 OPENAI_API_KEY=sk-... 입력
```

### 5. 대화형 전략 설계 실행

```bash
python -m backtest_engine.api.chat \
  --db database/db/data/db/backtest.db \
  --out runs/
```

---

## Quick Start

### 1. Run a backtest

```bash
python -m backtest_engine.api.run_backtest \
  --input backtest_engine/strategy_ir/examples/momentum_strategy.json \
  --db database/db/data/db/backtest.db \
  --out runs/ \
  --verbose
```

### 2. Validate a strategy

```bash
python -m backtest_engine.api.validate_strategy \
  --input backtest_engine/strategy_ir/examples/multifactor_strategy.json
```

### 3. Describe the dataset

```bash
python -m backtest_engine.api.describe_dataset \
  --db database/db/data/db/backtest.db
```

### 4. Python API

```python
from backtest_engine.api.compile_strategy import compile_strategy
from backtest_engine.api.run_backtest import run_backtest

# Compile a strategy from a dict
ir, warnings = compile_strategy({
    "strategy_id": "my_momentum",
    "date_range": {"start": "2023-01-01", "end": "2025-12-31"},
    "sleeves": [{
        "sleeve_id": "main",
        "node_graph": {
            "nodes": {
                "score": {"node_id": "score", "type": "field", "field_id": "ret_60d"}
            },
            "output": "score"
        },
        "selection": {"method": "top_n", "n": 20},
        "allocator": {"type": "equal_weight"},
        "constraints": {"max_weight": 0.15},
    }]
})

# Run the backtest
report = run_backtest(ir.model_dump(), verbose=True)
print(report["summary_metrics"])
```

---

## Directory Structure

```
backtest_engine/
  compiler/           NL → IR compilation pipeline
    intent_parser.py  LLM frontend stub (OpenAI Responses API)
    normalizer.py     Default injection & synonym resolution
    registry_resolver.py  Registry validation
    schema_validator.py   JSON schema validation

  strategy_ir/        Strategy IR types & validation
    models.py         Pydantic models (StrategyIR, SleeveConfig, etc.)
    schema.py         JSON schema export
    validator.py      Semantic validator
    examples/         Sample strategy JSON files

  registry/           Field/feature/allocator catalog
    field_registry.py 40+ DB fields with PIT-safe metadata
    feature_registry.py Computed feature descriptions
    allocator_registry.py Allocator catalog
    benchmark_registry.py Index code resolution
    constraint_registry.py Constraint catalog

  data/               SQLite data layer
    db.py             Connection manager
    calendar.py       CalendarProvider (trading days, rebalance dates)
    loaders.py        SnapshotLoader, PriceHistoryLoader, etc.
    queries.py        PIT-safe SQL query builders

  graph/              Node graph execution
    operators.py      CS/TS/combine operators
    node_executor.py  Topological DAG evaluator

  portfolio/          Portfolio construction
    selector.py       Universe selection (top_n, threshold, etc.)
    allocators.py     Weight allocation (6 allocator types)
    risk.py           Covariance models (diagonal, sample, Ledoit-Wolf)
    constraints.py    Constraint enforcement (max weight, sector cap, etc.)
    sleeve_mixer.py   Sleeve combination (fixed_mix, regime_switch)

  execution/          Trade execution simulation
    research_profile.py  Deterministic flat-cost research mode
    contest_profile.py   Market impact approximation + turnover monitor
    simulator.py         Main backtest loop

  analytics/          Performance analytics
    metrics.py        All performance metrics (Sharpe, IR, drawdown, etc.)
    attribution.py    Sleeve & Brinson-style attribution
    result_bundle.py  Narrative-ready report bundle builder
    reporting.py      Save/load bundles, describe dataset

  api/                Entry points
    compile_strategy.py
    validate_strategy.py
    run_backtest.py
    compare_runs.py
    describe_dataset.py

tests/               Unit & integration tests (62 passing)
strategy_ir.schema.json  Exported JSON schema
runs/                Output directory for report bundles
```

---

## Strategy IR Format

See `strategy_ir.schema.json` for the full schema. Key concepts:

### Sleeves
```json
{
  "sleeve_id": "momentum",
  "node_graph": {
    "nodes": {
      "score": {"node_id": "score", "type": "field", "field_id": "ret_60d"}
    },
    "output": "score"
  },
  "selection": {"method": "top_n", "n": 20},
  "allocator": {"type": "equal_weight"},
  "constraints": {"max_weight": 0.15, "min_names": 10}
}
```

### Node Graph Operations
| Category | Operations |
|----------|-----------|
| Field | `field` (DB field), `constant`, `benchmark_ref` |
| Time-series | `ts_op`: `lag`, `sma`, `ema`, `std`, `mean`, `zscore`, `rank`, `percentile` |
| Cross-sectional | `cs_op`: `rank`, `zscore`, `percentile`, `winsorize`, `sector_neutralize`, `vol_scale` |
| Combine | `combine`: `add`, `sub`, `mul`, `div`, `weighted_sum`, `negate`, `abs`, `clip`, `if_else` |
| Predicate | `predicate`: `gt`, `gte`, `lt`, `lte`, `eq`, `ne`, `logical_and`, `logical_or`, `logical_not` |
| Condition | `condition`: `if_else` branching |

### Allocators
| Type | Description |
|------|-------------|
| `equal_weight` | Uniform weights |
| `score_weighted` | Score-proportional (with `power` parameter) |
| `inverse_vol` | Inverse realized volatility |
| `mean_variance` | Markowitz (SciPy SLSQP, swappable to cvxpy) |
| `benchmark_tracking` | Minimize TE to benchmark proxy |
| `enhanced_index` | Benchmark tracking + alpha tilt |

### Selection Methods
`top_n`, `top_pct`, `threshold`, `all_positive`, `optimizer_only`

---

## Sample Strategies

| File | Strategy |
|------|----------|
| `momentum_strategy.json` | 60-day momentum (skip 1M), equal-weight, 20 stocks |
| `multifactor_strategy.json` | Quality + momentum + leverage composite, sector-neutral, score-weighted |
| `enhanced_index_strategy.json` | KOSPI200 enhanced index with momentum alpha tilt |

---

## DB Field Registry

Key fields available (from `backtest_engine/registry/field_registry.py`):

| Category | Fields |
|----------|--------|
| Universe | `is_eligible`, `is_listed`, `is_common_equity`, `is_not_caution/warning/risk/admin/halt` |
| Price | `close`, `adj_close`, `open`, `high`, `low`, `volume`, `traded_value`, `market_cap` |
| Liquidity | `adv5`, `adv20`, `listing_age_bd` |
| Features | `ret_1d`, `ret_5d`, `ret_20d`, `ret_60d`, `vol_20d`, `turnover_ratio`, `price_to_52w_high` |
| Fundamentals | `sales_growth_yoy`, `op_income_growth_yoy`, `net_debt_to_equity`, `cash_to_assets` |
| Raw Fundamentals | `total_assets`, `sales`, `operating_income`, `net_income_parent` (all PIT-safe via `available_date`) |
| Sector | `sector_name`, `sector_weight` |

---

## Contest Mode

Activate with `"mode": "contest"`. Additional constraints enforced:
- Per-stock cap: 15% (Samsung `005930`: 40%)
- Sector cap: 2× benchmark sector weight (sectors ≤5% BM weight: 10% absolute cap)
- Small-cap aggregate: ≤30% in stocks with market cap < 1T KRW
- Minimum weekly turnover: 5% (monitored, not enforced as hard constraint)
- Market impact: ADV5-based slippage scaling + aggressive order penalty

---

## Future OpenAI Integration (Phase 5)

The compiler layer is designed for OpenAI Responses API integration:

```python
# Future: swap IntentParser backend
from backtest_engine.compiler.intent_parser import IntentParser

parser = IntentParser(
    llm_client=openai_client,
    model="gpt-5.4",
    tools=[describe_dataset_tool, resolve_field_tool],
)
draft = parser.parse("모멘텀 전략을 짜줘. 상위 30종목, 월별 리밸런싱")
ir, warns = compile_strategy(draft)
```

Design points:
- `Responses API` with structured output (JSON Schema of StrategyIR)
- Tool calls for `describe_dataset` and `resolve_field`
- `previous_response_id` support for multi-turn conversation
- Background mode compatible (stateless run_backtest API)

---

## Running Tests

```bash
# Unit tests only (fast, no DB)
python -m pytest tests/test_strategy_ir.py tests/test_semantic_validator.py \
  tests/test_node_executor.py tests/test_allocators.py \
  tests/test_execution.py tests/test_metrics.py tests/test_contest.py -v

# Full suite including end-to-end backtest
python -m pytest tests/ -v

# Snapshot loader tests (requires DB)
python -m pytest tests/test_snapshot_loader.py -v
```

---

## Requirements

- Python 3.9+
- pandas, numpy, scipy (already in Anaconda)
- pydantic v2
- jsonschema
- click (optional, for CLI)
