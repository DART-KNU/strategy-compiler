# DART-backtest-NL 사용법

---

## 준비 — 환경 설정

### 1. 가상환경 생성 및 패키지 설치

```bash
# 1) 가상환경 생성 (Python 3.11+)
python -m venv .venv

# 2) 가상환경 활성화
# Windows (PowerShell)
.venv\Scripts\Activate.ps1
# Windows (Git Bash / MINGW64)
source .venv/Scripts/activate
# macOS / Linux
source .venv/bin/activate

# 3) 패키지 설치
pip install -r requirements.txt
```

> **주의 (Windows):** PowerShell에서 스크립트 실행 정책 오류가 나면 아래 명령 후 재시도하세요.
> ```powershell
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
> ```

### 2. OpenAI API 키 설정 (AI 대화 기능 사용 시)

```bash
# .env.example을 복사 후 키 입력
cp .env.example .env
# .env 파일에서 OPENAI_API_KEY=sk-... 입력

# 또는 셸 환경변수로 바로 설정
export OPENAI_API_KEY=sk-...        # macOS/Linux
set OPENAI_API_KEY=sk-...           # Windows CMD
$env:OPENAI_API_KEY="sk-..."        # Windows PowerShell
```

### 3. 설치 확인

```bash
python -m backtest_engine.api.describe_dataset \
  --db database/db/data/db/backtest.db
```

아래와 비슷한 출력이 나오면 정상입니다:
```
캘린더 기간: 2020-12-30 ~ 2026-03-21
가격 데이터: 2021-01-04 ~ 2026-03-20
유니버스 크기: 약 272 종목
인덱스 코드: KOSPI, KOSPI200, KOSDAQ, KRX300
```

---

## 데이터 커버리지

| 데이터 | 기간 | 비고 |
|--------|------|------|
| 가격·거래량 (DataGuide 주식) | 2021-01-01 ~ 2026-03-20 | |
| 지수 (KOSPI, KOSPI200, KOSDAQ, KRX300) | 2021-01-01 ~ 2026-03-20 | |
| 재무 데이터 (DataGuide 분기) | 2018 ~ 2026 | `available_date` 기준 PIT-safe |
| 투자경고·위험·주의 (KIND) | **2023-03-18 ~ 2026-03-18** | 이 기간 이전 없음 |
| 상장폐지·신규상장 이력 | 1999 ~ 2026-03-18 | 생존 편향 방지 |

**완전히 신뢰 가능한 백테스트 시작일: 2023-03-18**
2021~2023-03 구간은 가격 팩터(모멘텀 등)는 사용 가능하나 규제 데이터 누락으로 유니버스 편향이 있습니다.

---

## 1. 대화형 전략 설계 (권장)

AI와 대화하면서 전략을 단계적으로 구체화하고, 확정되면 자동으로 백테스트를 실행합니다.

```bash
python -m backtest_engine.api.chat \
  --db database/db/data/db/backtest.db \
  --out runs/
```

### 대화 흐름

AI가 7단계로 전략을 구체화합니다. **한 번에 하나씩 질문**합니다.

```
============================================================
  DART 전략 설계 대화
============================================================
  종료: Ctrl+C 또는 'exit'
============================================================

[전략] 안녕하세요! 이 엔진에서 만들 수 있는 전략을 소개해드릴게요.
       모멘텀, 멀티팩터, 퀄리티, 저변동성, 벤치마크 추종, 향상된 인덱스 등을
       설계할 수 있습니다. 어떤 전략을 만들어 볼까요?

> 모멘텀 + 마르코위츠 최적화로 KOSPI 상위 종목 전략 만들고 싶어

[전략] 좋습니다! 룩백 기간은 얼마로 할까요?
       단기(1개월), 중기(3~6개월), 장기(12개월) 중 선택해 주세요.

> 3개월

[전략] 1개월 단기 반전(리버설)을 제거할까요?
       모멘텀 전략에서 일반적으로 최근 1개월 성과를 제거하면
       단기 과매수 노이즈를 줄일 수 있습니다.

> 응 제거해줘

[전략] 섹터 중립화가 필요할까요?
       섹터 중립화 시 특정 섹터에 쏠리지 않고 섹터 내 상대 강도를 평가합니다.

> 필요해

[전략] 보유 종목 수는 몇 개로 할까요? (집중형 10~20 / 분산형 50+)

> 20개

[전략] 마르코위츠 최적화의 위험 회피 계수(gamma)를 어떻게 설정할까요?
       - 0.5: 수익 추구형 (고위험·고수익)
       - 1.0: 기본값 (균형)
       - 3~5: 보수형 (변동성 최소화)

> 2로 설정해줘

[전략] 공분산 추정 윈도우는 몇 일로 할까요?
       20일(단기 적응), 60일(기본), 120일(장기 안정) 중?

> 60일

[전략] 리밸런싱 주기는 어떻게 할까요? (주간/월간/분기별)

> 월간

[전략] 체결 기준은 어떻게 할까요?
       - next_open: 익일 시초가 (현실적, 권장)
       - same_close: 당일 종가 (연구용, 낙관적)

> next_open으로

[전략] 백테스트 기간은 어떻게 할까요?
       완전 신뢰 시작일은 2023-03-18입니다.

> 2023년 3월부터 2026년 3월까지

[전략] 초기 자금은 얼마로 할까요? (기본: 10억원)

> 기본으로

------------------------------------------------------------
  [전략 요약]
------------------------------------------------------------
  3개월 모멘텀(1개월 스킵·섹터중립), 상위 20종목,
  마르코위츠(gamma=2.0, cov_lookback=60일), 월별 리밸런싱
  기간: 2023-03-18 ~ 2026-03-20
------------------------------------------------------------

[전략] 전략이 완성됐습니다! 위 내용으로 백테스트를 실행할까요?

> ㅇㅇ 실행해

------------------------------------------------------------
  백테스트 실행 중...
------------------------------------------------------------
============================================================
  [백테스트 결과]
============================================================
  총 수익률           55.68%
  연환산(CAGR)        16.48%
  벤치마크 수익률     142.99%
  초과 수익률         -87.31%
  연환산 변동성       42.76%
  샤프 비율           0.569
  최대 낙폭(MDD)      -49.17%
  ...
============================================================

  결과 폴더:  runs/my_momentum_mv__a1b2c3d4/
  HTML 리포트: my_momentum_mv__a1b2c3d4.html
  JSON 데이터: my_momentum_mv__a1b2c3d4.json
```

### AI 질문 단계

| 단계 | 항목 |
|------|------|
| 1 | 팩터 선택 (모멘텀/밸류/퀄리티 등) |
| 2 | 룩백 기간, 단기반전 제거, 섹터중립화 |
| 3 | 보유 종목 수 |
| 4 | 비중 배분 방식 + **방식별 핵심 파라미터** (아래 참고) |
| 5 | 리밸런싱 주기 |
| 6 | 섹터 제약 여부 |
| 7 | 초기 자금, 체결 기준, 모드(research/contest) |

**비중 배분 방식별 추가 질문:**

| 방식 | 물어보는 파라미터 | 설명 |
|------|-----------------|------|
| `mean_variance` | `risk_aversion` | 0.5(공격) / 1.0(기본) / 3~5(보수) |
| | `cov_lookback` | 공분산 추정 윈도우 (20/60/120일) |
| | `cov_model` | shrinkage_cov(기본) / sample_cov / diagonal_vol |
| `score_weighted` | `power` | 1.0(선형) / 2.0(집중) / 0.5(완화) |
| `inverse_vol` | `vol_field` | vol_20d / vol_60d |
| `benchmark_tracking` | `te_target` | 목표 추적오차 (5%/10%/없음) |
| | `turnover_penalty` | 회전율 억제 강도 |
| `enhanced_index` | `alpha_weight` | 알파 vs 벤치마크 추적 강도 |
| | `te_penalty` | 추적오차 페널티 |

### 결과물

실행 완료 시 `runs/{strategy_id}__{run_id}/` 폴더에 저장:
- `{strategy_id}__{run_id}.json` — 전체 결과 번들 (NAV 시계열, 지표, 보유 종목 등)
- `{strategy_id}__{run_id}.html` — 자립형 HTML 리포트 (차트 포함, 외부 의존성 없음)

HTML 리포트 내용:
- 핵심 성과 지표 카드 (수익률/샤프/MDD 색상 코딩)
- 누적 수익률 차트 (전략 vs 벤치마크)
- 낙폭(Drawdown) 차트
- 월별 수익률 히트맵
- 섹터 배분 차트
- AI 전략 리뷰 (한국어)

### 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--db` | `database/db/data/db/backtest.db` | SQLite DB 경로 |
| `--out` | `runs/` | 결과 저장 디렉터리 |
| `--model` | `gpt-4o` | OpenAI 모델 |
| `--verbose` | off | LLM 원시 응답 출력 |

### AI 상태 흐름

- **clarifying**: 정보가 부족하면 질문 (한 번에 하나씩)
- **ready**: 모든 파라미터 확정 → 전략 요약 출력 후 실행 여부 확인
- **confirmed**: 사용자 동의 맥락 감지 → 백테스트 자동 실행 (키워드 매칭 아님)

---

## 2. 백테스트 직접 실행 (CLI)

전략 JSON 파일을 직접 실행합니다.

```bash
python -m backtest_engine.api.run_backtest \
  --input backtest_engine/strategy_ir/examples/momentum_strategy.json \
  --db database/db/data/db/backtest.db \
  --out runs/ \
  --verbose
```

---

## 3. 전략 검증 (백테스트 없이)

```bash
python -m backtest_engine.api.validate_strategy \
  --input backtest_engine/strategy_ir/examples/multifactor_strategy.json
```

---

## 4. DB 현황 확인

```bash
python -m backtest_engine.api.describe_dataset \
  --db database/db/data/db/backtest.db
```

날짜 범위, 종목 수, 사용 가능한 필드 목록 출력.

---

## 5. Python API

### 전략 JSON → 백테스트

```python
from backtest_engine.api.run_backtest import run_backtest

report = run_backtest(
    ir_dict={
        "strategy_id": "my_strategy",
        "date_range": {"start": "2023-03-18", "end": "2026-03-20"},
        "rebalance_frequency": "monthly",
        "mode": "research",
        "benchmark": {"index_code": "KOSPI200"},
        "initial_capital": 1000000000,
        "sleeves": [{
            "sleeve_id": "main",
            "node_graph": {
                "nodes": {
                    "score": {"node_id": "score", "type": "field", "field_id": "ret_60d"}
                },
                "output": "score"
            },
            "selection": {"method": "top_n", "n": 20},
            "allocator": {
                "type": "mean_variance",
                "risk_aversion": 1.0,
                "cov_lookback": 60,
                "cov_model": "shrinkage_cov",
                "alpha_ref": "score"
            },
            "constraints": {"max_weight": 0.15, "target_cash_weight": 0.005},
            "execution": {"fill_rule": "next_open", "commission_bps": 10, "slippage_bps": 10}
        }]
    },
    db_path="database/db/data/db/backtest.db",
    save_to="runs/",
)

print(report["summary_metrics"])
```

### 대화 루프를 코드에서 직접 사용

```python
from backtest_engine.compiler.strategy_chat import StrategyChat
from backtest_engine.compiler.chat_models import ChatStatus

chat = StrategyChat(db_path="database/db/data/db/backtest.db")

while True:
    user_input = input("> ")
    response = chat.send(user_input)
    print(response.message)

    if response.status == ChatStatus.CONFIRMED:
        report, narration = chat.run_and_narrate(out_dir="runs/")
        print(narration)
        break
```

### 전략만 컴파일 (실행 없이)

```python
from backtest_engine.api.compile_strategy import compile_strategy

ir, warnings = compile_strategy({
    "strategy_id": "test",
    "date_range": {"start": "2023-03-18", "end": "2026-03-20"},
    "sleeves": [...]
})
print(ir.model_dump_json(indent=2))
```

---

## 6. 샘플 전략

| 파일 | 내용 |
|------|------|
| `momentum_strategy.json` | 60일 모멘텀, 상위 20종목, 동일가중, 월별 리밸런싱 |
| `multifactor_strategy.json` | 퀄리티+모멘텀+레버리지 복합, 섹터중립, 스코어가중 |
| `enhanced_index_strategy.json` | KOSPI200 추종 + 모멘텀 알파 |

경로: `backtest_engine/strategy_ir/examples/`

---

## 7. 전략 JSON 작성 가이드

### 기본 구조

```json
{
  "strategy_id": "my_id",
  "mode": "research",
  "date_range": {"start": "2023-03-18", "end": "2026-03-20"},
  "rebalance_frequency": "monthly",
  "benchmark": {"index_code": "KOSPI200"},
  "initial_capital": 1000000000,
  "sleeves": [
    {
      "sleeve_id": "main",
      "node_graph": { ... },
      "selection": {"method": "top_n", "n": 30},
      "allocator": {"type": "equal_weight"},
      "constraints": {"max_weight": 0.10, "min_names": 10},
      "execution": {"fill_rule": "next_open", "commission_bps": 10, "slippage_bps": 10}
    }
  ]
}
```

### Node Graph — 팩터 계산 DAG

```json
"nodes": {
  "raw":     {"node_id": "raw",     "type": "field",   "field_id": "ret_60d"},
  "lag1m":   {"node_id": "lag1m",   "type": "ts_op",   "op": "lag",    "input": "raw", "window": 20},
  "score":   {"node_id": "score",   "type": "ts_op",   "op": "zscore", "input": "raw", "window": 252},
  "cs_rank": {"node_id": "cs_rank", "type": "cs_op",   "op": "rank",   "input": "score"},
  "combo":   {"node_id": "combo",   "type": "combine", "op": "weighted_sum",
              "inputs": ["cs_rank", "lag1m"], "weights": [0.7, 0.3]}
}
```

### 사용 가능한 필드 (`field_id`)

| 카테고리 | 필드 |
|---------|------|
| 수익률 | `ret_1d`, `ret_5d`, `ret_20d`, `ret_60d` |
| 변동성/유동성 | `vol_20d`, `adv5`, `adv20`, `turnover_ratio` |
| 가격 | `close`, `adj_close`, `open`, `high`, `low`, `volume` |
| 시장 | `market_cap`, `price_to_52w_high`, `listing_age_bd` |
| 펀더멘털 | `sales_growth_yoy`, `op_income_growth_yoy`, `net_debt_to_equity`, `cash_to_assets` |
| 원시 재무 | `total_assets`, `sales`, `operating_income`, `net_income_parent` |

### 비중 배분(Allocator) 타입 및 파라미터

| type | 설명 | 주요 파라미터 | 기본값 |
|------|------|------------|--------|
| `equal_weight` | 동일가중 | — | — |
| `score_weighted` | 스코어 비례 | `power` | 1.0 |
| `inverse_vol` | 역변동성 | `vol_field` | vol_20d |
| `mean_variance` | 마르코위츠 최적화 | `risk_aversion`, `cov_lookback`, `cov_model`, `alpha_ref` | 1.0, 60, shrinkage_cov |
| `benchmark_tracking` | 벤치마크 추적 | `benchmark_index`, `te_target`, `turnover_penalty` | KOSPI200, null, 0.001 |
| `enhanced_index` | 벤치마크 + 알파 | `alpha_weight`, `te_penalty`, `te_target` | 1.0, 1.0, null |

### 선택(Selection) 방법

| method | 설명 |
|--------|------|
| `top_n` | 상위 N종목 |
| `top_pct` | 상위 N% |
| `threshold` | 스코어 임계값 이상 |
| `all_positive` | 양수 스코어 전체 |

### 컨스트레인트

```json
"constraints": {
  "max_weight": 0.10,
  "min_weight": 0.01,
  "min_names": 10,
  "max_names": 50,
  "max_sector_weight": 0.30,
  "max_sector_multiplier": 2.0,
  "target_cash_weight": 0.005
}
```

### 체결(Execution) 파라미터

```json
"execution": {
  "fill_rule": "next_open",
  "commission_bps": 10,
  "sell_tax_bps": 20,
  "slippage_bps": 10
}
```

| fill_rule | 설명 |
|-----------|------|
| `next_open` | 익일 시초가 체결 (현실적, 권장) |
| `next_close` | 익일 종가 체결 |
| `same_close` | 당일 종가 체결 (연구용, 낙관적 편향) |

---

## 8. Contest 모드

```json
{
  "mode": "contest",
  ...
}
```

자동 적용 제약:
- 종목별 상한 15% (삼성전자 005930: 40%)
- 섹터 상한 2× 벤치마크 비중 (벤치마크 비중 ≤5% 섹터: 절대 10% 상한)
- 소형주 합산 ≤30% (시가총액 1조 미만)
- 주간 최소 회전율 5% (위반 시 경고)
- ADV5 기반 시장충격 비용 적용

---

## 9. 테스트 실행

```bash
# 유닛 테스트 (DB 불필요, 빠름)
python -m pytest tests/ -v \
  --ignore=tests/test_snapshot_loader.py \
  --ignore=tests/test_regression.py

# 전체 테스트 (DB 필요)
python -m pytest tests/ -v
```

---

## 10. 결과물 구조

### 저장 위치

실행 완료 시 `runs/{strategy_id}__{run_id}/` 폴더 자동 생성:

```
runs/
└── my_strategy__a1b2c3d4/
    ├── my_strategy__a1b2c3d4.json   # 전체 결과 번들
    └── my_strategy__a1b2c3d4.html  # HTML 리포트 (차트 포함)
```

여러 번 실행해도 결과가 섞이지 않습니다.

### JSON 번들 구조

```python
{
  "strategy_id": "...",
  "run_id": "...",               # 8자리 해시
  "ir_hash": "...",              # 전략 해시 (재현성 확인용)
  "date_range": {"start": "...", "end": "..."},
  "initial_capital": 1000000000,
  "benchmark_index": "KOSPI200",
  "summary_metrics": {
    "total_return": "55.68%",    # 문자열 형식
    "cagr": "16.48%",
    "sharpe": 0.569,
    "max_drawdown": "-49.17%",
    "benchmark_total_return": "142.99%",
    "excess_return": "-87.31%",
    "tracking_error": "33.96%",
    "information_ratio": -0.267,
    "beta": 1.115,
    "average_turnover": "18.66%",
    ...
  },
  "nav_series": {"2023-03-20": 1000000000.0, ...},
  "benchmark_nav_series": {"2023-03-20": 1000000000.0, ...},
  "monthly_returns": {...},
  "sector_exposure_history": {...},
  "top_holdings": [...],
  "constraint_violations": [...],
  "narration_hints": {...}
}
```
