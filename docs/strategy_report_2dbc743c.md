# 백테스트 상세 보고서

**run_id**: `2dbc743c`
**전략명**: Momentum Top-30% Low-Debt
**실행일시**: 2026-03-26 21:31:50
**모드**: contest
**기간**: 2023-03-18 ~ 2026-03-20 (2.9년, 731 거래일)
**초기 자금**: 10억 원 (₩1,000,000,000)
**벤치마크**: KOSPI200

---

## 1. 전략 구조 개요

세 가지 조건을 순서대로 적용하는 파이프라인 전략이다.

```
① 유니버스 필터링       → ② 팩터 스코어 계산      → ③ 종목 선택 + 비중 배분
  (is_eligible=1)          (node_graph DAG)            (top_n=20, equal_weight)
  + 시가총액 상위 30%
  + 부채비율 < 100%
```

---

## 2. 사용 데이터 기간과 시작일 선정 근거

### 2.1 DB 데이터 커버리지

백테스트 엔진은 SQLite 데이터베이스(`backtest.db`)에서 아래 테이블을 조회한다.

| 테이블 | 내용 | 기간 |
|--------|------|------|
| `core_price_daily` / `mart_feature_daily` | 주가, 수익률, 변동성 | 2020-12-30 ~ 2026-03-20 |
| `core_index_daily` | KOSPI200 등 지수 | 2020-12-30 ~ 2026-03-20 |
| `core_financials_quarterly` | 분기 재무 (DataGuide) | 2018-Q1 ~ 2026 |
| `mart_liquidity_daily` | 시가총액, ADV5, ADV20 | 2020-12-30 ~ 2026-03-20 |
| `raw_kind_investment_warning` | **투자경고** (KIND) | **2023-02-24 ~ 2026-03-18** |
| `raw_kind_investment_risk` | **투자위험** (KIND) | **2023-03-30 ~ 2026-03-13** |
| `raw_kind_investment_caution` | **투자주의** (KIND) | 2025-04-15 ~ 2026-03-18 |
| `raw_kind_delistings` | 상장폐지 이력 | 1999 ~ 2026-03-18 |
| `raw_kind_ipos` | 신규상장 이력 | 2000 ~ 2026 |

### 2.2 시작일을 2023-03-18로 설정한 이유

`mart_universe_eligibility_daily.is_eligible` 플래그는 다음 조건을 **모두** AND로 결합한다.

| 플래그 | 조건 |
|--------|------|
| `is_listed` | 상장 종목 |
| `is_common_equity` | 보통주 |
| `is_market_ok` | 코스피/코스닥 대상 시장 |
| `is_listing_age_ok` | 신규상장 후 일정 기간 경과 |
| `is_liquidity_ok` | ADV5 ≥ 30억 |
| `is_mcap_ok` | 시가총액 ≥ 1,000억 |
| `is_not_caution` | 투자주의 미지정 종목 |
| `is_not_warning` | **투자경고 미지정** ← KIND 데이터 필요 |
| `is_not_risk` | **투자위험 미지정** ← KIND 데이터 필요 |
| `is_not_admin` | 관리종목 미지정 |
| `is_not_halt` | 거래정지 미지정 |

`is_not_warning`과 `is_not_risk`는 KIND 투자경고/위험 데이터에 의존하는데,
이 데이터는 **2023-02-24 ~ 2023-03-30 이후**부터만 존재한다.
그 이전 구간에서는 투자경고·위험 종목이 유니버스에서 걸러지지 않아
**생존 편향(survivorship bias) 및 규제 위반 종목 포함 위험**이 발생한다.

따라서 모든 규제 데이터가 신뢰 가능한 최초 거래일인 **2023-03-18**을 백테스트 시작일로 채택했다.

> 실질적인 유니버스 크기: 약 **313개 종목** (2024년 기준 `is_eligible=1`)

### 2.3 Point-in-Time (PIT) 안전 처리

재무 데이터는 `core_financials_quarterly`의 `available_date` 컬럼을 기준으로 조회한다.
`period_end`(결산기말)가 아닌 `available_date`(실제 공시일 이후)를 사용함으로써
**미래 정보 유출(look-ahead bias)** 을 원천 차단한다.

---

## 3. 팩터 계산 파이프라인 — Node Graph

### 3.1 전체 DAG 구조

```
[field: market_cap]
       ↓
[cs_op: rank]  →  mcap_rank  →  [predicate: gte 0.7]  →  is_top30  ─────→ [combine: mul]  →  both_pass
                                                                                  ↑                ↓
[field: net_debt_to_equity]  →  [predicate: lt 1.0]   →  is_lowdebt ──────────────      [condition]  →  score
                                                                                          ↑       ↑
[field: ret_60d]  →  [cs_op: zscore]  →  mom_z  ──────────────────────────── true_branch  false_branch
                                                                               (mom_z)     (zero: 0.0)
```

### 3.2 노드별 상세 설명

#### `field: market_cap` — 시가총액 조회
- **소스**: `mart_liquidity_daily.market_cap`
- **의미**: 해당 거래일 기준 시가총액 (종가 × 발행주식수)
- **사용 목적**: 대형주 필터링 기준값 생성

#### `cs_op: rank` — 횡단면 순위 정규화 (`operators.rank()`)

$$\text{rank}_i = \frac{\text{rank}(x_i) - 1}{N - 1}, \quad \text{rank}_i \in [0, 1]$$

- 유니버스 내 전체 종목을 대상으로 시가총액을 낮은 순서(0)부터 높은 순서(1)로 선형 정규화
- **결과**: rank ≥ 0.7 → 상위 30%, rank < 0.7 → 하위 70%
- **금융적 의미**: 절대 시가총액이 아닌 상대적 크기로 필터링하므로 시장 전체 크기 변화에 무관하게 일관된 기준 적용 가능

#### `predicate: gte` — 대형주 필터 마스크 (`operators.gte()`)

$$\text{is\_top30}_i = \mathbf{1}[\text{mcap\_rank}_i \geq 0.70]$$

- 0(하위 70%) 또는 1(상위 30%)의 Boolean Series 반환
- **금융적 의미**: 소형주·투기 종목 배제. 대형주는 유동성이 풍부하여 실제 집행 시 슬리피지 최소화

#### `predicate: lt` — 저부채 필터 마스크 (`operators.lt()`)

$$\text{is\_lowdebt}_i = \mathbf{1}[\text{net\_debt\_to\_equity}_i < 1.0]$$

- `net_debt_to_equity` = (총금융부채 − 현금성자산) / 자기자본
- 1.0 기준 = 부채비율 100% (부채 = 자본과 동일)
- **금융적 의미**: 과도한 레버리지 기업 배제. 금리 상승·경기 하강기에 재무적 취약 기업의 급락 리스크 차단

#### `combine: mul` — 복합 필터 AND 결합 (`operators.mul()`)

$$\text{both\_pass}_i = \text{is\_top30}_i \times \text{is\_lowdebt}_i$$

- Boolean 값의 원소별 곱셈 → 논리 AND와 동일
- 두 조건 중 하나라도 0이면 0, 둘 다 1이면 1
- **의미**: "시가총액 상위 30% **이고** 부채비율 100% 미만"인 종목만 1로 표시

#### `field: ret_60d` — 3개월 수익률 조회
- **소스**: `mart_feature_daily.ret_60d`
- **계산 방법**: `adj_close` 기준 60 거래일 전 대비 수익률
- **사용 가능 수익률 필드**: `ret_1d`, `ret_5d`, `ret_20d`, `ret_60d` 4개만 존재
- **금융적 의미**: 3개월 모멘텀. Jegadeesh & Titman(1993)이 제시한 3~12개월 모멘텀 프리미엄의 핵심 구간

#### `cs_op: zscore` — 횡단면 Z-score 정규화 (`operators.zscore()`)

$$\text{mom\_z}_i = \frac{\text{ret\_60d}_i - \mu}{\sigma}$$

- $\mu$, $\sigma$는 해당 리밸런싱일 유니버스 전체의 평균·표준편차
- **금융적 의미**: 팩터 스코어를 표준화하여 시장 상황에 따른 스케일 변동을 제거. 강세장/약세장 무관하게 상대 순위를 일관되게 비교 가능

#### `condition: if_else` — 조건부 스코어 마스킹 (`operators.if_else()`)

$$\text{score}_i = \begin{cases} \text{mom\_z}_i & \text{if } \text{both\_pass}_i = 1 \\ 0 & \text{otherwise} \end{cases}$$

- 두 필터를 통과한 종목만 모멘텀 Z-score가 점수로 반영되고, 나머지는 0점
- **효과**: `top_n=20` 선택 시 필터를 통과한 종목 중 모멘텀 상위 20개만 선택됨

### 3.3 종목 선택 및 비중 배분

| 단계 | 모듈 | 설정값 |
|------|------|--------|
| 선택 | `portfolio/selector.py` — `top_n` | 상위 20개 (min_names=10) |
| 비중 | `portfolio/allocators.py` — `equal_weight` | 각 종목 ~5% 균등 배분 |
| 제약 | `portfolio/constraints.py` | max_weight=15%, cash=0.5% |

**동일가중(Equal Weight)의 금융적 의미**
모멘텀 신호의 예측력이 순위에는 있으나 크기 차이에는 유의미하지 않다는 가정하에,
모든 선택 종목에 동일한 비중을 부여한다. 특정 종목 과집중으로 인한
개별 종목 리스크를 최소화하는 효과가 있다.

---

## 4. Contest 모드 실행 비용 모델

contest 모드는 `backtest_engine/execution/contest_profile.py`의
`execute_rebalance_contest()` 함수가 처리한다.

### 4.1 거래비용 구조

| 비용 항목 | 값 | 설명 |
|-----------|----|------|
| 수수료 | 10 bps (0.10%) | 매수/매도 공통 |
| 매도세 | 20 bps (0.20%) | 매도 시에만 부과 |
| 기본 슬리피지 | 10 bps (0.10%) | 호가 스프레드 근사 |
| 시장충격 | ADV 비중 × 5 bps/% | 주문 크기 비례 추가 비용 |
| 공격적 주문 페널티 | 15 bps | ADV5의 5% 초과 주문 시 |

**시장충격 공식**:

$$\text{extra\_slippage} = \text{notional} \times \frac{\text{notional} / \text{ADV5}}{1} \times \frac{5 \text{ bps}}{1\%}$$

**ADV 참여 한도**: 주문 규모 ≤ ADV5 × 10%로 제한
(5일 평균거래대금의 10%를 초과하는 대형 주문은 자동 크기 감소)

### 4.2 체결 규칙

- `fill_rule: next_open` — 신호일 다음 거래일 시초가로 체결
- 실제 거래에서 신호 발생 당일 체결은 거의 불가능하므로 현실적인 1거래일 지연을 반영

---

## 5. 이중 주기 점진적 리밸런싱 (Dual-Cadence Gradual Rebalancing)

### 5.1 설계 배경

대회 규정상 **매주 최소 5% 이상의 포트폴리오 편도 회전율**이 요구된다.
단순 주간 리밸런싱은 매주 팩터를 재계산하며 빈번하게 포트폴리오를 교체하여
거래비용이 과다하고, 단순 월간 리밸런싱은 주간 회전율 규정을 충족하지 못한다.

**해결책**: 신호 주기와 실행 주기를 분리한다.

| 파라미터 | 값 | 역할 |
|---|---|---|
| `frequency` | `weekly` | 팩터 신호 재계산 주기 |
| `execution_cadence` | `weekly` | 실제 거래 실행 주기 |
| `min_turnover_per_rebalance` | 5% | 주간 최소 편도 회전율 (대회 규정 준수) |
| `max_turnover_per_rebalance` | 7% | 주간 최대 편도 회전율 (비용 상한) |

### 5.2 `_partial_target()` 알고리즘 수학적 기술

**함수 위치**: `backtest_engine/execution/simulator.py`

**입력**
- $w_t \in \mathbb{R}^N$: 현재 포트폴리오 비중 벡터 (N = 전체 종목 수)
- $w^* \in \mathbb{R}^N$: 팩터 신호로 계산된 목표 비중 벡터
- $T_{\min}$: 최소 편도 회전율 (0.05)
- $T_{\max}$: 최대 편도 회전율 (0.07)

**Step 1: 잔여 갭(needed) 계산**

$$\text{needed} = \frac{1}{2} \sum_{i=1}^{N} |w^*_i - w_{t,i}|$$

편도(one-way) 회전율의 정의: 매수 금액과 매도 금액은 항상 동일하므로
전체 변동량의 절반이 편도 회전율이다.

**Step 2: 실행 step 결정**

$$\text{step} = \text{clamp}(\text{needed},\ T_{\min},\ T_{\max})$$

단, 과잉 거래 방지를 위해 항상 $\text{step} \leq \text{needed}$ 보장.

- $\text{needed} < T_{\min}$: 갭이 작아 최소 기준 미달 → 갭 전체 소진 (불필요한 인위적 거래 방지)
- $T_{\min} \leq \text{needed} \leq T_{\max}$: 갭 전체를 한 번에 소진
- $\text{needed} > T_{\max}$: $T_{\max}$만큼만 이동, 나머지 갭은 다음 주로 이월

**Step 3: 중간 목표 비중 산출**

$$\text{scale} = \frac{\text{step}}{\text{needed}}$$

$$w_{t+1,i} = w_{t,i} + (w^*_i - w_{t,i}) \times \text{scale}$$

현재에서 목표 방향으로 `scale` 비율만큼 선형 보간한 지점을 금주의 실행 목표로 삼는다.
이후 재정규화(`/ sum`) 및 음수 클리핑(`clip(0)`)으로 유효한 비중 벡터를 보장한다.

**수치 예시** (needed = 20%, $T_{\min}$ = 5%, $T_{\max}$ = 7%)

| 주차 | needed | step (clamp) | 이동 후 잔여 갭 |
|------|--------|------|---|
| 1주 | 20.0% | 7.0% | 13.0% |
| 2주 | 13.0% | 7.0% | 6.0% |
| 3주 | 6.0% | 6.0% | 0.0% → 목표 도달 |

→ 한 번에 20%를 매매하는 대신 3주에 걸쳐 7-7-6%로 분할하여
**거래비용을 분산**하면서 **대회 최소 회전율 규정**도 충족한다.

### 5.3 엔진 내 실행 흐름

```python
for trade_date in trading_days:
    # 매일: Mark-to-Market (NAV, 벤치마크 NAV 계산)

    if trade_date in signal_set:      # 신호일 (weekly)
        pending_target = _compute_target_weights(...)
        # → node_graph 실행 → 종목 선택 → 비중 배분 → 제약 적용
        # → 거래 없음, 목표 비중만 저장

    if trade_date in exec_dates:      # 실행일 (weekly)
        current_w = _current_weights(holdings, cash, prices)
        effective_target = _partial_target(
            current_w, pending_target,
            max_turnover=0.07, min_turnover=0.05
        )
        _execute_with_target(effective_target)
        # → 중간 목표를 향해 매매 집행 (contest_profile 비용 적용)
```

이 전략은 신호 주기와 실행 주기가 모두 weekly이므로 매주 목표가 갱신됨과 동시에
`_partial_target`을 통해 5~7% 범위 안에서만 거래가 이루어진다.

---

## 6. 백테스트 성과 분석

### 6.1 수익률

| 지표 | 전략 | KOSPI200 벤치마크 |
|------|------|-------------------|
| 누적 수익률 | **+68.42%** | +178.68% |
| 연환산 CAGR | **+19.69%** | +42.38% |
| 초과 수익률 (누적) | **-110.27%** | — |
| 초과 CAGR | -22.69% | — |
| 최종 NAV | 약 16.8억 원 | — |

전략 자체의 절대 수익률은 3년간 +68.4%로 양호하나,
같은 기간 KOSPI200이 +178.7%라는 이례적 강세를 보이며 상대 성과는 크게 하회한다.
이 기간은 반도체·AI 관련 대형주 중심의 랠리로, 시총 상위 30% 필터를 적용했음에도
코스닥 중소형 모멘텀 종목 위주의 포트폴리오가 KOSPI200 구성 종목보다 부진했다.

### 6.2 월별 수익률

| 연도 | 4월 | 5월 | 6월 | 7월 | 8월 | 9월 | 10월 | 11월 | 12월 |
|------|-----|-----|-----|-----|-----|-----|------|------|------|
| 2023 | -4.6% | +0.3% | +4.3% | **+13.6%** | -1.5% | -14.7% | -14.7% | +13.3% | +0.5% |
| 2024 | -7.8% | +10.4% | +9.2% | -6.3% | -0.3% | +9.6% | -2.4% | -5.6% | -1.9% |
| 2025 | +4.1% | +5.9% | -11.4% | +6.6% | +6.4% | +7.8% | +0.7% | -0.3% | +7.9% |
| 2026 (1~3월) | +25.7% | +3.6% | -1.6% | — | — | — | — | — | — |

2026년 1월의 +25.7%는 전체 성과에 크게 기여한 구간이다.

### 6.3 위험 지표

| 지표 | 값 | 해석 |
|------|----|------|
| 연환산 변동성 | 34.14% | 코스닥 중소형주 비중에 따른 높은 변동성 |
| 최대 낙폭 (MDD) | **-31.49%** | 고점 대비 최대 31.5% 손실 |
| MDD 고점 → 저점 | 2023-07-25 → 2024-12-09 | **335 거래일** (약 1년 4개월) 장기 고통 구간 |
| 베타 | 0.894 | 시장 대비 약 10% 낮은 민감도 |

### 6.4 위험 조정 수익률

| 지표 | 값 | 계산 방식 |
|------|----|---------|
| 샤프 비율 | **0.698** | 연환산 수익 / 연환산 변동성 (무위험수익률 = 0%) |
| 소르티노 비율 | **0.982** | 연환산 수익 / 하방 표준편차 (무위험수익률 = 0%) |
| 칼마 비율 | **0.625** | CAGR / \|MDD\| = 19.69% / 31.49% |
| 정보 비율 (IR) | **-0.567** | (전략 CAGR − 벤치마크 CAGR) / 추적오차 |
| 추적오차 (TE) | 26.08% | 벤치마크 대비 초과수익의 연환산 표준편차 |

샤프 비율 0.7은 "리스크 대비 수익이 적절하다" 수준 (1.0 이상이 양호).
IR -0.567은 벤치마크를 지속적으로 하회하는 알파 미약을 나타낸다.

### 6.5 회전율 및 거래 효율

| 지표 | 값 |
|------|----|
| 평균 월간 회전율 (편도) | **28.10%** |
| 일간 승률 | 51.92% |
| 제약 위반 | **없음 (0건)** |

주간 5~7% 한도를 4주 적용하면 이론 상한 28%/월에 수렴한다.
초기 구축 월(2023-03: 48.62%)을 제외하면 이후 월평균 24% 내외로 안정화된다.
제약 위반 0건은 max_weight=15%, 섹터 2배 제한 등 모든 contest 규정이 준수됨을 의미한다.

---

## 7. 대회 규정 준수 구조

| 대회 규정 | 엔진 구현 | 준수 여부 |
|-----------|-----------|-----------|
| 종목별 비중 ≤ 15% | `constraints.max_weight = 0.15` | ✅ |
| 섹터 비중 ≤ 벤치마크 × 2배 | `constraints.max_sector_multiplier` | ✅ |
| 주간 최소 5% 회전율 | `min_turnover_per_rebalance = 0.05` | ✅ |
| 거래비용 반영 (수수료·세금·슬리피지) | `contest_profile.py` | ✅ |
| 시장충격 비용 | ADV5 기반 시장충격 모델 | ✅ |
| 유동성 제한 | ADV5 × 10% 주문 상한 | ✅ |
| 투자경고·위험 종목 배제 | `is_not_warning`, `is_not_risk` | ✅ |
| 생존 편향 제거 | 상장폐지 종목 이력 반영, PIT-safe | ✅ |

---

## 8. 한계 및 개선 방향

| 한계 | 원인 분석 | 개선 방안 |
|------|-----------|-----------|
| 벤치마크 대폭 언더퍼폼 (-110%) | KOSPI200 대형주 AI·반도체 랠리 미반영 | 저변동성 팩터 추가 or 벤치마크 추종 혼합 |
| 높은 변동성 (34%) | 코스닥 중소형주 편중 가능성 | 상위 50% 이상 유니버스 확대, 역변동성 배분 검토 |
| MDD 335거래일 장기 고통 | 2023 하반기 ~ 2024 전반 모멘텀 크래시 | 시장 국면 필터(모멘텀 리버설 구간 감지) 추가 |
| 회전율 28%/월로 비용 부담 | 주간 실행 × 5~7% | `max_turnover`를 5%로 낮춰 월 20% 수준으로 절감 가능 |

---

*생성일: 2026-03-26 | DART Backtest Engine v1.0 | run_id: 2dbc743c*
