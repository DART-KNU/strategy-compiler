# 전략 백테스트 보고서: Momentum Top30% Low-Debt

**run_id**: `d7edb298`
**실행 일시**: 2026-03-26 21:18:21
**모드**: contest
**기간**: 2023-03-18 ~ 2026-03-20 (2.9년, 731 거래일)
**초기 자금**: 10억 원
**벤치마크**: KOSPI200

---

## 1. 전략 개요

시가총액 상위 30%, 부채비율 100% 미만의 두 가지 필터를 통과한 종목 중에서
3개월 모멘텀(ret_60d) 상위 20개 종목을 동일가중으로 보유하는 전략.

**대회(contest) 규정 준수**를 위해 월간 신호 계산 + 주간 점진 실행(이중 주기) 방식을 채택하였으며,
매주 5 ~ 7% 범위 내에서 목표 포트폴리오를 향해 점진적으로 이동한다.

---

## 2. 전략 팩터 및 엔진 함수 구조

### 2.1 Node Graph (팩터 계산 파이프라인)

백테스트 엔진의 `backtest_engine/graph/node_executor.py`가 아래 DAG를 매 신호일에 실행한다.

```
market_cap  ──→  cs_op(rank)  ──→  predicate(gte 0.7)  ──→ ┐
                                                              combine(mul) ──→ condition ──→ score
net_debt_to_equity  ──────────→  predicate(lt 1.0)  ────→ ┘       ↑
                                                               [true]  ret_60d → cs_op(zscore)
                                                               [false] constant(0.0)
```

| 노드 | 유형 | 함수 | 설명 |
|------|------|------|------|
| `mcap` | `field` | DB 컬럼 직접 조회 | 시가총액 |
| `debt` | `field` | DB 컬럼 직접 조회 | 순부채비율 (net_debt_to_equity) |
| `mom` | `field` | DB 컬럼 직접 조회 | 3개월 수익률 (ret_60d, 약 60 거래일) |
| `mcap_rank` | `cs_op: rank` | `operators.rank()` | 횡단면 백분위 순위, [0, 1] 정규화 |
| `thr_70` | `constant` | — | 상수 0.7 (상위 30% 기준값) |
| `thr_debt` | `constant` | — | 상수 1.0 (부채비율 100%) |
| `is_top30` | `predicate: gte` | `operators.gte()` | mcap_rank ≥ 0.7 → 1, else 0 |
| `is_lowdebt` | `predicate: lt` | `operators.lt()` | net_debt_to_equity < 1.0 → 1, else 0 |
| `both_pass` | `combine: mul` | `operators.mul()` | AND 결합: is_top30 × is_lowdebt |
| `mom_z` | `cs_op: zscore` | `operators.zscore()` | 모멘텀 횡단면 Z-score 정규화 |
| `zero` | `constant` | — | 상수 0.0 |
| `score` | `condition` | `operators.if_else()` | both_pass=1 → mom_z, else 0 |

### 2.2 주요 엔진 함수 설명

#### `cs_op: rank` — `operators.rank(series)`
- 횡단면(cross-sectional) 백분위 순위 계산
- 전체 유니버스 내 시가총액을 낮은 것부터 0, 높은 것까지 1로 정규화
- 상위 30% 기준: rank ≥ 0.70

#### `predicate: gte / lt` — `operators.gte(a, b)` / `operators.lt(a, b)`
- 두 Series를 원소별 비교해 Boolean(0/1) Series 반환
- 인덱스 불일치 방지를 위해 내부에서 `b`를 `a.index`로 reindex 처리

#### `combine: mul` — `operators.mul(a, b)`
- 원소별 곱셈: 두 Boolean Series의 논리 AND
- 0 × 1 = 0, 1 × 1 = 1

#### `condition` — `operators.if_else(condition, true_val, false_val)`
- condition Series가 1(True)인 종목 → mom_z 값 반영
- condition이 0(False)인 종목 → 0으로 마스킹 (사실상 선택 불가)

#### `cs_op: zscore` — `operators.zscore(series)`
- 횡단면 Z-score: `(x - μ) / σ`
- 스코어를 평균 0, 표준편차 1로 정규화하여 팩터 간 스케일 통일

### 2.3 종목 선택 및 비중 배분

- **Selection**: `top_n`, n=20 → score 상위 20개 종목 선택 (최소 10개 미만이면 신호 스킵)
- **Allocator**: `equal_weight` → 선택된 종목에 균등 배분 (각 ~5%)
- **Constraints**: `max_weight=0.15`, `target_cash_weight=0.005` (현금 0.5% 유지)

---

## 3. 이중 주기 점진적 리밸런싱 (Dual-Cadence Gradual Rebalancing)

### 3.1 배경 및 설계 이유

대회 규정상 **매주 최소 5% 이상의 포트폴리오 회전**이 필요하다.
그러나 단순 주간 리밸런싱은 매주 팩터를 재계산하므로 거래비용이 과다하고,
월간 리밸런싱은 주간 회전율 규정을 충족하지 못한다.

이를 해결하기 위해 **신호 주기(월간)** 와 **실행 주기(주간)** 를 분리하는
이중 주기 방식을 채택한다.

| 파라미터 | 값 | 의미 |
|---|---|---|
| `frequency` | `weekly` | 신호 계산 주기 (매주 목표 포트폴리오 갱신) |
| `execution_cadence` | `weekly` | 실행 주기 |
| `min_turnover_per_rebalance` | 5% | 주간 최소 편도 회전율 (대회 규정 하한) |
| `max_turnover_per_rebalance` | 7% | 주간 최대 편도 회전율 (거래비용 상한) |

> 이 전략에서는 신호와 실행이 모두 주간이지만, 회전율 범위 [5%, 7%]로
> 점진적 이동이 강제된다.

### 3.2 알고리즘: `_partial_target()`

**입력**
- `current_weights` $w_t$: 현재 포트폴리오 비중 벡터
- `target_weights` $w^*$: 팩터 신호로 계산된 목표 비중 벡터
- `min_turnover` $T_{min}$, `max_turnover` $T_{max}$

**step 1: 필요 회전율 계산**

$$\text{needed} = \frac{1}{2} \sum_i |w^*_i - w_{t,i}|$$

이는 목표까지 가기 위한 편도(one-way) 회전율이다.
(매수합 = 매도합 = 전체 변동의 절반)

**step 2: 실제 실행 step 결정**

$$\text{step} = \text{clip}(\text{needed},\ T_{min},\ T_{max})$$

단, `step ≤ needed` 는 항상 보장 (목표를 초과하지 않음).

- `needed < T_min`: 갭이 min보다 작으면 갭 전체를 소진 (불필요한 과잉 거래 방지)
- `needed > T_max`: 최대 `T_max`만큼만 이동
- `T_min ≤ needed ≤ T_max`: 갭 전체를 소진

**step 3: 중간 목표 비중 계산**

$$\text{scale} = \frac{\text{step}}{\text{needed}}$$

$$w_{t+1} = w_t + (w^* - w_t) \times \text{scale}$$

즉, 현재에서 목표 방향으로 `scale` 비율만큼 이동한 중간 지점을 그 주의 목표로 삼는다.

**예시** (min=5%, max=7%, needed=20%인 경우)

| 주차 | needed | step | 이동 후 잔여 갭 |
|------|--------|------|----------------|
| 1주 | 20% | 7% | 13% |
| 2주 | 13% | 7% | 6% |
| 3주 | 6% | 6% | 0% → 도달 |

→ 3주에 걸쳐 20% 갭을 7%-7%-6%로 분할하여 소화.

### 3.3 엔진 내 실행 흐름

```
[매 거래일 루프]
  │
  ├─ 신호일(signal_set)이면:
  │     _compute_target_weights() 실행 → pending_target_weights 갱신
  │     (실제 거래 없음)
  │
  └─ 실행일(exec_dates)이면:
        current_weights = _current_weights(holdings, cash, prices)
        effective_target = _partial_target(current_weights, pending_target_weights,
                                           max=T_max, min=T_min)
        _execute_with_target(effective_target) → 실제 매매 집행
```

---

## 4. 백테스트 성과 요약

### 4.1 수익률

| 지표 | 전략 | KOSPI200 벤치마크 |
|------|------|-------------------|
| 누적 수익률 | **+70.58%** | +178.68% |
| 연환산 CAGR | **+20.21%** | +42.38% |
| 초과 수익률 (누적) | **-108.10%** | — |
| 초과 CAGR | -22.16% | — |

전략 자체는 3년간 +70.6%의 절대 수익을 달성했으나,
같은 기간 KOSPI200이 +178.7% 급등하면서 상대 성과는 크게 하회한다.
이 기간 벤치마크의 이례적 강세(2023~2026년 반도체·AI 랠리)가 주요 원인으로 추정된다.

### 4.2 위험

| 지표 | 값 |
|------|----|
| 연환산 변동성 | 34.23% |
| 최대 낙폭 (MDD) | -30.51% |
| MDD 고점일 | 2023-07-31 |
| MDD 저점일 | 2024-12-09 |
| MDD 지속 거래일 | 331일 (약 1년 4개월) |
| 베타 (vs KOSPI200) | 0.894 |

변동성 34.2%는 상당히 높은 수준이며, 코스닥 소형주 비중이 높은 전략 특성상
시장 대비 낙폭도 크게 나타난다. MDD 331거래일은 장기 고통 구간의 존재를 시사한다.

### 4.3 위험 조정 수익률

| 지표 | 값 | 해석 |
|------|----|------|
| 샤프 비율 | 0.710 | 보통 수준 (≥1.0이면 양호) |
| 소르티노 비율 | 1.001 | 하방 리스크 대비 적정 |
| 칼마 비율 | 0.662 | CAGR / MDD — 보통 |
| 정보 비율 (IR) | -0.546 | 벤치마크 대비 알파 미미 |
| 추적오차 (TE) | 26.08% | 벤치마크와 괴리 매우 큼 |

### 4.4 거래 효율성

| 지표 | 값 |
|------|----|
| 평균 월간 회전율 (편도) | 32.51% |
| 일간 승률 | 51.78% |
| 제약 위반 | 없음 (0건) |

첫 달(2023-03) 47.9%의 회전율은 초기 포트폴리오 구축 비용이며,
이후 월 27~37% 수준으로 안정화된다.
회전율 범위 [5%, 7%]를 주간 실행에 적용하면 월간 합산이 20~30%대에 수렴한다.

---

## 5. 한계 및 개선 방향

| 한계 | 개선 방안 |
|------|-----------|
| 벤치마크 대비 큰 언더퍼폼 | 시가총액 상위 30% 필터가 대형주를 과도하게 제한할 수 있음 → 상위 50%로 완화 검토 |
| 높은 변동성 (34%) | 저변동성 팩터(vol_20d neg) 추가하여 포트폴리오 안정화 |
| MDD 331거래일 | 모멘텀 크래시 구간 방어를 위한 시장 국면 필터 추가 고려 |
| 회전율 32%/월 | 거래비용 누적 부담 — max_turnover를 5%로 낮춰 비용 절감 가능 |

---

*생성일: 2026-03-26 | DART Backtest Engine v1.0*
