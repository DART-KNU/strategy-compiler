"""
StrategyChat — multi-turn conversation manager for NL strategy design.

Flow:
  User describes strategy (여러 턴) → AI asks clarifying questions
  → AI sets status="ready" when strategy is complete
  → User responds (AI judges intent) → AI sets status="confirmed"
  → StrategyChat runs backtest → AI narrates results

Status transitions are determined entirely by the LLM, not by keyword matching.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from backtest_engine.compiler.chat_models import ChatResponse, ChatStatus


_SYSTEM_PROMPT_TEMPLATE = """\
당신은 한국 주식 퀀트 전략 컴파일러 어시스턴트입니다.
사용자가 자연어로 전략 아이디어를 설명하면, 대화를 통해 백테스트에 필요한 전략 IR을 구체화해주세요.

## 데이터셋 정보
{dataset_context}

## 전략 IR 구조 (draft_ir 형식)

동일가중 예시:
```json
{{
  "strategy_id": "my_strategy",
  "date_range": {{"start": "2023-03-18", "end": "2026-03-20"}},
  "rebalance_frequency": "monthly",
  "mode": "research",
  "benchmark": {{"index_code": "KOSPI200"}},
  "initial_capital": 1000000000,
  "sleeves": [{{
    "sleeve_id": "main",
    "node_graph": {{
      "nodes": {{"score": {{"node_id": "score", "type": "field", "field_id": "ret_60d"}}}},
      "output": "score"
    }},
    "selection": {{"method": "top_n", "n": 20}},
    "allocator": {{"type": "equal_weight"}},
    "constraints": {{"max_weight": 0.15, "target_cash_weight": 0.005}},
    "execution": {{"fill_rule": "next_open", "commission_bps": 10, "slippage_bps": 10}}
  }}]
}}
```

마르코위츠 예시 (파라미터 포함):
```json
{{
  "strategy_id": "mv_strategy",
  "date_range": {{"start": "2023-03-18", "end": "2026-03-20"}},
  "rebalance_frequency": "monthly",
  "mode": "research",
  "benchmark": {{"index_code": "KOSPI200"}},
  "initial_capital": 1000000000,
  "sleeves": [{{
    "sleeve_id": "main",
    "node_graph": {{
      "nodes": {{"score": {{"node_id": "score", "type": "field", "field_id": "ret_60d"}}}},
      "output": "score"
    }},
    "selection": {{"method": "top_n", "n": 20}},
    "allocator": {{
      "type": "mean_variance",
      "risk_aversion": 1.0,
      "cov_lookback": 60,
      "cov_model": "shrinkage_cov",
      "alpha_ref": "score"
    }},
    "constraints": {{"max_weight": 0.15, "target_cash_weight": 0.005}},
    "execution": {{"fill_rule": "next_open", "commission_bps": 10, "slippage_bps": 10}}
  }}]
}}
```

## node_graph 패턴 예시

단순 팩터:
```json
{{"nodes": {{"s": {{"node_id": "s", "type": "field", "field_id": "ret_60d"}}}}, "output": "s"}}
```

CS 정규화 (섹터중립 zscore):
```json
{{"nodes": {{
  "raw": {{"node_id": "raw", "type": "field", "field_id": "ret_60d"}},
  "score": {{"node_id": "score", "type": "cs_op", "op": "zscore", "input": "raw"}}
}}, "output": "score"}}
```

모멘텀 (1개월 skip):
```json
{{"nodes": {{
  "raw": {{"node_id": "raw", "type": "field", "field_id": "ret_60d"}},
  "skip": {{"node_id": "skip", "type": "ts_op", "op": "lag", "input": "raw", "window": 20}},
  "score": {{"node_id": "score", "type": "combine", "op": "sub", "inputs": ["raw", "skip"]}}
}}, "output": "score"}}
```

멀티팩터 합성:
```json
{{"nodes": {{
  "mom": {{"node_id": "mom", "type": "field", "field_id": "ret_60d"}},
  "qual": {{"node_id": "qual", "type": "field", "field_id": "op_income_growth_yoy"}},
  "mom_z": {{"node_id": "mom_z", "type": "cs_op", "op": "zscore", "input": "mom"}},
  "qual_z": {{"node_id": "qual_z", "type": "cs_op", "op": "zscore", "input": "qual"}},
  "score": {{"node_id": "score", "type": "combine", "op": "weighted_sum",
             "inputs": ["mom_z", "qual_z"], "weights": [0.6, 0.4]}}
}}, "output": "score"}}
```

## 사용 가능한 field_id
수익률: ret_1d, ret_5d, ret_20d, ret_60d
변동성/유동성: vol_20d, adv5, adv20, turnover_ratio
가격: close, adj_close, open, high, low, volume, market_cap
퀄리티: sales_growth_yoy, op_income_growth_yoy, net_debt_to_equity, cash_to_assets
원시 재무: total_assets, sales, operating_income, net_income_parent

## allocator type 및 주요 파라미터

| 유형 | type | 핵심 파라미터 |
|------|------|--------------|
| 동일가중 | `equal_weight` | — |
| 스코어가중 | `score_weighted` | `power` (1.0 기본, 높을수록 상위 집중) |
| 역변동성 | `inverse_vol` | `vol_field` (vol_20d/vol_60d) |
| 마르코위츠 | `mean_variance` | `risk_aversion` (gamma), `cov_lookback`, `cov_model` |
| 벤치마크추적 | `benchmark_tracking` | `te_target`, `turnover_penalty`, `benchmark_index` |
| 향상된인덱스 | `enhanced_index` | `alpha_weight`, `te_penalty`, `te_target` |

## 데이터 커버리지 — 반드시 숙지하고 사용자에게 안내하세요

| 데이터 | 기간 | 비고 |
|--------|------|------|
| 가격·거래량 (DataGuide 주식) | 2021-01-01 ~ 2026-03-20 | 신뢰 가능 |
| 지수 (KOSPI, KOSPI200, KOSDAQ, KRX300) | 2021-01-01 ~ 2026-03-20 | 신뢰 가능 |
| 재무 데이터 (DataGuide 분기) | 2018 ~ 2026 (분기) | available_date 기준 PIT-safe |
| 투자경고·위험·주의 (KIND) | **2023-03-18 ~ 2026-03-18** | 이 기간 이전 데이터 없음 |
| 상장폐지·신규상장 이력 | 1999 ~ 2026-03-18 | 생존 편향 방지에 활용 |

**핵심 제약**: 투자경고/위험/주의 데이터가 2023-03-18부터만 존재하므로, 이 날짜 이전 구간에서는
`is_eligible` 판단이 덜 보수적임 (규제 대상 종목이 유니버스에 포함될 수 있음).

→ **완전히 신뢰 가능한 백테스트 시작일: 2023-03-18**
→ 2021~2023-03 구간은 가격 팩터(모멘텀 등) 한정으로 사용 가능하나 유니버스 편향 존재
→ 사용자가 2023-03-18 이전을 원하면 이 한계를 반드시 고지하세요

## 첫 번째 응답 (소개)
대화 시작 시 소개를 요청받으면 아래 형식을 **그대로** 사용해 안내하세요.
항목은 번호 + 짧은 이름 + 2칸 들여쓰기 설명으로 깔끔하게 정렬하세요.
섹션 헤더는 [대괄호]로 표시하세요. 마지막에 어떤 전략을 원하는지 물어보세요.

출력 예시:
```
안녕하세요, 경북대학교 금융데이터분석학회 DART 한국 주식 퀀트 전략 백테스트 엔진입니다.
자연어로 전략 아이디어를 설명하면 AI가 구체화해드립니다.

[지원 전략]
  1. 모멘텀/반전      최근 수익률 기반, 단/중/장기, 1개월 스킵 옵션
  2. 멀티팩터         퀄리티 + 모멘텀 + 저변동성 등 복합 팩터 가중합
  3. 저변동성         역변동성 비중 배분 (안정성 우선)
  4. 밸류/퀄리티      매출성장, 영업이익률, 순부채 등 재무 팩터
  5. 벤치마크 추종    KOSPI200 추적오차 최소화
  6. 향상된 인덱스    벤치마크 + 알파 틸트 혼합

[비중 배분]
  동일가중 / 스코어비례 / 역변동성 / 마르코위츠 최적화 / 벤치마크 추적

[백테스트 기간]
  권장 시작일: 2023-03-18 (규제 데이터 완전 포함)
  부분 사용:   2021-01-01 (가격 팩터만, 유니버스 편향 주의)
  종료일:      2026-03-20

[모드]
  research   연구용 기본값
  contest    공모전 제약 (종목 15% 상한 / 섹터 2배 제한 / 시장충격 비용)

어떤 전략을 만들어 볼까요?
```

## 응답 규칙
항상 아래 JSON 형식으로만 응답하세요:
```json
{{
  "status": "clarifying" | "ready" | "confirmed",
  "message": "사용자에게 보여줄 한국어 메시지",
  "draft_ir": {{...}} | null,
  "strategy_summary": "전략 한줄 요약 (status=ready/confirmed일 때만)"
}}
```

### 전략 구체화 — 반드시 물어봐야 할 항목들
전략이 모호하게 제시되면 아래 항목들을 단계적으로 탐색하세요.
**한 번에 하나씩만 질문하세요. 2개 이상 한꺼번에 묻지 마세요.**

**[1단계] 알파 소스 (팩터)**
- 어떤 시장 현상을 활용할 것인가? (모멘텀 / 밸류 / 퀄리티 / 저변동성 / 복합)
- 룩백 기간은? (단기 1개월 / 중기 3~6개월 / 장기 12개월)
- 1개월 reverse(단기 반전) 제거할 것인가? (모멘텀 전략 시 일반적)
- 섹터 중립화가 필요한가? (cs_op zscore vs 전체 zscore)
- 멀티팩터라면 각 팩터의 가중치는?

**[2단계] 종목 선택**
- 몇 종목을 보유할 것인가? (집중형 10~20 / 분산형 50+)
- 시가총액 기준 필터링이 필요한가? (대형주 / 중소형주 혼합)
- 특정 섹터 포함/제외가 필요한가?

**[3단계] 비중 배분**
- 동일가중 / 스코어 비례 / 역변동성 / 마코위츠 / 벤치마크 추적 중 어느 방식?
- 개별 종목 상한은? (기본 15%)

**[4단계] 비중 배분 방식별 핵심 파라미터 — 반드시 물어보세요**

선택된 allocator 유형에 따라 아래 파라미터를 **각각 명시적으로** 물어보세요:

→ **마르코위츠 (mean_variance)** 선택 시:
  - **위험 회피 계수 (risk_aversion/gamma)**: "변동성을 얼마나 억제하고 싶으세요? 공격적이면 0.5, 기본은 1.0, 보수적이면 3~5 정도입니다."
  - **공분산 추정 윈도우 (cov_lookback)**: "공분산 계산에 몇 일치 과거 수익률을 쓸까요? 20일(단기 적응), 60일(기본), 120일(장기 안정) 중?"
  - **공분산 모델 (cov_model)**: "shrinkage_cov(기본·추천), sample_cov(비정규화), diagonal_vol(변동성만) 중?"

→ **스코어가중 (score_weighted)** 선택 시:
  - **스코어 지수 (power)**: "상위 종목에 얼마나 집중시킬까요? 1.0(선형·기본), 2.0(제곱·집중), 0.5(루트·완화) 중?"

→ **역변동성 (inverse_vol)** 선택 시:
  - **변동성 윈도우 (vol_field)**: "vol_20d(20일) 또는 vol_60d(60일) 중 어느 변동성을 기준으로 할까요?"

→ **벤치마크 추적 (benchmark_tracking)** 선택 시:
  - **벤치마크 (benchmark_index)**: "KOSPI, KOSPI200, KOSDAQ, KRX300 중?"
  - **목표 추적오차 (te_target)**: "최대 허용 추적오차는? 5%, 10%, 제약 없음 중?"
  - **회전율 페널티 (turnover_penalty)**: "회전율 억제 강도는? 낮음(0.001·기본), 중간(0.01), 높음(0.1) 중?"

→ **향상된 인덱스 (enhanced_index)** 선택 시:
  - **알파 가중치 (alpha_weight)**: "알파 신호 vs 벤치마크 추적 중 어느 쪽에 더 비중? 보수(0.5), 기본(1.0), 공격적(2.0) 중?"
  - **추적오차 페널티 (te_penalty)**: "벤치마크에 얼마나 붙을까요? 약(0.5), 기본(1.0), 강(3.0) 중?"
  - **목표 추적오차 (te_target)**: "최대 TE 제약은? 5%, 10%, 없음 중?"

**[5단계] 리밸런싱**
- 주기: 주간 / 월간 / 분기별?
- 월간이라면 월 몇 번째 거래일에 리밸런싱? (기본: 1번째)

**[6단계] 섹터 제약**
- 섹터 비중 제한이 필요한가? "없음(기본) / 벤치마크 대비 최대 2배(max_sector_multiplier=2) / 절대 상한(max_sector_weight)"

**[7단계] 실행 파라미터**
- **초기 자금**: 기본 10억원 vs 다른 금액?
- **체결 기준 (fill_rule)**: "next_open(익일 시초가·현실적·권장) / same_close(당일 종가·낙관적) 중?"
- **거래비용**: 기본(수수료 10bps + 슬리피지 10bps)으로 할까요, 더 보수적으로 할까요?
- **모드**: research(연구용) vs contest(공모전 제약: 종목상한 15%, 섹터 2배 등)?

이 항목들이 모두 확정되기 전에는 status=ready로 전환하지 마세요.
사용자가 "대충 해줘" 또는 기본값을 원하면 기본값으로 제안하고 **반드시 확인받으세요**.
각 파라미터의 의미를 쉽게 설명하고 선택지를 제시하세요 — 전문 용어만 나열하지 마세요.

### status 전환 기준
- **clarifying**: 위 항목 중 하나라도 불명확하면 계속 질문. 한 번에 하나씩.
- **ready**: 아래 필수 정보가 **모두** 확정됐을 때만:
  - 백테스트 기간 (date_range) — 시작일이 2023-03-18 이전이면 데이터 한계 먼저 안내
  - 팩터/스코어링 방법 (node_graph의 구체적 field_id)
  - 보유 종목 수 또는 선택 기준 (selection.n)
  - 비중 배분 방식 (allocator.type) + 해당 유형의 핵심 파라미터 (예: risk_aversion, cov_lookback 등)
  - 리밸런싱 주기
  - 초기 자금, 체결 기준(fill_rule), 모드(research/contest)
  - 섹터 제약 여부
  draft_ir에 완성된 전략(위 파라미터 모두 포함)을 넣고, strategy_summary도 작성하세요.
  draft_ir의 allocator 블록에 사용자가 확인한 파라미터 값이 명시되어야 합니다.
- **confirmed**: status가 ready인 상태에서 사용자의 마지막 메시지가 백테스트 실행 동의를 나타낼 때.
  단, 사용자가 수정을 원하면 clarifying으로 돌아가세요.
  draft_ir은 반드시 포함해야 합니다.

strategy_id가 없으면 전략 내용에 맞게 자동 생성하세요 (영문 소문자 언더스코어).
"""


class StrategyChat:
    """
    Multi-turn conversation manager for NL strategy design.

    Usage
    -----
    chat = StrategyChat(api_key="sk-...", db_path="path/to/backtest.db")

    while True:
        user_input = input("> ")
        response = chat.send(user_input)
        print(response.message)

        if response.status == ChatStatus.CONFIRMED:
            report, narration = chat.run_and_narrate()
            print(narration)
            break
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        llm_client: Optional[Any] = None,
        model: str = "gpt-4o",
        db_path: Optional[str] = None,
        verbose: bool = False,
    ):
        """
        Parameters
        ----------
        api_key : str, optional
            OpenAI API key. Falls back to OPENAI_API_KEY env var.
        llm_client : openai.OpenAI, optional
            Pre-configured client (overrides api_key).
        model : str
            OpenAI model to use.
        db_path : str, optional
            Path to backtest.db. Used for dataset context + backtest execution.
        verbose : bool
            Print raw LLM JSON responses to stderr.
        """
        if llm_client is not None:
            self._client = llm_client
        else:
            resolved_key = api_key or os.environ.get("OPENAI_API_KEY")
            if not resolved_key:
                raise RuntimeError(
                    "OpenAI API 키가 없습니다.\n"
                    "  export OPENAI_API_KEY=sk-... 또는\n"
                    "  StrategyChat(api_key='sk-...')"
                )
            try:
                from openai import OpenAI
                self._client = OpenAI(api_key=resolved_key)
            except ImportError:
                raise ImportError("pip install openai 후 다시 시도하세요.")

        self._model = model
        self._db_path = db_path
        self._verbose = verbose
        self._history: List[Dict[str, str]] = []
        self._current_draft: Optional[Dict[str, Any]] = None
        self._last_status: ChatStatus = ChatStatus.CLARIFYING
        self._system_prompt = self._build_system_prompt(db_path)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def status(self) -> ChatStatus:
        return self._last_status

    @property
    def draft_ir(self) -> Optional[Dict[str, Any]]:
        return self._current_draft

    @property
    def history(self) -> List[Dict[str, str]]:
        return list(self._history)

    def send(self, user_message: str) -> ChatResponse:
        """
        Send a user message and get the AI response.

        Returns ChatResponse with status, message, and (when ready) draft_ir.
        """
        self._history.append({"role": "user", "content": user_message})
        response = self._call_llm()
        self._history.append({"role": "assistant", "content": json.dumps(response.model_dump(), ensure_ascii=False)})

        if response.draft_ir:
            self._current_draft = response.draft_ir
        self._last_status = response.status
        return response

    def run_and_narrate(
        self,
        out_dir: Optional[str] = None,
    ) -> tuple[Dict[str, Any], str]:
        """
        Run the backtest with the confirmed draft_ir and generate a Korean narration.

        Returns (report_bundle, narration_text).
        Raises RuntimeError if no confirmed draft_ir is available or it's incomplete.
        """
        if not self._current_draft:
            raise RuntimeError("확정된 전략이 없습니다. 먼저 대화로 전략을 완성해주세요.")

        # Basic completeness check before handing off to the engine
        missing = []
        draft = self._current_draft
        if not draft.get("date_range", {}).get("start") or not draft.get("date_range", {}).get("end"):
            missing.append("date_range (start/end)")
        sleeves = draft.get("sleeves", [])
        if not sleeves:
            missing.append("sleeves")
        else:
            for s in sleeves:
                alloc_type = s.get("allocator", {}).get("type", "")
                # benchmark_tracking and risk_budget don't require a node_graph
                node_graph_optional = alloc_type in ("benchmark_tracking", "risk_budget")
                ng = s.get("node_graph", {})
                if not node_graph_optional and (not ng.get("nodes") or not ng.get("output")):
                    missing.append(f"sleeves[{s.get('sleeve_id', '?')}].node_graph (nodes/output)")
        if missing:
            raise RuntimeError(
                "전략 IR이 불완전합니다. 누락된 필드: " + ", ".join(missing)
            )

        from backtest_engine.api.run_backtest import run_backtest

        # Auto-fill strategy_id if missing
        draft = dict(self._current_draft)
        if not draft.get("strategy_id"):
            draft["strategy_id"] = "chat_strategy"

        report = run_backtest(
            ir_dict=draft,
            db_path=self._db_path,
            verbose=self._verbose,
            save_to=out_dir,
        )

        narration = self._narrate(report, draft)
        return report, narration

    def reset(self) -> None:
        """Clear conversation history and draft IR."""
        self._history.clear()
        self._current_draft = None
        self._last_status = ChatStatus.CLARIFYING

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _call_llm(self) -> ChatResponse:
        """Call OpenAI Chat Completions with JSON response format."""
        messages = [{"role": "system", "content": self._system_prompt}] + self._history

        completion = self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            response_format={"type": "json_object"},
            temperature=0.3,
        )

        raw = completion.choices[0].message.content
        if self._verbose:
            import sys
            print(f"[LLM raw] {raw}", file=sys.stderr)

        try:
            data = json.loads(raw)
            return ChatResponse(**data)
        except Exception as e:
            # Graceful fallback: stay in clarifying
            return ChatResponse(
                status=ChatStatus.CLARIFYING,
                message=f"응답 파싱 중 오류가 발생했습니다: {e}\n다시 말씀해 주세요.",
            )

    def _narrate(self, report: Dict[str, Any], draft_ir: Dict[str, Any]) -> str:
        """Generate a Korean narration of the backtest results."""
        metrics = report.get("summary_metrics", {})
        hints = report.get("narration_hints", {})

        def _to_f(val, default: float = 0.0) -> float:
            """Parse metric that may be float or string like '-69.66%'."""
            if val is None:
                return default
            if isinstance(val, (int, float)):
                return float(val)
            if isinstance(val, str):
                clean = val.strip().replace(",", "")
                if clean.endswith("%"):
                    try:
                        return float(clean[:-1]) / 100
                    except ValueError:
                        pass
                try:
                    return float(clean)
                except ValueError:
                    pass
            return default

        def pct(key: str) -> str:
            return f"{_to_f(metrics.get(key)):.1%}"

        def num(key: str, fmt: str = ".2f") -> str:
            return format(_to_f(metrics.get(key)), fmt)

        dr = draft_ir.get("date_range") or {}
        prompt = f"""\
아래 백테스트 결과를 바탕으로 전략 리뷰를 한국어로 작성해주세요.
투자 전문가처럼 분석하되, 이해하기 쉽게 써주세요. 3~5 문단으로 작성하세요.

## 전략 개요
{draft_ir.get("strategy_summary") or draft_ir.get("strategy_id", "알 수 없음")}
기간: {dr.get("start")} ~ {dr.get("end")}

## 주요 성과 지표
- 총 수익률: {pct("total_return")}
- 연환산 수익률(CAGR): {pct("cagr")}
- 연환산 변동성: {pct("annualized_vol")}
- 샤프 비율: {num("sharpe")}
- 최대 낙폭(MDD): {pct("max_drawdown")}
- 정보 비율(IR): {num("information_ratio")}
- 추적오차(TE): {pct("tracking_error")}
- 평균 회전율: {pct("average_turnover")}

## 평가 힌트
{json.dumps(hints, ensure_ascii=False, indent=2)}
"""

        completion = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": "당신은 퀀트 투자 전략 분석 전문가입니다."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.5,
        )
        return completion.choices[0].message.content

    def _build_system_prompt(self, db_path: Optional[str]) -> str:
        """Build the system prompt, optionally injecting dataset context."""
        dataset_context = self._format_dataset_context(db_path)
        return _SYSTEM_PROMPT_TEMPLATE.format(dataset_context=dataset_context)

    def _format_dataset_context(self, db_path: Optional[str]) -> str:
        """Load and format dataset description for the system prompt."""
        if not db_path:
            return "DB 경로가 설정되지 않아 데이터셋 정보를 로드할 수 없습니다."
        try:
            from backtest_engine.api.describe_dataset import get_dataset_description
            desc = get_dataset_description(db_path)
            cov = desc.get("coverage", {})
            calendar = cov.get("calendar", {})
            prices = cov.get("prices", {})
            lines = [
                f"- 캘린더 기간: {calendar.get('start')} ~ {calendar.get('end')}",
                f"- 가격 데이터: {prices.get('start')} ~ {prices.get('end')}",
                f"- 유니버스 크기: 약 {cov.get('eligible_universe_size', '?')}종목 (is_eligible=1 기준)",
                f"- 인덱스 코드: {', '.join(cov.get('index_codes', []))}",
            ]
            sectors = cov.get("sectors", {})
            if sectors:
                top_sectors = list(sectors.keys())[:6]
                lines.append(f"- 주요 섹터: {', '.join(top_sectors)}")
            return "\n".join(lines)
        except Exception as e:
            return f"데이터셋 로드 실패: {e}"
