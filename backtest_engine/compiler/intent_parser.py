"""
IntentParser — NL -> Draft Strategy dict conversion via OpenAI Responses API.

API key configuration (in priority order):
  1. Pass api_key= directly to IntentParser()
  2. Set OPENAI_API_KEY environment variable
  3. Create your own OpenAI client and pass llm_client=

For local dev, create a .env file with:
  OPENAI_API_KEY=sk-...
Then load it before running:
  python -c "import dotenv; dotenv.load_dotenv()"
Or just export the env var in your shell.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional


class IntentParser:
    """
    Parses a natural language strategy description into a draft Strategy IR dict.

    Usage
    -----
    # Option 1: auto-read from OPENAI_API_KEY env var
    parser = IntentParser()

    # Option 2: pass key directly
    parser = IntentParser(api_key="sk-...")

    # Option 3: bring your own client
    from openai import OpenAI
    parser = IntentParser(llm_client=OpenAI(api_key="sk-..."))

    draft = parser.parse("모멘텀 전략, 상위 30종목, 월별 리밸런싱")
    """

    def __init__(
        self,
        llm_client: Optional[Any] = None,
        api_key: Optional[str] = None,
        model: str = "gpt-4o",
        tools: Optional[list] = None,
    ):
        """
        Parameters
        ----------
        llm_client : openai.OpenAI, optional
            Pre-configured OpenAI client. If provided, api_key is ignored.
        api_key : str, optional
            OpenAI API key. If not provided, reads from OPENAI_API_KEY env var.
        model : str
            Model to use. Default: gpt-4o (change to gpt-4.1 etc. as needed).
        tools : list, optional
            OpenAI tool definitions (describe_dataset, resolve_field).
        """
        if llm_client is not None:
            self._client = llm_client
        else:
            resolved_key = api_key or os.environ.get("OPENAI_API_KEY")
            if resolved_key:
                try:
                    from openai import OpenAI
                    self._client = OpenAI(api_key=resolved_key)
                except ImportError:
                    raise ImportError(
                        "openai 패키지가 설치되지 않았습니다. 설치 후 다시 시도하세요:\n"
                        "  pip install openai"
                    )
            else:
                self._client = None  # passthrough / dict-only mode

        self._model = model
        self._tools = tools or []
        self._conversation_id: Optional[str] = None

    def parse(
        self,
        user_input: str | Dict,
        conversation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Parse natural language input into a draft Strategy IR dict.

        Parameters
        ----------
        user_input : str or dict
            If str: natural language description (requires LLM backend).
            If dict: treated as a pre-parsed draft and returned as-is.
        conversation_id : str, optional
            For multi-turn conversation continuity (previous_response_id).

        Returns
        -------
        dict : Draft strategy IR (not yet validated).
        """
        if isinstance(user_input, dict):
            return user_input

        if self._client is None:
            raise RuntimeError(
                "OpenAI 클라이언트가 설정되지 않았습니다.\n"
                "다음 중 하나를 선택하세요:\n"
                "  1. 환경변수 설정: export OPENAI_API_KEY=sk-...\n"
                "  2. 직접 전달: IntentParser(api_key='sk-...')\n"
                "  3. 딕셔너리로 직접 전달: parser.parse({'strategy_id': ...})"
            )

        return self._call_llm(user_input, conversation_id)

    def _call_llm(self, text: str, conversation_id: Optional[str] = None) -> Dict[str, Any]:
        """Call OpenAI Responses API with structured output (StrategyIR JSON Schema)."""
        from backtest_engine.strategy_ir.models import StrategyIR

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a quantitative strategy compiler for Korean equities. "
                    "Convert the user's natural language strategy description into a "
                    "StrategyIR JSON object. Use the describe_dataset tool to check "
                    "available fields and date ranges before generating the IR."
                ),
            },
            {"role": "user", "content": text},
        ]

        kwargs: Dict[str, Any] = {
            "model": self._model,
            "input": messages,
            "text": {
                "format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "StrategyIR",
                        "schema": StrategyIR.model_json_schema(),
                        "strict": True,
                    },
                }
            },
        }
        if self._tools:
            kwargs["tools"] = self._tools
        if conversation_id:
            kwargs["previous_response_id"] = conversation_id

        response = self._client.responses.create(**kwargs)

        # Tool call loop
        while getattr(response, "stop_reason", None) == "tool_use":
            tool_results = []
            for item in response.output:
                if getattr(item, "type", None) == "tool_use":
                    result = self._execute_tool(item.name, item.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": item.id,
                        "content": json.dumps(result, ensure_ascii=False),
                    })
            kwargs["input"] = tool_results
            kwargs["previous_response_id"] = response.id
            response = self._client.responses.create(**kwargs)

        return json.loads(response.output_text)

    def _execute_tool(self, name: str, args: Dict) -> Any:
        """Dispatch tool calls from the LLM."""
        if name == "describe_dataset":
            from backtest_engine.api.describe_dataset import get_dataset_description
            return get_dataset_description()
        if name == "resolve_field":
            from backtest_engine.registry.field_registry import resolve_field
            meta = resolve_field(args.get("query", ""))
            if meta:
                return {"field_id": meta.field_id, "description": meta.description}
            return {"error": f"Field not found: {args.get('query')}"}
        return {"error": f"Unknown tool: {name}"}

    def clarify(self, missing_slots: list[str]) -> str:
        """Generate a clarification question for missing required slots."""
        slot_questions = {
            "date_range": "What time period should the backtest cover? (e.g., 2022-01-01 to 2025-12-31)",
            "sleeves[0].selection.n": "How many stocks should be held at a time?",
            "sleeves[0].allocator": "How should weights be allocated? (equal weight, score-based, inverse vol, etc.)",
            "benchmark.index_code": "What benchmark should performance be measured against? (KOSPI200, KOSPI, etc.)",
        }
        questions = []
        for slot in missing_slots:
            q = slot_questions.get(slot, f"Please specify: {slot}")
            questions.append(f"- {q}")
        return "To complete the strategy, please clarify:\n" + "\n".join(questions)

    def clarify(self, missing_slots: list[str]) -> str:
        """
        Generate a clarification question for missing required slots.

        Returns a string prompt to show to the user.
        """
        slot_questions = {
            "date_range": "What time period should the backtest cover? (e.g., 2022-01-01 to 2025-12-31)",
            "sleeves[0].selection.n": "How many stocks should be held at a time?",
            "sleeves[0].allocator": "How should weights be allocated? (equal weight, score-based, inverse vol, etc.)",
            "benchmark.index_code": "What benchmark should performance be measured against? (KOSPI200, KOSPI, etc.)",
        }
        questions = []
        for slot in missing_slots:
            q = slot_questions.get(slot, f"Please specify: {slot}")
            questions.append(f"- {q}")
        return "To complete the strategy, please clarify:\n" + "\n".join(questions)


class SlotPlanner:
    """
    Identifies missing required information in a draft strategy.

    Returns a list of missing slot names that should be clarified.
    """

    REQUIRED_SLOTS = [
        ("date_range.start", lambda d: "date_range" in d and "start" in d.get("date_range", {})),
        ("date_range.end", lambda d: "date_range" in d and "end" in d.get("date_range", {})),
        ("strategy_id", lambda d: "strategy_id" in d),
    ]

    def find_missing(self, draft: Dict[str, Any]) -> list[str]:
        """Return list of missing required slot paths."""
        missing = []
        for slot, check_fn in self.REQUIRED_SLOTS:
            if not check_fn(draft):
                missing.append(slot)
        return missing
