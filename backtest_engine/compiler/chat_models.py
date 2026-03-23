"""
Data models for StrategyChat conversation turns.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel


class ChatStatus(str, Enum):
    CLARIFYING = "clarifying"   # AI needs more information from user
    READY = "ready"             # Strategy is complete; waiting for user confirmation
    CONFIRMED = "confirmed"     # User confirmed → run backtest


class ChatResponse(BaseModel):
    status: ChatStatus
    message: str                              # Korean message shown to user
    draft_ir: Optional[Dict[str, Any]] = None # Current strategy IR draft (required when ready/confirmed)
    strategy_summary: Optional[str] = None    # Brief Korean description (filled when status=ready/confirmed)
