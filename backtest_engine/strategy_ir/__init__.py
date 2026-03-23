"""Strategy IR package — typed representation of a quant strategy."""

from backtest_engine.strategy_ir.models import StrategyIR
from backtest_engine.strategy_ir.validator import SemanticValidator

__all__ = ["StrategyIR", "SemanticValidator"]
