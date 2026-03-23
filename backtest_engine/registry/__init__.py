"""Registry package — maps field/feature names to DB columns and metadata."""

from backtest_engine.registry.field_registry import FIELD_REGISTRY, FieldMeta
from backtest_engine.registry.feature_registry import FEATURE_REGISTRY, FeatureMeta
from backtest_engine.registry.allocator_registry import ALLOCATOR_REGISTRY
from backtest_engine.registry.benchmark_registry import BENCHMARK_REGISTRY

__all__ = [
    "FIELD_REGISTRY", "FieldMeta",
    "FEATURE_REGISTRY", "FeatureMeta",
    "ALLOCATOR_REGISTRY",
    "BENCHMARK_REGISTRY",
]
