"""
RegistryResolver — validates and resolves field/benchmark/allocator references.

Used during compilation to ensure all referenced IDs exist in registries.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from backtest_engine.registry.field_registry import FIELD_REGISTRY
from backtest_engine.registry.feature_registry import FEATURE_REGISTRY
from backtest_engine.registry.benchmark_registry import BENCHMARK_REGISTRY, BENCHMARK_ALIASES
from backtest_engine.registry.allocator_registry import ALLOCATOR_REGISTRY


class RegistryResolver:
    """Resolves references against all registries."""

    def resolve_all(self, draft: Dict[str, Any]) -> Tuple[Dict, List[str]]:
        """
        Resolve all references in a draft dict.

        Returns (resolved_draft, warnings).
        """
        warnings: List[str] = []
        d = dict(draft)

        # Resolve benchmark
        bm = d.get("benchmark", {})
        if isinstance(bm, dict):
            idx = bm.get("index_code", "")
            resolved = BENCHMARK_ALIASES.get(idx.lower(), idx)
            if resolved not in BENCHMARK_REGISTRY:
                warnings.append(f"Unknown benchmark index: '{idx}' — using as-is")
            else:
                d["benchmark"]["index_code"] = resolved

        # Resolve field references in all node graphs
        for sleeve in d.get("sleeves", []):
            ng = sleeve.get("node_graph", {})
            nodes = ng.get("nodes", {})
            for nid, node in nodes.items():
                if node.get("type") == "field":
                    fid = node.get("field_id", "")
                    if fid not in FIELD_REGISTRY:
                        warnings.append(
                            f"sleeve:{sleeve.get('sleeve_id', '?')}.node:{nid} "
                            f"references unknown field '{fid}'"
                        )

        # Resolve allocator references
        for sleeve in d.get("sleeves", []):
            alloc = sleeve.get("allocator", {})
            if isinstance(alloc, dict):
                atype = alloc.get("type", "")
                if atype not in ALLOCATOR_REGISTRY:
                    warnings.append(f"Unknown allocator type: '{atype}'")

        return d, warnings
