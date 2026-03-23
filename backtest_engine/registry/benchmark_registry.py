"""Benchmark registry — maps index codes to DB metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class BenchmarkMeta:
    index_code: str             # canonical code stored in core_index_daily
    display_name: str
    market: str                 # KOSPI / KOSDAQ / KRX
    description: str
    approximate_constituents: int
    notes: Optional[str] = None


BENCHMARK_REGISTRY: dict[str, BenchmarkMeta] = {b.index_code: b for b in [
    BenchmarkMeta(
        "KOSPI", "코스피 종합지수", "KRX",
        "Korea Composite Stock Price Index — all KOSPI-listed stocks",
        800,
        notes="Broad market; not directly replicable from current DB"
    ),
    BenchmarkMeta(
        "KOSPI200", "코스피 200", "KOSPI",
        "Top 200 KOSPI stocks by market cap and liquidity",
        200,
        notes="Primary benchmark for most institutional strategies; proxy tracking supported"
    ),
    BenchmarkMeta(
        "KOSDAQ", "코스닥 종합지수", "KOSDAQ",
        "Korea Securities Dealers Automated Quotation — all KOSDAQ-listed stocks",
        1500,
        notes="Small/mid cap focus"
    ),
    BenchmarkMeta(
        "KRX300", "KRX 300", "KRX",
        "Top 300 KRX stocks — combined KOSPI + KOSDAQ",
        300,
        notes="Broader large-cap benchmark"
    ),
]}

# Alias map for common spellings
BENCHMARK_ALIASES: dict[str, str] = {
    "kospi": "KOSPI",
    "kospi200": "KOSPI200",
    "코스피": "KOSPI",
    "코스피200": "KOSPI200",
    "kosdaq": "KOSDAQ",
    "코스닥": "KOSDAQ",
    "krx300": "KRX300",
}


def resolve_benchmark(code: str) -> Optional[BenchmarkMeta]:
    """Resolve a benchmark code (case-insensitive) to BenchmarkMeta."""
    if code in BENCHMARK_REGISTRY:
        return BENCHMARK_REGISTRY[code]
    canonical = BENCHMARK_ALIASES.get(code.lower())
    if canonical:
        return BENCHMARK_REGISTRY.get(canonical)
    return None
