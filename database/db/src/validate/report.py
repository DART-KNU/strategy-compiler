"""
Generate validation report and sample queries.
"""

import datetime
import logging
import sqlite3
from pathlib import Path

from src.validate.checks import run_all_checks, CheckResult

logger = logging.getLogger(__name__)


def generate_report(conn: sqlite3.Connection, output_path: Path) -> str:
    """
    Run all checks and write a markdown validation report.
    Returns the path to the written report.
    """
    results = run_all_checks(conn)
    lines = _format_report(conn, results)
    text = "\n".join(lines)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    logger.info("Validation report written to %s", output_path)
    return text


def _format_report(conn: sqlite3.Connection, results: list[CheckResult]) -> list[str]:
    lines = []
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    lines.append("# DART Backtest DB - Validation Report")
    lines.append(f"\nGenerated: {now}")
    lines.append("")

    # Summary counts
    passed = sum(1 for r in results if r.status == "PASS")
    warned = sum(1 for r in results if r.status == "WARN")
    failed = sum(1 for r in results if r.status == "FAIL")

    lines.append(f"**Summary:** {passed} PASS | {warned} WARN | {failed} FAIL")
    lines.append("")

    # ----------------------------------------------------------------
    # Check results table
    # ----------------------------------------------------------------
    lines.append("## Check Results")
    lines.append("")
    lines.append("| Status | Check | Value | Message |")
    lines.append("|--------|-------|-------|---------|")
    for r in results:
        icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(r.status, "?")
        lines.append(f"| {icon} {r.status} | `{r.name}` | {r.value} | {r.message} |")

    lines.append("")

    # ----------------------------------------------------------------
    # Row counts by table
    # ----------------------------------------------------------------
    lines.append("## Row Counts by Table")
    lines.append("")
    lines.append("| Table | Row Count |")
    lines.append("|-------|-----------|")

    tables = [
        "raw_kind_listed_companies_current", "raw_kind_delistings", "raw_kind_ipos",
        "raw_kind_stock_issuance", "raw_kind_investment_caution",
        "raw_kind_investment_warning", "raw_kind_investment_risk",
        "raw_dg_index_daily", "raw_dg_stock_daily", "raw_dg_financials_quarterly",
        "raw_sector_map", "raw_build_manifest",
        "core_security_master", "core_calendar", "core_price_daily",
        "core_index_daily", "core_financials_quarterly",
        "core_regulatory_status_interval", "core_sector_map",
        "mart_liquidity_daily", "mart_universe_eligibility_daily",
        "mart_fundamentals_asof_daily", "mart_feature_daily",
        "mart_sector_weight_snapshot",
        "meta_field_catalog", "meta_dataset_coverage",
    ]
    for t in tables:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            lines.append(f"| `{t}` | {n:,} |")
        except Exception:
            lines.append(f"| `{t}` | _(error)_ |")

    lines.append("")

    # ----------------------------------------------------------------
    # Date coverage
    # ----------------------------------------------------------------
    lines.append("## Date Coverage")
    lines.append("")
    lines.append("| Table | Column | Min Date | Max Date |")
    lines.append("|-------|--------|----------|----------|")

    date_tables = [
        ("core_price_daily",                "trade_date"),
        ("core_index_daily",               "trade_date"),
        ("core_calendar",                  "trade_date"),
        ("core_financials_quarterly",      "period_end"),
        ("core_financials_quarterly",      "available_date"),
        ("mart_universe_eligibility_daily","trade_date"),
        ("mart_feature_daily",             "trade_date"),
    ]
    for t, col in date_tables:
        try:
            row = conn.execute(f"SELECT MIN({col}), MAX({col}) FROM {t}").fetchone()
            lines.append(f"| `{t}` | `{col}` | {row[0] or 'N/A'} | {row[1] or 'N/A'} |")
        except Exception:
            lines.append(f"| `{t}` | `{col}` | _(error)_ | _(error)_ |")

    lines.append("")

    # ----------------------------------------------------------------
    # Eligible universe size by recent sample dates
    # ----------------------------------------------------------------
    lines.append("## Eligible Universe Size (Sample Dates)")
    lines.append("")
    lines.append("| Date | Eligible | Total | Pct |")
    lines.append("|------|----------|-------|-----|")

    try:
        sample_dates = conn.execute(
            """
            SELECT DISTINCT trade_date FROM mart_universe_eligibility_daily
            WHERE trade_date >= '2024-01-01'
            ORDER BY trade_date DESC LIMIT 10
            """
        ).fetchall()

        for row in sample_dates:
            d = row[0]
            eligible = conn.execute(
                "SELECT COUNT(*) FROM mart_universe_eligibility_daily "
                "WHERE trade_date = ? AND is_eligible = 1", (d,)
            ).fetchone()[0]
            total = conn.execute(
                "SELECT COUNT(*) FROM mart_universe_eligibility_daily "
                "WHERE trade_date = ?", (d,)
            ).fetchone()[0]
            pct = f"{eligible/total*100:.1f}%" if total > 0 else "N/A"
            lines.append(f"| {d} | {eligible:,} | {total:,} | {pct} |")
    except Exception as e:
        lines.append(f"| _(error: {e})_ | | | |")

    lines.append("")

    # ----------------------------------------------------------------
    # Sample queries
    # ----------------------------------------------------------------
    lines.append("## Sample Queries")
    lines.append("")
    lines.append("### Was ticker X eligible on date T?")
    lines.append("```sql")
    lines.append("SELECT is_eligible, block_reason_json")
    lines.append("FROM mart_universe_eligibility_daily")
    lines.append("WHERE ticker = '005930' AND trade_date = '2024-01-15';")
    lines.append("```")
    lines.append("")
    lines.append("### Why was ticker X blocked on date T?")
    lines.append("```sql")
    lines.append("SELECT")
    lines.append("    ticker, trade_date,")
    lines.append("    is_listed, is_common_equity, is_market_ok,")
    lines.append("    is_listing_age_ok, is_liquidity_ok, is_mcap_ok,")
    lines.append("    is_not_caution, is_not_warning, is_not_risk,")
    lines.append("    is_not_admin, is_not_halt,")
    lines.append("    block_reason_json")
    lines.append("FROM mart_universe_eligibility_daily")
    lines.append("WHERE ticker = '005930' AND trade_date = '2024-01-15';")
    lines.append("```")
    lines.append("")
    lines.append("### Show eligible universe on date T")
    lines.append("```sql")
    lines.append("SELECT")
    lines.append("    e.ticker,")
    lines.append("    s.corp_name,")
    lines.append("    s.market_type,")
    lines.append("    sec.sector_name,")
    lines.append("    p.market_cap / 1e8 AS mcap_100m_krw,")
    lines.append("    l.adv5 / 1e9 AS adv5_bn_krw")
    lines.append("FROM mart_universe_eligibility_daily e")
    lines.append("JOIN core_security_master s ON e.ticker = s.ticker")
    lines.append("JOIN core_price_daily p ON e.trade_date = p.trade_date AND e.ticker = p.ticker")
    lines.append("JOIN mart_liquidity_daily l ON e.trade_date = l.trade_date AND e.ticker = l.ticker")
    lines.append("LEFT JOIN core_sector_map sec ON e.ticker = sec.ticker")
    lines.append("WHERE e.trade_date = '2024-01-15' AND e.is_eligible = 1")
    lines.append("ORDER BY p.market_cap DESC;")
    lines.append("```")
    lines.append("")
    lines.append("### PIT-safe financials for ticker X as of date T")
    lines.append("```sql")
    lines.append("SELECT *")
    lines.append("FROM mart_fundamentals_asof_daily")
    lines.append("WHERE ticker = '005930' AND trade_date = '2024-01-15';")
    lines.append("```")
    lines.append("")
    lines.append("### Sector weights on date T")
    lines.append("```sql")
    lines.append("SELECT sector_name, constituent_count, sector_weight, is_approximated")
    lines.append("FROM mart_sector_weight_snapshot")
    lines.append("WHERE trade_date = '2024-01-15'")
    lines.append("ORDER BY sector_weight DESC;")
    lines.append("```")
    lines.append("")

    # ----------------------------------------------------------------
    # Live query results for sample date
    # ----------------------------------------------------------------
    lines.append("## Live Sample Query Results")
    lines.append("")

    try:
        sample_date = conn.execute(
            "SELECT trade_date FROM mart_universe_eligibility_daily "
            "WHERE trade_date >= '2024-01-01' ORDER BY trade_date DESC LIMIT 1"
        ).fetchone()

        if sample_date:
            d = sample_date[0]
            lines.append(f"### Eligible universe on {d} (top 10 by market cap)")
            lines.append("")
            lines.append("| Ticker | Corp Name | Market | Sector | McapBnKRW | ADV5BnKRW |")
            lines.append("|--------|-----------|--------|--------|-----------|-----------|")

            top10 = conn.execute(
                """
                SELECT e.ticker, s.corp_name, s.market_type, sec.sector_name,
                       ROUND(p.market_cap / 1e11, 2) AS mcap_100bn,
                       ROUND(l.adv5 / 1e9, 1) AS adv5_bn
                FROM mart_universe_eligibility_daily e
                JOIN core_security_master s ON e.ticker = s.ticker
                JOIN core_price_daily p ON e.trade_date = p.trade_date AND e.ticker = p.ticker
                JOIN mart_liquidity_daily l ON e.trade_date = l.trade_date AND e.ticker = l.ticker
                LEFT JOIN core_sector_map sec ON e.ticker = sec.ticker
                WHERE e.trade_date = ? AND e.is_eligible = 1
                ORDER BY p.market_cap DESC
                LIMIT 10
                """,
                (d,)
            ).fetchall()

            for r in top10:
                lines.append(
                    f"| {r['ticker']} | {r['corp_name'] or ''} | "
                    f"{r['market_type'] or ''} | {r['sector_name'] or 'N/A'} | "
                    f"{r['mcap_100bn'] or 'N/A'} | {r['adv5_bn'] or 'N/A'} |"
                )
    except Exception as e:
        lines.append(f"_(error running live query: {e})_")

    lines.append("")
    lines.append("---")
    lines.append("*Report generated by DART Backtest DB validation pipeline*")

    return lines


def print_summary(results: list[CheckResult]) -> None:
    """Print a brief summary to stdout."""
    passed = sum(1 for r in results if r.status == "PASS")
    warned = sum(1 for r in results if r.status == "WARN")
    failed = sum(1 for r in results if r.status == "FAIL")

    print(f"\n{'='*60}")
    print(f"Validation: {passed} PASS | {warned} WARN | {failed} FAIL")
    print(f"{'='*60}")

    for r in results:
        if r.status != "PASS":
            icon = "[WARN]" if r.status == "WARN" else "[FAIL]"
            print(f"  {icon} {r.name}: {r.message}")

    if failed == 0 and warned == 0:
        print("  [ALL PASS] All checks passed!")
    print()
