"""
DART Backtest DB - Main CLI

Usage:
  python -m src.main build      --config configs/config.example.yaml
  python -m src.main validate   --config configs/config.example.yaml
  python -m src.main inspect    --config configs/config.example.yaml
  python -m src.main dry-run    --config configs/config.example.yaml

Run from the db/ folder:
  cd C:\\Users\\cmsch\\Desktop\\DART-backtest-NL\\database\\db
  python -m src.main build --config configs/config.example.yaml
"""

import datetime
import logging
import os
import sys
import time
from pathlib import Path

import click

# Add db/ folder to Python path so `src.*` imports work
_DB_ROOT = Path(__file__).resolve().parent.parent
if str(_DB_ROOT) not in sys.path:
    sys.path.insert(0, str(_DB_ROOT))

from src.config import load_config, get_resolved
from src.db import get_connection, apply_schema, apply_views, get_row_count
from src.utils.paths import validate_mandatory_files, ensure_dir
from src.utils.hashing import file_stat
from src.transform.manifest import create_build_run_id, record_manifest, is_file_unchanged
from src.ingest import kind as kind_ingest
from src.ingest import dataguide as dg_ingest
from src.ingest import sectors as sector_ingest
from src.transform import security_master, calendar, prices, financials
from src.transform import regulatory, eligibility, features, metadata, manifest as mf
from src.validate.checks import run_all_checks
from src.validate.report import generate_report, print_summary

_DB_SQL_ROOT = _DB_ROOT / "sql"


def _setup_logging(log_level: str, artifacts_dir: Path) -> Path:
    """Configure logging to file and console. Returns log file path."""
    ensure_dir(artifacts_dir)
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    log_path = artifacts_dir / f"build_{ts}.log"

    level = getattr(logging, log_level.upper(), logging.INFO)

    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(log_path), encoding="utf-8"),
    ]
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )
    return log_path


@click.group()
def cli():
    """DART Backtest DB - Korean equity backtesting database builder."""
    pass


@cli.command("build")
@click.option("--config", "-c", required=True, help="Path to YAML config file")
@click.option("--skip-unchanged/--no-skip-unchanged", default=True,
              help="Skip re-ingestion of files whose checksums haven't changed")
def cmd_build(config: str, skip_unchanged: bool):
    """Build the full database from scratch (idempotent)."""
    cfg = load_config(config)
    resolved = cfg["_resolved"]
    artifacts_dir = resolved["artifacts_dir"]
    log_level = cfg.get("build", {}).get("log_level", "INFO")

    log_path = _setup_logging(log_level, artifacts_dir)
    logger = logging.getLogger("main.build")

    logger.info("=" * 60)
    logger.info("DART Backtest DB - Build starting")
    logger.info("Config: %s", resolved["config_path"])
    logger.info("DB:     %s", resolved["db_path"])
    logger.info("=" * 60)

    start_total = time.time()

    # ----------------------------------------------------------------
    # Step 1: Validate input files
    # ----------------------------------------------------------------
    logger.info("[1/10] Validating input files ...")
    input_files = resolved["input_files"]
    errors = validate_mandatory_files(input_files)
    if errors:
        for e in errors:
            logger.error(e)
        sys.exit(1)
    logger.info("  All %d mandatory input files found.", len(input_files))

    # ----------------------------------------------------------------
    # Step 2: Open DB and apply schema
    # ----------------------------------------------------------------
    logger.info("[2/10] Opening database and applying schema ...")
    ensure_dir(resolved["db_path"].parent)
    conn = get_connection(resolved["db_path"])
    apply_schema(conn, _DB_SQL_ROOT / "schema.sql")

    # ----------------------------------------------------------------
    # Step 3: Record manifest / checksums
    # ----------------------------------------------------------------
    logger.info("[3/10] Recording manifest and checksums ...")
    build_run_id = create_build_run_id()
    logger.info("  Build run ID: %s", build_run_id)

    checksums = record_manifest(conn, build_run_id, input_files)

    # Determine which files need re-ingestion
    batch_size = cfg.get("build", {}).get("insert_batch_size", 50000)
    _skip = cfg.get("build", {}).get("skip_if_unchanged", True) and skip_unchanged

    def needs_ingest(name: str) -> bool:
        if not _skip:
            return True
        cs = checksums.get(name)
        if cs is None:
            return True
        # Check if there's a PREVIOUS build with same checksum
        cur = conn.execute(
            "SELECT sha256 FROM raw_build_manifest "
            "WHERE source_name = ? AND build_run_id != ? "
            "ORDER BY ingested_at DESC LIMIT 1",
            (name, build_run_id)
        ).fetchone()
        if cur and cur["sha256"] == cs["sha256"]:
            logger.info("  Skipping %s (checksum unchanged)", name)
            return False
        return True

    # ----------------------------------------------------------------
    # Step 4: Ingest KIND files
    # ----------------------------------------------------------------
    logger.info("[4/10] Ingesting KIND files ...")
    files = input_files

    if needs_ingest("kind_listed_companies"):
        kind_ingest.ingest_listed_companies(conn, files["kind_listed_companies"])
    if needs_ingest("kind_delistings"):
        kind_ingest.ingest_delistings(conn, files["kind_delistings"])
    if needs_ingest("kind_ipos"):
        kind_ingest.ingest_ipos(conn, files["kind_ipos"])
    if needs_ingest("kind_stock_issuance"):
        kind_ingest.ingest_stock_issuance(conn, files["kind_stock_issuance"])
    if needs_ingest("kind_investment_caution"):
        kind_ingest.ingest_investment_caution(conn, files["kind_investment_caution"])
    if needs_ingest("kind_investment_warning"):
        kind_ingest.ingest_investment_warning(conn, files["kind_investment_warning"])
    if needs_ingest("kind_investment_risk"):
        kind_ingest.ingest_investment_risk(conn, files["kind_investment_risk"])

    # ----------------------------------------------------------------
    # Step 5: Ingest sector file
    # ----------------------------------------------------------------
    logger.info("[5/10] Ingesting sector file ...")
    if needs_ingest("sector_file"):
        allowed_sectors = cfg["sector"]["allowed_values"]
        sector_ingest.ingest_sectors(conn, files["sector_file"], allowed_sectors)

    # ----------------------------------------------------------------
    # Step 6: Ingest DataGuide file
    # ----------------------------------------------------------------
    logger.info("[6/10] Ingesting DataGuide file ...")
    dg_path = files["dataguide_file"]
    dg_cfg = cfg["dataguide"]

    dg_stock_rows = conn.execute("SELECT COUNT(*) FROM raw_dg_stock_daily").fetchone()[0]
    dg_needs_ingest = needs_ingest("dataguide_file") or dg_stock_rows == 0
    if not dg_needs_ingest:
        logger.info("  DataGuide file unchanged - skipping re-ingestion")

    if dg_needs_ingest:
        t0 = time.time()
        logger.info("  Ingesting index daily (bm sheet) ...")
        dg_ingest.ingest_index_daily(
            conn, dg_path,
            sheet_name=dg_cfg["sheet_index_daily"],
            index_code_map=dg_cfg["index_code_map"],
            item_map=dg_cfg["index_item_map"],
            batch_size=batch_size,
        )

        logger.info("  Ingesting stock daily (type1 sheet) - this is the longest step ...")
        dg_ingest.ingest_stock_daily(
            conn, dg_path,
            sheet_name=dg_cfg["sheet_stock_daily"],
            batch_size=batch_size,
        )

        logger.info("  Ingesting quarterly financials (type2 sheet) ...")
        dg_ingest.ingest_financials_quarterly(
            conn, dg_path,
            sheet_name=dg_cfg["sheet_financials_quarterly"],
            batch_size=batch_size,
        )
        logger.info("  DataGuide ingestion complete in %.1f min", (time.time() - t0) / 60)

    # ----------------------------------------------------------------
    # Step 7: Build core tables
    # ----------------------------------------------------------------
    logger.info("[7/10] Building core tables ...")

    security_master.build_security_master(conn)
    calendar.build_calendar(conn)
    prices.build_price_daily(conn)
    prices.build_index_daily(conn)

    lag_cfg = cfg.get("financial_lag", {})
    financials.build_financials_quarterly(
        conn,
        lag_q1=lag_cfg.get("q1_days", 45),
        lag_q2=lag_cfg.get("q2_days", 45),
        lag_q3=lag_cfg.get("q3_days", 45),
        lag_q4=lag_cfg.get("q4_days", 90),
    )

    regulatory.build_regulatory_status_intervals(conn)
    regulatory.build_sector_map(conn)

    # ----------------------------------------------------------------
    # Step 8: Build mart tables
    # ----------------------------------------------------------------
    logger.info("[8/10] Building mart tables ...")

    elig_cfg = cfg.get("eligibility", {})
    eligibility.build_liquidity_daily(
        conn,
        min_adv5=float(elig_cfg.get("min_adv5_krw", 3e9)),
        min_mcap=float(elig_cfg.get("min_mcap_krw", 1e11)),
    )

    eligibility.build_universe_eligibility(
        conn,
        min_adv5=float(elig_cfg.get("min_adv5_krw", 3e9)),
        min_mcap=float(elig_cfg.get("min_mcap_krw", 1e11)),
        min_listing_age_bd=int(elig_cfg.get("min_listing_age_bd", 6)),
        eligible_markets=set(elig_cfg.get("eligible_markets", ["코스피", "코스닥"])),
    )

    financials.build_fundamentals_asof_daily(conn)
    features.build_features(conn)
    eligibility.build_sector_weight_snapshot(conn)

    # ----------------------------------------------------------------
    # Step 9: Build meta tables and apply views
    # ----------------------------------------------------------------
    logger.info("[9/10] Building meta tables and views ...")
    metadata.build_field_catalog(conn)
    metadata.build_dataset_coverage(conn)
    apply_views(conn, _DB_SQL_ROOT / "views.sql")

    # ----------------------------------------------------------------
    # Step 10: Validate and save report
    # ----------------------------------------------------------------
    logger.info("[10/10] Running validation and saving report ...")
    results = run_all_checks(conn)
    print_summary(results)

    report_path = artifacts_dir / "validation_report.md"
    generate_report(conn, report_path)

    # Write manifest CSV
    _write_manifest_csv(conn, artifacts_dir / "manifest.csv", build_run_id)

    elapsed = time.time() - start_total
    logger.info("")
    logger.info("=" * 60)
    logger.info("BUILD COMPLETE in %.1f minutes", elapsed / 60)
    logger.info("DB:             %s", resolved["db_path"])
    logger.info("Validation:     %s", report_path)
    logger.info("Log:            %s", log_path)
    logger.info("Manifest:       %s", artifacts_dir / "manifest.csv")
    logger.info("=" * 60)

    conn.close()
    failed = sum(1 for r in results if r.status == "FAIL")
    if failed > 0:
        sys.exit(1)


@cli.command("validate")
@click.option("--config", "-c", required=True, help="Path to YAML config file")
def cmd_validate(config: str):
    """Run validation checks on an existing database."""
    cfg = load_config(config)
    resolved = cfg["_resolved"]
    artifacts_dir = resolved["artifacts_dir"]
    log_path = _setup_logging("INFO", artifacts_dir)

    db_path = resolved["db_path"]
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        print("Run 'build' first.")
        sys.exit(1)

    conn = get_connection(db_path)
    results = run_all_checks(conn)
    print_summary(results)

    report_path = artifacts_dir / "validation_report.md"
    generate_report(conn, report_path)
    print(f"Report written: {report_path}")
    conn.close()


@cli.command("inspect")
@click.option("--config", "-c", required=True, help="Path to YAML config file")
@click.option("--ticker", "-t", default=None, help="Inspect a specific ticker")
@click.option("--date", "-d", default=None, help="Inspect a specific date (YYYY-MM-DD)")
def cmd_inspect(config: str, ticker: str, date: str):
    """Show quick row counts and date coverage."""
    cfg = load_config(config)
    resolved = cfg["_resolved"]
    db_path = resolved["db_path"]

    if not db_path.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    conn = get_connection(db_path)

    print("\n=== Row Counts ===")
    tables = [
        "raw_kind_listed_companies_current", "raw_kind_delistings", "raw_kind_ipos",
        "raw_dg_index_daily", "raw_dg_stock_daily", "raw_dg_financials_quarterly",
        "raw_sector_map",
        "core_security_master", "core_calendar", "core_price_daily",
        "core_financials_quarterly", "core_regulatory_status_interval", "core_sector_map",
        "mart_liquidity_daily", "mart_universe_eligibility_daily",
        "mart_fundamentals_asof_daily", "mart_feature_daily",
        "meta_field_catalog",
    ]
    for t in tables:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t:<45} {n:>12,}")
        except Exception:
            print(f"  {t:<45} {'(error)':>12}")

    print("\n=== Date Coverage ===")
    for t, col in [
        ("core_price_daily", "trade_date"),
        ("core_calendar",    "trade_date"),
        ("core_financials_quarterly", "available_date"),
        ("mart_universe_eligibility_daily", "trade_date"),
    ]:
        try:
            r = conn.execute(f"SELECT MIN({col}), MAX({col}) FROM {t}").fetchone()
            print(f"  {t}.{col}: {r[0]} to {r[1]}")
        except Exception:
            print(f"  {t}.{col}: (error)")

    if ticker:
        print(f"\n=== Ticker: {ticker} ===")
        _inspect_ticker(conn, ticker, date)

    if date and not ticker:
        print(f"\n=== Date: {date} ===")
        _inspect_date(conn, date)

    conn.close()


def _inspect_ticker(conn, ticker: str, date: str | None):
    # Security master
    sm = conn.execute(
        "SELECT * FROM core_security_master WHERE ticker = ?", (ticker,)
    ).fetchone()
    if sm:
        print(f"  Corp name:     {sm['corp_name']}")
        print(f"  Market type:   {sm['market_type']}")
        print(f"  Listed:        {sm['listing_date']} to {sm['delisting_date'] or 'active'}")
        print(f"  Common equity: {sm['is_common_equity']}")
    else:
        print(f"  Not found in core_security_master")

    # Sector
    sec = conn.execute(
        "SELECT sector_name FROM core_sector_map WHERE ticker = ?", (ticker,)
    ).fetchone()
    print(f"  Sector:        {sec['sector_name'] if sec else 'N/A'}")

    if date:
        # Eligibility
        elig = conn.execute(
            "SELECT is_eligible, block_reason_json FROM mart_universe_eligibility_daily "
            "WHERE ticker = ? AND trade_date = ?", (ticker, date)
        ).fetchone()
        if elig:
            print(f"  Eligible on {date}: {'YES' if elig['is_eligible'] else 'NO'}")
            if elig['block_reason_json']:
                print(f"  Blocks: {elig['block_reason_json']}")
        else:
            print(f"  No eligibility data for {date}")


def _inspect_date(conn, date: str):
    eligible = conn.execute(
        "SELECT COUNT(*) FROM mart_universe_eligibility_daily "
        "WHERE trade_date = ? AND is_eligible = 1", (date,)
    ).fetchone()[0]
    total = conn.execute(
        "SELECT COUNT(*) FROM mart_universe_eligibility_daily "
        "WHERE trade_date = ?", (date,)
    ).fetchone()[0]
    print(f"  Eligible tickers: {eligible} / {total}")

    # Sector breakdown
    sectors = conn.execute(
        """
        SELECT sec.sector_name, COUNT(*) as n
        FROM mart_universe_eligibility_daily e
        JOIN core_sector_map sec ON e.ticker = sec.ticker
        WHERE e.trade_date = ? AND e.is_eligible = 1
        GROUP BY sec.sector_name ORDER BY n DESC
        """,
        (date,)
    ).fetchall()
    for r in sectors:
        print(f"  {r['sector_name']:15}: {r['n']} stocks")


@cli.command("dry-run")
@click.option("--config", "-c", required=True, help="Path to YAML config file")
def cmd_dry_run(config: str):
    """Validate config and file paths without building the database."""
    cfg = load_config(config)
    resolved = cfg["_resolved"]

    print("\n=== DRY RUN - Path Validation ===")
    print(f"Config:       {resolved['config_path']}")
    print(f"Project root: {resolved['project_root']}")
    print(f"Raw root:     {resolved['raw_root']}")
    print(f"DB path:      {resolved['db_path']}")
    print(f"Artifacts:    {resolved['artifacts_dir']}")
    print()

    input_files = resolved["input_files"]
    print("=== Input Files ===")
    all_ok = True
    for name, path in input_files.items():
        exists = path.exists()
        size = f"{path.stat().st_size / 1024 / 1024:.1f} MB" if exists else "MISSING"
        status = "[OK]" if exists else "[MISSING]"
        print(f"  {status:<10} {name:<30} {size:<12} {path}")
        if not exists:
            all_ok = False

    print()
    print("=== Planned ETL Steps ===")
    steps = [
        "1.  Validate all input files",
        "2.  Open/create SQLite DB + apply schema",
        "3.  Record manifest / SHA-256 checksums",
        "4.  Ingest KIND files (7 HTML-as-XLS files)",
        "5.  Ingest sector file (xlsx)",
        "6a. Ingest DataGuide index daily (bm sheet)",
        "6b. Ingest DataGuide stock daily (type1 sheet) [SLOWEST STEP ~10-30 min]",
        "6c. Ingest DataGuide financials (type2 sheet)",
        "7a. Build core_security_master (KIND IPO + delistings + current list)",
        "7b. Build core_calendar (from trade dates in price data)",
        "7c. Build core_price_daily (pivot from raw_dg_stock_daily)",
        "7d. Build core_index_daily",
        "7e. Build core_financials_quarterly (with PIT lag)",
        "7f. Build core_regulatory_status_interval (caution/warning/risk/halt/admin)",
        "7g. Build core_sector_map",
        "8a. Build mart_liquidity_daily (ADV5, ADV20, listing age)",
        "8b. Build mart_universe_eligibility_daily [COMPUTE-INTENSIVE]",
        "8c. Build mart_fundamentals_asof_daily (PIT-safe as-of join)",
        "8d. Build mart_feature_daily (price + fundamental features)",
        "8e. Build mart_sector_weight_snapshot",
        "9.  Build meta_field_catalog and meta_dataset_coverage",
        "10. Apply SQL views",
        "11. Run validation checks",
        "12. Write validation report + manifest CSV",
    ]
    for step in steps:
        print(f"  {step}")

    print()
    if all_ok:
        print("[OK] All input files found. Ready to build.")
    else:
        errors = validate_mandatory_files(input_files)
        for e in errors:
            print(f"[ERROR] {e}")
        print("\nFix missing files before running build.")
        sys.exit(1)


def _write_manifest_csv(conn, path: Path, build_run_id: str):
    """Write manifest as CSV for easy inspection."""
    rows = conn.execute(
        "SELECT source_name, absolute_path, file_size_bytes, modified_time, sha256 "
        "FROM raw_build_manifest WHERE build_run_id = ?",
        (build_run_id,)
    ).fetchall()

    lines = ["source_name,absolute_path,file_size_bytes,modified_time,sha256"]
    for r in rows:
        lines.append(
            f"{r['source_name']},{r['absolute_path']},"
            f"{r['file_size_bytes']},{r['modified_time']},{r['sha256']}"
        )
    path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    cli()
