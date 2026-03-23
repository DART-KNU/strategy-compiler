"""
KIND (Korea Investor's Network for Disclosure) file ingest.

All KIND files are HTML-disguised-as-XLS (KRX portal serves HTML with .xls extension).
Use pd.read_html with euc-kr encoding.

Tables populated:
  raw_kind_listed_companies_current
  raw_kind_delistings
  raw_kind_ipos
  raw_kind_stock_issuance
  raw_kind_investment_caution
  raw_kind_investment_warning
  raw_kind_investment_risk
"""

import datetime
import logging
import sqlite3
from pathlib import Path

import pandas as pd

from src.db import insert_batch, truncate_table
from src.utils.io import read_html_xls, clean_date_str

logger = logging.getLogger(__name__)

_NOW = lambda: datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Listed companies (current snapshot)
# ---------------------------------------------------------------------------
# Source columns: 회사명 시장구분 종목코드 업종 주요제품 상장일 결산월 대표자명 홈페이지 지역

def ingest_listed_companies(conn: sqlite3.Connection, path: Path) -> int:
    """Ingest KIND 상장법인목록.xls into raw_kind_listed_companies_current."""
    df = read_html_xls(path)
    ingested_at = _NOW()

    # Rename columns to stable English names
    col_map = {
        "회사명":   "corp_name",
        "시장구분": "market_type",
        "종목코드": "raw_ticker",
        "업종":     "industry",
        "주요제품": "main_products",
        "상장일":   "listing_date",
        "결산월":   "fiscal_month",
        "대표자명": "representative",
        "홈페이지": "website",
        "지역":     "region",
    }
    df = df.rename(columns=col_map)

    # Ensure raw_ticker is string
    df["raw_ticker"] = df["raw_ticker"].astype(str).str.strip()

    # Clean listing_date
    df["listing_date"] = df["listing_date"].apply(clean_date_str)

    truncate_table(conn, "raw_kind_listed_companies_current")

    cols = ["raw_ticker", "corp_name", "market_type", "industry", "main_products",
            "listing_date", "fiscal_month", "representative", "website", "region",
            "ingested_at"]

    rows = []
    for _, row in df.iterrows():
        rows.append((
            row.get("raw_ticker"),
            row.get("corp_name"),
            row.get("market_type"),
            row.get("industry"),
            row.get("main_products"),
            row.get("listing_date"),
            str(row.get("fiscal_month", "") or "").strip(),
            row.get("representative"),
            row.get("website"),
            row.get("region"),
            ingested_at,
        ))

    n = insert_batch(conn, "raw_kind_listed_companies_current", rows, cols)
    logger.info("Ingested %d rows -> raw_kind_listed_companies_current", n)
    return n


# ---------------------------------------------------------------------------
# Delisting history
# ---------------------------------------------------------------------------
# Source columns: 번호 회사명 종목코드 폐지일자 폐지사유 비고

def ingest_delistings(conn: sqlite3.Connection, path: Path) -> int:
    """Ingest KIND 상장폐지현황.xls into raw_kind_delistings."""
    df = read_html_xls(path)
    ingested_at = _NOW()

    col_map = {
        "번호":     "seq",
        "회사명":   "corp_name",
        "종목코드": "raw_ticker",
        "폐지일자": "delisting_date",
        "폐지사유": "delisting_reason",
        "비고":     "notes",
    }
    df = df.rename(columns=col_map)
    df["raw_ticker"] = df["raw_ticker"].astype(str).str.strip()
    df["delisting_date"] = df["delisting_date"].apply(clean_date_str)

    # Drop rows with null delisting_date or ticker
    df = df.dropna(subset=["raw_ticker", "delisting_date"])

    truncate_table(conn, "raw_kind_delistings")

    cols = ["seq", "raw_ticker", "corp_name", "delisting_date", "delisting_reason", "notes", "ingested_at"]
    rows = []
    for _, row in df.iterrows():
        rows.append((
            _safe_int(row.get("seq")),
            row["raw_ticker"],
            row.get("corp_name"),
            row["delisting_date"],
            row.get("delisting_reason"),
            _safe_str(row.get("notes")),
            ingested_at,
        ))

    n = insert_batch(conn, "raw_kind_delistings", rows, cols)
    logger.info("Ingested %d rows -> raw_kind_delistings", n)
    return n


# ---------------------------------------------------------------------------
# IPO / New listing history
# ---------------------------------------------------------------------------
# Source columns: 회사명 종목코드 상장일 상장유형 증권구분 업종 국적 상장주선인/지정자문인

def ingest_ipos(conn: sqlite3.Connection, path: Path) -> int:
    """Ingest KIND 신규상장기업현황.xls into raw_kind_ipos."""
    df = read_html_xls(path)
    ingested_at = _NOW()

    col_map = {
        "회사명":               "corp_name",
        "종목코드":             "raw_ticker",
        "상장일":               "listing_date",
        "상장유형":             "listing_type",
        "증권구분":             "security_type",
        "업종":                 "industry",
        "국적":                 "nationality",
        "상장주선인/ 지정자문인": "underwriter",
    }
    # Handle slight variation in column name
    df = df.rename(columns={c: v for c, v in col_map.items() if c in df.columns})

    df["raw_ticker"] = df["raw_ticker"].astype(str).str.strip()
    df["listing_date"] = df["listing_date"].apply(clean_date_str)
    df = df.dropna(subset=["raw_ticker", "listing_date"])

    truncate_table(conn, "raw_kind_ipos")

    cols = ["raw_ticker", "corp_name", "listing_date", "listing_type",
            "security_type", "industry", "nationality", "underwriter", "ingested_at"]
    rows = []
    for _, row in df.iterrows():
        rows.append((
            row["raw_ticker"],
            row.get("corp_name"),
            row["listing_date"],
            row.get("listing_type"),
            row.get("security_type"),
            row.get("industry"),
            row.get("nationality"),
            row.get("underwriter"),
            ingested_at,
        ))

    n = insert_batch(conn, "raw_kind_ipos", rows, cols)
    logger.info("Ingested %d rows -> raw_kind_ipos", n)
    return n


# ---------------------------------------------------------------------------
# Stock issuance history
# ---------------------------------------------------------------------------
# Source columns: 회사명 종목코드 상장(예정)일 상장방식 발행주식수 액면가 발행사유

def ingest_stock_issuance(conn: sqlite3.Connection, path: Path) -> int:
    """Ingest KIND 주식발행내역.xls into raw_kind_stock_issuance."""
    df = read_html_xls(path)
    ingested_at = _NOW()

    col_map = {
        "회사명":       "corp_name",
        "종목코드":     "raw_ticker",
        "상장(예정)일": "listing_date",
        "상장방식":     "issuance_type",
        "발행주식수":   "shares_issued",
        "액면가":       "par_value",
        "발행사유":     "issuance_reason",
    }
    df = df.rename(columns={c: v for c, v in col_map.items() if c in df.columns})
    df["raw_ticker"] = df["raw_ticker"].astype(str).str.strip()
    df["listing_date"] = df["listing_date"].apply(clean_date_str)

    truncate_table(conn, "raw_kind_stock_issuance")

    cols = ["raw_ticker", "corp_name", "listing_date", "issuance_type",
            "shares_issued", "par_value", "issuance_reason", "ingested_at"]
    rows = []
    for _, row in df.iterrows():
        rows.append((
            row.get("raw_ticker"),
            row.get("corp_name"),
            row.get("listing_date"),
            row.get("issuance_type"),
            _safe_float(row.get("shares_issued")),
            _safe_float(row.get("par_value")),
            row.get("issuance_reason"),
            ingested_at,
        ))

    n = insert_batch(conn, "raw_kind_stock_issuance", rows, cols)
    logger.info("Ingested %d rows -> raw_kind_stock_issuance", n)
    return n


# ---------------------------------------------------------------------------
# Investment caution (투자주의종목)
# ---------------------------------------------------------------------------
# Source columns: 번호 종목명 종목코드 유형 공시일 지정일
# NOTE: No removal date in source. Designation is treated as a single-day event.

def ingest_investment_caution(conn: sqlite3.Connection, path: Path) -> int:
    """Ingest KIND 투자주의종목.xls into raw_kind_investment_caution."""
    df = read_html_xls(path)
    ingested_at = _NOW()

    col_map = {
        "번호":     "seq",
        "종목명":   "stock_name",
        "종목코드": "raw_ticker",
        "유형":     "caution_type",
        "공시일":   "announcement_date",
        "지정일":   "designation_date",
    }
    df = df.rename(columns={c: v for c, v in col_map.items() if c in df.columns})
    df["raw_ticker"] = df["raw_ticker"].astype(str).str.strip()
    df["designation_date"] = df["designation_date"].apply(clean_date_str)
    df["announcement_date"] = df.get("announcement_date", pd.Series()).apply(clean_date_str)
    df = df.dropna(subset=["raw_ticker", "designation_date"])

    truncate_table(conn, "raw_kind_investment_caution")

    cols = ["seq", "raw_ticker", "stock_name", "caution_type",
            "announcement_date", "designation_date", "ingested_at"]
    rows = []
    for _, row in df.iterrows():
        caution_type = _safe_str(row.get("caution_type")) or ""
        rows.append((
            _safe_int(row.get("seq")),
            row["raw_ticker"],
            row.get("stock_name"),
            caution_type,
            row.get("announcement_date"),
            row["designation_date"],
            ingested_at,
        ))

    n = insert_batch(conn, "raw_kind_investment_caution", rows, cols)
    logger.info("Ingested %d rows -> raw_kind_investment_caution", n)
    return n


# ---------------------------------------------------------------------------
# Investment warning (투자경고종목)
# ---------------------------------------------------------------------------
# Source columns: 번호 종목명 종목코드 공시일 지정일 해제일

def ingest_investment_warning(conn: sqlite3.Connection, path: Path) -> int:
    """Ingest KIND 투자경고종목.xls into raw_kind_investment_warning."""
    df = read_html_xls(path)
    ingested_at = _NOW()

    col_map = {
        "번호":     "seq",
        "종목명":   "stock_name",
        "종목코드": "raw_ticker",
        "공시일":   "announcement_date",
        "지정일":   "designation_date",
        "해제일":   "removal_date",
    }
    df = df.rename(columns={c: v for c, v in col_map.items() if c in df.columns})
    df["raw_ticker"] = df["raw_ticker"].astype(str).str.strip()
    df["designation_date"] = df["designation_date"].apply(clean_date_str)
    df["removal_date"] = df["removal_date"].apply(_clean_removal_date)
    df = df.dropna(subset=["raw_ticker", "designation_date"])

    truncate_table(conn, "raw_kind_investment_warning")

    cols = ["seq", "raw_ticker", "stock_name", "announcement_date",
            "designation_date", "removal_date", "ingested_at"]
    rows = []
    for _, row in df.iterrows():
        rows.append((
            _safe_int(row.get("seq")),
            row["raw_ticker"],
            row.get("stock_name"),
            row.get("announcement_date"),
            row["designation_date"],
            row.get("removal_date"),
            ingested_at,
        ))

    n = insert_batch(conn, "raw_kind_investment_warning", rows, cols)
    logger.info("Ingested %d rows -> raw_kind_investment_warning", n)
    return n


# ---------------------------------------------------------------------------
# Investment risk (투자위험종목)
# ---------------------------------------------------------------------------
# Source columns: 번호 종목명 종목코드 공시일 지정일 해제일

def ingest_investment_risk(conn: sqlite3.Connection, path: Path) -> int:
    """Ingest KIND 투자위험종목.xls into raw_kind_investment_risk."""
    df = read_html_xls(path)
    ingested_at = _NOW()

    col_map = {
        "번호":     "seq",
        "종목명":   "stock_name",
        "종목코드": "raw_ticker",
        "공시일":   "announcement_date",
        "지정일":   "designation_date",
        "해제일":   "removal_date",
    }
    df = df.rename(columns={c: v for c, v in col_map.items() if c in df.columns})
    df["raw_ticker"] = df["raw_ticker"].astype(str).str.strip()
    df["designation_date"] = df["designation_date"].apply(clean_date_str)
    df["removal_date"] = df["removal_date"].apply(_clean_removal_date)
    df = df.dropna(subset=["raw_ticker", "designation_date"])

    truncate_table(conn, "raw_kind_investment_risk")

    cols = ["seq", "raw_ticker", "stock_name", "announcement_date",
            "designation_date", "removal_date", "ingested_at"]
    rows = []
    for _, row in df.iterrows():
        rows.append((
            _safe_int(row.get("seq")),
            row["raw_ticker"],
            row.get("stock_name"),
            row.get("announcement_date"),
            row["designation_date"],
            row.get("removal_date"),
            ingested_at,
        ))

    n = insert_batch(conn, "raw_kind_investment_risk", rows, cols)
    logger.info("Ingested %d rows -> raw_kind_investment_risk", n)
    return n


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _clean_removal_date(val) -> str | None:
    """
    Clean removal date: '-' means still active (return None).
    Otherwise parse as a date string.
    """
    if val is None:
        return None
    s = str(val).strip()
    if s in ("-", "", "N/A", "NA", "nan", "None"):
        return None
    return clean_date_str(s)


def _safe_int(val) -> int | None:
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_float(val) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_str(val) -> str | None:
    if val is None:
        return None
    import pandas as pd
    if isinstance(val, float) and pd.isna(val):
        return None
    return str(val).strip() or None
