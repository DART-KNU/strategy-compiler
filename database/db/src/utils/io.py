"""
File I/O utilities.

KIND files from KRX are HTML-disguised-as-XLS (served with .xls extension
but are actually HTML tables). Use pandas read_html with euc-kr encoding.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def read_html_xls(path: str | Path, *, encodings=("euc-kr", "cp949", "utf-8")) -> pd.DataFrame:
    """
    Read a KIND .xls file that is actually an HTML table.

    KRX / KIND portals serve HTML with an .xls extension.
    Tries each encoding in order until one succeeds.

    Returns the first table found in the HTML document.
    Raises RuntimeError if no encoding works.
    """
    path = Path(path)
    last_error = None

    for enc in encodings:
        try:
            tables = pd.read_html(str(path), encoding=enc, flavor="lxml")
            if tables:
                df = tables[0]
                logger.debug("Read HTML-XLS %s: shape=%s, encoding=%s", path.name, df.shape, enc)
                return df
        except Exception as e:
            last_error = e
            continue

    raise RuntimeError(
        f"Failed to read HTML-XLS file {path}. Last error: {last_error}"
    )


def read_excel_sheet(
    path: str | Path,
    sheet_name: str,
    header: int | None = 0,
    nrows: int | None = None,
    dtype: dict | None = None,
) -> pd.DataFrame:
    """
    Read an .xlsx sheet using openpyxl engine.
    header=None means no header (all data rows).
    """
    return pd.read_excel(
        str(path),
        sheet_name=sheet_name,
        header=header,
        nrows=nrows,
        dtype=dtype,
        engine="openpyxl",
    )


def clean_date_str(val) -> str | None:
    """
    Coerce a value to ISO-8601 date string (YYYY-MM-DD).
    Handles datetime objects, date objects, strings in various formats.
    Returns None if the value cannot be parsed or is blank/null.
    """
    import datetime
    import pandas as pd

    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, str):
        val = val.strip()
        if not val or val in ("-", "N/A", "NA", "nan", "None"):
            return None
        # Try to parse
        for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d", "%Y%m%d"):
            try:
                return datetime.datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
        # Try pandas fallback
        try:
            return pd.to_datetime(val).strftime("%Y-%m-%d")
        except Exception:
            return None
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.strftime("%Y-%m-%d")
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    return None
