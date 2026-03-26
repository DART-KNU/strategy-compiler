"""
Chart generation for backtest report bundles.

Korean font support: tries Malgun Gothic (Windows) → AppleGothic (Mac) → NanumGothic (Linux).
All charts saved as PNG; paths returned for HTML embedding.
"""

from __future__ import annotations

import base64
import io
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Font setup
# ---------------------------------------------------------------------------

def _setup_korean_font() -> Optional[str]:
    """Configure matplotlib for Korean text. Returns the font name used."""
    import matplotlib.pyplot as plt
    from matplotlib import font_manager

    candidates = [
        "Malgun Gothic",    # Windows
        "AppleGothic",      # macOS
        "Apple SD Gothic Neo",
        "NanumGothic",      # Linux
        "NanumBarunGothic",
        "Gulim",
        "Dotum",
    ]
    available = {f.name for f in font_manager.fontManager.ttflist}
    for font in candidates:
        if font in available:
            plt.rcParams["font.family"] = font
            plt.rcParams["axes.unicode_minus"] = False
            return font

    plt.rcParams["axes.unicode_minus"] = False
    return None


def _fig_to_base64(fig) -> str:
    """Convert matplotlib figure to base64 PNG string."""
    import matplotlib.pyplot as plt
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="white")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return b64


def _save_fig(fig, output_dir: str, name: str) -> str:
    """Save figure to file and return path."""
    import matplotlib.pyplot as plt
    path = Path(output_dir) / name
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return str(path)


def _parse_metric(val, default: float = 0.0) -> float:
    """Parse metric value that may be float, int, or formatted string like '-69.66%'."""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        clean = val.strip().replace(",", "")
        if clean.endswith("%"):
            try:
                return float(clean[:-1]) / 100
            except ValueError:
                pass
        try:
            return float(clean)
        except ValueError:
            pass
    return default


# ---------------------------------------------------------------------------
# Individual charts
# ---------------------------------------------------------------------------

def _plot_nav(report: dict) -> str:
    """NAV performance chart vs benchmark. Returns base64 PNG."""
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    nav = report.get("nav_series", {})
    bm_nav = report.get("benchmark_nav_series", {})
    if not nav:
        return ""

    dates = pd.to_datetime(list(nav.keys()))
    values = np.array(list(nav.values()), dtype=float)
    init = values[0]
    norm = values / init * 100

    fig, ax = plt.subplots(figsize=(11, 4.5))

    ax.plot(dates, norm, color="#1f77b4", linewidth=1.8, label="전략", zorder=3)

    if bm_nav:
        bm_dates = pd.to_datetime(list(bm_nav.keys()))
        bm_vals = np.array(list(bm_nav.values()), dtype=float)
        bm_init = bm_vals[0]
        bm_norm = bm_vals / bm_init * 100
        ax.plot(bm_dates, bm_norm, color="#ff7f0e", linewidth=1.4,
                linestyle="--", alpha=0.85, label=report.get("benchmark_index", "벤치마크"), zorder=2)

    ax.axhline(100, color="#cccccc", linewidth=0.8, linestyle=":")
    ax.fill_between(dates, norm, 100, where=(norm < 100), alpha=0.12, color="#d62728")
    ax.set_title("누적 수익률 (기준=100)", fontsize=13, fontweight="bold", pad=10)
    ax.set_ylabel("수익률 지수")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    return _fig_to_base64(fig)


def _plot_drawdown(report: dict) -> str:
    """Drawdown chart. Returns base64 PNG."""
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    dd = report.get("drawdown_series", {})
    nav = report.get("nav_series", {})

    if dd:
        dates = pd.to_datetime(list(dd.keys()))
        values = np.array(list(dd.values()), dtype=float)
    elif nav:
        nav_s = pd.Series(list(nav.values()), index=pd.to_datetime(list(nav.keys())), dtype=float)
        rolling_max = nav_s.cummax()
        dd_s = (nav_s - rolling_max) / rolling_max
        dates = dd_s.index
        values = dd_s.values
    else:
        return ""

    fig, ax = plt.subplots(figsize=(11, 3))
    ax.fill_between(dates, values * 100, 0, alpha=0.55, color="#d62728")
    ax.plot(dates, values * 100, color="#d62728", linewidth=0.9)
    ax.set_title("낙폭 (Drawdown)", fontsize=13, fontweight="bold", pad=10)
    ax.set_ylabel("낙폭 (%)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    return _fig_to_base64(fig)


def _plot_monthly_returns(report: dict) -> str:
    """Monthly returns heatmap (year × month). Returns base64 PNG."""
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors

    mrt = report.get("monthly_returns_table", {})
    if not mrt:
        return ""

    years = sorted(mrt.keys())
    months = [str(m) for m in range(1, 13)]
    month_labels = ["1월", "2월", "3월", "4월", "5월", "6월",
                    "7월", "8월", "9월", "10월", "11월", "12월"]

    data = np.full((len(years), 12), np.nan)
    for yi, yr in enumerate(years):
        for mi, mo in enumerate(months):
            v = mrt[yr].get(mo)
            if v is not None:
                data[yi, mi] = float(v)

    finite = data[np.isfinite(data)]
    vmax = max(abs(finite).max(), 5) if len(finite) > 0 else 5
    cmap = plt.cm.RdYlGn

    fig, ax = plt.subplots(figsize=(11, max(2.5, len(years) * 0.65)))
    im = ax.imshow(data, cmap=cmap, vmin=-vmax, vmax=vmax, aspect="auto")

    ax.set_xticks(range(12))
    ax.set_xticklabels(month_labels, fontsize=9)
    ax.set_yticks(range(len(years)))
    ax.set_yticklabels(years, fontsize=9)
    ax.set_title("월별 수익률 (%)", fontsize=13, fontweight="bold", pad=10)

    for yi in range(len(years)):
        for mi in range(12):
            v = data[yi, mi]
            if not np.isnan(v):
                color = "white" if abs(v) > vmax * 0.55 else "black"
                ax.text(mi, yi, f"{v:.1f}", ha="center", va="center",
                        fontsize=7.5, color=color, fontweight="bold")

    plt.colorbar(im, ax=ax, shrink=0.8, label="%")
    fig.tight_layout()
    return _fig_to_base64(fig)


def _plot_sector_exposure(report: dict) -> str:
    """Stacked bar chart of monthly sector exposure over full period. Returns base64 PNG."""
    import matplotlib.pyplot as plt
    import pandas as pd

    seh = report.get("sector_exposure_history", {})
    if not seh:
        return ""

    # Build DataFrame: rows=dates, cols=sectors
    rows = {}
    for date, sectors in seh.items():
        clean = {k: v for k, v in sectors.items()
                 if isinstance(k, str) and not any(c == "\ufffd" for c in k) and v > 0.001}
        if clean:
            rows[date] = clean
    if not rows:
        return ""

    df = pd.DataFrame(rows).T.fillna(0.0) * 100  # % weights
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # Keep top sectors by average weight; group rest as "기타"
    avg = df.mean().sort_values(ascending=False)
    top_sectors = avg.index[:9].tolist()
    other_cols = [c for c in df.columns if c not in top_sectors]
    if other_cols:
        df["기타"] = df[other_cols].sum(axis=1)
        df = df[top_sectors + ["기타"]]
    else:
        df = df[top_sectors]

    colors = plt.cm.tab10(np.linspace(0, 1, len(df.columns)))
    fig, ax = plt.subplots(figsize=(12, 4))
    bottom = np.zeros(len(df))
    x = np.arange(len(df))
    for i, col in enumerate(df.columns):
        vals = df[col].values
        ax.bar(x, vals, bottom=bottom, label=col, color=colors[i],
               edgecolor="white", linewidth=0.3)
        bottom += vals

    # X-axis: quarterly labels
    tick_step = max(1, len(df) // 8)
    ax.set_xticks(x[::tick_step])
    ax.set_xticklabels([d.strftime("%Y-%m") for d in df.index[::tick_step]],
                       rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("비중 (%)")
    ax.set_ylim(0, 105)
    dr = report.get("date_range", {})
    period = f"{dr.get('start', '')} ~ {dr.get('end', '')}"
    ax.set_title(f"섹터 배분 추이 ({period})", fontsize=13, fontweight="bold", pad=10)
    ax.legend(loc="upper right", fontsize=7.5, ncol=2, framealpha=0.7)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    return _fig_to_base64(fig)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_charts(report: dict) -> Dict[str, str]:
    """
    Generate all charts for a report bundle.

    Returns dict of chart_name -> base64 PNG string.
    Returns empty string for a chart if data is unavailable.
    """
    _setup_korean_font()
    return {
        "nav":      _plot_nav(report),
        "drawdown": _plot_drawdown(report),
        "monthly":  _plot_monthly_returns(report),
        "sector":   _plot_sector_exposure(report),
    }
