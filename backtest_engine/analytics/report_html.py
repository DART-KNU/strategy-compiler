"""
HTML report generator — securities firm style.

Produces a self-contained HTML file with embedded base64 charts.
No external CSS/JS dependencies.
"""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any, Dict, Optional


_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', 'NanumGothic', sans-serif;
    font-size: 13px; color: #1a1a1a; background: #f8f8f8;
    line-height: 1.6;
}
.page { max-width: 1000px; margin: 0 auto; padding: 32px 24px; background: white; }
h1 { font-size: 22px; color: #0d1f3c; border-bottom: 3px solid #0d1f3c;
     padding-bottom: 10px; margin-bottom: 6px; }
.meta { font-size: 11px; color: #666; margin-bottom: 28px; }
h2 { font-size: 15px; color: #0d1f3c; border-left: 4px solid #2563eb;
     padding-left: 10px; margin: 28px 0 12px; }
.metrics-grid {
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px;
    margin-bottom: 8px;
}
.metric-card {
    background: #f4f7fb; border-radius: 6px; padding: 12px 14px;
    border-top: 3px solid #2563eb;
}
.metric-card.negative { border-top-color: #dc2626; }
.metric-card.positive { border-top-color: #16a34a; }
.metric-card .label { font-size: 10px; color: #666; margin-bottom: 4px; }
.metric-card .value { font-size: 18px; font-weight: 700; }
.metric-card .value.neg { color: #dc2626; }
.metric-card .value.pos { color: #16a34a; }
.metric-card .value.neutral { color: #1a1a1a; }
.table-wrap { overflow-x: auto; margin-bottom: 8px; }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
th { background: #0d1f3c; color: white; padding: 8px 10px;
     text-align: left; font-weight: 600; }
td { padding: 7px 10px; border-bottom: 1px solid #e5e7eb; }
tr:nth-child(even) td { background: #f9fafb; }
tr:hover td { background: #eff6ff; }
.chart-img { width: 100%; border: 1px solid #e5e7eb; border-radius: 6px;
             margin-bottom: 8px; }
.review-box {
    background: #f0f7ff; border-left: 4px solid #2563eb;
    padding: 16px 18px; border-radius: 0 6px 6px 0; margin-top: 8px;
    white-space: pre-wrap; font-size: 13px; line-height: 1.8;
}
.warning-box {
    background: #fff7ed; border: 1px solid #f97316; border-radius: 6px;
    padding: 10px 14px; margin-bottom: 16px; font-size: 12px; color: #7c2d12;
}
.footer { margin-top: 32px; padding-top: 12px; border-top: 1px solid #e5e7eb;
          font-size: 10px; color: #9ca3af; text-align: right; }
.section-divider { height: 1px; background: #e5e7eb; margin: 8px 0; }
"""


def _fmt_metric(val: Any, as_pct: bool = True, precision: int = 2) -> str:
    """Format a metric value that may be a float, int, or %-string."""
    if val is None:
        return "–"
    if isinstance(val, str):
        return val  # already formatted (e.g., "-69.66%")
    if isinstance(val, (int, float)):
        if as_pct:
            return f"{val:.{precision}%}"
        return f"{val:,.{precision}f}"
    return str(val)


def _sign_class(val: Any) -> str:
    """Return CSS class based on sign of value."""
    try:
        if isinstance(val, str):
            v = float(val.replace("%", "").replace(",", ""))
        else:
            v = float(val)
        return "pos" if v > 0 else ("neg" if v < 0 else "neutral")
    except (TypeError, ValueError):
        return "neutral"


def _card(label: str, value: Any, card_cls: str = "") -> str:
    cls = _sign_class(value)
    val_str = value if isinstance(value, str) else _fmt_metric(value)
    return (
        f'<div class="metric-card {card_cls}">'
        f'<div class="label">{html.escape(label)}</div>'
        f'<div class="value {cls}">{html.escape(str(val_str))}</div>'
        f'</div>'
    )


def _chart_section(title: str, b64: str) -> str:
    if not b64:
        return ""
    return (
        f"<h2>{html.escape(title)}</h2>"
        f'<img class="chart-img" src="data:image/png;base64,{b64}" alt="{html.escape(title)}">'
    )


def generate_html_report(
    report: Dict[str, Any],
    charts: Dict[str, str],
    narration: str,
    output_path: str,
) -> str:
    """
    Generate a self-contained HTML report.

    Parameters
    ----------
    report : dict   — backtest result bundle
    charts : dict   — {chart_name: base64_png_string}
    narration : str — AI-generated Korean review text
    output_path : str — file path to write HTML

    Returns
    -------
    str — path to the saved HTML file
    """
    m = report.get("summary_metrics", {})
    dr = report.get("date_range", {})
    strategy_id = report.get("strategy_id", "")
    initial_cap = report.get("initial_capital", 0)
    benchmark = report.get("benchmark_index", "")
    run_ts = report.get("run_timestamp", "")[:19].replace("T", " ")

    # ── Hero metrics ──────────────────────────────────────────────
    hero = (
        _card("총 수익률", m.get("total_return")) +
        _card("연환산(CAGR)", m.get("cagr")) +
        _card("샤프 비율", m.get("sharpe"), card_cls="") +
        _card("최대 낙폭(MDD)", m.get("max_drawdown"))
    )

    # ── Detailed metrics table ─────────────────────────────────────
    rows = [
        ("초기 자산", m.get("start_nav"), False),
        ("최종 자산", m.get("end_nav"), False),
        ("총 수익률", m.get("total_return"), True),
        ("연환산(CAGR)", m.get("cagr"), True),
        ("벤치마크 수익률", m.get("benchmark_total_return"), True),
        ("초과 수익률", m.get("excess_return"), True),
        ("연환산 변동성", m.get("annualized_vol"), True),
        ("샤프 비율", m.get("sharpe"), False),
        ("소르티노 비율", m.get("sortino"), False),
        ("칼마 비율", m.get("calmar"), False),
        ("최대 낙폭(MDD)", m.get("max_drawdown"), True),
        ("MDD 고점일", m.get("max_dd_peak_date"), False),
        ("MDD 저점일", m.get("max_dd_trough_date"), False),
        ("추적오차(TE)", m.get("tracking_error"), True),
        ("정보 비율(IR)", m.get("information_ratio"), False),
        ("베타", m.get("beta"), False),
        ("일간 승률", m.get("win_rate"), True),
        ("평균 회전율", m.get("average_turnover"), True),
        ("거래일 수", m.get("n_trading_days"), False),
    ]
    table_rows_html = ""
    for label, val, is_pct in rows:
        if val is None:
            continue
        val_str = val if isinstance(val, str) else _fmt_metric(val, as_pct=is_pct)
        cls = _sign_class(val)
        table_rows_html += (
            f"<tr><td>{html.escape(label)}</td>"
            f'<td class="{cls}" style="font-weight:600">{html.escape(str(val_str))}</td></tr>'
        )

    # ── Top holdings ──────────────────────────────────────────────
    holdings = report.get("top_holdings", [])
    holding_rows = ""
    for i, h in enumerate(holdings[:15], 1):
        ticker = h.get("ticker", "")
        weight = h.get("avg_weight", 0)
        holding_rows += (
            f"<tr><td>{i}</td><td>{html.escape(ticker)}</td>"
            f"<td>{weight:.2%}</td></tr>"
        )

    # ── Constraint violations ─────────────────────────────────────
    viols = report.get("constraint_violations", [])
    viol_html = ""
    if viols:
        viol_rows = "".join(f"<tr><td>{html.escape(str(v))}</td></tr>" for v in viols[:20])
        viol_html = (
            f'<div class="warning-box">⚠️ 제약 조건 위반 {len(viols)}건</div>'
            f"<h2>제약 조건 위반</h2>"
            f'<div class="table-wrap"><table><thead><tr><th>내용</th></tr></thead>'
            f"<tbody>{viol_rows}</tbody></table></div>"
        )

    # ── Assemble HTML ─────────────────────────────────────────────
    body = f"""
<div class="page">
  <h1>전략 백테스트 리포트</h1>
  <div class="meta">
    전략: <strong>{html.escape(strategy_id)}</strong> &nbsp;|&nbsp;
    기간: {html.escape(dr.get("start",""))} ~ {html.escape(dr.get("end",""))} &nbsp;|&nbsp;
    벤치마크: {html.escape(benchmark)} &nbsp;|&nbsp;
    초기자금: {initial_cap:,.0f} KRW &nbsp;|&nbsp;
    실행: {html.escape(run_ts)}
  </div>

  <h2>핵심 성과 지표</h2>
  <div class="metrics-grid">{hero}</div>

  <h2>상세 성과 지표</h2>
  <div class="table-wrap">
    <table>
      <thead><tr><th>지표</th><th>값</th></tr></thead>
      <tbody>{table_rows_html}</tbody>
    </table>
  </div>

  {_chart_section("누적 수익률", charts.get("nav",""))}
  {_chart_section("낙폭 (Drawdown)", charts.get("drawdown",""))}
  {_chart_section("월별 수익률 히트맵", charts.get("monthly",""))}
  {_chart_section("섹터 배분", charts.get("sector",""))}

  <h2>주요 보유 종목 (평균 비중 상위)</h2>
  <div class="table-wrap">
    <table>
      <thead><tr><th>#</th><th>종목코드</th><th>평균 비중</th></tr></thead>
      <tbody>{holding_rows}</tbody>
    </table>
  </div>

  {viol_html}

  <h2>AI 전략 리뷰</h2>
  <div class="review-box">{html.escape(narration or "리뷰가 없습니다.")}</div>

  <div class="footer">DART-backtest-NL &nbsp;|&nbsp; 본 리포트는 연구 목적의 백테스트 결과이며 실제 투자 권유가 아닙니다.</div>
</div>
"""

    full_html = (
        f'<!DOCTYPE html>\n<html lang="ko">\n<head>\n'
        f'<meta charset="UTF-8">\n'
        f'<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{html.escape(strategy_id)} — 백테스트 리포트</title>\n"
        f"<style>{_CSS}</style>\n"
        f"</head>\n<body>\n{body}\n</body>\n</html>"
    )

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(full_html)

    return str(path)
