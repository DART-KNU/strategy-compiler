"""
Interactive strategy design chat CLI.

Usage:
    python -m backtest_engine.api.chat
    python -m backtest_engine.api.chat --db database/db/data/db/backtest.db --out runs/
    python -m backtest_engine.api.chat --model gpt-4o --verbose
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_DEFAULT_DB = str(Path(__file__).parent.parent.parent / "database" / "db" / "data" / "db" / "backtest.db")

_W = 62  # total width


def _dw(s: str) -> int:
    """Display width: CJK/Korean characters count as 2 columns."""
    return sum(
        2 if "\uAC00" <= c <= "\uD7A3" or "\u4E00" <= c <= "\u9FFF" or "\u3400" <= c <= "\u4DBF" else 1
        for c in s
    )


# ── Layout helpers ─────────────────────────────────────────────────────────────

def _box_top(title: str = "") -> str:
    if title:
        pad = _W - 4 - len(title)
        return f"+-- {title} " + "-" * max(0, pad) + "+"
    return "+" + "-" * (_W - 2) + "+"

def _box_row(text: str = "", indent: int = 1) -> str:
    prefix = " " * indent
    content = prefix + text
    pad = _W - 2 - len(content)
    return "|" + content + " " * max(0, pad) + "|"

def _box_sep() -> str:
    return "|" + "-" * (_W - 2) + "|"

def _box_bot() -> str:
    return "+" + "-" * (_W - 2) + "+"

def _heavy(title: str = "") -> str:
    if title:
        side = (_W - 2 - len(title)) // 2
        return "=" * side + " " + title + " " + "=" * (_W - 2 - side - len(title) - 1)
    return "=" * _W

def _light() -> str:
    return "-" * _W


# ── Print helpers ──────────────────────────────────────────────────────────────

def _print_ai(message: str) -> None:
    """Print AI message indented for visual separation from user input."""
    # Strip all CR variants to prevent MINGW64 terminal display glitch
    clean = message.replace("\r\n", "\n").replace("\r", "\n")
    print()
    for line in clean.split("\n"):
        safe = line.replace("\r", "")
        print(f"  {safe}" if safe.strip() else "")
    print()


def _parse_pct_str(val) -> str:
    if val is None:
        return "-"
    if isinstance(val, str):
        return val
    try:
        return f"{float(val):.2%}"
    except (TypeError, ValueError):
        return str(val)


def _parse_float_str(val) -> str:
    if val is None:
        return "-"
    if isinstance(val, str):
        return val
    try:
        return f"{float(val):.3f}"
    except (TypeError, ValueError):
        return str(val)


def _print_metrics(metrics: dict) -> None:
    groups = [
        ("수익률", [
            ("총 수익률",          "total_return",           _parse_pct_str),
            ("연환산(CAGR)",       "cagr",                   _parse_pct_str),
            ("벤치마크 수익률",    "benchmark_total_return", _parse_pct_str),
            ("초과 수익률",        "excess_return",          _parse_pct_str),
        ]),
        ("위험", [
            ("연환산 변동성",      "annualized_vol",         _parse_pct_str),
            ("최대 낙폭(MDD)",     "max_drawdown",           _parse_pct_str),
            ("  고점일",           "max_dd_peak_date",       str),
            ("  저점일",           "max_dd_trough_date",     str),
        ]),
        ("위험 조정 수익률", [
            ("샤프 비율",          "sharpe",                 _parse_float_str),
            ("정보 비율(IR)",      "information_ratio",      _parse_float_str),
            ("추적오차(TE)",       "tracking_error",         _parse_pct_str),
            ("베타",               "beta",                   _parse_float_str),
        ]),
        ("거래", [
            ("평균 회전율",        "average_turnover",       _parse_pct_str),
            ("일간 승률",          "win_rate",               _parse_pct_str),
        ]),
    ]

    print()
    print(_box_top("백테스트 결과"))
    for group_name, fields in groups:
        items = [(lbl, key, fn) for lbl, key, fn in fields if metrics.get(key) is not None]
        if not items:
            continue
        print(_box_sep())
        print(_box_row(f"[ {group_name} ]"))
        for label, key, fmt_fn in items:
            val_str = fmt_fn(metrics[key])
            # dot-fill using display width so Korean chars (2 cols) align correctly
            col_w = 24
            dots = max(1, col_w - _dw(label))
            print(_box_row(f"{label} {'.' * dots} {val_str}"))
    print(_box_bot())


def _print_summary(strategy_summary: str) -> None:
    lines = [l for l in strategy_summary.split("\n") if l.strip()]
    print()
    print(_box_top("전략 요약"))
    for line in lines:
        print(_box_row(line))
    print(_box_bot())


def _print_section(title: str) -> None:
    print()
    print(_heavy(title))
    print()


def _fix_stdout_for_mingw() -> None:
    """
    Python on Windows opens stdout in text mode, which translates \\n → \\r\\n.
    MINGW64 (Mintty) treats \\r as cursor-to-column-0, causing the terminal
    scrollback buffer to capture multiple intermediate states — output appears
    duplicated.  Switching to newline='\\n' sends bare LF so Mintty renders
    each line exactly once.
    """
    import io
    if sys.platform == "win32" and hasattr(sys.stdout, "buffer"):
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer,
            encoding=sys.stdout.encoding or "utf-8",
            line_buffering=True,
            newline="\n",
        )


def main() -> None:
    _fix_stdout_for_mingw()
    parser = argparse.ArgumentParser(description="전략 설계 대화 인터페이스")
    parser.add_argument("--db",      default=_DEFAULT_DB, help="SQLite DB 경로")
    parser.add_argument("--out",     default="runs",      help="결과 저장 디렉터리 (기본: runs/)")
    parser.add_argument("--model",   default="gpt-4o",    help="OpenAI 모델 (기본: gpt-4o)")
    parser.add_argument("--verbose", action="store_true", help="LLM 원시 응답 출력")
    args = parser.parse_args()

    try:
        from backtest_engine.compiler.strategy_chat import StrategyChat
        from backtest_engine.compiler.chat_models import ChatStatus
    except ImportError as e:
        print(f"오류: {e}")
        sys.exit(1)

    try:
        chat = StrategyChat(model=args.model, db_path=args.db, verbose=args.verbose)
    except RuntimeError as e:
        print(f"\n오류: {e}")
        sys.exit(1)

    # ── Startup banner ────────────────────────────────────────────────
    print()
    print(_box_top())
    print(_box_row("DART Strategy Compiler Ver 1.0", indent=(_W - 2 - 30) // 2))
    print(_box_sep())
    print(_box_row("경북대학교 금융데이터분석학회 DART"))
    print(_box_sep())
    print(_box_row("전략 유형: 모멘텀 / 멀티팩터 / 저변동성 / 밸류 / 벤치마크 추종 / 향상된 인덱스"))
    print(_box_row("비중 배분: 동일가중 / 스코어비례 / 역변동성 / 마르코위츠 / 리스크버짓"))
    print(_box_row("백테스트: 2023-03-18 ~ 2026-03-20  |  모드: research / contest"))
    print(_box_sep())
    print(_box_row("종료: Ctrl+C 또는 'exit'"))
    print(_box_bot())
    print()

    # ── Main conversation loop ────────────────────────────────────────
    while True:
        try:
            user_input = input("> ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n\n대화를 종료합니다.")
            break

        if not user_input:
            continue
        if user_input.lower() in ("exit", "quit", "종료"):
            print("\n대화를 종료합니다.")
            break

        response = chat.send(user_input)

        if response.status == ChatStatus.READY and response.strategy_summary:
            _print_summary(response.strategy_summary)

        _print_ai(response.message)

        if response.status == ChatStatus.CONFIRMED:
            _print_section("백테스트 실행 중")
            try:
                report, narration = chat.run_and_narrate(out_dir=args.out)

                # ── Metrics table ─────────────────────────────────────
                _print_metrics(report.get("summary_metrics", {}))

                # ── AI review ─────────────────────────────────────────
                narration_clean = narration.replace("\r\n", "\n").replace("\r", "\n")
                _print_section("AI 전략 리뷰")
                _print_ai(narration_clean)

                # ── HTML report + charts ──────────────────────────────
                _generate_report(report, narration, args.out, args.verbose)

                break  # success

            except Exception as e:
                if args.verbose:
                    import traceback
                    traceback.print_exc()
                err_str = str(e)
                print(f"\n  오류: {err_str}\n")
                fix_response = chat.send(
                    f"[시스템 오류] 백테스트 실행 실패: {err_str}\n"
                    "draft_ir의 누락된 필드를 채워서 수정된 전략을 다시 제안해주세요. "
                    "특히 date_range, node_graph(nodes+output), selection이 모두 있는지 확인하세요."
                )
                _print_ai(fix_response.message)


def _generate_report(report: dict, narration: str, out_dir: str, verbose: bool) -> None:
    """Generate charts and HTML report inside a per-run subdirectory."""
    try:
        from backtest_engine.analytics.visualizer import generate_charts
        from backtest_engine.analytics.report_html import generate_html_report
        from backtest_engine.analytics.reporting import run_output_dir
        from pathlib import Path

        run_dir = run_output_dir(report, out_dir)
        run_dir.mkdir(parents=True, exist_ok=True)

        print("  차트 생성 중...")
        charts = generate_charts(report)

        strategy_id = report.get("strategy_id", "report")
        run_id = report.get("run_id", "")
        html_filename = f"{strategy_id}__{run_id}.html"
        generate_html_report(
            report=report,
            charts=charts,
            narration=narration,
            output_path=str(run_dir / html_filename),
        )

        json_name = f"{strategy_id}__{run_id}.json"
        print()
        print(_box_top("저장 완료"))
        print(_box_row(f"폴더   {run_dir}"))
        print(_box_sep())
        print(_box_row(f"HTML   {html_filename}"))
        print(_box_row(f"JSON   {json_name}"))
        print(_box_bot())
        print()

    except ImportError:
        if verbose:
            print("  [matplotlib 미설치 — 차트 생략]")
    except Exception as e:
        if verbose:
            import traceback
            traceback.print_exc()
        else:
            print(f"  [리포트 생성 오류: {e}]")


if __name__ == "__main__":
    main()
