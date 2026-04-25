#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from postmortem_probe import analyze_probe
from utils.telegram import send_message


def build_telegram_summary(db_path: str | Path, *, log_path: str | Path | None = None, max_chars: int = 3500) -> str:
    report = analyze_probe(db_path, log_path=log_path)
    status = "clean" if report.closed_cleanly and report.log_error_count == 0 else "needs review"
    lines = [
        f"<b>Probe completed: {html.escape(report.run_id or Path(db_path).name)}</b>",
        f"status: <b>{html.escape(status)}</b>",
        f"runtime: {html.escape(str(report.started_at or 'unknown'))} → {html.escape(str(report.finished_at or report.last_event_time or 'unknown'))} ({report.runtime_minutes:.1f}m)" if report.runtime_minutes is not None else "runtime: unknown",
        "",
        f"intents/fills/rejects: <b>{report.intents}/{report.fills}/{report.rejects}</b>",
        f"filled stake: <b>{report.cumulative_stake_usdc:.4f} USDC</b>",
        f"resolved PnL: <b>{report.resolved_pnl_usdc:.4f} USDC</b>",
        f"unresolved cost: <b>{report.unresolved_cost_usdc:.4f} USDC</b>",
        "",
        f"anti-chase: <b>{'PASS' if not report.anti_chase_violations and not report.ladder_violations else 'FAIL'}</b>",
        f"ws send failures: <b>{sum(report.ws_warning_counts[name] for name in report.ws_warning_counts if 'send_failed' in name)}</b>",
        f"ws disconnects: market={report.ws_warning_counts.get('ws_disconnected', 0)}, user={report.ws_warning_counts.get('user_ws_disconnected', 0)}",
        f"log errors: <b>{report.log_error_count}</b>",
        "",
        "top skips:",
    ]
    if report.skip_histogram:
        for reason, count in report.skip_histogram.most_common(6):
            lines.append(f"• {html.escape(reason)}: {count}")
    else:
        lines.append("• none")
    lines.extend(["", f"DB: <code>{html.escape(str(report.db_path))}</code>"])
    if report.log_path is not None:
        lines.append(f"log: <code>{html.escape(str(report.log_path))}</code>")
    text = "\n".join(lines)
    if len(text) > max_chars:
        return text[: max_chars - 1] + "…"
    return text


def main() -> None:
    parser = argparse.ArgumentParser(description="Send Telegram summary for a completed short-horizon probe")
    parser.add_argument("db_path")
    parser.add_argument("--log-path", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    text = build_telegram_summary(args.db_path, log_path=args.log_path)
    if args.dry_run:
        print(text)
        return
    if not send_message(text):
        raise SystemExit("Telegram notification failed")


if __name__ == "__main__":
    main()
