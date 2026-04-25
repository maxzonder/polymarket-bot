#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import os
import urllib.request
from pathlib import Path

from postmortem_probe import analyze_probe


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


def send_telegram(text: str, *, bot_token: str, chat_id: str) -> None:
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10):
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Send Telegram summary for a completed short-horizon probe")
    parser.add_argument("db_path")
    parser.add_argument("--log-path", default=None)
    parser.add_argument("--bot-token", default=os.environ.get("TELEGRAM_BOT_TOKEN"))
    parser.add_argument("--chat-id", default=os.environ.get("TELEGRAM_CHAT_ID"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    text = build_telegram_summary(args.db_path, log_path=args.log_path)
    if args.dry_run:
        print(text)
        return
    if not args.bot_token or not args.chat_id:
        raise SystemExit("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required")
    send_telegram(text, bot_token=args.bot_token, chat_id=args.chat_id)


if __name__ == "__main__":
    main()
