"""
Paper balance CLI — issue #21.

Commands:
    python scripts/paper_balance.py status
    python scripts/paper_balance.py topup --amount 10
    python scripts/paper_balance.py topup --amount 10 --note "second dry-run batch"
    python scripts/paper_balance.py history [--limit 20]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.paths import DATA_DIR
from utils.logger import setup_logger
from execution.paper_balance import get_balance, topup, init_tables, ensure_seeded
from utils.telegram import send_message

logger = setup_logger("paper_balance_cli")

DB_PATH = DATA_DIR / "positions.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def cmd_status() -> None:
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}")
        sys.exit(1)
    conn = _conn()
    b = get_balance(conn)
    conn.close()
    print(f"\n  Paper Balance")
    print(f"  {'cash_balance':<24}: ${b['cash_balance']:.4f}")
    print(f"  {'reserved (resting orders)':<24}: ${b['reserved_resting']:.4f}")
    print(f"  {'reserved (open positions)':<24}: ${b['reserved_positions']:.4f}")
    print(f"  {'reserved (total)':<24}: ${b['reserved']:.4f}")
    print(f"  {'free_balance':<24}: ${b['free_balance']:.4f}")
    blocked = b["free_balance"] <= 0
    print(f"  {'trading':<24}: {'BLOCKED (exhausted)' if blocked else 'ACTIVE'}")
    print()


def cmd_topup(amount: float, note: str) -> None:
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}")
        sys.exit(1)
    conn = _conn()
    init_tables(conn)
    ensure_seeded(conn)
    new_balance = topup(conn, amount, note)
    conn.commit()
    b = get_balance(conn)
    conn.close()
    print(f"  Top-up applied: +${amount:.2f}")
    print(f"  cash_balance=${new_balance:.4f} | free_balance=${b['free_balance']:.4f}")
    send_message(
        f"💰 <b>Paper balance top-up</b>\n"
        f"+${amount:.2f} USDC\n"
        f"Note: {note}\n"
        f"Cash: ${new_balance:.4f} | Free: ${b['free_balance']:.4f}"
    )


def cmd_history(limit: int) -> None:
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}")
        sys.exit(1)
    conn = _conn()
    rows = conn.execute(
        "SELECT ts, delta_usdc, note FROM paper_balance_events ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    if not rows:
        print("  No balance events found.")
        return
    print(f"\n  {'Timestamp':<22}  {'Delta':>10}  Note")
    print(f"  {'-'*22}  {'-'*10}  {'-'*40}")
    for r in rows:
        dt = datetime.fromtimestamp(r["ts"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sign = "+" if r["delta_usdc"] >= 0 else ""
        print(f"  {dt:<22}  {sign}{r['delta_usdc']:>9.4f}  {r['note'] or ''}")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="Paper balance CLI (issue #21)")
    sub = ap.add_subparsers(dest="command")

    sub.add_parser("status", help="Show current balance")

    tp = sub.add_parser("topup", help="Add paper USDC to the balance")
    tp.add_argument("--amount", type=float, required=True, help="USDC to add")
    tp.add_argument("--note", default="manual topup", help="Reason/label for this top-up")

    hist = sub.add_parser("history", help="Show recent balance events")
    hist.add_argument("--limit", type=int, default=20, help="Max rows to show")

    args = ap.parse_args()

    if args.command == "status":
        cmd_status()
    elif args.command == "topup":
        cmd_topup(args.amount, args.note)
    elif args.command == "history":
        cmd_history(args.limit)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
