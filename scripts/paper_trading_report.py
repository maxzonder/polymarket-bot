"""
Paper trading report — issue #14.

Reads positions.db from a live dry-run session and produces a concise
comparison between what the scanner saw vs what actually happened:
  - candidates evaluated per outcome (placed / duplicate / depth_gate / ...)
  - fill rate on placed orders
  - TP hit rate
  - stale / unfilled rate
  - time-to-fill distribution

Replay baseline (from #8/#9) is included for direct comparison.

Usage:
    python3 scripts/paper_trading_report.py
    python3 scripts/paper_trading_report.py --db /path/to/positions.db
    python3 scripts/paper_trading_report.py --since 2026-03-18   # filter by date

How to start the live dry-run bot (no real capital):
    python3 main.py                   # dry_run=True by default, big_swan_mode
    DRY_RUN=true python3 main.py      # explicit
    BOT_MODE=big_swan_mode DRY_RUN=true python3 main.py

Position lifecycle (order_manager.py):
    resting_orders.status : live → matched | cancelled
    positions.status      : open → resolved
    tp_orders.status      : live → matched
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.paths import DATA_DIR
from utils.logger import setup_logger

logger = setup_logger("paper_trading_report")

# Historical replay baseline (high cohort, pruned stack, Dec+Jan+Feb from #8/#9)
REPLAY_BASELINE = {
    "candidates":   1155,
    "fill_rate_pct": 63.1,   # % of candidates that got ≥1 entry fill
    "winner_rate_pct": 12.4,
    "tp_per_entry":  0.48,
    "roi_pct":       300.0,
}


def load_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def scan_log_summary(conn: sqlite3.Connection, since_ts: int) -> dict:
    rows = conn.execute(
        "SELECT outcome, COUNT(*) AS n FROM scan_log WHERE scanned_at >= ? GROUP BY outcome",
        (since_ts,),
    ).fetchall()
    return {r["outcome"]: r["n"] for r in rows}


def order_outcomes(conn: sqlite3.Connection, since_ts: int) -> dict:
    """
    For each placed order in scan_log, join resting_orders to see final status.
    Returns counts and timing.
    """
    rows = conn.execute(
        """
        SELECT
            r.status,
            r.created_at,
            -- positions opened from this order
            COUNT(p.position_id) AS positions_opened,
            -- TP orders from those positions
            SUM(CASE WHEN tp.status = 'matched' THEN 1 ELSE 0 END) AS tp_filled,
            -- time from order placed to position opened
            MIN(p.opened_at - r.created_at)  AS min_ttf_s,
            MAX(p.opened_at - r.created_at)  AS max_ttf_s,
            AVG(p.opened_at - r.created_at)  AS avg_ttf_s
        FROM scan_log sl
        JOIN resting_orders r ON sl.order_id = r.order_id
        LEFT JOIN positions p ON p.entry_order_id = r.order_id
        LEFT JOIN tp_orders tp ON tp.position_id = p.position_id
        WHERE sl.outcome = 'placed'
          AND sl.scanned_at >= ?
        GROUP BY r.order_id
        """,
        (since_ts,),
    ).fetchall()
    return rows


def positions_summary(conn: sqlite3.Connection, since_ts: int) -> dict:
    row = conn.execute(
        """
        SELECT
            COUNT(*)                                AS total,
            SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END) AS closed,
            SUM(CASE WHEN status='open'     THEN 1 ELSE 0 END) AS open,
            SUM(CASE WHEN realized_pnl > 0  THEN 1 ELSE 0 END) AS profitable,
            SUM(entry_size_usdc)                    AS total_stake,
            SUM(COALESCE(realized_pnl, 0))          AS total_pnl
        FROM positions
        WHERE opened_at >= ?
        """,
        (since_ts,),
    ).fetchone()
    return dict(row) if row else {}


def stale_orders(conn: sqlite3.Connection, since_ts: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM resting_orders
        WHERE status IN ('cancelled', 'expired')
          AND created_at >= ?
        """,
        (since_ts,),
    ).fetchone()
    return row["n"] if row else 0


def has_scan_log(conn: sqlite3.Connection) -> bool:
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    return "scan_log" in tables


def print_report(
    scan: dict,
    order_rows: list,
    pos: dict,
    stale: int,
    since_ts: int,
    db_path: Path,
) -> None:
    sep = "=" * 72
    since_str = datetime.fromtimestamp(since_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"\n{sep}")
    print(f"  PAPER TRADING REPORT   dry_run=True | since {since_str}")
    print(sep)

    # ── Scan funnel ──────────────────────────────────────────────────────────
    total_evals = sum(scan.values())
    placed       = scan.get("placed", 0)
    duplicate    = scan.get("duplicate", 0)
    depth_dead   = scan.get("depth_gate_dead_book", 0)
    depth_thin   = scan.get("depth_gate_thin", 0)
    depth_err    = scan.get("depth_gate_error", 0)
    risk_rej     = scan.get("risk_rejected", 0)
    max_pos      = scan.get("max_positions", 0)
    order_fail   = scan.get("order_failed", 0)

    print(f"\n  SCANNER FUNNEL  (one row per candidate × price_level)")
    print(f"  {'Outcome':<28}  {'N':>6}  {'%':>6}")
    print(f"  {'-'*28}  {'-'*6}  {'-'*6}")
    for label, n in [
        ("placed",             placed),
        ("duplicate (skip)",   duplicate),
        ("depth_gate dead book", depth_dead),
        ("depth_gate thin",    depth_thin),
        ("depth_gate error",   depth_err),
        ("risk_rejected",      risk_rej),
        ("max_positions",      max_pos),
        ("order_failed",       order_fail),
    ]:
        if n == 0:
            continue
        pct = 100.0 * n / total_evals if total_evals else 0
        print(f"  {label:<28}  {n:>6}  {pct:>5.1f}%")
    print(f"  {'TOTAL':<28}  {total_evals:>6}")

    # ── Order outcomes ───────────────────────────────────────────────────────
    n_placed_orders = len(order_rows)
    n_filled        = sum(1 for r in order_rows if r["positions_opened"] > 0)
    n_tp_hit        = sum(1 for r in order_rows if r["tp_filled"] > 0)
    n_stale         = stale

    fill_rate = 100.0 * n_filled / n_placed_orders if n_placed_orders else 0.0
    tp_rate   = 100.0 * n_tp_hit  / n_filled       if n_filled else 0.0

    ttf_values = [r["avg_ttf_s"] for r in order_rows
                  if r["avg_ttf_s"] is not None and r["avg_ttf_s"] >= 0]
    avg_ttf_h = (sum(ttf_values) / len(ttf_values) / 3600) if ttf_values else None

    print(f"\n  ORDER OUTCOMES")
    print(f"  Orders placed              : {n_placed_orders}")
    print(f"  Filled (position opened)   : {n_filled}  ({fill_rate:.1f}%)")
    print(f"  TP hit                     : {n_tp_hit}  ({tp_rate:.1f}% of filled)")
    print(f"  Stale / cancelled          : {n_stale}")
    if avg_ttf_h is not None:
        print(f"  Avg time to fill           : {avg_ttf_h:.1f}h")

    # ── Positions ────────────────────────────────────────────────────────────
    total_stake = pos.get("total_stake") or 0.0
    total_pnl   = pos.get("total_pnl") or 0.0
    roi         = 100.0 * total_pnl / total_stake if total_stake > 0 else 0.0

    print(f"\n  POSITIONS")
    print(f"  Total opened               : {pos.get('total', 0)}")
    print(f"  Closed                     : {pos.get('closed', 0)}")
    print(f"  Still open                 : {pos.get('open', 0)}")
    print(f"  Profitable                 : {pos.get('profitable', 0)}")
    if total_stake > 0:
        print(f"  Stake deployed             : ${total_stake:.4f}")
        print(f"  Realized PnL               : ${total_pnl:+.4f}")
        print(f"  ROI (closed only)          : {roi:+.1f}%")

    # ── vs Replay baseline ───────────────────────────────────────────────────
    b = REPLAY_BASELINE
    # Replay baseline scope: big_swan_mode, entry_levels=(0.001,0.005,0.010),
    # swan_score >= 7, Dec 2025 + Jan 2026 + Feb 2026, ~1,022 positions.
    # Live paper trading scope: same mode/config, current live markets.
    # Comparison is directionally valid but not apples-to-apples:
    # replay used historical fills; live uses real orderbook fills.
    # Use the gap to detect systematic differences, not for exact alpha projection.
    print(f"\n  VS REPLAY BASELINE (#8/#9 | big_swan_mode, swan>=7, pruned stack)")
    print(f"  NOTE: replay=historical fills; live=real orderbook — gap shows execution realism")
    print(f"  {'Metric':<30}  {'Live':>10}  {'Replay':>10}  {'Gap':>8}")
    print(f"  {'-'*30}  {'-'*10}  {'-'*10}  {'-'*8}")

    def _row(label: str, live_val, replay_val, fmt: str = ".1f", suffix: str = "") -> None:
        if live_val is None:
            live_str = "n/a"
            gap_str  = "n/a"
        else:
            live_str = f"{live_val:{fmt}}{suffix}"
            gap_str  = f"{live_val - replay_val:+{fmt}}{suffix}"
        replay_str = f"{replay_val:{fmt}}{suffix}"
        print(f"  {label:<30}  {live_str:>10}  {replay_str:>10}  {gap_str:>8}")

    _row("fill_rate",   fill_rate if n_placed_orders else None,  b["fill_rate_pct"],  suffix="%")
    _row("tp / filled", tp_rate   if n_filled        else None,  b["tp_per_entry"]*100, suffix="%")
    _row("ROI",         roi        if total_stake > 0 else None,  b["roi_pct"],         suffix="%")

    print(sep)
    print(f"\n  DB: {db_path}")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="Paper trading report (issue #14)")
    ap.add_argument("--db", default=None, help="Path to positions.db")
    ap.add_argument("--since", default=None,
                    help="Only include events on/after this date (YYYY-MM-DD)")
    args = ap.parse_args()

    db_path = Path(args.db) if args.db else DATA_DIR / "positions.db"
    if not db_path.exists():
        logger.error(f"positions.db not found at {db_path}")
        sys.exit(1)

    if args.since:
        since_dt = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        since_ts = int(since_dt.timestamp())
    else:
        since_ts = 0

    conn = load_db(db_path)

    if not has_scan_log(conn):
        print(
            "\nNo scan_log table found. This positions.db predates issue #14 "
            "instrumentation.\nRun the bot with the updated order_manager.py to "
            "generate scan audit data.\n"
        )
        conn.close()
        sys.exit(0)

    scan        = scan_log_summary(conn, since_ts)
    order_rows  = order_outcomes(conn, since_ts)
    pos         = positions_summary(conn, since_ts)
    stale       = stale_orders(conn, since_ts)
    conn.close()

    print_report(scan, order_rows, pos, stale, since_ts, db_path)


if __name__ == "__main__":
    main()
