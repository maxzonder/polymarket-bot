#!/usr/bin/env python3
"""
Live terminal dashboard for the Polymarket bot.

Run in a separate tmux pane:
    python3 scripts/dashboard.py
    python3 scripts/dashboard.py --interval 5
    python3 scripts/dashboard.py --db /path/to/positions.db

Keys:
    q / Ctrl+C  — quit
    r           — force refresh now
"""

from __future__ import annotations

import argparse
import curses
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.paths import DATA_DIR

DB_PATH = DATA_DIR / "positions.db"

# ── Color pair IDs ────────────────────────────────────────────────────────────
C_HEADER = 1   # cyan bold  — section titles
C_GOOD   = 2   # green      — positive values / active
C_BAD    = 3   # red        — negative / blocked / errors
C_WARN   = 4   # yellow     — warnings / gates
C_DIM    = 5   # dark       — timestamps, borders, secondary info
C_TITLE  = 6   # magenta    — main title

# ── Scan outcome display config ───────────────────────────────────────────────
OUTCOME_SHORT = {
    "placed":               "placed",
    "duplicate":            "dup",
    "depth_gate_dead_book": "dead_book",
    "depth_gate_thin":      "thin_book",
    "depth_gate_error":     "book_err",
    "risk_rejected":        "risk_rej",
    "max_positions":        "max_pos",
    "order_failed":         "ord_fail",
    "balance_exhausted":    "bal_exh",
}

OUTCOME_COLOR = {
    "placed":               C_GOOD,
    "duplicate":            C_DIM,
    "depth_gate_dead_book": C_WARN,
    "depth_gate_thin":      C_WARN,
    "depth_gate_error":     C_WARN,
    "risk_rejected":        C_DIM,
    "max_positions":        C_WARN,
    "order_failed":         C_BAD,
    "balance_exhausted":    C_BAD,
}

OUTCOME_SYM = {
    "placed":               "✓",
    "duplicate":            "↩",
    "depth_gate_dead_book": "✗",
    "depth_gate_thin":      "~",
    "depth_gate_error":     "!",
    "risk_rejected":        "⊘",
    "max_positions":        "■",
    "order_failed":         "✗",
    "balance_exhausted":    "⊘",
}

# Funnel display order
FUNNEL_ORDER = [
    "placed",
    "duplicate",
    "depth_gate_dead_book",
    "depth_gate_thin",
    "depth_gate_error",
    "risk_rejected",
    "max_positions",
    "order_failed",
    "balance_exhausted",
]


# ── Data loading ──────────────────────────────────────────────────────────────

def load_data(db_path: Path) -> dict:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}

    d: dict = {}

    # ── Balance ──────────────────────────────────────────────────────────────
    if "paper_balance" in tables:
        row = conn.execute(
            "SELECT cash_balance FROM paper_balance WHERE id=1"
        ).fetchone()
        d["cash"] = float(row["cash_balance"]) if row else 0.0
    else:
        d["cash"] = 0.0

    d["reserved_resting"] = float(conn.execute(
        "SELECT COALESCE(SUM(price*size),0) FROM resting_orders WHERE status='live'"
    ).fetchone()[0]) if "resting_orders" in tables else 0.0

    d["free"] = d["cash"] - d["reserved_resting"]

    d["deployed"] = float(conn.execute(
        "SELECT COALESCE(SUM(entry_size_usdc),0) FROM positions WHERE status='open'"
    ).fetchone()[0]) if "positions" in tables else 0.0

    # ── Counts ───────────────────────────────────────────────────────────────
    d["open_pos"] = conn.execute(
        "SELECT COUNT(*) FROM positions WHERE status='open'"
    ).fetchone()[0] if "positions" in tables else 0

    d["live_resting"] = conn.execute(
        "SELECT COUNT(*) FROM resting_orders WHERE status='live'"
    ).fetchone()[0] if "resting_orders" in tables else 0

    d["live_tp"] = conn.execute(
        "SELECT COUNT(*) FROM tp_orders WHERE status='live'"
    ).fetchone()[0] if "tp_orders" in tables else 0

    # ── Performance ──────────────────────────────────────────────────────────
    if "positions" in tables:
        row = conn.execute(
            "SELECT COUNT(*), "
            "COALESCE(SUM(realized_pnl), 0), "
            "COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END), 0), "
            "COALESCE(SUM(entry_size_usdc), 0) "
            "FROM positions WHERE status='resolved'"
        ).fetchone()
        d["resolved"]    = row[0] or 0
        d["total_pnl"]   = float(row[1])
        d["winners"]     = int(row[2])
        d["total_stake"] = float(row[3])
    else:
        d["resolved"] = d["winners"] = 0
        d["total_pnl"] = d["total_stake"] = 0.0

    # ── Scan funnel — last hour ───────────────────────────────────────────────
    since_1h = int(time.time()) - 3600
    if "scan_log" in tables:
        rows = conn.execute(
            "SELECT outcome, COUNT(*) n FROM scan_log "
            "WHERE scanned_at >= ? GROUP BY outcome",
            (since_1h,),
        ).fetchall()
        d["scan"] = {r["outcome"]: r["n"] for r in rows}

        # Recent events — last 20 rows
        rows = conn.execute(
            "SELECT scanned_at, outcome, question, entry_level "
            "FROM scan_log ORDER BY id DESC LIMIT 20"
        ).fetchall()
        d["recent"] = [dict(r) for r in rows]
    else:
        d["scan"] = {}
        d["recent"] = []

    conn.close()
    return d


def check_bot_alive() -> bool:
    try:
        r = subprocess.run(
            ["pgrep", "-f", r"python.*main\.py"],
            capture_output=True,
        )
        return r.returncode == 0
    except Exception:
        return False


# ── Drawing helpers ───────────────────────────────────────────────────────────

def _w(win, y: int, x: int, text: str, attr: int = 0) -> None:
    """Safe addstr — silently ignores out-of-bounds writes."""
    h, w = win.getmaxyx()
    if y < 0 or y >= h - 1 or x < 0 or x >= w:
        return
    available = w - x - 1
    if available <= 0:
        return
    try:
        win.addstr(y, x, text[:available], attr)
    except curses.error:
        pass


def _hline(win, y: int, color_pair: int = 0) -> None:
    h, w = win.getmaxyx()
    if 0 <= y < h - 1:
        try:
            win.hline(y, 0, "─", w, curses.color_pair(color_pair))
        except curses.error:
            pass


# ── Section renderers ─────────────────────────────────────────────────────────

def draw_header(win, bot_alive: bool) -> int:
    h, w = win.getmaxyx()
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d  %H:%M:%S  UTC")
    status_str = "  ● LIVE " if bot_alive else "  ○ STOPPED "
    status_attr = curses.color_pair(C_GOOD) | curses.A_BOLD if bot_alive else curses.color_pair(C_BAD) | curses.A_BOLD

    _hline(win, 0, C_DIM)
    title = "  POLYMARKET BOT  "
    _w(win, 0, 1, title, curses.color_pair(C_TITLE) | curses.A_BOLD)
    right = now_str + status_str
    _w(win, 0, max(0, w - len(right) - 1), now_str, curses.color_pair(C_DIM))
    _w(win, 0, max(0, w - len(status_str) - 1), status_str, status_attr)
    return 2


def draw_balance_positions(win, d: dict, row: int) -> int:
    h, w = win.getmaxyx()
    mid = w // 2

    # Section headers
    _w(win, row, 2,   "BALANCE",   curses.color_pair(C_HEADER) | curses.A_BOLD)
    _w(win, row, mid, "POSITIONS", curses.color_pair(C_HEADER) | curses.A_BOLD)
    row += 1

    free_attr = curses.color_pair(C_BAD) | curses.A_BOLD if d["free"] <= 0 else curses.color_pair(C_GOOD) | curses.A_BOLD

    # Row 1: free cash  |  open positions
    _w(win, row, 2,   f"  Free cash      ${d['free']:>9.4f}", free_attr)
    _w(win, row, mid, f"  Open positions  {d['open_pos']:>4}   Deployed  ${d['deployed']:>8.4f}")
    row += 1

    # Row 2: resting reserved  |  resting bids
    _w(win, row, 2,   f"  Resting rsrv   ${d['reserved_resting']:>9.4f}", curses.color_pair(C_WARN))
    _w(win, row, mid, f"  Resting bids   {d['live_resting']:>5}   Reserved  ${d['reserved_resting']:>8.4f}", curses.color_pair(C_WARN))
    row += 1

    # Row 3: cash balance  |  TP orders live
    _w(win, row, 2,   f"  Cash balance   ${d['cash']:>9.4f}", curses.color_pair(C_DIM))
    _w(win, row, mid, f"  TP live        {d['live_tp']:>5}")
    row += 1

    return row + 1


def draw_performance(win, d: dict, row: int) -> int:
    h, w = win.getmaxyx()
    _w(win, row, 2, "PERFORMANCE", curses.color_pair(C_HEADER) | curses.A_BOLD)
    try:
        win.hline(row, 14, "─", max(0, w - 16), curses.color_pair(C_DIM))
    except curses.error:
        pass
    row += 1

    resolved = d["resolved"]
    winners  = d["winners"]
    pnl      = d["total_pnl"]
    stake    = d["total_stake"]
    win_rate = 100.0 * winners / resolved if resolved else 0.0
    roi      = 100.0 * pnl / stake if stake > 0 else 0.0

    pnl_attr = (curses.color_pair(C_GOOD) if pnl >= 0 else curses.color_pair(C_BAD)) | curses.A_BOLD
    roi_attr = (curses.color_pair(C_GOOD) if roi >= 0 else curses.color_pair(C_BAD)) | curses.A_BOLD

    x = 2
    prefix = f"  Resolved {resolved:>5}   Winners {winners:>4} ({win_rate:>5.1f}%)   PnL "
    _w(win, row, x, prefix)
    x += len(prefix)
    pnl_str = f"{pnl:+.4f} USDC"
    _w(win, row, x, pnl_str, pnl_attr)
    x += len(pnl_str)
    if stake > 0:
        roi_pre = "   ROI "
        _w(win, row, x, roi_pre, curses.color_pair(C_DIM))
        x += len(roi_pre)
        _w(win, row, x, f"{roi:+.1f}%", roi_attr)

    return row + 2


def draw_scan_funnel(win, d: dict, row: int) -> int:
    h, w = win.getmaxyx()
    _w(win, row, 2, "SCAN FUNNEL", curses.color_pair(C_HEADER) | curses.A_BOLD)
    _w(win, row, 14, " — last 60 min  ", curses.color_pair(C_DIM))
    try:
        win.hline(row, 30, "─", max(0, w - 32), curses.color_pair(C_DIM))
    except curses.error:
        pass
    row += 1

    scan = d["scan"]
    total_evals = sum(scan.values())
    if total_evals == 0:
        _w(win, row, 4, "no scan activity yet", curses.color_pair(C_DIM))
        return row + 2

    # Display in columns: label  N  pct%
    COL_W = 22
    cols_per_row = max(1, (w - 4) // COL_W)
    col = 0

    for key in FUNNEL_ORDER:
        n = scan.get(key, 0)
        if n == 0:
            continue
        pct  = 100.0 * n / total_evals
        label = OUTCOME_SHORT.get(key, key)
        color = curses.color_pair(OUTCOME_COLOR.get(key, C_DIM))
        x = 2 + col * COL_W

        _w(win, row, x,          f"{label:<12}", color)
        _w(win, row, x + 12,     f"{n:>4} ", curses.color_pair(C_DIM))
        _w(win, row, x + 17,     f"({pct:>4.0f}%)")

        col += 1
        if col >= cols_per_row:
            col = 0
            row += 1

    if col > 0:
        row += 1

    return row + 1


def draw_recent(win, d: dict, row: int) -> int:
    h, w = win.getmaxyx()
    if row >= h - 2:
        return row

    _w(win, row, 2, "RECENT ACTIVITY", curses.color_pair(C_HEADER) | curses.A_BOLD)
    try:
        win.hline(row, 18, "─", max(0, w - 20), curses.color_pair(C_DIM))
    except curses.error:
        pass
    row += 1

    for event in d.get("recent", []):
        if row >= h - 2:
            break

        ts      = datetime.fromtimestamp(event["scanned_at"], tz=timezone.utc).strftime("%H:%M:%S")
        outcome = event["outcome"] or ""
        q       = (event["question"] or "").strip()
        level   = event.get("entry_level") or 0.0

        sym   = OUTCOME_SYM.get(outcome, " ")
        short = OUTCOME_SHORT.get(outcome, outcome[:10])
        color = curses.color_pair(OUTCOME_COLOR.get(outcome, C_DIM))

        # Layout:  HH:MM:SS  sym short(12)  question...  @0.001
        level_str = f"@{level:.3f}"
        q_max     = max(10, w - 2 - 9 - 2 - 13 - len(level_str) - 3)
        q_trunc   = q[:q_max] if len(q) > q_max else q

        x = 2
        _w(win, row, x, ts, curses.color_pair(C_DIM));           x += 9
        _w(win, row, x, sym + " ", color)
        _w(win, row, x + 2, f"{short:<12}", color);              x += 14
        _w(win, row, x, q_trunc)
        _w(win, row, w - len(level_str) - 1, level_str, curses.color_pair(C_DIM))
        row += 1

    return row


def draw_footer(win, interval: int) -> None:
    h, w = win.getmaxyx()
    msg = f"  q quit   r refresh   auto-refresh every {interval}s  "
    _hline(win, h - 1, C_DIM)
    _w(win, h - 1, 0, msg, curses.color_pair(C_DIM))


def draw_no_db(win, db_path: Path) -> None:
    h, w = win.getmaxyx()
    _w(win, h // 2 - 1, 2, f"Waiting for DB: {db_path}", curses.color_pair(C_WARN))
    _w(win, h // 2,     2, "Start the bot with:  DRY_RUN=true python3 main.py", curses.color_pair(C_DIM))


# ── Main curses loop ──────────────────────────────────────────────────────────

def run(stdscr, db_path: Path, interval: int) -> None:
    curses.curs_set(0)
    curses.start_color()
    curses.use_default_colors()

    curses.init_pair(C_HEADER, curses.COLOR_CYAN,    -1)
    curses.init_pair(C_GOOD,   curses.COLOR_GREEN,   -1)
    curses.init_pair(C_BAD,    curses.COLOR_RED,     -1)
    curses.init_pair(C_WARN,   curses.COLOR_YELLOW,  -1)
    curses.init_pair(C_DIM,    8 if curses.COLORS >= 16 else curses.COLOR_WHITE, -1)
    curses.init_pair(C_TITLE,  curses.COLOR_MAGENTA, -1)

    stdscr.timeout(interval * 1000)

    last_data: dict | None = None
    last_err  = ""
    last_ts   = 0.0

    while True:
        now = time.monotonic()

        # Reload data if interval elapsed or forced
        if now - last_ts >= interval:
            last_err = ""
            if not db_path.exists():
                last_data = None
                last_err  = ""
            else:
                try:
                    last_data = load_data(db_path)
                    last_data["_alive"] = check_bot_alive()
                except Exception as e:
                    last_err = str(e)
            last_ts = now

        stdscr.erase()

        if last_data is None:
            draw_no_db(stdscr, db_path)
            draw_footer(stdscr, interval)
        elif last_err:
            draw_header(stdscr, False)
            h, w = stdscr.getmaxyx()
            _w(stdscr, h // 2, 2, f"Error: {last_err}", curses.color_pair(C_BAD))
            draw_footer(stdscr, interval)
        else:
            d = last_data
            bot_alive = d.get("_alive", False)
            row = draw_header(stdscr, bot_alive)
            row = draw_balance_positions(stdscr, d, row)
            row = draw_performance(stdscr, d, row)
            row = draw_scan_funnel(stdscr, d, row)
            draw_recent(stdscr, d, row)
            draw_footer(stdscr, interval)

        stdscr.refresh()

        # Wait for key or timeout
        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):   # q / ESC
            break
        if key in (ord("r"), ord("R")):        # force refresh
            last_ts = 0.0
        # Resize: re-draw next cycle automatically


def main() -> None:
    ap = argparse.ArgumentParser(description="Polymarket bot live dashboard")
    ap.add_argument("--db",       default=None, help="Path to positions.db")
    ap.add_argument("--interval", type=int, default=3, help="Refresh interval in seconds")
    args = ap.parse_args()

    db_path  = Path(args.db) if args.db else DB_PATH
    interval = max(1, args.interval)

    try:
        curses.wrapper(run, db_path, interval)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
