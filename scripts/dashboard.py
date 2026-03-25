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
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.paths import DATA_DIR
from config import load_config as _load_cfg

try:
    _mc = _load_cfg().mode_config
    TIER_PRICES  = [t[0] for t in _mc.stake_tiers] if _mc.stake_tiers else []
    TIER_STAKES  = {t[0]: t[1] for t in _mc.stake_tiers} if _mc.stake_tiers else {}
except Exception:
    TIER_PRICES = []
    TIER_STAKES = {}

DB_PATH = DATA_DIR / "positions.db"

# -- Color pair IDs ------------------------------------------------------------
C_HEADER = 1   # cyan bold  — section titles
C_GOOD   = 2   # green      — positive values / active
C_BAD    = 3   # red        — negative / blocked / errors
C_WARN   = 4   # yellow     — warnings / gates
C_DIM    = 5   # dark       — timestamps, borders, secondary info
C_TITLE  = 6   # magenta    — main title

# -- Scan outcome display config -----------------------------------------------
OUTCOME_SHORT = {
    "placed":               "placed",
    "duplicate":            "dup",
    "cluster_cap":          "cluster_cap",
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
    "cluster_cap":          C_DIM,
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
    "cluster_cap":          "■",
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
    "cluster_cap",
    "depth_gate_dead_book",
    "depth_gate_thin",
    "depth_gate_error",
    "risk_rejected",
    "max_positions",
    "order_failed",
    "balance_exhausted",
]


# -- Data loading --------------------------------------------------------------

def load_data(db_path: Path) -> dict:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}

    d: dict = {}

    # -- Balance --------------------------------------------------------------
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

    # -- Counts ---------------------------------------------------------------
    d["open_pos"] = conn.execute(
        "SELECT COUNT(*) FROM positions WHERE status='open'"
    ).fetchone()[0] if "positions" in tables else 0

    d["open_markets"] = conn.execute(
        "SELECT COUNT(DISTINCT market_id) FROM positions WHERE status='open'"
    ).fetchone()[0] if "positions" in tables else 0

    d["live_resting"] = conn.execute(
        "SELECT COUNT(*) FROM resting_orders WHERE status='live'"
    ).fetchone()[0] if "resting_orders" in tables else 0

    d["resting_markets"] = conn.execute(
        "SELECT COUNT(DISTINCT market_id) FROM resting_orders WHERE status='live'"
    ).fetchone()[0] if "resting_orders" in tables else 0

    d["live_tp"] = conn.execute(
        "SELECT COUNT(*) FROM tp_orders WHERE status='live'"
    ).fetchone()[0] if "tp_orders" in tables else 0

    # -- Performance ----------------------------------------------------------
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

    # -- Last scan cycle stats ------------------------------------------------
    now_ts = int(time.time())
    since_1h = now_ts - 3600
    today_start = now_ts - (now_ts % 86400)  # midnight UTC

    if "screener_log" in tables:
        # Last cycle: all screener_log rows within 30s of the latest scanned_at
        last_scan_ts = conn.execute(
            "SELECT MAX(scanned_at) FROM screener_log"
        ).fetchone()[0]
        d["last_scan_ts"] = last_scan_ts
        if last_scan_ts:
            cycle_rows = conn.execute(
                "SELECT outcome, COUNT(*) n FROM screener_log "
                "WHERE scanned_at >= ? GROUP BY outcome",
                (last_scan_ts - 30,),
            ).fetchall()
            cycle = {r["outcome"]: r["n"] for r in cycle_rows}
            d["cycle_fetched"]  = sum(cycle.values())
            d["cycle_passed"]   = cycle.get("passed_to_order_manager", 0)
            d["cycle_rejected"] = d["cycle_fetched"] - d["cycle_passed"]
        else:
            d["cycle_fetched"] = d["cycle_passed"] = d["cycle_rejected"] = 0

        # Screener funnel — last hour
        rows = conn.execute(
            "SELECT outcome, COUNT(*) n FROM screener_log "
            "WHERE scanned_at >= ? GROUP BY outcome",
            (since_1h,),
        ).fetchall()
        d["screener"] = {r["outcome"]: r["n"] for r in rows}
    else:
        d["last_scan_ts"] = None
        d["cycle_fetched"] = d["cycle_passed"] = d["cycle_rejected"] = 0
        d["screener"] = {}

    # -- Today's order activity -----------------------------------------------
    if "resting_orders" in tables:
        d["placed_today"] = conn.execute(
            "SELECT COUNT(*) FROM resting_orders WHERE created_at >= ?",
            (today_start,),
        ).fetchone()[0]
        d["placed_today_markets"] = conn.execute(
            "SELECT COUNT(DISTINCT market_id) FROM resting_orders WHERE created_at >= ?",
            (today_start,),
        ).fetchone()[0]
        d["cancelled_today"] = conn.execute(
            "SELECT COUNT(*) FROM resting_orders WHERE status='cancelled' AND created_at >= ?",
            (today_start,),
        ).fetchone()[0]
    else:
        d["placed_today"] = d["placed_today_markets"] = d["cancelled_today"] = 0

    if "positions" in tables:
        d["filled_today"] = conn.execute(
            "SELECT COUNT(*) FROM positions WHERE opened_at >= ?",
            (today_start,),
        ).fetchone()[0]

        # Open positions grouped by market, with question and estimated close time
        open_mkts = conn.execute("""
            SELECT
                p.market_id,
                SUM(p.entry_size_usdc)  AS total_invested,
                COUNT(*)                AS n_positions,
                (SELECT question FROM screener_log
                 WHERE market_id = p.market_id
                 ORDER BY scanned_at DESC LIMIT 1) AS question,
                (SELECT CAST(scanned_at + hours_to_close * 3600 AS INTEGER)
                 FROM screener_log
                 WHERE market_id = p.market_id AND hours_to_close IS NOT NULL
                 ORDER BY scanned_at DESC LIMIT 1) AS end_ts
            FROM positions p
            WHERE p.status = 'open'
            GROUP BY p.market_id
            ORDER BY total_invested DESC
        """).fetchall()

        # Per-position entry details for tier display
        pos_detail = conn.execute(
            "SELECT market_id, entry_price, entry_size_usdc "
            "FROM positions WHERE status='open' ORDER BY entry_price DESC"
        ).fetchall()
        pos_by_market: dict = {}
        for r in pos_detail:
            pos_by_market.setdefault(r["market_id"], []).append(
                (float(r["entry_price"]), float(r["entry_size_usdc"]))
            )

        # Live resting bids per market (with partial fill info)
        rest_detail = conn.execute(
            "SELECT market_id, price, size, filled_quantity FROM resting_orders "
            "WHERE status='live' ORDER BY price DESC"
        ).fetchall() if "resting_orders" in tables else []
        rest_by_market: dict = {}
        for r in rest_detail:
            size = float(r["size"] or 0)
            filled = float(r["filled_quantity"] or 0)
            pct = filled / size if size > 0 else 0.0
            rest_by_market.setdefault(r["market_id"], []).append((float(r["price"]), pct))

        markets_detail = []
        for row in open_mkts:
            m = dict(row)
            m["pos_tiers"]  = pos_by_market.get(m["market_id"], [])
            m["rest_tiers"] = rest_by_market.get(m["market_id"], [])
            markets_detail.append(m)
        d["open_markets_detail"] = markets_detail
    else:
        d["filled_today"] = 0
        d["open_markets_detail"] = []

    # ── Resting bids by market ────────────────────────────────────────────────
    if "resting_orders" in tables:
        resting_mkts = conn.execute("""
            SELECT
                r.market_id,
                COUNT(*)               AS n_orders,
                SUM(r.price * r.size)  AS reserved_usdc,
                GROUP_CONCAT(CAST(ROUND(r.price,4) AS TEXT), '/') AS price_levels,
                (SELECT question FROM screener_log
                 WHERE market_id = r.market_id
                 ORDER BY scanned_at DESC LIMIT 1) AS question,
                (SELECT CAST(scanned_at + hours_to_close * 3600 AS INTEGER)
                 FROM screener_log
                 WHERE market_id = r.market_id AND hours_to_close IS NOT NULL
                 ORDER BY scanned_at DESC LIMIT 1) AS end_ts
            FROM resting_orders r
            WHERE r.status = 'live'
            GROUP BY r.market_id
            ORDER BY reserved_usdc DESC
        """).fetchall()
        d["resting_markets_detail"] = [dict(r) for r in resting_mkts]
    else:
        d["resting_markets_detail"] = []

    # ── Exposure summary (v1.1) ───────────────────────────────────────────────
    if "exposure_v1_1" in tables:
        rows = conn.execute(
            "SELECT COUNT(*) n, SUM(total_stake_usdc) total, MAX(fill_count) max_fills "
            "FROM exposure_v1_1"
        ).fetchone()
        d["exposure_markets"] = rows[0] or 0
        d["exposure_total"]   = float(rows[1] or 0.0)
        d["exposure_max_fills"] = rows[2] or 0
        top = conn.execute(
            "SELECT market_id, token_id, total_stake_usdc, fill_count, avg_entry_price "
            "FROM exposure_v1_1 ORDER BY total_stake_usdc DESC LIMIT 5"
        ).fetchall()
        d["exposure_top"] = [dict(r) for r in top]
    else:
        d["exposure_markets"] = 0
        d["exposure_total"]   = 0.0
        d["exposure_max_fills"] = 0
        d["exposure_top"] = []

    # ── Order-manager scan funnel — last hour (late rejections) ──────────────
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


# -- Drawing helpers -----------------------------------------------------------

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
            win.hline(y, 0, "-", w, curses.color_pair(color_pair))
        except curses.error:
            pass


# -- Section renderers ---------------------------------------------------------

def _fmt_ago(ts: int) -> str:
    """Return 'Xm Ys ago' or 'Xs ago' string from a unix timestamp."""
    if not ts:
        return "never"
    secs = int(time.time()) - ts
    if secs < 0:
        return "just now"
    if secs < 60:
        return f"{secs}s ago"
    m, s = divmod(secs, 60)
    if m < 60:
        return f"{m}m {s:02d}s ago"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m ago"


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


def draw_activity(win, d: dict, row: int) -> int:
    h, w = win.getmaxyx()

    _w(win, row, 2, "SCAN ACTIVITY", curses.color_pair(C_HEADER) | curses.A_BOLD)
    try:
        win.hline(row, 16, "-", max(0, w - 18), curses.color_pair(C_DIM))
    except curses.error:
        pass
    row += 1

    # Last scan summary
    last_ts  = d.get("last_scan_ts")
    fetched  = d.get("cycle_fetched", 0)
    passed   = d.get("cycle_passed", 0)
    rejected = d.get("cycle_rejected", 0)

    ago_str = _fmt_ago(last_ts) if last_ts else "no data"
    if last_ts:
        scan_time = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%H:%M:%S")
        scan_label = f"  Last scan  {scan_time} UTC  ({ago_str})"
    else:
        scan_label = "  Last scan  —"

    _w(win, row, 0, scan_label, curses.color_pair(C_DIM))

    if fetched > 0:
        x = 2
        parts = [
            (f"  fetched=",   0),
            (f"{fetched}",    C_DIM),
            (f"   passed=",   0),
            (f"{passed}",     C_GOOD if passed > 0 else C_DIM),
            (f"   rejected=", 0),
            (f"{rejected}",   C_DIM),
        ]
        x = len(scan_label) + 4
        for text, color in parts:
            attr = curses.color_pair(color) if color else 0
            _w(win, row, x, text, attr)
            x += len(text)
    row += 1

    # Today's activity
    placed    = d.get("placed_today", 0)
    filled    = d.get("filled_today", 0)
    cancelled = d.get("cancelled_today", 0)

    x = 2
    _w(win, row, x, "  Today      ", curses.color_pair(C_DIM))
    x += 13
    placed_markets = d.get("placed_today_markets", 0)
    for label, val, extra, color in [
        ("placed=",    placed,    f" ({placed_markets}m)", C_GOOD if placed > 0 else C_DIM),
        ("   filled=", filled,    "",                      C_GOOD if filled > 0 else C_DIM),
        ("   cancelled=", cancelled, "",                   C_WARN if cancelled > 0 else C_DIM),
    ]:
        _w(win, row, x, label, curses.color_pair(C_DIM))
        x += len(label)
        _w(win, row, x, str(val), curses.color_pair(color))
        x += len(str(val))
        if extra:
            _w(win, row, x, extra, curses.color_pair(C_DIM))
            x += len(extra)
    row += 1

    return row + 1


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
    _w(win, row, mid, f"  Open positions  {d['open_pos']:>4}  ({d['open_markets']} markets)   Deployed  ${d['deployed']:>8.4f}")
    row += 1

    # Row 2: resting reserved  |  resting bids
    _w(win, row, 2,   f"  Resting rsrv   ${d['reserved_resting']:>9.4f}", curses.color_pair(C_WARN))
    _w(win, row, mid, f"  Resting bids   {d['live_resting']:>5} ({d.get('resting_markets', '?')}m)   Rsrv ${d['reserved_resting']:>8.4f}", curses.color_pair(C_WARN))
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
        win.hline(row, 14, "-", max(0, w - 16), curses.color_pair(C_DIM))
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


SCREENER_FUNNEL_ORDER = [
    "passed_to_order_manager",
    "hours_to_close_null_default_applied",
    "rejected_hours_to_close_min",
    "rejected_hours_to_close_max",
    "rejected_missing_token_ids",
    "rejected_entry_fill_score",
    "rejected_resolution_score",
    "rejected_market_score",
    "rejected_price_none",
    "rejected_price_le_zero",
    "rejected_price_above_entry_max",
    "rejected_price_ge_0_99",
    "rejected_no_entry_levels",
]

SCREENER_SHORT = {
    "passed_to_order_manager":            "-> order_mgr",
    "hours_to_close_null_default_applied":"htc_null_def",
    "rejected_hours_to_close_min":        "htc_min",
    "rejected_hours_to_close_max":        "htc_max",
    "rejected_missing_token_ids":         "no_tokens",
    "rejected_entry_fill_score":          "ef_score",
    "rejected_resolution_score":          "res_score",
    "rejected_market_score":              "mkt_score",
    "rejected_price_none":                "price_none",
    "rejected_price_le_zero":             "price_zero",
    "rejected_price_above_entry_max":     "price_high",
    "rejected_price_ge_0_99":             "price_99",
    "rejected_no_entry_levels":           "no_levels",
}

SCREENER_COLOR = {
    "passed_to_order_manager":            C_GOOD,
    "hours_to_close_null_default_applied":C_DIM,
    "rejected_hours_to_close_min":        C_DIM,
    "rejected_hours_to_close_max":        C_DIM,
    "rejected_missing_token_ids":         C_WARN,
    "rejected_entry_fill_score":          C_DIM,
    "rejected_resolution_score":          C_DIM,
    "rejected_market_score":              C_WARN,
    "rejected_price_none":                C_WARN,
    "rejected_price_le_zero":             C_WARN,
    "rejected_price_above_entry_max":     C_DIM,
    "rejected_price_ge_0_99":             C_DIM,
    "rejected_no_entry_levels":           C_WARN,
}


def _draw_funnel(win, row: int, title: str, items: list[tuple], total: int) -> int:
    """
    Render a funnel table in two side-by-side columns.
    items: list of (label, n, color_id)
    Layout per cell:  label(12)  n(6)  pct(5)   — 26 chars + 2 gap = 28 per col
    """
    h, w = win.getmaxyx()
    COL_W   = 28
    LABEL_W = 12
    N_W     = 6
    PCT_W   = 5   # " 78% "

    cols_per_row = min(2, max(1, (w - 4) // COL_W))
    col = 0

    for label, n, color_id in items:
        if row >= h - 2:
            break
        pct = 100.0 * n / total if total else 0
        x   = 2 + col * COL_W

        _w(win, row, x,              f"{label:<{LABEL_W}}", curses.color_pair(color_id))
        _w(win, row, x + LABEL_W,    f"{n:>{N_W},}",        curses.color_pair(C_DIM))
        _w(win, row, x + LABEL_W + N_W + 1, f"{pct:>4.0f}%", curses.color_pair(C_DIM))

        col += 1
        if col >= cols_per_row:
            col = 0
            row += 1

    if col > 0:
        row += 1
    return row + 1


def draw_screener_funnel(win, d: dict, row: int) -> int:
    h, w = win.getmaxyx()
    _w(win, row, 2, "SCREENER FUNNEL", curses.color_pair(C_HEADER) | curses.A_BOLD)
    _w(win, row, 18, " — last 60 min", curses.color_pair(C_DIM))
    try:
        win.hline(row, 33, "-", max(0, w - 35), curses.color_pair(C_DIM))
    except curses.error:
        pass
    row += 1

    screener = d.get("screener", {})
    total    = sum(screener.values())
    if total == 0:
        _w(win, row, 4, "no screener activity yet", curses.color_pair(C_DIM))
        return row + 2

    items = [
        (SCREENER_SHORT.get(k, k[:12]), screener[k], SCREENER_COLOR.get(k, C_DIM))
        for k in SCREENER_FUNNEL_ORDER
        if screener.get(k, 0) > 0
    ]
    return _draw_funnel(win, row, "SCREENER FUNNEL", items, total)


def draw_exposure(win, d: dict, row: int) -> int:
    h, w = win.getmaxyx()
    markets = d.get("exposure_markets", 0)
    total   = d.get("exposure_total", 0.0)
    top     = d.get("exposure_top", [])
    if not top:
        return row

    _w(win, row, 2, "EXPOSURE (v1.1)", curses.color_pair(C_HEADER) | curses.A_BOLD)
    summary = f"  {markets} markets  ${total:.2f} deployed"
    _w(win, row, 18, summary, curses.color_pair(C_DIM))
    try:
        win.hline(row, 18 + len(summary), "-", max(0, w - 20 - len(summary)), curses.color_pair(C_DIM))
    except curses.error:
        pass
    row += 1

    for r in top:
        if row >= h - 2:
            break
        mid = r["market_id"][:8]
        tok = r["token_id"][-6:]
        stake = float(r["total_stake_usdc"])
        fills = int(r["fill_count"])
        avg   = float(r["avg_entry_price"])
        _w(win, row, 4,
           f"  mkt={mid}… tok=…{tok}  stake=${stake:.2f}  fills={fills}  avg_price=${avg:.4f}",
           curses.color_pair(C_DIM))
        row += 1

    return row + 1


def draw_resting_markets(win, d: dict, row: int) -> int:
    h, w = win.getmaxyx()
    markets = d.get("resting_markets_detail", [])
    if not markets:
        return row

    _w(win, row, 2, "RESTING BIDS BY MARKET", curses.color_pair(C_HEADER) | curses.A_BOLD)
    summary = f"  {len(markets)} markets"
    _w(win, row, 25, summary, curses.color_pair(C_DIM))
    try:
        win.hline(row, 25 + len(summary), "-", max(0, w - 27 - len(summary)), curses.color_pair(C_DIM))
    except curses.error:
        pass
    row += 1

    for m in markets:
        if row >= h - 2:
            break
        question = (m.get("question") or m["market_id"])
        ttc_str  = f"[{_fmt_ttc(m.get('end_ts'))}]"
        prices   = m.get("price_levels", "")
        rsrv     = float(m.get("reserved_usdc") or 0)
        n        = int(m.get("n_orders") or 0)

        q_max = max(10, w - 4 - len(ttc_str) - 3)
        _w(win, row, 2, "  " + question[:q_max], curses.color_pair(C_DIM))
        _w(win, row, w - len(ttc_str) - 1, ttc_str, curses.color_pair(C_WARN))
        row += 1

        if row < h - 2:
            detail = f"    {n} bids @ {prices}  rsrv=${rsrv:.2f}"
            _w(win, row, 2, detail, curses.color_pair(C_DIM))
            row += 1

    return row + 1


def draw_scan_funnel(win, d: dict, row: int) -> int:
    h, w = win.getmaxyx()
    _w(win, row, 2, "ORDER MGR FUNNEL", curses.color_pair(C_HEADER) | curses.A_BOLD)
    _w(win, row, 19, " — last 60 min", curses.color_pair(C_DIM))
    try:
        win.hline(row, 34, "-", max(0, w - 36), curses.color_pair(C_DIM))
    except curses.error:
        pass
    row += 1

    scan        = d.get("scan", {})
    total_evals = sum(scan.values())
    if total_evals == 0:
        _w(win, row, 4, "no scan activity yet", curses.color_pair(C_DIM))
        return row + 2

    items = [
        (OUTCOME_SHORT.get(k, k[:12]), scan[k], OUTCOME_COLOR.get(k, C_DIM))
        for k in FUNNEL_ORDER
        if scan.get(k, 0) > 0
    ]
    return _draw_funnel(win, row, "ORDER MGR FUNNEL", items, total_evals)


def _fmt_ttc(end_ts: int | None) -> str:
    """Format time-to-close as 1d:2h:3m or 'expired'."""
    if not end_ts:
        return "?"
    secs = int(end_ts - time.time())
    if secs <= 0:
        return "expired"
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    if d > 0:
        return f"{d}d:{h}h:{m:02d}m"
    if h > 0:
        return f"{h}h:{m:02d}m"
    return f"{m}m"


def _fmt_tier_col(tier_price: float, pos_map: dict, rest_map: dict) -> tuple[str, int]:
    """
    Return (text, color) for one tier column.
    pos_map:  price → stake_usdc (filled position)
    rest_map: price → fill_pct   (live resting bid)
    Format: Tier1(0.001)=$0.50|100%   or   Tier2(0.005)=~40%   or   Tier3(0.010)=--
    """
    label = f"(0.{int(tier_price*1000):03d})"
    if tier_price in pos_map:
        stake = pos_map[tier_price]
        text  = f"{label}=${stake:.2f}|100%"
        color = C_GOOD
    elif tier_price in rest_map:
        pct = rest_map[tier_price]
        if pct > 0.01:
            text  = f"{label}=~{pct:.0%}"
        else:
            text  = f"{label}=resting"
        color = C_WARN
    else:
        text  = f"{label}=--"
        color = C_DIM
    return text, color


def draw_open_markets(win, d: dict, row: int) -> int:
    h, w = win.getmaxyx()
    markets = d.get("open_markets_detail", [])
    if not markets:
        return row

    _w(win, row, 2, "OPEN POSITIONS BY MARKET", curses.color_pair(C_HEADER) | curses.A_BOLD)
    try:
        win.hline(row, 27, "-", max(0, w - 29), curses.color_pair(C_DIM))
    except curses.error:
        pass
    row += 1

    # Tier price labels: use config tiers, fallback to union of seen prices
    tier_prices = TIER_PRICES or []

    for m in markets:
        if row >= h - 3:
            break

        question = (m.get("question") or m["market_id"])
        ttc      = _fmt_ttc(m.get("end_ts"))
        ttc_str  = f"[{ttc}]"

        # Line 1: question  [ttc]
        q_max = max(10, w - 4 - len(ttc_str) - 3)
        _w(win, row, 2, "  " + question[:q_max], curses.color_pair(C_DIM))
        _w(win, row, w - len(ttc_str) - 1, ttc_str, curses.color_pair(C_WARN))
        row += 1

        # Line 2: tier columns
        if row < h - 2:
            pos_map  = {price: stake for price, stake in m.get("pos_tiers",  [])}
            rest_map = {price: pct   for price, pct   in m.get("rest_tiers", [])}

            # Derive tier list: config tiers + any seen prices not in config
            all_prices = sorted(set(tier_prices) | set(pos_map) | set(rest_map))

            x = 4
            for i, tp in enumerate(all_prices):
                if x >= w - 10:
                    break
                prefix = f"Tier{i+1}"
                col_text, col_color = _fmt_tier_col(tp, pos_map, rest_map)
                full = prefix + col_text
                _w(win, row, x, prefix, curses.color_pair(C_DIM))
                _w(win, row, x + len(prefix), col_text, curses.color_pair(col_color))
                x += len(full) + 3   # 3-char gap between tiers
            row += 1

    return row + 1


def draw_recent(win, d: dict, row: int) -> int:
    h, w = win.getmaxyx()
    if row >= h - 2:
        return row

    _w(win, row, 2, "RECENT ACTIVITY", curses.color_pair(C_HEADER) | curses.A_BOLD)
    try:
        win.hline(row, 18, "-", max(0, w - 20), curses.color_pair(C_DIM))
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


# -- Main curses loop ----------------------------------------------------------

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
            row = draw_activity(stdscr, d, row)
            row = draw_balance_positions(stdscr, d, row)
            row = draw_performance(stdscr, d, row)
            row = draw_open_markets(stdscr, d, row)
            row = draw_resting_markets(stdscr, d, row)
            row = draw_screener_funnel(stdscr, d, row)
            row = draw_scan_funnel(stdscr, d, row)
            row = draw_exposure(stdscr, d, row)
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


# -- Watchdog -----------------------------------------------------------------

# How long screener_log silence = bot considered dead (seconds)
WATCHDOG_SILENCE_SECS = 15 * 60   # 15 min (screener runs every 5 min)
WATCHDOG_CHECK_SECS   = 60        # check every 60 seconds


def _tg_send(text: str) -> None:
    """Fire-and-forget Telegram alert from watchdog thread."""
    try:
        import os
        token   = "8660557840:AAEOHHZJpF6B5ttQrNIvh-AxKu9LsFuOj4k"
        chat_id = os.getenv("TG_CHAT_ID", "39371757")
        import urllib.request, json as _json
        data = _json.dumps({"chat_id": chat_id, "text": text, "parse_mode": "HTML"}).encode()
        req  = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=data, headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def _watchdog(db_path: Path, stop_event: threading.Event) -> None:
    """
    Background thread: checks every 60s if the bot is alive.
    Sends a Telegram alert if:
      - process gone (pgrep miss) AND
      - screener_log has been silent for > WATCHDOG_SILENCE_SECS
    Rate-limited: one alert per hour max.
    """
    last_alert_ts: float = 0.0

    while not stop_event.is_set():
        stop_event.wait(WATCHDOG_CHECK_SECS)
        if stop_event.is_set():
            break

        try:
            process_alive = check_bot_alive()
            if process_alive:
                continue  # all good

            # Process gone — check screener_log recency
            last_scan_ts: int | None = None
            if db_path.exists():
                try:
                    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
                    row = conn.execute(
                        "SELECT MAX(scanned_at) FROM screener_log"
                    ).fetchone()
                    conn.close()
                    last_scan_ts = row[0]
                except Exception:
                    pass

            now = time.time()
            silence = now - last_scan_ts if last_scan_ts else WATCHDOG_SILENCE_SECS + 1

            if silence < WATCHDOG_SILENCE_SECS:
                continue  # just restarting, screener data still fresh

            # Rate-limit: one alert per hour
            if now - last_alert_ts < 3600:
                continue

            last_alert_ts = now
            ago = int(silence // 60)
            _tg_send(
                f"🔴 <b>Bot appears DOWN</b>\n"
                f"Process not found + screener silent for {ago} min.\n"
                f"Last scan: {datetime.fromtimestamp(last_scan_ts, tz=timezone.utc).strftime('%H:%M UTC') if last_scan_ts else 'never'}"
            )
        except Exception:
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Polymarket bot live dashboard")
    ap.add_argument("--db",       default=None, help="Path to positions.db")
    ap.add_argument("--interval", type=int, default=3, help="Refresh interval in seconds")
    ap.add_argument("--no-watchdog", action="store_true", help="Disable Telegram watchdog")
    args = ap.parse_args()

    db_path  = Path(args.db) if args.db else DB_PATH
    interval = max(1, args.interval)

    stop_event = threading.Event()
    if not args.no_watchdog:
        wd = threading.Thread(target=_watchdog, args=(db_path, stop_event), daemon=True)
        wd.start()

    try:
        curses.wrapper(run, db_path, interval)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()


if __name__ == "__main__":
    main()
