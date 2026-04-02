"""
Execution layer health check — issue #71.

Detects common execution anomalies by querying the positions DB directly.

Checks:
  1. TP orders marked 'matched' with incomplete fill (partial TP bug)
  2. Multiple live resting orders on the same token_id (scanner dedupe bug)
  3. Multiple open positions on the same token_id (exposure/dedupe leak)
  4. exposure_v1_1 table empty while positions are open (ExposureManager not wired)

Usage:
    python scripts/execution_health.py
    python scripts/execution_health.py --db /path/to/positions.db
    python scripts/execution_health.py --telegram   # send result to Telegram on failure
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.paths import DATA_DIR

DB_PATH = DATA_DIR / "positions.db"

PASS = "OK"
FAIL = "FAIL"
WARN = "WARN"


def _conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def check_premature_matched_tp(conn: sqlite3.Connection) -> tuple[str, str, list]:
    """TP orders marked matched but filled_quantity < sell_quantity * 0.99."""
    rows = conn.execute("""
        SELECT order_id, sell_quantity, filled_quantity, label
        FROM tp_orders
        WHERE status = 'matched'
          AND filled_quantity < sell_quantity * 0.99
    """).fetchall()
    if not rows:
        return PASS, "No prematurely-matched TP orders", []
    detail = [
        f"  {r['order_id'][:12]} {r['label']} filled={r['filled_quantity']:.2f}/{r['sell_quantity']:.2f}"
        for r in rows
    ]
    return FAIL, f"{len(rows)} TP order(s) matched before fully filled", detail


def check_duplicate_resting_orders(conn: sqlite3.Connection) -> tuple[str, str, list]:
    """Multiple live resting orders on the same token_id."""
    rows = conn.execute("""
        SELECT token_id, COUNT(*) as cnt
        FROM resting_orders
        WHERE status = 'live'
        GROUP BY token_id
        HAVING cnt > 1
    """).fetchall()
    if not rows:
        return PASS, "No duplicate live resting orders", []
    detail = [f"  token={r['token_id'][:20]} count={r['cnt']}" for r in rows]
    return FAIL, f"{len(rows)} token(s) with duplicate live resting orders", detail


def check_duplicate_open_positions(conn: sqlite3.Connection) -> tuple[str, str, list]:
    """
    Multiple open positions on the same token_id AND same market_id.

    Note: multiple positions on the same token across different price levels is
    intentional (resting-bid ladder). This check only fires if the same
    (market_id, token_id) pair has more than one open position, which indicates
    a dedupe failure rather than normal ladder behaviour.
    """
    rows = conn.execute("""
        SELECT market_id, token_id, COUNT(*) as cnt
        FROM positions
        WHERE status = 'open'
        GROUP BY market_id, token_id
        HAVING cnt > 1
    """).fetchall()
    if not rows:
        return PASS, "No duplicate open positions per (market, token)", []
    detail = [
        f"  market={r['market_id'][:16]} token={r['token_id'][:20]} count={r['cnt']}"
        for r in rows
    ]
    return FAIL, f"{len(rows)} (market, token) pair(s) with duplicate open positions", detail


def check_orphaned_tp_orders(conn: sqlite3.Connection) -> tuple[str, str, list]:
    """Live TP orders whose parent position is no longer open."""
    rows = conn.execute("""
        SELECT t.order_id, t.label, t.token_id, p.status AS pos_status
        FROM tp_orders t
        JOIN positions p ON t.position_id = p.position_id
        WHERE t.status = 'live'
          AND p.status != 'open'
    """).fetchall()
    if not rows:
        return PASS, "No orphaned TP orders", []
    detail = [
        f"  {r['order_id'][:12]} {r['label']} token={r['token_id'][:20]} pos_status={r['pos_status']}"
        for r in rows
    ]
    return FAIL, f"{len(rows)} live TP order(s) on non-open position(s)", detail


def check_open_positions_without_tp(conn: sqlite3.Connection) -> tuple[str, str, list]:
    """Open positions with no live or moonbag TP orders — can never close via TP."""
    rows = conn.execute("""
        SELECT p.position_id, p.token_id, p.opened_at
        FROM positions p
        WHERE p.status = 'open'
          AND NOT EXISTS (
              SELECT 1 FROM tp_orders t
              WHERE t.position_id = p.position_id
                AND t.status IN ('live', 'moonbag')
          )
    """).fetchall()
    if not rows:
        return PASS, "All open positions have live TP orders", []
    detail = [
        f"  pos={r['position_id'][:12]} token={r['token_id'][:20]}"
        for r in rows
    ]
    return WARN, f"{len(rows)} open position(s) with no live TP orders", detail


def check_tp_overfilled(conn: sqlite3.Connection) -> tuple[str, str, list]:
    """TP orders where filled_quantity exceeds sell_quantity — data integrity."""
    rows = conn.execute("""
        SELECT order_id, label, sell_quantity, filled_quantity
        FROM tp_orders
        WHERE filled_quantity > sell_quantity * 1.01
    """).fetchall()
    if not rows:
        return PASS, "No over-filled TP orders", []
    detail = [
        f"  {r['order_id'][:12]} {r['label']} filled={r['filled_quantity']:.2f} > sell={r['sell_quantity']:.2f}"
        for r in rows
    ]
    return FAIL, f"{len(rows)} TP order(s) with filled_quantity > sell_quantity", detail


def check_exposure_manager_wired(conn: sqlite3.Connection) -> tuple[str, str, list]:
    """exposure_v1_1 table empty while there are open positions."""
    try:
        exp_count = conn.execute("SELECT COUNT(*) FROM exposure_v1_1").fetchone()[0]
    except sqlite3.OperationalError:
        exp_count = 0  # table doesn't exist yet
    open_count = conn.execute(
        "SELECT COUNT(*) FROM positions WHERE status='open'"
    ).fetchone()[0]

    if open_count == 0:
        return PASS, "No open positions — ExposureManager state N/A", []
    if exp_count == 0:
        return WARN, (
            f"exposure_v1_1 is empty but {open_count} position(s) are open "
            "— ExposureManager may not be wired"
        ), []
    return PASS, f"ExposureManager has {exp_count} record(s) for {open_count} open position(s)", []


CHECKS = [
    ("Premature TP matched", check_premature_matched_tp),
    ("TP overfilled", check_tp_overfilled),
    ("Orphaned TP orders", check_orphaned_tp_orders),
    ("Positions without TP", check_open_positions_without_tp),
    ("Duplicate resting orders", check_duplicate_resting_orders),
    ("Duplicate open positions", check_duplicate_open_positions),
    ("ExposureManager wired", check_exposure_manager_wired),
]


def run_checks(db_path: Path) -> tuple[bool, list[tuple[str, str, str, list]]]:
    """Run all checks. Returns (all_passed, results)."""
    conn = _conn(db_path)
    results = []
    all_passed = True
    for name, fn in CHECKS:
        try:
            status, message, detail = fn(conn)
        except Exception as e:
            status, message, detail = FAIL, f"Error: {e}", []
        if status != PASS:
            all_passed = False
        results.append((name, status, message, detail))
    conn.close()
    return all_passed, results


def format_report(results: list[tuple[str, str, str, list]], db_path: Path) -> str:
    lines = [f"Execution health check — {db_path}"]
    lines.append("=" * 60)
    for name, status, message, detail in results:
        lines.append(f"[{status:4s}] {name}: {message}")
        lines.extend(detail)
    lines.append("=" * 60)
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Execution layer health check")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--telegram", action="store_true", help="Send to Telegram if any failures")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"DB not found: {args.db}")
        sys.exit(1)

    all_passed, results = run_checks(args.db)
    report = format_report(results, args.db)
    print(report)

    if not all_passed and args.telegram:
        from utils.telegram import send_message
        lines = ["<b>⚠️ Execution health check FAILED</b>"]
        for name, status, message, detail in results:
            if status != PASS:
                lines.append(f"[{status}] {name}: {message}")
                lines.extend(detail)
        send_message("\n".join(lines))

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
