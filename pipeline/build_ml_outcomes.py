"""
Build ML Outcomes — materializes a training-ready dataset from the operational DB.

Links the full lifecycle:
  screener_log (candidate features)
  → resting_orders (entry level, order_id)   via candidate_id
  → positions (fill status, PnL, resolution)  via entry_order_id = resting_orders.order_id

Output: ml_outcomes table in positions.db  (UPSERT — safe to re-run any time).

Usage:
    # Full rebuild (drop + recreate)
    python pipeline/build_ml_outcomes.py --rebuild

    # Incremental update (default, adds/updates rows)
    python pipeline/build_ml_outcomes.py

    # Print summary after build
    python pipeline/build_ml_outcomes.py --summary

    # Use a custom DB path
    python pipeline/build_ml_outcomes.py --db /path/to/positions.db

Only candidates that reached resting_orders are included (got a placement attempt).
Candidates rejected upstream (no entry_level) are not included — use screener_log for those.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from utils.paths import DATA_DIR

logger = setup_logger("build_ml_outcomes")

POSITIONS_DB = DATA_DIR / "positions.db"

_BUILD_QUERY = """
SELECT
    sl.candidate_id,
    sl.market_id,
    sl.token_id,
    sl.question,
    sl.category,
    sl.current_price,
    sl.hours_to_close,
    sl.volume_usdc,
    sl.ef_score,
    sl.res_score,
    r.price                                     AS entry_level,
    r.order_id,
    r.created_at                                AS order_created_at,

    CASE WHEN p.position_id IS NOT NULL
         THEN 1 ELSE 0 END                      AS got_fill,
    p.is_winner,
    p.realized_pnl,
    CASE WHEN p.entry_size_usdc > 0
         THEN ROUND(p.realized_pnl / p.entry_size_usdc, 6)
         ELSE NULL END                          AS realized_roi,
    CASE WHEN p.opened_at IS NOT NULL AND r.created_at > 0
         THEN ROUND((p.opened_at - r.created_at) / 3600.0, 4)
         ELSE NULL END                          AS time_to_fill_hours,
    CASE WHEN EXISTS (
             SELECT 1 FROM tp_orders tp
             WHERE tp.position_id = p.position_id
               AND tp.status = 'matched'
               AND p.entry_price > 0
               AND tp.sell_price >= p.entry_price * 5
         ) THEN 1 ELSE 0 END                    AS tp_5x_hit,
    CASE WHEN EXISTS (
             SELECT 1 FROM tp_orders tp
             WHERE tp.position_id = p.position_id
               AND tp.status = 'matched'
               AND p.entry_price > 0
               AND tp.sell_price >= p.entry_price * 10
         ) THEN 1 ELSE 0 END                    AS tp_10x_hit,
    CASE WHEN EXISTS (
             SELECT 1 FROM tp_orders tp
             WHERE tp.position_id = p.position_id
               AND tp.status = 'matched'
               AND p.entry_price > 0
               AND tp.sell_price >= p.entry_price * 20
         ) THEN 1 ELSE 0 END                    AS tp_20x_hit,
    CASE WHEN EXISTS (
             SELECT 1 FROM tp_orders tp
             WHERE tp.position_id = p.position_id
               AND tp.label = 'moonbag_resolution'
               AND tp.status IN ('matched', 'resolved')
               AND p.is_winner = 1
         ) THEN 1 ELSE 0 END                    AS tp_moonbag_hit,
    p.peak_price,
    p.peak_x,

    p.entry_price,
    p.entry_size_usdc,
    p.status                                    AS position_status,
    p.opened_at,
    p.closed_at

FROM screener_log sl
JOIN resting_orders r ON r.candidate_id = sl.candidate_id
LEFT JOIN positions p ON p.entry_order_id = r.order_id
WHERE sl.outcome = 'passed_to_order_manager'
  AND sl.candidate_id IS NOT NULL
"""

_UPSERT = """
INSERT INTO ml_outcomes (
    materialized_at, candidate_id, market_id, token_id, question, category,
    current_price, hours_to_close, volume_usdc, ef_score, res_score,
    entry_level, order_id,
    got_fill, is_winner, realized_pnl, realized_roi, time_to_fill_hours,
    tp_5x_hit, tp_10x_hit, tp_20x_hit, tp_moonbag_hit,
    peak_price, peak_x,
    entry_price, entry_size_usdc, position_status, opened_at, closed_at
)
VALUES (
    ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?,
    ?, ?,
    ?, ?, ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?,
    ?, ?, ?, ?, ?
)
ON CONFLICT(candidate_id, entry_level) DO UPDATE SET
    materialized_at    = excluded.materialized_at,
    got_fill           = excluded.got_fill,
    is_winner          = excluded.is_winner,
    realized_pnl       = excluded.realized_pnl,
    realized_roi       = excluded.realized_roi,
    time_to_fill_hours = excluded.time_to_fill_hours,
    tp_5x_hit          = excluded.tp_5x_hit,
    tp_10x_hit         = excluded.tp_10x_hit,
    tp_20x_hit         = excluded.tp_20x_hit,
    tp_moonbag_hit     = excluded.tp_moonbag_hit,
    peak_price         = excluded.peak_price,
    peak_x             = excluded.peak_x,
    entry_price        = excluded.entry_price,
    entry_size_usdc    = excluded.entry_size_usdc,
    position_status    = excluded.position_status,
    opened_at          = excluded.opened_at,
    closed_at          = excluded.closed_at
"""


def build(conn: sqlite3.Connection, rebuild: bool = False) -> int:
    """
    Materialize ml_outcomes from the linked operational tables.
    Returns number of rows upserted.
    """
    if rebuild:
        conn.execute("DELETE FROM ml_outcomes")
        conn.commit()
        logger.info("ml_outcomes cleared for full rebuild")

    now = int(time.time())
    rows = conn.execute(_BUILD_QUERY).fetchall()
    if not rows:
        logger.info("No candidates with resting orders found — ml_outcomes unchanged")
        return 0

    params = [
        (
            now,
            r["candidate_id"],
            r["market_id"],
            r["token_id"],
            r["question"],
            r["category"],
            r["current_price"],
            r["hours_to_close"],
            r["volume_usdc"],
            r["ef_score"],
            r["res_score"],
            r["entry_level"],
            r["order_id"],
            r["got_fill"],
            r["is_winner"],
            r["realized_pnl"],
            r["realized_roi"],
            r["time_to_fill_hours"],
            r["tp_5x_hit"],
            r["tp_10x_hit"],
            r["tp_20x_hit"],
            r["tp_moonbag_hit"],
            r["peak_price"],
            r["peak_x"],
            r["entry_price"],
            r["entry_size_usdc"],
            r["position_status"],
            r["opened_at"],
            r["closed_at"],
        )
        for r in rows
    ]
    conn.executemany(_UPSERT, params)
    conn.commit()
    logger.info(f"ml_outcomes: {len(params)} rows upserted")
    return len(params)


def print_summary(conn: sqlite3.Connection) -> None:
    total = conn.execute("SELECT COUNT(*) FROM ml_outcomes").fetchone()[0]
    if total == 0:
        print("ml_outcomes: empty")
        return

    stats = conn.execute("""
        SELECT
            COUNT(*)                                           AS total,
            SUM(got_fill)                                      AS fills,
            ROUND(100.0 * SUM(got_fill) / COUNT(*), 1)        AS fill_rate_pct,
            SUM(CASE WHEN is_winner = 1 THEN 1 ELSE 0 END)    AS winners,
            SUM(CASE WHEN position_status = 'resolved' THEN 1 ELSE 0 END) AS resolved,
            ROUND(SUM(COALESCE(realized_pnl, 0)), 4)          AS total_pnl,
            ROUND(AVG(CASE WHEN time_to_fill_hours IS NOT NULL
                           THEN time_to_fill_hours END), 2)   AS avg_fill_hours,
            SUM(CASE WHEN tp_5x_hit = 1 THEN 1 ELSE 0 END)   AS tp5_hits
        FROM ml_outcomes
    """).fetchone()

    print(f"\n{'─'*48}")
    print(f"  ml_outcomes summary")
    print(f"{'─'*48}")
    print(f"  Total rows (candidate×level): {stats[0]}")
    print(f"  Got fill:      {stats[1]}  ({stats[2]}%)")
    print(f"  Winners:       {stats[3]}")
    print(f"  Resolved:      {stats[4]}")
    print(f"  Realized PnL:  ${stats[5]:.4f}")
    print(f"  Avg fill time: {stats[6]} h")
    print(f"  TP 5x hits:    {stats[7]}")
    print(f"{'─'*48}\n")

    # By category
    cats = conn.execute("""
        SELECT category,
               COUNT(*) AS n,
               SUM(got_fill) AS fills,
               ROUND(100.0 * SUM(got_fill) / COUNT(*), 1) AS fill_pct,
               ROUND(SUM(COALESCE(realized_pnl, 0)), 4) AS pnl
        FROM ml_outcomes
        GROUP BY category
        ORDER BY n DESC
    """).fetchall()
    if cats:
        print("  By category:")
        for c in cats:
            print(f"    {(c[0] or 'unknown'):<16} n={c[1]}  fill={c[3]}%  pnl=${c[4]:.4f}")
        print()


def main() -> None:
    ap = argparse.ArgumentParser(description="Build ML outcomes from operational DB")
    ap.add_argument("--db", default=str(POSITIONS_DB), help="Path to positions.db")
    ap.add_argument("--rebuild", action="store_true",
                    help="Clear ml_outcomes and rebuild from scratch")
    ap.add_argument("--summary", action="store_true",
                    help="Print summary after build")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    upserted = build(conn, rebuild=args.rebuild)

    if args.summary or upserted > 0:
        print_summary(conn)

    conn.close()


if __name__ == "__main__":
    main()
