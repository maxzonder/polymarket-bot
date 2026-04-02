"""
Build Rejected Outcomes — post-hoc labels for markets the screener rejected.

Joins:
  screener_log (positions.db)  — features and rejection reason at scan time
  swans_v2     (dataset DB)    — post-hoc ground truth: did a floor event happen?
  tokens       (dataset DB)    — is_winner

Output: ml_rejected_outcomes table in dataset DB.
One row per (market_id, token_id) — aggregated from multiple scans.

Labels are populated once the analyzer has processed the closed market.
Until then, had_swan_event=0 (NULL token_id rows) — re-run daily to fill them in.

Usage:
    python pipeline/build_rejected_outcomes.py
    python pipeline/build_rejected_outcomes.py --rebuild
    python pipeline/build_rejected_outcomes.py --summary
    python pipeline/build_rejected_outcomes.py \\
        --positions-db /path/to/positions.db \\
        --dataset-db   /path/to/polymarket_dataset.db
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from utils.paths import DATA_DIR, DB_PATH
from config import SWAN_ENTRY_MAX

logger = setup_logger("build_rejected_outcomes")

POSITIONS_DB = DATA_DIR / "positions.db"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ml_rejected_outcomes (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    materialized_at      INTEGER NOT NULL,

    -- Screener-side features (from screener_log)
    market_id            TEXT    NOT NULL,
    token_id             TEXT,
    question             TEXT,
    category             TEXT,
    rejection_reason     TEXT    NOT NULL,
    first_scanned_at     INTEGER,
    last_scanned_at      INTEGER,
    scan_count           INTEGER NOT NULL DEFAULT 1,
    avg_current_price    REAL,
    avg_hours_to_close   REAL,
    avg_volume_usdc      REAL,
    ef_score             REAL,
    res_score            REAL,

    -- Post-hoc ground truth (populated after analyzer runs on closed market)
    -- NULL = not yet labeled (analyzer hasn't processed this market yet)
    -- 0   = confirmed no swan event
    -- 1   = swan event observed
    had_swan_event       INTEGER,
    entry_min_price      REAL,
    possible_x           REAL,
    is_winner            INTEGER,

    -- Derived label
    -- 1 if had_swan_event AND entry_min_price <= SWAN_ENTRY_MAX (covers all bot entry levels)
    was_missed_opportunity INTEGER NOT NULL DEFAULT 0,

    UNIQUE(market_id, token_id)
);
CREATE INDEX IF NOT EXISTS idx_mro_rejection ON ml_rejected_outcomes(rejection_reason);
CREATE INDEX IF NOT EXISTS idx_mro_category  ON ml_rejected_outcomes(category);
CREATE INDEX IF NOT EXISTS idx_mro_missed    ON ml_rejected_outcomes(was_missed_opportunity);
"""

_UPSERT = """
INSERT INTO ml_rejected_outcomes (
    materialized_at, market_id, token_id, question, category,
    rejection_reason, first_scanned_at, last_scanned_at, scan_count,
    avg_current_price, avg_hours_to_close, avg_volume_usdc, ef_score, res_score,
    had_swan_event, entry_min_price, possible_x, is_winner, was_missed_opportunity
) VALUES (
    ?, ?, ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?
)
ON CONFLICT(market_id, token_id) DO UPDATE SET
    materialized_at       = excluded.materialized_at,
    scan_count            = excluded.scan_count,
    last_scanned_at       = excluded.last_scanned_at,
    avg_current_price     = excluded.avg_current_price,
    avg_hours_to_close    = excluded.avg_hours_to_close,
    avg_volume_usdc       = excluded.avg_volume_usdc,
    ef_score              = excluded.ef_score,
    res_score             = excluded.res_score,
    had_swan_event        = excluded.had_swan_event,
    entry_min_price       = excluded.entry_min_price,
    possible_x            = excluded.possible_x,
    is_winner             = excluded.is_winner,
    was_missed_opportunity = excluded.was_missed_opportunity
"""


def _avg(lst: list) -> float | None:
    return sum(lst) / len(lst) if lst else None


def build(
    ops_db_path: str,
    dataset_db_path: str,
    rebuild: bool = False,
) -> int:
    """
    Build ml_rejected_outcomes.
    Returns number of rows upserted.
    """
    now = int(time.time())

    # ── 1. Load ground-truth lookup tables from dataset DB ────────────────────
    ds_conn = sqlite3.connect(dataset_db_path)
    ds_conn.row_factory = sqlite3.Row

    # Ensure output table exists
    ds_conn.executescript(_CREATE_TABLE)
    ds_conn.commit()

    if rebuild:
        ds_conn.execute("DELETE FROM ml_rejected_outcomes")
        ds_conn.commit()
        logger.info("ml_rejected_outcomes cleared for full rebuild")

    # Load swans_v2 into memory for post-hoc labeling
    ts_rows = ds_conn.execute(
        "SELECT token_id, buy_min_price, max_traded_x FROM swans_v2"
    ).fetchall()
    ts_map: dict[str, dict] = {r["token_id"]: dict(r) for r in ts_rows}
    logger.info(f"Loaded {len(ts_map)} swans_v2 rows for labeling")

    # Load is_winner from tokens table
    tk_rows = ds_conn.execute("SELECT token_id, is_winner FROM tokens").fetchall()
    tk_map: dict[str, int | None] = {r["token_id"]: r["is_winner"] for r in tk_rows}
    logger.info(f"Loaded {len(tk_map)} tokens rows for is_winner")

    # ── 2. Load and aggregate rejected screener_log from live DB ─────────────
    ops_conn = sqlite3.connect(ops_db_path)
    ops_conn.row_factory = sqlite3.Row

    logger.info("Loading rejected screener_log rows...")
    sl_rows = ops_conn.execute("""
        SELECT market_id, token_id, question, category, outcome,
               scanned_at, current_price, hours_to_close, volume_usdc,
               ef_score, res_score
        FROM screener_log
        WHERE outcome != 'passed_to_order_manager'
    """).fetchall()
    ops_conn.close()

    if not sl_rows:
        logger.info("No rejected screener_log rows found")
        ds_conn.close()
        return 0

    logger.info(f"Aggregating {len(sl_rows)} screener_log rows by (market_id, token_id)...")

    # Aggregate per (market_id, token_id)
    groups: dict[tuple, dict] = {}
    for r in sl_rows:
        key = (r["market_id"], r["token_id"])
        if key not in groups:
            groups[key] = {
                "market_id":  r["market_id"],
                "token_id":   r["token_id"],
                "question":   r["question"],
                "category":   r["category"],
                "reasons":    [],
                "timestamps": [],
                "prices":     [],
                "hours":      [],
                "volumes":    [],
                "ef_scores":  [],
                "res_scores": [],
            }
        g = groups[key]
        g["reasons"].append(r["outcome"])
        g["timestamps"].append(r["scanned_at"])
        if r["current_price"]  is not None: g["prices"].append(r["current_price"])
        if r["hours_to_close"] is not None: g["hours"].append(r["hours_to_close"])
        if r["volume_usdc"]    is not None: g["volumes"].append(r["volume_usdc"])
        if r["ef_score"]       is not None: g["ef_scores"].append(r["ef_score"])
        if r["res_score"]      is not None: g["res_scores"].append(r["res_score"])

    # ── 3. Join to ground truth and build upsert params ───────────────────────
    params = []
    for g in groups.values():
        token_id = g["token_id"]
        dominant_reason = Counter(g["reasons"]).most_common(1)[0][0]

        # Post-hoc truth — present only if analyzer has processed this market.
        # had_swan semantics:
        #   1    = swan event confirmed (token is in swans_v2)
        #   0    = confirmed no swan (market resolved, not in swans_v2)
        #   NULL = not yet labeled (market unresolved / analyzer not yet run)
        ts = ts_map.get(token_id) if token_id else None
        is_winner    = tk_map.get(token_id) if token_id else None
        if ts:
            had_swan = 1
        elif is_winner is not None:
            had_swan = 0  # resolved but no swan event
        else:
            had_swan = None  # not yet labeled
        entry_min_p  = ts["buy_min_price"]  if ts else None
        possible_x   = ts["max_traded_x"]   if ts else None

        # Missed opportunity = had swan AND floor was within our entry range
        was_missed = 1 if (had_swan and entry_min_p is not None and entry_min_p <= SWAN_ENTRY_MAX) else 0

        params.append((
            now,
            g["market_id"], token_id, g["question"], g["category"],
            dominant_reason,
            min(g["timestamps"]), max(g["timestamps"]), len(g["timestamps"]),
            _avg(g["prices"]), _avg(g["hours"]), _avg(g["volumes"]),
            _avg(g["ef_scores"]), _avg(g["res_scores"]),
            had_swan, entry_min_p, possible_x, is_winner, was_missed,
        ))

    ds_conn.executemany(_UPSERT, params)
    ds_conn.commit()
    ds_conn.close()

    labeled = sum(1 for p in params if p[15] is not None)  # entry_min_price not None
    missed  = sum(1 for p in params if p[18] == 1)         # was_missed_opportunity
    logger.info(
        f"ml_rejected_outcomes: {len(params)} rows upserted | "
        f"labeled={labeled} | missed_opportunities={missed}"
    )
    return len(params)


def print_summary(dataset_db_path: str) -> None:
    conn = sqlite3.connect(dataset_db_path)
    conn.row_factory = sqlite3.Row

    total = conn.execute("SELECT COUNT(*) FROM ml_rejected_outcomes").fetchone()[0]
    if total == 0:
        print("ml_rejected_outcomes: empty")
        conn.close()
        return

    stats = conn.execute("""
        SELECT
            COUNT(*)                                              AS total,
            SUM(had_swan_event)                                   AS had_swan,
            SUM(was_missed_opportunity)                           AS missed,
            ROUND(100.0 * SUM(had_swan_event)     / COUNT(*), 2) AS swan_rate_pct,
            ROUND(100.0 * SUM(was_missed_opportunity) / COUNT(*), 2) AS miss_rate_pct
        FROM ml_rejected_outcomes
    """).fetchone()

    print(f"\n{'─'*52}")
    print(f"  ml_rejected_outcomes summary")
    print(f"{'─'*52}")
    print(f"  Total rejected (unique market/token): {stats['total']}")
    print(f"  Had swan event:      {stats['had_swan']}  ({stats['swan_rate_pct']}%)")
    print(f"  Missed opportunity:  {stats['missed']}  ({stats['miss_rate_pct']}%)")
    print()

    rows = conn.execute("""
        SELECT rejection_reason,
               COUNT(*)                                              AS n,
               SUM(had_swan_event)                                   AS swans,
               SUM(was_missed_opportunity)                           AS missed,
               ROUND(100.0 * SUM(was_missed_opportunity) / COUNT(*), 1) AS miss_pct
        FROM ml_rejected_outcomes
        GROUP BY rejection_reason
        ORDER BY n DESC
    """).fetchall()

    print("  By rejection reason:")
    for r in rows:
        print(f"    {r['rejection_reason']:<38} n={r['n']:<7} "
              f"missed={r['missed']}  ({r['miss_pct']}%)")

    print()
    rows2 = conn.execute("""
        SELECT category,
               COUNT(*)                                              AS n,
               SUM(had_swan_event)                                   AS swans,
               SUM(was_missed_opportunity)                           AS missed,
               ROUND(100.0 * SUM(was_missed_opportunity) / COUNT(*), 1) AS miss_pct,
               ROUND(AVG(CASE WHEN had_swan_event=1 THEN possible_x END), 1) AS avg_x
        FROM ml_rejected_outcomes
        GROUP BY category
        ORDER BY missed DESC
    """).fetchall()

    print("  By category (missed opportunities):")
    for r in rows2:
        print(f"    {str(r['category']):<16} n={r['n']:<6} "
              f"missed={r['missed']}  ({r['miss_pct']}%)  avg_x={r['avg_x']}")
    print(f"{'─'*52}\n")
    conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Build rejected outcomes labels")
    ap.add_argument("--positions-db", default=str(POSITIONS_DB))
    ap.add_argument("--dataset-db",   default=str(DB_PATH))
    ap.add_argument("--rebuild",  action="store_true",
                    help="Clear table and rebuild from scratch")
    ap.add_argument("--summary",  action="store_true",
                    help="Print summary after build")
    args = ap.parse_args()

    n = build(args.positions_db, args.dataset_db, rebuild=args.rebuild)
    if args.summary or n > 0:
        print_summary(args.dataset_db)


if __name__ == "__main__":
    main()
