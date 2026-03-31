"""
Feedback Loop — analyze ml_outcomes and write feedback_penalties to dataset DB.

Computes per-(category, vol_bucket) penalty multipliers from observed fill and
win rates. The MarketScorer loads these penalties at init time and applies them
to market scores so that cohorts with a proven poor track record are down-ranked.

Penalty logic:
    empty_rate > 0.95 AND avg_roi < 0  →  penalty = 0.50
    filled_loser_rate > 0.80           →  penalty = 0.60
    otherwise                          →  penalty = 1.00  (no change)

Note: requires at least 10 candidates per cohort (HAVING clause). With < 2 weeks
of paper trading the table will be mostly empty — that is expected. The scorer
falls back to penalty=1.0 for unseen cohorts.

Usage:
    python scripts/analyze_empty_candidates.py
    python scripts/analyze_empty_candidates.py \\
        --positions-db /path/to/positions.db \\
        --dataset-db   /path/to/polymarket_dataset.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from utils.paths import DATA_DIR, DB_PATH

logger = setup_logger("feedback_penalties")

POSITIONS_DB = DATA_DIR / "positions.db"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS feedback_penalties (
    category            TEXT NOT NULL,
    vol_bucket          TEXT NOT NULL,
    total_candidates    INTEGER,
    empty_rate          REAL,
    filled_loser_rate   REAL,
    avg_roi             REAL,
    penalty             REAL NOT NULL DEFAULT 1.0,
    computed_at         INTEGER,
    PRIMARY KEY (category, vol_bucket)
)
"""

_ANALYSIS_QUERY = """
SELECT
    COALESCE(category, 'null') AS category,
    CASE
        WHEN volume_usdc < 10000     THEN '<10k'
        WHEN volume_usdc < 100000    THEN '10k-100k'
        WHEN volume_usdc < 1000000   THEN '100k-1M'
        ELSE                              '>1M'
    END AS vol_bucket,
    COUNT(*)                                                           AS total_candidates,
    SUM(CASE WHEN got_fill = 0 THEN 1 ELSE 0 END)                     AS empty_count,
    ROUND(100.0 * SUM(CASE WHEN got_fill = 0 THEN 1 ELSE 0 END)
          / COUNT(*), 1)                                               AS empty_rate_pct,
    SUM(CASE WHEN got_fill = 1 AND is_winner = 0 THEN 1 ELSE 0 END)   AS filled_losers,
    SUM(got_fill)                                                      AS fills,
    ROUND(AVG(CASE WHEN got_fill = 1 THEN realized_roi END), 6)        AS avg_roi
FROM ml_outcomes
GROUP BY category, vol_bucket
HAVING total_candidates >= 10
ORDER BY empty_rate_pct DESC
"""


def compute_penalty(empty_rate: float, filled_loser_rate: float, avg_roi) -> float:
    if empty_rate > 0.95 and (avg_roi is None or avg_roi < 0):
        return 0.50
    if filled_loser_rate > 0.80:
        return 0.60
    return 1.00


def build(positions_db: str, dataset_db: str) -> int:
    """Compute feedback_penalties and write to dataset_db. Returns rows written."""
    pos_conn = sqlite3.connect(positions_db)
    pos_conn.row_factory = sqlite3.Row

    # Check ml_outcomes exists and has data
    try:
        total = pos_conn.execute("SELECT COUNT(*) FROM ml_outcomes").fetchone()[0]
    except Exception:
        logger.warning("ml_outcomes table not found or empty — skipping feedback penalties")
        pos_conn.close()
        return 0

    if total == 0:
        logger.info("ml_outcomes is empty — feedback_penalties not updated (need 2+ weeks of data)")
        pos_conn.close()
        return 0

    rows = pos_conn.execute(_ANALYSIS_QUERY).fetchall()
    pos_conn.close()

    if not rows:
        logger.info(f"ml_outcomes has {total} rows but no cohort reached min_samples=10")
        return 0

    ds_conn = sqlite3.connect(dataset_db)
    ds_conn.execute("PRAGMA journal_mode=WAL")
    ds_conn.execute(_CREATE_TABLE)

    now = int(time.time())
    written = 0
    for r in rows:
        category  = r["category"]
        vol_bucket = r["vol_bucket"]
        total_c    = r["total_candidates"]
        empty_rate = (r["empty_count"] or 0) / max(total_c, 1)
        fills      = r["fills"] or 0
        filled_loser_rate = (r["filled_losers"] or 0) / max(fills, 1) if fills > 0 else 0.0
        avg_roi    = r["avg_roi"]
        penalty    = compute_penalty(empty_rate, filled_loser_rate, avg_roi)

        ds_conn.execute("""
            INSERT INTO feedback_penalties
                (category, vol_bucket, total_candidates, empty_rate,
                 filled_loser_rate, avg_roi, penalty, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(category, vol_bucket) DO UPDATE SET
                total_candidates  = excluded.total_candidates,
                empty_rate        = excluded.empty_rate,
                filled_loser_rate = excluded.filled_loser_rate,
                avg_roi           = excluded.avg_roi,
                penalty           = excluded.penalty,
                computed_at       = excluded.computed_at
        """, (category, vol_bucket, total_c, empty_rate, filled_loser_rate, avg_roi, penalty, now))

        logger.debug(
            f"  {category:<14} {vol_bucket:<10} "
            f"n={total_c} empty={empty_rate:.1%} "
            f"loser_rate={filled_loser_rate:.1%} avg_roi={avg_roi} "
            f"→ penalty={penalty}"
        )
        written += 1

    ds_conn.commit()
    ds_conn.close()
    logger.info(f"feedback_penalties: {written} cohorts written (from {total} ml_outcomes rows)")
    return written


def main() -> None:
    ap = argparse.ArgumentParser(description="Build feedback_penalties from ml_outcomes")
    ap.add_argument("--positions-db", default=str(POSITIONS_DB))
    ap.add_argument("--dataset-db",   default=str(DB_PATH))
    args = ap.parse_args()

    n = build(args.positions_db, args.dataset_db)
    if n == 0:
        sys.exit(0)  # not an error — just no data yet

    # Print the table
    conn = sqlite3.connect(args.dataset_db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT category, vol_bucket, total_candidates, empty_rate,
               filled_loser_rate, avg_roi, penalty
        FROM feedback_penalties
        ORDER BY penalty ASC, empty_rate DESC
    """).fetchall()
    conn.close()

    print(f"\n{'─'*80}")
    print("  Feedback Penalties")
    print(f"{'─'*80}")
    print(f"  {'category':<14} {'vol':>9} {'n':>6} {'empty%':>8} {'loser%':>8} {'avg_roi':>10} {'penalty':>8}")
    for r in rows:
        print(
            f"  {r['category']:<14} {r['vol_bucket']:>9} {r['total_candidates']:>6} "
            f"{r['empty_rate']:>7.1%} {r['filled_loser_rate']:>7.1%} "
            f"{(r['avg_roi'] or 0):>10.4f} {r['penalty']:>8.2f}"
        )


if __name__ == "__main__":
    main()
