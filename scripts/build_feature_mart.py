"""
Build Feature Mart — denormalized training table for big_swan_mode scorer.

Joins: markets + tokens + token_swans (Dec+Jan+Feb window only)
Output: table `feature_mart` in polymarket_dataset.db

Run:
    python scripts/build_feature_mart.py
    python scripts/build_feature_mart.py --recompute

Working window: strictly Dec 2025 + Jan 2026 + Feb 2026. March rows are excluded.

Features (per ТЗ section F):
  market metadata:   category, volume, duration_hours, fees_enabled, comment_count
  raw trade:         entry_min_price, entry_volume_usdc, entry_trade_count, floor_duration_seconds
  liquidity:         avg_bid_depth (future), entry_volume_usdc as proxy
  temporal:          time_to_resolution_hours, entry_hour_of_day
  category/pattern:  category, is_neg_risk, cyom

Scoring (issue #5 approved design):
  price_score        (0–4): based on entry_min_price bucket
  neglect_score      (0–3): based on entry_trade_count bucket (inverse: fewer = better)
  freshness_score    (0–2): based on floor_duration_seconds (inverse: fresher = better)
  swan_score         (0–9): price_score + neglect_score + freshness_score
  NULL for negatives (no swan event detected)

Labels:
  tp_5x_hit, tp_10x_hit, tp_20x_hit
  is_winner
  real_x   (= token_swans.possible_x for positives, 1.0 for negatives)
  tail_bucket (0: <5x, 1: 5–20x, 2: 20–50x, 3: 50–100x, 4: 100x+)
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from utils.paths import DB_PATH

logger = setup_logger("build_feature_mart")

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS feature_mart (
    -- Primary keys
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id                TEXT NOT NULL,
    market_id               TEXT NOT NULL,
    date                    TEXT NOT NULL,

    -- ── Market metadata features ────────────────────────────────────────────
    category                TEXT,
    volume_usdc             REAL,
    liquidity_usdc          REAL,
    duration_hours          REAL,
    comment_count           INTEGER,
    fees_enabled            INTEGER,
    neg_risk                INTEGER,
    cyom                    INTEGER,
    -- log-transformed for ML
    log_volume              REAL,
    log_liquidity           REAL,

    -- ── Raw-trade-derived features ───────────────────────────────────────────
    entry_min_price         REAL,
    entry_volume_usdc       REAL,
    entry_trade_count       INTEGER,
    -- duration the price stayed in the floor zone
    floor_duration_seconds  REAL,
    -- derived from entry zone metrics
    avg_trade_size_usdc     REAL,

    -- ── Temporal features ────────────────────────────────────────────────────
    -- hours from first floor trade to market close
    time_to_resolution_hours REAL,
    -- day of week when floor zone started (0=Mon, 6=Sun)
    entry_dow               INTEGER,
    -- hour of day (UTC) when floor zone started
    entry_hour_utc          INTEGER,

    -- ── Category/pattern features ────────────────────────────────────────────
    -- binary flags for known high-EV categories
    is_sports               INTEGER,
    is_crypto               INTEGER,
    is_politics             INTEGER,
    is_geopolitics          INTEGER,

    -- ── Scoring (issue #5 approved design) ──────────────────────────────────
    -- NULL for negatives (no swan event); 0–N for positives
    price_score             INTEGER,   -- 0–4: entry_min_price bucket
    neglect_score           INTEGER,   -- 0–3: entry_trade_count bucket (inverse)
    freshness_score         INTEGER,   -- 0–2: floor_duration_seconds bucket (inverse)
    swan_score              INTEGER,   -- 0–9: price + neglect + freshness

    -- ── Labels ───────────────────────────────────────────────────────────────
    -- Classification
    tp_5x_hit               INTEGER,   -- 1 if real_x >= 5
    tp_10x_hit              INTEGER,   -- 1 if real_x >= 10
    tp_20x_hit              INTEGER,   -- 1 if real_x >= 20
    is_winner               INTEGER,   -- 1 if token resolved at $1

    -- Regression
    real_x                  REAL,      -- possible_x for positives, 1.0 for negatives
    resolution_x            REAL,

    -- Ranking (ordinal bucket for tail)
    -- 0: <5x, 1: 5-20x, 2: 20-50x, 3: 50-100x, 4: 100x+
    tail_bucket             INTEGER,

    UNIQUE(token_id, date)
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fm_category ON feature_mart(category)",
    "CREATE INDEX IF NOT EXISTS idx_fm_tail_bucket ON feature_mart(tail_bucket)",
    "CREATE INDEX IF NOT EXISTS idx_fm_is_winner ON feature_mart(is_winner)",
    "CREATE INDEX IF NOT EXISTS idx_fm_real_x ON feature_mart(real_x)",
    "CREATE INDEX IF NOT EXISTS idx_fm_date ON feature_mart(date)",
]


def _tail_bucket(real_x: float) -> int:
    if real_x < 5:
        return 0
    if real_x < 20:
        return 1
    if real_x < 50:
        return 2
    if real_x < 100:
        return 3
    return 4


def build(conn: sqlite3.Connection, recompute: bool = False) -> None:
    if recompute:
        conn.execute("DROP TABLE IF EXISTS feature_mart")
        logger.info("Dropped existing feature_mart")

    conn.execute(CREATE_TABLE)
    for idx in CREATE_INDEXES:
        conn.execute(idx)
    conn.commit()

    # ── Candidate universe: screener-gated resolved tokens ────────────────────
    # We approximate the "could have been a bot candidate" universe using the
    # same volume thresholds the live screener applies, so that negatives are
    # realistic missed opportunities rather than arbitrary resolved tokens.
    #
    # Positive: token has a matching swans_v2 row → real dip + measurable outcome
    # Negative: token passed volume gate, resolved, but no swan event detected
    #           → real_x = 1.0, all tp_*_hit = 0, tail_bucket = 0
    #
    # We cannot apply a price gate here because historical per-token prices are
    # not stored in markets/tokens — that would require raw trades. Volume gating
    # is sufficient to exclude obviously-irrelevant markets (huge liquid markets
    # and trivially-thin zero-activity markets).
    SCREENER_MIN_VOLUME = 50.0      # same as BotConfig.min_volume_usdc
    SCREENER_MAX_VOLUME = 50_000.0  # same as BotConfig.max_volume_usdc

    # Working window: strictly Dec 2025 + Jan 2026 + Feb 2026
    WINDOW_MONTHS = ('2025-12', '2026-01', '2026-02')

    logger.info("Loading candidate universe (screener-gated, Dec+Jan+Feb): tokens LEFT JOIN token_swans ...")
    rows = conn.execute("""
        SELECT
            t.token_id,
            t.market_id,
            COALESCE(s.date, DATE(m.closed_time, 'unixepoch')) AS date,
            m.category,
            m.volume,
            m.liquidity,
            m.duration_hours,
            m.comment_count,
            m.fees_enabled,
            m.neg_risk,
            m.cyom,
            m.closed_time,

            s.entry_min_price,
            s.entry_volume_usdc,
            s.entry_trade_count,
            s.entry_ts_first,
            s.entry_ts_last,
            s.duration_entry_zone_seconds,
            t.is_winner                        AS is_winner,
            COALESCE(s.possible_x, 1.0)        AS real_x,
            COALESCE(s.possible_x, 1.0)        AS resolution_x
        FROM tokens t
        JOIN markets m ON t.market_id = m.id
        LEFT JOIN token_swans s ON s.token_id = t.token_id
        WHERE m.closed_time > 0
          AND m.volume >= :min_vol
          AND m.volume <= :max_vol
          AND m.duration_hours > 0
          AND substr(COALESCE(s.date, DATE(m.closed_time, 'unixepoch')), 1, 7)
              IN ('2025-12', '2026-01', '2026-02')
    """, {"min_vol": SCREENER_MIN_VOLUME, "max_vol": SCREENER_MAX_VOLUME}).fetchall()

    total = len(rows)
    positives = sum(1 for r in rows if r[12] is not None)  # entry_min_price not NULL = swan positive
    months = set(r[2][:7] for r in rows if r[2])
    logger.info(f"Date window covered: {sorted(months)}")
    logger.info(
        f"Processing {total} candidate rows "
        f"({positives} positives, {total - positives} negatives) "
        f"[vol {SCREENER_MIN_VOLUME}–{SCREENER_MAX_VOLUME} USDC gate applied]"
    )
    t0 = time.monotonic()
    inserted = updated = 0

    import math

    for row in rows:
        try:
            token_id = row[0]
            market_id = row[1]
            date = row[2]
            category = row[3]
            volume = float(row[4] or 0)
            liquidity = float(row[5] or 0)
            duration_hours = float(row[6] or 0)
            comment_count = int(row[7] or 0)
            fees_enabled = int(row[8] or 0)
            neg_risk = int(row[9] or 0)
            cyom = int(row[10] or 0)
            closed_time = int(row[11] or 0)

            # Swan-derived features are NULL for negatives (no floor event)
            entry_min_price = float(row[12]) if row[12] is not None else None
            entry_volume_usdc = float(row[13] or 0)
            entry_trade_count = int(row[14] or 0)
            entry_ts_first = int(row[15] or 0)
            entry_ts_last = int(row[16] or 0)
            duration_entry_zone_stored = row[17]  # from token_swans directly
            is_winner = int(row[18] or 0)
            real_x = float(row[19] or 1.0)
            resolution_x = float(row[20] or real_x)

            # Log-transforms
            log_volume = math.log1p(volume)
            log_liquidity = math.log1p(liquidity)

            # Floor duration — use stored column if available, fallback to ts diff
            if duration_entry_zone_stored is not None:
                floor_duration_seconds = float(duration_entry_zone_stored)
            else:
                floor_duration_seconds = max(0.0, entry_ts_last - entry_ts_first)

            # ── Swan score (issue #5) — NULL for negatives ─────────────────────
            if entry_min_price is not None:
                if entry_min_price <= 0.01:
                    price_score = 4
                elif entry_min_price <= 0.02:
                    price_score = 3
                elif entry_min_price <= 0.03:
                    price_score = 2
                elif entry_min_price <= 0.04:
                    price_score = 1
                else:
                    price_score = 0

                if entry_trade_count <= 10:
                    neglect_score = 3
                elif entry_trade_count <= 30:
                    neglect_score = 2
                elif entry_trade_count <= 100:
                    neglect_score = 1
                else:
                    neglect_score = 0

                if floor_duration_seconds < 3600:
                    freshness_score = 2
                elif floor_duration_seconds < 86400:
                    freshness_score = 1
                else:
                    freshness_score = 0

                swan_score = price_score + neglect_score + freshness_score
            else:
                price_score = neglect_score = freshness_score = swan_score = None

            # Avg trade size at floor
            avg_trade_size = entry_volume_usdc / max(entry_trade_count, 1)

            # Time to resolution from floor entry
            time_to_resolution_hours = (
                (closed_time - entry_ts_first) / 3600.0
                if closed_time > entry_ts_first > 0
                else None
            )

            # Temporal features
            import datetime
            entry_dow = None
            entry_hour_utc = None
            if entry_ts_first > 0:
                dt = datetime.datetime.utcfromtimestamp(entry_ts_first)
                entry_dow = dt.weekday()
                entry_hour_utc = dt.hour

            # Category binary flags
            cat = (category or "").lower()
            is_sports = 1 if cat in ("sports", "esports") else 0
            is_crypto = 1 if cat == "crypto" else 0
            is_politics = 1 if cat in ("politics", "geopolitics") else 0
            is_geopolitics = 1 if cat == "geopolitics" else 0

            # Labels
            tp_5x_hit = 1 if real_x >= 5.0 else 0
            tp_10x_hit = 1 if real_x >= 10.0 else 0
            tp_20x_hit = 1 if real_x >= 20.0 else 0
            bucket = _tail_bucket(real_x)

            conn.execute(
                """
                INSERT INTO feature_mart (
                    token_id, market_id, date,
                    category, volume_usdc, liquidity_usdc, duration_hours,
                    comment_count, fees_enabled, neg_risk, cyom,
                    log_volume, log_liquidity,
                    entry_min_price, entry_volume_usdc, entry_trade_count,
                    floor_duration_seconds, avg_trade_size_usdc,
                    time_to_resolution_hours, entry_dow, entry_hour_utc,
                    is_sports, is_crypto, is_politics, is_geopolitics,
                    price_score, neglect_score, freshness_score, swan_score,
                    tp_5x_hit, tp_10x_hit, tp_20x_hit, is_winner,
                    real_x, resolution_x, tail_bucket
                ) VALUES (
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?
                ) ON CONFLICT(token_id, date) DO UPDATE SET
                    real_x=excluded.real_x,
                    is_winner=excluded.is_winner,
                    resolution_x=excluded.resolution_x,
                    tail_bucket=excluded.tail_bucket,
                    tp_5x_hit=excluded.tp_5x_hit,
                    tp_10x_hit=excluded.tp_10x_hit,
                    tp_20x_hit=excluded.tp_20x_hit,
                    price_score=excluded.price_score,
                    neglect_score=excluded.neglect_score,
                    freshness_score=excluded.freshness_score,
                    swan_score=excluded.swan_score
                """,
                (
                    token_id, market_id, date,
                    category, volume, liquidity, duration_hours,
                    comment_count, fees_enabled, neg_risk, cyom,
                    log_volume, log_liquidity,
                    entry_min_price, entry_volume_usdc, entry_trade_count,
                    floor_duration_seconds, avg_trade_size,
                    time_to_resolution_hours, entry_dow, entry_hour_utc,
                    is_sports, is_crypto, is_politics, is_geopolitics,
                    price_score, neglect_score, freshness_score, swan_score,
                    tp_5x_hit, tp_10x_hit, tp_20x_hit, is_winner,
                    real_x, resolution_x, bucket,
                ),
            )
            inserted += 1
        except Exception as e:
            logger.warning(f"Row error {row[0]}: {e}")
            continue

        if inserted % 1000 == 0:
            conn.commit()

    conn.commit()
    elapsed = int(time.monotonic() - t0)

    # Summary stats
    stats = conn.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(is_winner) AS winners,
            ROUND(AVG(real_x), 2) AS avg_real_x,
            MAX(real_x) AS max_real_x,
            SUM(CASE WHEN tail_bucket >= 2 THEN 1 ELSE 0 END) AS cnt_20x_plus,
            SUM(CASE WHEN tail_bucket >= 3 THEN 1 ELSE 0 END) AS cnt_50x_plus,
            SUM(CASE WHEN tail_bucket >= 4 THEN 1 ELSE 0 END) AS cnt_100x_plus
        FROM feature_mart
    """).fetchone()

    logger.info(
        f"Feature mart built in {elapsed}s | "
        f"rows={stats[0]} winners={stats[1]} "
        f"avg_x={stats[2]} max_x={stats[3]:.0f} "
        f"20x+={stats[4]} 50x+={stats[5]} 100x+={stats[6]}"
    )
    logger.info("Label distribution by tail_bucket:")
    dist = conn.execute(
        "SELECT tail_bucket, COUNT(*) FROM feature_mart GROUP BY tail_bucket ORDER BY tail_bucket"
    ).fetchall()
    for bucket, cnt in dist:
        labels = ["<5x", "5-20x", "20-50x", "50-100x", "100x+"]
        label = labels[bucket] if bucket < len(labels) else f"bucket_{bucket}"
        logger.info(f"  {label}: {cnt} rows ({100*cnt/max(stats[0],1):.1f}%)")

    # Sanity check: label distributions must not be degenerate
    tp5_cnt = conn.execute("SELECT SUM(tp_5x_hit) FROM feature_mart").fetchone()[0] or 0
    tb0_cnt = conn.execute("SELECT COUNT(*) FROM feature_mart WHERE tail_bucket=0").fetchone()[0] or 0
    if tp5_cnt == stats[0]:
        logger.warning("LABEL SANITY: tp_5x_hit is constant 1 — negatives may be missing")
    if tb0_cnt == 0:
        logger.warning("LABEL SANITY: tail_bucket=0 is absent — negatives may be missing")
    logger.info(
        f"Label sanity: tp_5x_hit={tp5_cnt}/{stats[0]} "
        f"tail_bucket_0={tb0_cnt}/{stats[0]}"
    )

    # ── Score cohort validation ────────────────────────────────────────────────
    logger.info("Score cohort validation (positives only):")
    cohorts = conn.execute("""
        SELECT
            CASE
                WHEN swan_score >= 7 THEN 'high  (7-9)'
                WHEN swan_score >= 4 THEN 'mid   (4-6)'
                ELSE                      'low   (0-3)'
            END AS cohort,
            swan_score,
            COUNT(*) AS n,
            ROUND(AVG(real_x), 1) AS avg_x,
            ROUND(100.0 * SUM(CASE WHEN real_x >= 20 THEN 1 ELSE 0 END) / COUNT(*), 1) AS tail_pct
        FROM feature_mart
        WHERE swan_score IS NOT NULL
        GROUP BY cohort
        ORDER BY MIN(swan_score) DESC
    """).fetchall()
    for c in cohorts:
        logger.info(f"  {c[0]}: n={c[2]}, avg_x={c[3]}, tail%={c[4]}%")

    score_dist = conn.execute("""
        SELECT swan_score, COUNT(*) FROM feature_mart
        WHERE swan_score IS NOT NULL
        GROUP BY swan_score ORDER BY swan_score DESC
    """).fetchall()
    dist_str = "  ".join(f"{s}:{n}" for s, n in score_dist)
    logger.info(f"Score distribution (score:count): {dist_str}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build feature mart for big_swan_mode training")
    ap.add_argument("--recompute", action="store_true", help="Drop and rebuild from scratch")
    ap.add_argument("--db", default=str(DB_PATH), help="SQLite DB path")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    build(conn, recompute=args.recompute)
    conn.close()


if __name__ == "__main__":
    main()
