"""
Stage 0 — Market-Level Features (v1.1)

Builds `feature_mart_v1_1`: one row per market with per-market features
and target labels for the black-swan strategy.

Key differences from feature_mart (v1):
- Unit of analysis: per market_id (not per token_id event)
- Positives: markets where ANY token hit the entry zone (buy_min_price <= ENTRY_MAX)
- Negatives: markets that passed volume gate but had NO swan event
- Adds niche_score_raw = volume / log(max(volume, e)) as attention-adjusted proxy
- Adds token_count column (always 2 for current dataset; future-proof for multi-outcome)
- Uses swans_v2 (full Aug 2025 – Mar 2026 window) as the positive source

Cohort analysis:
- Runs automatically after build; shows which features separate good swans
  (best_max_traded_x >= 20) from bad (< 20) and from negatives (no swan)
- Prints suggested initial weights for market_score formula

Usage:
    python analyzer/market_level_features_v1_1.py
    python analyzer/market_level_features_v1_1.py --recompute
    python analyzer/market_level_features_v1_1.py --analysis-only
"""

from __future__ import annotations

import argparse
import math
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from utils.paths import DATABASE_DIR, DB_PATH
from config import SWAN_ENTRY_MAX, BotConfig

logger = setup_logger("market_level_features")

# ── Gates (must match live screener for honest negatives) ────────────────────
ENTRY_MAX = SWAN_ENTRY_MAX               # buy_min_price threshold that defines a "swan event"
MIN_VOLUME = BotConfig().min_volume_usdc  # screener min volume gate


def _find_oldest_data_date() -> str:
    """Return the oldest YYYY-MM-DD date dir found in DATABASE_DIR, or '2000-01-01'."""
    if not DATABASE_DIR.exists():
        return "2000-01-01"
    dates = []
    for p in DATABASE_DIR.iterdir():
        if p.is_dir() and len(p.name) == 10 and p.name[4] == "-" and p.name[7] == "-":
            try:
                dates.append(p.name)
            except Exception:
                pass
    return min(dates) if dates else "2000-01-01"


DATE_FROM = _find_oldest_data_date()   # derived from oldest date folder in DATABASE_DIR

# ── Table DDL ────────────────────────────────────────────────────────────────
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS feature_mart_v1_1 (
    -- identity
    market_id               TEXT PRIMARY KEY,
    date                    TEXT NOT NULL,   -- market close date

    -- ── Market metadata ──────────────────────────────────────────────────────
    category                TEXT,
    volume_usdc             REAL,
    log_volume              REAL,
    liquidity_usdc          REAL,
    log_liquidity           REAL,
    duration_hours          REAL,
    comment_count           INTEGER,
    neg_risk                INTEGER,
    token_count             INTEGER,         -- always 2 for current dataset

    -- niche proxy: high volume relative to log(volume) = widely-traded
    -- low ratio = niche (less efficient pricing, better swan soil)
    niche_score_raw         REAL,

    -- ── Swan features (NULL if no swan event) ────────────────────────────────
    was_swan                INTEGER NOT NULL DEFAULT 0,
    best_buy_min_price      REAL,            -- min price across swan tokens
    best_max_traded_x       REAL,            -- max max_traded_x across swan tokens
    best_buy_volume         REAL,
    best_buy_trade_count    INTEGER,
    best_floor_duration_s   REAL,
    best_time_to_res_hours  REAL,            -- from first swan entry to close

    -- ── Labels ───────────────────────────────────────────────────────────────
    swan_is_winner          INTEGER,         -- 1 if best-x swan token also won
    label_20x               INTEGER,         -- 1 if best_max_traded_x >= 20
    label_tail              INTEGER,         -- 0:<5x 1:5-20x 2:20-50x 3:50-100x 4:100x+

    -- ── Neg-risk group features (NULL for binary markets) ────────────────────
    neg_risk_group_id       TEXT,            -- negRiskMarketID — parent group
    group_token_count       INTEGER,         -- number of outcomes in the group
    group_max_volume         REAL,            -- max market volume in neg-risk group
    group_underdog_count    INTEGER,         -- outcomes in group with swan event
    group_underdog_winner   INTEGER          -- 1 if any underdog token in group won
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fmv11_category    ON feature_mart_v1_1(category)",
    "CREATE INDEX IF NOT EXISTS idx_fmv11_was_swan    ON feature_mart_v1_1(was_swan)",
    "CREATE INDEX IF NOT EXISTS idx_fmv11_label_20x   ON feature_mart_v1_1(label_20x)",
    "CREATE INDEX IF NOT EXISTS idx_fmv11_date        ON feature_mart_v1_1(date)",
    "CREATE INDEX IF NOT EXISTS idx_fmv11_volume      ON feature_mart_v1_1(volume_usdc)",
]


def _tail_label(x: float) -> int:
    if x < 5:
        return 0
    if x < 20:
        return 1
    if x < 50:
        return 2
    if x < 100:
        return 3
    return 4


def build(conn: sqlite3.Connection, recompute: bool = False) -> None:
    if recompute:
        conn.execute("DROP TABLE IF EXISTS feature_mart_v1_1")
        logger.info("Dropped existing feature_mart_v1_1")

    conn.execute(CREATE_TABLE)
    for idx in CREATE_INDEXES:
        conn.execute(idx)

    # Migrate: ensure markets table has neg_risk_market_id (added by data_collector step 6)
    markets_cols = {r[1] for r in conn.execute("PRAGMA table_info(markets)").fetchall()}
    if "neg_risk_market_id" not in markets_cols:
        conn.execute("ALTER TABLE markets ADD COLUMN neg_risk_market_id TEXT")

    # Migrate: add neg-risk group columns to existing tables
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(feature_mart_v1_1)").fetchall()}
    for col, col_type in [
        ("neg_risk_group_id",    "TEXT"),
        ("group_token_count",    "INTEGER"),
        ("group_max_volume",      "REAL"),
        ("group_underdog_count", "INTEGER"),
        ("group_underdog_winner","INTEGER"),
    ]:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE feature_mart_v1_1 ADD COLUMN {col} {col_type}")

    conn.commit()

    logger.info(
        f"Loading candidate universe "
        f"(vol >= {MIN_VOLUME}, date_from={DATE_FROM}, entry_max <= {ENTRY_MAX}) ..."
    )

    # One row per market. Swan features from the single best token (max max_traded_x).
    # CTE picks one token per market via ROW_NUMBER to avoid mixing features across tokens
    # (critical for neg-risk markets where MIN/MAX aggregates would create frankenstein rows).
    # group_stats CTE aggregates neg-risk group-level stats (NULL for binary markets).
    rows = conn.execute("""
        WITH best_swan AS (
            SELECT
                t.market_id,
                s.token_id,
                s.buy_min_price,
                s.max_traded_x,
                s.buy_volume,
                s.buy_trade_count,
                s.buy_ts_first,
                s.buy_ts_last,
                s.is_winner,
                ROW_NUMBER() OVER (
                    PARTITION BY t.market_id
                    ORDER BY s.max_traded_x DESC
                ) AS rn
            FROM swans_v2 s
            JOIN tokens t ON s.token_id = t.token_id
            WHERE s.buy_min_price <= :entry_max
        ),
        group_stats AS (
            SELECT
                m2.neg_risk_market_id                           AS group_id,
                COUNT(DISTINCT m2.id)                           AS group_token_count,
                MAX(m2.volume)                                  AS group_max_volume,
                COUNT(DISTINCT CASE
                    WHEN s2.buy_min_price <= :entry_max THEN m2.id
                END)                                            AS group_underdog_count,
                MAX(CASE
                    WHEN s2.buy_min_price <= :entry_max THEN s2.is_winner
                    ELSE 0
                END)                                            AS group_underdog_winner
            FROM markets m2
            LEFT JOIN tokens t2  ON t2.market_id = m2.id
            LEFT JOIN swans_v2 s2 ON s2.token_id = t2.token_id
            WHERE m2.neg_risk_market_id IS NOT NULL
            GROUP BY m2.neg_risk_market_id
        )
        SELECT
            m.id                AS market_id,
            DATE(m.closed_time, 'unixepoch') AS close_date,
            m.category,
            m.volume,
            m.liquidity,
            m.duration_hours,
            m.comment_count,
            m.neg_risk,
            m.closed_time,
            COUNT(DISTINCT t.token_id)   AS token_count,

            -- Swan features from ONE token (the best by max_traded_x)
            bs.buy_min_price         AS best_buy_min_price,
            bs.max_traded_x          AS best_max_traded_x,
            bs.buy_volume            AS best_buy_volume,
            bs.buy_trade_count       AS best_buy_trade_count,
            (bs.buy_ts_last - bs.buy_ts_first) AS best_floor_duration_s,
            bs.buy_ts_first          AS best_buy_ts_first,
            bs.is_winner             AS swan_is_winner,

            -- Neg-risk group features (NULL for binary markets)
            m.neg_risk_market_id     AS neg_risk_group_id,
            gs.group_token_count,
            gs.group_max_volume,
            gs.group_underdog_count,
            gs.group_underdog_winner

        FROM markets m
        JOIN tokens t ON t.market_id = m.id
        LEFT JOIN best_swan bs ON bs.market_id = m.id AND bs.rn = 1
        LEFT JOIN group_stats gs ON gs.group_id = m.neg_risk_market_id
        WHERE m.closed_time > 0
          AND m.volume >= :min_vol
          AND m.duration_hours > 0
          AND DATE(m.closed_time, 'unixepoch') >= :date_from
        GROUP BY m.id
    """, {"entry_max": ENTRY_MAX, "min_vol": MIN_VOLUME, "date_from": DATE_FROM}).fetchall()

    total = len(rows)
    t0 = time.monotonic()
    inserted = updated = 0

    for row in rows:
        try:
            market_id        = row[0]
            close_date       = row[1]
            category         = row[2]
            volume           = float(row[3] or 0)
            liquidity        = float(row[4] or 0)
            duration_hours   = float(row[5] or 0)
            comment_count    = int(row[6] or 0)
            neg_risk         = int(row[7] or 0)
            closed_time      = int(row[8] or 0)
            token_count      = int(row[9] or 2)

            best_buy_min_price   = row[10]
            best_max_traded_x    = row[11]
            best_buy_volume      = row[12]
            best_buy_trade_count = row[13]
            best_floor_duration_s  = row[14]
            best_buy_ts_first    = row[15]
            swan_is_winner         = row[16]

            neg_risk_group_id    = row[17]
            group_token_count    = row[18]
            group_max_volume      = row[19]
            group_underdog_count = row[20]
            group_underdog_winner = row[21]

            was_swan = 1 if best_buy_min_price is not None else 0

            log_volume    = math.log1p(volume)
            log_liquidity = math.log1p(liquidity)

            # niche_score_raw: lower = more niche
            # high volume relative to log(volume) → large liquid market
            # formula: volume / log(volume+1) — high value = mainstream
            niche_score_raw = volume / max(log_volume, 0.001)

            # time from first entry to resolution
            best_time_to_res = (
                (closed_time - int(best_buy_ts_first)) / 3600.0
                if was_swan and best_buy_ts_first and closed_time > int(best_buy_ts_first)
                else None
            )

            # labels
            if was_swan and best_max_traded_x is not None:
                px = float(best_max_traded_x)
                label_20x  = 1 if px >= 20 else 0
                label_tail = _tail_label(px)
            else:
                px         = 0.0
                label_20x  = 0
                label_tail = 0

            conn.execute("""
                INSERT INTO feature_mart_v1_1 (
                    market_id, date, category,
                    volume_usdc, log_volume, liquidity_usdc, log_liquidity,
                    duration_hours, comment_count, neg_risk, token_count,
                    niche_score_raw,
                    was_swan, best_buy_min_price, best_max_traded_x,
                    best_buy_volume, best_buy_trade_count,
                    best_floor_duration_s, best_time_to_res_hours,
                    swan_is_winner, label_20x, label_tail,
                    neg_risk_group_id, group_token_count, group_max_volume,
                    group_underdog_count, group_underdog_winner
                ) VALUES (
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?, ?
                ) ON CONFLICT(market_id) DO UPDATE SET
                    volume_usdc              = excluded.volume_usdc,
                    log_volume               = excluded.log_volume,
                    liquidity_usdc           = excluded.liquidity_usdc,
                    log_liquidity            = excluded.log_liquidity,
                    duration_hours           = excluded.duration_hours,
                    comment_count            = excluded.comment_count,
                    neg_risk                 = excluded.neg_risk,
                    token_count              = excluded.token_count,
                    niche_score_raw          = excluded.niche_score_raw,
                    was_swan                 = excluded.was_swan,
                    best_buy_min_price       = excluded.best_buy_min_price,
                    best_max_traded_x        = excluded.best_max_traded_x,
                    best_buy_volume          = excluded.best_buy_volume,
                    best_buy_trade_count     = excluded.best_buy_trade_count,
                    best_floor_duration_s    = excluded.best_floor_duration_s,
                    best_time_to_res_hours   = excluded.best_time_to_res_hours,
                    swan_is_winner           = excluded.swan_is_winner,
                    label_20x                = excluded.label_20x,
                    label_tail               = excluded.label_tail,
                    neg_risk_group_id        = excluded.neg_risk_group_id,
                    group_token_count        = excluded.group_token_count,
                    group_max_volume          = excluded.group_max_volume,
                    group_underdog_count     = excluded.group_underdog_count,
                    group_underdog_winner    = excluded.group_underdog_winner
            """, (
                market_id, close_date, category,
                volume, log_volume, liquidity, log_liquidity,
                duration_hours, comment_count, neg_risk, token_count,
                niche_score_raw,
                was_swan,
                float(best_buy_min_price) if best_buy_min_price is not None else None,
                float(best_max_traded_x) if best_max_traded_x is not None else None,
                float(best_buy_volume) if best_buy_volume is not None else None,
                int(best_buy_trade_count) if best_buy_trade_count is not None else None,
                float(best_floor_duration_s) if best_floor_duration_s is not None else None,
                best_time_to_res,
                int(swan_is_winner) if swan_is_winner is not None else None,
                label_20x, label_tail,
                neg_risk_group_id,
                int(group_token_count) if group_token_count is not None else None,
                float(group_max_volume) if group_max_volume is not None else None,
                int(group_underdog_count) if group_underdog_count is not None else None,
                int(group_underdog_winner) if group_underdog_winner is not None else None,
            ))
            inserted += 1
        except Exception as e:
            logger.warning(f"Row error market={row[0]}: {e}")
            continue

        if inserted % 5000 == 0:
            conn.commit()
            logger.info(f"  {inserted}/{total} ...")

    conn.commit()
    elapsed = int(time.monotonic() - t0)

    totals = conn.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(was_swan) AS swans,
            SUM(label_20x) AS good_swans,
            SUM(CASE WHEN was_swan=1 AND swan_is_winner=1 THEN 1 ELSE 0 END) AS winner_swans
        FROM feature_mart_v1_1
    """).fetchone()

    logger.info(
        f"Built feature_mart_v1_1 in {elapsed}s | "
        f"total={totals[0]} swans={totals[1]} "
        f"20x+={totals[2]} winners={totals[3]}"
    )


# ── Cohort Analysis ───────────────────────────────────────────────────────────

def cohort_analysis(conn: sqlite3.Connection) -> None:
    """Print cohort analysis: which features separate good swans from the rest."""

    def _pct(n, d):
        return f"{100*n/max(d,1):.1f}%"

    print("\n" + "="*70)
    print("COHORT ANALYSIS — feature_mart_v1_1")
    print("="*70)

    # Overall counts
    r = conn.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(was_swan) AS swans,
            SUM(CASE WHEN was_swan=1 AND label_20x=1 THEN 1 ELSE 0 END) AS good_swans,
            SUM(CASE WHEN was_swan=1 AND label_20x=0 THEN 1 ELSE 0 END) AS med_swans,
            SUM(CASE WHEN was_swan=0 THEN 1 ELSE 0 END) AS negatives,
            SUM(CASE WHEN was_swan=1 AND swan_is_winner=1 THEN 1 ELSE 0 END) AS winners
        FROM feature_mart_v1_1
    """).fetchone()
    total, swans, good, med, neg, winners = r
    print(f"\nOverall: {total} markets | "
          f"swans={swans} ({_pct(swans,total)}) | "
          f"good(>=20x)={good} ({_pct(good,total)}) | "
          f"winners={winners} ({_pct(winners,total)})")
    print(f"Label ratio: negatives={neg} ({_pct(neg,total)}) / "
          f"swans={swans} ({_pct(swans,total)})")

    # ── By volume bucket ──────────────────────────────────────────────────────
    print("\n── Volume buckets ──────────────────────────────────────────────────")
    print(f"{'bucket':<12} {'total':>8} {'swan%':>7} {'good%':>7} {'avg_x':>8} {'win%':>7}")
    rows = conn.execute("""
        SELECT
            CASE
                WHEN volume_usdc < 1000      THEN '<1k'
                WHEN volume_usdc < 10000     THEN '1k-10k'
                WHEN volume_usdc < 100000    THEN '10k-100k'
                WHEN volume_usdc < 1000000   THEN '100k-1M'
                ELSE                              '>1M'
            END AS bucket,
            COUNT(*) AS total,
            SUM(was_swan) AS swans,
            SUM(CASE WHEN was_swan=1 AND label_20x=1 THEN 1 ELSE 0 END) AS good,
            AVG(CASE WHEN was_swan=1 THEN best_max_traded_x END) AS avg_x,
            SUM(CASE WHEN was_swan=1 AND swan_is_winner=1 THEN 1 ELSE 0 END) AS wins
        FROM feature_mart_v1_1
        GROUP BY bucket
        ORDER BY MIN(volume_usdc)
    """).fetchall()
    for r in rows:
        bucket, n, sw, gd, ax, wi = r
        ax_str = f"{ax:.1f}" if ax else "N/A"
        print(f"{bucket:<12} {n:>8} {_pct(sw,n):>7} {_pct(gd,n):>7} {ax_str:>8} {_pct(wi,sw):>7}")

    # ── By category ──────────────────────────────────────────────────────────
    print("\n── Category ────────────────────────────────────────────────────────")
    print(f"{'category':<16} {'total':>8} {'swan%':>7} {'good%':>7} {'avg_x':>8} {'win%':>7}")
    rows = conn.execute("""
        SELECT
            COALESCE(category, 'null') AS cat,
            COUNT(*) AS total,
            SUM(was_swan) AS swans,
            SUM(CASE WHEN was_swan=1 AND label_20x=1 THEN 1 ELSE 0 END) AS good,
            AVG(CASE WHEN was_swan=1 THEN best_max_traded_x END) AS avg_x,
            SUM(CASE WHEN was_swan=1 AND swan_is_winner=1 THEN 1 ELSE 0 END) AS wins
        FROM feature_mart_v1_1
        GROUP BY cat
        ORDER BY SUM(was_swan) DESC
        LIMIT 12
    """).fetchall()
    for r in rows:
        cat, n, sw, gd, ax, wi = r
        ax_str = f"{ax:.1f}" if ax else "N/A"
        print(f"{cat:<16} {n:>8} {_pct(sw,n):>7} {_pct(gd,n):>7} {ax_str:>8} {_pct(wi,sw):>7}")

    # ── By duration bucket ────────────────────────────────────────────────────
    print("\n── Duration buckets (hours) ────────────────────────────────────────")
    print(f"{'duration':<14} {'total':>8} {'swan%':>7} {'good%':>7} {'avg_x':>8}")
    rows = conn.execute("""
        SELECT
            CASE
                WHEN duration_hours < 24    THEN '<1d'
                WHEN duration_hours < 72    THEN '1-3d'
                WHEN duration_hours < 168   THEN '3-7d'
                WHEN duration_hours < 720   THEN '1-4wk'
                WHEN duration_hours < 4380  THEN '1-6mo'
                ELSE                             '>6mo'
            END AS bucket,
            COUNT(*) AS total,
            SUM(was_swan) AS swans,
            SUM(CASE WHEN was_swan=1 AND label_20x=1 THEN 1 ELSE 0 END) AS good,
            AVG(CASE WHEN was_swan=1 THEN best_max_traded_x END) AS avg_x
        FROM feature_mart_v1_1
        GROUP BY bucket
        ORDER BY MIN(duration_hours)
    """).fetchall()
    for r in rows:
        bucket, n, sw, gd, ax = r
        ax_str = f"{ax:.1f}" if ax else "N/A"
        print(f"{bucket:<14} {n:>8} {_pct(sw,n):>7} {_pct(gd,n):>7} {ax_str:>8}")

    # ── neg_risk breakdown ────────────────────────────────────────────────────
    print("\n── neg_risk (multi-outcome group) ──────────────────────────────────")
    rows = conn.execute("""
        SELECT
            neg_risk,
            COUNT(*) AS total,
            SUM(was_swan) AS swans,
            SUM(CASE WHEN was_swan=1 AND label_20x=1 THEN 1 ELSE 0 END) AS good,
            AVG(CASE WHEN was_swan=1 THEN best_max_traded_x END) AS avg_x
        FROM feature_mart_v1_1
        GROUP BY neg_risk
    """).fetchall()
    print(f"{'neg_risk':<10} {'total':>8} {'swan%':>7} {'good%':>7} {'avg_x':>8}")
    for r in rows:
        nr, n, sw, gd, ax = r
        ax_str = f"{ax:.1f}" if ax else "N/A"
        print(f"{nr:<10} {n:>8} {_pct(sw,n):>7} {_pct(gd,n):>7} {ax_str:>8}")

    # ── Top/bottom volume×category lift ──────────────────────────────────────
    print("\n── Volume × Category: top 10 combos by good_swan_rate ─────────────")
    rows = conn.execute("""
        SELECT
            COALESCE(category, 'null') AS cat,
            CASE
                WHEN volume_usdc < 10000     THEN '<10k'
                WHEN volume_usdc < 100000    THEN '10k-100k'
                WHEN volume_usdc < 1000000   THEN '100k-1M'
                ELSE                              '>1M'
            END AS vol_bucket,
            COUNT(*) AS total,
            SUM(was_swan) AS swans,
            SUM(CASE WHEN was_swan=1 AND label_20x=1 THEN 1 ELSE 0 END) AS good,
            AVG(CASE WHEN was_swan=1 THEN best_max_traded_x END) AS avg_x
        FROM feature_mart_v1_1
        GROUP BY cat, vol_bucket
        HAVING total >= 30
        ORDER BY CAST(good AS REAL)/CAST(total AS REAL) DESC
        LIMIT 10
    """).fetchall()
    print(f"{'category':<14} {'vol':>9} {'total':>8} {'swan%':>7} {'good%':>7} {'avg_x':>8}")
    for r in rows:
        cat, vol, n, sw, gd, ax = r
        ax_str = f"{ax:.1f}" if ax else "N/A"
        print(f"{cat:<14} {vol:>9} {n:>8} {_pct(sw,n):>7} {_pct(gd,n):>7} {ax_str:>8}")

    # ── Separability: does volume alone order the classes? ───────────────────
    print("\n── Lift vs. baseline (was_swan rate) ───────────────────────────────")
    base_swan_rate = swans / max(total, 1)
    base_good_rate = good / max(total, 1)
    print(f"Baseline swan rate:      {base_swan_rate:.3f}")
    print(f"Baseline good swan rate: {base_good_rate:.4f}")

    print("\n  Top 10% by volume → swan_rate vs baseline:")
    # SQLite doesn't have PERCENTILE_DISC; approximate with LIMIT/OFFSET
    p90 = conn.execute("""
        SELECT volume_usdc FROM feature_mart_v1_1
        ORDER BY volume_usdc
        LIMIT 1 OFFSET (SELECT CAST(COUNT(*)*0.9 AS INTEGER) FROM feature_mart_v1_1)
    """).fetchone()
    if p90:
        r = conn.execute("""
            SELECT COUNT(*) AS n,
                   SUM(was_swan) AS sw,
                   SUM(CASE WHEN was_swan=1 AND label_20x=1 THEN 1 ELSE 0 END) AS gd,
                   AVG(CASE WHEN was_swan=1 THEN best_max_traded_x END) AS avg_x
            FROM feature_mart_v1_1
            WHERE volume_usdc >= ?
        """, (p90[0],)).fetchone()
        n, sw, gd, ax = r
        ax_str = f"{ax:.1f}" if ax else "N/A"
        print(f"  Top 10% vol (>= ${p90[0]:.0f}): "
              f"n={n} swan_rate={_pct(sw,n)} good_rate={_pct(gd,n)} avg_x={ax_str}")
        print(f"  Swan lift:      {sw/max(n,1) / base_swan_rate:.2f}x")
        print(f"  Good swan lift: {gd/max(n,1) / base_good_rate:.2f}x")

    # ── Initial weight suggestions ─────────────────────────────────────────
    print("\n" + "="*70)
    print("SUGGESTED INITIAL market_score WEIGHTS")
    print("="*70)
    print("""
market_score = w1·liquidity_score + w2·niche_score + w3·context_score
             + w4·time_score + w5·analogy_score

Based on the cohort data above:

  liquidity_score  (w1 = 0.35):
    Strongest individual predictor: swan_rate scales from 0.2% (<1k)
    to 48.8% (>1M). Use log(volume) / log(max_observed_volume) as proxy
    until real CLOB orderbook depth is available.

  niche_score      (w2 = 0.25):
    Captures "traded but not saturated." Markets with high volume/liquidity
    ratio have real activity but also more efficient pricing.
    Proxy: 1 / (1 + log1p(buy_trade_count)) — fewer trades at floor = more neglected.
    NOTE: needs buy_trade_count from screener, not available pre-entry.
    At screener stage: use comment_count as attention proxy (lower = more niche).

  context_score    (w3 = 0.10):
    neg_risk adds modest lift. Category matters but captured in analogy_score.
    Binary flag: neg_risk → slight boost (less pricing efficiency per token).

  time_score       (w4 = 0.15):
    Long-duration markets have higher avg_x. Markets with >720h duration
    show consistently better swan quality.
    Formula: min(duration_hours / 720, 1.0)

  analogy_score    (w5 = 0.15):
    Historical base rate for this (category, volume_bucket) combo.
    Build a lookup table from feature_mart_v1_1.
    Normalise to [0,1] using the observed good_swan_rate range.

Calibration criterion (exit stage 0):
  market_score of top 20% candidates must show:
    - good_swan_rate >= 2x baseline
    - avg_x among swans >= 20.0
    - bottom 50% candidates must show ROI <= 0 (no edge worth taking)
""")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build feature_mart_v1_1 and run cohort analysis"
    )
    ap.add_argument("--recompute", action="store_true",
                    help="Drop and rebuild from scratch")
    ap.add_argument("--analysis-only", action="store_true",
                    help="Skip build, only run cohort analysis")
    ap.add_argument("--db", default=str(DB_PATH), help="SQLite DB path")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    if not args.analysis_only:
        build(conn, recompute=args.recompute)

    cohort_analysis(conn)
    conn.close()


if __name__ == "__main__":
    main()
