"""
Empirical check: at filterAmount=1, what fraction of markets < N days fit within 4000 trades?

Samples markets from the DB, queries the live Polymarket trades API with filterAmount=1,
and reports truncation rate by duration bucket.

Usage:
    python3 scripts/check_filter_coverage.py --sample 200 --max-duration-days 30
    python3 scripts/check_filter_coverage.py --sample 100 --max-duration-days 7
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logger import setup_logger
from utils.paths import DATABASE_DIR, DB_PATH

logger = setup_logger("check_filter_coverage")

TRADES_PAGE_LIMIT = 1000
TRADES_MAX_OFFSET = 3000
DATA_API_BASE = "https://data-api.polymarket.com"


def _count_trades_at_filter1(condition_id: str) -> tuple[int, bool]:
    """
    Fetch all trades with filterAmount=1. Returns (count, truncated).
    We only count — we don't store any data.
    """
    total = 0
    offset = 0
    truncated = False

    while True:
        params = {
            "market": condition_id,
            "limit": TRADES_PAGE_LIMIT,
            "offset": offset,
            "filterType": "CASH",
            "filterAmount": 1,
        }
        try:
            resp = requests.get(f"{DATA_API_BASE}/trades", params=params, timeout=30)
            resp.raise_for_status()
            page = resp.json()
        except Exception as e:
            logger.warning(f"{condition_id}: API error at offset {offset} — {e}")
            return total, True

        if not isinstance(page, list) or not page:
            break

        total += len(page)

        if len(page) < TRADES_PAGE_LIMIT:
            break
        if offset >= TRADES_MAX_OFFSET:
            truncated = True
            break

        offset += TRADES_PAGE_LIMIT
        time.sleep(0.1)

    return total, truncated


def run(sample: int, max_duration_days: int):
    conn = sqlite3.connect(DB_PATH)

    rows = conn.execute(
        """
        SELECT m.id, m.question, m.start_date, m.end_date, m.duration_hours,
               t.token_id
        FROM markets m
        JOIN tokens t ON t.market_id = m.id AND t.outcome_name = 'Yes'
        WHERE m.duration_hours IS NOT NULL
          AND m.duration_hours > 0
          AND m.duration_hours / 24.0 <= ?
        ORDER BY RANDOM()
        LIMIT ?
        """,
        (max_duration_days, sample),
    ).fetchall()
    conn.close()

    if not rows:
        logger.error("No markets found matching criteria")
        return

    logger.info(f"Sampled {len(rows)} markets with duration <= {max_duration_days} days")

    buckets: dict[str, dict] = {
        "0-1d":   {"total": 0, "fits": 0, "truncated": 0},
        "1-3d":   {"total": 0, "fits": 0, "truncated": 0},
        "3-7d":   {"total": 0, "fits": 0, "truncated": 0},
        "7-14d":  {"total": 0, "fits": 0, "truncated": 0},
        "14-30d": {"total": 0, "fits": 0, "truncated": 0},
    }

    def _bucket(duration_hours: float) -> str:
        d = duration_hours / 24.0
        if d <= 1:   return "0-1d"
        if d <= 3:   return "1-3d"
        if d <= 7:   return "3-7d"
        if d <= 14:  return "7-14d"
        return "14-30d"

    for i, (market_id, question, start_ts, end_ts, duration_hours, token_id) in enumerate(rows, 1):
        b = _bucket(duration_hours)
        buckets[b]["total"] += 1

        # Derive conditionId from token file path in database directory
        # We need conditionId, not market_id. Find it from market JSON files.
        condition_id = None
        for day_dir in sorted(os.listdir(DATABASE_DIR), reverse=True):
            mpath = os.path.join(DATABASE_DIR, day_dir, f"{market_id}.json")
            if os.path.exists(mpath):
                try:
                    with open(mpath, encoding="utf-8") as f:
                        data = json.load(f)
                    condition_id = data.get("conditionId")
                except Exception:
                    pass
                break

        if not condition_id:
            logger.warning(f"[{i}/{len(rows)}] {market_id}: no conditionId found, skipping")
            continue

        count, truncated = _count_trades_at_filter1(condition_id)

        if truncated:
            buckets[b]["truncated"] += 1
        else:
            buckets[b]["fits"] += 1

        status = f"truncated ({count}+ trades)" if truncated else f"fits ({count} trades)"
        logger.info(f"[{i}/{len(rows)}] {market_id} [{b}] {duration_hours/24:.1f}d — {status}")

    print("\n" + "=" * 60)
    print(f"filterAmount=1 coverage — {len(rows)} markets, max {max_duration_days}d")
    print("=" * 60)
    print(f"{'Bucket':<10} {'Total':>6} {'Fits':>6} {'Trunc':>6} {'Fit%':>7}")
    print("-" * 40)
    grand_total = grand_fits = grand_trunc = 0
    for name, b in buckets.items():
        if b["total"] == 0:
            continue
        pct = 100.0 * b["fits"] / b["total"]
        print(f"{name:<10} {b['total']:>6} {b['fits']:>6} {b['truncated']:>6} {pct:>6.0f}%")
        grand_total += b["total"]
        grand_fits  += b["fits"]
        grand_trunc += b["truncated"]
    print("-" * 40)
    if grand_total:
        pct = 100.0 * grand_fits / grand_total
        print(f"{'TOTAL':<10} {grand_total:>6} {grand_fits:>6} {grand_trunc:>6} {pct:>6.0f}%")
    print("=" * 60)
    print(f"\nConclusion: {pct:.0f}% of markets <= {max_duration_days}d fit within 4000 trades at filterAmount=1")
    if pct >= 90:
        print("→ Safe to lower filterAmount to 1 for short markets without significant truncation.")
    else:
        print("→ Many short markets still truncate at filterAmount=1; keep filterAmount=10 or use --max-market-duration-days.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Empirical filterAmount=1 coverage check")
    ap.add_argument("--sample", type=int, default=200,
                    help="Number of markets to sample (default: 200)")
    ap.add_argument("--max-duration-days", type=int, default=30,
                    help="Only sample markets shorter than N days (default: 30)")
    args = ap.parse_args()
    run(sample=args.sample, max_duration_days=args.max_duration_days)
