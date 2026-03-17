"""
Backfill restricted + volume_1wk for markets already in the DB.

The parser skips markets already marked as parsed (state_db), so a normal
re-run will not populate new columns for historical data. This script:
  - iterates raw market JSON across all downloaded date directories
  - for each market already in the DB with NULL restricted or volume_1wk
  - reads the two fields from raw JSON and issues a targeted UPDATE

Only touches restricted and volume_1wk. Does not re-run the analyzer.

Run:
    python3 scripts/backfill_new_fields.py
    python3 scripts/backfill_new_fields.py --dry-run   # report counts, no writes
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.paths import DATABASE_DIR, DB_PATH
from utils.logger import setup_logger

logger = setup_logger("backfill_new_fields")


def backfill(dry_run: bool = False) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Ensure columns exist (safe no-op if already present)
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(markets)").fetchall()}
    for col, col_type in [("restricted", "INTEGER"), ("volume_1wk", "REAL")]:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE markets ADD COLUMN {col} {col_type}")
    conn.commit()

    # Markets that still have NULL in either new field
    null_ids = {
        r[0] for r in conn.execute(
            "SELECT id FROM markets WHERE restricted IS NULL OR volume_1wk IS NULL"
        ).fetchall()
    }
    logger.info(f"Markets with NULL restricted or volume_1wk: {len(null_ids)}")
    if not null_ids:
        logger.info("Nothing to backfill.")
        conn.close()
        return

    updated = skipped = missing_file = 0
    t0 = time.monotonic()

    # Walk all date directories under DATABASE_DIR
    date_dirs = sorted(
        d for d in os.listdir(DATABASE_DIR)
        if os.path.isdir(os.path.join(DATABASE_DIR, d)) and not d.endswith("_trades")
    )

    for date_dir in date_dirs:
        day_path = os.path.join(DATABASE_DIR, date_dir)
        for fname in os.listdir(day_path):
            if not fname.endswith(".json"):
                continue
            market_id = fname[:-5]
            if market_id not in null_ids:
                continue

            fpath = os.path.join(day_path, fname)
            try:
                with open(fpath, encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                logger.warning(f"{market_id}: read error — {e}")
                missing_file += 1
                continue

            restricted = 1 if data.get("restricted") else 0
            volume_1wk = data.get("volume1wk") or data.get("volume1wkClob")

            if not dry_run:
                conn.execute(
                    "UPDATE markets SET restricted=?, volume_1wk=? WHERE id=?",
                    (restricted, volume_1wk, market_id),
                )
            null_ids.discard(market_id)
            updated += 1

            if updated % 5000 == 0:
                if not dry_run:
                    conn.commit()
                elapsed = time.monotonic() - t0
                logger.info(f"  {updated} updated ({elapsed:.0f}s elapsed, {len(null_ids)} remaining)")

    if not dry_run:
        conn.commit()
    conn.close()

    elapsed = int(time.monotonic() - t0)
    logger.info(
        f"Done in {elapsed}s | updated={updated} missing_file={missing_file} "
        f"still_null_after={len(null_ids)}"
        + (" [DRY RUN — no writes]" if dry_run else "")
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill restricted + volume_1wk for existing markets")
    ap.add_argument("--dry-run", action="store_true", help="Report counts without writing")
    args = ap.parse_args()
    backfill(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
