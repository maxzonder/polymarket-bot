"""
Polymarket Data Ingest

Combines three steps into one run for a given date range:
  1. Download market JSONs from Gamma API  (collector.py)
  2. Download trade history per market      (trades_collector.py)
  3. Parse JSONs into markets + tokens DB   (parser.py, without old analyzer)

After ingest, run swan_analyzer.py separately to build swans_v2.

Usage:
    python -m data_collector.ingest --start 2026-03-27 --end 2026-03-27
    python -m data_collector.ingest --start 2025-09-01 --end 2025-09-30
    python -m data_collector.ingest --start 2026-03-27 --end 2026-03-27 --skip-trades
    python -m data_collector.ingest --start 2026-03-27 --end 2026-03-27 --skip-markets --skip-trades
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

if __package__:
    from data_collector import collector, trades_collector, parser, state_db
    from utils.logger import setup_logger
else:
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from data_collector import collector, trades_collector, parser, state_db
    from utils.logger import setup_logger

import sqlite3
from datetime import timedelta
from utils.paths import DB_PATH

logger = setup_logger("ingest")


def run(
    start: date,
    end: date,
    skip_markets: bool = False,
    skip_trades: bool = False,
    skip_parse: bool = False,
) -> None:
    state_db.init_db()

    logger.info(f"Ingest started: {start} → {end}")

    if not skip_markets:
        logger.info("=== Step 1/3: Download markets ===")
        collector.run(start, end)
    else:
        logger.info("=== Step 1/3: Download markets [SKIPPED] ===")

    if not skip_trades:
        logger.info("=== Step 2/3: Download trades ===")
        trades_collector.run(start, end)
    else:
        logger.info("=== Step 2/3: Download trades [SKIPPED] ===")

    if not skip_parse:
        logger.info("=== Step 3/3: Parse into DB ===")
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        parser.init_db(conn)
        current = start
        while current <= end:
            parser.parse_day(conn, current.isoformat())
            current += timedelta(days=1)
        conn.close()
    else:
        logger.info("=== Step 3/3: Parse into DB [SKIPPED] ===")

    logger.info(f"Ingest finished: {start} → {end}")
    logger.info("Next step: python scripts/swan_analyzer.py --date-from ... --date-to ...")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Polymarket Data Ingest — download + parse in one run")
    ap.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--end", required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--skip-markets", action="store_true", help="Skip market JSON download")
    ap.add_argument("--skip-trades", action="store_true", help="Skip trades download")
    ap.add_argument("--skip-parse", action="store_true", help="Skip DB parsing step")
    args = ap.parse_args()

    run(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        skip_markets=args.skip_markets,
        skip_trades=args.skip_trades,
        skip_parse=args.skip_parse,
    )
