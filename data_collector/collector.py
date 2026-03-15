"""
Polymarket Data Collector

Downloads closed market JSONs for a given date range.

Structure:
    database/
        yyyy-mm-dd/
            {market_id}.json   ← full Market object from Gamma API

Usage:
    python -m data_collector.collector --start 2026-01-01 --end 2026-01-31
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, timedelta

import requests

if __package__:
    from data_collector import state_db
    from utils.logger import setup_logger
else:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from data_collector import state_db
    from utils.logger import setup_logger

GAMMA_BASE = "https://gamma-api.polymarket.com"

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
DATABASE_DIR = os.path.abspath(os.path.join(_PROJECT_ROOT, 'database'))

PAGE_SIZE = 100
SLEEP_MARKETS_PAGE = 0.05

logger = setup_logger("collector")


def _market_json_path(end_date: str, market_id: str) -> str:
    return os.path.join(DATABASE_DIR, end_date, f"{market_id}.json")


def fetch_markets_for_date(day: date) -> list[dict]:
    """
    Returns all closed markets with end_date == day.

    Important:
    - include_tag=true so market JSON contains tags/category-like metadata.
    - paginates with offset until page is short.
    """
    day_str = day.isoformat()
    next_day = (day + timedelta(days=1)).isoformat()

    markets: list[dict] = []
    offset = 0

    while True:
        params = {
            "closed": "true",
            "include_tag": "true",
            "end_date_min": f"{day_str}T00:00:00Z",
            "end_date_max": f"{next_day}T00:00:00Z",
            "volume_num_min": 50,
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        resp = requests.get(f"{GAMMA_BASE}/markets", params=params, timeout=30)
        resp.raise_for_status()
        page = resp.json()

        if not page:
            break

        markets.extend(page)
        logger.debug(f"{day_str}: offset={offset}, page_size={len(page)}")

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(SLEEP_MARKETS_PAGE)

    return markets


def save_market(end_date: str, market: dict) -> str:
    market_id = str(market["id"])
    path = _market_json_path(end_date, market_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(market, f, ensure_ascii=False, indent=2)
    return market_id


def collect_day(day: date):
    import time as _time

    day_str = day.isoformat()
    t_day_start = _time.monotonic()
    logger.info(f"[{day_str}] Fetching markets...")

    try:
        markets = fetch_markets_for_date(day)
    except Exception as e:
        logger.error(f"[{day_str}] Failed to fetch markets — {e}")
        return

    total = len(markets)
    saved = 0
    errors = 0
    PROGRESS_STEP = 100

    logger.info(f"[{day_str}] Got {total} markets, saving JSONs...")

    t0 = _time.monotonic()
    for i, market in enumerate(markets, 1):
        market_id = market.get("id")
        if not market_id:
            continue

        try:
            save_market(day_str, market)
            state_db.mark_downloaded(day_str, str(market_id))
            saved += 1
        except Exception as e:
            logger.warning(f"[{day_str}] {market_id}: failed to save market — {e}")
            state_db.mark_error(day_str, str(market_id), str(e))
            errors += 1

        if i % PROGRESS_STEP == 0:
            elapsed = _time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(f"[{day_str}] {i}/{total} saved — {rate:.0f}/s, ETA ~{eta}s")

    elapsed_total = int(_time.monotonic() - t_day_start)
    logger.info(f"[{day_str}] DONE in {elapsed_total}s | total: {total} | saved: {saved} | errors: {errors}")



def run(start: date, end: date):
    state_db.init_db()
    logger.info(f"Collection started: {start} -> {end}")

    current = start
    while current <= end:
        collect_day(current)
        current += timedelta(days=1)
        if current <= end:
            logger.info(f"Moving to next date: {current}")

    logger.info(f"Collection finished: {start} -> {end}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket Data Collector — downloads market JSONs for a date range")
    parser.add_argument("--start", required=True, metavar="YYYY-MM-DD", help="Начальная дата (включительно)")
    parser.add_argument("--end", required=True, metavar="YYYY-MM-DD", help="Конечная дата (включительно)")
    args = parser.parse_args()

    run(
        date.fromisoformat(args.start),
        date.fromisoformat(args.end),
    )
