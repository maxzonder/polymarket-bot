"""
Polymarket Trades Collector

Downloads raw trade history per market from data-api.polymarket.com/trades and stores
per-token JSON files for analyzer.

Structure:
    database/
        yyyy-mm-dd/
            {market_id}_trades/
                {token_id}.json    ← [{side, size, price, timestamp}, ...]

Usage:
    python -m data_collector.trades_collector --start 2026-02-28 --end 2026-02-28
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

import requests

if __package__:
    from data_collector import state_db
    from utils.logger import setup_logger
else:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from data_collector import state_db
    from utils.logger import setup_logger

DATA_API_BASE = "https://data-api.polymarket.com"

from utils.paths import DATABASE_DIR, ensure_runtime_dirs

PAGE_LIMIT = 1000
MAX_OFFSET = 3000
SLEEP_TRADES = 0.1

ensure_runtime_dirs()
logger = setup_logger("trades_collector")


def _trades_dir(end_date: str, market_id: str) -> str:
    return os.path.join(DATABASE_DIR, end_date, f"{market_id}_trades")


def _market_json_path(end_date: str, market_id: str) -> str:
    return os.path.join(DATABASE_DIR, end_date, f"{market_id}.json")


def _to_ts(value) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value:
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except Exception:
            return 0
    return 0


def fetch_trades(condition_id: str, start_ts: int, end_ts: int) -> list[dict]:
    all_trades: list[dict] = []
    offset = 0

    while True:
        params = {
            "market": condition_id,
            "limit": PAGE_LIMIT,
            "offset": offset,
            "filterType": "CASH",
            "filterAmount": 10,
        }

        try:
            resp = requests.get(f"{DATA_API_BASE}/trades", params=params, timeout=30)
            resp.raise_for_status()
            page = resp.json()
        except Exception as e:
            if offset == MAX_OFFSET:
                break
            raise e

        if not isinstance(page, list) or not page:
            break

        filtered = [t for t in page if start_ts <= t.get("timestamp", 0) <= end_ts]
        all_trades.extend(filtered)

        if len(page) < PAGE_LIMIT:
            break

        oldest_on_page = min(t.get("timestamp", 0) for t in page)
        if oldest_on_page < start_ts:
            break

        if offset >= MAX_OFFSET:
            logger.warning(f"Reached API max offset {MAX_OFFSET} for {condition_id}, truncating history")
            break

        offset += PAGE_LIMIT

    return all_trades


def split_by_token(trades: list[dict]) -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = {}
    for t in trades:
        token_id = t.get("asset")
        if not token_id:
            continue
        result.setdefault(token_id, []).append(
            {
                "side": t["side"],
                "size": t["size"],
                "price": t["price"],
                "timestamp": t["timestamp"],
            }
        )
    return result


def save_trades(end_date: str, market_id: str, token_id: str, trades: list[dict]):
    out_dir = _trades_dir(end_date, market_id)
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f"{token_id}.json"), "w", encoding="utf-8") as f:
        json.dump(trades, f, ensure_ascii=False)


def collect_trades_for_day(day: date):
    day_str = day.isoformat()
    day_dir = os.path.join(DATABASE_DIR, day_str)

    if not os.path.isdir(day_dir):
        logger.warning(f"[{day_str}] Directory not found, skipping")
        return

    market_files = [f for f in os.listdir(day_dir) if f.endswith(".json") and not f.startswith("collector")]
    total = len(market_files)
    logger.info(f"[{day_str}] Found {total} markets, downloading trades with CASH>=10...")

    ok = skipped = errors = 0
    t0 = time.monotonic()
    PROGRESS_STEP = 100

    for i, fname in enumerate(market_files, 1):
        market_id = fname[:-5]

        if state_db.is_trades_downloaded(day_str, market_id):
            skipped += 1
            continue

        market_path = _market_json_path(day_str, market_id)
        try:
            with open(market_path, encoding="utf-8") as f:
                market = json.load(f)
        except Exception as e:
            logger.warning(f"[{day_str}] {market_id}: cannot read market JSON — {e}")
            errors += 1
            continue

        condition_id = market.get("conditionId")
        start_ts = _to_ts(market.get("startDate") or market.get("startDateIso"))
        end_ts = _to_ts(market.get("closedTime") or market.get("endDate")) or int(time.time())

        if not condition_id:
            logger.warning(f"[{day_str}] {market_id}: no conditionId, skipping")
            errors += 1
            continue

        try:
            trades = fetch_trades(condition_id, start_ts, end_ts)
            by_token = split_by_token(trades)
            for token_id, token_trades in by_token.items():
                save_trades(day_str, market_id, token_id, token_trades)
            state_db.mark_trades_downloaded(day_str, market_id)
            ok += 1
        except Exception as e:
            logger.warning(f"[{day_str}] {market_id}: trades fetch error — {e}")
            state_db.mark_error(day_str, market_id, f"trades: {e}")
            errors += 1

        time.sleep(SLEEP_TRADES)

        if i % PROGRESS_STEP == 0:
            elapsed = time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(f"[{day_str}] Trades {i}/{total} markets — {rate:.1f}/s, ETA ~{eta}s | ok={ok} skip={skipped} err={errors}")

    elapsed = int(time.monotonic() - t0)
    logger.info(f"[{day_str}] DONE in {elapsed}s | ok={ok} skipped={skipped} errors={errors}")


def run(start: date, end: date):
    state_db.init_db()
    logger.info(f"Trades collection started: {start} -> {end} (CASH >= 10)")
    current = start
    while current <= end:
        collect_trades_for_day(current)
        current += timedelta(days=1)
    logger.info(f"Trades collection finished: {start} -> {end}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket Trades Collector")
    parser.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--end", required=True, metavar="YYYY-MM-DD")
    args = parser.parse_args()
    run(date.fromisoformat(args.start), date.fromisoformat(args.end))
