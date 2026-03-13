"""
Polymarket Data Collector

Скачивает закрытые рынки за указанный диапазон дат и историю цен по каждому токену.

Структура файлов:
    database/
        yyyy-mm-dd/
            {market_id}.json               ← полный объект Market (Gamma API)
            {market_id}_hours/
                {token_id}.json            ← history [{t, p}, ...] (CLOB /prices-history)

Использование:
    python -m data_collector.collector --start 2026-01-01 --end 2026-01-31
"""
import argparse
import json
import os
import time
from datetime import date, timedelta

import requests

from data_collector import state_db
from utils.logger import setup_logger

# ── Константы ─────────────────────────────────────────────────────────────────
GAMMA_BASE   = "https://gamma-api.polymarket.com"
CLOB_BASE    = "https://clob.polymarket.com"

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
DATABASE_DIR  = os.path.abspath(os.path.join(_PROJECT_ROOT, 'database'))

PAGE_SIZE = 100

# Задержки из API_LIMITS.md:
#   /markets       — 30 req/sec  → sleep 0.05s
#   /prices-history — 100 req/sec → sleep 0.15s (небольшой запас)
SLEEP_MARKETS_PAGE = 0.05
SLEEP_PRICES       = 0.15

logger = setup_logger("collector")


# ── Пути ──────────────────────────────────────────────────────────────────────

def _market_json_path(end_date: str, market_id: str) -> str:
    return os.path.join(DATABASE_DIR, end_date, f"{market_id}.json")


def _hours_dir(end_date: str, market_id: str) -> str:
    return os.path.join(DATABASE_DIR, end_date, f"{market_id}_hours")


# ── Запросы к API ─────────────────────────────────────────────────────────────

def fetch_markets_for_date(day: date) -> list[dict]:
    """
    Возвращает все закрытые рынки с end_date == day.
    Пагинирует через offset до тех пор, пока страница не окажется неполной.
    """
    day_str  = day.isoformat()
    next_day = (day + timedelta(days=1)).isoformat()

    markets, offset = [], 0

    while True:
        params = {
            "closed":       "true",
            "end_date_min": f"{day_str}T00:00:00Z",
            "end_date_max": f"{next_day}T00:00:00Z",
            "limit":        PAGE_SIZE,
            "offset":       offset,
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


def fetch_price_history(token_id: str) -> list[dict]:
    """
    Скачивает почасовую историю цен токена за всё время (interval=max, fidelity=60).
    Возвращает список [{t: unix_ts, p: float}, ...].
    """
    params = {
        "market":   token_id,
        "interval": "max",
        "fidelity": 60,
    }
    resp = requests.get(f"{CLOB_BASE}/prices-history", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("history", [])


# ── Сохранение файлов ─────────────────────────────────────────────────────────

def save_market(end_date: str, market: dict) -> str:
    """Сохраняет JSON рынка. Возвращает market_id."""
    market_id = market["id"]
    path = _market_json_path(end_date, market_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(market, f, ensure_ascii=False, indent=2)
    return market_id


def save_prices(end_date: str, market_id: str, token_id: str, history: list):
    """Сохраняет историю цен токена в {market_id}_hours/{token_id}.json."""
    out_dir = _hours_dir(end_date, market_id)
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f"{token_id}.json"), "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False)


# ── Логика одного дня ─────────────────────────────────────────────────────────

def collect_day(day: date):
    day_str = day.isoformat()
    logger.info(f"Start downloading markets for {day_str}")

    # 1. Fetch market list
    try:
        markets = fetch_markets_for_date(day)
    except Exception as e:
        logger.error(f"{day_str}: failed to fetch markets — {e}")
        return

    total       = len(markets)
    mkt_new     = 0
    mkt_skipped = 0
    price_ok    = 0
    price_err   = 0

    PROGRESS_STEP = 100

    # ── Phase 1: download market JSONs ───────────────────────────────────────
    for i, market in enumerate(markets, 1):
        market_id = market.get("id")
        if not market_id:
            continue

        if state_db.is_market_downloaded(day_str, market_id):
            mkt_skipped += 1
        else:
            try:
                save_market(day_str, market)
                state_db.mark_downloaded(day_str, market_id)
                mkt_new += 1
            except Exception as e:
                logger.warning(f"{day_str} | {market_id}: failed to save market — {e}")
                state_db.mark_error(day_str, market_id, str(e))

        if i % PROGRESS_STEP == 0:
            logger.info(f"{day_str}: markets {i}/{total} saved")

    logger.info(f"{day_str}: markets done — {mkt_new} new, {mkt_skipped} skipped. Starting price download...")

    # ── Phase 2: download price history ──────────────────────────────────────
    for i, market in enumerate(markets, 1):
        market_id = market.get("id")
        if not market_id:
            continue

        if state_db.is_prices_downloaded(day_str, market_id):
            price_ok += 1
            continue

        raw_tokens = market.get("clobTokenIds", [])
        try:
            token_ids = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens
        except Exception:
            token_ids = []

        if not token_ids:
            logger.warning(f"{day_str} | {market_id}: clobTokenIds empty, skipping prices")
            continue

        tokens_ok = 0
        for token_id in token_ids:
            try:
                history = fetch_price_history(token_id)
                save_prices(day_str, market_id, token_id, history)
                tokens_ok += 1
                logger.debug(f"{day_str} | {market_id} | token={token_id}: {len(history)} points")
            except Exception as e:
                logger.warning(f"{day_str} | {market_id} | token={token_id}: price fetch error — {e}")
                price_err += 1
            time.sleep(SLEEP_PRICES)

        if tokens_ok == len(token_ids):
            state_db.mark_prices_downloaded(day_str, market_id)
            price_ok += 1
        else:
            state_db.mark_error(
                day_str, market_id,
                f"prices: downloaded {tokens_ok}/{len(token_ids)} tokens"
            )

        if i % PROGRESS_STEP == 0:
            logger.info(f"{day_str}: prices {i}/{total} markets processed")

    logger.info(
        f"{day_str}: DONE | total: {total} | "
        f"new: {mkt_new} | skipped: {mkt_skipped} | "
        f"prices ok: {price_ok} | price errors: {price_err}"
    )


# ── Точка входа ───────────────────────────────────────────────────────────────

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
    parser = argparse.ArgumentParser(
        description="Polymarket Data Collector — скачивает рынки и историю цен по диапазону дат"
    )
    parser.add_argument("--start", required=True, metavar="YYYY-MM-DD", help="Начальная дата (включительно)")
    parser.add_argument("--end",   required=True, metavar="YYYY-MM-DD", help="Конечная дата (включительно)")
    args = parser.parse_args()

    run(
        date.fromisoformat(args.start),
        date.fromisoformat(args.end),
    )
