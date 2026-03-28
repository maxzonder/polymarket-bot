"""
Polymarket Data Collector and Parser

Three steps in one run for a given date range:
  1. Download market JSONs from Gamma API
  2. Download trade history per market
  3. Parse JSONs into markets + tokens tables in polymarket_dataset.db

After this, run swan_analyzer.py separately to build swans_v2.

Usage:
    python -m data_collector.data_collector_and_parsing --start 2026-03-27 --end 2026-03-27
    python -m data_collector.data_collector_and_parsing --start 2025-09-01 --end 2025-09-30
    python -m data_collector.data_collector_and_parsing --start 2026-03-27 --end 2026-03-27 --skip-trades
    python -m data_collector.data_collector_and_parsing --start 2026-03-27 --end 2026-03-27 --skip-markets --skip-trades
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests

if __package__:
    from utils.logger import setup_logger
else:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from utils.logger import setup_logger

from utils.paths import DATABASE_DIR, DB_PATH, ensure_runtime_dirs

ensure_runtime_dirs()
logger = setup_logger("data_collector_and_parsing")

# ─── Constants ────────────────────────────────────────────────────────────────

GAMMA_BASE    = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"

MARKETS_PAGE_SIZE   = 100
MARKETS_PAGE_SLEEP  = 0.05
TRADES_PAGE_LIMIT   = 1000
TRADES_MAX_OFFSET   = 3000
TRADES_SLEEP        = 0.1
PROGRESS_STEP       = 100


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Download market JSONs
# ═══════════════════════════════════════════════════════════════════════════════

def _market_json_path(day_str: str, market_id: str) -> str:
    return os.path.join(DATABASE_DIR, day_str, f"{market_id}.json")


def _fetch_markets_for_date(day: date) -> list[dict]:
    day_str  = day.isoformat()
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
            "limit": MARKETS_PAGE_SIZE,
            "offset": offset,
        }
        resp = requests.get(f"{GAMMA_BASE}/markets", params=params, timeout=30)
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        markets.extend(page)
        if len(page) < MARKETS_PAGE_SIZE:
            break
        offset += MARKETS_PAGE_SIZE
        time.sleep(MARKETS_PAGE_SLEEP)

    return markets


def _save_market_json(day_str: str, market: dict) -> str:
    market_id = str(market["id"])
    path = _market_json_path(day_str, market_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(market, f, ensure_ascii=False, indent=2)
    return market_id


def _collect_markets_day(day: date):
    day_str = day.isoformat()
    t0 = time.monotonic()
    logger.info(f"[markets][{day_str}] Fetching...")

    try:
        markets = _fetch_markets_for_date(day)
    except Exception as e:
        logger.error(f"[markets][{day_str}] Failed — {e}")
        return

    total = len(markets)
    saved = errors = 0
    logger.info(f"[markets][{day_str}] Got {total}, saving JSONs...")
    t_save = time.monotonic()

    skipped = 0
    for i, market in enumerate(markets, 1):
        market_id = market.get("id")
        if not market_id:
            continue
        if os.path.exists(_market_json_path(day_str, str(market_id))):
            skipped += 1
            continue
        try:
            _save_market_json(day_str, market)
            saved += 1
        except Exception as e:
            logger.warning(f"[markets][{day_str}] {market_id}: save failed — {e}")
            errors += 1

        if i % PROGRESS_STEP == 0:
            elapsed = time.monotonic() - t_save
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(f"[markets][{day_str}] {i}/{total} saved — {rate:.0f}/s, ETA ~{eta}s")

    logger.info(f"[markets][{day_str}] DONE in {int(time.monotonic()-t0)}s | saved={saved} skipped={skipped} errors={errors}")


def collect_markets(start: date, end: date):
    logger.info(f"Markets download: {start} → {end}")
    current = start
    while current <= end:
        _collect_markets_day(current)
        current += timedelta(days=1)
    logger.info(f"Markets download done: {start} → {end}")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Download trades
# ═══════════════════════════════════════════════════════════════════════════════

def _trades_dir(day_str: str, market_id: str) -> str:
    return os.path.join(DATABASE_DIR, day_str, f"{market_id}_trades")


def _to_ts(value) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value:
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except Exception:
            return 0
    return 0


def _fetch_trades(condition_id: str, start_ts: int, end_ts: int) -> list[dict]:
    all_trades: list[dict] = []
    offset = 0

    while True:
        params = {
            "market": condition_id,
            "limit": TRADES_PAGE_LIMIT,
            "offset": offset,
            "filterType": "CASH",
            "filterAmount": 10,
        }
        try:
            resp = requests.get(f"{DATA_API_BASE}/trades", params=params, timeout=30)
            resp.raise_for_status()
            page = resp.json()
        except Exception as e:
            if offset == TRADES_MAX_OFFSET:
                break
            raise e

        if not isinstance(page, list) or not page:
            break

        all_trades.extend(t for t in page if start_ts <= t.get("timestamp", 0) <= end_ts)

        if len(page) < TRADES_PAGE_LIMIT:
            break
        if min(t.get("timestamp", 0) for t in page) < start_ts:
            break
        if offset >= TRADES_MAX_OFFSET:
            logger.warning(f"Reached API max offset {TRADES_MAX_OFFSET} for {condition_id}, truncating")
            break

        offset += TRADES_PAGE_LIMIT

    return all_trades


def _split_by_token(trades: list[dict]) -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = {}
    for t in trades:
        tid = t.get("asset")
        if not tid:
            continue
        result.setdefault(tid, []).append({
            "side": t["side"],
            "size": t["size"],
            "price": t["price"],
            "timestamp": t["timestamp"],
        })
    return result


def _save_trades(day_str: str, market_id: str, token_id: str, trades: list[dict]):
    out_dir = _trades_dir(day_str, market_id)
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f"{token_id}.json"), "w", encoding="utf-8") as f:
        json.dump(trades, f, ensure_ascii=False)


def _collect_trades_day(day: date):
    day_str = day.isoformat()
    day_dir = os.path.join(DATABASE_DIR, day_str)

    if not os.path.isdir(day_dir):
        logger.warning(f"[trades][{day_str}] Directory not found, skipping")
        return

    market_files = [f for f in os.listdir(day_dir) if f.endswith(".json") and not f.startswith("collector")]
    total = len(market_files)
    logger.info(f"[trades][{day_str}] Found {total} markets, downloading trades...")

    ok = skipped = errors = 0
    t0 = time.monotonic()

    for i, fname in enumerate(market_files, 1):
        market_id = fname[:-5]

        trades_folder = _trades_dir(day_str, market_id)
        if os.path.isdir(trades_folder) and os.listdir(trades_folder):
            skipped += 1
            continue

        market_path = _market_json_path(day_str, market_id)
        try:
            with open(market_path, encoding="utf-8") as f:
                market = json.load(f)
        except Exception as e:
            logger.warning(f"[trades][{day_str}] {market_id}: cannot read market JSON — {e}")
            errors += 1
            continue

        condition_id = market.get("conditionId")
        start_ts = _to_ts(market.get("startDate") or market.get("startDateIso"))
        end_ts = _to_ts(market.get("closedTime") or market.get("endDate")) or int(time.time())

        if not condition_id:
            logger.warning(f"[trades][{day_str}] {market_id}: no conditionId, skipping")
            errors += 1
            continue

        try:
            trades = _fetch_trades(condition_id, start_ts, end_ts)
            for token_id, token_trades in _split_by_token(trades).items():
                _save_trades(day_str, market_id, token_id, token_trades)
            ok += 1
        except Exception as e:
            logger.warning(f"[trades][{day_str}] {market_id}: fetch error — {e}")
            errors += 1

        time.sleep(TRADES_SLEEP)

        if i % PROGRESS_STEP == 0:
            elapsed = time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(f"[trades][{day_str}] {i}/{total} — {rate:.1f}/s ETA ~{eta}s | ok={ok} skip={skipped} err={errors}")

    logger.info(f"[trades][{day_str}] DONE in {int(time.monotonic()-t0)}s | ok={ok} skipped={skipped} errors={errors}")


def collect_trades(start: date, end: date):
    logger.info(f"Trades download: {start} → {end} (CASH >= 10)")
    current = start
    while current <= end:
        _collect_trades_day(current)
        current += timedelta(days=1)
    logger.info(f"Trades download done: {start} → {end}")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Parse market JSONs into DB (markets + tokens tables)
# ═══════════════════════════════════════════════════════════════════════════════

_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS markets (
    id                  TEXT PRIMARY KEY,
    question            TEXT,
    description         TEXT,
    category            TEXT,
    slug                TEXT,
    event_title         TEXT,
    event_slug          TEXT,
    event_description   TEXT,
    tags                TEXT,
    ticker              TEXT,
    resolution_source   TEXT,
    start_date          INTEGER,
    end_date            INTEGER,
    closed_time         INTEGER,
    duration_hours      REAL,
    volume              REAL,
    liquidity           REAL,
    comment_count       INTEGER,
    fees_enabled        INTEGER,
    neg_risk            INTEGER,
    group_item_title    TEXT,
    cyom                INTEGER,
    restricted          INTEGER,
    volume_1wk          REAL
);

CREATE TABLE IF NOT EXISTS tokens (
    token_id        TEXT PRIMARY KEY,
    market_id       TEXT,
    outcome_name    TEXT,
    is_winner       INTEGER,
    FOREIGN KEY (market_id) REFERENCES markets(id)
);

CREATE INDEX IF NOT EXISTS idx_tokens_market ON tokens(market_id);
"""

_MARKET_UPSERT = """
INSERT INTO markets (
    id, question, description, category, slug,
    event_title, event_slug, event_description, tags, ticker,
    resolution_source, start_date, end_date, closed_time, duration_hours,
    volume, liquidity, comment_count, fees_enabled,
    neg_risk, group_item_title, cyom, restricted, volume_1wk
) VALUES (
    :id, :question, :description, :category, :slug,
    :event_title, :event_slug, :event_description, :tags, :ticker,
    :resolution_source, :start_date, :end_date, :closed_time, :duration_hours,
    :volume, :liquidity, :comment_count, :fees_enabled,
    :neg_risk, :group_item_title, :cyom, :restricted, :volume_1wk
)
ON CONFLICT(id) DO UPDATE SET
    question=excluded.question,
    description=excluded.description,
    category=COALESCE(excluded.category, markets.category),
    slug=excluded.slug,
    event_title=excluded.event_title,
    event_slug=excluded.event_slug,
    event_description=excluded.event_description,
    tags=excluded.tags,
    ticker=excluded.ticker,
    resolution_source=excluded.resolution_source,
    start_date=excluded.start_date,
    end_date=excluded.end_date,
    closed_time=excluded.closed_time,
    duration_hours=excluded.duration_hours,
    volume=excluded.volume,
    liquidity=excluded.liquidity,
    comment_count=excluded.comment_count,
    fees_enabled=excluded.fees_enabled,
    neg_risk=excluded.neg_risk,
    group_item_title=excluded.group_item_title,
    cyom=excluded.cyom,
    restricted=excluded.restricted,
    volume_1wk=excluded.volume_1wk
"""

_TOKEN_UPSERT = """
INSERT INTO tokens (token_id, market_id, outcome_name, is_winner)
VALUES (:token_id, :market_id, :outcome_name, :is_winner)
ON CONFLICT(token_id) DO UPDATE SET
    market_id=excluded.market_id,
    outcome_name=excluded.outcome_name,
    is_winner=excluded.is_winner
"""

_CATEGORY_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("crypto", (
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "doge", "xrp", "gold", "silver",
        "oil", "nasdaq", "s&p", "spy", "qqq", "fed", "cpi", "rate cut", "treasury", "eur", "jpy", "rial",
    )),
    ("sports", (
        "fc ", " vs ", "game ", "match ", "mlb", "nba", "nhl", "nfl", "uefa", "champions league",
        "world cup", "premier league", "serie a", "la liga", "bundesliga", "tennis", "f1", "formula 1",
        "ufc", "cricket", "afghanistan", "south africa", "win on ",
    )),
    ("politics", (
        "trump", "biden", "election", "senate", "house", "republican", "democrat", "white house",
        "prime minister", "president", "governor", "mayor", "parliament", "minister",
    )),
    ("geopolitics", (
        "iran", "iraq", "israel", "gaza", "hamas", "hezbollah", "ukraine", "russia", "china", "taiwan",
        "strike", "missile", "nuclear", "military", "ceasefire", "war", "attack",
    )),
    ("weather", (
        "temperature", "°c", "°f", "highest temperature", "rain", "snow", "storm", "hurricane",
    )),
    ("entertainment", (
        "oscar", "grammy", "emmy", "sag awards", "box office", "movie", "album", "netflix", "actor", "actress",
    )),
    ("tech", (
        "openai", "chatgpt", "apple", "google", "microsoft", "nvidia", "tesla", "meta", "amazon",
    )),
]


def _parse_ts(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    try:
        s = s.strip().replace(" ", "T")
        if s.endswith("+00"):
            s = s[:-3] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def _infer_category(data: dict) -> Optional[str]:
    top = data.get("category")
    if isinstance(top, str) and top.strip():
        return top.strip().lower()

    events = data.get("events") or []
    if events:
        ev = events[0] or {}
        ev_cat = ev.get("category")
        if isinstance(ev_cat, str) and ev_cat.strip():
            return ev_cat.strip().lower()
        for tag in (ev.get("tags") or []):
            if not isinstance(tag, dict):
                continue
            for key in ("label", "slug", "name"):
                val = tag.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip().lower()

    ev0 = (events[0] or {}) if events else {}
    haystack = " ".join(filter(None, [
        str(data.get("question") or ""),
        str(data.get("description") or ""),
        str(data.get("slug") or ""),
        str(ev0.get("title") or ""),
        str(ev0.get("slug") or ""),
        str(data.get("groupItemTitle") or ""),
    ])).lower()

    for category, keywords in _CATEGORY_KEYWORDS:
        if any(re.search(rf"\b{re.escape(k)}\b", haystack) for k in keywords):
            return category

    return None


def _parse_market_row(data: dict) -> Optional[dict]:
    market_id = data.get("id")
    if not market_id:
        return None

    events = data.get("events") or []
    ev = (events[0] or {}) if events else {}

    start_ts  = _parse_ts(data.get("startDate"))
    end_ts    = _parse_ts(data.get("endDate"))
    closed_ts = _parse_ts(data.get("closedTime"))

    volume = data.get("volumeNum")
    if volume is None:
        try:
            volume = float(data.get("volume") or 0)
        except (ValueError, TypeError):
            volume = None

    tags = ev.get("tags") or []

    return {
        "id":                str(market_id),
        "question":          data.get("question"),
        "description":       data.get("description"),
        "category":          _infer_category(data),
        "slug":              data.get("slug") or None,
        "event_title":       ev.get("title") or None,
        "event_slug":        ev.get("slug") or None,
        "event_description": ev.get("description") or None,
        "tags":              json.dumps(tags, ensure_ascii=False) if tags else None,
        "ticker":            ev.get("ticker") or None,
        "resolution_source": data.get("resolutionSource", ""),
        "start_date":        start_ts,
        "end_date":          end_ts,
        "closed_time":       closed_ts,
        "duration_hours":    (closed_ts - start_ts) / 3600.0 if start_ts and closed_ts else None,
        "volume":            volume,
        "liquidity":         data.get("liquidity"),
        "comment_count":     ev.get("commentCount"),
        "fees_enabled":      1 if data.get("feesEnabled") else 0,
        "neg_risk":          1 if data.get("negRisk") else 0,
        "group_item_title":  data.get("groupItemTitle") or None,
        "cyom":              1 if data.get("cyom") else 0,
        "restricted":        1 if data.get("restricted") else 0,
        "volume_1wk":        data.get("volume1wk") or data.get("volume1wkClob"),
    }


def _parse_token_rows(data: dict) -> list[dict]:
    market_id = str(data.get("id", ""))

    def _load(val):
        if isinstance(val, list):
            return val
        try:
            return json.loads(val or "[]")
        except Exception:
            return []

    token_ids = _load(data.get("clobTokenIds", "[]"))
    outcomes  = _load(data.get("outcomes", "[]"))
    prices    = _load(data.get("outcomePrices", "[]"))

    rows = []
    for i, token_id in enumerate(token_ids):
        try:
            is_winner = 1 if float(prices[i]) >= 0.99 else 0
        except (IndexError, ValueError, TypeError):
            is_winner = 0
        rows.append({
            "token_id":     str(token_id),
            "market_id":    market_id,
            "outcome_name": outcomes[i] if i < len(outcomes) else None,
            "is_winner":    is_winner,
        })
    return rows


def _init_db(conn: sqlite3.Connection):
    conn.executescript(_DB_SCHEMA)
    for col, col_type in [
        ("restricted", "INTEGER"),
        ("volume_1wk", "REAL"),
        ("slug", "TEXT"),
        ("event_title", "TEXT"),
        ("event_slug", "TEXT"),
        ("event_description", "TEXT"),
        ("tags", "TEXT"),
        ("ticker", "TEXT"),
    ]:
        rows = conn.execute(f"PRAGMA table_info(markets)").fetchall()
        if not any(r[1] == col for r in rows):
            conn.execute(f"ALTER TABLE markets ADD COLUMN {col} {col_type}")
    conn.commit()


def _parse_day(conn: sqlite3.Connection, day_str: str):
    day_dir = os.path.join(DATABASE_DIR, day_str)
    if not os.path.isdir(day_dir):
        logger.warning(f"[parse][{day_str}] Directory not found")
        return

    market_files = sorted(f for f in os.listdir(day_dir) if f.endswith(".json"))
    total = len(market_files)
    logger.info(f"[parse][{day_str}] Found {total} market JSONs")

    ok = skipped = errors = 0
    t0 = time.monotonic()

    for i, fname in enumerate(market_files, 1):
        market_id = fname[:-5]
        if conn.execute("SELECT 1 FROM markets WHERE id=?", (market_id,)).fetchone():
            skipped += 1
            continue

        fpath = os.path.join(day_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.warning(f"[parse][{day_str}] {market_id}: read error — {e}")
            errors += 1
            continue

        market_row = _parse_market_row(data)
        if not market_row:
            logger.warning(f"[parse][{day_str}] {market_id}: parse failed")
            errors += 1
            continue

        try:
            conn.execute(_MARKET_UPSERT, market_row)
            for tr in _parse_token_rows(data):
                conn.execute(_TOKEN_UPSERT, tr)
            conn.commit()
            ok += 1
        except Exception as e:
            conn.rollback()
            logger.warning(f"[parse][{day_str}] {market_id}: DB error — {e}")
            errors += 1

        if i % PROGRESS_STEP == 0:
            elapsed = time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(f"[parse][{day_str}] {i}/{total} — {rate:.0f}/s ETA ~{eta}s | ok={ok} skip={skipped} err={errors}")

    logger.info(f"[parse][{day_str}] DONE in {int(time.monotonic()-t0)}s | ok={ok} skipped={skipped} errors={errors}")


def parse_markets(start: date, end: date):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _init_db(conn)
    logger.info(f"Parsing: {start} → {end}")
    current = start
    while current <= end:
        _parse_day(conn, current.isoformat())
        current += timedelta(days=1)
    conn.close()
    logger.info(f"Parsing done: {start} → {end}")


# ═══════════════════════════════════════════════════════════════════════════════
# Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def run(
    start: date,
    end: date,
    skip_markets: bool = False,
    skip_trades: bool = False,
    skip_parse: bool = False,
) -> None:
    logger.info(f"Ingest started: {start} → {end}")

    if not skip_markets:
        logger.info("=== Step 1/3: Download markets ===")
        collect_markets(start, end)
    else:
        logger.info("=== Step 1/3: Download markets [SKIPPED] ===")

    if not skip_trades:
        logger.info("=== Step 2/3: Download trades ===")
        collect_trades(start, end)
    else:
        logger.info("=== Step 2/3: Download trades [SKIPPED] ===")

    if not skip_parse:
        logger.info("=== Step 3/3: Parse into DB ===")
        parse_markets(start, end)
    else:
        logger.info("=== Step 3/3: Parse into DB [SKIPPED] ===")

    logger.info(f"Ingest finished: {start} → {end}")
    logger.info("Next step: python scripts/swan_analyzer.py --date-from ... --date-to ...")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Polymarket Data Collector and Parser — download markets + trades + parse into DB"
    )
    ap.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--end",   required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--skip-markets", action="store_true", help="Skip market JSON download")
    ap.add_argument("--skip-trades",  action="store_true", help="Skip trades download")
    ap.add_argument("--skip-parse",   action="store_true", help="Skip DB parsing step")
    args = ap.parse_args()

    run(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        skip_markets=args.skip_markets,
        skip_trades=args.skip_trades,
        skip_parse=args.skip_parse,
    )
