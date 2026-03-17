"""
Polymarket Parser

Reads downloaded market JSON files from database/ and writes normalized metadata into
polymarket_dataset.db. Then triggers analyzer to rebuild/append token_swans from raw trades.

Current scope:
- markets
- tokens
- analyzer/token_swans

Legacy layers removed from parser:
- price_history
- token_analytics

Usage:
    python -m data_collector.parser --start 2026-02-28 --end 2026-02-28
    python -m data_collector.parser --start 2026-02-14 --end 2026-02-28
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

if __package__:
    from data_collector import state_db, analyzer
    from utils.logger import setup_logger
else:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from data_collector import state_db, analyzer
    from utils.logger import setup_logger

from utils.paths import DATABASE_DIR, DB_PATH, ensure_runtime_dirs

ensure_runtime_dirs()
logger = setup_logger("parser")

SCHEMA = """
CREATE TABLE IF NOT EXISTS markets (
    id                  TEXT PRIMARY KEY,
    question            TEXT,
    description         TEXT,
    category            TEXT,
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


def init_db(conn: sqlite3.Connection):
    conn.executescript(SCHEMA)
    # Migrate existing DBs: add columns added after initial schema deployment
    for col, col_type in [("restricted", "INTEGER"), ("volume_1wk", "REAL")]:
        existing = {r[1] for r in conn.execute("PRAGMA table_info(markets)").fetchall()}
        if col not in existing:
            conn.execute(f"ALTER TABLE markets ADD COLUMN {col} {col_type}")
    conn.commit()


def parse_ts(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    try:
        s = s.strip().replace(' ', 'T')
        if s.endswith('+00'):
            s = s[:-3] + '+00:00'
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


CATEGORY_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
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


def infer_category(data: dict) -> Optional[str]:
    """Best-effort category extraction.

    Priority:
    1. explicit top-level category
    2. explicit event category
    3. event tags labels/slugs
    4. heuristic from question/title/slug/description
    """
    top_category = data.get("category")
    if isinstance(top_category, str) and top_category.strip():
        return top_category.strip().lower()

    events = data.get("events") or []
    if events:
        ev = events[0] or {}
        ev_category = ev.get("category")
        if isinstance(ev_category, str) and ev_category.strip():
            return ev_category.strip().lower()

        tags = ev.get("tags") or []
        for tag in tags:
            if not isinstance(tag, dict):
                continue
            for key in ("label", "slug", "name"):
                val = tag.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip().lower()

    haystack = " ".join(filter(None, [
        str(data.get("question") or ""),
        str(data.get("description") or ""),
        str(data.get("slug") or ""),
        str((events[0] or {}).get("title") if events else ""),
        str((events[0] or {}).get("slug") if events else ""),
        str(data.get("groupItemTitle") or ""),
    ])).lower()

    for category, keywords in CATEGORY_KEYWORDS:
        if any(re.search(rf"\b{re.escape(k)}\b", haystack) for k in keywords):
            return category

    return None


def parse_market(data: dict) -> Optional[dict]:
    market_id = data.get("id")
    if not market_id:
        return None

    events = data.get("events") or []
    comment_count = None
    if events:
        ev = events[0] or {}
        comment_count = ev.get("commentCount")

    category = infer_category(data)
    start_ts = parse_ts(data.get("startDate"))
    end_ts = parse_ts(data.get("endDate"))
    closed_ts = parse_ts(data.get("closedTime"))

    duration_hours = None
    if start_ts and closed_ts:
        duration_hours = (closed_ts - start_ts) / 3600.0

    volume = data.get("volumeNum")
    if volume is None:
        try:
            volume = float(data.get("volume") or 0)
        except (ValueError, TypeError):
            volume = None

    liquidity = data.get("liquidity")
    fees_enabled = 1 if data.get("feesEnabled") else 0
    neg_risk = 1 if data.get("negRisk") else 0
    group_item_title = data.get("groupItemTitle") or None
    cyom = 1 if data.get("cyom") else 0
    restricted = 1 if data.get("restricted") else 0
    volume_1wk = data.get("volume1wk") or data.get("volume1wkClob")

    return {
        "id": str(market_id),
        "question": data.get("question"),
        "description": data.get("description"),
        "category": category,
        "resolution_source": data.get("resolutionSource", ""),
        "start_date": start_ts,
        "end_date": end_ts,
        "closed_time": closed_ts,
        "duration_hours": duration_hours,
        "volume": volume,
        "liquidity": liquidity,
        "comment_count": comment_count,
        "fees_enabled": fees_enabled,
        "neg_risk": neg_risk,
        "group_item_title": group_item_title,
        "cyom": cyom,
        "restricted": restricted,
        "volume_1wk": volume_1wk,
    }


def parse_tokens(data: dict) -> list[dict]:
    market_id = str(data.get("id", ""))

    def _load_json_list(val, default=None):
        if default is None:
            default = []
        if isinstance(val, list):
            return val
        try:
            return json.loads(val or "[]")
        except Exception:
            return default

    token_ids = _load_json_list(data.get("clobTokenIds", "[]"))
    outcomes = _load_json_list(data.get("outcomes", "[]"))
    price_strs = _load_json_list(data.get("outcomePrices", "[]"))

    rows = []
    for i, token_id in enumerate(token_ids):
        outcome_name = outcomes[i] if i < len(outcomes) else None
        try:
            is_winner = 1 if float(price_strs[i]) >= 0.99 else 0
        except (IndexError, ValueError, TypeError):
            is_winner = 0
        rows.append({
            "token_id": str(token_id),
            "market_id": market_id,
            "outcome_name": outcome_name,
            "is_winner": is_winner,
        })
    return rows


MARKET_UPSERT_SQL = """
INSERT INTO markets (
    id, question, description, category, resolution_source,
    start_date, end_date, closed_time, duration_hours,
    volume, liquidity, comment_count, fees_enabled,
    neg_risk, group_item_title, cyom,
    restricted, volume_1wk
) VALUES (
    :id, :question, :description, :category, :resolution_source,
    :start_date, :end_date, :closed_time, :duration_hours,
    :volume, :liquidity, :comment_count, :fees_enabled,
    :neg_risk, :group_item_title, :cyom,
    :restricted, :volume_1wk
)
ON CONFLICT(id) DO UPDATE SET
    question=excluded.question,
    description=excluded.description,
    category=COALESCE(excluded.category, markets.category),
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

TOKEN_UPSERT_SQL = """
INSERT INTO tokens (token_id, market_id, outcome_name, is_winner)
VALUES (:token_id, :market_id, :outcome_name, :is_winner)
ON CONFLICT(token_id) DO UPDATE SET
    market_id=excluded.market_id,
    outcome_name=excluded.outcome_name,
    is_winner=excluded.is_winner
"""


def parse_day(conn: sqlite3.Connection, day_str: str):
    day_dir = os.path.join(DATABASE_DIR, day_str)
    if not os.path.isdir(day_dir):
        logger.warning(f"[{day_str}] Directory not found: {day_dir}")
        return

    market_files = sorted([f for f in os.listdir(day_dir) if f.endswith('.json')])
    total = len(market_files)
    logger.info(f"[{day_str}] Found {total} market JSONs")

    ok = skipped = err = 0
    t0 = time.monotonic()
    PROGRESS_STEP = 100

    for i, fname in enumerate(market_files, 1):
        market_id = fname[:-5]
        if state_db.is_market_parsed(day_str, market_id):
            skipped += 1
            continue

        fpath = os.path.join(day_dir, fname)
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            logger.warning(f"[{day_str}] {market_id}: read error — {e}")
            err += 1
            continue

        market_row = parse_market(data)
        if not market_row:
            logger.warning(f"[{day_str}] {market_id}: parse_market failed")
            err += 1
            continue

        token_rows = parse_tokens(data)

        try:
            conn.execute(MARKET_UPSERT_SQL, market_row)
            for tr in token_rows:
                conn.execute(TOKEN_UPSERT_SQL, tr)
            conn.commit()
            state_db.mark_parsed(day_str, market_id)
            ok += 1
        except Exception as e:
            conn.rollback()
            logger.warning(f"[{day_str}] {market_id}: DB upsert error — {e}")
            err += 1
            continue

        if i % PROGRESS_STEP == 0:
            elapsed = time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(f"[{day_str}] {i}/{total} — {rate:.0f}/s ETA ~{eta}s | ok={ok} skip={skipped} err={err}")

    elapsed = int(time.monotonic() - t0)
    logger.info(f"[{day_str}] Done in {elapsed}s | ok={ok} skipped={skipped} err={err}")


def run(start: date, end: date):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_db(conn)

    logger.info(f"Parser started: {start} → {end} | DB: {DB_PATH}")

    current = start
    while current <= end:
        parse_day(conn, current.isoformat())
        current += timedelta(days=1)

    conn.close()

    analyzer.run(recompute=False, filter_date=None, filter_market_id=None)
    logger.info("Parser finished.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Polymarket Parser — reads market JSON from database/ and writes normalized metadata")
    ap.add_argument("--start", metavar="YYYY-MM-DD", required=True, help="Начальная дата")
    ap.add_argument("--end", metavar="YYYY-MM-DD", required=True, help="Конечная дата")
    args = ap.parse_args()

    run(
        date.fromisoformat(args.start),
        date.fromisoformat(args.end),
    )
