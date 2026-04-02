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

# Markets longer than this are excluded from trade collection by default.
# Long markets exhaust the 4000-trade API cap without adding useful history
# for the strategy (max_hours_to_close=24h means we never trade them anyway).
MAX_MARKET_DURATION_DAYS = 30


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Download market JSONs
# ═══════════════════════════════════════════════════════════════════════════════

def _market_json_path(day_str: str, market_id: str) -> str:
    return os.path.join(DATABASE_DIR, day_str, f"{market_id}.json")


def _fetch_markets_for_date(day: date) -> list[dict]:
    day_str  = day.isoformat()
    markets: list[dict] = []
    offset = 0

    while True:
        params = {
            "closed": "true",
            "include_tag": "true",
            "end_date_min": f"{day_str}T00:00:00Z",
            "end_date_max": f"{day_str}T23:59:59Z",
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
    saved = skipped = errors = 0
    t_save = time.monotonic()
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

    if saved:
        logger.info(f"[markets][{day_str}] Got {total} — saved={saved} skipped={skipped} errors={errors} ({int(time.monotonic()-t0)}s)")
    else:
        logger.info(f"[markets][{day_str}] Got {total} — all skipped")


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


def _fetch_trades(condition_id: str, start_ts: int, end_ts: int, filter_amount: float = 10) -> tuple[list[dict], bool]:
    """
    Fetch trades for a condition_id.
    Returns (trades, truncated) where truncated=True means the fetch hit the
    offset cap and the history may be incomplete.
    """
    all_trades: list[dict] = []
    offset = 0
    truncated = False

    while True:
        params = {
            "market": condition_id,
            "limit": TRADES_PAGE_LIMIT,
            "offset": offset,
            "filterType": "CASH",
            "filterAmount": filter_amount,
        }
        try:
            resp = requests.get(f"{DATA_API_BASE}/trades", params=params, timeout=30)
            resp.raise_for_status()
            page = resp.json()
        except Exception as e:
            if offset == TRADES_MAX_OFFSET:
                truncated = True
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
            truncated = True
            break

        offset += TRADES_PAGE_LIMIT

    return all_trades, truncated


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


def _manifest_path(day_str: str, market_id: str) -> str:
    return os.path.join(_trades_dir(day_str, market_id), ".complete.json")


def _write_manifest(day_str: str, market_id: str, truncated: bool) -> None:
    manifest = {
        "fetch_complete": not truncated,
        "truncated": truncated,
        "fetched_at": int(time.time()),
    }
    path = _manifest_path(day_str, market_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f)


def _is_fetch_complete(day_str: str, market_id: str) -> bool:
    """Return True only if a manifest exists and says fetch_complete=True."""
    path = _manifest_path(day_str, market_id)
    if not os.path.exists(path):
        return False
    try:
        with open(path, encoding="utf-8") as f:
            return bool(json.load(f).get("fetch_complete"))
    except Exception:
        return False


def _collect_trades_day(day: date, filter_amount: float = 10, max_duration_days: Optional[int] = MAX_MARKET_DURATION_DAYS):
    day_str = day.isoformat()
    day_dir = os.path.join(DATABASE_DIR, day_str)

    if not os.path.isdir(day_dir):
        logger.warning(f"[trades][{day_str}] Directory not found, skipping")
        return

    market_files = [f for f in os.listdir(day_dir) if f.endswith(".json") and not f.startswith("collector")]
    total = len(market_files)
    logger.info(f"[trades][{day_str}] Found {total} markets")

    ok = skipped = skipped_long = errors = 0
    t0 = time.monotonic()

    for i, fname in enumerate(market_files, 1):
        market_id = fname[:-5]

        if _is_fetch_complete(day_str, market_id):
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

        if max_duration_days is not None and start_ts and end_ts:
            duration_days = (end_ts - start_ts) / 86400.0
            if duration_days > max_duration_days:
                skipped_long += 1
                continue

        try:
            trades, truncated = _fetch_trades(condition_id, start_ts, end_ts, filter_amount=filter_amount)
            for token_id, token_trades in _split_by_token(trades).items():
                _save_trades(day_str, market_id, token_id, token_trades)
            _write_manifest(day_str, market_id, truncated=truncated)
            if truncated:
                logger.warning(f"[trades][{day_str}] {market_id}: saved with truncated=True (offset cap hit)")
            ok += 1
        except Exception as e:
            logger.warning(f"[trades][{day_str}] {market_id}: fetch error — {e}")
            errors += 1

        time.sleep(TRADES_SLEEP)

        if i % PROGRESS_STEP == 0:
            elapsed = time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(f"[trades][{day_str}] {i}/{total} — {rate:.1f}/s ETA ~{eta}s | ok={ok} skip={skipped} long={skipped_long} err={errors}")

    logger.info(f"[trades][{day_str}] DONE in {int(time.monotonic()-t0)}s | ok={ok} skipped={skipped} long={skipped_long} errors={errors}")


def collect_trades(start: date, end: date, filter_amount: float = 10, max_duration_days: Optional[int] = MAX_MARKET_DURATION_DAYS):
    label = f"CASH >= {filter_amount}"
    if max_duration_days is not None:
        label += f", duration <= {max_duration_days}d"
    logger.info(f"Trades download: {start} → {end} ({label})")
    current = start
    while current <= end:
        _collect_trades_day(current, filter_amount=filter_amount, max_duration_days=max_duration_days)
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
    neg_risk_market_id  TEXT,
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
    neg_risk, neg_risk_market_id, group_item_title, cyom, restricted, volume_1wk
) VALUES (
    :id, :question, :description, :category, :slug,
    :event_title, :event_slug, :event_description, :tags, :ticker,
    :resolution_source, :start_date, :end_date, :closed_time, :duration_hours,
    :volume, :liquidity, :comment_count, :fees_enabled,
    :neg_risk, :neg_risk_market_id, :group_item_title, :cyom, :restricted, :volume_1wk
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
    neg_risk_market_id=excluded.neg_risk_market_id,
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
        # crypto tokens
        "bnb", "hyperliquid", "hype", "ethena", "ena", "chainlink", "link", "avax", "avalanche",
        "polygon", "matic", "arbitrum", "arb", "optimism", "op ", "sui ", "aptos", "apt ",
        "pepe", "shib", "floki", "wif ", "bonk", "meme", "airdrop", "nft",
        "litecoin", "ltc", "cardano", "ada", "polkadot", "dot ", "uniswap", "uni ",
        "near ", "injective", "inj ", "sei ", "mantle", "mnt ",
        # stock indices
        "nikkei", "dow jones", "dji", "ftse", "dax", "hang seng", "hsi", "russell", "rut ",
        "nya ", "nyk ",
        # individual stocks
        "palantir", "pltr", "airbnb", "abnb", "rocket lab", "rklb", "opendoor",
        "coinbase", "coin ", "robinhood", "hood ", "rivian", "lucid", "rivn",
        # commodities
        "uranium", "natural gas", "copper", "platinum", "wheat", "corn ", "soybean",
        "up or down",
    )),
    ("sports", (
        "fc ", " vs ", "game ", "match ", "mlb", "nba", "nhl", "nfl", "uefa", "champions league",
        "world cup", "premier league", "serie a", "la liga", "bundesliga", "tennis", "f1", "formula 1",
        "ufc", "cricket", "afghanistan", "south africa", "win on ",
        "sailgp", "sailing", "golf", "pga", "masters", "nascar", "indycar", "mls", "wnba",
        "olympics", "wimbledon", "tour de france", "cycling", "swimming", "athletics",
        "boxing", "wrestling", "esports", "league of legends", "dota", "cs2",
        # chess
        "chess", "fide", "world chess", "grand chess tour", "sinquefield cup", "candidates tournament",
        # darts
        "darts", "pdc", "bdo darts",
        # motorsport
        "motogp", "moto2", "moto3", "superbike", "wsbk",
        # pickleball
        "pickleball", "ppa",
        # volleyball
        "volleyball", "beach volleyball",
        # basketball (non-NBA)
        "basketball", "eurobasket", "fiba", "ncaa", "march madness",
        # college sports
        "college football", "college basketball", "cfp", "heisman", "big 10", "big ten", "sec ", "acc ",
        "pac-12", "big 12", "carabao cup", "fa cup", "copa del rey", "dfb pokal",
        # transfer windows / specific sports terms
        "transfer window", "sign with", "sign for",
        # esports (additional)
        "valorant", "overwatch", "rocket league", "lck", "lpl", "cblol", "fncs", "fortnite championship",
        "hltv", "blast", "iem ", "esl pro",
        # other sports
        "rugby", "handball", "snooker", "badminton", "table tennis", "archery", "equestrian",
        "marathon", "triathlon", "ironman", "motocross",
        # tennis tours (ATP/WTA often appear without the word "tennis")
        "atp ", "wta ",
        # horse racing
        "horse racing", "ladbrokes", "jockey", "racecourse", "epsom", "kentucky derby",
        "melbourne cup", "ascot", "cheltenham",
        # additional leagues
        "ligue 1", "ligue 2", "eredivisie", "a-league", "j-league", "k-league",
        "super lig", "primeira liga",
    )),
    ("politics", (
        "trump", "biden", "election", "senate", "house", "republican", "democrat", "white house",
        "prime minister", "president", "governor", "mayor", "parliament", "minister",
        "elon musk", "doge ", "department of",
        "unemployment rate", "gdp", "inflation", "interest rate", "interest rates", "jobs report",
        "nonfarm payroll", "add jobs", "bps after",
        # specific politicians likely to appear in markets
        "zelenskyy", "zelensky", "maduro", "macron", "scholz", "modi ", "erdogan", "netanyahu",
        "sanders", "aoc ", "ocasio-cortez", "pelosi", "mcconnell", "schumer",
        "south korea", "yoon ",
        # policy topics
        "tariff", "sanctions", "veto", "impeach", "resign", "cabinet",
        "supreme court", "federal reserve", "powell", "yellen",
        "bank of england", "european central bank", "ecb ", "bank of japan",
        "redistrict", "border encounter", "border crossing",
    )),
    ("geopolitics", (
        "iran", "iraq", "israel", "gaza", "hamas", "hezbollah", "ukraine", "russia", "china", "taiwan",
        "strike", "missile", "nuclear", "military", "ceasefire", "war", "attack",
        "venezuela", "north korea", "kim jong", "nato", "un security council",
        "coup", "invasion", "occupation", "sanction", "airstrike",
    )),
    ("weather", (
        "temperature", "°c", "°f", "highest temperature", "rain", "snow", "storm", "hurricane",
        "tornado", "typhoon", "earthquake", "magnitude", "flood", "wildfire", "drought",
        "tsa passengers",
    )),
    ("entertainment", (
        "oscar", "grammy", "emmy", "sag awards", "box office", "movie", "album", "netflix", "actor", "actress",
        "rotten tomatoes", "tomatometer", "box office", "spotify", "billboard",
        "casino", "poker", "hustler",
        # awards
        "bafta", "golden globe", "tony award", "cma awards", "ama awards", "mtv awards",
        "dga award", "directors guild", "film independent", "spirit award",
        "goodreads", "book award", "literary award",
        # events
        "eurovision",
        # artists / creators
        "taylor swift", "beyonce", "drake ", "kanye", "rihanna",
        "mrbeast", "youtube views", "tiktok views",
        # tv shows (general patterns)
        "season finale", "box office",
    )),
    ("tech", (
        "openai", "chatgpt", "apple", "google", "microsoft", "nvidia", "tesla", "meta", "amazon",
        "spacex", "starship", "falcon 9",
        "tiktok", "bytedance",
        "anthropic", "gemini", "grok", "deepseek", "claude",
        "alphabet", "waymo", "x.com",
        "ipo", "spac",
        "earnings call", "quarterly earnings",
    )),
    ("health", (
        "measles", "flu ", "influenza", "covid", "coronavirus", "pandemic", "epidemic",
        "vaccine", "vaccination", "cdc ", "who ", "fda approval", "drug approval",
        "hospitalization rate", "infection rate", "outbreak", "ebola", "mpox", "monkeypox",
        "cancer", "clinical trial",
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
        "neg_risk_market_id": data.get("negRiskMarketID") or data.get("negRiskRequestID") or None,
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
        ("neg_risk_market_id", "TEXT"),
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

    ok = errors = 0
    t0 = time.monotonic()

    for i, fname in enumerate(market_files, 1):
        market_id = fname[:-5]

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
            logger.info(f"[parse][{day_str}] {i}/{total} — {rate:.0f}/s ETA ~{eta}s | ok={ok} err={errors}")

    logger.info(f"[parse][{day_str}] DONE in {int(time.monotonic()-t0)}s | ok={ok} errors={errors}")


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
    filter_amount: float = 10,
    max_duration_days: Optional[int] = MAX_MARKET_DURATION_DAYS,
) -> None:
    logger.info(f"Ingest started: {start} → {end}")

    if not skip_markets:
        logger.info("=== Step 1/3: Download markets ===")
        collect_markets(start, end)
    else:
        logger.info("=== Step 1/3: Download markets [SKIPPED] ===")

    if not skip_trades:
        logger.info("=== Step 2/3: Download trades ===")
        collect_trades(start, end, filter_amount=filter_amount, max_duration_days=max_duration_days)
    else:
        logger.info("=== Step 2/3: Download trades [SKIPPED] ===")

    if not skip_parse:
        logger.info("=== Step 3/3: Parse into DB ===")
        parse_markets(start, end)
    else:
        logger.info("=== Step 3/3: Parse into DB [SKIPPED] ===")

    logger.info(f"Ingest finished: {start} → {end}")
    logger.info("Next step: python analyzer/swan_analyzer.py --date-from ... --date-to ...")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Polymarket Data Collector and Parser — download markets + trades + parse into DB"
    )
    ap.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--end",   required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--skip-markets", action="store_true", help="Skip market JSON download")
    ap.add_argument("--skip-trades",  action="store_true", help="Skip trades download")
    ap.add_argument("--skip-parse",   action="store_true", help="Skip DB parsing step")
    ap.add_argument("--filter-amount", type=float, default=10, metavar="USDC",
                    help="Minimum trade size in USDC passed to filterAmount (default: 10). "
                         "Accepts fractional values (e.g. 0.1). Lower values give finer price history.")
    ap.add_argument("--max-market-duration-days", type=int, default=MAX_MARKET_DURATION_DAYS, metavar="DAYS",
                    help=f"Skip trades for markets longer than N days, market JSON is kept "
                         f"(default: {MAX_MARKET_DURATION_DAYS}). Pass 0 to disable the limit.")
    args = ap.parse_args()

    run(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        skip_markets=args.skip_markets,
        skip_trades=args.skip_trades,
        skip_parse=args.skip_parse,
        filter_amount=args.filter_amount,
        max_duration_days=args.max_market_duration_days or None,
    )
