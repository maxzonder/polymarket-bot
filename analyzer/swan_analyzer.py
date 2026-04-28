"""
swan_analyzer.py — Improved Swan Detector (v2)

Отличия от analyzer.py (Zigzag):
1. Учитывает финальную выплату $1 при разрешении рынка (is_winner=1).
   max_traded_x = 1.0 / buy_min_price для победителей (не просто max_price / buy_min_price).
2. Не пропускает "удержанные" победы — токен вырос и остался наверху до закрытия.
3. Одно событие на один вход в зону дна (не множество зигзагов на один рынок).
4. Нет фильтра по времени — стратегия рестинг-лимиток не зависит от длительности.
   Нам важна только ликвидность: сколько можно купить на дне и сколько можно продать на выходе.

Фильтры:
    - buy_volume >= min_buy_volume  (ликвидность на дне — можно войти)
    - sell_volume >= min_sell_volume    (ликвидность на выходе — можно продать)
      Для победителей (is_winner=1) выход = выплата $1 от Polymarket, sell_volume не требуется.
    - max_traded_x >= min_real_x

Использование:
    python scripts/swan_analyzer.py --recompute
    python scripts/swan_analyzer.py --date 2026-02-28
    python scripts/swan_analyzer.py --date-from 2026-02-01 --date-to 2026-02-28
    python scripts/swan_analyzer.py --recompute --min-buy-volume 1.0 --min-sell-volume 5.0
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import math
import time
from typing import Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from utils.paths import DATABASE_DIR, DB_PATH, ensure_runtime_dirs
from config import (
    MODES,
    SWAN_BUY_PRICE_THRESHOLD,
    SWAN_MIN_BUY_VOLUME,
    SWAN_MIN_SELL_VOLUME,
    SWAN_MIN_REAL_X,
    check_swan_buy_price_threshold,
)

logger = setup_logger("swan_analyzer")

# ── Defaults ─────────────────────────────────────────────────────────────────
DEFAULT_BUY_PRICE_THRESHOLD = SWAN_BUY_PRICE_THRESHOLD  # derived from max(entry_price_levels) across all modes
DEFAULT_MIN_BUY_VOLUME  = SWAN_MIN_BUY_VOLUME    # мин. ликвидность на дне (объём сделок < threshold)
DEFAULT_MIN_SELL_VOLUME = SWAN_MIN_SELL_VOLUME   # мин. ликвидность на выходе (объём сделок >= exit_price)
DEFAULT_MIN_REAL_X      = SWAN_MIN_REAL_X        # минимальный реальный икс

# Rule-based layer for "real" black swans: executable low-zone -> sharp
# regime repricing -> winner resolution.  swans_v2 remains the broad x-layer;
# these defaults mark the stricter subset where the market actually changed pose.
DEFAULT_BLACK_SWAN_BUY_PRICE_MAX = 0.05
DEFAULT_BLACK_SWAN_MIN_SHOCK_X = 5.0
DEFAULT_BLACK_SWAN_BREAKOUT_PRICE = 0.20
DEFAULT_BLACK_SWAN_MAX_SHOCK_DELAY_S = 6 * 60 * 60
DEFAULT_BLACK_SWAN_SHOCK_WINDOW_S = 15 * 60
DEFAULT_BLACK_SWAN_MIN_SHOCK_VOLUME = 30.0
DEFAULT_BLACK_SWAN_MIN_BUY_TIME_TO_CLOSE_S = 5 * 60

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS swans_v2 (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id                TEXT NOT NULL,
    market_id               TEXT NOT NULL,
    date                    TEXT NOT NULL,

    -- Параметры запуска анализа
    buy_price_threshold     REAL NOT NULL,
    min_buy_volume          REAL NOT NULL,
    min_sell_volume         REAL NOT NULL,
    min_real_x              REAL NOT NULL,

    -- Зона входа (дно)
    buy_min_price           REAL NOT NULL,
    buy_volume              REAL NOT NULL,
    buy_trade_count         INTEGER NOT NULL,
    buy_ts_first            INTEGER NOT NULL,
    buy_ts_last             INTEGER NOT NULL,

    -- Выход / результат
    sell_volume             REAL NOT NULL,   -- объём сделок >= buy_min_price * min_real_x (0 для победителей)
    max_price_in_history    REAL NOT NULL,
    last_price_in_history   REAL NOT NULL,
    is_winner               INTEGER NOT NULL DEFAULT 0,
    max_traded_x            REAL NOT NULL,   -- итоговый икс (с учётом выплаты $1 для победителей)
    payout_x                REAL NOT NULL,   -- 1/entry если winner, иначе = max_traded_x

    -- Строгий слой настоящих black swan events:
    -- купили winner за копейки, потом рынок резко переоценил исход.
    black_swan              INTEGER NOT NULL DEFAULT 0,
    black_swan_reason       TEXT,
    black_swan_score        REAL,

    -- Когда реально можно было купить относительно жизни рынка / close.
    buy_time_to_close_s     INTEGER,
    buy_time_from_start_s   INTEGER,
    buy_position_pct        REAL,
    buy_phase               TEXT,
    buy_window_s            INTEGER,

    -- Первый подтверждённый shock после дна.
    shock_ts                INTEGER,
    shock_price             REAL,
    shock_time_to_close_s   INTEGER,
    shock_delay_s           INTEGER,
    shock_x                 REAL,
    shock_delta_logit       REAL,
    shock_volume            REAL,
    shock_trade_count       INTEGER,
    shock_phase             TEXT,

    UNIQUE(token_id, date)
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_token ON swans_v2(token_id)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_market ON swans_v2(market_id)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_date ON swans_v2(date)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_max_traded_x ON swans_v2(max_traded_x)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_winner ON swans_v2(is_winner)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_black_swan ON swans_v2(black_swan)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_buy_phase ON swans_v2(buy_phase)",
]

SCHEMA_MIGRATIONS: dict[str, str] = {
    "black_swan": "INTEGER NOT NULL DEFAULT 0",
    "black_swan_reason": "TEXT",
    "black_swan_score": "REAL",
    "buy_time_to_close_s": "INTEGER",
    "buy_time_from_start_s": "INTEGER",
    "buy_position_pct": "REAL",
    "buy_phase": "TEXT",
    "buy_window_s": "INTEGER",
    "shock_ts": "INTEGER",
    "shock_price": "REAL",
    "shock_time_to_close_s": "INTEGER",
    "shock_delay_s": "INTEGER",
    "shock_x": "REAL",
    "shock_delta_logit": "REAL",
    "shock_volume": "REAL",
    "shock_trade_count": "INTEGER",
    "shock_phase": "TEXT",
}


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_TABLE)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(swans_v2)")}
    for column, ddl in SCHEMA_MIGRATIONS.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE swans_v2 ADD COLUMN {column} {ddl}")
    for idx in CREATE_INDEXES:
        conn.execute(idx)
    conn.commit()


def load_trades(date_str: str, market_id: str, token_id: str) -> list[dict]:
    path = os.path.join(DATABASE_DIR, date_str, f"{market_id}_trades", f"{token_id}.json")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        trades = json.load(f)
    # Сортируем от старых к новым (API отдаёт DESC)
    return sorted(trades, key=lambda t: t["timestamp"])


def _sum_usdc(trades: list[dict]) -> float:
    """Суммирует объём в USDC (price * size) по списку трейдов."""
    total = 0.0
    for t in trades:
        try:
            total += float(t["price"]) * float(t["size"])
        except (KeyError, ValueError, TypeError):
            pass
    return total


def _logit(price: float) -> float:
    p = min(max(price, 1e-6), 1 - 1e-6)
    return math.log(p / (1 - p))


def _phase_from_timing(
    ts: int,
    *,
    market_start_ts: int | None = None,
    market_end_ts: int | None = None,
) -> str:
    """Human-readable position of an event inside market lifetime.

    Close-proximity buckets win over generic opening/middle/late labels because
    those are the dangerous false-positive zones we want visible in analytics.
    """
    if market_end_ts:
        to_close = market_end_ts - ts
        if to_close <= 30:
            return "final_seconds"
        if to_close <= 60:
            return "final_minute"
        if to_close <= 5 * 60:
            return "final_5m"
        if to_close <= 15 * 60:
            return "final_15m"
        if to_close <= 60 * 60:
            return "final_hour"
        if to_close <= 6 * 60 * 60:
            return "final_6h"

    if market_start_ts and market_end_ts and market_end_ts > market_start_ts:
        pct = (ts - market_start_ts) / (market_end_ts - market_start_ts)
        if pct <= 0.10:
            return "opening_10pct"
        if pct >= 0.75:
            return "late"
        return "middle"

    return "unknown"


def _timing_features(
    ts: int,
    *,
    market_start_ts: int | None = None,
    market_end_ts: int | None = None,
) -> dict[str, int | float | str | None]:
    time_to_close = market_end_ts - ts if market_end_ts else None
    time_from_start = ts - market_start_ts if market_start_ts else None
    position_pct = None
    if market_start_ts and market_end_ts and market_end_ts > market_start_ts:
        position_pct = (ts - market_start_ts) / (market_end_ts - market_start_ts)
    return {
        "time_to_close_s": time_to_close,
        "time_from_start_s": time_from_start,
        "position_pct": position_pct,
        "phase": _phase_from_timing(ts, market_start_ts=market_start_ts, market_end_ts=market_end_ts),
    }


def _find_regime_shock(
    trades: list[dict],
    *,
    floor_end: int,
    buy_min_price: float,
    market_start_ts: int | None,
    market_end_ts: int | None,
    breakout_price: float,
    min_shock_x: float,
    shock_window_s: int,
) -> dict[str, int | float | str | None]:
    """Find first confirmed repricing after floor-zone.

    The trigger is intentionally stricter than max(price)/min(price): it records
    when the token first becomes materially repriced, then measures confirming
    cash volume/trade count inside a short window.
    """
    target_price = max(breakout_price, buy_min_price * min_shock_x)
    shock_idx = None
    for idx in range(floor_end + 1, len(trades)):
        if float(trades[idx]["price"]) >= target_price:
            shock_idx = idx
            break

    if shock_idx is None:
        return {
            "shock_ts": None,
            "shock_price": None,
            "shock_time_to_close_s": None,
            "shock_delay_s": None,
            "shock_x": None,
            "shock_delta_logit": None,
            "shock_volume": 0.0,
            "shock_trade_count": 0,
            "shock_phase": None,
        }

    shock_trade = trades[shock_idx]
    shock_ts = int(shock_trade["timestamp"])
    shock_price = float(shock_trade["price"])
    window_end_ts = shock_ts + shock_window_s
    confirming = [
        t for t in trades[shock_idx:]
        if int(t["timestamp"]) <= window_end_ts and float(t["price"]) >= target_price
    ]
    timing = _timing_features(shock_ts, market_start_ts=market_start_ts, market_end_ts=market_end_ts)
    return {
        "shock_ts": shock_ts,
        "shock_price": shock_price,
        "shock_time_to_close_s": timing["time_to_close_s"],
        "shock_delay_s": shock_ts - int(trades[floor_end]["timestamp"]),
        "shock_x": shock_price / buy_min_price,
        "shock_delta_logit": _logit(shock_price) - _logit(buy_min_price),
        "shock_volume": _sum_usdc(confirming),
        "shock_trade_count": len(confirming),
        "shock_phase": timing["phase"],
    }


def _black_swan_decision(
    *,
    is_winner: int,
    buy_min_price: float,
    buy_volume: float,
    buy_trade_count: int,
    buy_time_to_close_s: int | None,
    shock: dict[str, int | float | str | None],
    buy_price_max: float,
    max_shock_delay_s: int,
    min_shock_volume: float,
    min_buy_time_to_close_s: int,
    min_buy_volume: float,
) -> tuple[int, str, float]:
    checks: list[tuple[str, bool]] = [
        ("winner", is_winner == 1),
        ("penny_buy", buy_min_price <= buy_price_max),
        ("buyable_floor", buy_volume >= min_buy_volume and (buy_trade_count >= 2 or buy_volume >= 10.0)),
        ("not_last_seconds", buy_time_to_close_s is None or buy_time_to_close_s >= min_buy_time_to_close_s),
        ("shock_found", shock["shock_ts"] is not None),
        ("sharp_reprice", shock["shock_delay_s"] is not None and int(shock["shock_delay_s"]) <= max_shock_delay_s),
        ("shock_confirmed", float(shock["shock_volume"] or 0.0) >= min_shock_volume and int(shock["shock_trade_count"] or 0) >= 2),
    ]
    passed = [name for name, ok in checks if ok]
    failed = [name for name, ok in checks if not ok]
    black_swan = 1 if not failed else 0

    # Transparent score for ranking near-misses without making them labels.
    score = len(passed) / len(checks)
    if shock["shock_x"]:
        score += min(float(shock["shock_x"] or 0.0) / 100.0, 0.25)
    if buy_time_to_close_s is not None and buy_time_to_close_s < min_buy_time_to_close_s:
        score -= 0.25
    score = max(0.0, min(score, 1.0))

    reason = "pass" if black_swan else "failed:" + ",".join(failed)
    return black_swan, reason, score


def _find_floor_zones(
    prices: list[float], threshold: float
) -> list[tuple[int, int]]:
    """
    Find all contiguous zones where price <= threshold.
    Returns list of (start_idx, end_idx) sorted by minimum price ascending
    (lowest floor first = highest potential x).
    """
    zones: list[tuple[int, int]] = []
    i = 0
    while i < len(prices):
        if prices[i] <= threshold:
            start = i
            while i < len(prices) and prices[i] <= threshold:
                i += 1
            zones.append((start, i - 1))
        else:
            i += 1
    zones.sort(key=lambda z: min(prices[z[0]: z[1] + 1]))
    return zones


def analyze_token(
    trades: list[dict],
    is_winner: int,
    buy_price_threshold: float,
    min_buy_volume: float,
    min_sell_volume: float,
    min_real_x: float,
    market_start_ts: int | None = None,
    market_end_ts: int | None = None,
    black_swan_buy_price_max: float = DEFAULT_BLACK_SWAN_BUY_PRICE_MAX,
    black_swan_min_shock_x: float = DEFAULT_BLACK_SWAN_MIN_SHOCK_X,
    black_swan_breakout_price: float = DEFAULT_BLACK_SWAN_BREAKOUT_PRICE,
    black_swan_max_shock_delay_s: int = DEFAULT_BLACK_SWAN_MAX_SHOCK_DELAY_S,
    black_swan_shock_window_s: int = DEFAULT_BLACK_SWAN_SHOCK_WINDOW_S,
    black_swan_min_shock_volume: float = DEFAULT_BLACK_SWAN_MIN_SHOCK_VOLUME,
    black_swan_min_buy_time_to_close_s: int = DEFAULT_BLACK_SWAN_MIN_BUY_TIME_TO_CLOSE_S,
) -> Optional[dict]:
    """
    Анализирует историю трейдов одного токена.
    Возвращает dict с метриками или None если лебедь не найден.

    Логика:
    1. Найти все contiguous floor-zones где price <= buy_price_threshold
    2. Перебрать зоны в порядке возрастания минимальной цены (самый глубокий флор первым)
    3. Выбрать первую зону, где buy_volume >= min_buy_volume
       (micro-print без ликвидности не блокирует поиск реального floor event)
    4. Для не-победителей: проверить sell_volume >= min_sell_volume
       sell_volume = объём сделок после зоны дна по цене >= buy_min_price * min_real_x
    5. Для победителей (is_winner=1): Polymarket выплачивает $1 → sell_volume не нужен
    6. max_traded_x = 1/buy_min_price если winner, иначе max(price_after_floor) / buy_min_price
    """
    if not trades:
        return None

    prices = [float(t["price"]) for t in trades]

    # --- 1. Найти все floor-zones и выбрать первую с достаточным buy_volume ---
    zones = _find_floor_zones(prices, buy_price_threshold)
    if not zones:
        return None  # цена никогда не падала до порога

    floor_start = floor_end = -1
    for zs, ze in zones:
        zone_trades = trades[zs: ze + 1]
        if _sum_usdc(zone_trades) >= min_buy_volume:
            floor_start, floor_end = zs, ze
            break

    if floor_start == -1:
        return None  # ни одна floor-zone не прошла buy_volume gate

    floor_trades = trades[floor_start: floor_end + 1]
    buy_volume = _sum_usdc(floor_trades)
    buy_min_price = min(prices[floor_start: floor_end + 1])
    buy_ts_first = int(floor_trades[0]["timestamp"])
    buy_ts_last = int(floor_trades[-1]["timestamp"])
    buy_window_s = max(0, buy_ts_last - buy_ts_first)
    buy_timing = _timing_features(
        buy_ts_last,
        market_start_ts=market_start_ts,
        market_end_ts=market_end_ts,
    )

    # --- 4. Метрики после зоны дна ---
    after_floor = trades[floor_end + 1:]
    max_price_after = max((float(t["price"]) for t in after_floor), default=buy_min_price)
    max_price_overall = max(prices)
    last_price = prices[-1]

    # --- 5. Ликвидность на выходе (только для не-победителей) ---
    sell_volume = 0.0
    if is_winner == 1:
        # Polymarket выплачивает $1 — sell ликвидность не нужна
        sell_volume = 0.0
    else:
        # Считаем объём сделок по цене >= buy_min_price * min_real_x (после зоны дна)
        sell_trades = [t for t in after_floor if float(t["price"]) >= buy_min_price * min_real_x]
        sell_volume = _sum_usdc(sell_trades)
        if sell_volume < min_sell_volume:
            return None

    # --- 6. Считаем max_traded_x ---
    if is_winner == 1:
        max_traded_x = 1.0 / buy_min_price
    else:
        max_traded_x = max_price_after / buy_min_price if max_price_after > buy_min_price else 1.0

    payout_x = (1.0 / buy_min_price) if is_winner == 1 else max_traded_x

    if max_traded_x < min_real_x:
        return None

    shock = _find_regime_shock(
        trades,
        floor_end=floor_end,
        buy_min_price=buy_min_price,
        market_start_ts=market_start_ts,
        market_end_ts=market_end_ts,
        breakout_price=black_swan_breakout_price,
        min_shock_x=black_swan_min_shock_x,
        shock_window_s=black_swan_shock_window_s,
    )
    black_swan, black_swan_reason, black_swan_score = _black_swan_decision(
        is_winner=is_winner,
        buy_min_price=buy_min_price,
        buy_volume=buy_volume,
        buy_trade_count=len(floor_trades),
        buy_time_to_close_s=buy_timing["time_to_close_s"],
        shock=shock,
        buy_price_max=black_swan_buy_price_max,
        max_shock_delay_s=black_swan_max_shock_delay_s,
        min_shock_volume=black_swan_min_shock_volume,
        min_buy_time_to_close_s=black_swan_min_buy_time_to_close_s,
        min_buy_volume=min_buy_volume,
    )

    return {
        "buy_price_threshold": buy_price_threshold,
        "min_buy_volume": min_buy_volume,
        "min_sell_volume": min_sell_volume,
        "min_real_x": min_real_x,
        "buy_min_price": buy_min_price,
        "buy_volume": buy_volume,
        "buy_trade_count": len(floor_trades),
        "buy_ts_first": buy_ts_first,
        "buy_ts_last": buy_ts_last,
        "sell_volume": sell_volume,
        "max_price_in_history": max_price_overall,
        "last_price_in_history": last_price,
        "is_winner": is_winner,
        "max_traded_x": max_traded_x,
        "payout_x": payout_x,
        "black_swan": black_swan,
        "black_swan_reason": black_swan_reason,
        "black_swan_score": black_swan_score,
        "buy_time_to_close_s": buy_timing["time_to_close_s"],
        "buy_time_from_start_s": buy_timing["time_from_start_s"],
        "buy_position_pct": buy_timing["position_pct"],
        "buy_phase": buy_timing["phase"],
        "buy_window_s": buy_window_s,
        **shock,
    }


def run(
    conn: sqlite3.Connection,
    filter_date: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    recompute: bool = False,
    buy_price_threshold: float = DEFAULT_BUY_PRICE_THRESHOLD,
    min_buy_volume: float = DEFAULT_MIN_BUY_VOLUME,
    min_sell_volume: float = DEFAULT_MIN_SELL_VOLUME,
    min_real_x: float = DEFAULT_MIN_REAL_X,
    black_swan_buy_price_max: float = DEFAULT_BLACK_SWAN_BUY_PRICE_MAX,
    black_swan_min_shock_x: float = DEFAULT_BLACK_SWAN_MIN_SHOCK_X,
    black_swan_breakout_price: float = DEFAULT_BLACK_SWAN_BREAKOUT_PRICE,
    black_swan_max_shock_delay_s: int = DEFAULT_BLACK_SWAN_MAX_SHOCK_DELAY_S,
    black_swan_shock_window_s: int = DEFAULT_BLACK_SWAN_SHOCK_WINDOW_S,
    black_swan_min_shock_volume: float = DEFAULT_BLACK_SWAN_MIN_SHOCK_VOLUME,
    black_swan_min_buy_time_to_close_s: int = DEFAULT_BLACK_SWAN_MIN_BUY_TIME_TO_CLOSE_S,
) -> None:
    ensure_runtime_dirs()

    if recompute:
        conn.execute("DELETE FROM swans_v2")
        conn.commit()
        logger.info("Recompute: cleared swans_v2")

    # Строим folder index (date → set of market_ids)
    logger.info("Building folder index...")
    folder_index: dict[str, set[str]] = {}
    if DATABASE_DIR.exists():
        for entry in os.scandir(DATABASE_DIR):
            if entry.is_dir() and not entry.name.endswith(".db"):
                folder_index[entry.name] = set()
                for sub in os.scandir(entry.path):
                    if sub.is_dir() and sub.name.endswith("_trades"):
                        folder_index[entry.name].add(sub.name[:-7])

    # Получаем токены из БД
    market_ids: Optional[set] = None
    if filter_date:
        market_ids = folder_index.get(filter_date, set())
        if not market_ids:
            logger.error(f"No trade folders found for date {filter_date}")
            return
    elif date_from and date_to:
        dates = sorted(d for d in folder_index if date_from <= d <= date_to)
        market_ids = {mid for d in dates for mid in folder_index[d]}
        if not market_ids:
            logger.error("No trade folders found for given date range")
            return

    if market_ids is not None:
        conn.execute("CREATE TEMP TABLE _mids (market_id TEXT PRIMARY KEY)")
        conn.executemany("INSERT OR IGNORE INTO _mids VALUES (?)", [(m,) for m in market_ids])
        tokens = conn.execute(
            "SELECT t.token_id, t.market_id, t.is_winner, mkt.start_date, "
            "COALESCE(mkt.closed_time, mkt.end_date) AS market_end_ts FROM tokens t"
            " JOIN _mids m ON t.market_id = m.market_id"
            " LEFT JOIN markets mkt ON mkt.id = t.market_id"
        ).fetchall()
        conn.execute("DROP TABLE _mids")
    else:
        tokens = conn.execute(
            "SELECT t.token_id, t.market_id, t.is_winner, m.start_date, "
            "COALESCE(m.closed_time, m.end_date) AS market_end_ts "
            "FROM tokens t LEFT JOIN markets m ON m.id = t.market_id"
        ).fetchall()

    total = len(tokens)
    logger.info(
        f"Processing {total} tokens | "
        f"threshold=${buy_price_threshold}, min_buy=${min_buy_volume}, "
        f"min_sell=${min_sell_volume}, min_real_x={min_real_x}x"
    )

    ok = no_trades = no_swan = rejected = errors = 0
    t0 = time.monotonic()

    for i, (token_id, market_id, is_winner, market_start_ts, market_end_ts) in enumerate(tokens, 1):
        # Найти дату этого токена
        token_date = None
        for d, mids in folder_index.items():
            if market_id in mids:
                if filter_date and d != filter_date:
                    continue
                if date_from and date_to and not (date_from <= d <= date_to):
                    continue
                token_date = d
                break

        if not token_date:
            no_trades += 1
            continue

        trades = load_trades(token_date, market_id, token_id)
        if not trades:
            no_trades += 1
            continue

        try:
            result = analyze_token(
                trades,
                is_winner=is_winner or 0,
                buy_price_threshold=buy_price_threshold,
                min_buy_volume=min_buy_volume,
                min_sell_volume=min_sell_volume,
                min_real_x=min_real_x,
                market_start_ts=market_start_ts,
                market_end_ts=market_end_ts,
                black_swan_buy_price_max=black_swan_buy_price_max,
                black_swan_min_shock_x=black_swan_min_shock_x,
                black_swan_breakout_price=black_swan_breakout_price,
                black_swan_max_shock_delay_s=black_swan_max_shock_delay_s,
                black_swan_shock_window_s=black_swan_shock_window_s,
                black_swan_min_shock_volume=black_swan_min_shock_volume,
                black_swan_min_buy_time_to_close_s=black_swan_min_buy_time_to_close_s,
            )
        except Exception as e:
            logger.warning(f"{token_id}: analysis error — {e}")
            errors += 1
            continue

        if result is None:
            no_swan += 1
            continue

        try:
            conn.execute(
                """
                INSERT INTO swans_v2 (
                    token_id, market_id, date,
                    buy_price_threshold, min_buy_volume, min_sell_volume,
                    min_real_x,
                    buy_min_price, buy_volume, buy_trade_count,
                    buy_ts_first, buy_ts_last,
                    sell_volume,
                    max_price_in_history, last_price_in_history,
                    is_winner, max_traded_x, payout_x,
                    black_swan, black_swan_reason, black_swan_score,
                    buy_time_to_close_s, buy_time_from_start_s,
                    buy_position_pct, buy_phase, buy_window_s,
                    shock_ts, shock_price, shock_time_to_close_s,
                    shock_delay_s, shock_x, shock_delta_logit,
                    shock_volume, shock_trade_count, shock_phase
                ) VALUES (
                    :token_id, :market_id, :date,
                    :buy_price_threshold, :min_buy_volume, :min_sell_volume,
                    :min_real_x,
                    :buy_min_price, :buy_volume, :buy_trade_count,
                    :buy_ts_first, :buy_ts_last,
                    :sell_volume,
                    :max_price_in_history, :last_price_in_history,
                    :is_winner, :max_traded_x, :payout_x,
                    :black_swan, :black_swan_reason, :black_swan_score,
                    :buy_time_to_close_s, :buy_time_from_start_s,
                    :buy_position_pct, :buy_phase, :buy_window_s,
                    :shock_ts, :shock_price, :shock_time_to_close_s,
                    :shock_delay_s, :shock_x, :shock_delta_logit,
                    :shock_volume, :shock_trade_count, :shock_phase
                ) ON CONFLICT(token_id, date) DO UPDATE SET
                    buy_price_threshold   = excluded.buy_price_threshold,
                    min_buy_volume        = excluded.min_buy_volume,
                    min_sell_volume       = excluded.min_sell_volume,
                    min_real_x            = excluded.min_real_x,
                    buy_min_price         = excluded.buy_min_price,
                    buy_volume            = excluded.buy_volume,
                    buy_trade_count       = excluded.buy_trade_count,
                    buy_ts_first          = excluded.buy_ts_first,
                    buy_ts_last           = excluded.buy_ts_last,
                    sell_volume           = excluded.sell_volume,
                    max_price_in_history  = excluded.max_price_in_history,
                    last_price_in_history = excluded.last_price_in_history,
                    is_winner             = excluded.is_winner,
                    max_traded_x          = excluded.max_traded_x,
                    payout_x              = excluded.payout_x,
                    black_swan            = excluded.black_swan,
                    black_swan_reason     = excluded.black_swan_reason,
                    black_swan_score      = excluded.black_swan_score,
                    buy_time_to_close_s   = excluded.buy_time_to_close_s,
                    buy_time_from_start_s = excluded.buy_time_from_start_s,
                    buy_position_pct      = excluded.buy_position_pct,
                    buy_phase             = excluded.buy_phase,
                    buy_window_s          = excluded.buy_window_s,
                    shock_ts              = excluded.shock_ts,
                    shock_price           = excluded.shock_price,
                    shock_time_to_close_s = excluded.shock_time_to_close_s,
                    shock_delay_s         = excluded.shock_delay_s,
                    shock_x               = excluded.shock_x,
                    shock_delta_logit     = excluded.shock_delta_logit,
                    shock_volume          = excluded.shock_volume,
                    shock_trade_count     = excluded.shock_trade_count,
                    shock_phase           = excluded.shock_phase
                """,
                {
                    "token_id": token_id,
                    "market_id": market_id,
                    "date": token_date,
                    **result,
                },
            )
            ok += 1
        except Exception as e:
            logger.warning(f"{token_id}: DB insert error — {e}")
            rejected += 1
            continue

        if i % 5000 == 0:
            conn.commit()
            elapsed = time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(
                f"{i}/{total} — {rate:.0f}/s ETA ~{eta}s | "
                f"ok={ok} no_trades={no_trades} no_swan={no_swan} "
                f"rejected={rejected} errors={errors}"
            )

    conn.commit()  # flush final batch
    elapsed = int(time.monotonic() - t0)
    row = conn.execute(
        "SELECT COUNT(*), AVG(max_traded_x), MAX(max_traded_x), SUM(is_winner), AVG(buy_volume), SUM(black_swan) FROM swans_v2"
    ).fetchone()

    logger.info(
        f"Done in {elapsed}s | ok={ok} no_trades={no_trades} no_swan={no_swan} "
        f"rejected={rejected} errors={errors}"
    )
    if row and row[0]:
        winners = row[3] or 0
        logger.info(
            f"Stats: total={row[0]}, avg_real_x={row[1]:.2f}x, max_real_x={row[2]:.0f}x, "
            f"winners={winners} ({winners/row[0]*100:.0f}%), "
            f"black_swans={row[5] or 0} ({(row[5] or 0)/row[0]*100:.1f}%), "
            f"avg_entry_liq=${row[4]:.2f}"
        )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Swan Analyzer v2 — no time filters, only entry+exit liquidity"
    )
    ap.add_argument("--date", metavar="YYYY-MM-DD")
    ap.add_argument("--date-from", metavar="YYYY-MM-DD")
    ap.add_argument("--date-to", metavar="YYYY-MM-DD")
    ap.add_argument("--recompute", action="store_true", help="Очистить swans_v2 и пересчитать")
    ap.add_argument("--buy-price-threshold", type=float, default=DEFAULT_BUY_PRICE_THRESHOLD,
                    help=f"Порог цены дна (default: ${DEFAULT_BUY_PRICE_THRESHOLD})")
    ap.add_argument("--min-buy-volume", type=float, default=DEFAULT_MIN_BUY_VOLUME,
                    help=f"Мин. ликвидность на входе (default: ${DEFAULT_MIN_BUY_VOLUME})")
    ap.add_argument("--min-sell-volume", type=float, default=DEFAULT_MIN_SELL_VOLUME,
                    help=f"Мин. ликвидность на выходе (default: ${DEFAULT_MIN_SELL_VOLUME})")
    ap.add_argument("--min-real-x", type=float, default=DEFAULT_MIN_REAL_X)
    ap.add_argument("--black-swan-buy-price-max", type=float, default=DEFAULT_BLACK_SWAN_BUY_PRICE_MAX,
                    help=f"Max executable low-zone price for strict black_swan label (default: {DEFAULT_BLACK_SWAN_BUY_PRICE_MAX})")
    ap.add_argument("--black-swan-min-shock-x", type=float, default=DEFAULT_BLACK_SWAN_MIN_SHOCK_X,
                    help=f"Min repricing multiple from buy_min_price to shock (default: {DEFAULT_BLACK_SWAN_MIN_SHOCK_X}x)")
    ap.add_argument("--black-swan-breakout-price", type=float, default=DEFAULT_BLACK_SWAN_BREAKOUT_PRICE,
                    help=f"Absolute repricing threshold for shock detection (default: {DEFAULT_BLACK_SWAN_BREAKOUT_PRICE})")
    ap.add_argument("--black-swan-max-shock-delay-s", type=int, default=DEFAULT_BLACK_SWAN_MAX_SHOCK_DELAY_S,
                    help=f"Max seconds from buy-zone to repricing shock (default: {DEFAULT_BLACK_SWAN_MAX_SHOCK_DELAY_S})")
    ap.add_argument("--black-swan-shock-window-s", type=int, default=DEFAULT_BLACK_SWAN_SHOCK_WINDOW_S,
                    help=f"Confirmation window after first shock print (default: {DEFAULT_BLACK_SWAN_SHOCK_WINDOW_S})")
    ap.add_argument("--black-swan-min-shock-volume", type=float, default=DEFAULT_BLACK_SWAN_MIN_SHOCK_VOLUME,
                    help=f"Min USDC volume at/above shock threshold inside shock window (default: {DEFAULT_BLACK_SWAN_MIN_SHOCK_VOLUME})")
    ap.add_argument("--black-swan-min-buy-time-to-close-s", type=int, default=DEFAULT_BLACK_SWAN_MIN_BUY_TIME_TO_CLOSE_S,
                    help=f"Do not label buys closer to close than this (default: {DEFAULT_BLACK_SWAN_MIN_BUY_TIME_TO_CLOSE_S})")
    args = ap.parse_args()

    for mode in MODES.values():
        for w in check_swan_buy_price_threshold(mode):
            if args.buy_price_threshold < SWAN_BUY_PRICE_THRESHOLD:
                # We are lowering it or it mismatches config.py constant
                pass
            logger.warning(w)


    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Пересоздаём таблицу (схема изменилась — убрали floor_duration, добавили exit_volume)
    conn.execute("DROP TABLE IF EXISTS swans_v2") if args.recompute else None
    init_db(conn)

    run(
        conn,
        filter_date=args.date,
        date_from=args.date_from,
        date_to=args.date_to,
        recompute=False,  # уже очищено выше если recompute
        buy_price_threshold=args.buy_price_threshold,
        min_buy_volume=args.min_buy_volume,
        min_sell_volume=args.min_sell_volume,
        min_real_x=args.min_real_x,
        black_swan_buy_price_max=args.black_swan_buy_price_max,
        black_swan_min_shock_x=args.black_swan_min_shock_x,
        black_swan_breakout_price=args.black_swan_breakout_price,
        black_swan_max_shock_delay_s=args.black_swan_max_shock_delay_s,
        black_swan_shock_window_s=args.black_swan_shock_window_s,
        black_swan_min_shock_volume=args.black_swan_min_shock_volume,
        black_swan_min_buy_time_to_close_s=args.black_swan_min_buy_time_to_close_s,
    )
    conn.close()


if __name__ == "__main__":
    main()
