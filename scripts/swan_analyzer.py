"""
swan_analyzer.py — Improved Swan Detector

Отличия от analyzer.py (Zigzag):
1. Учитывает финальную выплату $1 при разрешении рынка (is_winner=1).
   Реальный икс = 1.0 / entry_min_price, а не просто max_price / entry_min_price.
2. Не пропускает "удержанные" победы (токен вырос и остался наверху до закрытия).
   Событие записывается не только при откате, но и при любом значимом росте.
3. Одно событие на один вход в зону дна (не множество зигзагов на один рынок).

Логика:
    Для каждого токена:
    - Найти момент, когда цена упала ниже entry_threshold (по умолчанию $0.02)
    - Посчитать ликвидность и длительность на дне
    - Посчитать real_x = 1/entry_min если winner, иначе max_price_after / entry_min
    - Записать в таблицу swans_v2

Использование:
    python scripts/swan_analyzer.py --recompute
    python scripts/swan_analyzer.py --date 2026-02-28
    python scripts/swan_analyzer.py --date-from 2026-02-01 --date-to 2026-02-28
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from typing import Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from utils.paths import DATABASE_DIR, DB_PATH, ensure_runtime_dirs

logger = setup_logger("swan_analyzer")

# ── Defaults ─────────────────────────────────────────────────────────────────
DEFAULT_ENTRY_THRESHOLD = 0.02   # цена дна: < $0.02 = зона входа
DEFAULT_MIN_ENTRY_USDC = 10.0    # мин. ликвидность на дне
DEFAULT_MIN_REAL_X = 5.0         # минимальный реальный икс чтобы считать "лебедем"
DEFAULT_MIN_FLOOR_MINUTES = 5.0  # мин. время сидения на дне

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS swans_v2 (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id                    TEXT NOT NULL,
    market_id                   TEXT NOT NULL,
    date                        TEXT NOT NULL,

    -- Параметры запуска анализа
    entry_threshold             REAL NOT NULL,
    min_entry_liquidity         REAL NOT NULL,
    min_real_x                  REAL NOT NULL,

    -- Зона входа (дно)
    entry_min_price             REAL NOT NULL,
    entry_volume_usdc           REAL NOT NULL,
    entry_trade_count           INTEGER NOT NULL,
    entry_ts_first              INTEGER NOT NULL,
    entry_ts_last               INTEGER NOT NULL,
    floor_duration_seconds      INTEGER NOT NULL,

    -- Выход / результат
    max_price_in_history        REAL NOT NULL,
    last_price_in_history       REAL NOT NULL,
    is_winner                   INTEGER NOT NULL DEFAULT 0,
    real_x                      REAL NOT NULL,
    resolution_x                REAL NOT NULL,
    time_to_last_trade_hours    REAL,

    UNIQUE(token_id, date)
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_token ON swans_v2(token_id)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_market ON swans_v2(market_id)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_date ON swans_v2(date)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_real_x ON swans_v2(real_x)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_winner ON swans_v2(is_winner)",
]


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_TABLE)
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
    total = 0.0
    for t in trades:
        try:
            total += float(t["price"]) * float(t["size"])
        except (KeyError, ValueError, TypeError):
            pass
    return total


def analyze_token(
    trades: list[dict],
    is_winner: int,
    entry_threshold: float,
    min_entry_usdc: float,
    min_real_x: float,
    min_floor_minutes: float,
) -> Optional[dict]:
    """
    Анализирует историю трейдов одного токена.
    Возвращает dict с метриками или None если лебедь не найден.
    """
    if len(trades) < 2:
        return None

    prices = [float(t["price"]) for t in trades]

    # --- Найти зону дна ---
    # Ищем первый трейд ниже entry_threshold
    floor_start_idx = None
    for i, p in enumerate(prices):
        if p < entry_threshold:
            floor_start_idx = i
            break

    if floor_start_idx is None:
        return None  # цена никогда не падала до порога

    # Собрать все трейды пока цена остаётся < entry_threshold (contiguous zone)
    floor_trades = []
    floor_end_idx = floor_start_idx
    for i in range(floor_start_idx, len(trades)):
        if prices[i] < entry_threshold:
            floor_trades.append(trades[i])
            floor_end_idx = i
        else:
            # Допускаем небольшие всплески выше порога (шум)
            # Но если цена ушла выше threshold и больше не возвращалась — выходим
            returned = any(p < entry_threshold for p in prices[i:i + 5])
            if not returned:
                break

    if not floor_trades:
        return None

    # --- Метрики зоны входа ---
    entry_volume = _sum_usdc(floor_trades)
    if entry_volume < min_entry_usdc:
        return None

    entry_min_price = min(float(t["price"]) for t in floor_trades)
    entry_ts_first = int(floor_trades[0]["timestamp"])
    entry_ts_last = int(floor_trades[-1]["timestamp"])
    floor_duration_sec = max(0, entry_ts_last - entry_ts_first)

    if floor_duration_sec < min_floor_minutes * 60:
        return None

    # --- Метрики после зоны входа ---
    after_floor = trades[floor_end_idx + 1:]
    if not after_floor and is_winner == 0:
        return None  # нет движения и не победил

    max_price_after = max((float(t["price"]) for t in after_floor), default=entry_min_price)
    max_price_overall = max(prices)
    last_price = prices[-1]

    # --- Считаем real_x ---
    # Если рынок разрешился в нашу пользу — Polymarket заплатит $1 за токен
    if is_winner == 1:
        real_x = 1.0 / entry_min_price
    else:
        # Используем максимальную цену после зоны входа
        real_x = max_price_after / entry_min_price if max_price_after > entry_min_price else 1.0

    # resolution_x — "чистый" икс от разрешения (только для победителей)
    resolution_x = (1.0 / entry_min_price) if is_winner == 1 else real_x

    if real_x < min_real_x:
        return None

    last_trade_ts = int(trades[-1]["timestamp"])
    time_to_last_hours = (last_trade_ts - entry_ts_first) / 3600.0

    return {
        "entry_threshold": entry_threshold,
        "min_entry_liquidity": min_entry_usdc,
        "min_real_x": min_real_x,
        "entry_min_price": entry_min_price,
        "entry_volume_usdc": entry_volume,
        "entry_trade_count": len(floor_trades),
        "entry_ts_first": entry_ts_first,
        "entry_ts_last": entry_ts_last,
        "floor_duration_seconds": floor_duration_sec,
        "max_price_in_history": max_price_overall,
        "last_price_in_history": last_price,
        "is_winner": is_winner,
        "real_x": real_x,
        "resolution_x": resolution_x,
        "time_to_last_trade_hours": time_to_last_hours,
    }


def run(
    conn: sqlite3.Connection,
    filter_date: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    recompute: bool = False,
    entry_threshold: float = DEFAULT_ENTRY_THRESHOLD,
    min_entry_usdc: float = DEFAULT_MIN_ENTRY_USDC,
    min_real_x: float = DEFAULT_MIN_REAL_X,
    min_floor_minutes: float = DEFAULT_MIN_FLOOR_MINUTES,
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
    clauses, params = [], []
    if filter_date:
        market_ids = list(folder_index.get(filter_date, set()))
        if not market_ids:
            logger.error(f"No trade folders found for date {filter_date}")
            return
        placeholders = ",".join("?" * len(market_ids))
        clauses.append(f"market_id IN ({placeholders})")
        params.extend(market_ids)
    elif date_from and date_to:
        dates = sorted(d for d in folder_index if date_from <= d <= date_to)
        market_ids = list({mid for d in dates for mid in folder_index[d]})
        if not market_ids:
            logger.error("No trade folders found for given date range")
            return
        placeholders = ",".join("?" * len(market_ids))
        clauses.append(f"market_id IN ({placeholders})")
        params.extend(market_ids)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    tokens = conn.execute(
        f"SELECT token_id, market_id, is_winner FROM tokens {where_sql}", params
    ).fetchall()

    total = len(tokens)
    logger.info(
        f"Processing {total} tokens | "
        f"threshold=${entry_threshold}, min_entry=${min_entry_usdc}, "
        f"min_real_x={min_real_x}x, min_floor={min_floor_minutes}m"
    )

    ok = no_trades = no_swan = rejected = errors = 0
    swans_total = 0
    t0 = time.monotonic()

    for i, (token_id, market_id, is_winner) in enumerate(tokens, 1):
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
                entry_threshold=entry_threshold,
                min_entry_usdc=min_entry_usdc,
                min_real_x=min_real_x,
                min_floor_minutes=min_floor_minutes,
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
                    entry_threshold, min_entry_liquidity, min_real_x,
                    entry_min_price, entry_volume_usdc, entry_trade_count,
                    entry_ts_first, entry_ts_last, floor_duration_seconds,
                    max_price_in_history, last_price_in_history,
                    is_winner, real_x, resolution_x, time_to_last_trade_hours
                ) VALUES (
                    :token_id, :market_id, :date,
                    :entry_threshold, :min_entry_liquidity, :min_real_x,
                    :entry_min_price, :entry_volume_usdc, :entry_trade_count,
                    :entry_ts_first, :entry_ts_last, :floor_duration_seconds,
                    :max_price_in_history, :last_price_in_history,
                    :is_winner, :real_x, :resolution_x, :time_to_last_trade_hours
                ) ON CONFLICT(token_id, date) DO UPDATE SET
                    real_x=excluded.real_x,
                    is_winner=excluded.is_winner,
                    resolution_x=excluded.resolution_x
                """,
                {
                    "token_id": token_id,
                    "market_id": market_id,
                    "date": token_date,
                    **result,
                },
            )
            conn.commit()
            ok += 1
            swans_total += 1
        except Exception as e:
            logger.warning(f"{token_id}: DB insert error — {e}")
            rejected += 1
            continue

        if i % 5000 == 0:
            elapsed = time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(
                f"{i}/{total} — {rate:.0f}/s ETA ~{eta}s | "
                f"ok={ok} no_trades={no_trades} no_swan={no_swan} "
                f"rejected={rejected} swans_total={swans_total}"
            )

    elapsed = int(time.monotonic() - t0)

    # Финальная статистика
    row = conn.execute(
        "SELECT COUNT(*), AVG(real_x), MAX(real_x), "
        "SUM(is_winner), AVG(entry_volume_usdc) FROM swans_v2"
    ).fetchone()

    logger.info(
        f"Done in {elapsed}s | ok={ok} no_trades={no_trades} no_swan={no_swan} "
        f"rejected={rejected} errors={errors} swans_total={swans_total}"
    )
    if row and row[0]:
        winners = row[3] or 0
        logger.info(
            f"Stats: total_swans={row[0]}, avg_real_x={row[1]:.2f}, "
            f"max_real_x={row[2]:.0f}, winners={winners} ({winners/row[0]*100:.0f}%), "
            f"avg_entry_liq=${row[4]:.2f}"
        )


def main() -> None:
    ap = argparse.ArgumentParser(description="Polymarket Swan Analyzer v2 — учитывает выплату $1 при разрешении")
    ap.add_argument("--date", metavar="YYYY-MM-DD", help="Только один день")
    ap.add_argument("--date-from", metavar="YYYY-MM-DD", help="Начало диапазона")
    ap.add_argument("--date-to", metavar="YYYY-MM-DD", help="Конец диапазона")
    ap.add_argument("--recompute", action="store_true", help="Пересчитать заново (очищает swans_v2)")
    ap.add_argument("--entry-threshold", type=float, default=DEFAULT_ENTRY_THRESHOLD,
                    help=f"Порог цены дна (default: ${DEFAULT_ENTRY_THRESHOLD})")
    ap.add_argument("--min-entry-usdc", type=float, default=DEFAULT_MIN_ENTRY_USDC)
    ap.add_argument("--min-real-x", type=float, default=DEFAULT_MIN_REAL_X)
    ap.add_argument("--min-floor-minutes", type=float, default=DEFAULT_MIN_FLOOR_MINUTES)
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_db(conn)

    run(
        conn,
        filter_date=args.date,
        date_from=args.date_from,
        date_to=args.date_to,
        recompute=args.recompute,
        entry_threshold=args.entry_threshold,
        min_entry_usdc=args.min_entry_usdc,
        min_real_x=args.min_real_x,
        min_floor_minutes=args.min_floor_minutes,
    )
    conn.close()


if __name__ == "__main__":
    main()
