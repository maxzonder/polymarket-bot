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
import time
from typing import Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from utils.paths import DATABASE_DIR, DB_PATH, ensure_runtime_dirs
from config import (
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

    UNIQUE(token_id, date)
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_token ON swans_v2(token_id)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_market ON swans_v2(market_id)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_date ON swans_v2(date)",
    "CREATE INDEX IF NOT EXISTS idx_swans_v2_max_traded_x ON swans_v2(max_traded_x)",
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
    """Суммирует объём в USDC (price * size) по списку трейдов."""
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
    buy_price_threshold: float,
    min_buy_volume: float,
    min_sell_volume: float,
    min_real_x: float,
) -> Optional[dict]:
    """
    Анализирует историю трейдов одного токена.
    Возвращает dict с метриками или None если лебедь не найден.

    Логика:
    1. Найти глобальный минимум цены по всей истории токена
       (не первый вход в зону — рынок мог несколько раз проваливаться)
    2. Если min_price >= buy_price_threshold → не лебедь
    3. Построить зону дна вокруг глобального минимума:
       все трейды в окне ±lookback от минимума с ценой < buy_price_threshold
    4. Проверить buy_volume >= min_buy_volume
    5. Для не-победителей: проверить sell_volume >= min_sell_volume
       sell_volume = объём сделок после зоны дна по цене >= buy_min_price * min_real_x
    6. Для победителей (is_winner=1): Polymarket выплачивает $1 → sell_volume не нужен
    7. max_traded_x = 1/buy_min_price если winner, иначе max(price_after_floor) / buy_min_price
    """
    if not trades:
        return None

    prices = [float(t["price"]) for t in trades]

    # --- 1. Глобальный минимум ---
    global_min = min(prices)
    if global_min > buy_price_threshold:
        return None  # цена никогда не падала до порога

    global_min_idx = prices.index(global_min)

    # --- 2. Зона дна вокруг глобального минимума ---
    # Расширяем в обе стороны пока цена <= buy_price_threshold (включительно)
    floor_start = global_min_idx
    while floor_start > 0 and prices[floor_start - 1] <= buy_price_threshold:
        floor_start -= 1

    floor_end = global_min_idx
    while floor_end < len(prices) - 1 and prices[floor_end + 1] <= buy_price_threshold:
        floor_end += 1

    floor_trades = trades[floor_start: floor_end + 1]
    if not floor_trades:
        return None

    # --- 3. Проверить ликвидность на входе ---
    buy_volume = _sum_usdc(floor_trades)
    if buy_volume < min_buy_volume:
        return None

    buy_min_price = global_min
    buy_ts_first = int(floor_trades[0]["timestamp"])
    buy_ts_last = int(floor_trades[-1]["timestamp"])

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
            "SELECT t.token_id, t.market_id, t.is_winner FROM tokens t"
            " JOIN _mids m ON t.market_id = m.market_id"
        ).fetchall()
        conn.execute("DROP TABLE _mids")
    else:
        tokens = conn.execute(
            "SELECT token_id, market_id, is_winner FROM tokens"
        ).fetchall()

    total = len(tokens)
    logger.info(
        f"Processing {total} tokens | "
        f"threshold=${buy_price_threshold}, min_buy=${min_buy_volume}, "
        f"min_sell=${min_sell_volume}, min_real_x={min_real_x}x"
    )

    ok = no_trades = no_swan = rejected = errors = 0
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
                buy_price_threshold=buy_price_threshold,
                min_buy_volume=min_buy_volume,
                min_sell_volume=min_sell_volume,
                min_real_x=min_real_x,
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
                    is_winner, max_traded_x, payout_x
                ) VALUES (
                    :token_id, :market_id, :date,
                    :buy_price_threshold, :min_buy_volume, :min_sell_volume,
                    :min_real_x,
                    :buy_min_price, :buy_volume, :buy_trade_count,
                    :buy_ts_first, :buy_ts_last,
                    :sell_volume,
                    :max_price_in_history, :last_price_in_history,
                    :is_winner, :max_traded_x, :payout_x
                ) ON CONFLICT(token_id, date) DO UPDATE SET
                    max_traded_x=excluded.max_traded_x,
                    is_winner=excluded.is_winner,
                    payout_x=excluded.payout_x,
                    sell_volume=excluded.sell_volume
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
        "SELECT COUNT(*), AVG(max_traded_x), MAX(max_traded_x), SUM(is_winner), AVG(buy_volume) FROM swans_v2"
    ).fetchone()

    logger.info(
        f"Done in {elapsed}s | ok={ok} no_trades={no_trades} no_swan={no_swan} "
        f"rejected={rejected} errors={errors}"
    )
    if row and row[0]:
        winners = row[3] or 0
        logger.info(
            f"Stats: total={row[0]}, avg_real_x={row[1]:.2f}x, max_real_x={row[2]:.0f}x, "
            f"winners={winners} ({winners/row[0]*100:.0f}%), avg_entry_liq=${row[4]:.2f}"
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
    args = ap.parse_args()

    for w in check_swan_buy_price_threshold(args.buy_price_threshold):
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
    )
    conn.close()


if __name__ == "__main__":
    main()
