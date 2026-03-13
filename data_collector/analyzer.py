"""
Polymarket Analyzer

Шаг 3 пайплайна: факт-чекинг на основе price_history.
Читает данные из polymarket_dataset.db, вычисляет объективные флаги
и записывает в таблицу token_facts.

НЕ содержит стратегических порогов — только факты о поведении цены.

Использование:
    python -m data_collector.analyzer
    python -m data_collector.analyzer --recompute   # пересчитать всё заново
"""
import argparse
import sqlite3
import time

from utils.logger import setup_logger
import os

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
DB_PATH = os.path.abspath(os.path.join(_PROJECT_ROOT, 'polymarket_dataset.db'))

logger = setup_logger("analyzer")

SCHEMA = """
CREATE TABLE IF NOT EXISTS token_facts (
    token_id                TEXT PRIMARY KEY,
    market_id               TEXT,

    -- Артефакт закрытия рынка: цена болтается между 0 и высокой в хвосте
    is_paper_swan           INTEGER,

    -- Сколько свечей цена была <= 0.05 (всего, не только подряд)
    low_zone_candles        INTEGER,

    -- Где в жизни рынка началась низкая зона: 0.0 = в начале, 1.0 = в самом конце
    -- NULL если low_zone_candles = 0
    low_zone_pct_from_end   REAL,

    -- Сколько раз цена пересекала уровень 0.5 (прокси волатильности)
    price_crossings         INTEGER,

    -- Последняя торговая цена
    final_price             REAL,

    -- Через сколько часов после старта рынка цена впервые упала ниже 5¢
    -- NULL если цена никогда не опускалась ниже 5¢
    hours_from_start_to_low REAL,

    FOREIGN KEY (token_id) REFERENCES tokens(token_id)
);
"""

LOW_ZONE_THRESHOLD = 0.05  # 5¢


def detect_paper_swan(prices: list[tuple]) -> bool:
    """
    Определяет артефакт закрытия рынка.

    Pattern A: в последних 6 свечах цена чередуется между < 0.01 и > 0.3
               минимум 3 раза подряд.
    Pattern B: все low-zone свечи (< 5¢) находятся в последних 15% жизни рынка
               И таких свечей <= 3.
    """
    if not prices:
        return False

    price_vals = [p[1] for p in prices]
    total = len(price_vals)

    # Pattern A: чередование в хвосте
    tail = price_vals[-6:]
    alternations = sum(
        1 for i in range(1, len(tail))
        if (tail[i] < 0.01 and tail[i - 1] > 0.3) or
           (tail[i] > 0.3 and tail[i - 1] < 0.01)
    )
    if alternations >= 3:
        return True

    # Pattern B: все low-zone свечи только в последних 15% жизни
    low_indices = [i for i, p in enumerate(price_vals) if p <= LOW_ZONE_THRESHOLD]
    if low_indices and len(low_indices) <= 3:
        first_low_idx = low_indices[0]
        if first_low_idx >= total * 0.85:
            return True

    return False


def compute_facts(token_id: str, market_id: str, start_date: int, prices: list[tuple]) -> dict:
    """
    Вычисляет все факты для одного токена.
    prices: [(ts, price), ...] отсортированные по ts.
    """
    if not prices:
        return {
            "token_id": token_id,
            "market_id": market_id,
            "is_paper_swan": 0,
            "low_zone_candles": 0,
            "low_zone_pct_from_end": None,
            "price_crossings": 0,
            "final_price": None,
            "hours_from_start_to_low": None,
        }

    total = len(prices)
    price_vals = [p[1] for p in prices]

    # is_paper_swan
    is_paper_swan = 1 if detect_paper_swan(prices) else 0

    # low_zone_candles
    low_indices = [i for i, p in enumerate(price_vals) if p <= LOW_ZONE_THRESHOLD]
    low_zone_candles = len(low_indices)

    # low_zone_pct_from_end: позиция первой low-zone свечи от конца (0 = конец, 1 = начало)
    # Инвертируем: 1.0 = самый конец рынка, 0.0 = самое начало
    if low_indices:
        first_low_idx = low_indices[0]
        low_zone_pct_from_end = 1.0 - (first_low_idx / total)
    else:
        low_zone_pct_from_end = None

    # price_crossings: сколько раз цена пересекала 0.5
    crossings = sum(
        1 for i in range(1, len(price_vals))
        if (price_vals[i] >= 0.5 and price_vals[i - 1] < 0.5) or
           (price_vals[i] < 0.5 and price_vals[i - 1] >= 0.5)
    )

    # final_price
    final_price = price_vals[-1]

    # hours_from_start_to_low
    if low_indices and start_date:
        first_low_ts = prices[low_indices[0]][0]
        hours_from_start_to_low = (first_low_ts - start_date) / 3600.0
    else:
        hours_from_start_to_low = None

    return {
        "token_id": token_id,
        "market_id": market_id,
        "is_paper_swan": is_paper_swan,
        "low_zone_candles": low_zone_candles,
        "low_zone_pct_from_end": low_zone_pct_from_end,
        "price_crossings": crossings,
        "final_price": final_price,
        "hours_from_start_to_low": hours_from_start_to_low,
    }


def run(recompute: bool = False):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    conn.commit()

    # Токены без facts (или все, если recompute)
    if recompute:
        conn.execute("DELETE FROM token_facts")
        conn.commit()
        logger.info("Recompute mode: cleared token_facts")

    cursor = conn.execute("""
        SELECT t.token_id, t.market_id, m.start_date
        FROM tokens t
        JOIN markets m ON t.market_id = m.id
        WHERE t.token_id NOT IN (SELECT token_id FROM token_facts)
    """)
    tokens = cursor.fetchall()
    total = len(tokens)
    logger.info(f"Computing facts for {total} tokens...")

    ok = skipped = 0
    t0 = time.monotonic()
    PROGRESS_STEP = 1000
    COMMIT_STEP = 2000

    for i, (token_id, market_id, start_date) in enumerate(tokens, 1):
        prices = conn.execute(
            "SELECT ts, price FROM price_history WHERE token_id = ? ORDER BY ts",
            (token_id,)
        ).fetchall()

        if not prices:
            skipped += 1
            continue

        facts = compute_facts(token_id, market_id, start_date, prices)

        conn.execute("""
            INSERT OR REPLACE INTO token_facts
                (token_id, market_id, is_paper_swan, low_zone_candles,
                 low_zone_pct_from_end, price_crossings, final_price,
                 hours_from_start_to_low)
            VALUES
                (:token_id, :market_id, :is_paper_swan, :low_zone_candles,
                 :low_zone_pct_from_end, :price_crossings, :final_price,
                 :hours_from_start_to_low)
        """, facts)
        ok += 1

        if i % COMMIT_STEP == 0:
            conn.commit()

        if i % PROGRESS_STEP == 0:
            elapsed = time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(f"{i}/{total} — {rate:.0f}/s ETA ~{eta}s | ok={ok} skip={skipped}")

    conn.commit()
    elapsed = int(time.monotonic() - t0)
    logger.info(f"Done in {elapsed}s | ok={ok} skipped={skipped}")

    # Итоговая статистика
    stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(is_paper_swan) as paper_swans,
            SUM(CASE WHEN low_zone_candles > 0 THEN 1 ELSE 0 END) as has_low_zone,
            ROUND(AVG(price_crossings), 1) as avg_crossings
        FROM token_facts
    """).fetchone()

    logger.info(
        f"Stats: total={stats[0]}, paper_swans={stats[1]}, "
        f"has_low_zone={stats[2]}, avg_crossings={stats[3]}"
    )

    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Polymarket Analyzer — факт-чекинг price_history")
    ap.add_argument("--recompute", action="store_true", help="Пересчитать все факты заново")
    args = ap.parse_args()
    run(recompute=args.recompute)
