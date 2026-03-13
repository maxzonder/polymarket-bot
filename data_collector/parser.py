"""
Polymarket Parser

Читает скачанные JSON-файлы из database/ и записывает в polymarket_dataset.db.
После загрузки данных выполняет ETL: заполняет token_analytics.

Идемпотентен — пропускает уже распарсенные рынки (parsed_at в collector_state.db).

Использование:
    python -m data_collector.parser --start 2026-02-28 --end 2026-02-28
    python -m data_collector.parser --start 2026-02-28 --end 2026-02-28 --skip-analytics
    python -m data_collector.parser --analytics-only
"""
import argparse
import json
import os
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from data_collector import state_db
from utils.logger import setup_logger

# ── Пути ──────────────────────────────────────────────────────────────────────
_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
DATABASE_DIR  = os.path.abspath(os.path.join(_PROJECT_ROOT, 'database'))
DB_PATH       = os.path.abspath(os.path.join(_PROJECT_ROOT, 'polymarket_dataset.db'))

logger = setup_logger("parser")

# ── Схема БД ──────────────────────────────────────────────────────────────────
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
    fees_enabled        INTEGER
);

CREATE TABLE IF NOT EXISTS tokens (
    token_id        TEXT PRIMARY KEY,
    market_id       TEXT,
    outcome_name    TEXT,
    is_winner       INTEGER,
    FOREIGN KEY (market_id) REFERENCES markets(id)
);

CREATE INDEX IF NOT EXISTS idx_tokens_market ON tokens(market_id);

CREATE TABLE IF NOT EXISTS price_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id    TEXT,
    ts          INTEGER,
    price       REAL,
    FOREIGN KEY (token_id) REFERENCES tokens(token_id)
);

CREATE INDEX IF NOT EXISTS idx_ph_token ON price_history(token_id);
CREATE INDEX IF NOT EXISTS idx_ph_token_ts ON price_history(token_id, ts);

CREATE TABLE IF NOT EXISTS token_analytics (
    token_id                TEXT PRIMARY KEY,
    market_id               TEXT,
    min_price               REAL,
    min_price_ts            INTEGER,
    hours_to_close_at_min   REAL,
    max_price_after_min     REAL,
    max_spike_multiplier    REAL,
    hours_at_bottom         INTEGER,
    FOREIGN KEY (token_id) REFERENCES tokens(token_id)
);
"""


def init_db(conn: sqlite3.Connection):
    conn.executescript(SCHEMA)
    conn.commit()


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_ts(s: Optional[str]) -> Optional[int]:
    """Парсит ISO datetime строку в Unix timestamp. Возвращает None при ошибке."""
    if not s:
        return None
    try:
        s = s.strip().replace(' ', 'T')
        # "2026-03-02T07:43:35+00" → "+00:00"
        if s.endswith('+00'):
            s = s[:-3] + '+00:00'
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def parse_market(data: dict) -> Optional[dict]:
    """Извлекает строку для таблицы markets из JSON рынка."""
    market_id = data.get("id")
    if not market_id:
        return None

    # category из тегов события (не всегда есть)
    category = None
    comment_count = None
    events = data.get("events") or []
    if events:
        ev = events[0]
        comment_count = ev.get("commentCount")
        tags = ev.get("tags") or []
        if tags:
            category = tags[0].get("label")

    start_ts  = parse_ts(data.get("startDate"))
    end_ts    = parse_ts(data.get("endDate"))
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

    liquidity    = data.get("liquidity")
    fees_enabled = 1 if data.get("feesEnabled") else 0

    return {
        "id":               str(market_id),
        "question":         data.get("question"),
        "description":      data.get("description"),
        "category":         category,
        "resolution_source": data.get("resolutionSource", ""),
        "start_date":       start_ts,
        "end_date":         end_ts,
        "closed_time":      closed_ts,
        "duration_hours":   duration_hours,
        "volume":           volume,
        "liquidity":        liquidity,
        "comment_count":    comment_count,
        "fees_enabled":     fees_enabled,
    }


def parse_tokens(data: dict) -> list[dict]:
    """Извлекает строки для таблицы tokens из JSON рынка."""
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

    token_ids    = _load_json_list(data.get("clobTokenIds", "[]"))
    outcomes     = _load_json_list(data.get("outcomes", "[]"))
    price_strs   = _load_json_list(data.get("outcomePrices", "[]"))

    rows = []
    for i, token_id in enumerate(token_ids):
        outcome_name = outcomes[i] if i < len(outcomes) else None
        try:
            is_winner = 1 if float(price_strs[i]) >= 0.99 else 0
        except (IndexError, ValueError, TypeError):
            is_winner = 0
        rows.append({
            "token_id":     str(token_id),
            "market_id":    market_id,
            "outcome_name": outcome_name,
            "is_winner":    is_winner,
        })
    return rows


# ── Парсинг одного дня ────────────────────────────────────────────────────────

def parse_day(conn: sqlite3.Connection, day_str: str):
    day_dir = os.path.join(DATABASE_DIR, day_str)
    if not os.path.isdir(day_dir):
        logger.warning(f"[{day_str}] Directory not found: {day_dir}")
        return

    # Только market JSONs (не _hours папки)
    market_files = sorted([
        f for f in os.listdir(day_dir)
        if f.endswith(".json")
    ])
    total = len(market_files)
    logger.info(f"[{day_str}] Found {total} market JSONs")

    ok = skipped = err = price_rows_total = 0
    t0 = time.monotonic()
    PROGRESS_STEP = 100

    for i, fname in enumerate(market_files, 1):
        market_id = fname[:-5]  # strip .json

        # Идемпотентность: пропускаем уже распарсенные
        if state_db.is_market_parsed(day_str, market_id):
            skipped += 1
            continue

        fpath = os.path.join(day_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
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
            conn.execute("""
                INSERT OR IGNORE INTO markets
                    (id, question, description, category, resolution_source,
                     start_date, end_date, closed_time, duration_hours,
                     volume, liquidity, comment_count, fees_enabled)
                VALUES
                    (:id, :question, :description, :category, :resolution_source,
                     :start_date, :end_date, :closed_time, :duration_hours,
                     :volume, :liquidity, :comment_count, :fees_enabled)
            """, market_row)

            for tr in token_rows:
                conn.execute("""
                    INSERT OR IGNORE INTO tokens (token_id, market_id, outcome_name, is_winner)
                    VALUES (:token_id, :market_id, :outcome_name, :is_winner)
                """, tr)
        except Exception as e:
            logger.warning(f"[{day_str}] {market_id}: DB insert error — {e}")
            err += 1
            continue

        # Загружаем историю цен
        hours_dir = os.path.join(day_dir, f"{market_id}_hours")
        price_rows = 0
        if os.path.isdir(hours_dir):
            for phfile in os.listdir(hours_dir):
                if not phfile.endswith(".json"):
                    continue
                token_id = phfile[:-5]
                phpath = os.path.join(hours_dir, phfile)
                try:
                    with open(phpath, "r", encoding="utf-8") as f:
                        history = json.load(f)
                    if history:
                        conn.executemany(
                            "INSERT INTO price_history (token_id, ts, price) VALUES (?, ?, ?)",
                            [(token_id, entry["t"], entry["p"]) for entry in history]
                        )
                        price_rows += len(history)
                except Exception as e:
                    logger.warning(f"[{day_str}] {market_id} token {token_id}: price load error — {e}")

        conn.commit()
        state_db.mark_parsed(day_str, market_id)
        price_rows_total += price_rows
        ok += 1

        if i % PROGRESS_STEP == 0:
            elapsed = time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(
                f"[{day_str}] {i}/{total} — {rate:.0f}/s ETA ~{eta}s | "
                f"ok={ok} skip={skipped} err={err}"
            )

    elapsed = int(time.monotonic() - t0)
    logger.info(
        f"[{day_str}] Done in {elapsed}s | "
        f"ok={ok} skipped={skipped} err={err} | price_rows={price_rows_total}"
    )


# ── ETL: token_analytics ──────────────────────────────────────────────────────

def count_consecutive_candles_below(prices: list[tuple], threshold: float, start_ts: int) -> int:
    """
    Считает количество свечей подряд начиная с start_ts, где price <= threshold.
    prices: [(ts, price), ...] отсортированные по ts.
    """
    start_idx = None
    for idx, (ts, _) in enumerate(prices):
        if ts >= start_ts:
            start_idx = idx
            break
    if start_idx is None:
        return 0

    count = 0
    for _, p in prices[start_idx:]:
        if p <= threshold:
            count += 1
        else:
            break
    return count


def compute_token_analytics(conn: sqlite3.Connection):
    """
    Вычисляет token_analytics для всех токенов у которых есть price_history,
    но ещё нет записи в token_analytics.
    """
    cursor = conn.execute("""
        SELECT t.token_id, t.market_id, m.closed_time
        FROM tokens t
        JOIN markets m ON t.market_id = m.id
        WHERE t.token_id NOT IN (SELECT token_id FROM token_analytics)
          AND m.closed_time IS NOT NULL
    """)
    tokens = cursor.fetchall()
    total = len(tokens)
    logger.info(f"[analytics] Computing for {total} tokens...")

    ok = skipped = 0
    t0 = time.monotonic()
    PROGRESS_STEP = 500
    COMMIT_STEP = 1000

    for i, (token_id, market_id, closed_time) in enumerate(tokens, 1):
        ph_cursor = conn.execute(
            "SELECT ts, price FROM price_history WHERE token_id = ? ORDER BY ts",
            (token_id,)
        )
        prices = ph_cursor.fetchall()

        if not prices:
            skipped += 1
            continue

        prices_list = list(prices)  # [(ts, price), ...]

        min_price = min(p for _, p in prices_list)
        if min_price <= 0:
            # Деление на ноль — пропускаем
            skipped += 1
            continue

        min_price_ts = next(ts for ts, p in prices_list if p == min_price)
        after_min    = [(ts, p) for ts, p in prices_list if ts > min_price_ts]

        max_price_after_min  = max((p for _, p in after_min), default=min_price)
        max_spike_multiplier = max_price_after_min / min_price
        hours_to_close_at_min = (closed_time - min_price_ts) / 3600.0

        bottom_threshold = min_price * 1.5
        hours_at_bottom  = count_consecutive_candles_below(prices_list, bottom_threshold, min_price_ts)

        try:
            conn.execute("""
                INSERT OR REPLACE INTO token_analytics
                    (token_id, market_id, min_price, min_price_ts,
                     hours_to_close_at_min, max_price_after_min,
                     max_spike_multiplier, hours_at_bottom)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (token_id, market_id, min_price, min_price_ts,
                  hours_to_close_at_min, max_price_after_min,
                  max_spike_multiplier, hours_at_bottom))
            ok += 1
        except Exception as e:
            logger.warning(f"[analytics] {token_id}: insert error — {e}")

        if i % COMMIT_STEP == 0:
            conn.commit()

        if i % PROGRESS_STEP == 0:
            elapsed = time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(
                f"[analytics] {i}/{total} — {rate:.0f}/s ETA ~{eta}s | "
                f"ok={ok} skip={skipped}"
            )

    conn.commit()
    elapsed = int(time.monotonic() - t0)
    logger.info(f"[analytics] Done in {elapsed}s | ok={ok} skipped={skipped}")


# ── Entry point ───────────────────────────────────────────────────────────────

def run(start: date, end: date, skip_analytics: bool = False):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_db(conn)

    logger.info(f"Parser started: {start} → {end} | DB: {DB_PATH}")

    current = start
    while current <= end:
        parse_day(conn, current.isoformat())
        current += timedelta(days=1)

    if not skip_analytics:
        compute_token_analytics(conn)

    conn.close()
    logger.info("Parser finished.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Polymarket Parser — читает JSON из database/ и пишет в polymarket_dataset.db"
    )
    ap.add_argument("--start",          metavar="YYYY-MM-DD", help="Начальная дата")
    ap.add_argument("--end",            metavar="YYYY-MM-DD", help="Конечная дата")
    ap.add_argument("--skip-analytics", action="store_true",
                    help="Пропустить ETL шаг token_analytics")
    ap.add_argument("--analytics-only", action="store_true",
                    help="Только пересчитать token_analytics (без парсинга JSONов)")
    args = ap.parse_args()

    if args.analytics_only:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        init_db(conn)
        compute_token_analytics(conn)
        conn.close()
    else:
        if not args.start or not args.end:
            ap.error("--start и --end обязательны (или используй --analytics-only)")
        run(
            date.fromisoformat(args.start),
            date.fromisoformat(args.end),
            skip_analytics=args.skip_analytics,
        )
