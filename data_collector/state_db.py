"""
collector_state.db — операционный журнал сборщика.
Хранит статус скачивания и парсинга по каждому рынку/дате.
НЕ смешивать с polymarket_raw.db (аналитические данные).
"""
import os
import sqlite3
from datetime import datetime, timezone

# Путь к БД — рядом с папкой database/ в корне проекта
_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
STATE_DB_PATH = os.path.join(_PROJECT_ROOT, 'database', 'collector_state.db')


def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(STATE_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(STATE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Создаёт таблицу состояний, если её нет."""
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS collection_state (
                date                    TEXT NOT NULL,
                market_id               TEXT NOT NULL,
                downloaded_at           TEXT,
                prices_downloaded_at    TEXT,
                parsed_at               TEXT,
                error                   TEXT,
                PRIMARY KEY (date, market_id)
            )
        """)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Чтение состояния ──────────────────────────────────────────────────────────

def is_market_downloaded(date: str, market_id: str) -> bool:
    with _conn() as conn:
        row = conn.execute(
            "SELECT downloaded_at FROM collection_state WHERE date=? AND market_id=?",
            (date, market_id)
        ).fetchone()
        return bool(row and row["downloaded_at"])


def is_prices_downloaded(date: str, market_id: str) -> bool:
    with _conn() as conn:
        row = conn.execute(
            "SELECT prices_downloaded_at FROM collection_state WHERE date=? AND market_id=?",
            (date, market_id)
        ).fetchone()
        return bool(row and row["prices_downloaded_at"])


def get_unparsed(date: str) -> list[str]:
    """Возвращает market_id где downloaded_at есть, но parsed_at пустой."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT market_id FROM collection_state "
            "WHERE date=? AND downloaded_at IS NOT NULL AND parsed_at IS NULL",
            (date,)
        ).fetchall()
        return [r["market_id"] for r in rows]


# ── Запись состояния ──────────────────────────────────────────────────────────

def mark_downloaded(date: str, market_id: str):
    with _conn() as conn:
        conn.execute("""
            INSERT INTO collection_state (date, market_id, downloaded_at)
            VALUES (?, ?, ?)
            ON CONFLICT(date, market_id) DO UPDATE SET downloaded_at = excluded.downloaded_at
        """, (date, market_id, _now()))


def mark_prices_downloaded(date: str, market_id: str):
    with _conn() as conn:
        conn.execute("""
            INSERT INTO collection_state (date, market_id, prices_downloaded_at)
            VALUES (?, ?, ?)
            ON CONFLICT(date, market_id) DO UPDATE SET prices_downloaded_at = excluded.prices_downloaded_at
        """, (date, market_id, _now()))


def mark_parsed(date: str, market_id: str):
    with _conn() as conn:
        conn.execute("""
            INSERT INTO collection_state (date, market_id, parsed_at)
            VALUES (?, ?, ?)
            ON CONFLICT(date, market_id) DO UPDATE SET parsed_at = excluded.parsed_at
        """, (date, market_id, _now()))


def mark_error(date: str, market_id: str, error: str):
    with _conn() as conn:
        conn.execute("""
            INSERT INTO collection_state (date, market_id, error)
            VALUES (?, ?, ?)
            ON CONFLICT(date, market_id) DO UPDATE SET error = excluded.error
        """, (date, market_id, error))
