"""
collector_state.db — операционный журнал пайплайна.

Хранит статусы по каждому рынку/дате:
- market JSON downloaded
- raw trades downloaded
- parsed into SQLite
- last error
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
STATE_DB_PATH = os.path.join(_PROJECT_ROOT, 'database', 'collector_state.db')


def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(STATE_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(STATE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def init_db():
    with _conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS collection_state (
                date                    TEXT NOT NULL,
                market_id               TEXT NOT NULL,
                downloaded_at           TEXT,
                trades_downloaded_at    TEXT,
                parsed_at               TEXT,
                error                   TEXT,
                PRIMARY KEY (date, market_id)
            )
            """
        )
        if not _has_column(conn, 'collection_state', 'trades_downloaded_at'):
            conn.execute("ALTER TABLE collection_state ADD COLUMN trades_downloaded_at TEXT")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_market_downloaded(date: str, market_id: str) -> bool:
    with _conn() as conn:
        row = conn.execute(
            "SELECT downloaded_at FROM collection_state WHERE date=? AND market_id=?",
            (date, market_id),
        ).fetchone()
        return bool(row and row['downloaded_at'])


def is_trades_downloaded(date: str, market_id: str) -> bool:
    with _conn() as conn:
        row = conn.execute(
            "SELECT trades_downloaded_at FROM collection_state WHERE date=? AND market_id=?",
            (date, market_id),
        ).fetchone()
        return bool(row and row['trades_downloaded_at'])


def is_market_parsed(date: str, market_id: str) -> bool:
    with _conn() as conn:
        row = conn.execute(
            "SELECT parsed_at FROM collection_state WHERE date=? AND market_id=?",
            (date, market_id),
        ).fetchone()
        return bool(row and row['parsed_at'])


def get_unparsed(date: str) -> list[str]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT market_id FROM collection_state WHERE date=? AND downloaded_at IS NOT NULL AND parsed_at IS NULL",
            (date,),
        ).fetchall()
        return [r['market_id'] for r in rows]


def mark_downloaded(date: str, market_id: str):
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO collection_state (date, market_id, downloaded_at)
            VALUES (?, ?, ?)
            ON CONFLICT(date, market_id) DO UPDATE SET downloaded_at = excluded.downloaded_at
            """,
            (date, market_id, _now()),
        )


def mark_trades_downloaded(date: str, market_id: str):
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO collection_state (date, market_id, trades_downloaded_at)
            VALUES (?, ?, ?)
            ON CONFLICT(date, market_id) DO UPDATE SET trades_downloaded_at = excluded.trades_downloaded_at
            """,
            (date, market_id, _now()),
        )


def mark_parsed(date: str, market_id: str):
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO collection_state (date, market_id, parsed_at)
            VALUES (?, ?, ?)
            ON CONFLICT(date, market_id) DO UPDATE SET parsed_at = excluded.parsed_at
            """,
            (date, market_id, _now()),
        )


def mark_error(date: str, market_id: str, error: str):
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO collection_state (date, market_id, error)
            VALUES (?, ?, ?)
            ON CONFLICT(date, market_id) DO UPDATE SET error = excluded.error
            """,
            (date, market_id, error),
        )
