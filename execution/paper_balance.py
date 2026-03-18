"""
Persistent paper-balance ledger for dry-run mode — issue #21.

One singleton row in paper_balance tracks cash_balance (USDC we have available).
Reserved capital is computed live from resting_orders + positions tables:
  reserved = SUM(price*size) for live resting orders
           + SUM(entry_size_usdc) for open positions
  free_balance = cash_balance - reserved

Balance lifecycle:
  - Debited at entry fill (we spent USDC to buy tokens)
  - Credited at TP fill (we received USDC from selling tokens)
  - Credited at resolution (moonbag + unfilled TP tokens at resolution price)
  - Top-up: operator adds paper USDC via CLI

All functions take an open sqlite3.Connection with row_factory=sqlite3.Row.
The caller owns commit/close.
"""

from __future__ import annotations

import sqlite3
import time

from utils.logger import setup_logger

logger = setup_logger("paper_balance")

INITIAL_BALANCE_USDC = 10.0


def init_tables(conn: sqlite3.Connection) -> None:
    """Create paper_balance and paper_balance_events tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS paper_balance (
            id            INTEGER PRIMARY KEY CHECK (id = 1),
            cash_balance  REAL    NOT NULL DEFAULT 0,
            updated_at    INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS paper_balance_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          INTEGER NOT NULL,
            delta_usdc  REAL    NOT NULL,
            note        TEXT
        );
    """)


def ensure_seeded(conn: sqlite3.Connection, initial_usdc: float = INITIAL_BALANCE_USDC) -> None:
    """Insert initial balance row if none exists yet."""
    existing = conn.execute("SELECT id FROM paper_balance WHERE id=1").fetchone()
    if not existing:
        now = int(time.time())
        conn.execute(
            "INSERT INTO paper_balance (id, cash_balance, updated_at) VALUES (1, ?, ?)",
            (initial_usdc, now),
        )
        conn.execute(
            "INSERT INTO paper_balance_events (ts, delta_usdc, note) VALUES (?, ?, ?)",
            (now, initial_usdc, "initial balance"),
        )
        logger.info(f"Paper balance initialized: ${initial_usdc:.2f}")


def get_balance(conn: sqlite3.Connection) -> dict:
    """
    Return current balance snapshot.

    Returns:
        cash_balance       — USDC the paper wallet holds
        reserved_resting   — capital committed to live resting orders
        reserved_positions — capital deployed in open positions
        reserved           — total reserved (resting + positions)
        free_balance       — cash_balance minus all reserved capital
    """
    row = conn.execute("SELECT cash_balance FROM paper_balance WHERE id=1").fetchone()
    cash = float(row["cash_balance"]) if row else 0.0

    r_resting = conn.execute(
        "SELECT COALESCE(SUM(price * size), 0) FROM resting_orders WHERE status='live'"
    ).fetchone()[0]
    r_positions = conn.execute(
        "SELECT COALESCE(SUM(entry_size_usdc), 0) FROM positions WHERE status='open'"
    ).fetchone()[0]

    reserved = float(r_resting) + float(r_positions)
    return {
        "cash_balance":       round(cash, 6),
        "reserved_resting":   round(float(r_resting), 6),
        "reserved_positions": round(float(r_positions), 6),
        "reserved":           round(reserved, 6),
        "free_balance":       round(cash - reserved, 6),
    }


def _adjust(conn: sqlite3.Connection, delta: float, note: str) -> float:
    now = int(time.time())
    conn.execute(
        "UPDATE paper_balance SET cash_balance = cash_balance + ?, updated_at = ? WHERE id = 1",
        (delta, now),
    )
    conn.execute(
        "INSERT INTO paper_balance_events (ts, delta_usdc, note) VALUES (?, ?, ?)",
        (now, delta, note),
    )
    row = conn.execute("SELECT cash_balance FROM paper_balance WHERE id=1").fetchone()
    return float(row["cash_balance"]) if row else 0.0


def debit(conn: sqlite3.Connection, amount: float, note: str = "") -> float:
    """Debit (spend) paper USDC. Returns new cash_balance."""
    return _adjust(conn, -abs(amount), note or f"debit {amount:.6f}")


def credit(conn: sqlite3.Connection, amount: float, note: str = "") -> float:
    """Credit (receive) paper USDC. Returns new cash_balance."""
    return _adjust(conn, abs(amount), note or f"credit {amount:.6f}")


def topup(conn: sqlite3.Connection, amount: float, note: str = "manual topup") -> float:
    """Operator top-up: add paper USDC. Returns new cash_balance."""
    new_balance = _adjust(conn, abs(amount), note)
    logger.info(f"Paper balance top-up: +${amount:.2f} | new cash=${new_balance:.4f}")
    return new_balance
