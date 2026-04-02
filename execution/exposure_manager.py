"""
Exposure Manager (v1.1) — aggregated position tracking per (market_id, token_id).

Fixes issue #44 Problem 3: v1 opened separate position rows for each fill
on the same (market_id, token_id), producing false diversification and
making RiskManager count N fills as N independent positions.

This module provides a single source of truth for per-market exposure.
It tracks:
    - Total stake deployed (USDC) per (market_id, token_id)
    - Average entry price (volume-weighted)
    - Number of fills
    - Whether the per-market cap has been reached

Usage:
    em = ExposureManager(db_path, max_per_market=2.0)

    # Before placing a new fill:
    if em.can_add(market_id, token_id, proposed_stake):
        # place order ...
        em.record_fill(market_id, token_id, fill_price, fill_qty)

    # Current exposure:
    exp = em.get(market_id, token_id)
    if exp:
        print(exp.avg_entry_price, exp.total_stake_usdc)

Design:
    - Backed by a lightweight SQLite table (exposure_v1_1) in the positions DB.
    - In-memory cache for hot-path reads; flushed on write.
    - Thread-safe for single-process use (SQLite WAL + no separate threads).

Lifetime cap semantics (intentional):
    record_fill() accumulates stake; there is no decrement on position close or
    market resolution. This is intentional: ExposureManager acts as a *lifetime*
    cap — "never deploy more than max_per_market USDC into this (market, token)
    across the entire DB lifetime".

    An active-exposure cap (decrement on close) is not needed in practice because:
    1. The dedupe guard in OrderManager blocks re-entry on any token that already
       has a live resting order OR an open position. A position stays open until
       on_market_resolved() fires.
    2. Once a market resolves, it cannot be re-entered — so the cap can never
       block a legitimate second entry.
    3. If the DB is reset between runs (e.g. new dry-run cycle), exposure resets
       naturally via ExposureManager.reset().

    If active-exposure semantics are ever needed, add a record_close() method
    that decrements total_stake_usdc and call it from on_market_resolved().
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from utils.paths import DATA_DIR

# Default per-market cap: $2.00 USDC across all fills on the same token
DEFAULT_MAX_PER_MARKET = 2.0

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS exposure_v1_1 (
    market_id        TEXT NOT NULL,
    token_id         TEXT NOT NULL,
    total_stake_usdc REAL NOT NULL DEFAULT 0.0,
    total_qty        REAL NOT NULL DEFAULT 0.0,
    fill_count       INTEGER NOT NULL DEFAULT 0,
    avg_entry_price  REAL NOT NULL DEFAULT 0.0,
    first_fill_at    INTEGER,
    last_fill_at     INTEGER,
    PRIMARY KEY (market_id, token_id)
)
"""


@dataclass
class Exposure:
    market_id: str
    token_id: str
    total_stake_usdc: float
    total_qty: float
    fill_count: int
    avg_entry_price: float
    first_fill_at: Optional[int]
    last_fill_at: Optional[int]


class ExposureManager:
    """Tracks and enforces per-(market_id, token_id) exposure limits."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        max_per_market: float = DEFAULT_MAX_PER_MARKET,
    ):
        self._db_path = str(db_path or DATA_DIR / "positions.db")
        self.max_per_market = max_per_market
        self._cache: dict[tuple[str, str], Exposure] = {}
        self._ensure_table()

    def _ensure_table(self) -> None:
        conn = sqlite3.connect(self._db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(_CREATE_TABLE)
        conn.commit()
        conn.close()

    def _load(self, market_id: str, token_id: str) -> Optional[Exposure]:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        r = conn.execute(
            "SELECT * FROM exposure_v1_1 WHERE market_id=? AND token_id=?",
            (market_id, token_id),
        ).fetchone()
        conn.close()
        if r is None:
            return None
        return Exposure(
            market_id=r["market_id"],
            token_id=r["token_id"],
            total_stake_usdc=float(r["total_stake_usdc"]),
            total_qty=float(r["total_qty"]),
            fill_count=int(r["fill_count"]),
            avg_entry_price=float(r["avg_entry_price"]),
            first_fill_at=r["first_fill_at"],
            last_fill_at=r["last_fill_at"],
        )

    def get(self, market_id: str, token_id: str) -> Optional[Exposure]:
        """Return current exposure for (market_id, token_id), or None if no fills."""
        key = (market_id, token_id)
        if key not in self._cache:
            exp = self._load(market_id, token_id)
            if exp:
                self._cache[key] = exp
        return self._cache.get(key)

    def current_stake(self, market_id: str, token_id: str) -> float:
        """Total USDC deployed in this (market_id, token_id) position."""
        exp = self.get(market_id, token_id)
        return exp.total_stake_usdc if exp else 0.0

    def can_add(
        self,
        market_id: str,
        token_id: str,
        proposed_stake: float,
    ) -> bool:
        """
        Returns True if adding proposed_stake USDC does not exceed max_per_market.
        Also returns True if this is the first fill on this (market_id, token_id).
        """
        existing = self.current_stake(market_id, token_id)
        return (existing + proposed_stake) <= self.max_per_market

    def record_fill(
        self,
        market_id: str,
        token_id: str,
        fill_price: float,
        fill_qty: float,
        fill_ts: Optional[int] = None,
    ) -> Exposure:
        """
        Record a new fill for (market_id, token_id).
        Updates avg_entry_price (volume-weighted) and cumulative totals.
        """
        import time as _time
        now = fill_ts or int(_time.time())
        stake = fill_price * fill_qty

        existing = self.get(market_id, token_id)
        if existing:
            new_total_stake = existing.total_stake_usdc + stake
            new_total_qty   = existing.total_qty + fill_qty
            new_avg_price   = new_total_stake / max(new_total_qty, 1e-12)
            new_count       = existing.fill_count + 1
            first_at        = existing.first_fill_at or now
        else:
            new_total_stake = stake
            new_total_qty   = fill_qty
            new_avg_price   = fill_price
            new_count       = 1
            first_at        = now

        exp = Exposure(
            market_id=market_id,
            token_id=token_id,
            total_stake_usdc=new_total_stake,
            total_qty=new_total_qty,
            fill_count=new_count,
            avg_entry_price=new_avg_price,
            first_fill_at=first_at,
            last_fill_at=now,
        )

        conn = sqlite3.connect(self._db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            INSERT INTO exposure_v1_1
                (market_id, token_id, total_stake_usdc, total_qty, fill_count,
                 avg_entry_price, first_fill_at, last_fill_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(market_id, token_id) DO UPDATE SET
                total_stake_usdc = excluded.total_stake_usdc,
                total_qty        = excluded.total_qty,
                fill_count       = excluded.fill_count,
                avg_entry_price  = excluded.avg_entry_price,
                last_fill_at     = excluded.last_fill_at
        """, (
            market_id, token_id,
            new_total_stake, new_total_qty, new_count,
            new_avg_price, first_at, now,
        ))
        conn.commit()
        conn.close()

        self._cache[(market_id, token_id)] = exp
        return exp

    def total_market_exposure(self, market_id: str) -> float:
        """Total USDC deployed across ALL tokens in a market."""
        conn = sqlite3.connect(self._db_path)
        r = conn.execute(
            "SELECT SUM(total_stake_usdc) FROM exposure_v1_1 WHERE market_id=?",
            (market_id,),
        ).fetchone()
        conn.close()
        return float(r[0] or 0.0)

    def reset(self) -> None:
        """Clear all exposure records (use for replay/test isolation)."""
        conn = sqlite3.connect(self._db_path)
        conn.execute("DELETE FROM exposure_v1_1")
        conn.commit()
        conn.close()
        self._cache.clear()
