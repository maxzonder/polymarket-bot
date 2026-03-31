"""
Polymarket Data API client — https://data-api.polymarket.com

Used by the screener for dead-market detection and by the neg-risk strategy
for tracking favorite price movement.
"""

from __future__ import annotations

from typing import Optional

import requests

DATA_API_BASE = "https://data-api.polymarket.com"
DEFAULT_TIMEOUT = 10


def get_last_trade_ts(condition_id: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[int]:
    """Return the timestamp of the most recent trade, or None if unavailable."""
    try:
        resp = requests.get(
            f"{DATA_API_BASE}/trades",
            params={"market": condition_id, "limit": 1},
            timeout=timeout,
        )
        resp.raise_for_status()
        trades = resp.json()
        if trades and isinstance(trades, list):
            return int(trades[0].get("timestamp", 0))
    except Exception:
        pass
    return None


def get_recent_trades(condition_id: str, limit: int = 50, timeout: int = DEFAULT_TIMEOUT) -> list[dict]:
    """Return the most recent trades for a market, newest first."""
    try:
        resp = requests.get(
            f"{DATA_API_BASE}/trades",
            params={"market": condition_id, "limit": limit},
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json() or []
    except Exception:
        return []
