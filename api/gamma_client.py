"""
Gamma API Client — fetches open/closed markets from gamma-api.polymarket.com.

Used by Screener to find active markets for entry candidate selection.
Public API — no authentication required.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import requests

from market_classifier import infer_market_category

GAMMA_BASE = "https://gamma-api.polymarket.com"
PAGE_SIZE = 100
DEFAULT_TIMEOUT = 30
SLEEP_BETWEEN_PAGES = 0.1


@dataclass
class MarketInfo:
    """Lightweight snapshot of an open market for the screener."""
    market_id: str
    condition_id: str
    question: str
    category: Optional[str]
    token_ids: list[str]         # [yes_token_id, no_token_id]
    outcome_names: list[str]     # ["Yes", "No"]
    best_ask: Optional[float]    # current best ask per token
    best_bid: Optional[float]    # current best bid per token
    last_trade_price: Optional[float]
    volume_usdc: float
    liquidity_usdc: float
    comment_count: int
    fees_enabled: bool
    end_date_ts: Optional[int]   # unix timestamp
    hours_to_close: Optional[float]
    neg_risk: bool = False         # negRisk flag from Gamma (4.5x higher swan_rate)
    neg_risk_group_id: Optional[str] = None  # negRiskMarketID — parent group for cluster cap
    slug: Optional[str] = None     # market slug (e.g. "epl-arsenal-vs-chelsea-2026-04-01")
    total_duration_hours: Optional[float] = None  # market/event start → end duration when available


def _parse_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _parse_int(val) -> Optional[int]:
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _load_json_list(val) -> list:
    import json
    if isinstance(val, list):
        return val
    try:
        return json.loads(val or "[]")
    except Exception:
        return []


def _parse_iso_ts(val) -> Optional[int]:
    if not val:
        return None
    try:
        from datetime import datetime, timezone

        raw = str(val).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except Exception:
        return None


def _parse_market(raw: dict, now_ts: float) -> Optional[MarketInfo]:
    market_id = raw.get("id")
    if not market_id:
        return None

    # Skip inactive / archived
    if raw.get("closed") or raw.get("archived"):
        return None
    if not raw.get("active", True):
        return None

    condition_id = raw.get("conditionId") or raw.get("id")

    token_ids = _load_json_list(raw.get("clobTokenIds", "[]"))
    outcome_names = _load_json_list(raw.get("outcomes", "[]"))

    # Volume
    volume = _parse_float(raw.get("volumeNum")) or _parse_float(raw.get("volume")) or 0.0
    liquidity = _parse_float(raw.get("liquidity") or raw.get("liquidityNum")) or 0.0

    # Prices — from market-level fields
    best_ask = _parse_float(raw.get("bestAsk"))
    best_bid = _parse_float(raw.get("bestBid"))
    last_price = _parse_float(raw.get("lastTradePrice"))

    # Comment count lives in events[0]
    comment_count = 0
    events = raw.get("events") or []
    if events and isinstance(events[0], dict):
        comment_count = _parse_int(events[0].get("commentCount")) or 0

    # Category — shared with the historical parser and live collectors so
    # screener filters/priors use the same internal strategy buckets.
    category = infer_market_category(raw, events[0] if events and isinstance(events[0], dict) else None)

    # Start/end date and total duration.
    end_date_ts = None
    hours_to_close = None
    total_duration_hours = None
    event0 = events[0] if events and isinstance(events[0], dict) else {}
    start_date_raw = (
        raw.get("startDate")
        or raw.get("startDateIso")
        or raw.get("gameStartTime")
        or event0.get("startTime")
        or event0.get("startDate")
        or event0.get("eventStartTime")
    )
    start_date_ts = _parse_iso_ts(start_date_raw)
    end_date_raw = raw.get("endDate") or raw.get("endDateIso")
    end_date_ts = _parse_iso_ts(end_date_raw)
    if end_date_ts is not None:
        hours_to_close = (end_date_ts - now_ts) / 3600.0
    if start_date_ts is not None and end_date_ts is not None and end_date_ts > start_date_ts:
        total_duration_hours = (end_date_ts - start_date_ts) / 3600.0

    fees_enabled = bool(raw.get("feesEnabled"))
    neg_risk = bool(raw.get("negRisk", False))
    neg_risk_group_id = raw.get("negRiskMarketID") or raw.get("negRiskRequestID") or None
    if neg_risk_group_id is not None:
        neg_risk_group_id = str(neg_risk_group_id)

    return MarketInfo(
        market_id=str(market_id),
        condition_id=str(condition_id),
        question=raw.get("question", ""),
        category=category,
        token_ids=[str(t) for t in token_ids],
        outcome_names=[str(o) for o in outcome_names],
        best_ask=best_ask,
        best_bid=best_bid,
        last_trade_price=last_price,
        volume_usdc=volume,
        liquidity_usdc=liquidity,
        comment_count=comment_count,
        fees_enabled=fees_enabled,
        end_date_ts=end_date_ts,
        hours_to_close=hours_to_close,
        neg_risk=neg_risk,
        neg_risk_group_id=neg_risk_group_id,
        slug=(raw.get("slug") or None),
        total_duration_hours=total_duration_hours,
    )


def fetch_open_markets(
    price_max: float = 0.30,
    volume_min: float = 50.0,
    volume_max: float = float("inf"),
    limit_pages: int = 50,
) -> list[MarketInfo]:
    """
    Returns all active markets where at least one token is in the entry zone.

    Entry zone: token price <= price_max.
    Gamma's bestAsk is the YES-token price.  The NO-token price is synthetic:
    no_price = 1 - yes_price.

    A market passes if:
      • YES price <= price_max  (YES is a swan candidate), OR
      • NO  price <= price_max  (NO is a swan candidate, i.e. YES >= 1 - price_max)

    Previously only the first condition was checked, silently dropping half the
    universe — every high-probability YES market whose NO token is in the floor zone.

    Paginates through Gamma API with volume filter applied server-side.
    Client-side filter on price since Gamma API doesn't filter by price.
    """
    now_ts = time.time()
    markets: list[MarketInfo] = []
    offset = 0

    for _ in range(limit_pages):
        params = {
            "closed": "false",
            "active": "true",
            "include_tag": "true",
            "volume_num_min": volume_min,
            **({"volume_num_max": volume_max} if volume_max != float("inf") else {}),
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        try:
            resp = requests.get(f"{GAMMA_BASE}/markets", params=params, timeout=DEFAULT_TIMEOUT)
            resp.raise_for_status()
            page = resp.json()
        except Exception as e:
            raise RuntimeError(f"Gamma API fetch_open_markets failed at offset={offset}: {e}") from e

        if not page:
            break

        for raw in page:
            m = _parse_market(raw, now_ts)
            if m is None:
                continue
            # price filter: include market if AT LEAST ONE token is in the entry zone.
            # YES price  = best_ask (Gamma market-level field).
            # NO  price  = 1 - YES (synthetic; CLOB real price fetched later per-token).
            # Condition: yes_price <= price_max  OR  (1 - yes_price) <= price_max
            #   ↳ equivalent to: yes_price <= price_max  OR  yes_price >= 1 - price_max
            yes_price = m.best_ask if m.best_ask is not None else m.last_trade_price
            if yes_price is None:
                continue
            no_price = 1.0 - yes_price
            if yes_price <= price_max or no_price <= price_max:
                markets.append(m)

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(SLEEP_BETWEEN_PAGES)

    return markets


def fetch_market(market_id: str) -> Optional[MarketInfo]:
    """Fetch a single market by ID."""
    now_ts = time.time()
    try:
        resp = requests.get(
            f"{GAMMA_BASE}/markets/{market_id}",
            timeout=DEFAULT_TIMEOUT,
        )
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        raise RuntimeError(f"Gamma API fetch_market({market_id}) failed: {e}") from e

    # API may return list or single object
    if isinstance(raw, list):
        raw = raw[0] if raw else {}

    return _parse_market(raw, now_ts)
