"""
MarketPatternTracker — fetches full trade history on first sight of a market
and classifies the price-action pattern used as a scoring multiplier in the
screener.  Pattern labels and multipliers derive from issue #180 Phase E
analytics.

Lifecycle-fraction normalization: patterns are detected relative to
(ts - first_trade_ts) / (end_date_ts - first_trade_ts) so that a "rapid
collapse" means the same fraction of market life regardless of whether the
market is 2 hours or 7 days long.

Policy key: (pattern, category, duration_bucket) with None wildcards.
Lookup order: (p,c,b) → (p,None,b) → (p,c,None) → (p,None,None).
This lets 15m markets carry different multipliers without touching the
classification algorithm itself.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import requests

DATA_API_BASE = "https://data-api.polymarket.com"
_TRADES_TIMEOUT = 15
_CACHE_TTL_SECONDS = 1800  # re-classify every 30 min

# Pattern labels (8 total)
FLOOR_ACCUMULATION    = "floor_accumulation"
CHOP_THEN_FLOOR       = "chop_then_floor"
WICK_FLOOR_TOUCH      = "wick_floor_touch"
SUDDEN_COLLAPSE       = "sudden_collapse"
GRIND_DOWN_TO_FLOOR   = "grind_down_to_floor"
PENNY_FLOOR_TOUCH     = "penny_floor_touch"
NO_PRE_HISTORY        = "no_pre_history"
NO_FLOOR_YET          = "no_floor_yet"

_ALL_PATTERNS = frozenset({
    FLOOR_ACCUMULATION, CHOP_THEN_FLOOR, WICK_FLOOR_TOUCH,
    SUDDEN_COLLAPSE, GRIND_DOWN_TO_FLOOR, PENNY_FLOOR_TOUCH,
    NO_PRE_HISTORY, NO_FLOOR_YET,
})

# Policy key: (pattern, category, duration_bucket); None = wildcard for that axis.
# Lookup order in _policy_mult: (p,c,b) → (p,None,b) → (p,c,None) → (p,None,None).
_POLICY: dict[tuple[str, Optional[str], Optional[str]], float] = {

    # ── floor_accumulation: long residence at floor, confirmed shock ──────────
    (FLOOR_ACCUMULATION, "weather", None): 1.5,
    (FLOOR_ACCUMULATION, "crypto",  None): 1.2,
    (FLOOR_ACCUMULATION, "sports",  None): 1.1,
    (FLOOR_ACCUMULATION, None,      None): 1.0,
    # 15m: floor residence is only minutes — weaker signal
    (FLOOR_ACCUMULATION, "weather", "15m"): 0.9,
    (FLOOR_ACCUMULATION, "crypto",  "15m"): 0.8,
    (FLOOR_ACCUMULATION, "sports",  "15m"): 0.7,
    (FLOOR_ACCUMULATION, None,      "15m"): 0.8,

    # ── chop_then_floor: high pre-floor variance then settled ─────────────────
    (CHOP_THEN_FLOOR, "weather", None): 1.5,
    (CHOP_THEN_FLOOR, "crypto",  None): 1.1,
    (CHOP_THEN_FLOOR, "sports",  None): 1.0,
    (CHOP_THEN_FLOOR, None,      None): 1.0,
    # 15m: chop in minutes is less meaningful
    (CHOP_THEN_FLOOR, None, "15m"): 0.7,

    # ── wick_floor_touch: brief dip, recovered ────────────────────────────────
    (WICK_FLOOR_TOUCH, "weather", None): 1.2,
    (WICK_FLOOR_TOUCH, "crypto",  None): 1.1,
    (WICK_FLOOR_TOUCH, "sports",  None): 0.8,
    (WICK_FLOOR_TOUCH, None,      None): 1.0,
    # 15m: a wick in minutes is noise
    (WICK_FLOOR_TOUCH, None, "15m"): 0.7,

    # ── sudden_collapse: floor first hit in last 20% of lifecycle ────────────
    (SUDDEN_COLLAPSE, "weather", None): 0.7,
    (SUDDEN_COLLAPSE, "crypto",  None): 1.0,
    (SUDDEN_COLLAPSE, "sports",  None): 0.6,
    (SUDDEN_COLLAPSE, None,      None): 0.8,
    # 15m: fresh crash in final seconds = fresh price discovery, more interesting
    (SUDDEN_COLLAPSE, "crypto",  "15m"): 1.3,
    (SUDDEN_COLLAPSE, "weather", "15m"): 1.0,
    (SUDDEN_COLLAPSE, "sports",  "15m"): 0.8,
    (SUDDEN_COLLAPSE, None,      "15m"): 0.9,

    # ── grind_down_to_floor: slow bleed → SKIP universally ───────────────────
    (GRIND_DOWN_TO_FLOOR, None, None): 0.0,

    # ── penny_floor_touch: ≤ 0.02 → likely dead ──────────────────────────────
    (PENNY_FLOOR_TOUCH, "weather", None): 0.0,
    (PENNY_FLOOR_TOUCH, "sports",  None): 0.0,
    (PENNY_FLOOR_TOUCH, "crypto",  None): 0.4,
    (PENNY_FLOOR_TOUCH, None,      None): 0.0,
    # 15m: penny in a 15m market is almost certainly dead
    (PENNY_FLOOR_TOUCH, "crypto",  "15m"): 0.3,

    # ── no_pre_history: too few trades ───────────────────────────────────────
    (NO_PRE_HISTORY, "sports", None): 0.0,
    (NO_PRE_HISTORY, None,     None): 0.5,
    # 15m: no trades in a 15m market = too risky, skip all categories
    (NO_PRE_HISTORY, None, "15m"): 0.0,

    # ── no_floor_yet: price never reached floor ───────────────────────────────
    (NO_FLOOR_YET, None, None): 0.0,
}


@dataclass
class _PatternState:
    condition_id: str
    pattern: str      # label only; mult is recomputed live (hours change each scan)
    fetched_at: float
    trade_count: int


class MarketPatternTracker:
    """
    Stateful component that fetches trade history once per market and classifies
    its price-action pattern.  The pattern label is cached; the multiplier is
    recomputed on each call using the current hours_to_close so that the
    duration bucket reflects the actual remaining time.
    """

    def __init__(self) -> None:
        self._cache: dict[str, _PatternState] = {}

    def get_pattern_mult(self, m, hours: float) -> float:
        """
        Return the pattern multiplier (0.0 = skip, <1.0 = reduce, >1.0 = boost).
        Pattern label is fetched/cached for 30 min; mult is looked up fresh each
        call using current hours so the duration bucket stays accurate.
        m must have .market_id, .condition_id, .category, .end_date_ts.
        """
        state = self._cache.get(m.market_id)
        now = time.time()
        if state is None or (now - state.fetched_at) > _CACHE_TTL_SECONDS:
            state = self._fetch_and_classify(m, now)
            self._cache[m.market_id] = state
        bucket = _duration_bucket(hours)
        return _policy_mult(state.pattern, m.category, bucket)

    def get_pattern_label(self, market_id: str) -> Optional[str]:
        state = self._cache.get(market_id)
        return state.pattern if state else None

    def _fetch_and_classify(self, m, now: float) -> _PatternState:
        trades = _fetch_trades(m.condition_id)
        pattern = _classify(trades, m.end_date_ts)
        return _PatternState(
            condition_id=m.condition_id,
            pattern=pattern,
            fetched_at=now,
            trade_count=len(trades),
        )


# ── Duration bucket ───────────────────────────────────────────────────────────

def _duration_bucket(hours: float) -> str:
    if hours <= 0.5:
        return "15m"
    if hours <= 2.0:
        return "1h"
    if hours <= 6.0:
        return "6h"
    if hours <= 168.0:
        return "1-7d"
    return "long"


# ── Trades fetch ──────────────────────────────────────────────────────────────

def _fetch_trades(condition_id: str) -> list[dict]:
    """Return trades in chronological order (API returns newest-first)."""
    try:
        resp = requests.get(
            f"{DATA_API_BASE}/trades",
            params={
                "market": condition_id,
                "limit": 3000,
                "filterAmount": 1.0,
            },
            timeout=_TRADES_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json() or []
        data.reverse()  # chronological
        return data
    except Exception:
        return []


# ── Pattern classification ────────────────────────────────────────────────────

def _classify(trades: list[dict], end_date_ts: Optional[int]) -> str:
    """
    Classify price-action pattern from chronological trade history.
    Returns one of the 8 pattern label constants.
    """
    if len(trades) < 3:
        return NO_PRE_HISTORY

    prices: list[float] = []
    timestamps: list[float] = []
    for t in trades:
        try:
            prices.append(float(t["price"]))
            timestamps.append(float(t["timestamp"]))
        except (KeyError, ValueError, TypeError):
            continue

    if len(prices) < 3:
        return NO_PRE_HISTORY

    start_ts = timestamps[0]
    if end_date_ts is not None and float(end_date_ts) > start_ts:
        span = float(end_date_ts) - start_ts
    else:
        span = timestamps[-1] - start_ts
    if span <= 0:
        span = 1.0

    def lf(ts: float) -> float:
        return max(0.0, min(1.0, (ts - start_ts) / span))

    # ── 1. Penny touch: any price ≤ 0.02 ─────────────────────────────────────
    if any(p <= 0.02 for p in prices):
        return PENNY_FLOOR_TOUCH

    # ── 2. No floor yet: price never reached ≤ 0.05 ──────────────────────────
    floor_indices = [i for i, p in enumerate(prices) if p <= 0.05]
    if not floor_indices:
        return NO_FLOOR_YET

    first_floor_idx = floor_indices[0]
    first_floor_frac = lf(timestamps[first_floor_idx])
    last_floor_frac = lf(timestamps[floor_indices[-1]])
    floor_duration_frac = last_floor_frac - first_floor_frac
    last_price = prices[-1]

    # ── 3. Wick floor touch: brief dip that recovered ────────────────────────
    if floor_duration_frac < 0.10 and last_price > 0.08:
        return WICK_FLOOR_TOUCH

    # ── 4. Sudden collapse: floor first reached in last 20% of lifecycle ──────
    if first_floor_frac >= 0.80:
        return SUDDEN_COLLAPSE

    # ── 5. Grind down: monotone bleed across >60% of market life ─────────────
    pre_floor_prices = prices[: first_floor_idx + 1]
    if len(pre_floor_prices) >= 4:
        pre_floor_span_frac = lf(timestamps[first_floor_idx]) - lf(timestamps[0])
        if pre_floor_span_frac > 0.60:
            moves = [(b - a) for a, b in zip(pre_floor_prices, pre_floor_prices[1:])]
            non_zero = [mv for mv in moves if mv != 0]
            if non_zero:
                down_frac = sum(1 for mv in non_zero if mv < 0) / len(non_zero)
                if down_frac >= 0.65:
                    return GRIND_DOWN_TO_FLOOR

    # ── 6. Chop then floor: high pre-floor variance followed by floor ─────────
    if len(pre_floor_prices) >= 4 and floor_duration_frac > 0.15:
        mean_p = sum(pre_floor_prices) / len(pre_floor_prices)
        variance = sum((p - mean_p) ** 2 for p in pre_floor_prices) / len(pre_floor_prices)
        std_p = variance ** 0.5
        if std_p > 0.10:
            return CHOP_THEN_FLOOR

    # ── 7. Floor accumulation: default for sustained floor residence ──────────
    return FLOOR_ACCUMULATION


# ── Policy lookup ─────────────────────────────────────────────────────────────

def _policy_mult(pattern: str, category: Optional[str], bucket: str) -> float:
    """
    Look up multiplier for (pattern, category, bucket).
    Tries most-specific key first, falls back via wildcard chain.
    """
    if pattern not in _ALL_PATTERNS:
        return 0.5
    for cat, bkt in (
        (category, bucket),     # exact
        (None,     bucket),     # any category, this bucket
        (category, None),       # any bucket, this category
        (None,     None),       # any category, any bucket
    ):
        val = _POLICY.get((pattern, cat, bkt))
        if val is not None:
            return val
    return 1.0
