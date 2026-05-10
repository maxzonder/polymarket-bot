"""
MarketPatternTracker — fetches full trade history on first sight of a market
and classifies the price-action pattern used as a scoring multiplier in the
screener.  Pattern labels and multipliers derive from issue #180 Phase E
analytics.

Lifecycle-fraction normalization: patterns are detected relative to
(ts - first_trade_ts) / (end_date_ts - first_trade_ts) so that a "rapid
collapse" means the same fraction of market life regardless of whether the
market is 2 hours or 7 days long.
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

# (pattern, category) → multiplier; None = any category fallback
_POLICY: dict[tuple[str, Optional[str]], float] = {
    # floor_accumulation: best pattern — long residence at floor with confirmed shock
    (FLOOR_ACCUMULATION, "weather"): 1.5,
    (FLOOR_ACCUMULATION, "crypto"):  1.2,
    (FLOOR_ACCUMULATION, "sports"):  1.1,
    (FLOOR_ACCUMULATION, None):      1.0,
    # chop_then_floor: high pre-floor variance, then settled — good signal
    (CHOP_THEN_FLOOR, "weather"): 1.5,
    (CHOP_THEN_FLOOR, "crypto"):  1.1,
    (CHOP_THEN_FLOOR, "sports"):  1.0,
    (CHOP_THEN_FLOOR, None):      1.0,
    # wick_floor_touch: brief dip, recovered — mild signal
    (WICK_FLOOR_TOUCH, "weather"): 1.2,
    (WICK_FLOOR_TOUCH, "crypto"):  1.1,
    (WICK_FLOOR_TOUCH, "sports"):  0.8,
    (WICK_FLOOR_TOUCH, None):      1.0,
    # sudden_collapse: very recent crash to floor — uncertain direction
    (SUDDEN_COLLAPSE, "weather"): 0.7,
    (SUDDEN_COLLAPSE, "crypto"):  1.0,
    (SUDDEN_COLLAPSE, "sports"):  0.6,
    (SUDDEN_COLLAPSE, None):      0.8,
    # grind_down_to_floor: slow bleed → SKIP universally (loser trajectory)
    (GRIND_DOWN_TO_FLOOR, None): 0.0,
    # penny_floor_touch: reached ≤ 0.02 → likely dead; skip except crypto
    (PENNY_FLOOR_TOUCH, "weather"): 0.0,
    (PENNY_FLOOR_TOUCH, "sports"):  0.0,
    (PENNY_FLOOR_TOUCH, "crypto"):  0.4,
    (PENNY_FLOOR_TOUCH, None):      0.0,
    # no_pre_history: too few trades to judge; conservative
    (NO_PRE_HISTORY, "sports"): 0.0,
    (NO_PRE_HISTORY, None):     0.5,
    # no_floor_yet: price never reached floor — don't seed yet
    (NO_FLOOR_YET, None): 0.0,
}

_ALL_PATTERNS = frozenset({
    FLOOR_ACCUMULATION, CHOP_THEN_FLOOR, WICK_FLOOR_TOUCH,
    SUDDEN_COLLAPSE, GRIND_DOWN_TO_FLOOR, PENNY_FLOOR_TOUCH,
    NO_PRE_HISTORY, NO_FLOOR_YET,
})


@dataclass
class _PatternState:
    condition_id: str
    pattern: str
    mult: float
    fetched_at: float
    trade_count: int


class MarketPatternTracker:
    """
    Stateful component that fetches trade history once per market and classifies
    its price-action pattern.  All calls to get_pattern_mult() from a single
    screener thread are safe; no concurrency locking is needed since only one
    screener task exists.
    """

    def __init__(self) -> None:
        self._cache: dict[str, _PatternState] = {}

    def get_pattern_mult(self, m) -> float:
        """
        Return the pattern multiplier (0.0 = skip, <1.0 = reduce, >1.0 = boost).
        Fetches trades on first call; returns cached result for 30 min thereafter.
        m must be a MarketInfo with .market_id, .condition_id, .category, .end_date_ts.
        """
        state = self._cache.get(m.market_id)
        now = time.time()
        if state is None or (now - state.fetched_at) > _CACHE_TTL_SECONDS:
            state = self._fetch_and_classify(m, now)
            self._cache[m.market_id] = state
        return state.mult

    def get_pattern_label(self, market_id: str) -> Optional[str]:
        state = self._cache.get(market_id)
        return state.pattern if state else None

    def _fetch_and_classify(self, m, now: float) -> _PatternState:
        trades = _fetch_trades(m.condition_id)
        pattern = _classify(trades, m.end_date_ts)
        mult = _policy_mult(pattern, m.category)
        return _PatternState(
            condition_id=m.condition_id,
            pattern=pattern,
            mult=mult,
            fetched_at=now,
            trade_count=len(trades),
        )


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
    # floor_duration_frac < 10% of life AND latest price bounced above 0.08
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
            non_zero = [m for m in moves if m != 0]
            if non_zero:
                down_frac = sum(1 for m in non_zero if m < 0) / len(non_zero)
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

def _policy_mult(pattern: str, category: Optional[str]) -> float:
    """Look up (pattern, category) → multiplier; fall back to (pattern, None)."""
    if pattern not in _ALL_PATTERNS:
        return 0.5
    specific = _POLICY.get((pattern, category))
    if specific is not None:
        return specific
    return _POLICY.get((pattern, None), 1.0)
