"""
Black Swan Strategy V1
======================

Resting-bid strategy targeting strict black_swan events: tokens that were
available for pennies and resolved to $1.

Mechanics are identical to SwanStrategyV1 (resting bids, stale cleanup).
The key difference is the candidate universe and config defaults:

- Candidates must come from a screener filtered by `black_swan=1` in
  polymarket_dataset.db (issue #180).
- Entry levels are more aggressive (sub-penny to ~$0.01).
- Longer stale TTL to survive the final_6h buy phase (the dominant cohort).

Issue #180 analytics (2025-08-01 → 2026-04-26):
  - 4639 strict black swans over the period, avg 17.6/day
  - overall hit-rate: 69.3% (winners 4639, strict losers 2051)
  - avg X: 66.7x, median X: 33.3x
  - avg visible low-zone volume: $616.8
  - dominant buy_phase: final_6h (3315 events, avg 62.1x)
"""
from __future__ import annotations

from dataclasses import dataclass

from .swan_strategy_v1 import SwanCandidate, SwanConfig, SwanStrategyV1

from ..core.clock import Clock
from ..execution.order_translator import (
    DEFAULT_POLYMARKET_MIN_ORDER_SHARES_FALLBACK,
    DEFAULT_VENUE_MIN_ORDER_NOTIONAL_USDC,
)


@dataclass
class BlackSwanConfig(SwanConfig):
    strategy_id: str = "black_swan_v1"
    # Stricter universe → fewer candidates but higher quality; keep headroom.
    max_open_resting_bids: int = 500
    max_resting_markets: int = 5000
    max_total_stake_usdc: float = 1000.0
    # Most events resolve within 6h of entry (final_6h cohort dominates), but
    # issue #203 paper audit showed the old 6h cap caused unnecessary
    # cancel/reopen churn on long markets. Keep the duration-relative guard for
    # short markets while allowing long-market bids to rest up to 10h.
    stale_order_ttl_seconds: float = 36000.0
    stale_order_ttl_fraction_of_duration: float = 0.50
    # Match BLACK_SWAN_MODE's relative remaining-time gate. 24m remains the
    # fallback/cap for markets missing total-duration metadata, but known 1h
    # markets use the final 10% (~6m) instead of the old fixed 24m window.
    cancel_when_remaining_seconds_lt: float = 1440.0
    cancel_when_remaining_fraction_lt: float = 0.10
    # Venue-minimum guardrail: allow BLACK_SWAN_MODE.stake_per_level=1.0 to be
    # translated into venue-valid orders.  Per-market ladder size is controlled
    # by BLACK_SWAN_MODE.entry_price_levels + stake_per_level, not by a separate
    # manually maintained 5.5 USDC cap.
    use_effective_notional_for_caps: bool = True
    max_effective_notional_per_market_usdc: float = 0.0
    skip_below_venue_min_effective_notional: bool = True
    venue_min_order_notional_usdc: float = DEFAULT_VENUE_MIN_ORDER_NOTIONAL_USDC
    venue_min_order_shares_fallback: float = DEFAULT_POLYMARKET_MIN_ORDER_SHARES_FALLBACK


class BlackSwanStrategyV1(SwanStrategyV1):
    """
    Black Swan resting-bid strategy.

    Inherits all order-management logic from SwanStrategyV1.
    Candidates must be pre-filtered by the screener to black_swan=1 markets.
    """

    def __init__(
        self,
        *,
        config: BlackSwanConfig | None = None,
        clock: Clock | None = None,
    ) -> None:
        super().__init__(config=config or BlackSwanConfig(), clock=clock)
