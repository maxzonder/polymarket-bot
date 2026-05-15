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


@dataclass
class BlackSwanConfig(SwanConfig):
    strategy_id: str = "black_swan_v1"
    # Stricter universe → fewer candidates but higher quality; keep headroom.
    max_open_resting_bids: int = 500
    max_resting_markets: int = 5000
    max_total_stake_usdc: float = 1000.0
    # Most events resolve within 6h of entry (final_6h cohort dominates).
    # For short markets, use a relative cap so a fixed 6h TTL does not dominate
    # the whole market lifecycle (1h market → 30m TTL).
    stale_order_ttl_seconds: float = 21600.0
    stale_order_ttl_fraction_of_duration: float = 0.50
    # Match BLACK_SWAN_MODE's relative remaining-time gate. 24m remains the
    # fallback/cap for markets missing total-duration metadata, but known 1h
    # markets use the final 10% (~6m) instead of the old fixed 24m window.
    cancel_when_remaining_seconds_lt: float = 1440.0
    cancel_when_remaining_fraction_lt: float = 0.10
    # Venue-minimum guardrail: allow the full BLACK_SWAN_MODE ladder with
    # --stake-per-level=1.0 while bounding each market around one effective
    # venue-minimum order per level.  The previous ~$1/market cap intentionally
    # narrowed paper to 0.005-only; issue #202 switches paper coverage to the
    # full 0.005..0.05 low-zone ladder for #196 validation.
    use_effective_notional_for_caps: bool = True
    max_effective_notional_per_market_usdc: float = 5.5
    skip_below_venue_min_effective_notional: bool = True
    venue_min_order_notional_usdc: float = 1.0
    venue_min_order_shares_fallback: float = 5.0


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
