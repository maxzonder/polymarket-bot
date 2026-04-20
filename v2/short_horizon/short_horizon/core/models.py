from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketState:
    market_id: str
    token_id: str
    condition_id: str
    question: str
    asset_slug: str
    start_time_ms: int
    end_time_ms: int
    is_active: bool
    metadata_is_fresh: bool
    fee_rate_bps: float | None
    fee_metadata_age_ms: int | None


@dataclass(frozen=True)
class TouchSignal:
    market_id: str
    token_id: str
    level: float
    previous_best_ask: float
    current_best_ask: float
    event_time_ms: int


@dataclass(frozen=True)
class OrderIntent:
    intent_id: str
    strategy_id: str
    market_id: str
    token_id: str
    condition_id: str
    question: str
    asset_slug: str
    level: float
    entry_price: float
    notional_usdc: float
    lifecycle_fraction: float
    event_time_ms: int
    reason: str = "ascending_first_touch"


@dataclass(frozen=True)
class SkipDecision:
    reason: str
    market_id: str
    token_id: str
    level: float
    event_time_ms: int
    details: str = ""
