from __future__ import annotations

from dataclasses import dataclass

from .ids import EventTime, MarketId, StrategyId, TokenId


@dataclass(frozen=True)
class MarketState:
    market_id: MarketId
    token_id: TokenId
    condition_id: str | None
    question: str | None
    asset_slug: str
    start_time_ms: EventTime
    end_time_ms: EventTime
    is_active: bool
    metadata_is_fresh: bool
    fee_rate_bps: float | None
    fee_metadata_age_ms: int | None


@dataclass(frozen=True)
class TouchSignal:
    market_id: MarketId
    token_id: TokenId
    level: float
    previous_best_ask: float
    current_best_ask: float
    event_time_ms: EventTime


@dataclass(frozen=True)
class OrderIntent:
    intent_id: str
    strategy_id: StrategyId | str
    market_id: MarketId
    token_id: TokenId
    condition_id: str | None
    question: str | None
    asset_slug: str
    level: float
    entry_price: float
    notional_usdc: float
    lifecycle_fraction: float
    event_time_ms: EventTime
    reason: str = "ascending_first_touch"


@dataclass(frozen=True)
class SkipDecision:
    reason: str
    market_id: MarketId
    token_id: TokenId
    level: float
    event_time_ms: EventTime
    details: str = ""
