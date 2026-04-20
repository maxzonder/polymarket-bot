from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, TypeAlias

from .ids import (
    ClientOrderId,
    ConditionId,
    EventTime,
    IngestTime,
    MarketId,
    OrderId,
    RunId,
    TokenId,
    TradeId,
    VenueFillId,
)


class EventType(StrEnum):
    BOOK_UPDATE = "BookUpdate"
    TRADE_TICK = "TradeTick"
    MARKET_STATE_UPDATE = "MarketStateUpdate"
    TIMER_EVENT = "TimerEvent"
    ORDER_ACCEPTED = "OrderAccepted"
    ORDER_REJECTED = "OrderRejected"
    ORDER_FILLED = "OrderFilled"
    ORDER_CANCELED = "OrderCanceled"


class MarketStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    CLOSED = "closed"
    RESOLVED = "resolved"
    UNKNOWN = "unknown"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class AggressorSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class LiquidityRole(StrEnum):
    MAKER = "maker"
    TAKER = "taker"


@dataclass(frozen=True)
class BookLevel:
    price: float
    size: float


@dataclass(frozen=True)
class BookUpdate:
    event_time_ms: EventTime
    ingest_time_ms: IngestTime
    market_id: MarketId
    token_id: TokenId
    best_bid: float | None
    best_ask: float | None
    spread: float | None = None
    mid_price: float | None = None
    bid_levels: tuple[BookLevel, ...] = ()
    ask_levels: tuple[BookLevel, ...] = ()
    book_seq: int | None = None
    is_snapshot: bool = False
    source: str = "book_update"
    run_id: RunId | None = None
    event_type: EventType = field(default=EventType.BOOK_UPDATE, init=False)


@dataclass(frozen=True)
class TradeTick:
    event_time_ms: EventTime
    ingest_time_ms: IngestTime
    market_id: MarketId
    token_id: TokenId
    price: float
    size: float
    source: str
    trade_id: TradeId | None = None
    aggressor_side: AggressorSide | None = None
    venue_seq: int | None = None
    run_id: RunId | None = None
    event_type: EventType = field(default=EventType.TRADE_TICK, init=False)


@dataclass(frozen=True)
class MarketStateUpdate:
    event_time_ms: EventTime
    ingest_time_ms: IngestTime
    market_id: MarketId
    condition_id: ConditionId | None = None
    question: str | None = None
    status: MarketStatus = MarketStatus.ACTIVE
    start_time_ms: EventTime | None = None
    end_time_ms: EventTime | None = None
    duration_seconds: int | None = None
    token_yes_id: TokenId | None = None
    token_no_id: TokenId | None = None
    fee_rate_bps: float | None = None
    fee_fetched_at_ms: IngestTime | None = None
    fees_enabled: bool | None = None
    is_ascending_market: bool | None = None
    market_source_revision: str | None = None
    source: str = "market_state"
    run_id: RunId | None = None
    # Compatibility fields used by the current MVP vertical slice.
    token_id: TokenId | None = None
    asset_slug: str | None = None
    is_active: bool | None = None
    metadata_is_fresh: bool = True
    fee_metadata_age_ms: int | None = None
    event_type: EventType = field(default=EventType.MARKET_STATE_UPDATE, init=False)

    def __post_init__(self) -> None:
        if self.is_active is None:
            object.__setattr__(self, "is_active", self.status == MarketStatus.ACTIVE)
        elif self.is_active and self.status != MarketStatus.ACTIVE:
            object.__setattr__(self, "status", MarketStatus.ACTIVE)
        elif not self.is_active and self.status == MarketStatus.ACTIVE:
            object.__setattr__(self, "status", MarketStatus.CLOSED)


@dataclass(frozen=True)
class TimerEvent:
    event_time_ms: EventTime
    ingest_time_ms: IngestTime
    timer_kind: str
    source: str = "internal.timer"
    market_id: MarketId | None = None
    token_id: TokenId | None = None
    deadline_ms: int | None = None
    payload: dict[str, Any] | None = None
    run_id: RunId | None = None
    event_type: EventType = field(default=EventType.TIMER_EVENT, init=False)


@dataclass(frozen=True)
class OrderAccepted:
    event_time_ms: EventTime
    ingest_time_ms: IngestTime
    order_id: OrderId
    market_id: MarketId
    token_id: TokenId
    side: OrderSide
    price: float
    size: float
    source: str
    client_order_id: ClientOrderId | None = None
    time_in_force: str | None = None
    post_only: bool | None = None
    venue_status: str = "live"
    run_id: RunId | None = None
    event_type: EventType = field(default=EventType.ORDER_ACCEPTED, init=False)


@dataclass(frozen=True)
class OrderRejected:
    event_time_ms: EventTime
    ingest_time_ms: IngestTime
    market_id: MarketId
    token_id: TokenId
    side: OrderSide
    source: str
    client_order_id: ClientOrderId | None = None
    price: float | None = None
    size: float | None = None
    reject_reason_code: str | None = None
    reject_reason_text: str | None = None
    is_retryable: bool | None = None
    run_id: RunId | None = None
    event_type: EventType = field(default=EventType.ORDER_REJECTED, init=False)


@dataclass(frozen=True)
class OrderFilled:
    event_time_ms: EventTime
    ingest_time_ms: IngestTime
    order_id: OrderId
    market_id: MarketId
    token_id: TokenId
    side: OrderSide
    fill_price: float
    fill_size: float
    cumulative_filled_size: float
    remaining_size: float
    source: str
    client_order_id: ClientOrderId | None = None
    fee_paid_usdc: float | None = None
    liquidity_role: LiquidityRole | None = None
    venue_fill_id: VenueFillId | None = None
    run_id: RunId | None = None
    event_type: EventType = field(default=EventType.ORDER_FILLED, init=False)


@dataclass(frozen=True)
class OrderCanceled:
    event_time_ms: EventTime
    ingest_time_ms: IngestTime
    order_id: OrderId
    market_id: MarketId
    token_id: TokenId
    source: str
    client_order_id: ClientOrderId | None = None
    cancel_reason: str | None = None
    cumulative_filled_size: float | None = None
    remaining_size: float | None = None
    run_id: RunId | None = None
    event_type: EventType = field(default=EventType.ORDER_CANCELED, init=False)


NormalizedEvent: TypeAlias = (
    BookUpdate
    | TradeTick
    | MarketStateUpdate
    | TimerEvent
    | OrderAccepted
    | OrderRejected
    | OrderFilled
    | OrderCanceled
)
