from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, TypeAlias

from ..core.events import (
    BookUpdate,
    MarketStateUpdate,
    OrderAccepted,
    OrderCanceled,
    OrderFilled,
    OrderRejected,
    TimerEvent,
    TradeTick,
)
from ..core.models import OrderIntent


@dataclass(frozen=True)
class PlaceOrder:
    intent: OrderIntent


@dataclass(frozen=True)
class CancelOrder:
    market_id: str
    token_id: str
    reason: str = ""


@dataclass(frozen=True)
class Noop:
    reason: str = ""
    market_id: str | None = None
    token_id: str | None = None
    details: str = ""


StrategyIntent: TypeAlias = PlaceOrder | CancelOrder | Noop
MarketEvent: TypeAlias = BookUpdate | MarketStateUpdate | TradeTick
OrderEvent: TypeAlias = OrderAccepted | OrderRejected | OrderFilled | OrderCanceled


class Strategy(Protocol):
    def on_market_event(self, event: MarketEvent) -> list[StrategyIntent]:
        ...

    def on_order_event(self, event: OrderEvent) -> list[StrategyIntent]:
        ...

    def on_timer(self, timer: TimerEvent) -> list[StrategyIntent]:
        ...


class TouchStrategy(Protocol):
    def on_market_state(self, event: MarketStateUpdate) -> None:
        ...

    def detect_touches(self, event: BookUpdate):
        ...

    def decide_on_touch(self, *, event: BookUpdate, touch):
        ...


__all__ = [
    "CancelOrder",
    "MarketEvent",
    "Noop",
    "OrderEvent",
    "PlaceOrder",
    "Strategy",
    "StrategyIntent",
    "TouchStrategy",
]
