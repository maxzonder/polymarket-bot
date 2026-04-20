from __future__ import annotations

from typing import Protocol

from ..core.events import BookUpdate, MarketStateUpdate
from ..core.models import OrderIntent, SkipDecision, TouchSignal


class TouchStrategy(Protocol):
    def on_market_state(self, event: MarketStateUpdate) -> None:
        ...

    def detect_touches(self, event: BookUpdate) -> list[TouchSignal]:
        ...

    def decide_on_touch(self, *, event: BookUpdate, touch: TouchSignal) -> OrderIntent | SkipDecision:
        ...


__all__ = ["TouchStrategy"]
