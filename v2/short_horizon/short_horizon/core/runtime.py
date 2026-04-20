from __future__ import annotations

from ..core.events import BookUpdate, MarketStateUpdate
from ..core.models import OrderIntent, SkipDecision
from ..storage.runtime import RuntimeStore
from ..strategy_api import TouchStrategy


class StrategyRuntime:
    """Shared event-path shell for replay and live runners.

    Current responsibility in P1-1:
    - append normalized events to storage
    - feed them into a strategy implementation
    - persist first-touch state and order intents
    """

    def __init__(self, *, strategy: TouchStrategy, intent_store: RuntimeStore):
        self.strategy = strategy
        self.store = intent_store

    def on_market_state(self, event: MarketStateUpdate) -> None:
        self.store.append_event(event)
        self.store.upsert_market_state(event)
        self.strategy.on_market_state(event)

    def on_book_update(self, event: BookUpdate) -> list[OrderIntent | SkipDecision]:
        self.store.append_event(event)
        outputs: list[OrderIntent | SkipDecision] = []
        for touch in self.strategy.detect_touches(event):
            self.store.record_first_touch(
                market_id=touch.market_id,
                token_id=touch.token_id,
                level=touch.level,
                event_time_ms=touch.event_time_ms,
            )
            decision = self.strategy.decide_on_touch(event=event, touch=touch)
            if isinstance(decision, OrderIntent):
                self.store.persist_intent(decision)
            outputs.append(decision)
        return outputs
