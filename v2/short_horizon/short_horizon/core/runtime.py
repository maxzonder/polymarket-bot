from __future__ import annotations

from ..core.events import BookUpdate, MarketStateUpdate
from ..core.models import OrderIntent, SkipDecision
from ..storage.runtime import RuntimeStore
from ..strategy_api import TouchStrategy
from ..telemetry import event_log_fields, get_logger


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
        self.logger = get_logger("short_horizon.runtime", run_id=intent_store.current_run_id)

    def on_market_state(self, event: MarketStateUpdate) -> None:
        log = self.logger.bind(**event_log_fields(event))
        self.store.append_event(event)
        self.store.upsert_market_state(event)
        self.strategy.on_market_state(event)
        log.info(
            "market_state_ingested",
            condition_id=event.condition_id,
            question=event.question,
            status=event.status,
            asset_slug=event.asset_slug,
        )

    def on_book_update(self, event: BookUpdate) -> list[OrderIntent | SkipDecision]:
        log = self.logger.bind(**event_log_fields(event))
        self.store.append_event(event)
        log.info(
            "book_update_ingested",
            best_bid=event.best_bid,
            best_ask=event.best_ask,
            spread=event.spread,
            mid_price=event.mid_price,
        )
        outputs: list[OrderIntent | SkipDecision] = []
        for touch in self.strategy.detect_touches(event):
            self.store.record_first_touch(
                market_id=touch.market_id,
                token_id=touch.token_id,
                level=touch.level,
                event_time_ms=touch.event_time_ms,
            )
            touch_log = log.bind(level=touch.level, previous_best_ask=touch.previous_best_ask, current_best_ask=touch.current_best_ask)
            touch_log.info("touch_detected")
            decision = self.strategy.decide_on_touch(event=event, touch=touch)
            if isinstance(decision, OrderIntent):
                self.store.persist_intent(decision)
                touch_log.info(
                    "order_intent_created",
                    order_id=decision.intent_id,
                    entry_price=decision.entry_price,
                    notional_usdc=decision.notional_usdc,
                    lifecycle_fraction=decision.lifecycle_fraction,
                    strategy_id=decision.strategy_id,
                )
            else:
                touch_log.info(
                    "touch_skipped",
                    reason=decision.reason,
                    details=decision.details,
                )
            outputs.append(decision)
        return outputs
