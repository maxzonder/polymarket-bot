from __future__ import annotations

from ..config import ShortHorizonConfig
from ..core.events import BookUpdate, MarketStateUpdate, OrderAccepted, OrderCanceled, OrderFilled, OrderRejected, TimerEvent, TradeTick
from ..core.lifecycle import compute_lifecycle_fraction
from ..core.models import MarketState, OrderIntent, SkipDecision, TouchSignal
from ..risk import gate_touch
from ..strategy_api import Noop, PlaceOrder, StrategyIntent


class FirstTouchTracker:
    def __init__(self, levels: tuple[float, ...]):
        self.levels = tuple(sorted(levels))
        self._last_best_ask: dict[tuple[str, float], float] = {}
        self._fired: set[tuple[str, str, float]] = set()

    def observe_best_ask(
        self,
        *,
        market_id: str,
        token_id: str,
        best_ask: float | None,
        event_time_ms: int,
    ) -> list[TouchSignal]:
        if best_ask is None:
            return []

        touches: list[TouchSignal] = []
        best_ask = float(best_ask)
        for level in self.levels:
            last_key = (token_id, level)
            fired_key = (market_id, token_id, level)
            prev = self._last_best_ask.get(last_key)
            self._last_best_ask[last_key] = best_ask
            if prev is None:
                continue
            if fired_key in self._fired:
                continue
            if prev < level <= best_ask:
                self._fired.add(fired_key)
                touches.append(
                    TouchSignal(
                        market_id=market_id,
                        token_id=token_id,
                        level=level,
                        previous_best_ask=prev,
                        current_best_ask=best_ask,
                        event_time_ms=event_time_ms,
                    )
                )
        return touches


class ShortHorizon15mTouchStrategy:
    def __init__(self, *, config: ShortHorizonConfig):
        self.config = config
        self.touch_tracker = FirstTouchTracker(config.triggers.price_levels)
        self.market_state_by_token: dict[str, MarketState] = {}

    def on_market_event(self, event: BookUpdate | MarketStateUpdate | TradeTick) -> list[StrategyIntent]:
        if isinstance(event, MarketStateUpdate):
            self.on_market_state(event)
            return []
        if isinstance(event, TradeTick):
            return []

        outputs: list[StrategyIntent] = []
        for touch in self.detect_touches(event):
            decision = self.decide_on_touch(event=event, touch=touch)
            if isinstance(decision, OrderIntent):
                outputs.append(PlaceOrder(decision))
            else:
                outputs.append(
                    Noop(
                        reason=decision.reason,
                        market_id=decision.market_id,
                        token_id=decision.token_id,
                        details=decision.details,
                    )
                )
        return outputs

    def on_order_event(self, event: OrderAccepted | OrderRejected | OrderFilled | OrderCanceled) -> list[StrategyIntent]:
        return []

    def on_timer(self, timer: TimerEvent) -> list[StrategyIntent]:
        return []

    def on_market_state(self, event: MarketStateUpdate) -> None:
        self.market_state_by_token[event.token_id] = MarketState(
            market_id=event.market_id,
            token_id=event.token_id,
            condition_id=event.condition_id,
            question=event.question,
            asset_slug=event.asset_slug.lower(),
            start_time_ms=event.start_time_ms,
            end_time_ms=event.end_time_ms,
            is_active=event.is_active,
            metadata_is_fresh=event.metadata_is_fresh,
            fee_rate_bps=event.fee_rate_bps,
            fee_fetched_at_ms=event.fee_fetched_at_ms,
            fee_metadata_age_ms=event.fee_metadata_age_ms,
        )

    def detect_touches(self, event: BookUpdate) -> list[TouchSignal]:
        return self.touch_tracker.observe_best_ask(
            market_id=event.market_id,
            token_id=event.token_id,
            best_ask=event.best_ask,
            event_time_ms=event.event_time_ms,
        )

    def decide_on_touch(self, *, event: BookUpdate, touch: TouchSignal) -> OrderIntent | SkipDecision:
        state = self.market_state_by_token.get(event.token_id)
        skip = gate_touch(config=self.config, event=event, state=state, level=touch.level)
        if skip is not None:
            return skip

        assert state is not None
        lifecycle_fraction = compute_lifecycle_fraction(
            start_time_ms=state.start_time_ms,
            end_time_ms=state.end_time_ms,
            event_time_ms=event.event_time_ms,
        )
        return OrderIntent(
            intent_id=f"{state.market_id}:{state.token_id}:{touch.level:.2f}:{event.event_time_ms}",
            strategy_id=self.config.strategy_id,
            market_id=state.market_id,
            token_id=state.token_id,
            condition_id=state.condition_id,
            question=state.question,
            asset_slug=state.asset_slug,
            level=touch.level,
            entry_price=float(event.best_ask),
            notional_usdc=self.config.execution.target_trade_size_usdc,
            lifecycle_fraction=float(lifecycle_fraction),
            event_time_ms=event.event_time_ms,
        )
