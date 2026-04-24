from __future__ import annotations

from typing import Iterable

from ..config import ShortHorizonConfig
from ..core.clock import Clock, SystemClock
from ..core.order_state import OrderState
from ..core.events import BookUpdate, MarketStateUpdate, OrderAccepted, OrderCanceled, OrderFilled, OrderRejected, TimerEvent, TradeTick
from ..core.lifecycle import compute_lifecycle_fraction
from ..core.models import MarketState, OrderIntent, SkipDecision, TouchSignal
from ..risk import gate_touch
from ..strategy_api import CancelOrder, Noop, PlaceOrder, StrategyIntent


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
    def __init__(self, *, config: ShortHorizonConfig, clock: Clock | None = None):
        self.config = config
        self.clock = clock or SystemClock()
        self.touch_tracker = FirstTouchTracker(config.triggers.price_levels)
        self.market_state_by_token: dict[str, MarketState] = {}
        self.active_order_ids_by_market_token: dict[tuple[str, str], str] = {}
        self.active_order_states_by_market_token: dict[tuple[str, str], OrderState] = {}
        self.active_inventory_by_market: dict[str, set[str]] = {}

    def on_market_event(self, event: BookUpdate | MarketStateUpdate | TradeTick) -> list[StrategyIntent]:
        if isinstance(event, MarketStateUpdate):
            return self.on_market_state(event)
        if isinstance(event, TradeTick):
            return []

        outputs: list[StrategyIntent] = []
        for touch in self.detect_touches(event):
            if self._has_active_order(market_id=event.market_id, token_id=event.token_id):
                outputs.append(
                    Noop(
                        reason="open_order_exists",
                        market_id=event.market_id,
                        token_id=event.token_id,
                        details={"existing_order_id": self.active_order_ids_by_market_token[(event.market_id, event.token_id)]},
                    )
                )
                continue
            market_inventory = self.active_inventory_by_market.get(event.market_id, set())
            if market_inventory:
                outputs.append(
                    Noop(
                        reason="opposite_side_position_on_market",
                        market_id=event.market_id,
                        token_id=event.token_id,
                        details={"inventory_token_ids": sorted(market_inventory)},
                    )
                )
                continue
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
        key = (event.market_id, event.token_id)
        if isinstance(event, OrderAccepted):
            self.active_order_ids_by_market_token[key] = event.order_id
            self.active_order_states_by_market_token[key] = OrderState.ACCEPTED
            return []
        if isinstance(event, OrderFilled):
            self.active_inventory_by_market.setdefault(event.market_id, set()).add(event.token_id)
            if event.remaining_size > 1e-12:
                self.active_order_ids_by_market_token[key] = event.order_id
                self.active_order_states_by_market_token[key] = OrderState.PARTIALLY_FILLED
            else:
                self.active_order_ids_by_market_token.pop(key, None)
                self.active_order_states_by_market_token.pop(key, None)
            return []
        self.active_order_ids_by_market_token.pop(key, None)
        self.active_order_states_by_market_token.pop(key, None)
        return []

    def on_timer(self, timer: TimerEvent) -> list[StrategyIntent]:
        return []

    def hydrate_open_orders(self, rows: Iterable[dict]) -> None:
        self.active_order_ids_by_market_token.clear()
        self.active_order_states_by_market_token.clear()
        self.active_inventory_by_market.clear()
        for row in rows:
            state = OrderState(str(row["state"]))
            market_id = str(row["market_id"])
            token_id = str(row["token_id"])
            if _row_has_inventory_exposure(row, state=state):
                self.active_inventory_by_market.setdefault(market_id, set()).add(token_id)
            if state in {OrderState.REJECTED, OrderState.FILLED, OrderState.CANCEL_CONFIRMED, OrderState.EXPIRED, OrderState.REPLACED}:
                continue
            key = (market_id, token_id)
            self.active_order_ids_by_market_token[key] = str(row["order_id"])
            self.active_order_states_by_market_token[key] = state

    def on_market_state(self, event: MarketStateUpdate) -> list[StrategyIntent]:
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
        if not event.is_active or event.event_time_ms >= event.end_time_ms:
            self.active_inventory_by_market.pop(event.market_id, None)
        if self.config.execution.hold_to_resolution:
            return []
        key = (event.market_id, event.token_id)
        order_id = self.active_order_ids_by_market_token.get(key)
        order_state = self.active_order_states_by_market_token.get(key)
        if not order_id or order_state not in {OrderState.ACCEPTED, OrderState.PARTIALLY_FILLED}:
            return []
        if event.is_active and event.event_time_ms < event.end_time_ms:
            return []
        self.active_order_states_by_market_token[key] = OrderState.CANCEL_REQUESTED
        return [
            CancelOrder(
                market_id=event.market_id,
                token_id=event.token_id,
                reason="market_inactive_or_window_closed",
            )
        ]

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

    def _has_active_order(self, *, market_id: str, token_id: str) -> bool:
        return (market_id, token_id) in self.active_order_ids_by_market_token


def _row_has_inventory_exposure(row: dict, *, state: OrderState) -> bool:
    if state is OrderState.FILLED:
        return True
    cumulative_filled_size = row.get("cumulative_filled_size")
    if cumulative_filled_size is None:
        return False
    try:
        return float(cumulative_filled_size) > 1e-12
    except (TypeError, ValueError):
        return False
