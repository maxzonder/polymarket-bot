from __future__ import annotations

from .config import ShortHorizonConfig
from .events import BookUpdate, MarketStateUpdate
from .lifecycle import compute_lifecycle_fraction, is_in_bucket
from .models import MarketState, OrderIntent, SkipDecision
from .storage import RuntimeStore
from .touch import FirstTouchTracker


class ShortHorizonEngine:
    """Minimal vertical slice for the short-horizon MVP.

    Current responsibility:
    - accept normalized market-state events
    - accept normalized book updates
    - detect ascending first touches without intra-lifecycle re-arm
    - apply the MVP decision gates
    - persist order intents for the execution layer to consume later
    """

    def __init__(self, *, config: ShortHorizonConfig, intent_store: RuntimeStore):
        self.config = config
        self.store = intent_store
        self.touch_tracker = FirstTouchTracker(config.triggers.price_levels)
        self.market_state_by_token: dict[str, MarketState] = {}

    def on_market_state(self, event: MarketStateUpdate) -> None:
        self.store.append_event(event)
        self.store.upsert_market_state(event)
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
            fee_metadata_age_ms=event.fee_metadata_age_ms,
        )

    def on_book_update(self, event: BookUpdate) -> list[OrderIntent | SkipDecision]:
        self.store.append_event(event)
        touches = self.touch_tracker.observe_best_ask(
            market_id=event.market_id,
            token_id=event.token_id,
            best_ask=event.best_ask,
            event_time_ms=event.event_time_ms,
        )
        if not touches:
            return []

        state = self.market_state_by_token.get(event.token_id)
        outputs: list[OrderIntent | SkipDecision] = []
        for touch in touches:
            self.store.record_first_touch(
                market_id=touch.market_id,
                token_id=touch.token_id,
                level=touch.level,
                event_time_ms=touch.event_time_ms,
            )
            skip = self._gate_touch(event=event, state=state, level=touch.level)
            if skip is not None:
                outputs.append(skip)
                continue

            assert state is not None
            lifecycle_fraction = compute_lifecycle_fraction(
                start_time_ms=state.start_time_ms,
                end_time_ms=state.end_time_ms,
                event_time_ms=event.event_time_ms,
            )
            intent = OrderIntent(
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
            self.store.persist_intent(intent)
            outputs.append(intent)
        return outputs

    def _gate_touch(
        self,
        *,
        event: BookUpdate,
        state: MarketState | None,
        level: float,
    ) -> SkipDecision | None:
        if state is None:
            return SkipDecision(
                reason="missing_market_state",
                market_id=event.market_id,
                token_id=event.token_id,
                level=level,
                event_time_ms=event.event_time_ms,
            )
        if not state.is_active:
            return SkipDecision(
                reason="inactive_market",
                market_id=state.market_id,
                token_id=state.token_id,
                level=level,
                event_time_ms=event.event_time_ms,
            )
        if state.asset_slug not in set(self.config.universe.allowed_assets):
            return SkipDecision(
                reason="asset_not_allowed",
                market_id=state.market_id,
                token_id=state.token_id,
                level=level,
                event_time_ms=event.event_time_ms,
                details=state.asset_slug,
            )
        if not state.metadata_is_fresh:
            return SkipDecision(
                reason="stale_market_metadata",
                market_id=state.market_id,
                token_id=state.token_id,
                level=level,
                event_time_ms=event.event_time_ms,
            )
        if self.config.fees.reject_if_fee_metadata_stale:
            if state.fee_metadata_age_ms is None:
                return SkipDecision(
                    reason="missing_fee_metadata",
                    market_id=state.market_id,
                    token_id=state.token_id,
                    level=level,
                    event_time_ms=event.event_time_ms,
                )
            if state.fee_metadata_age_ms > self.config.fees.fee_metadata_ttl_seconds * 1000:
                return SkipDecision(
                    reason="stale_fee_metadata",
                    market_id=state.market_id,
                    token_id=state.token_id,
                    level=level,
                    event_time_ms=event.event_time_ms,
                )
        if event.best_ask is None:
            return SkipDecision(
                reason="missing_best_ask",
                market_id=state.market_id,
                token_id=state.token_id,
                level=level,
                event_time_ms=event.event_time_ms,
            )
        if event.ingest_time_ms - event.event_time_ms > self.config.execution.stale_market_data_threshold_ms:
            return SkipDecision(
                reason="stale_market_data",
                market_id=state.market_id,
                token_id=state.token_id,
                level=level,
                event_time_ms=event.event_time_ms,
            )

        lifecycle_fraction = compute_lifecycle_fraction(
            start_time_ms=state.start_time_ms,
            end_time_ms=state.end_time_ms,
            event_time_ms=event.event_time_ms,
        )
        if not is_in_bucket(
            fraction=lifecycle_fraction,
            bucket_start=self.config.lifecycle.bucket_start_fraction,
            bucket_end=self.config.lifecycle.bucket_end_fraction,
        ):
            return SkipDecision(
                reason="wrong_lifecycle_bucket",
                market_id=state.market_id,
                token_id=state.token_id,
                level=level,
                event_time_ms=event.event_time_ms,
                details=f"fraction={lifecycle_fraction}",
            )

        max_entry_price = level + self.config.triggers.max_entry_drift_ticks * self.config.triggers.tick_size
        if float(event.best_ask) > max_entry_price + 1e-9:
            return SkipDecision(
                reason="bad_entry_ask_state",
                market_id=state.market_id,
                token_id=state.token_id,
                level=level,
                event_time_ms=event.event_time_ms,
                details=f"best_ask={event.best_ask}",
            )
        return None
