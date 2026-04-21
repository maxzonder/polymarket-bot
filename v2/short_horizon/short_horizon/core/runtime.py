from __future__ import annotations

from ..core.events import BookUpdate, MarketStateUpdate
from ..core.models import OrderIntent, SkipDecision
from ..storage.runtime import RuntimeStore
from ..strategy_api import StrategyIntent, TouchStrategy
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

    def on_market_state(self, event: MarketStateUpdate) -> list[StrategyIntent]:
        log = self.logger.bind(**event_log_fields(event))
        self.store.append_event(event)
        self.store.upsert_market_state(event)
        outputs = self.strategy.on_market_state(event)
        log.info(
            "market_state_ingested",
            condition_id=event.condition_id,
            question=event.question,
            status=event.status,
            asset_slug=event.asset_slug,
        )
        return outputs

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
            if self.store.has_unknown_order_for_market(event.market_id):
                decision = SkipDecision(
                    reason="market_reconciliation_blocked",
                    market_id=event.market_id,
                    token_id=event.token_id,
                    level=touch.level,
                    event_time_ms=event.event_time_ms,
                    details="unknown_order_present",
                )
                touch_log.info(
                    "touch_skipped",
                    reason=decision.reason,
                    details=decision.details,
                )
                outputs.append(decision)
                continue
            decision = self.strategy.decide_on_touch(event=event, touch=touch)
            if isinstance(decision, OrderIntent):
                decision = self._apply_runtime_guards(decision)
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

    def _apply_runtime_guards(self, decision: OrderIntent) -> OrderIntent | SkipDecision:
        config = getattr(self.strategy, "config", None)
        risk = getattr(config, "risk", None)
        if risk is None:
            return decision

        if bool(getattr(risk, "global_safe_mode", False)):
            return SkipDecision(
                reason="global_safe_mode",
                market_id=decision.market_id,
                token_id=decision.token_id,
                level=decision.level,
                event_time_ms=decision.event_time_ms,
                details="operator_requested",
            )

        open_orders = self.store.load_non_terminal_orders()
        max_open_orders_total = int(getattr(risk, "max_open_orders_total", 0) or 0)
        if max_open_orders_total > 0 and len(open_orders) >= max_open_orders_total:
            return SkipDecision(
                reason="max_open_orders_total_reached",
                market_id=decision.market_id,
                token_id=decision.token_id,
                level=decision.level,
                event_time_ms=decision.event_time_ms,
                details=f"open_orders={len(open_orders)} cap={max_open_orders_total}",
            )

        stake_cap_usdc = float(getattr(risk, "micro_live_total_stake_cap_usdc", 0.0) or 0.0)
        if stake_cap_usdc > 0:
            current_open_notional = 0.0
            for row in open_orders:
                price = row.get("price")
                size = row.get("remaining_size") if row.get("remaining_size") is not None else row.get("size")
                if price is None or size is None:
                    continue
                current_open_notional += float(price) * float(size)
            projected_open_notional = current_open_notional + float(decision.notional_usdc)
            if projected_open_notional > stake_cap_usdc + 1e-9:
                return SkipDecision(
                    reason="micro_live_total_stake_cap_reached",
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    level=decision.level,
                    event_time_ms=decision.event_time_ms,
                    details=(
                        f"open_notional_usdc={current_open_notional:.6f} "
                        f"projected_usdc={projected_open_notional:.6f} cap_usdc={stake_cap_usdc:.6f}"
                    ),
                )
        return decision
