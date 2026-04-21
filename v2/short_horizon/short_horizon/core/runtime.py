from __future__ import annotations

from collections import defaultdict

from ..core.events import BookUpdate, MarketStateUpdate
from ..core.models import OrderIntent, SkipDecision
from ..core.order_state import OrderState
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

        all_orders = self.store.load_all_orders()
        all_fills = self.store.load_fills()

        if bool(getattr(risk, "global_safe_mode", False)):
            return SkipDecision(
                reason="global_safe_mode",
                market_id=decision.market_id,
                token_id=decision.token_id,
                level=decision.level,
                event_time_ms=decision.event_time_ms,
                details="operator_requested",
            )

        open_orders = [row for row in all_orders if str(row.get("state")) in _NON_TERMINAL_ORDER_STATES]

        max_notional_per_strategy_usdc = float(getattr(risk, "max_notional_per_strategy_usdc", 0.0) or 0.0)
        if max_notional_per_strategy_usdc > 0:
            current_strategy_notional = _sum_open_notional_usdc(open_orders)
            projected_strategy_notional = current_strategy_notional + float(decision.notional_usdc)
            if projected_strategy_notional > max_notional_per_strategy_usdc + 1e-9:
                return SkipDecision(
                    reason="max_notional_per_strategy_usdc_reached",
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    level=decision.level,
                    event_time_ms=decision.event_time_ms,
                    details=(
                        f"strategy_notional_usdc={current_strategy_notional:.6f} "
                        f"projected_usdc={projected_strategy_notional:.6f} "
                        f"cap_usdc={max_notional_per_strategy_usdc:.6f}"
                    ),
                )

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

        max_open_orders_per_market = int(getattr(risk, "max_open_orders_per_market", 0) or 0)
        if max_open_orders_per_market > 0:
            market_open_orders = sum(1 for row in open_orders if row.get("market_id") == decision.market_id)
            projected_market_open_orders = market_open_orders + 1
            if projected_market_open_orders > max_open_orders_per_market:
                return SkipDecision(
                    reason="max_open_orders_per_market_reached",
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    level=decision.level,
                    event_time_ms=decision.event_time_ms,
                    details=(
                        f"market_open_orders={market_open_orders} "
                        f"projected_open_orders={projected_market_open_orders} "
                        f"cap={max_open_orders_per_market}"
                    ),
                )

        max_daily_loss_usdc = float(getattr(risk, "max_daily_loss_usdc", 0.0) or 0.0)
        if max_daily_loss_usdc > 0:
            realized_daily_pnl_usdc = _compute_realized_daily_pnl_usdc(
                orders=all_orders,
                fills=all_fills,
                event_time_ms=decision.event_time_ms,
            )
            realized_daily_loss_usdc = max(0.0, -realized_daily_pnl_usdc)
            if realized_daily_loss_usdc > max_daily_loss_usdc + 1e-9:
                return SkipDecision(
                    reason="max_daily_loss_usdc_reached",
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    level=decision.level,
                    event_time_ms=decision.event_time_ms,
                    details=(
                        f"realized_daily_pnl_usdc={realized_daily_pnl_usdc:.6f} "
                        f"realized_daily_loss_usdc={realized_daily_loss_usdc:.6f} "
                        f"cap_usdc={max_daily_loss_usdc:.6f}"
                    ),
                )

        max_consecutive_rejects = int(getattr(risk, "max_consecutive_rejects", 0) or 0)
        if max_consecutive_rejects > 0:
            consecutive_rejects = _count_consecutive_rejects(all_orders)
            if consecutive_rejects >= max_consecutive_rejects:
                return SkipDecision(
                    reason="max_consecutive_rejects_reached",
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    level=decision.level,
                    event_time_ms=decision.event_time_ms,
                    details=f"consecutive_rejects={consecutive_rejects} cap={max_consecutive_rejects}",
                )

        stake_cap_usdc = float(getattr(risk, "micro_live_total_stake_cap_usdc", 0.0) or 0.0)
        if stake_cap_usdc > 0:
            current_open_notional = _sum_open_notional_usdc(open_orders)
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


_NON_TERMINAL_ORDER_STATES = {
    OrderState.INTENT.value,
    OrderState.PENDING_SEND.value,
    OrderState.ACCEPTED.value,
    OrderState.PARTIALLY_FILLED.value,
    OrderState.CANCEL_REQUESTED.value,
    OrderState.REPLACE_REQUESTED.value,
    OrderState.UNKNOWN.value,
}


def _sum_open_notional_usdc(open_orders: list[dict]) -> float:
    current_open_notional = 0.0
    for row in open_orders:
        price = row.get("price")
        size = row.get("remaining_size") if row.get("remaining_size") is not None else row.get("size")
        if price is None or size is None:
            continue
        current_open_notional += float(price) * float(size)
    return current_open_notional


def _count_consecutive_rejects(all_orders: list[dict]) -> int:
    ordered_rows = sorted(
        all_orders,
        key=lambda row: (
            str(row.get("last_state_change_at") or row.get("intent_created_at") or ""),
            str(row.get("order_id") or ""),
        ),
    )
    consecutive_rejects = 0
    for row in reversed(ordered_rows):
        if str(row.get("state") or "") == OrderState.REJECTED.value:
            consecutive_rejects += 1
            continue
        break
    return consecutive_rejects


def _compute_realized_daily_pnl_usdc(*, orders: list[dict], fills: list[dict], event_time_ms: int) -> float:
    target_day = _iso_day_from_ms(event_time_ms)
    order_by_id = {str(row.get("order_id")): row for row in orders if row.get("order_id") is not None}
    inventory_lots: dict[tuple[str | None, str | None], list[list[float]]] = defaultdict(list)
    realized_pnl_usdc = 0.0
    for fill in sorted(fills, key=lambda row: (str(row.get("filled_at") or ""), str(row.get("fill_id") or ""))):
        filled_at = str(fill.get("filled_at") or "")
        if not filled_at.startswith(target_day):
            continue
        order = order_by_id.get(str(fill.get("order_id") or ""), {})
        side = str(order.get("side") or "").upper()
        price = float(fill.get("price") or 0.0)
        size = float(fill.get("size") or 0.0)
        fee_paid_usdc = float(fill.get("fee_paid_usdc") or 0.0)
        market_token = (fill.get("market_id"), fill.get("token_id"))
        if size <= 0:
            realized_pnl_usdc -= fee_paid_usdc
            continue
        if side == "BUY":
            inventory_lots[market_token].append([size, (price * size + fee_paid_usdc) / size])
            continue
        if side == "SELL":
            cost_basis = _consume_cost_basis(inventory_lots[market_token], size=size, fallback_price=price)
            proceeds = price * size - fee_paid_usdc
            realized_pnl_usdc += proceeds - cost_basis
            continue
        realized_pnl_usdc -= fee_paid_usdc
    return realized_pnl_usdc


def _consume_cost_basis(lots: list[list[float]], *, size: float, fallback_price: float) -> float:
    remaining = float(size)
    cost_basis = 0.0
    while remaining > 1e-12 and lots:
        lot_size, unit_cost = lots[0]
        consumed = min(remaining, float(lot_size))
        cost_basis += consumed * float(unit_cost)
        lot_size -= consumed
        remaining -= consumed
        if lot_size <= 1e-12:
            lots.pop(0)
        else:
            lots[0][0] = lot_size
    if remaining > 1e-12:
        cost_basis += remaining * float(fallback_price)
    return cost_basis


def _iso_day_from_ms(event_time_ms: int) -> str:
    from ..storage.runtime import iso_from_ms

    return iso_from_ms(event_time_ms).split("T", 1)[0]
