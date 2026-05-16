from __future__ import annotations

from collections import defaultdict

from ..core.clock import Clock, SystemClock
from ..core.events import BookUpdate, MarketResolvedWithInventory, MarketStateUpdate, OrderIntentEvent, OrderSide, SkipDecisionEvent, SpotPriceUpdate
from ..core.models import OrderIntent, SkipDecision
from ..core.order_state import OrderState
from ..storage.runtime import RuntimeStore
from ..strategy_api import StrategyIntent, TouchStrategy
from ..telemetry import event_log_fields, get_logger


DEFAULT_VENUE_MIN_NOTIONAL_USDC = 1.0
DEFAULT_VENUE_TICK_SIZE = 0.01
DEFAULT_VENUE_MIN_ORDER_SHARES_FALLBACK = 0.0
DEFAULT_BOOK_FRESHNESS_MAX_AGE_MS = 5 * 60 * 1000


class StrategyRuntime:
    """Shared event-path shell for replay and live runners.

    Current responsibility in P1-1:
    - append normalized events to storage
    - feed them into a strategy implementation
    - persist first-touch state and order intents
    """

    def __init__(
        self,
        *,
        strategy: TouchStrategy,
        intent_store: RuntimeStore,
        venue_min_notional_usdc: float = DEFAULT_VENUE_MIN_NOTIONAL_USDC,
        venue_default_tick_size: float = DEFAULT_VENUE_TICK_SIZE,
        venue_min_order_shares_fallback: float = DEFAULT_VENUE_MIN_ORDER_SHARES_FALLBACK,
        book_freshness_max_age_ms: int = DEFAULT_BOOK_FRESHNESS_MAX_AGE_MS,
        clock: Clock | None = None,
    ):
        self.strategy = strategy
        self.store = intent_store
        self.clock = clock or getattr(strategy, "clock", None) or SystemClock()
        if getattr(self.strategy, "clock", None) is not self.clock:
            setattr(self.strategy, "clock", self.clock)
        self.logger = get_logger("short_horizon.runtime", run_id=intent_store.current_run_id)
        self.venue_min_notional_usdc = float(venue_min_notional_usdc)
        self.venue_default_tick_size = float(venue_default_tick_size)
        self.venue_min_order_shares_fallback = float(venue_min_order_shares_fallback)
        self.book_freshness_max_age_ms = int(book_freshness_max_age_ms)
        self.latest_best_bid_by_market_token: dict[tuple[str, str], float] = {}
        self.latest_book_event_ms_by_market_token: dict[tuple[str, str], int] = {}
        self.latest_book_ingest_ms_by_market_token: dict[tuple[str, str], int] = {}
        self.latest_spot_by_asset: dict[str, SpotPriceUpdate] = {}
        self.resolved_inventory_marks_by_market_token: dict[tuple[str, str], MarketResolvedWithInventory] = {}
        self._reported_resolved_inventory: set[tuple[str, str]] = set()

    def on_market_state(self, event: MarketStateUpdate) -> list[StrategyIntent]:
        log = self.logger.bind(**event_log_fields(event))
        self.store.append_event(event)
        self.store.upsert_market_state(event)
        outputs = self.strategy.on_market_state(event)
        resolved_inventory_events = self._emit_market_resolved_inventory_events(event)
        log.info(
            "market_state_ingested",
            condition_id=event.condition_id,
            question=event.question,
            status=event.status,
            asset_slug=event.asset_slug,
            resolved_inventory_events=len(resolved_inventory_events),
        )
        return outputs

    def on_book_update(self, event: BookUpdate) -> list[OrderIntent | SkipDecision]:
        log = self.logger.bind(**event_log_fields(event))
        self.store.append_event(event)
        key = (event.market_id, event.token_id)
        self.latest_book_event_ms_by_market_token[key] = int(event.event_time_ms)
        self.latest_book_ingest_ms_by_market_token[key] = int(event.ingest_time_ms)
        if event.best_bid is not None:
            self.latest_best_bid_by_market_token[key] = float(event.best_bid)
        log.debug(
            "book_update_ingested",
            best_bid=event.best_bid,
            best_ask=event.best_ask,
            spread=event.spread,
            mid_price=event.mid_price,
            book_age_ms=max(0, int(self.clock.now_ms()) - int(event.ingest_time_ms)),
            is_snapshot=event.is_snapshot,
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
                self.store.append_event_log(_skip_decision_event(decision))
                outputs.append(decision)
                continue
            decision = self.strategy.decide_on_touch(event=event, touch=touch)
            if isinstance(decision, OrderIntent):
                decision = self._apply_runtime_guards(decision)
            if isinstance(decision, OrderIntent):
                self.store.persist_intent(decision)
                self.store.append_event_log(_order_intent_event(decision), order_id=decision.intent_id)
                touch_log.info(
                    "order_intent_created",
                    order_id=decision.intent_id,
                    entry_price=decision.entry_price,
                    notional_usdc=decision.notional_usdc,
                    effective_notional_usdc=self._effective_notional_usdc(decision),
                    lifecycle_fraction=decision.lifecycle_fraction,
                    strategy_id=decision.strategy_id,
                )
            else:
                touch_log.info(
                    "touch_skipped",
                    reason=decision.reason,
                    details=decision.details,
                )
                self.store.append_event_log(_skip_decision_event(decision))
            outputs.append(decision)
        return outputs

    def on_spot_price_update(self, event: SpotPriceUpdate) -> None:
        self.latest_spot_by_asset[event.asset_slug] = event
        on_strategy_spot = getattr(self.strategy, "on_spot_price_update", None)
        if callable(on_strategy_spot):
            on_strategy_spot(event)
        self.logger.bind(**event_log_fields(event)).info(
            "spot_price_update_ingested",
            asset_slug=event.asset_slug,
            spot_price=event.spot_price,
            bid=event.bid,
            ask=event.ask,
            staleness_ms=event.staleness_ms,
            venue=event.venue,
        )

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
        effective_notional = self._effective_notional_usdc(decision)

        max_trade_notional_usdc = _resolve_max_trade_notional_usdc(
            risk=risk,
            config=config,
            venue_min_order_shares_fallback=self.venue_min_order_shares_fallback,
        )
        if max_trade_notional_usdc > 0 and effective_notional > max_trade_notional_usdc + 1e-9:
            return SkipDecision(
                reason="max_trade_notional_usdc_reached",
                market_id=decision.market_id,
                token_id=decision.token_id,
                level=decision.level,
                event_time_ms=decision.event_time_ms,
                details=(
                    f"intent_notional_usdc={float(decision.notional_usdc):.6f} "
                    f"effective_notional_usdc={effective_notional:.6f} "
                    f"cap_usdc={max_trade_notional_usdc:.6f}"
                ),
            )

        max_notional_per_strategy_usdc = float(getattr(risk, "max_notional_per_strategy_usdc", 0.0) or 0.0)
        if max_notional_per_strategy_usdc > 0:
            current_strategy_notional = _sum_open_notional_usdc(open_orders)
            projected_strategy_notional = current_strategy_notional + effective_notional
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

        max_orders_per_market_per_run = int(getattr(risk, "max_orders_per_market_per_run", 0) or 0)
        if max_orders_per_market_per_run > 0:
            market_orders_so_far = _count_order_attempts_for_market(all_orders, market_id=decision.market_id)
            projected_market_orders = market_orders_so_far + 1
            if projected_market_orders > max_orders_per_market_per_run:
                return SkipDecision(
                    reason="max_orders_per_market_per_run_reached",
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    level=decision.level,
                    event_time_ms=decision.event_time_ms,
                    details=(
                        f"market_orders_so_far={market_orders_so_far} "
                        f"projected_orders={projected_market_orders} "
                        f"cap={max_orders_per_market_per_run}"
                    ),
                )

        max_tokens_with_exposure_per_market = int(getattr(risk, "max_tokens_with_exposure_per_market", 0) or 0)
        if max_tokens_with_exposure_per_market > 0:
            market_tokens_with_exposure = _tokens_with_exposure_for_market(all_orders, market_id=decision.market_id)
            projected_market_tokens_with_exposure = set(market_tokens_with_exposure)
            projected_market_tokens_with_exposure.add(decision.token_id)
            if len(projected_market_tokens_with_exposure) > max_tokens_with_exposure_per_market:
                return SkipDecision(
                    reason="max_tokens_with_exposure_per_market_reached",
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    level=decision.level,
                    event_time_ms=decision.event_time_ms,
                    details=(
                        f"market_tokens_with_exposure={sorted(market_tokens_with_exposure)} "
                        f"projected_tokens_with_exposure={sorted(projected_market_tokens_with_exposure)} "
                        f"cap={max_tokens_with_exposure_per_market}"
                    ),
                )

        max_daily_loss_usdc = float(getattr(risk, "max_daily_loss_usdc", 0.0) or 0.0)
        if max_daily_loss_usdc > 0:
            daily_pnl_usdc = _compute_daily_pnl_usdc(
                orders=all_orders,
                fills=all_fills,
                event_time_ms=decision.event_time_ms,
                latest_best_bid_by_market_token=self.latest_best_bid_by_market_token,
                latest_book_ingest_ms_by_market_token=self.latest_book_ingest_ms_by_market_token,
                book_freshness_max_age_ms=self.book_freshness_max_age_ms,
                resolved_inventory_marks_by_market_token=self.resolved_inventory_marks_by_market_token,
            )
            realized_daily_loss_usdc = max(0.0, -daily_pnl_usdc)
            if realized_daily_loss_usdc > max_daily_loss_usdc + 1e-9:
                return SkipDecision(
                    reason="max_daily_loss_usdc_reached",
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    level=decision.level,
                    event_time_ms=decision.event_time_ms,
                    details=(
                        f"daily_pnl_usdc={daily_pnl_usdc:.6f} "
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

        concurrent_open_cap_usdc = float(getattr(risk, "micro_live_concurrent_open_notional_cap_usdc", 0.0) or 0.0)
        if concurrent_open_cap_usdc > 0:
            current_open_notional = _sum_open_notional_usdc(open_orders)
            projected_open_notional = current_open_notional + effective_notional
            if projected_open_notional > concurrent_open_cap_usdc + 1e-9:
                return SkipDecision(
                    reason="micro_live_concurrent_open_notional_cap_reached",
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    level=decision.level,
                    event_time_ms=decision.event_time_ms,
                    details=(
                        f"open_notional_usdc={current_open_notional:.6f} "
                        f"projected_usdc={projected_open_notional:.6f} cap_usdc={concurrent_open_cap_usdc:.6f}"
                    ),
                )

        cumulative_stake_cap_usdc = float(getattr(risk, "micro_live_cumulative_stake_cap_usdc", 0.0) or 0.0)
        if cumulative_stake_cap_usdc > 0:
            current_cumulative_stake_usdc = _sum_cumulative_stake_usdc(all_orders)
            projected_cumulative_stake_usdc = current_cumulative_stake_usdc + effective_notional
            if projected_cumulative_stake_usdc > cumulative_stake_cap_usdc + 1e-9:
                return SkipDecision(
                    reason="micro_live_cumulative_stake_cap_reached",
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    level=decision.level,
                    event_time_ms=decision.event_time_ms,
                    details=(
                        f"cumulative_stake_usdc={current_cumulative_stake_usdc:.6f} "
                        f"projected_usdc={projected_cumulative_stake_usdc:.6f} cap_usdc={cumulative_stake_cap_usdc:.6f}"
                    ),
                )
        return decision

    def _emit_market_resolved_inventory_events(self, event: MarketStateUpdate) -> list[MarketResolvedWithInventory]:
        if event.is_active and (event.end_time_ms is None or event.event_time_ms < event.end_time_ms):
            return []

        inventory_lots = _build_inventory_lots(
            orders=self.store.load_all_orders(),
            fills=self.store.load_fills(),
            event_time_ms=event.event_time_ms,
        )
        resolved_events: list[MarketResolvedWithInventory] = []
        for market_token, lots in inventory_lots.items():
            market_id, token_id = market_token
            if market_id != event.market_id or token_id is None or not lots:
                continue
            resolved_key = (str(market_id), str(token_id))
            if resolved_key in self._reported_resolved_inventory:
                continue
            outcome_price = _canonical_settlement_price(event=event, token_id=str(token_id))
            if outcome_price is None:
                # A market leaving the active universe or being marked closed by Gamma is
                # not enough to settle binary inventory.  For markets like equity
                # Up/Down, last BBO marks can be non-binary during the close/proposal
                # window; treating that mark as resolution fabricates PnL.  Emit a
                # resolved-inventory event only once canonical settlement is available
                # via explicit settlement prices or a resolved token id.
                self.logger.debug(
                    "market_resolution_pending_canonical_settlement",
                    market_id=str(market_id),
                    token_id=str(token_id),
                    status=event.status,
                    is_active=event.is_active,
                )
                continue
            event_source = "runtime.market_resolution_settlement"
            total_size = sum(float(size) for size, _unit_cost in lots)
            if total_size <= 1e-12:
                continue
            total_cost_basis = sum(float(size) * float(unit_cost) for size, unit_cost in lots)
            average_entry_price = total_cost_basis / total_size
            estimated_pnl_usdc = sum(float(size) * (float(outcome_price) - float(unit_cost)) for size, unit_cost in lots)
            resolved_event = MarketResolvedWithInventory(
                event_time_ms=event.event_time_ms,
                ingest_time_ms=event.ingest_time_ms,
                market_id=str(market_id),
                token_id=str(token_id),
                side=OrderSide.BUY,
                size=total_size,
                outcome_price=float(outcome_price),
                average_entry_price=average_entry_price,
                estimated_pnl_usdc=estimated_pnl_usdc,
                source=event_source,
                run_id=self.store.current_run_id,
            )
            self.store.append_event_log(resolved_event)
            self.resolved_inventory_marks_by_market_token[resolved_key] = resolved_event
            self._reported_resolved_inventory.add(resolved_key)
            self.logger.info(
                "live_market_resolved_holding",
                market_id=resolved_event.market_id,
                token_id=resolved_event.token_id,
                side=resolved_event.side,
                size=resolved_event.size,
                outcome_price=resolved_event.outcome_price,
                average_entry_price=resolved_event.average_entry_price,
                estimated_pnl_usdc=resolved_event.estimated_pnl_usdc,
            )
            resolved_events.append(resolved_event)
        return resolved_events

    def _effective_notional_usdc(self, decision: OrderIntent) -> float:
        """Expected submitted notional after translator upscales for venue minimums.

        Falls back to the raw intent notional when market metadata is missing so
        absence of data never silently inflates cap projections.
        """
        tick_size = self.venue_default_tick_size
        min_order_shares: float | None = self.venue_min_order_shares_fallback
        load_latest_market_state = getattr(self.store, "load_latest_market_state", None)
        if callable(load_latest_market_state):
            market_state = load_latest_market_state(decision.market_id)
            if market_state is not None:
                if market_state.tick_size is not None:
                    tick_size = float(market_state.tick_size)
                if market_state.min_order_size is not None:
                    min_order_shares = float(market_state.min_order_size)
        if tick_size <= 0:
            return float(decision.notional_usdc)
        from ..execution.order_translator import VenueConstraints, estimate_effective_buy_notional

        constraints = VenueConstraints(
            tick_size=tick_size,
            min_order_size=self.venue_min_notional_usdc,
            min_order_shares=min_order_shares,
        )
        return estimate_effective_buy_notional(
            notional_usdc=decision.notional_usdc,
            entry_price=decision.entry_price,
            venue_constraints=constraints,
        )

    def book_age_ms(self, market_id: str, token_id: str, *, now_ms: int | None = None) -> int | None:
        last_ingest_ms = self.latest_book_ingest_ms_by_market_token.get((str(market_id), str(token_id)))
        if last_ingest_ms is None:
            return None
        effective_now_ms = int(self.clock.now_ms() if now_ms is None else now_ms)
        return max(0, effective_now_ms - int(last_ingest_ms))

    def is_book_fresh(self, market_id: str, token_id: str, *, now_ms: int | None = None) -> bool:
        age_ms = self.book_age_ms(market_id, token_id, now_ms=now_ms)
        return age_ms is not None and age_ms <= self.book_freshness_max_age_ms


_NON_TERMINAL_ORDER_STATES = {
    OrderState.INTENT.value,
    OrderState.PENDING_SEND.value,
    OrderState.ACCEPTED.value,
    OrderState.PARTIALLY_FILLED.value,
    OrderState.CANCEL_REQUESTED.value,
    OrderState.REPLACE_REQUESTED.value,
    OrderState.UNKNOWN.value,
}


def _resolve_max_trade_notional_usdc(
    *,
    risk: object,
    config: object | None,
    venue_min_order_shares_fallback: float = 0.0,
) -> float:
    configured_cap = getattr(risk, "max_trade_notional_usdc", None)
    if configured_cap is not None:
        return float(configured_cap or 0.0)
    execution = getattr(config, "execution", None)
    target_trade_size_usdc = float(getattr(execution, "target_trade_size_usdc", 0.0) or 0.0)
    if target_trade_size_usdc <= 0:
        return 0.0
    auto_cap = target_trade_size_usdc * 1.5
    venue_floor = max(float(venue_min_order_shares_fallback or 0.0), 0.0)
    return max(auto_cap, venue_floor)


def _sum_open_notional_usdc(open_orders: list[dict]) -> float:
    return _sum_order_notional_usdc(open_orders, size_field="remaining_size")


def _sum_cumulative_stake_usdc(all_orders: list[dict]) -> float:
    attempted_orders = [row for row in all_orders if str(row.get("state") or "") != OrderState.REJECTED.value]
    return _sum_order_notional_usdc(attempted_orders, size_field="size")


def _sum_order_notional_usdc(rows: list[dict], *, size_field: str) -> float:
    total_notional = 0.0
    for row in rows:
        price = row.get("price")
        size = row.get(size_field)
        if size is None and size_field != "size":
            size = row.get("size")
        if price is None or size is None:
            continue
        total_notional += float(price) * float(size)
    return total_notional


def _count_order_attempts_for_market(all_orders: list[dict], *, market_id: str) -> int:
    count = 0
    for row in all_orders:
        if str(row.get("market_id") or "") != market_id:
            continue
        count += 1
    return count


def _tokens_with_exposure_for_market(all_orders: list[dict], *, market_id: str) -> set[str]:
    tokens: set[str] = set()
    for row in all_orders:
        if str(row.get("market_id") or "") != market_id:
            continue
        if not _row_has_inventory_exposure(row):
            continue
        token_id = row.get("token_id")
        if token_id is None:
            continue
        tokens.add(str(token_id))
    return tokens


def _row_has_inventory_exposure(row: dict) -> bool:
    state = str(row.get("state") or "")
    if state == OrderState.FILLED.value:
        return True
    cumulative_filled_size = row.get("cumulative_filled_size")
    if cumulative_filled_size is None:
        return False
    try:
        return float(cumulative_filled_size) > 1e-12
    except (TypeError, ValueError):
        return False


def _order_intent_event(intent: OrderIntent) -> OrderIntentEvent:
    return OrderIntentEvent(
        event_time_ms=intent.event_time_ms,
        ingest_time_ms=intent.event_time_ms,
        order_id=intent.intent_id,
        strategy_id=str(intent.strategy_id),
        market_id=intent.market_id,
        token_id=intent.token_id,
        level=float(intent.level),
        entry_price=float(intent.entry_price),
        notional_usdc=float(intent.notional_usdc),
        lifecycle_fraction=float(intent.lifecycle_fraction),
        reason=intent.reason,
        side=OrderSide(str(intent.side)),
        size_shares=float(intent.size_shares) if intent.size_shares is not None else None,
        time_in_force=intent.time_in_force,
        post_only=bool(intent.post_only),
    )


def _canonical_settlement_price(*, event: MarketStateUpdate, token_id: str) -> float | None:
    if event.settlement_prices is not None and token_id in event.settlement_prices:
        return float(event.settlement_prices[token_id])
    if event.resolved_token_id is not None:
        return 1.0 if token_id == str(event.resolved_token_id) else 0.0
    return None


def _skip_decision_event(decision: SkipDecision) -> SkipDecisionEvent:
    return SkipDecisionEvent(
        event_time_ms=decision.event_time_ms,
        ingest_time_ms=decision.event_time_ms,
        reason=decision.reason,
        market_id=decision.market_id,
        token_id=decision.token_id,
        level=float(decision.level),
        details=decision.details,
    )


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


def _compute_daily_pnl_usdc(
    *,
    orders: list[dict],
    fills: list[dict],
    event_time_ms: int,
    latest_best_bid_by_market_token: dict[tuple[str, str], float],
    resolved_inventory_marks_by_market_token: dict[tuple[str, str], MarketResolvedWithInventory],
    latest_book_ingest_ms_by_market_token: dict[tuple[str, str], int] | None = None,
    book_freshness_max_age_ms: int = DEFAULT_BOOK_FRESHNESS_MAX_AGE_MS,
) -> float:
    inventory_lots = _build_intraday_inventory_lots(orders=orders, fills=fills, event_time_ms=event_time_ms)
    realized_pnl_usdc = _compute_intraday_realized_sell_pnl_usdc(orders=orders, fills=fills, event_time_ms=event_time_ms)
    mark_to_market_pnl_usdc = 0.0
    for market_token, lots in inventory_lots.items():
        if not lots:
            continue
        resolved_mark = resolved_inventory_marks_by_market_token.get((str(market_token[0]), str(market_token[1])))
        if resolved_mark is not None:
            mark_price = float(resolved_mark.outcome_price)
        else:
            token_id = market_token[1]
            if token_id is None:
                continue
            mark_price = latest_best_bid_by_market_token.get((str(market_token[0]), str(token_id)))
            if mark_price is None:
                continue
            if latest_book_ingest_ms_by_market_token is not None and book_freshness_max_age_ms > 0:
                book_ingest_ms = latest_book_ingest_ms_by_market_token.get((str(market_token[0]), str(token_id)))
                if book_ingest_ms is None:
                    continue
                if int(event_time_ms) - int(book_ingest_ms) > int(book_freshness_max_age_ms):
                    continue
        mark_to_market_pnl_usdc += sum(float(size) * (float(mark_price) - float(unit_cost)) for size, unit_cost in lots)
    return realized_pnl_usdc + mark_to_market_pnl_usdc


def _compute_intraday_realized_sell_pnl_usdc(*, orders: list[dict], fills: list[dict], event_time_ms: int) -> float:
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


def _build_intraday_inventory_lots(*, orders: list[dict], fills: list[dict], event_time_ms: int) -> dict[tuple[str | None, str | None], list[list[float]]]:
    target_day = _iso_day_from_ms(event_time_ms)
    return _build_inventory_lots(orders=orders, fills=fills, event_time_ms=event_time_ms, target_day=target_day)


def _build_inventory_lots(
    *,
    orders: list[dict],
    fills: list[dict],
    event_time_ms: int,
    target_day: str | None = None,
) -> dict[tuple[str | None, str | None], list[list[float]]]:
    order_by_id = {str(row.get("order_id")): row for row in orders if row.get("order_id") is not None}
    inventory_lots: dict[tuple[str | None, str | None], list[list[float]]] = defaultdict(list)
    for fill in sorted(fills, key=lambda row: (str(row.get("filled_at") or ""), str(row.get("fill_id") or ""))):
        filled_at = str(fill.get("filled_at") or "")
        if target_day is not None and not filled_at.startswith(target_day):
            continue
        order = order_by_id.get(str(fill.get("order_id") or ""), {})
        side = str(order.get("side") or "").upper()
        price = float(fill.get("price") or 0.0)
        size = float(fill.get("size") or 0.0)
        fee_paid_usdc = float(fill.get("fee_paid_usdc") or 0.0)
        market_token = (fill.get("market_id"), fill.get("token_id"))
        if size <= 0:
            continue
        if side == "BUY":
            inventory_lots[market_token].append([size, (price * size + fee_paid_usdc) / size])
            continue
        if side == "SELL":
            _consume_cost_basis(inventory_lots[market_token], size=size, fallback_price=price)
    return inventory_lots


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
