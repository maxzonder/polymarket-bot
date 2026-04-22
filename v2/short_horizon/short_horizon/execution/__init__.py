from __future__ import annotations

from dataclasses import replace
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
from datetime import datetime
from enum import StrEnum
from typing import Any, Callable, Protocol

from ..core.events import OrderAccepted, OrderCanceled, OrderFilled, OrderRejected, OrderSide, TimerEvent
from ..core.models import OrderIntent
from ..core.order_state import OrderState
from ..strategy_api import CancelOrder, Noop, PlaceOrder, StrategyIntent
from ..telemetry import get_logger
from ..venue_polymarket.execution_client import VenueOrderRequest, VenueOrderState, VenuePlaceResult
from ..venue_polymarket.markets import MarketMetadata
from .order_translator import TranslationPolicy, VenueConstraints, VenueTranslationError, translate_place_order


TERMINAL_ORDER_STATES = {
    OrderState.FILLED,
    OrderState.REJECTED,
    OrderState.CANCEL_CONFIRMED,
    OrderState.EXPIRED,
    OrderState.REPLACED,
}

ALLOWED_TRANSITIONS: dict[OrderState, set[OrderState]] = {
    OrderState.INTENT: {OrderState.PENDING_SEND},
    OrderState.PENDING_SEND: {OrderState.ACCEPTED, OrderState.REJECTED, OrderState.UNKNOWN},
    OrderState.ACCEPTED: {
        OrderState.PARTIALLY_FILLED,
        OrderState.FILLED,
        OrderState.CANCEL_REQUESTED,
        OrderState.CANCEL_CONFIRMED,
        OrderState.REPLACE_REQUESTED,
        OrderState.EXPIRED,
        OrderState.UNKNOWN,
    },
    OrderState.PARTIALLY_FILLED: {
        OrderState.PARTIALLY_FILLED,
        OrderState.FILLED,
        OrderState.CANCEL_REQUESTED,
        OrderState.REPLACE_REQUESTED,
        OrderState.UNKNOWN,
    },
    OrderState.CANCEL_REQUESTED: {
        OrderState.ACCEPTED,
        OrderState.CANCEL_CONFIRMED,
        OrderState.PARTIALLY_FILLED,
        OrderState.FILLED,
        OrderState.UNKNOWN,
    },
    OrderState.REPLACE_REQUESTED: {
        OrderState.REPLACED,
        OrderState.ACCEPTED,
        OrderState.PARTIALLY_FILLED,
        OrderState.UNKNOWN,
    },
    OrderState.UNKNOWN: {
        OrderState.INTENT,
        OrderState.PENDING_SEND,
        OrderState.ACCEPTED,
        OrderState.PARTIALLY_FILLED,
        OrderState.FILLED,
        OrderState.CANCEL_REQUESTED,
        OrderState.CANCEL_CONFIRMED,
        OrderState.REJECTED,
        OrderState.EXPIRED,
        OrderState.REPLACE_REQUESTED,
        OrderState.REPLACED,
    },
    OrderState.REJECTED: set(),
    OrderState.FILLED: set(),
    OrderState.CANCEL_CONFIRMED: set(),
    OrderState.EXPIRED: set(),
    OrderState.REPLACED: set(),
}


class ExecutionMode(StrEnum):
    SYNTHETIC = "synthetic"
    DRY_RUN = "dry_run"
    LIVE = "live"


class ExecutionTransitionError(RuntimeError):
    pass


class ExecutionValidationError(RuntimeError):
    pass


class LiveSubmitGuardRejected(ExecutionValidationError):
    def __init__(self, reason: str, *, reason_code: str = "LIVE_SUBMIT_BLOCKED") -> None:
        super().__init__(reason)
        self.reason = str(reason)
        self.reason_code = str(reason_code)


LiveSubmitGuard = Callable[[OrderIntent, VenueOrderRequest, dict[str, Any]], None]


@dataclass(frozen=True)
class SyntheticFillRequest:
    order_id: str
    event_time_ms: int
    fill_size: float | None = None
    fill_price: float | None = None
    fee_paid_usdc: float | None = None
    source: str = "replay"


class ExecutionVenueClient(Protocol):
    def place_order(self, order_request: VenueOrderRequest) -> VenuePlaceResult:
        ...

    def cancel_order(self, order_id: str):
        ...

    def get_order(self, order_id: str):
        ...

    def list_open_orders(self, market_id: str | None = None):
        ...


class ExecutionStore(Protocol):
    @property
    def current_run_id(self) -> str:
        ...

    def append_event_log(self, event, *, order_id: str | None = None) -> None:
        ...

    def insert_order(
        self,
        *,
        order_id: str,
        market_id: str,
        token_id: str,
        side: str,
        price: float | None,
        size: float | None,
        state: OrderState | str,
        client_order_id: str | None,
        venue_order_id: str | None = None,
        intent_created_at_ms: int,
        last_state_change_at_ms: int,
        remaining_size: float | None = None,
        parent_order_id: str | None = None,
        venue_order_status: str | None = None,
        reconciliation_required: bool = False,
    ) -> None:
        ...

    def update_order_state(
        self,
        *,
        order_id: str,
        state: OrderState | str,
        event_time_ms: int,
        venue_order_id: str | None = None,
        venue_order_status: str | None = None,
        cumulative_filled_size: float | None = None,
        remaining_size: float | None = None,
        reconciliation_required: bool | None = None,
        reject_code: str | None = None,
        reject_reason: str | None = None,
    ) -> None:
        ...

    def insert_fill(
        self,
        *,
        fill_id: str,
        order_id: str,
        market_id: str,
        token_id: str,
        price: float,
        size: float,
        filled_at_ms: int,
        source: str,
        fee_paid_usdc: float | None = None,
        liquidity_role: str | None = None,
        venue_fill_id: str | None = None,
    ) -> None:
        ...

    def load_order(self, order_id: str) -> dict | None:
        ...

    def load_order_by_client_order_id(self, client_order_id: str) -> dict | None:
        ...

    def load_order_by_venue_order_id(self, venue_order_id: str) -> dict | None:
        ...

    def load_non_terminal_orders(self) -> list[dict]:
        ...

    def load_latest_market_state(self, market_id: str):
        ...


class ExecutionEngine:
    """Execution boundary for synthetic, dry-run, and live order placement."""

    def __init__(
        self,
        *,
        store: ExecutionStore,
        client: ExecutionVenueClient | None = None,
        mode: ExecutionMode | str = ExecutionMode.SYNTHETIC,
        tick_size: float = 0.01,
        min_order_size: float = 1.0,
        translation_policy: TranslationPolicy | None = None,
        live_submit_guard: LiveSubmitGuard | None = None,
    ):
        self.store = store
        self.client = client
        self.mode = ExecutionMode(str(mode))
        self.tick_size = float(tick_size)
        self.min_order_size = float(min_order_size)
        self.translation_policy = translation_policy or TranslationPolicy()
        self.live_submit_guard = live_submit_guard
        self.logger = get_logger("short_horizon.execution", run_id=store.current_run_id)

    def handle_intent(self, intent: StrategyIntent, *, event_time_ms: int | None = None):
        if isinstance(intent, PlaceOrder):
            return self.submit(intent.intent, event_time_ms=event_time_ms)
        if isinstance(intent, CancelOrder):
            event = self.cancel(market_id=intent.market_id, token_id=intent.token_id, event_time_ms=event_time_ms, reason=intent.reason)
            return [event] if event is not None else []
        if isinstance(intent, Noop):
            return []
        raise TypeError(f"Unsupported strategy intent: {type(intent)!r}")

    def submit(self, intent: OrderIntent, *, event_time_ms: int | None = None) -> list[OrderAccepted | OrderRejected]:
        send_time_ms = int(event_time_ms if event_time_ms is not None else intent.event_time_ms)
        order_row = self._ensure_order_row(intent)

        if self.mode is ExecutionMode.SYNTHETIC:
            self._validate_order_row(order_row)
            return [self._emit_synthetic_accept(intent=intent, order_row=order_row, send_time_ms=send_time_ms)]

        try:
            order_request = self._translate_order_request(intent)
        except VenueTranslationError as exc:
            return [self._emit_rejected(intent=intent, order_row=order_row, send_time_ms=send_time_ms, reason=str(exc), reason_code="TRANSLATION_ERROR")]

        order_row = self._bind_translated_request(intent=intent, order_request=order_request, event_time_ms=send_time_ms)
        if self.mode is ExecutionMode.DRY_RUN:
            self._log_dry_run_order(intent=intent, order_request=order_request, event_time_ms=send_time_ms)
            return [self._emit_synthetic_accept(intent=intent, order_row=order_row, send_time_ms=send_time_ms, source="execution.dry_run.synthetic_accept")]

        if self.client is None:
            raise ExecutionValidationError("Execution client is required in live mode")

        try:
            if self.live_submit_guard is not None:
                self.live_submit_guard(intent, order_request, order_row)
        except LiveSubmitGuardRejected as exc:
            return [
                self._emit_rejected(
                    intent=intent,
                    order_row=order_row,
                    send_time_ms=send_time_ms,
                    reason=exc.reason,
                    reason_code=exc.reason_code,
                )
            ]

        self._transition(order_row, OrderState.PENDING_SEND, event_time_ms=send_time_ms, venue_order_status="sending")
        try:
            place_result = self.client.place_order(order_request)
        except Exception as exc:
            return [self._emit_rejected(intent=intent, order_row=order_row, send_time_ms=send_time_ms + 1, reason=str(exc), reason_code="VENUE_ERROR")]
        if self.live_submit_guard is not None:
            record_submit_success = getattr(self.live_submit_guard, "record_submit_success", None)
            if callable(record_submit_success):
                record_submit_success(intent, order_request, order_row, place_result)
        return [self._emit_live_accept(intent=intent, order_row=order_row, place_result=place_result, accepted_time_ms=send_time_ms + 1)]

    def _translate_order_request(self, intent: OrderIntent) -> VenueOrderRequest:
        market_meta = self._market_meta_for_intent(intent)
        return translate_place_order(
            intent,
            market_meta,
            VenueConstraints(
                tick_size=market_meta.tick_size or self.tick_size,
                min_order_size=self.min_order_size,
                min_order_shares=market_meta.min_order_size,
            ),
            client_order_id_seed=self.store.current_run_id,
            policy=self.translation_policy,
        )

    def _bind_translated_request(self, *, intent: OrderIntent, order_request: VenueOrderRequest, event_time_ms: int) -> dict:
        order_row = self._require_order(intent.intent_id)
        self.store.insert_order(
            order_id=order_row["order_id"],
            market_id=order_row["market_id"],
            token_id=order_row["token_id"],
            side=order_row["side"],
            price=order_request.price,
            size=order_request.size,
            state=order_row["state"],
            client_order_id=order_request.client_order_id,
            venue_order_id=order_row.get("venue_order_id"),
            intent_created_at_ms=self._event_time_ms_from_iso(str(order_row["intent_created_at"])),
            last_state_change_at_ms=event_time_ms,
            remaining_size=order_request.size,
            parent_order_id=order_row.get("parent_order_id"),
            venue_order_status=order_row.get("venue_order_status") or "intent",
            reconciliation_required=bool(order_row.get("reconciliation_required")),
        )
        return self._require_order(intent.intent_id)

    def _emit_synthetic_accept(
        self,
        *,
        intent: OrderIntent,
        order_row: dict,
        send_time_ms: int,
        source: str = "execution.synthetic_accept",
    ) -> OrderAccepted:
        accepted_time_ms = send_time_ms + 1
        self._transition(order_row, OrderState.PENDING_SEND, event_time_ms=send_time_ms, venue_order_status="sending")
        order_row = self._require_order(intent.intent_id)
        accepted = OrderAccepted(
            event_time_ms=accepted_time_ms,
            ingest_time_ms=accepted_time_ms,
            order_id=intent.intent_id,
            market_id=intent.market_id,
            token_id=intent.token_id,
            side=OrderSide.BUY,
            price=float(order_row["price"]),
            size=float(order_row["size"]),
            source=source,
            client_order_id=order_row.get("client_order_id") or intent.intent_id,
            venue_status="accepted",
            run_id=self.store.current_run_id,
        )
        self._transition(order_row, OrderState.ACCEPTED, event_time_ms=accepted_time_ms, venue_order_status="accepted")
        self.store.append_event_log(accepted, order_id=intent.intent_id)
        self.logger.info(
            "synthetic_order_accepted",
            order_id=intent.intent_id,
            market_id=intent.market_id,
            token_id=intent.token_id,
            event_time_ms=accepted_time_ms,
            execution_mode=self.mode.value,
        )
        return accepted

    def _emit_live_accept(self, *, intent: OrderIntent, order_row: dict, place_result: VenuePlaceResult, accepted_time_ms: int) -> OrderAccepted:
        self._transition(
            order_row,
            OrderState.ACCEPTED,
            event_time_ms=accepted_time_ms,
            venue_order_id=place_result.order_id or None,
            venue_order_status=place_result.status,
        )
        refreshed = self._require_order(intent.intent_id)
        accepted = OrderAccepted(
            event_time_ms=accepted_time_ms,
            ingest_time_ms=accepted_time_ms,
            order_id=refreshed["order_id"],
            market_id=intent.market_id,
            token_id=intent.token_id,
            side=OrderSide.BUY,
            price=float(refreshed["price"]),
            size=float(refreshed["size"]),
            source="execution.live_accept",
            client_order_id=refreshed.get("client_order_id"),
            venue_status=place_result.status,
            run_id=self.store.current_run_id,
        )
        self.store.append_event_log(accepted, order_id=refreshed["order_id"])
        self.logger.info(
            "live_order_accepted",
            order_id=refreshed["order_id"],
            market_id=intent.market_id,
            token_id=intent.token_id,
            client_order_id=refreshed.get("client_order_id"),
            venue_order_id=place_result.order_id,
            event_time_ms=accepted_time_ms,
        )
        return accepted

    def _emit_rejected(
        self,
        *,
        intent: OrderIntent,
        order_row: dict,
        send_time_ms: int,
        reason: str,
        reason_code: str,
    ) -> OrderRejected:
        current_state = self._state_from_row(order_row)
        if current_state is OrderState.INTENT:
            self._transition(
                order_row,
                OrderState.PENDING_SEND,
                event_time_ms=max(send_time_ms - 1, intent.event_time_ms),
                venue_order_status="sending",
            )
            order_row = self._require_order(intent.intent_id)
        self._transition(
            order_row,
            OrderState.REJECTED,
            event_time_ms=send_time_ms,
            venue_order_status="rejected",
            reject_code=reason_code,
            reject_reason=reason,
        )
        refreshed = self._require_order(intent.intent_id)
        rejected = OrderRejected(
            event_time_ms=send_time_ms,
            ingest_time_ms=send_time_ms,
            market_id=intent.market_id,
            token_id=intent.token_id,
            side=OrderSide.BUY,
            source="execution.live_reject" if self.mode is ExecutionMode.LIVE else "execution.dry_run_reject",
            client_order_id=refreshed.get("client_order_id"),
            price=float(refreshed["price"]) if refreshed.get("price") is not None else None,
            size=float(refreshed["size"]) if refreshed.get("size") is not None else None,
            reject_reason_code=reason_code,
            reject_reason_text=reason,
            is_retryable=False,
            run_id=self.store.current_run_id,
        )
        self.store.append_event_log(rejected, order_id=refreshed["order_id"])
        self.logger.warning(
            "order_rejected",
            order_id=refreshed["order_id"],
            market_id=intent.market_id,
            token_id=intent.token_id,
            client_order_id=refreshed.get("client_order_id"),
            reason_code=reason_code,
            reason=reason,
            execution_mode=self.mode.value,
        )
        return rejected

    def _log_dry_run_order(self, *, intent: OrderIntent, order_request: VenueOrderRequest, event_time_ms: int) -> None:
        event = TimerEvent(
            event_time_ms=event_time_ms,
            ingest_time_ms=event_time_ms,
            timer_kind="dry_run_order_logged",
            source="execution.dry_run",
            market_id=intent.market_id,
            token_id=intent.token_id,
            payload={
                "order_id": intent.intent_id,
                "client_order_id": order_request.client_order_id,
                "token_id": order_request.token_id,
                "side": order_request.side,
                "price": order_request.price,
                "size": order_request.size,
                "time_in_force": order_request.time_in_force,
                "post_only": order_request.post_only,
            },
            run_id=self.store.current_run_id,
        )
        self.store.append_event_log(event, order_id=intent.intent_id)
        self.logger.info(
            "dry_run_order_logged",
            order_id=intent.intent_id,
            market_id=intent.market_id,
            token_id=intent.token_id,
            client_order_id=order_request.client_order_id,
            price=order_request.price,
            size=order_request.size,
        )

    def apply_fill(self, request: SyntheticFillRequest) -> OrderFilled:
        order_row = self._require_order(request.order_id)
        current_state = self._state_from_row(order_row)
        if current_state not in {OrderState.ACCEPTED, OrderState.PARTIALLY_FILLED}:
            raise ExecutionTransitionError(
                f"Cannot fill order {request.order_id} from state {current_state.value}"
            )

        order_size = float(order_row["size"] or 0.0)
        cumulative_before = float(order_row.get("cumulative_filled_size") or 0.0)
        remaining_before = float(order_row.get("remaining_size") or order_size)
        fill_size = float(request.fill_size if request.fill_size is not None else remaining_before)
        if fill_size <= 0 or fill_size > remaining_before + 1e-12:
            raise ExecutionTransitionError(
                f"Invalid synthetic fill size {fill_size} for remaining {remaining_before} on {request.order_id}"
            )

        cumulative_after = cumulative_before + fill_size
        remaining_after = max(order_size - cumulative_after, 0.0)
        next_state = OrderState.FILLED if remaining_after <= 1e-12 else OrderState.PARTIALLY_FILLED
        fill_price = float(request.fill_price if request.fill_price is not None else order_row["price"])
        fill_id = f"{request.order_id}:fill:{request.event_time_ms}:{int(round(fill_size * 1_000_000))}"
        self.store.insert_fill(
            fill_id=fill_id,
            order_id=request.order_id,
            market_id=order_row["market_id"],
            token_id=order_row["token_id"],
            price=fill_price,
            size=fill_size,
            filled_at_ms=request.event_time_ms,
            source=request.source,
            fee_paid_usdc=request.fee_paid_usdc,
            liquidity_role="taker",
        )
        self._transition(
            order_row,
            next_state,
            event_time_ms=request.event_time_ms,
            venue_order_status="filled" if next_state is OrderState.FILLED else "partially_filled",
            cumulative_filled_size=cumulative_after,
            remaining_size=remaining_after,
        )
        event = OrderFilled(
            event_time_ms=request.event_time_ms,
            ingest_time_ms=request.event_time_ms,
            order_id=request.order_id,
            market_id=order_row["market_id"],
            token_id=order_row["token_id"],
            side=OrderSide(order_row["side"]),
            fill_price=fill_price,
            fill_size=fill_size,
            cumulative_filled_size=cumulative_after,
            remaining_size=remaining_after,
            source=request.source,
            client_order_id=order_row.get("client_order_id"),
            fee_paid_usdc=request.fee_paid_usdc,
            liquidity_role="taker",
            run_id=self.store.current_run_id,
        )
        self.store.append_event_log(event, order_id=request.order_id)
        self.logger.info(
            "synthetic_order_filled",
            order_id=request.order_id,
            market_id=order_row["market_id"],
            token_id=order_row["token_id"],
            fill_size=fill_size,
            remaining_size=remaining_after,
            event_time_ms=request.event_time_ms,
        )
        return event

    def cancel(self, *, market_id: str, token_id: str, event_time_ms: int | None = None, reason: str = "") -> OrderCanceled | None:
        order_row = self._find_open_order(market_id=market_id, token_id=token_id)
        if order_row is None:
            return None
        cancel_time_ms = int(event_time_ms if event_time_ms is not None else self._event_time_ms_from_order(order_row))
        if self.mode is ExecutionMode.LIVE:
            return self._cancel_live(order_row=order_row, cancel_time_ms=cancel_time_ms, reason=reason)
        self._transition(order_row, OrderState.CANCEL_REQUESTED, event_time_ms=cancel_time_ms, venue_order_status="cancel_requested")
        confirm_time_ms = cancel_time_ms + 1
        self._transition(order_row, OrderState.CANCEL_CONFIRMED, event_time_ms=confirm_time_ms, venue_order_status="canceled")
        event = OrderCanceled(
            event_time_ms=confirm_time_ms,
            ingest_time_ms=confirm_time_ms,
            order_id=order_row["order_id"],
            market_id=market_id,
            token_id=token_id,
            source="execution.synthetic_cancel",
            client_order_id=order_row.get("client_order_id"),
            cancel_reason=reason,
            cumulative_filled_size=float(order_row.get("cumulative_filled_size") or 0.0),
            remaining_size=float(order_row.get("remaining_size") or 0.0),
            run_id=self.store.current_run_id,
        )
        self.store.append_event_log(event, order_id=order_row["order_id"])
        self.logger.info(
            "synthetic_order_canceled",
            order_id=order_row["order_id"],
            market_id=market_id,
            token_id=token_id,
            event_time_ms=confirm_time_ms,
            reason=reason,
        )
        return event

    def reconcile_order_event(self, event: OrderAccepted | OrderRejected | OrderFilled | OrderCanceled):
        order_row = self._resolve_order_for_event(event)
        if order_row is None:
            return None
        if isinstance(event, OrderAccepted):
            return self._reconcile_order_accepted(event, order_row=order_row)
        if isinstance(event, OrderRejected):
            return self._reconcile_order_rejected(event, order_row=order_row)
        if isinstance(event, OrderFilled):
            return self._reconcile_order_filled(event, order_row=order_row)
        if isinstance(event, OrderCanceled):
            return self._reconcile_order_canceled(event, order_row=order_row)
        raise TypeError(f"Unsupported order event for reconciliation: {type(event)!r}")

    def reconcile_persisted_orders(self, *, event_time_ms: int | None = None) -> int:
        if self.mode is not ExecutionMode.LIVE:
            return 0
        if self.client is None:
            raise ExecutionValidationError("Execution client is required in live mode")
        reconciled = 0
        open_orders_by_market: dict[str, list[VenueOrderState]] = {}
        for order_row in self.store.load_non_terminal_orders():
            venue_state = self._lookup_venue_order_state(order_row, open_orders_by_market=open_orders_by_market)
            effective_event_time_ms = int(
                event_time_ms if event_time_ms is not None else self._event_time_ms_from_order(order_row)
            )
            if venue_state is None:
                self._reconcile_not_found_order(order_row, event_time_ms=effective_event_time_ms)
                reconciled += 1
                continue
            self._reconcile_from_venue_state(order_row, venue_state=venue_state, event_time_ms=effective_event_time_ms)
            reconciled += 1
        return reconciled

    def _ensure_order_row(self, intent: OrderIntent) -> dict:
        order_row = self.store.load_order(intent.intent_id)
        if order_row is not None:
            return order_row
        size = intent.notional_usdc / intent.entry_price if intent.entry_price > 0 else None
        self.store.insert_order(
            order_id=intent.intent_id,
            market_id=intent.market_id,
            token_id=intent.token_id,
            side=OrderSide.BUY.value,
            price=intent.entry_price,
            size=size,
            state=OrderState.INTENT,
            client_order_id=intent.intent_id,
            intent_created_at_ms=intent.event_time_ms,
            last_state_change_at_ms=intent.event_time_ms,
            remaining_size=size,
            venue_order_status="intent",
        )
        return self._require_order(intent.intent_id)

    def _find_open_order(self, *, market_id: str, token_id: str) -> dict | None:
        for row in reversed(self.store.load_non_terminal_orders()):
            if row["market_id"] == market_id and row["token_id"] == token_id:
                return row
        return None

    def _cancel_live(self, *, order_row: dict, cancel_time_ms: int, reason: str) -> OrderCanceled | None:
        if self.client is None:
            raise ExecutionValidationError("Execution client is required in live mode")
        venue_order_id = str(order_row.get("venue_order_id") or "").strip()
        if not venue_order_id:
            raise ExecutionValidationError(
                f"Live cancel requires venue_order_id for order {order_row['order_id']}"
            )
        self._transition(
            order_row,
            OrderState.CANCEL_REQUESTED,
            event_time_ms=cancel_time_ms,
            venue_order_status="cancel_requested",
            reconciliation_required=True,
        )
        try:
            cancel_result = self.client.cancel_order(venue_order_id)
        except Exception as exc:
            self.logger.warning(
                "live_cancel_submit_failed",
                order_id=order_row["order_id"],
                venue_order_id=venue_order_id,
                reason=reason,
                error=str(exc),
            )
            return None
        if _is_canceled_status(cancel_result.status):
            confirm_time_ms = cancel_time_ms + 1
            self._transition(
                order_row,
                OrderState.CANCEL_CONFIRMED,
                event_time_ms=confirm_time_ms,
                venue_order_status=cancel_result.status,
                remaining_size=0.0,
                reconciliation_required=False,
            )
            refreshed = self._require_order(order_row["order_id"])
            event = OrderCanceled(
                event_time_ms=confirm_time_ms,
                ingest_time_ms=confirm_time_ms,
                order_id=refreshed["order_id"],
                market_id=refreshed["market_id"],
                token_id=refreshed["token_id"],
                source="execution.live_cancel_ack",
                client_order_id=refreshed.get("client_order_id"),
                cancel_reason=reason,
                cumulative_filled_size=float(refreshed.get("cumulative_filled_size") or 0.0),
                remaining_size=0.0,
                run_id=self.store.current_run_id,
            )
            self.store.append_event_log(event, order_id=refreshed["order_id"])
            self.logger.info(
                "live_order_canceled",
                order_id=refreshed["order_id"],
                market_id=refreshed["market_id"],
                token_id=refreshed["token_id"],
                venue_order_id=venue_order_id,
                event_time_ms=confirm_time_ms,
                reason=reason,
            )
            return event
        self.logger.info(
            "live_cancel_submitted",
            order_id=order_row["order_id"],
            market_id=order_row["market_id"],
            token_id=order_row["token_id"],
            venue_order_id=venue_order_id,
            venue_status=cancel_result.status,
            event_time_ms=cancel_time_ms,
            reason=reason,
        )
        return None

    def _lookup_venue_order_state(
        self,
        order_row: dict,
        *,
        open_orders_by_market: dict[str, list[VenueOrderState]],
    ) -> VenueOrderState | None:
        if self.client is None:
            raise ExecutionValidationError("Execution client is required in live mode")
        venue_order_id = str(order_row.get("venue_order_id") or "").strip()
        if venue_order_id:
            try:
                venue_state = self.client.get_order(venue_order_id)
            except Exception:
                venue_state = None
            else:
                if venue_state.order_id:
                    return venue_state

        market_id = str(order_row.get("market_id") or "")
        if market_id not in open_orders_by_market:
            try:
                open_orders_by_market[market_id] = list(self.client.list_open_orders(market_id=market_id))
            except Exception:
                open_orders_by_market[market_id] = []
        client_order_id = str(order_row.get("client_order_id") or "").strip()
        token_id = str(order_row.get("token_id") or "").strip()
        size = _as_float_or_none(order_row.get("size"))
        price = _as_float_or_none(order_row.get("price"))
        for venue_state in open_orders_by_market[market_id]:
            if client_order_id and venue_state.client_order_id == client_order_id:
                return venue_state
            if venue_order_id and venue_state.order_id == venue_order_id:
                return venue_state
            if (
                token_id
                and venue_state.token_id == token_id
                and _float_eq(venue_state.price, price)
                and _float_eq(venue_state.original_size, size)
            ):
                return venue_state
        return None

    def _reconcile_not_found_order(self, order_row: dict, *, event_time_ms: int) -> None:
        current_state = self._state_from_row(order_row)
        if current_state is OrderState.INTENT:
            self._transition(
                order_row,
                OrderState.REJECTED,
                event_time_ms=event_time_ms,
                venue_order_status="not_found",
                reconciliation_required=False,
                reject_code="VENUE_NOT_FOUND",
                reject_reason="not_sent_or_not_persisted_at_venue",
            )
            return
        self._transition(
            order_row,
            OrderState.UNKNOWN,
            event_time_ms=event_time_ms,
            venue_order_status="not_found",
            reconciliation_required=True,
            reject_code="VENUE_NOT_FOUND",
            reject_reason="venue_lookup_not_found_during_reconciliation",
        )

    def _reconcile_from_venue_state(self, order_row: dict, *, venue_state: VenueOrderState, event_time_ms: int) -> None:
        status = str(venue_state.status or "")
        current_state = self._state_from_row(order_row)
        original_size = _as_float_or_none(order_row.get("size")) or _as_float_or_none(venue_state.original_size) or 0.0
        cumulative = _as_float_or_none(venue_state.cumulative_filled_size) or 0.0
        remaining = _as_float_or_none(venue_state.remaining_size)
        if remaining is None:
            remaining = max(original_size - cumulative, 0.0)

        if _is_live_venue_status(status):
            if cumulative > 1e-12:
                self._reconcile_live_partial(
                    order_row,
                    venue_state=venue_state,
                    event_time_ms=event_time_ms,
                    cumulative=cumulative,
                    remaining=remaining,
                )
                return
            if current_state is OrderState.PARTIALLY_FILLED:
                self._transition(
                    order_row,
                    OrderState.UNKNOWN,
                    event_time_ms=event_time_ms,
                    venue_order_id=venue_state.order_id or None,
                    venue_order_status=status,
                    reconciliation_required=True,
                    reject_code="RECONCILIATION_MISMATCH",
                    reject_reason="venue_reports_live_zero_fill_for_locally_partially_filled_order",
                )
                return
            self._transition(
                order_row,
                OrderState.ACCEPTED,
                event_time_ms=event_time_ms,
                venue_order_id=venue_state.order_id or None,
                venue_order_status=status,
                reconciliation_required=False,
            )
            return

        if _is_filled_status(status) or (original_size > 0 and cumulative >= original_size - 1e-12):
            self._reconcile_terminal_fill(
                order_row,
                venue_state=venue_state,
                event_time_ms=event_time_ms,
                cumulative=max(cumulative, original_size),
            )
            return

        if _is_canceled_status(status):
            if cumulative > 1e-12:
                self._reconcile_live_partial(
                    order_row,
                    venue_state=venue_state,
                    event_time_ms=max(event_time_ms - 1, 0),
                    cumulative=cumulative,
                    remaining=remaining,
                )
                order_row = self._require_order(order_row["order_id"])
            self._transition(
                order_row,
                OrderState.CANCEL_CONFIRMED,
                event_time_ms=event_time_ms,
                venue_order_id=venue_state.order_id or None,
                venue_order_status=status,
                cumulative_filled_size=cumulative,
                remaining_size=remaining,
                reconciliation_required=False,
            )
            return

        if _is_rejected_status(status):
            self._transition(
                order_row,
                OrderState.REJECTED,
                event_time_ms=event_time_ms,
                venue_order_id=venue_state.order_id or None,
                venue_order_status=status,
                reconciliation_required=False,
                reject_code="VENUE_REJECTED",
                reject_reason="venue_reconciliation_rejected",
            )
            return

        if _is_expired_status(status):
            self._transition(
                order_row,
                OrderState.EXPIRED,
                event_time_ms=event_time_ms,
                venue_order_id=venue_state.order_id or None,
                venue_order_status=status,
                reconciliation_required=False,
            )
            return

        self._transition(
            order_row,
            OrderState.UNKNOWN,
            event_time_ms=event_time_ms,
            venue_order_id=venue_state.order_id or None,
            venue_order_status=status,
            reconciliation_required=True,
            reject_code="VENUE_STATUS_UNKNOWN",
            reject_reason="venue_status_unmapped_during_reconciliation",
        )

    def _reconcile_live_partial(
        self,
        order_row: dict,
        *,
        venue_state: VenueOrderState,
        event_time_ms: int,
        cumulative: float,
        remaining: float,
    ) -> None:
        current_cumulative = _as_float_or_none(order_row.get("cumulative_filled_size")) or 0.0
        fill_delta = max(cumulative - current_cumulative, 0.0)
        if fill_delta > 1e-12:
            fill_id = f"{order_row['order_id']}:reconcile:{event_time_ms}:{int(round(cumulative * 1_000_000))}"
            self.store.insert_fill(
                fill_id=fill_id,
                order_id=order_row["order_id"],
                market_id=order_row["market_id"],
                token_id=order_row["token_id"],
                price=_as_float_or_none(venue_state.price) or _as_float_or_none(order_row.get("price")) or 0.0,
                size=fill_delta,
                filled_at_ms=event_time_ms,
                source="reconciled",
                venue_fill_id=None,
            )
        self._transition(
            order_row,
            OrderState.PARTIALLY_FILLED,
            event_time_ms=event_time_ms,
            venue_order_id=venue_state.order_id or None,
            venue_order_status=venue_state.status,
            cumulative_filled_size=cumulative,
            remaining_size=remaining,
            reconciliation_required=False,
        )

    def _reconcile_terminal_fill(
        self,
        order_row: dict,
        *,
        venue_state: VenueOrderState,
        event_time_ms: int,
        cumulative: float,
    ) -> None:
        current_cumulative = _as_float_or_none(order_row.get("cumulative_filled_size")) or 0.0
        fill_delta = max(cumulative - current_cumulative, 0.0)
        if fill_delta > 1e-12:
            fill_id = f"{order_row['order_id']}:reconcile:{event_time_ms}:{int(round(cumulative * 1_000_000))}"
            self.store.insert_fill(
                fill_id=fill_id,
                order_id=order_row["order_id"],
                market_id=order_row["market_id"],
                token_id=order_row["token_id"],
                price=_as_float_or_none(venue_state.price) or _as_float_or_none(order_row.get("price")) or 0.0,
                size=fill_delta,
                filled_at_ms=event_time_ms,
                source="reconciled",
                venue_fill_id=None,
            )
        self._transition(
            order_row,
            OrderState.FILLED,
            event_time_ms=event_time_ms,
            venue_order_id=venue_state.order_id or None,
            venue_order_status=venue_state.status,
            cumulative_filled_size=cumulative,
            remaining_size=0.0,
            reconciliation_required=False,
        )

    def _resolve_order_for_event(self, event: OrderAccepted | OrderRejected | OrderFilled | OrderCanceled) -> dict | None:
        order_id = getattr(event, "order_id", None)
        if order_id:
            row = self.store.load_order(str(order_id))
            if row is not None:
                return row
            row = self.store.load_order_by_venue_order_id(str(order_id))
            if row is not None:
                return row
        client_order_id = getattr(event, "client_order_id", None)
        if client_order_id:
            row = self.store.load_order_by_client_order_id(str(client_order_id))
            if row is not None:
                return row
        self.logger.warning(
            "external_order_event_unmatched",
            event_type=type(event).__name__,
            event_order_id=getattr(event, "order_id", None),
            client_order_id=getattr(event, "client_order_id", None),
            source=getattr(event, "source", None),
        )
        return None

    def _reconcile_order_accepted(self, event: OrderAccepted, *, order_row: dict) -> OrderAccepted | None:
        current_state = self._state_from_row(order_row)
        if current_state in {OrderState.CANCEL_CONFIRMED, OrderState.FILLED, OrderState.REJECTED, OrderState.EXPIRED, OrderState.REPLACED}:
            return None
        if current_state in {OrderState.ACCEPTED, OrderState.PARTIALLY_FILLED, OrderState.CANCEL_REQUESTED}:
            return self._canonicalize_order_accepted(event, order_row=order_row)
        self._transition(
            order_row,
            OrderState.ACCEPTED,
            event_time_ms=event.event_time_ms,
            venue_order_id=self._external_event_order_id(event, order_row=order_row),
            venue_order_status=event.venue_status,
            reconciliation_required=False,
        )
        return self._canonicalize_order_accepted(event, order_row=self._require_order(order_row["order_id"]))

    def _reconcile_order_rejected(self, event: OrderRejected, *, order_row: dict) -> OrderRejected | None:
        current_state = self._state_from_row(order_row)
        if current_state is OrderState.REJECTED:
            return self._canonicalize_order_rejected(event, order_row=order_row)
        if current_state in TERMINAL_ORDER_STATES - {OrderState.REJECTED}:
            return None
        self._transition(
            order_row,
            OrderState.REJECTED,
            event_time_ms=event.event_time_ms,
            venue_order_status="rejected",
            reconciliation_required=False,
            reject_code=event.reject_reason_code,
            reject_reason=event.reject_reason_text,
        )
        return self._canonicalize_order_rejected(event, order_row=self._require_order(order_row["order_id"]))

    def _reconcile_order_filled(self, event: OrderFilled, *, order_row: dict) -> OrderFilled | None:
        current_state = self._state_from_row(order_row)
        if current_state in {OrderState.CANCEL_CONFIRMED, OrderState.REJECTED, OrderState.EXPIRED, OrderState.REPLACED}:
            return None
        current_cumulative = float(order_row.get("cumulative_filled_size") or 0.0)
        event_cumulative = float(event.cumulative_filled_size)
        if event_cumulative <= current_cumulative + 1e-12:
            return None
        canonical = self._canonicalize_order_filled(event, order_row=order_row)
        next_state = OrderState.FILLED if canonical.remaining_size <= 1e-12 else OrderState.PARTIALLY_FILLED
        fill_id = canonical.venue_fill_id or f"{order_row['order_id']}:fill:{canonical.event_time_ms}:{int(round(canonical.cumulative_filled_size * 1_000_000))}"
        self.store.insert_fill(
            fill_id=fill_id,
            order_id=order_row["order_id"],
            market_id=order_row["market_id"],
            token_id=order_row["token_id"],
            price=canonical.fill_price,
            size=canonical.fill_size,
            filled_at_ms=canonical.event_time_ms,
            source=_normalize_fill_source(canonical.source),
            fee_paid_usdc=canonical.fee_paid_usdc,
            liquidity_role=canonical.liquidity_role.value if hasattr(canonical.liquidity_role, "value") else canonical.liquidity_role,
            venue_fill_id=canonical.venue_fill_id,
        )
        self._transition(
            order_row,
            next_state,
            event_time_ms=canonical.event_time_ms,
            venue_order_id=self._external_event_order_id(event, order_row=order_row),
            venue_order_status="filled" if next_state is OrderState.FILLED else "partially_filled",
            cumulative_filled_size=canonical.cumulative_filled_size,
            remaining_size=canonical.remaining_size,
            reconciliation_required=False,
        )
        return canonical

    def _reconcile_order_canceled(self, event: OrderCanceled, *, order_row: dict) -> OrderCanceled | None:
        current_state = self._state_from_row(order_row)
        if current_state is OrderState.CANCEL_CONFIRMED:
            return self._canonicalize_order_canceled(event, order_row=order_row)
        if current_state in {OrderState.FILLED, OrderState.REJECTED, OrderState.EXPIRED, OrderState.REPLACED}:
            return None
        if current_state in {OrderState.ACCEPTED, OrderState.PARTIALLY_FILLED}:
            self._transition(
                order_row,
                OrderState.CANCEL_REQUESTED,
                event_time_ms=max(event.event_time_ms - 1, 0),
                venue_order_id=self._external_event_order_id(event, order_row=order_row),
                venue_order_status="cancel_requested",
                reconciliation_required=True,
            )
            order_row = self._require_order(order_row["order_id"])
        remaining_size = float(event.remaining_size) if event.remaining_size is not None else 0.0
        cumulative = (
            float(event.cumulative_filled_size)
            if event.cumulative_filled_size is not None
            else float(order_row.get("cumulative_filled_size") or 0.0)
        )
        self._transition(
            order_row,
            OrderState.CANCEL_CONFIRMED,
            event_time_ms=event.event_time_ms,
            venue_order_id=self._external_event_order_id(event, order_row=order_row),
            venue_order_status="canceled",
            cumulative_filled_size=cumulative,
            remaining_size=remaining_size,
            reconciliation_required=False,
        )
        return self._canonicalize_order_canceled(event, order_row=self._require_order(order_row["order_id"]))

    def _canonicalize_order_accepted(self, event: OrderAccepted, *, order_row: dict) -> OrderAccepted:
        return replace(
            event,
            order_id=order_row["order_id"],
            client_order_id=order_row.get("client_order_id") or event.client_order_id,
            market_id=order_row["market_id"],
            token_id=order_row["token_id"],
        )

    def _canonicalize_order_rejected(self, event: OrderRejected, *, order_row: dict) -> OrderRejected:
        return replace(
            event,
            client_order_id=order_row.get("client_order_id") or event.client_order_id,
            market_id=order_row["market_id"],
            token_id=order_row["token_id"],
        )

    def _canonicalize_order_filled(self, event: OrderFilled, *, order_row: dict) -> OrderFilled:
        return replace(
            event,
            order_id=order_row["order_id"],
            client_order_id=order_row.get("client_order_id") or event.client_order_id,
            market_id=order_row["market_id"],
            token_id=order_row["token_id"],
        )

    def _canonicalize_order_canceled(self, event: OrderCanceled, *, order_row: dict) -> OrderCanceled:
        return replace(
            event,
            order_id=order_row["order_id"],
            client_order_id=order_row.get("client_order_id") or event.client_order_id,
            market_id=order_row["market_id"],
            token_id=order_row["token_id"],
        )

    @staticmethod
    def _external_event_order_id(event: OrderAccepted | OrderFilled | OrderCanceled, *, order_row: dict) -> str | None:
        raw = getattr(event, "order_id", None)
        if raw and str(raw) != str(order_row["order_id"]):
            return str(raw)
        existing = order_row.get("venue_order_id")
        return str(existing) if existing else None

    def _require_order(self, order_id: str) -> dict:
        row = self.store.load_order(order_id)
        if row is None:
            raise ExecutionTransitionError(f"Order {order_id} not found in storage")
        return row

    def _transition(
        self,
        order_row: dict,
        next_state: OrderState,
        *,
        event_time_ms: int,
        venue_order_id: str | None = None,
        venue_order_status: str | None = None,
        cumulative_filled_size: float | None = None,
        remaining_size: float | None = None,
        reconciliation_required: bool | None = None,
        reject_code: str | None = None,
        reject_reason: str | None = None,
    ) -> None:
        current_state = self._state_from_row(order_row)
        if next_state is current_state:
            self.store.update_order_state(
                order_id=order_row["order_id"],
                state=next_state,
                event_time_ms=event_time_ms,
                venue_order_id=venue_order_id,
                venue_order_status=venue_order_status,
                cumulative_filled_size=cumulative_filled_size,
                remaining_size=remaining_size,
                reconciliation_required=reconciliation_required,
                reject_code=reject_code,
                reject_reason=reject_reason,
            )
            order_row.update(self._require_order(order_row["order_id"]))
            return
        if current_state in TERMINAL_ORDER_STATES:
            raise ExecutionTransitionError(
                f"Cannot transition terminal order {order_row['order_id']} from {current_state.value} to {next_state.value}"
            )
        allowed = ALLOWED_TRANSITIONS.get(current_state, set())
        if next_state not in allowed:
            raise ExecutionTransitionError(
                f"Illegal transition for {order_row['order_id']}: {current_state.value} -> {next_state.value}"
            )
        self.store.update_order_state(
            order_id=order_row["order_id"],
            state=next_state,
            event_time_ms=event_time_ms,
            venue_order_id=venue_order_id,
            venue_order_status=venue_order_status,
            cumulative_filled_size=cumulative_filled_size,
            remaining_size=remaining_size,
            reconciliation_required=reconciliation_required,
            reject_code=reject_code,
            reject_reason=reject_reason,
        )
        order_row.update(self._require_order(order_row["order_id"]))

    def _validate_order_row(self, order_row: dict) -> None:
        price = float(order_row["price"] or 0.0)
        size = float(order_row["size"] or 0.0)
        if price <= 0:
            raise ExecutionValidationError(f"Order {order_row['order_id']} has non-positive price {price}")
        if size < self.min_order_size - 1e-12:
            raise ExecutionValidationError(
                f"Order {order_row['order_id']} size {size} is below minimum {self.min_order_size}"
            )
        if not is_valid_tick_size(price, self.tick_size):
            raise ExecutionValidationError(
                f"Order {order_row['order_id']} price {price} is not aligned to tick size {self.tick_size}"
            )

    def _market_meta_for_intent(self, intent: OrderIntent) -> MarketMetadata:
        load_latest_market_state = getattr(self.store, "load_latest_market_state", None)
        if callable(load_latest_market_state):
            market_state = load_latest_market_state(intent.market_id)
            if market_state is not None:
                token_yes_id = market_state.token_yes_id or intent.token_id
                token_no_id = market_state.token_no_id or (f"{intent.market_id}:other" if token_yes_id == intent.token_id else intent.token_id)
                return MarketMetadata(
                    market_id=intent.market_id,
                    condition_id=market_state.condition_id or intent.condition_id or intent.market_id,
                    question=market_state.question or intent.question or "",
                    token_yes_id=token_yes_id,
                    token_no_id=token_no_id,
                    start_time_ms=market_state.start_time_ms or intent.event_time_ms,
                    end_time_ms=market_state.end_time_ms or intent.event_time_ms,
                    asset_slug=market_state.asset_slug or intent.asset_slug,
                    is_active=True if market_state.is_active is None else bool(market_state.is_active),
                    duration_seconds=market_state.duration_seconds,
                    fees_enabled=bool(market_state.fees_enabled) if market_state.fees_enabled is not None else True,
                    fee_rate_bps=market_state.fee_rate_bps,
                    tick_size=market_state.tick_size or self.tick_size,
                    min_order_size=market_state.min_order_size,
                )
        return MarketMetadata(
            market_id=intent.market_id,
            condition_id=intent.condition_id or intent.market_id,
            question=intent.question or "",
            token_yes_id=intent.token_id,
            token_no_id=f"{intent.market_id}:other",
            start_time_ms=intent.event_time_ms,
            end_time_ms=intent.event_time_ms,
            asset_slug=intent.asset_slug,
            is_active=True,
            duration_seconds=None,
            fees_enabled=True,
            fee_rate_bps=None,
            tick_size=self.tick_size,
            min_order_size=None,
        )

    @staticmethod
    def _state_from_row(order_row: dict) -> OrderState:
        return OrderState(str(order_row["state"]))

    @staticmethod
    def _event_time_ms_from_order(order_row: dict) -> int:
        text = str(order_row.get("last_state_change_at") or order_row.get("intent_created_at"))
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(round(datetime.fromisoformat(text).timestamp() * 1000))

    @staticmethod
    def _event_time_ms_from_iso(text: str) -> int:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(round(datetime.fromisoformat(text).timestamp() * 1000))


def estimate_fee_usdc(*, price: float, size: float, fee_rate_bps: float, rounding_decimals: int = 6) -> float:
    if price < 0 or size < 0 or fee_rate_bps < 0:
        raise ValueError("price, size, and fee_rate_bps must be non-negative")
    gross_notional = Decimal(str(price)) * Decimal(str(size))
    fee = gross_notional * Decimal(str(fee_rate_bps)) / Decimal("10000")
    quantum = Decimal("1").scaleb(-int(rounding_decimals))
    return float(fee.quantize(quantum, rounding=ROUND_CEILING))


def is_valid_tick_size(price: float, tick_size: float, *, tolerance: float = 1e-9) -> bool:
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    units = price / tick_size
    return abs(units - round(units)) <= tolerance


def _is_canceled_status(status: str | None) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized in {"canceled", "cancelled", "order_status_canceled", "order_status_cancelled"}


def _is_live_venue_status(status: str | None) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized in {"live", "open", "accepted", "order_status_live", "order_status_open", "order_status_accepted"}


def _is_filled_status(status: str | None) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized in {"filled", "matched", "order_status_filled", "order_status_matched"}


def _is_rejected_status(status: str | None) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized in {"rejected", "invalid", "order_status_rejected", "order_status_invalid"}


def _is_expired_status(status: str | None) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized in {"expired", "order_status_expired"}


def _as_float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _float_eq(left: float | None, right: float | None, *, tolerance: float = 1e-9) -> bool:
    if left is None or right is None:
        return False
    return abs(left - right) <= tolerance


def _normalize_fill_source(source: str | None) -> str:
    normalized = str(source or "").strip().lower()
    if normalized in {"venue_ws", "venue_rest", "reconciled", "replay"}:
        return normalized
    if "ws" in normalized or "websocket" in normalized:
        return "venue_ws"
    if "rest" in normalized or "execution" in normalized:
        return "venue_rest"
    return "reconciled"


__all__ = [
    "ALLOWED_TRANSITIONS",
    "ExecutionEngine",
    "ExecutionMode",
    "ExecutionStore",
    "LiveSubmitGuard",
    "LiveSubmitGuardRejected",
    "ExecutionTransitionError",
    "ExecutionValidationError",
    "ExecutionVenueClient",
    "SyntheticFillRequest",
    "TranslationPolicy",
    "VenueConstraints",
    "VenueTranslationError",
    "estimate_fee_usdc",
    "is_valid_tick_size",
    "translate_place_order",
]
