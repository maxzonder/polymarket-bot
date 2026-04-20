from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
from datetime import datetime
from typing import Protocol

from ..core.events import OrderAccepted, OrderCanceled, OrderFilled, OrderSide
from ..core.models import OrderIntent
from ..core.order_state import OrderState
from ..strategy_api import CancelOrder, Noop, PlaceOrder, StrategyIntent
from ..telemetry import get_logger
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


class ExecutionTransitionError(RuntimeError):
    pass


class ExecutionValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class SyntheticFillRequest:
    order_id: str
    event_time_ms: int
    fill_size: float | None = None
    fill_price: float | None = None
    fee_paid_usdc: float | None = None
    source: str = "replay"


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

    def load_non_terminal_orders(self) -> list[dict]:
        ...


class ExecutionEngine:
    """Synthetic Phase-1 execution boundary.

    Current responsibility in P1-6:
    - consume strategy intents
    - materialize orders in storage when needed
    - enforce the P0-C state machine for placeholder send/cancel/fill flows
    - emit synthetic order events until real venue wiring lands in Phase 3
    """

    def __init__(self, *, store: ExecutionStore, tick_size: float = 0.01, min_order_size: float = 1.0):
        self.store = store
        self.tick_size = float(tick_size)
        self.min_order_size = float(min_order_size)
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

    def submit(self, intent: OrderIntent, *, event_time_ms: int | None = None) -> list[OrderAccepted]:
        send_time_ms = int(event_time_ms if event_time_ms is not None else intent.event_time_ms)
        accepted_time_ms = send_time_ms + 1
        order_row = self._ensure_order_row(intent)
        self._validate_order_row(order_row)
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
            source="execution.synthetic_accept",
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
        )
        return [accepted]

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
        venue_order_status: str | None = None,
        cumulative_filled_size: float | None = None,
        remaining_size: float | None = None,
    ) -> None:
        current_state = self._state_from_row(order_row)
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
            venue_order_status=venue_order_status,
            cumulative_filled_size=cumulative_filled_size,
            remaining_size=remaining_size,
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

    @staticmethod
    def _state_from_row(order_row: dict) -> OrderState:
        return OrderState(str(order_row["state"]))

    @staticmethod
    def _event_time_ms_from_order(order_row: dict) -> int:
        text = str(order_row.get("last_state_change_at") or order_row.get("intent_created_at"))
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


__all__ = [
    "ALLOWED_TRANSITIONS",
    "ExecutionEngine",
    "ExecutionStore",
    "ExecutionTransitionError",
    "ExecutionValidationError",
    "SyntheticFillRequest",
    "TranslationPolicy",
    "VenueConstraints",
    "VenueTranslationError",
    "estimate_fee_usdc",
    "is_valid_tick_size",
    "translate_place_order",
]
