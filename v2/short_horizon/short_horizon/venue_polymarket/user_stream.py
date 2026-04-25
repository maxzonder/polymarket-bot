from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..core.events import OrderAccepted, OrderCanceled, OrderFilled, OrderRejected, OrderSide
from ..telemetry import get_logger
from .execution_client import VenueApiCredentials


WS_USER_ENDPOINT = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
_FILL_EPSILON = 1e-12


@dataclass
class _ObservedOrderState:
    order_id: str
    market_id: str
    token_id: str
    side: OrderSide | None
    price: float | None
    original_size: float | None
    matched_size: float = 0.0
    client_order_id: str | None = None


class UserStreamNormalizer:
    """Normalize Polymarket authenticated user-stream frames into canonical order events."""

    def __init__(self, *, source: str = "polymarket_clob_user_ws"):
        self.source = source
        self._orders: dict[str, _ObservedOrderState] = {}

    def normalize_frame(self, frame: str | dict[str, Any] | list[Any], *, ingest_time_ms: int | None = None) -> list[object]:
        payload: Any = frame
        if isinstance(frame, str):
            payload = json.loads(frame)
        if isinstance(payload, list):
            out: list[object] = []
            for item in payload:
                if isinstance(item, dict):
                    out.extend(self.normalize_event(item, ingest_time_ms=ingest_time_ms))
            return out
        if not isinstance(payload, dict):
            return []
        return self.normalize_event(payload, ingest_time_ms=ingest_time_ms)

    def normalize_event(self, event: dict[str, Any], *, ingest_time_ms: int | None = None) -> list[object]:
        event_type = str(event.get("event_type") or "").strip().lower()
        resolved_ingest = int(ingest_time_ms if ingest_time_ms is not None else time.time() * 1000)
        if event_type == "order":
            return self._normalize_order_event(event, ingest_time_ms=resolved_ingest)
        if event_type == "trade":
            return self._normalize_trade_event(event, ingest_time_ms=resolved_ingest)
        return []

    def _normalize_order_event(self, event: dict[str, Any], *, ingest_time_ms: int) -> list[object]:
        order_id = _parse_optional_str(event.get("id") or event.get("order_id"))
        market_id = _parse_optional_str(event.get("market"))
        token_id = _parse_optional_str(event.get("asset_id") or event.get("token_id"))
        event_time_ms = _parse_int(event.get("timestamp") or event.get("last_update"))
        event_kind = str(event.get("type") or event.get("status") or "").strip().upper()
        side = _parse_order_side(event.get("side"))
        price = _parse_float(event.get("price"))
        original_size = _parse_float(event.get("original_size") or event.get("size"))
        size_matched = _parse_float(event.get("size_matched"))
        client_order_id = _parse_optional_str(event.get("client_order_id"))
        if not order_id or not market_id or not token_id or event_time_ms is None:
            return []

        state = self._ensure_order_state(
            order_id=order_id,
            market_id=market_id,
            token_id=token_id,
            side=side,
            price=price,
            original_size=original_size,
            client_order_id=client_order_id,
        )
        if event_kind == "PLACEMENT":
            if size_matched is not None and size_matched > state.matched_size:
                state.matched_size = size_matched
            return [
                OrderAccepted(
                    event_time_ms=event_time_ms,
                    ingest_time_ms=ingest_time_ms,
                    order_id=order_id,
                    market_id=market_id,
                    token_id=token_id,
                    side=side or OrderSide.BUY,
                    price=price or 0.0,
                    size=original_size or 0.0,
                    source=f"{self.source}.order",
                    client_order_id=state.client_order_id,
                    venue_status="placed",
                )
            ] if side is not None and price is not None and original_size is not None else []

        if event_kind == "UPDATE":
            if size_matched is None:
                return []
            previous_matched = state.matched_size
            if size_matched <= previous_matched + _FILL_EPSILON:
                return []
            return self._fill_delta_events(
                state=state,
                event_time_ms=event_time_ms,
                ingest_time_ms=ingest_time_ms,
                cumulative_filled_size=size_matched,
                previous_filled_size=previous_matched,
                source=f"{self.source}.order_update",
            )

        if event_kind in {"CANCELLATION", "CANCELED", "CANCELLED"}:
            cumulative = size_matched if size_matched is not None else state.matched_size
            if cumulative is not None:
                state.matched_size = max(state.matched_size, cumulative)
            remaining = _remaining_size(state.original_size, cumulative)
            return [
                OrderCanceled(
                    event_time_ms=event_time_ms,
                    ingest_time_ms=ingest_time_ms,
                    order_id=order_id,
                    market_id=market_id,
                    token_id=token_id,
                    source=f"{self.source}.order_cancel",
                    client_order_id=state.client_order_id,
                    cancel_reason=_parse_optional_str(event.get("reason") or event.get("cancel_reason")),
                    cumulative_filled_size=cumulative,
                    remaining_size=remaining,
                )
            ]

        if event_kind in {"REJECTION", "REJECTED"}:
            if side is None:
                return []
            return [
                OrderRejected(
                    event_time_ms=event_time_ms,
                    ingest_time_ms=ingest_time_ms,
                    market_id=market_id,
                    token_id=token_id,
                    side=side,
                    source=f"{self.source}.order_reject",
                    client_order_id=state.client_order_id,
                    price=price,
                    size=original_size,
                    reject_reason_code=_parse_optional_str(event.get("reject_reason_code") or event.get("code")),
                    reject_reason_text=_parse_optional_str(event.get("reject_reason_text") or event.get("reason") or event.get("message")),
                    is_retryable=False,
                )
            ]

        return []

    def _normalize_trade_event(self, event: dict[str, Any], *, ingest_time_ms: int) -> list[object]:
        if str(event.get("status") or "").strip().upper() != "MATCHED":
            return []
        event_time_ms = _parse_int(event.get("timestamp") or event.get("matchtime") or event.get("last_update"))
        market_id = _parse_optional_str(event.get("market"))
        token_id = _parse_optional_str(event.get("asset_id") or event.get("token_id"))
        trade_price = _parse_float(event.get("price"))
        trade_size = _parse_float(event.get("size"))
        taker_side = _parse_order_side(event.get("side"))
        if event_time_ms is None or market_id is None or token_id is None or trade_price is None:
            return []

        out: list[object] = []
        taker_order_id = _parse_optional_str(event.get("taker_order_id"))
        if taker_order_id and trade_size is not None:
            state = self._ensure_order_state(
                order_id=taker_order_id,
                market_id=market_id,
                token_id=token_id,
                side=taker_side,
                price=trade_price,
                original_size=trade_size,
                client_order_id=None,
            )
            out.extend(
                self._fill_delta_events(
                    state=state,
                    event_time_ms=event_time_ms,
                    ingest_time_ms=ingest_time_ms,
                    cumulative_filled_size=max(state.matched_size, trade_size),
                    previous_filled_size=state.matched_size,
                    source=f"{self.source}.trade_taker",
                )
            )

        owner_markers = {
            marker
            for marker in (
                _parse_optional_str(event.get("owner")),
                _parse_optional_str(event.get("trade_owner")),
                _parse_optional_str(event.get("order_owner")),
            )
            if marker
        }
        for maker_order in event.get("maker_orders") or []:
            if not isinstance(maker_order, dict):
                continue
            maker_owner = _parse_optional_str(maker_order.get("owner"))
            if owner_markers and maker_owner and maker_owner not in owner_markers:
                continue
            maker_order_id = _parse_optional_str(maker_order.get("order_id") or maker_order.get("id"))
            matched_amount = _parse_float(maker_order.get("matched_amount") or maker_order.get("size"))
            maker_token_id = _parse_optional_str(maker_order.get("asset_id")) or token_id
            maker_price = _parse_float(maker_order.get("price")) or trade_price
            maker_side = _opposite_side(taker_side)
            if not maker_order_id or matched_amount is None or maker_side is None:
                continue
            state = self._ensure_order_state(
                order_id=maker_order_id,
                market_id=market_id,
                token_id=maker_token_id,
                side=maker_side,
                price=maker_price,
                original_size=matched_amount,
                client_order_id=None,
            )
            out.extend(
                self._fill_delta_events(
                    state=state,
                    event_time_ms=event_time_ms,
                    ingest_time_ms=ingest_time_ms,
                    cumulative_filled_size=max(state.matched_size, matched_amount),
                    previous_filled_size=state.matched_size,
                    source=f"{self.source}.trade_maker",
                )
            )
        return out

    def _ensure_order_state(
        self,
        *,
        order_id: str,
        market_id: str,
        token_id: str,
        side: OrderSide | None,
        price: float | None,
        original_size: float | None,
        client_order_id: str | None,
    ) -> _ObservedOrderState:
        state = self._orders.get(order_id)
        if state is None:
            state = _ObservedOrderState(
                order_id=order_id,
                market_id=market_id,
                token_id=token_id,
                side=side,
                price=price,
                original_size=original_size,
                client_order_id=client_order_id,
            )
            self._orders[order_id] = state
            return state
        state.market_id = market_id or state.market_id
        state.token_id = token_id or state.token_id
        state.side = side or state.side
        state.price = price if price is not None else state.price
        state.original_size = original_size if original_size is not None else state.original_size
        state.client_order_id = client_order_id or state.client_order_id
        return state

    def _fill_delta_events(
        self,
        *,
        state: _ObservedOrderState,
        event_time_ms: int,
        ingest_time_ms: int,
        cumulative_filled_size: float,
        previous_filled_size: float,
        source: str,
    ) -> list[OrderFilled]:
        if state.side is None or state.price is None:
            return []
        if cumulative_filled_size <= previous_filled_size + _FILL_EPSILON:
            return []
        fill_size = cumulative_filled_size - previous_filled_size
        state.matched_size = cumulative_filled_size
        original_size = state.original_size if state.original_size is not None else cumulative_filled_size
        remaining_size = _remaining_size(original_size, cumulative_filled_size)
        return [
            OrderFilled(
                event_time_ms=event_time_ms,
                ingest_time_ms=ingest_time_ms,
                order_id=state.order_id,
                market_id=state.market_id,
                token_id=state.token_id,
                side=state.side,
                fill_price=state.price,
                fill_size=fill_size,
                cumulative_filled_size=cumulative_filled_size,
                remaining_size=remaining_size,
                source=source,
                client_order_id=state.client_order_id,
                liquidity_role=None,
            )
        ]


class PolymarketUserStream:
    def __init__(
        self,
        *,
        auth: VenueApiCredentials,
        endpoint: str = WS_USER_ENDPOINT,
        connect_factory: Callable[[str], Any] | None = None,
        normalizer: UserStreamNormalizer | None = None,
        backoff_initial_seconds: float = 1.0,
        backoff_max_seconds: float = 30.0,
        ping_interval_seconds: float | None = 10.0,
        clock_ms: Callable[[], int] | None = None,
    ):
        self.auth = auth
        self.endpoint = endpoint
        self.connect_factory = connect_factory or _default_connect
        self.normalizer = normalizer or UserStreamNormalizer()
        self.backoff_initial_seconds = float(backoff_initial_seconds)
        self.backoff_max_seconds = float(backoff_max_seconds)
        self.ping_interval_seconds = ping_interval_seconds
        self.clock_ms = clock_ms or _default_clock_ms
        self.logger = get_logger("short_horizon.venue_polymarket.user_stream")
        self.messages: asyncio.Queue[object] = asyncio.Queue()
        self._subscribed_markets: set[str] = set()
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._connected_event = asyncio.Event()
        self._ws: Any = None
        self._subscription_send_failure_count = 0

    async def connect(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="polymarket_user_stream")

    async def close(self) -> None:
        self._stop_event.set()
        ws = self._ws
        if ws is not None:
            await ws.close()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._connected_event.clear()
        self._ws = None

    async def subscribe(self, market_ids: list[str] | tuple[str, ...] | set[str]) -> None:
        async with self._lock:
            self._subscribed_markets.update(str(market_id) for market_id in market_ids if str(market_id).strip())
            payload = _user_subscription_payload(self.auth, self._subscribed_markets)
            ws = self._ws
        self.logger.info("user_ws_subscribe_requested", market_count=len(payload["markets"]))
        if ws is not None and not _is_connection_closed(ws):
            await self._send_subscription_best_effort(
                ws,
                json.dumps(payload),
                success_event="user_ws_subscribe_sent",
                failure_event="user_ws_subscribe_send_failed",
                market_count=len(payload["markets"]),
            )

    async def unsubscribe(self, market_ids: list[str] | tuple[str, ...] | set[str]) -> None:
        async with self._lock:
            requested = sorted(str(market_id) for market_id in market_ids if str(market_id).strip())
            for market_id in requested:
                self._subscribed_markets.discard(market_id)
            ws = self._ws
        self.logger.info("user_ws_unsubscribe_requested", market_count=len(requested), remaining=len(self._subscribed_markets))
        if ws is not None and requested and not _is_connection_closed(ws):
            await self._send_subscription_best_effort(
                ws,
                json.dumps({"markets": requested, "operation": "unsubscribe"}),
                success_event="user_ws_unsubscribe_sent",
                failure_event="user_ws_unsubscribe_send_failed",
                market_count=len(requested),
                remaining=len(self._subscribed_markets),
            )

    async def recv(self) -> object:
        return await self.messages.get()

    async def wait_until_connected(self, timeout: float | None = None) -> None:
        await asyncio.wait_for(self._connected_event.wait(), timeout=timeout)

    async def _run(self) -> None:
        backoff = self.backoff_initial_seconds
        while not self._stop_event.is_set():
            try:
                async with self.connect_factory(self.endpoint) as ws:
                    self._ws = ws
                    self._connected_event.set()
                    self.logger.info("user_ws_connected", endpoint=self.endpoint)
                    await self._resubscribe_current_markets()
                    ping_task = (
                        asyncio.create_task(self._ping_loop(ws), name="polymarket_user_stream_ping")
                        if self.ping_interval_seconds is not None
                        else None
                    )
                    try:
                        async for message in ws:
                            if isinstance(message, bytes):
                                message = message.decode("utf-8")
                            if _is_control_message(message):
                                continue
                            payload = _decode_payload(message)
                            if payload is None:
                                continue
                            events = self.normalizer.normalize_frame(payload, ingest_time_ms=self.clock_ms())
                            for event in events:
                                await self.messages.put(event)
                    finally:
                        if ping_task is not None:
                            ping_task.cancel()
                            try:
                                await ping_task
                            except asyncio.CancelledError:
                                pass
                    backoff = self.backoff_initial_seconds
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("user_ws_disconnected", endpoint=self.endpoint, error=str(exc), retry_in_seconds=backoff)
                self._connected_event.clear()
                self._ws = None
                await asyncio.sleep(backoff)
                backoff = min(self.backoff_max_seconds, max(self.backoff_initial_seconds, backoff * 2))
            else:
                self._connected_event.clear()
                self._ws = None
                if not self._stop_event.is_set():
                    await asyncio.sleep(backoff)
                    backoff = min(self.backoff_max_seconds, max(self.backoff_initial_seconds, backoff * 2))

    async def _resubscribe_current_markets(self) -> None:
        async with self._lock:
            payload = _user_subscription_payload(self.auth, self._subscribed_markets)
            ws = self._ws
        if ws is None:
            return
        await ws.send(json.dumps(payload))
        self.logger.info("user_ws_subscribe_sent", market_count=len(payload["markets"]))

    async def _send_subscription_best_effort(
        self,
        ws: Any,
        payload_json: str,
        *,
        success_event: str,
        failure_event: str,
        market_count: int,
        remaining: int | None = None,
    ) -> None:
        try:
            await ws.send(payload_json)
        except Exception as exc:
            self._subscription_send_failure_count += 1
            if self._ws is ws:
                self._connected_event.clear()
                self._ws = None
            self.logger.warning(
                failure_event,
                endpoint=self.endpoint,
                error=str(exc),
                market_count=market_count,
                remaining=remaining,
                failure_count=self._subscription_send_failure_count,
            )
            return
        if remaining is None:
            self.logger.info(success_event, market_count=market_count)
        else:
            self.logger.info(success_event, market_count=market_count, remaining=remaining)

    async def _ping_loop(self, ws: Any) -> None:
        assert self.ping_interval_seconds is not None
        while True:
            await asyncio.sleep(self.ping_interval_seconds)
            await ws.send("PING")


def _user_subscription_payload(auth: VenueApiCredentials, market_ids: set[str]) -> dict[str, Any]:
    return {
        "auth": {
            "apiKey": auth.api_key,
            "secret": auth.secret,
            "passphrase": auth.passphrase,
        },
        "markets": sorted(market_ids),
        "type": "user",
    }


def _decode_payload(message: Any) -> Any:
    if isinstance(message, (dict, list)):
        return message
    if isinstance(message, str):
        text = message.strip()
        if not text or text.upper() == "PONG":
            return None
        return json.loads(text)
    raise TypeError(f"Unsupported user-stream message type: {type(message)!r}")


def _is_control_message(message: Any) -> bool:
    return isinstance(message, str) and message.strip().upper() in {"PING", "PONG"}


def _is_connection_closed(ws: Any) -> bool:
    closed = getattr(ws, "closed", False)
    if bool(closed):
        return True
    state = getattr(ws, "state", None)
    if state is None:
        return False
    state_name = str(getattr(state, "name", state)).upper()
    if state_name in {"CLOSED", "CLOSING"}:
        return True
    try:
        return int(state) in {2, 3}
    except (TypeError, ValueError):
        return False


def _parse_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _parse_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_order_side(value: Any) -> OrderSide | None:
    text = str(value).strip().upper() if value is not None else ""
    if text == "BUY":
        return OrderSide.BUY
    if text == "SELL":
        return OrderSide.SELL
    return None


def _opposite_side(side: OrderSide | None) -> OrderSide | None:
    if side is OrderSide.BUY:
        return OrderSide.SELL
    if side is OrderSide.SELL:
        return OrderSide.BUY
    return None


def _remaining_size(original_size: float | None, cumulative_filled_size: float | None) -> float | None:
    if original_size is None or cumulative_filled_size is None:
        return None
    return max(float(original_size) - float(cumulative_filled_size), 0.0)


def _default_connect(endpoint: str) -> Any:
    import websockets

    return websockets.connect(endpoint, ping_interval=None, max_size=8_000_000)


def _default_clock_ms() -> int:
    return int(time.time() * 1000)


__all__ = ["PolymarketUserStream", "UserStreamNormalizer", "WS_USER_ENDPOINT"]
