from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

from ..telemetry import get_logger


WS_MARKET_ENDPOINT = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class PolymarketWebsocket:
    def __init__(
        self,
        *,
        endpoint: str = WS_MARKET_ENDPOINT,
        connect_factory: Callable[[str], Any] | None = None,
        backoff_initial_seconds: float = 1.0,
        backoff_max_seconds: float = 30.0,
        ping_interval_seconds: float | None = 10.0,
    ):
        self.endpoint = endpoint
        self.connect_factory = connect_factory or _default_connect
        self.backoff_initial_seconds = float(backoff_initial_seconds)
        self.backoff_max_seconds = float(backoff_max_seconds)
        self.ping_interval_seconds = ping_interval_seconds
        self.logger = get_logger("short_horizon.venue_polymarket.websocket")
        self.messages: asyncio.Queue[str] = asyncio.Queue()
        self._subscribed_tokens: set[str] = set()
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
        self._task = asyncio.create_task(self._run(), name="polymarket_websocket")

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

    async def subscribe(self, token_ids: list[str] | tuple[str, ...] | set[str]) -> None:
        async with self._lock:
            self._subscribed_tokens.update(str(token_id) for token_id in token_ids)
            payload = _subscription_payload(self._subscribed_tokens)
            ws = self._ws
        self.logger.info("ws_subscribe_requested", token_count=len(payload["assets_ids"]))
        if ws is not None and payload["assets_ids"] and not _is_connection_closed(ws):
            await self._send_subscription_best_effort(
                ws,
                json.dumps(payload),
                success_event="ws_subscribe_sent",
                failure_event="ws_subscribe_send_failed",
                token_count=len(payload["assets_ids"]),
            )

    async def unsubscribe(self, token_ids: list[str] | tuple[str, ...] | set[str]) -> None:
        async with self._lock:
            for token_id in token_ids:
                self._subscribed_tokens.discard(str(token_id))
            payload = _subscription_payload(self._subscribed_tokens)
            ws = self._ws
        self.logger.info("ws_unsubscribe_requested", token_count=len(token_ids), remaining=len(payload["assets_ids"]))
        if ws is not None and payload["assets_ids"] and not _is_connection_closed(ws):
            await self._send_subscription_best_effort(
                ws,
                json.dumps(payload),
                success_event="ws_subscribe_sent",
                failure_event="ws_unsubscribe_send_failed",
                token_count=len(payload["assets_ids"]),
                requested_count=len(token_ids),
            )

    async def recv(self) -> str:
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
                    self.logger.info("ws_connected", endpoint=self.endpoint)
                    await self._resubscribe_current_tokens()
                    ping_task = (
                        asyncio.create_task(self._ping_loop(ws), name="polymarket_websocket_ping")
                        if self.ping_interval_seconds is not None
                        else None
                    )
                    try:
                        async for message in ws:
                            if isinstance(message, bytes):
                                continue
                            if _is_control_message(message):
                                continue
                            await self.messages.put(message)
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
                self.logger.warning("ws_disconnected", endpoint=self.endpoint, error=str(exc), retry_in_seconds=backoff)
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

    async def _resubscribe_current_tokens(self) -> None:
        async with self._lock:
            payload = _subscription_payload(self._subscribed_tokens)
            ws = self._ws
        if ws is None or not payload["assets_ids"]:
            return
        await ws.send(json.dumps(payload))
        self.logger.info("ws_subscribe_sent", token_count=len(payload["assets_ids"]))

    async def _send_subscription_best_effort(
        self,
        ws: Any,
        payload_json: str,
        *,
        success_event: str,
        failure_event: str,
        token_count: int,
        requested_count: int | None = None,
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
                token_count=token_count,
                requested_count=requested_count,
                failure_count=self._subscription_send_failure_count,
            )
            return
        self.logger.info(success_event, token_count=token_count)

    async def _ping_loop(self, ws: Any) -> None:
        assert self.ping_interval_seconds is not None
        while True:
            await asyncio.sleep(self.ping_interval_seconds)
            await ws.send("PING")


def _is_control_message(message: Any) -> bool:
    return isinstance(message, str) and message.strip().upper() in {"PONG", "PING"}


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


def _subscription_payload(token_ids: set[str]) -> dict[str, Any]:
    return {
        "assets_ids": sorted(token_ids),
        "type": "market",
        "custom_feature_enabled": True,
    }


def _default_connect(endpoint: str) -> Any:
    import websockets

    return websockets.connect(endpoint, ping_interval=None, max_size=8_000_000)


__all__ = ["PolymarketWebsocket", "WS_MARKET_ENDPOINT"]
