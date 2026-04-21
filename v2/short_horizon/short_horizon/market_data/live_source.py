from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator
from typing import Any

from ..config import MarketDiscoveryConfig
from ..core.events import MarketStateUpdate, NormalizedEvent
from ..telemetry import get_logger
from ..venue_polymarket import (
    BookNormalizer,
    FeeMetadataRefreshLoop,
    MarketRefreshLoop,
    PolymarketUserStream,
    PolymarketWebsocket,
    SharedMarketDiscovery,
    TradeNormalizer,
)


class LiveEventSource:
    """Composite Polymarket live source for normalized short-horizon events."""

    def __init__(
        self,
        *,
        market_refresh: MarketRefreshLoop | AsyncIterator[MarketStateUpdate] | None = None,
        fee_refresh: FeeMetadataRefreshLoop | AsyncIterator[MarketStateUpdate] | None = None,
        websocket: PolymarketWebsocket | Any | None = None,
        user_stream: PolymarketUserStream | Any | None = None,
        book_normalizer: BookNormalizer | None = None,
        trade_normalizer: TradeNormalizer | None = None,
        clock_ms: callable | None = None,
    ):
        discovery_config = MarketDiscoveryConfig()
        shared_discovery = SharedMarketDiscovery(
            refresh_interval_seconds=discovery_config.refresh_interval_seconds,
            max_rows=discovery_config.max_rows,
        )
        self.market_refresh = market_refresh or MarketRefreshLoop(discovery_fn=shared_discovery, max_rows=discovery_config.max_rows)
        self.fee_refresh = fee_refresh or FeeMetadataRefreshLoop(discovery_fn=shared_discovery, max_rows=discovery_config.max_rows)
        self.websocket = websocket or PolymarketWebsocket()
        self.user_stream = user_stream
        self.book_normalizer = book_normalizer or BookNormalizer()
        self.trade_normalizer = trade_normalizer or TradeNormalizer()
        self.clock_ms = clock_ms or _default_clock_ms
        self.logger = get_logger("short_horizon.market_data.live_source")
        self._queue: asyncio.Queue[NormalizedEvent | object] = asyncio.Queue()
        self._sentinel = object()
        self._forward_tasks: list[asyncio.Task[None]] = []
        self._market_tokens: dict[str, set[str]] = {}
        self._market_conditions: dict[str, str] = {}
        self._started = False
        self._terminal_error: BaseException | None = None

    @property
    def events(self) -> AsyncIterator[NormalizedEvent]:
        return self

    def __aiter__(self) -> AsyncIterator[NormalizedEvent]:
        return self

    async def __anext__(self) -> NormalizedEvent:
        if self._terminal_error is not None:
            raise self._terminal_error
        item = await self._queue.get()
        if item is self._sentinel:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            self._terminal_error = item
            raise item
        return item  # type: ignore[return-value]

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._terminal_error = None
        await _maybe_call(self.websocket, "connect")
        await _maybe_call(self.user_stream, "connect")
        await _maybe_call(self.market_refresh, "start")
        await _maybe_call(self.fee_refresh, "start")
        self._forward_tasks = [
            asyncio.create_task(self._consume_market_refresh(), name="live_source_market_refresh"),
            asyncio.create_task(self._consume_fee_refresh(), name="live_source_fee_refresh"),
            asyncio.create_task(self._consume_websocket(), name="live_source_websocket"),
            asyncio.create_task(self._monitor_components(), name="live_source_component_monitor"),
        ]
        if self.user_stream is not None:
            self._forward_tasks.append(asyncio.create_task(self._consume_user_stream(), name="live_source_user_stream"))

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        for task in self._forward_tasks:
            task.cancel()
        for task in self._forward_tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._forward_tasks.clear()
        await _maybe_call(self.market_refresh, "stop")
        await _maybe_call(self.fee_refresh, "stop")
        await _maybe_call(self.websocket, "close")
        await _maybe_call(self.user_stream, "close")
        await self._queue.put(self._sentinel)

    async def _consume_market_refresh(self) -> None:
        try:
            async for event in self.market_refresh:
                self._register_market_event(event)
                await self._reconcile_market_subscriptions(event)
                for token_event in expand_market_state_update(event):
                    await self._queue.put(token_event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._signal_terminal_error(exc, component="market_refresh_consumer")

    async def _consume_fee_refresh(self) -> None:
        try:
            async for event in self.fee_refresh:
                self._register_market_event(event)
                for token_event in expand_market_state_update(event):
                    await self._queue.put(token_event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._signal_terminal_error(exc, component="fee_refresh_consumer")

    async def _consume_websocket(self) -> None:
        try:
            while True:
                raw_message = await self.websocket.recv()
                if _is_control_message(raw_message):
                    continue
                ingest_time_ms = self.clock_ms()
                try:
                    payload = _decode_json_payload(raw_message)
                except json.JSONDecodeError:
                    self.logger.warning(
                        "live_source_ws_message_dropped",
                        reason="invalid_json",
                        raw_message_preview=str(raw_message)[:120],
                    )
                    continue
                if payload is None:
                    continue
                book_events = self.book_normalizer.normalize_frame(payload, ingest_time_ms=ingest_time_ms)
                trade_events = self.trade_normalizer.normalize_frame(payload, ingest_time_ms=ingest_time_ms)
                for event in [*book_events, *trade_events]:
                    await self._queue.put(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._signal_terminal_error(exc, component="websocket_consumer")

    async def _consume_user_stream(self) -> None:
        if self.user_stream is None:
            return
        try:
            while True:
                event = await self.user_stream.recv()
                await self._queue.put(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._signal_terminal_error(exc, component="user_stream_consumer")

    async def _monitor_components(self) -> None:
        try:
            while self._started and self._terminal_error is None:
                for task in self._forward_tasks:
                    if task.done() and not task.cancelled():
                        exc = task.exception()
                        if exc is not None:
                            await self._signal_terminal_error(exc, component=task.get_name())
                            return
                for component_name, component in (("market_refresh", self.market_refresh), ("fee_refresh", self.fee_refresh)):
                    if _component_failed(component):
                        await self._signal_terminal_error(
                            _component_failure_exception(component) or RuntimeError(f"{component_name} failed"),
                            component=component_name,
                        )
                        return
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            raise

    async def _signal_terminal_error(self, exc: BaseException, *, component: str) -> None:
        if self._terminal_error is not None:
            return
        error = RuntimeError(f"LiveEventSource component failed: {component}: {exc}")
        self._terminal_error = error
        self.logger.error("live_source_component_failed", component=component, error=str(exc))
        await self._queue.put(error)

    def _register_market_event(self, event: MarketStateUpdate) -> None:
        register_book = getattr(self.book_normalizer, "register_market", None)
        if callable(register_book):
            register_book(event)
        register_trade = getattr(self.trade_normalizer, "register_market", None)
        if callable(register_trade):
            register_trade(event)

    async def _reconcile_market_subscriptions(self, event: MarketStateUpdate) -> None:
        market_id = str(event.market_id)
        previous_tokens = self._market_tokens.get(market_id, set())
        current_tokens = extract_market_token_ids(event) if bool(event.is_active) else set()
        previous_condition = self._market_conditions.get(market_id)
        current_condition = str(event.condition_id).strip() if bool(event.is_active) and event.condition_id else None
        added = current_tokens - previous_tokens
        removed = previous_tokens - current_tokens
        if added:
            await self.websocket.subscribe(sorted(added))
        if removed:
            await self.websocket.unsubscribe(sorted(removed))
        if self.user_stream is not None:
            if previous_condition and previous_condition != current_condition:
                await self.user_stream.unsubscribe([previous_condition])
            if current_condition and current_condition != previous_condition:
                await self.user_stream.subscribe([current_condition])
        if current_tokens:
            self._market_tokens[market_id] = current_tokens
        else:
            self._market_tokens.pop(market_id, None)
        if current_condition:
            self._market_conditions[market_id] = current_condition
        else:
            self._market_conditions.pop(market_id, None)
        if added or removed:
            self.logger.info(
                "live_source_market_subscription_reconciled",
                market_id=market_id,
                added=len(added),
                removed=len(removed),
                user_market_subscribed=bool(self.user_stream is not None and current_condition),
                subscribed_markets=len(self._market_tokens),
            )


async def _maybe_call(target: Any, method_name: str) -> Any:
    method = getattr(target, method_name, None)
    if not callable(method):
        return None
    result = method()
    if asyncio.iscoroutine(result):
        return await result
    return result


def expand_market_state_update(event: MarketStateUpdate) -> list[MarketStateUpdate]:
    token_ids = list(dict.fromkeys(token_id for token_id in extract_market_token_ids(event) if token_id))
    if not token_ids:
        if event.token_id is not None:
            return [event]
        return []
    expanded: list[MarketStateUpdate] = []
    for token_id in token_ids:
        expanded.append(
            MarketStateUpdate(
                event_time_ms=event.event_time_ms,
                ingest_time_ms=event.ingest_time_ms,
                market_id=event.market_id,
                condition_id=event.condition_id,
                question=event.question,
                status=event.status,
                start_time_ms=event.start_time_ms,
                end_time_ms=event.end_time_ms,
                duration_seconds=event.duration_seconds,
                token_yes_id=event.token_yes_id,
                token_no_id=event.token_no_id,
                fee_rate_bps=event.fee_rate_bps,
                fee_fetched_at_ms=event.fee_fetched_at_ms,
                fees_enabled=event.fees_enabled,
                is_ascending_market=event.is_ascending_market,
                market_source_revision=event.market_source_revision,
                source=event.source,
                run_id=event.run_id,
                token_id=token_id,
                asset_slug=event.asset_slug,
                is_active=event.is_active,
                metadata_is_fresh=event.metadata_is_fresh,
                fee_metadata_age_ms=event.fee_metadata_age_ms,
            )
        )
    return expanded


def extract_market_token_ids(event: MarketStateUpdate) -> set[str]:
    token_ids: set[str] = set()
    if event.token_yes_id:
        token_ids.add(str(event.token_yes_id))
    if event.token_no_id:
        token_ids.add(str(event.token_no_id))
    if event.token_id:
        token_ids.add(str(event.token_id))
    return token_ids


def _decode_json_payload(raw_message: Any) -> Any:
    if isinstance(raw_message, (dict, list)):
        return raw_message
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    if isinstance(raw_message, str):
        text = raw_message.strip()
        if not text or text.upper() == "PONG":
            return None
        return json.loads(text)
    raise TypeError(f"Unsupported websocket message type: {type(raw_message)!r}")


def _is_control_message(raw_message: Any) -> bool:
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8", errors="ignore")
    return isinstance(raw_message, str) and raw_message.strip().upper() in {"PONG", "PING"}


def _default_clock_ms() -> int:
    return int(time.time() * 1000)


def _component_failed(component: Any) -> bool:
    failed = getattr(component, "failed", None)
    if callable(failed):
        return bool(failed())
    return False


def _component_failure_exception(component: Any) -> BaseException | None:
    failure_exception = getattr(component, "failure_exception", None)
    if callable(failure_exception):
        result = failure_exception()
        if isinstance(result, BaseException):
            return result
    return None


__all__ = ["LiveEventSource", "expand_market_state_update", "extract_market_token_ids"]
