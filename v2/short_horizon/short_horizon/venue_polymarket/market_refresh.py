from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import requests

from ..config import MarketDiscoveryConfig
from ..core.events import MarketStateUpdate, MarketStatus
from ..telemetry import get_logger
from .markets import DurationWindow, MarketMetadata, UniverseFilter, discover_short_horizon_markets


DiscoveryFn = Callable[[UniverseFilter | None, DurationWindow | None, int], Awaitable[list[MarketMetadata]]]
ResolutionSnapshotFn = Callable[[str], Awaitable["MarketResolutionSnapshot | None"]]


@dataclass(frozen=True)
class MarketResolutionSnapshot:
    is_active: bool | None = None
    status: MarketStatus | None = None
    settlement_prices: dict[str, float] | None = None
    resolved_token_id: str | None = None


class MarketRefreshLoop:
    def __init__(
        self,
        *,
        discovery_fn: DiscoveryFn | None = None,
        universe_filter: UniverseFilter | None = None,
        duration_window: DurationWindow | None = None,
        refresh_interval_seconds: int | None = None,
        max_rows: int | None = None,
        retry_backoff_initial_seconds: float = 5.0,
        retry_backoff_max_seconds: float = 120.0,
        max_consecutive_failures: int = 5,
        resolution_snapshot_fn: ResolutionSnapshotFn | None = None,
    ):
        config = MarketDiscoveryConfig()
        self.discovery_fn = discovery_fn or discover_short_horizon_markets
        self.universe_filter = universe_filter or UniverseFilter()
        self.duration_window = duration_window or DurationWindow()
        self.refresh_interval_seconds = int(
            refresh_interval_seconds
            if refresh_interval_seconds is not None
            else config.refresh_interval_seconds
        )
        self.max_rows = int(max_rows if max_rows is not None else config.max_rows)
        self.retry_backoff_initial_seconds = float(retry_backoff_initial_seconds)
        self.retry_backoff_max_seconds = float(retry_backoff_max_seconds)
        self.max_consecutive_failures = int(max_consecutive_failures)
        self.resolution_snapshot_fn = resolution_snapshot_fn or _fetch_market_resolution_snapshot
        self.logger = get_logger("short_horizon.venue_polymarket.market_refresh")
        self._queue: asyncio.Queue[MarketStateUpdate | object] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._previous_markets: dict[str, MarketMetadata] = {}
        self._sentinel = object()
        self._consecutive_failures = 0
        self._last_exception: Exception | None = None

    @property
    def events(self) -> AsyncIterator[MarketStateUpdate]:
        return self

    def __aiter__(self) -> AsyncIterator[MarketStateUpdate]:
        return self

    async def __anext__(self) -> MarketStateUpdate:
        item = await self._queue.get()
        if item is self._sentinel:
            raise StopAsyncIteration
        assert isinstance(item, MarketStateUpdate)
        return item

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="market_refresh_loop")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
        await self._queue.put(self._sentinel)

    def failed(self) -> bool:
        return self._task is not None and self._task.done() and not self._stop_event.is_set()

    def failure_exception(self) -> BaseException | None:
        if not self.failed() or self._task is None:
            return None
        try:
            return self._task.exception()
        except asyncio.CancelledError:
            return None

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    await self.refresh_once()
                    self._consecutive_failures = 0
                    self._last_exception = None
                except Exception as exc:
                    self._consecutive_failures += 1
                    self._last_exception = exc
                    backoff_seconds = min(
                        self.retry_backoff_initial_seconds * (2 ** (self._consecutive_failures - 1)),
                        self.retry_backoff_max_seconds,
                    )
                    self.logger.warning(
                        "market_refresh_retry_scheduled",
                        consecutive_failures=self._consecutive_failures,
                        backoff_seconds=backoff_seconds,
                        error=str(exc),
                    )
                    if self._consecutive_failures >= self.max_consecutive_failures:
                        self.logger.error(
                            "market_refresh_failed",
                            consecutive_failures=self._consecutive_failures,
                            error=str(exc),
                        )
                        raise
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=backoff_seconds)
                    except asyncio.TimeoutError:
                        continue
                    continue
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.refresh_interval_seconds)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise

    async def refresh_once(self) -> list[MarketStateUpdate]:
        now_ms = int(time.time() * 1000)
        latest = await self.discovery_fn(self.universe_filter, self.duration_window, self.max_rows)
        latest_by_id = {market.market_id: market for market in latest}

        queued_events: list[MarketStateUpdate] = []
        new_count = 0
        updated_count = 0
        dropped_count = 0

        for market_id, market in latest_by_id.items():
            previous = self._previous_markets.get(market_id)
            if previous is None:
                new_count += 1
                event = _to_market_state_update(market, event_time_ms=now_ms, ingest_time_ms=now_ms)
                queued_events.append(event)
                await self._queue.put(event)
                continue
            if _market_changed(previous, market):
                updated_count += 1
                event = _to_market_state_update(market, event_time_ms=now_ms, ingest_time_ms=now_ms)
                queued_events.append(event)
                await self._queue.put(event)

        for market_id, previous in self._previous_markets.items():
            if market_id in latest_by_id:
                continue
            dropped_count += 1
            resolution_snapshot = await self.resolution_snapshot_fn(market_id)
            snapshot_active = resolution_snapshot.is_active if resolution_snapshot is not None else None
            snapshot_status = resolution_snapshot.status if resolution_snapshot is not None else None
            event = _to_market_state_update(
                previous,
                event_time_ms=now_ms,
                ingest_time_ms=now_ms,
                is_active=snapshot_active if snapshot_active is not None else False,
                status=snapshot_status or MarketStatus.CLOSED,
                resolved_token_id=resolution_snapshot.resolved_token_id if resolution_snapshot is not None else None,
                settlement_prices=resolution_snapshot.settlement_prices if resolution_snapshot is not None else None,
            )
            queued_events.append(event)
            await self._queue.put(event)

        self._previous_markets = latest_by_id
        self.logger.info(
            "market_refresh_completed",
            eligible_markets=len(latest_by_id),
            new=new_count,
            dropped=dropped_count,
            updated=updated_count,
        )
        return queued_events


def _market_changed(previous: MarketMetadata, current: MarketMetadata) -> bool:
    return (
        previous.end_time_ms != current.end_time_ms
        or previous.start_time_ms != current.start_time_ms
        or previous.is_active != current.is_active
        or previous.token_yes_id != current.token_yes_id
        or previous.token_no_id != current.token_no_id
        or previous.asset_slug != current.asset_slug
        or previous.fees_enabled != current.fees_enabled
        or previous.fee_rate_bps != current.fee_rate_bps
        or previous.fee_info != current.fee_info
        or previous.tick_size != current.tick_size
        or previous.min_order_size != current.min_order_size
    )


def _to_market_state_update(
    market: MarketMetadata,
    *,
    event_time_ms: int,
    ingest_time_ms: int,
    is_active: bool | None = None,
    status: MarketStatus | None = None,
    resolved_token_id: str | None = None,
    settlement_prices: dict[str, float] | None = None,
) -> MarketStateUpdate:
    resolved_is_active = market.is_active if is_active is None else is_active
    resolved_status = status or (MarketStatus.ACTIVE if resolved_is_active else MarketStatus.CLOSED)
    return MarketStateUpdate(
        event_time_ms=event_time_ms,
        ingest_time_ms=ingest_time_ms,
        market_id=market.market_id,
        condition_id=market.condition_id,
        question=market.question,
        status=resolved_status,
        start_time_ms=market.start_time_ms,
        end_time_ms=market.end_time_ms,
        duration_seconds=market.duration_seconds,
        token_yes_id=market.token_yes_id,
        token_no_id=market.token_no_id,
        fee_rate_bps=market.fee_rate_bps,
        fee_info=market.fee_info,
        tick_size=market.tick_size,
        min_order_size=market.min_order_size,
        fee_fetched_at_ms=ingest_time_ms,
        fees_enabled=market.fees_enabled,
        source="polymarket.gamma.refresh",
        asset_slug=market.asset_slug,
        is_active=resolved_is_active,
        metadata_is_fresh=True,
        fee_metadata_age_ms=0,
        resolved_token_id=resolved_token_id,
        settlement_prices=settlement_prices,
    )


async def _fetch_market_resolution_snapshot(market_id: str) -> MarketResolutionSnapshot | None:
    return await asyncio.to_thread(_fetch_market_resolution_snapshot_sync, market_id)


def _fetch_market_resolution_snapshot_sync(market_id: str) -> MarketResolutionSnapshot | None:
    try:
        response = requests.get(f"https://gamma-api.polymarket.com/markets/{market_id}", timeout=20)
        response.raise_for_status()
        payload: Any = response.json()
    except Exception:
        return None
    if isinstance(payload, list):
        payload = payload[0] if payload else {}
    if not isinstance(payload, dict):
        return None

    is_closed = bool(payload.get("closed"))
    is_active = bool(payload.get("active", not is_closed)) and not is_closed
    uma_status = str(payload.get("umaResolutionStatus") or "").lower()
    status = MarketStatus.RESOLVED if uma_status == "resolved" else (MarketStatus.CLOSED if is_closed else MarketStatus.ACTIVE)

    settlement_prices, resolved_token_id = _extract_binary_settlement(payload)
    return MarketResolutionSnapshot(
        is_active=is_active,
        status=status,
        settlement_prices=settlement_prices,
        resolved_token_id=resolved_token_id,
    )


def _extract_binary_settlement(payload: dict[str, Any]) -> tuple[dict[str, float] | None, str | None]:
    if str(payload.get("umaResolutionStatus") or "").lower() != "resolved" and not bool(payload.get("closed")):
        return None, None
    token_ids = _load_json_list(payload.get("clobTokenIds"))
    prices = _load_json_list(payload.get("outcomePrices"))
    if len(token_ids) != len(prices) or not token_ids:
        return None, None
    settlement_prices: dict[str, float] = {}
    resolved_token_id: str | None = None
    for token_id, raw_price in zip(token_ids, prices):
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            return None, None
        if abs(price - 1.0) <= 1e-9:
            resolved_token_id = str(token_id)
            price = 1.0
        elif abs(price) <= 1e-9:
            price = 0.0
        else:
            return None, None
        settlement_prices[str(token_id)] = price
    if resolved_token_id is None:
        return None, None
    return settlement_prices, resolved_token_id


def _load_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


__all__ = ["MarketRefreshLoop"]
