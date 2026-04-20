from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Awaitable, Callable

from ..config import MarketDiscoveryConfig
from ..core.events import MarketStateUpdate, MarketStatus
from ..telemetry import get_logger
from .markets import DurationWindow, MarketMetadata, UniverseFilter, discover_short_horizon_markets


DiscoveryFn = Callable[[UniverseFilter | None, DurationWindow | None, int], Awaitable[list[MarketMetadata]]]


class MarketRefreshLoop:
    def __init__(
        self,
        *,
        discovery_fn: DiscoveryFn | None = None,
        universe_filter: UniverseFilter | None = None,
        duration_window: DurationWindow | None = None,
        refresh_interval_seconds: int | None = None,
        max_rows: int = 20_000,
    ):
        self.discovery_fn = discovery_fn or discover_short_horizon_markets
        self.universe_filter = universe_filter or UniverseFilter()
        self.duration_window = duration_window or DurationWindow()
        self.refresh_interval_seconds = int(
            refresh_interval_seconds
            if refresh_interval_seconds is not None
            else MarketDiscoveryConfig().refresh_interval_seconds
        )
        self.max_rows = int(max_rows)
        self.logger = get_logger("short_horizon.venue_polymarket.market_refresh")
        self._queue: asyncio.Queue[MarketStateUpdate | object] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._previous_markets: dict[str, MarketMetadata] = {}
        self._sentinel = object()

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
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._queue.put(self._sentinel)

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                await self.refresh_once()
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
            event = _to_market_state_update(
                previous,
                event_time_ms=now_ms,
                ingest_time_ms=now_ms,
                is_active=False,
                status=MarketStatus.CLOSED,
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
    )


def _to_market_state_update(
    market: MarketMetadata,
    *,
    event_time_ms: int,
    ingest_time_ms: int,
    is_active: bool | None = None,
    status: MarketStatus | None = None,
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
        fee_rate_bps=None,
        fee_fetched_at_ms=None,
        fees_enabled=market.fees_enabled,
        source="polymarket.gamma.refresh",
        asset_slug=market.asset_slug,
        is_active=resolved_is_active,
        metadata_is_fresh=True,
        fee_metadata_age_ms=None,
    )


__all__ = ["MarketRefreshLoop"]
