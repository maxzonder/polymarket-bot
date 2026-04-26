from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Awaitable, Callable

from ..config import FeesConfig, MarketDiscoveryConfig
from ..core.events import MarketStateUpdate, MarketStatus
from ..telemetry import get_logger
from .markets import DurationWindow, MarketMetadata, UniverseFilter, discover_short_horizon_markets


DiscoveryFn = Callable[[UniverseFilter | None, DurationWindow | None, int], Awaitable[list[MarketMetadata]]]
ClockFn = Callable[[], int]


class FeeMetadataRefreshLoop:
    """Refresh fee snapshots for active markets on a cadence safely inside TTL."""

    def __init__(
        self,
        *,
        discovery_fn: DiscoveryFn | None = None,
        universe_filter: UniverseFilter | None = None,
        duration_window: DurationWindow | None = None,
        refresh_interval_seconds: int | None = None,
        fee_metadata_ttl_seconds: int | None = None,
        max_rows: int | None = None,
        retry_backoff_initial_seconds: float = 5.0,
        retry_backoff_max_seconds: float = 120.0,
        max_consecutive_failures: int = 5,
        clock_ms: ClockFn | None = None,
    ):
        discovery_config = MarketDiscoveryConfig()
        self.discovery_fn = discovery_fn or discover_short_horizon_markets
        self.universe_filter = universe_filter or UniverseFilter()
        self.duration_window = duration_window or DurationWindow()
        self.fee_metadata_ttl_seconds = int(
            fee_metadata_ttl_seconds
            if fee_metadata_ttl_seconds is not None
            else FeesConfig().fee_metadata_ttl_seconds
        )
        self.refresh_interval_seconds = int(
            refresh_interval_seconds
            if refresh_interval_seconds is not None
            else min(discovery_config.refresh_interval_seconds, self.fee_metadata_ttl_seconds)
        )
        self.max_rows = int(max_rows if max_rows is not None else discovery_config.max_rows)
        self.retry_backoff_initial_seconds = float(retry_backoff_initial_seconds)
        self.retry_backoff_max_seconds = float(retry_backoff_max_seconds)
        self.max_consecutive_failures = int(max_consecutive_failures)
        self.clock_ms = clock_ms or _default_clock_ms
        self.logger = get_logger("short_horizon.venue_polymarket.fee_metadata")
        self._queue: asyncio.Queue[MarketStateUpdate | object] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
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
        self._task = asyncio.create_task(self._run(), name="fee_metadata_refresh_loop")

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
                        "fee_metadata_refresh_retry_scheduled",
                        consecutive_failures=self._consecutive_failures,
                        backoff_seconds=backoff_seconds,
                        error=str(exc),
                    )
                    if self._consecutive_failures >= self.max_consecutive_failures:
                        self.logger.error(
                            "fee_metadata_refresh_failed",
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
        now_ms = self.clock_ms()
        latest = await self.discovery_fn(self.universe_filter, self.duration_window, self.max_rows)

        queued_events: list[MarketStateUpdate] = []
        for market in latest:
            event = _to_fee_market_state_update(market, event_time_ms=now_ms, ingest_time_ms=now_ms)
            queued_events.append(event)
            await self._queue.put(event)

        self.logger.info(
            "fee_metadata_refresh_completed",
            eligible_markets=len(latest),
            ttl_seconds=self.fee_metadata_ttl_seconds,
            refresh_interval_seconds=self.refresh_interval_seconds,
            emitted=len(queued_events),
        )
        return queued_events


def _to_fee_market_state_update(
    market: MarketMetadata,
    *,
    event_time_ms: int,
    ingest_time_ms: int,
) -> MarketStateUpdate:
    return MarketStateUpdate(
        event_time_ms=event_time_ms,
        ingest_time_ms=ingest_time_ms,
        market_id=market.market_id,
        condition_id=market.condition_id,
        question=market.question,
        status=MarketStatus.ACTIVE if market.is_active else MarketStatus.CLOSED,
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
        source="polymarket.gamma.fee_refresh",
        asset_slug=market.asset_slug,
        is_active=market.is_active,
        metadata_is_fresh=True,
        fee_metadata_age_ms=0,
    )


def _default_clock_ms() -> int:
    return int(time.time() * 1000)


__all__ = ["FeeMetadataRefreshLoop"]
