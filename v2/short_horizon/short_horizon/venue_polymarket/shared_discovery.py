from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

from ..config import MarketDiscoveryConfig
from ..telemetry import get_logger
from .markets import DurationWindow, MarketMetadata, UniverseFilter, discover_short_horizon_markets


DiscoveryFn = Callable[[UniverseFilter | None, DurationWindow | None, int], Awaitable[list[MarketMetadata]]]
ClockFn = Callable[[], float]


class SharedMarketDiscovery:
    """Deduplicate live discovery refreshes across multiple consumers."""

    def __init__(
        self,
        *,
        discovery_fn: DiscoveryFn | None = None,
        universe_filter: UniverseFilter | None = None,
        duration_window: DurationWindow | None = None,
        refresh_interval_seconds: float | None = None,
        max_rows: int | None = None,
        clock_s: ClockFn | None = None,
    ):
        config = MarketDiscoveryConfig()
        self.discovery_fn = discovery_fn or discover_short_horizon_markets
        self.universe_filter = universe_filter or UniverseFilter()
        self.duration_window = duration_window or DurationWindow()
        self.refresh_interval_seconds = float(
            refresh_interval_seconds if refresh_interval_seconds is not None else config.refresh_interval_seconds
        )
        self.max_rows = int(max_rows if max_rows is not None else config.max_rows)
        if self.max_rows <= 0:
            raise ValueError("max_rows must be positive")
        self.clock_s = clock_s or time.monotonic
        self.logger = get_logger("short_horizon.venue_polymarket.shared_discovery")
        self._lock = asyncio.Lock()
        self._inflight: asyncio.Task[list[MarketMetadata]] | None = None
        self._snapshot: list[MarketMetadata] | None = None
        self._snapshot_key: tuple[UniverseFilter, DurationWindow, int] | None = None
        self._snapshot_fetched_at_s: float | None = None

    async def __call__(
        self,
        universe_filter: UniverseFilter | None = None,
        duration_window: DurationWindow | None = None,
        max_rows: int | None = None,
    ) -> list[MarketMetadata]:
        return await self.get_markets(universe_filter=universe_filter, duration_window=duration_window, max_rows=max_rows)

    async def get_markets(
        self,
        *,
        universe_filter: UniverseFilter | None = None,
        duration_window: DurationWindow | None = None,
        max_rows: int | None = None,
    ) -> list[MarketMetadata]:
        resolved_universe = universe_filter or self.universe_filter
        resolved_window = duration_window or self.duration_window
        resolved_max_rows = int(max_rows if max_rows is not None else self.max_rows)
        resolved_max_rows = min(resolved_max_rows, self.max_rows)
        if resolved_max_rows <= 0:
            raise ValueError("max_rows must be positive")
        cache_key = (resolved_universe, resolved_window, resolved_max_rows)

        cached = self._read_snapshot(cache_key)
        if cached is not None:
            return cached

        async with self._lock:
            cached = self._read_snapshot(cache_key)
            if cached is not None:
                return cached
            task = self._inflight
            if task is None:
                task = asyncio.create_task(
                    self._refresh(cache_key=cache_key),
                    name="shared_market_discovery_refresh",
                )
                self._inflight = task

        try:
            markets = await task
        finally:
            async with self._lock:
                if self._inflight is task:
                    self._inflight = None
        return list(markets)

    def _read_snapshot(self, cache_key: tuple[UniverseFilter, DurationWindow, int]) -> list[MarketMetadata] | None:
        if self._snapshot is None or self._snapshot_key != cache_key:
            return None
        fetched_at_s = self._snapshot_fetched_at_s
        if fetched_at_s is None:
            return None
        if self.clock_s() - fetched_at_s > max(self.refresh_interval_seconds, 0.0):
            return None
        return list(self._snapshot)

    async def _refresh(self, *, cache_key: tuple[UniverseFilter, DurationWindow, int]) -> list[MarketMetadata]:
        universe_filter, duration_window, max_rows = cache_key
        markets = await self.discovery_fn(universe_filter, duration_window, max_rows)
        self._snapshot = list(markets)
        self._snapshot_key = cache_key
        self._snapshot_fetched_at_s = self.clock_s()
        self.logger.info("shared_market_discovery_refreshed", eligible_markets=len(markets), max_rows=max_rows)
        return list(markets)


__all__ = ["SharedMarketDiscovery"]
