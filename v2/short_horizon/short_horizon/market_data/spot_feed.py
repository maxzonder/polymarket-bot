from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from ..core.events import SpotPriceUpdate


BINANCE_TICKER_BASE = "https://api.binance.com"


@dataclass(frozen=True)
class BinanceSpotSymbol:
    asset_slug: str
    symbol: str


DEFAULT_BINANCE_SPOT_SYMBOLS: tuple[BinanceSpotSymbol, ...] = (
    BinanceSpotSymbol("btc", "BTCUSDT"),
    BinanceSpotSymbol("eth", "ETHUSDT"),
    BinanceSpotSymbol("sol", "SOLUSDT"),
    BinanceSpotSymbol("xrp", "XRPUSDT"),
)


class BinanceSpotPriceFeed:
    """Small polling feed for live P6 spot-dislocation dry-runs.

    It intentionally emits normalized `SpotPriceUpdate` events and leaves all
    strategy decisions to runtime code. The feed uses Binance ticker prices as a
    high-resolution live proxy; historical/offline evaluation still uses 1s
    klines.
    """

    def __init__(
        self,
        *,
        symbols: tuple[BinanceSpotSymbol, ...] = DEFAULT_BINANCE_SPOT_SYMBOLS,
        poll_interval_seconds: float = 1.0,
        binance_base: str = BINANCE_TICKER_BASE,
        timeout_seconds: float = 5.0,
        clock_ms: callable | None = None,
    ) -> None:
        self.symbols = tuple(symbols)
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.binance_base = binance_base.rstrip("/")
        self.timeout_seconds = float(timeout_seconds)
        self.clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._queue: asyncio.Queue[SpotPriceUpdate | object] = asyncio.Queue()
        self._sentinel = object()
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self._failed: BaseException | None = None

    @property
    def events(self):
        return self

    def __aiter__(self):
        return self

    async def __anext__(self) -> SpotPriceUpdate:
        item = await self._queue.get()
        if item is self._sentinel:
            raise StopAsyncIteration
        return item  # type: ignore[return-value]

    async def start(self) -> None:
        if self._task is not None:
            return
        self._closed = False
        self._task = asyncio.create_task(self._run(), name="binance_spot_price_feed")

    async def stop(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._queue.put(self._sentinel)

    async def close(self) -> None:
        await self.stop()

    def failed(self) -> bool:
        return self._failed is not None

    def failure_exception(self) -> BaseException | None:
        return self._failed

    async def recv(self) -> SpotPriceUpdate:
        return await self.__anext__()

    async def _run(self) -> None:
        try:
            while not self._closed:
                started = time.monotonic()
                for item in self.symbols:
                    event = await asyncio.to_thread(
                        fetch_binance_ticker_price,
                        item,
                        binance_base=self.binance_base,
                        timeout_seconds=self.timeout_seconds,
                        clock_ms=self.clock_ms,
                    )
                    await self._queue.put(event)
                elapsed = time.monotonic() - started
                await asyncio.sleep(max(0.0, self.poll_interval_seconds - elapsed))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - exercised by live component monitor
            self._failed = exc
            await self._queue.put(self._sentinel)


def fetch_binance_ticker_price(
    item: BinanceSpotSymbol,
    *,
    binance_base: str = BINANCE_TICKER_BASE,
    timeout_seconds: float = 5.0,
    clock_ms: callable | None = None,
) -> SpotPriceUpdate:
    clock_ms = clock_ms or (lambda: int(time.time() * 1000))
    ingest_ms = int(clock_ms())
    query = urlencode({"symbol": item.symbol})
    url = f"{binance_base.rstrip('/')}/api/v3/ticker/price?{query}"
    with urlopen(url, timeout=timeout_seconds) as response:  # nosec B310 - public Binance HTTPS endpoint
        payload: dict[str, Any] = json.loads(response.read().decode("utf-8"))
    price = float(payload["price"])
    return SpotPriceUpdate(
        event_time_ms=ingest_ms,
        ingest_time_ms=ingest_ms,
        source="binance.ticker",
        asset_slug=item.asset_slug,
        spot_price=price,
        venue="binance",
    )


__all__ = ["BINANCE_TICKER_BASE", "BinanceSpotPriceFeed", "BinanceSpotSymbol", "DEFAULT_BINANCE_SPOT_SYMBOLS", "fetch_binance_ticker_price"]
