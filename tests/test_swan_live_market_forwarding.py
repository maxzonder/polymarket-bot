from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_V2 = _REPO / "v2" / "short_horizon"
for p in (_REPO, _V2):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from short_horizon.core.clock import SystemClock
from short_horizon.core.events import BookUpdate, MarketStateUpdate, TradeTick
from short_horizon.market_data import LiveEventSource
from v2.short_horizon.swan_live import _WsCandidateCache, _ws_price_monitor_loop


class _FakeWs:
    def __init__(self, shutdown: asyncio.Event):
        self.shutdown = shutdown
        self.sent = False

    async def recv(self):
        if self.sent:
            await asyncio.sleep(0.01)
            return ""
        self.sent = True
        self.shutdown.set()
        return [
            {
                "event_type": "best_bid_ask",
                "market": "cond-1",
                "asset_id": "tok_yes",
                "timestamp": "230000",
                "best_bid": "0.04",
                "best_ask": "0.06",
            },
            {
                "event_type": "last_trade_price",
                "market": "cond-1",
                "asset_id": "tok_yes",
                "timestamp": "230001",
                "price": "0.005",
                "size": "12",
                "side": "SELL",
                "trade_id": "trade-1",
            },
        ]


class _NoopLogger:
    def exception(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass


class SwanLiveMarketForwardingTest(unittest.TestCase):
    def test_price_monitor_forwards_book_and_trade_events_to_runtime_source(self) -> None:
        async def run() -> list[object]:
            shutdown = asyncio.Event()
            source = LiveEventSource(websocket=object())
            market = MarketStateUpdate(
                event_time_ms=200_000,
                ingest_time_ms=200_050,
                market_id="m1",
                condition_id="cond-1",
                question="Q?",
                token_yes_id="tok_yes",
                token_no_id="tok_no",
                is_active=True,
            )
            source._register_market_event(market)

            await _ws_price_monitor_loop(
                ws=_FakeWs(shutdown),
                screener=object(),
                strategy=object(),
                source=source,
                clock=SystemClock(),
                stake_usdc_per_level=1.0,
                duration_stake_multipliers=(),
                price_threshold=0.01,
                token_to_market_id={"tok_yes": "m1"},
                token_to_side_index={"tok_yes": 0},
                candidate_cache=_WsCandidateCache(),
                logger=_NoopLogger(),
                shutdown=shutdown,
                forward_market_events=True,
            )
            events: list[object] = []
            while not source._queue.empty():
                events.append(source._queue.get_nowait())
            return events

        events = asyncio.run(run())

        self.assertTrue(any(isinstance(event, BookUpdate) for event in events))
        trades = [event for event in events if isinstance(event, TradeTick)]
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].market_id, "m1")
        self.assertEqual(trades[0].token_id, "tok_yes")
