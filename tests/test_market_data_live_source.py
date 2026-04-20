from __future__ import annotations

import asyncio
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.core import EventType, MarketStateUpdate, MarketStatus
from short_horizon.market_data import LiveEventSource, expand_market_state_update, extract_market_token_ids


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


class _AsyncEventStream:
    def __init__(self, events):
        self._events = list(events)
        self._queue: asyncio.Queue[object] = asyncio.Queue()
        self._sentinel = object()

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self._queue.get()
        if item is self._sentinel:
            raise StopAsyncIteration
        return item

    async def start(self) -> None:
        for event in self._events:
            await self._queue.put(event)
        await self._queue.put(self._sentinel)

    async def stop(self) -> None:
        await self._queue.put(self._sentinel)


class _FakeWebsocket:
    def __init__(self, messages=None):
        self._messages: asyncio.Queue[object] = asyncio.Queue()
        self.connected = False
        self.closed = False
        self.subscribe_calls: list[list[str]] = []
        self.unsubscribe_calls: list[list[str]] = []
        for message in messages or []:
            self._messages.put_nowait(message)

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def subscribe(self, token_ids):
        self.subscribe_calls.append(list(token_ids))

    async def unsubscribe(self, token_ids):
        self.unsubscribe_calls.append(list(token_ids))

    async def recv(self):
        return await self._messages.get()


class LiveSourceHelpersTest(unittest.TestCase):
    def test_expand_market_state_update_fans_out_per_token(self) -> None:
        event = MarketStateUpdate(
            event_time_ms=1000,
            ingest_time_ms=1001,
            market_id="m1",
            token_yes_id="tok_yes",
            token_no_id="tok_no",
            condition_id="c1",
            question="Bitcoin Up or Down?",
            status=MarketStatus.ACTIVE,
            fee_rate_bps=35.0,
            fee_fetched_at_ms=1001,
            fees_enabled=True,
            asset_slug="bitcoin",
            is_active=True,
            metadata_is_fresh=True,
            fee_metadata_age_ms=0,
            source="polymarket.gamma.refresh",
        )

        expanded = expand_market_state_update(event)

        self.assertEqual(len(expanded), 2)
        self.assertEqual({item.token_id for item in expanded}, {"tok_yes", "tok_no"})
        self.assertTrue(all(item.market_id == "m1" for item in expanded))
        self.assertTrue(all(item.fee_rate_bps == 35.0 for item in expanded))
        self.assertTrue(all(item.fee_fetched_at_ms == 1001 for item in expanded))

    def test_extract_market_token_ids_includes_compatibility_token_id(self) -> None:
        event = MarketStateUpdate(
            event_time_ms=1000,
            ingest_time_ms=1001,
            market_id="m1",
            token_yes_id="tok_yes",
            token_no_id="tok_no",
            token_id="tok_yes",
        )
        self.assertEqual(extract_market_token_ids(event), {"tok_yes", "tok_no"})


class LiveEventSourceTest(unittest.IsolatedAsyncioTestCase):
    async def test_live_event_source_merges_refresh_and_ws_streams(self) -> None:
        market_events = [
            MarketStateUpdate(
                event_time_ms=1000,
                ingest_time_ms=1000,
                market_id="m1",
                condition_id="c1",
                question="Bitcoin Up or Down?",
                status=MarketStatus.ACTIVE,
                token_yes_id="tok_yes",
                token_no_id="tok_no",
                fee_rate_bps=35.0,
                fee_fetched_at_ms=1000,
                fees_enabled=True,
                asset_slug="bitcoin",
                is_active=True,
                metadata_is_fresh=True,
                fee_metadata_age_ms=0,
                source="polymarket.gamma.refresh",
            ),
            MarketStateUpdate(
                event_time_ms=2000,
                ingest_time_ms=2000,
                market_id="m1",
                condition_id="c1",
                question="Bitcoin Up or Down?",
                status=MarketStatus.CLOSED,
                token_yes_id="tok_yes",
                token_no_id="tok_no",
                fee_rate_bps=35.0,
                fee_fetched_at_ms=2000,
                fees_enabled=True,
                asset_slug="bitcoin",
                is_active=False,
                metadata_is_fresh=True,
                fee_metadata_age_ms=0,
                source="polymarket.gamma.refresh",
            ),
        ]
        fee_events = [
            MarketStateUpdate(
                event_time_ms=1100,
                ingest_time_ms=1100,
                market_id="m1",
                condition_id="c1",
                question="Bitcoin Up or Down?",
                status=MarketStatus.ACTIVE,
                token_yes_id="tok_yes",
                token_no_id="tok_no",
                fee_rate_bps=35.0,
                fee_fetched_at_ms=1100,
                fees_enabled=True,
                asset_slug="bitcoin",
                is_active=True,
                metadata_is_fresh=True,
                fee_metadata_age_ms=0,
                source="polymarket.gamma.fee_refresh",
            )
        ]
        websocket = _FakeWebsocket(
            messages=[
                json.dumps(
                    {
                        "event_type": "best_bid_ask",
                        "market": "m1",
                        "asset_id": "tok_yes",
                        "best_bid": "0.54",
                        "best_ask": "0.55",
                        "timestamp": "1200",
                    }
                ),
                json.dumps(
                    {
                        "event_type": "last_trade_price",
                        "market": "m1",
                        "asset_id": "tok_yes",
                        "price": "0.55",
                        "side": "BUY",
                        "size": "10",
                        "timestamp": "1300",
                    }
                ),
            ]
        )
        source = LiveEventSource(
            market_refresh=_AsyncEventStream(market_events),
            fee_refresh=_AsyncEventStream(fee_events),
            websocket=websocket,
            clock_ms=lambda: 5000,
        )

        await source.start()
        try:
            emitted = [await asyncio.wait_for(source.__anext__(), timeout=1.0) for _ in range(8)]
        finally:
            await source.stop()

        market_state_events = [event for event in emitted if event.event_type == EventType.MARKET_STATE_UPDATE]
        book_events = [event for event in emitted if event.event_type == EventType.BOOK_UPDATE]
        trade_events = [event for event in emitted if event.event_type == EventType.TRADE_TICK]

        self.assertEqual(len(market_state_events), 6)
        self.assertEqual({event.token_id for event in market_state_events[:2]}, {"tok_yes", "tok_no"})
        self.assertTrue(any(event.source == "polymarket.gamma.fee_refresh" for event in market_state_events))
        self.assertTrue(any(event.status == MarketStatus.CLOSED for event in market_state_events))

        self.assertEqual(len(book_events), 1)
        self.assertEqual(book_events[0].token_id, "tok_yes")
        self.assertEqual(book_events[0].best_ask, 0.55)
        self.assertEqual(book_events[0].ingest_time_ms, 5000)

        self.assertEqual(len(trade_events), 1)
        self.assertEqual(trade_events[0].token_id, "tok_yes")
        self.assertEqual(trade_events[0].price, 0.55)
        self.assertEqual(trade_events[0].ingest_time_ms, 5000)

        self.assertTrue(websocket.connected)
        self.assertTrue(websocket.closed)
        self.assertEqual(websocket.subscribe_calls, [["tok_no", "tok_yes"]])
        self.assertEqual(websocket.unsubscribe_calls, [["tok_no", "tok_yes"]])

    async def test_live_event_source_handles_decoded_batch_websocket_payload(self) -> None:
        fixture_path = FIXTURES_DIR / "clob_ws_mixed_batch.json"
        websocket = _FakeWebsocket(messages=[json.loads(fixture_path.read_text(encoding="utf-8"))])
        source = LiveEventSource(
            market_refresh=_AsyncEventStream([]),
            fee_refresh=_AsyncEventStream([]),
            websocket=websocket,
            clock_ms=lambda: 9000,
        )

        await source.start()
        try:
            emitted = [await asyncio.wait_for(source.__anext__(), timeout=1.0) for _ in range(3)]
        finally:
            await source.stop()

        self.assertEqual([event.event_type for event in emitted], [EventType.BOOK_UPDATE, EventType.BOOK_UPDATE, EventType.TRADE_TICK])
        self.assertTrue(all(event.market_id == "m1" for event in emitted))
        self.assertTrue(all(event.token_id == "tok_yes" for event in emitted))
        self.assertTrue(all(event.ingest_time_ms == 9000 for event in emitted))


if __name__ == "__main__":
    unittest.main()
