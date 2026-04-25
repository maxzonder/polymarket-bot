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

from short_horizon.core import EventType, OrderAccepted, OrderCanceled, OrderFilled, OrderSide
from short_horizon.venue_polymarket import PolymarketUserStream, UserStreamNormalizer, VenueApiCredentials


class _FakeWebsocket:
    def __init__(
        self,
        messages: list[str],
        *,
        disconnect_after_messages: bool = False,
        fail_send: bool = False,
        closed: bool = False,
    ):
        self._messages = list(messages)
        self._disconnect_after_messages = disconnect_after_messages
        self._raised_disconnect = False
        self._fail_send = fail_send
        self._closed_event = asyncio.Event()
        self.entered = asyncio.Event()
        self.sent: list[str] = []
        self.closed = closed

    async def send(self, data: str) -> None:
        if self._fail_send:
            raise RuntimeError("send failed")
        self.sent.append(data)

    async def close(self) -> None:
        self.closed = True
        self._closed_event.set()

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if self._messages:
            return self._messages.pop(0)
        if self._disconnect_after_messages and not self._raised_disconnect:
            self._raised_disconnect = True
            raise RuntimeError("disconnect")
        await self._closed_event.wait()
        raise StopAsyncIteration


class _FakeConnectContext:
    def __init__(self, websocket: _FakeWebsocket):
        self.websocket = websocket

    async def __aenter__(self) -> _FakeWebsocket:
        self.websocket.entered.set()
        return self.websocket

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeConnectFactory:
    def __init__(self, sockets: list[_FakeWebsocket]):
        self._sockets = list(sockets)
        self.calls: list[str] = []

    def __call__(self, endpoint: str) -> _FakeConnectContext:
        self.calls.append(endpoint)
        return _FakeConnectContext(self._sockets.pop(0))


class VenuePolymarketUserStreamTest(unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_replays_authenticated_market_subscription(self) -> None:
        placement_1 = json.dumps(
            {
                "event_type": "order",
                "id": "ord-1",
                "market": "c1",
                "asset_id": "tok_yes",
                "side": "BUY",
                "price": "0.55",
                "original_size": "10",
                "size_matched": "0",
                "timestamp": "1000",
                "type": "PLACEMENT",
            }
        )
        placement_2 = json.dumps(
            {
                "event_type": "order",
                "id": "ord-2",
                "market": "c1",
                "asset_id": "tok_yes",
                "side": "SELL",
                "price": "0.56",
                "original_size": "5",
                "size_matched": "0",
                "timestamp": "1001",
                "type": "PLACEMENT",
            }
        )
        ws1 = _FakeWebsocket([placement_1], disconnect_after_messages=True)
        ws2 = _FakeWebsocket([placement_2])
        factory = _FakeConnectFactory([ws1, ws2])
        stream = PolymarketUserStream(
            auth=VenueApiCredentials(api_key="key", secret="secret", passphrase="pass"),
            connect_factory=factory,
            backoff_initial_seconds=0.01,
            backoff_max_seconds=0.02,
            ping_interval_seconds=None,
            clock_ms=lambda: 5000,
        )

        await stream.subscribe(["c2", "c1"])
        await stream.connect()

        await asyncio.wait_for(ws1.entered.wait(), timeout=0.2)
        first_event = await asyncio.wait_for(stream.recv(), timeout=0.2)
        self.assertIsInstance(first_event, OrderAccepted)
        self.assertEqual(first_event.order_id, "ord-1")

        await asyncio.wait_for(ws2.entered.wait(), timeout=0.2)
        second_event = await asyncio.wait_for(stream.recv(), timeout=0.2)
        self.assertIsInstance(second_event, OrderAccepted)
        self.assertEqual(second_event.order_id, "ord-2")

        self.assertEqual(len(ws1.sent), 1)
        self.assertEqual(len(ws2.sent), 1)
        self.assertEqual(
            json.loads(ws1.sent[0]),
            {
                "auth": {"apiKey": "key", "secret": "secret", "passphrase": "pass"},
                "markets": ["c1", "c2"],
                "type": "user",
            },
        )
        self.assertEqual(json.loads(ws2.sent[0])["markets"], ["c1", "c2"])
        self.assertEqual(len(factory.calls), 2)

        await stream.close()

    async def test_unsubscribe_sends_explicit_unsubscribe_message(self) -> None:
        ws = _FakeWebsocket([])
        factory = _FakeConnectFactory([ws])
        stream = PolymarketUserStream(
            auth=VenueApiCredentials(api_key="key", secret="secret", passphrase="pass"),
            connect_factory=factory,
            backoff_initial_seconds=0.01,
            backoff_max_seconds=0.02,
            ping_interval_seconds=None,
        )

        await stream.connect()
        await asyncio.wait_for(ws.entered.wait(), timeout=0.2)
        await stream.subscribe(["c1", "c2", "c3"])
        await stream.unsubscribe(["c2"])

        self.assertEqual(len(ws.sent), 3)
        self.assertEqual(json.loads(ws.sent[0])["markets"], [])
        self.assertEqual(json.loads(ws.sent[1])["markets"], ["c1", "c2", "c3"])
        self.assertEqual(json.loads(ws.sent[2]), {"markets": ["c2"], "operation": "unsubscribe"})

        await stream.close()

    async def test_subscribe_send_failure_is_best_effort_and_replayed_on_reconnect(self) -> None:
        ws = _FakeWebsocket([], fail_send=True)
        stream = PolymarketUserStream(
            auth=VenueApiCredentials(api_key="key", secret="secret", passphrase="pass"),
            backoff_initial_seconds=0.01,
            backoff_max_seconds=0.02,
            ping_interval_seconds=None,
        )
        stream._ws = ws

        await stream.subscribe(["c1", "c2"])

        self.assertEqual(stream._subscribed_markets, {"c1", "c2"})
        self.assertEqual(stream._subscription_send_failure_count, 1)

        ws2 = _FakeWebsocket([])
        stream = PolymarketUserStream(
            auth=VenueApiCredentials(api_key="key", secret="secret", passphrase="pass"),
            connect_factory=_FakeConnectFactory([ws2]),
            backoff_initial_seconds=0.01,
            backoff_max_seconds=0.02,
            ping_interval_seconds=None,
        )
        stream._subscribed_markets.update(["c1", "c2"])
        await stream.connect()
        await asyncio.wait_for(ws2.entered.wait(), timeout=0.2)
        self.assertEqual(json.loads(ws2.sent[0])["markets"], ["c1", "c2"])
        await stream.close()

    async def test_unsubscribe_send_failure_is_best_effort(self) -> None:
        ws = _FakeWebsocket([], fail_send=True)
        stream = PolymarketUserStream(
            auth=VenueApiCredentials(api_key="key", secret="secret", passphrase="pass"),
            backoff_initial_seconds=0.01,
            backoff_max_seconds=0.02,
            ping_interval_seconds=None,
        )
        stream._ws = ws
        stream._subscribed_markets.update(["c1", "c2"])

        await stream.unsubscribe(["c2"])

        self.assertEqual(stream._subscribed_markets, {"c1"})
        self.assertEqual(stream._subscription_send_failure_count, 1)
        await stream.close()

    async def test_subscribe_skips_closed_connection_without_failure_count(self) -> None:
        ws = _FakeWebsocket([], fail_send=True, closed=True)
        stream = PolymarketUserStream(
            auth=VenueApiCredentials(api_key="key", secret="secret", passphrase="pass"),
            backoff_initial_seconds=0.01,
            backoff_max_seconds=0.02,
            ping_interval_seconds=None,
        )
        stream._ws = ws

        await stream.subscribe(["c1"])

        self.assertEqual(stream._subscribed_markets, {"c1"})
        self.assertEqual(stream._subscription_send_failure_count, 0)
        await stream.close()

    async def test_resubscribe_current_markets_remains_fail_fast_inside_reconnect_loop(self) -> None:
        ws = _FakeWebsocket([], fail_send=True)
        stream = PolymarketUserStream(
            auth=VenueApiCredentials(api_key="key", secret="secret", passphrase="pass"),
            connect_factory=_FakeConnectFactory([ws]),
            backoff_initial_seconds=0.01,
            backoff_max_seconds=0.02,
            ping_interval_seconds=None,
        )
        stream._ws = ws
        stream._subscribed_markets.update(["c1"])

        with self.assertRaises(RuntimeError):
            await stream._resubscribe_current_markets()


class UserStreamNormalizerTest(unittest.TestCase):
    def test_order_lifecycle_emits_accept_fill_and_cancel(self) -> None:
        normalizer = UserStreamNormalizer()

        accepted = normalizer.normalize_event(
            {
                "event_type": "order",
                "id": "ord-1",
                "market": "m1",
                "asset_id": "tok_yes",
                "side": "BUY",
                "price": "0.55",
                "original_size": "10",
                "size_matched": "0",
                "timestamp": "1000",
                "type": "PLACEMENT",
            },
            ingest_time_ms=5000,
        )
        fill = normalizer.normalize_event(
            {
                "event_type": "order",
                "id": "ord-1",
                "market": "m1",
                "asset_id": "tok_yes",
                "side": "BUY",
                "price": "0.55",
                "original_size": "10",
                "size_matched": "4",
                "timestamp": "1010",
                "type": "UPDATE",
            },
            ingest_time_ms=5001,
        )
        duplicate_fill = normalizer.normalize_event(
            {
                "event_type": "order",
                "id": "ord-1",
                "market": "m1",
                "asset_id": "tok_yes",
                "side": "BUY",
                "price": "0.55",
                "original_size": "10",
                "size_matched": "4",
                "timestamp": "1011",
                "type": "UPDATE",
            },
            ingest_time_ms=5002,
        )
        canceled = normalizer.normalize_event(
            {
                "event_type": "order",
                "id": "ord-1",
                "market": "m1",
                "asset_id": "tok_yes",
                "side": "BUY",
                "price": "0.55",
                "original_size": "10",
                "size_matched": "4",
                "timestamp": "1020",
                "type": "CANCELLATION",
            },
            ingest_time_ms=5003,
        )

        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0].event_type, EventType.ORDER_ACCEPTED)
        self.assertEqual(accepted[0].size, 10.0)

        self.assertEqual(len(fill), 1)
        self.assertIsInstance(fill[0], OrderFilled)
        self.assertEqual(fill[0].fill_size, 4.0)
        self.assertEqual(fill[0].cumulative_filled_size, 4.0)
        self.assertEqual(fill[0].remaining_size, 6.0)
        self.assertEqual(duplicate_fill, [])

        self.assertEqual(len(canceled), 1)
        self.assertIsInstance(canceled[0], OrderCanceled)
        self.assertEqual(canceled[0].cumulative_filled_size, 4.0)
        self.assertEqual(canceled[0].remaining_size, 6.0)

    def test_trade_match_backfills_fill_when_order_state_missing(self) -> None:
        normalizer = UserStreamNormalizer()

        fill = normalizer.normalize_event(
            {
                "event_type": "trade",
                "id": "trade-1",
                "market": "m1",
                "asset_id": "tok_yes",
                "price": "0.57",
                "size": "3",
                "side": "BUY",
                "status": "MATCHED",
                "timestamp": "2000",
                "taker_order_id": "ord-taker-1",
                "maker_orders": [],
            },
            ingest_time_ms=7000,
        )
        duplicate = normalizer.normalize_event(
            {
                "event_type": "trade",
                "id": "trade-1",
                "market": "m1",
                "asset_id": "tok_yes",
                "price": "0.57",
                "size": "3",
                "side": "BUY",
                "status": "MATCHED",
                "timestamp": "2001",
                "taker_order_id": "ord-taker-1",
                "maker_orders": [],
            },
            ingest_time_ms=7001,
        )

        self.assertEqual(len(fill), 1)
        self.assertIsInstance(fill[0], OrderFilled)
        self.assertEqual(fill[0].order_id, "ord-taker-1")
        self.assertEqual(fill[0].side, OrderSide.BUY)
        self.assertEqual(fill[0].fill_size, 3.0)
        self.assertEqual(fill[0].remaining_size, 0.0)
        self.assertEqual(duplicate, [])


if __name__ == "__main__":
    unittest.main()
