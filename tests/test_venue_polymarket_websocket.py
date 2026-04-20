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

from short_horizon.venue_polymarket import PolymarketWebsocket


class _FakeWebsocket:
    def __init__(self, messages: list[str], *, disconnect_after_messages: bool = False):
        self._messages = list(messages)
        self._disconnect_after_messages = disconnect_after_messages
        self._raised_disconnect = False
        self._closed_event = asyncio.Event()
        self.entered = asyncio.Event()
        self.sent: list[str] = []
        self.closed = False

    async def send(self, data: str) -> None:
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


class VenuePolymarketWebsocketTest(unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_replays_subscribed_token_set(self) -> None:
        ws1 = _FakeWebsocket(['{"event_type":"book"}'], disconnect_after_messages=True)
        ws2 = _FakeWebsocket(['{"event_type":"best_bid_ask"}'])
        factory = _FakeConnectFactory([ws1, ws2])
        manager = PolymarketWebsocket(
            connect_factory=factory,
            backoff_initial_seconds=0.01,
            backoff_max_seconds=0.02,
            ping_interval_seconds=None,
        )

        await manager.subscribe(["tok_b", "tok_a"])
        await manager.connect()

        await asyncio.wait_for(ws1.entered.wait(), timeout=0.2)
        first_message = await asyncio.wait_for(manager.recv(), timeout=0.2)
        self.assertEqual(json.loads(first_message)["event_type"], "book")

        await asyncio.wait_for(ws2.entered.wait(), timeout=0.2)
        second_message = await asyncio.wait_for(manager.recv(), timeout=0.2)
        self.assertEqual(json.loads(second_message)["event_type"], "best_bid_ask")

        self.assertEqual(len(ws1.sent), 1)
        self.assertEqual(len(ws2.sent), 1)
        self.assertEqual(json.loads(ws1.sent[0])["assets_ids"], ["tok_a", "tok_b"])
        self.assertEqual(json.loads(ws2.sent[0])["assets_ids"], ["tok_a", "tok_b"])
        self.assertEqual(len(factory.calls), 2)

        await manager.close()

    async def test_unsubscribe_sends_remaining_subscription_set(self) -> None:
        ws = _FakeWebsocket([])
        factory = _FakeConnectFactory([ws])
        manager = PolymarketWebsocket(
            connect_factory=factory,
            backoff_initial_seconds=0.01,
            backoff_max_seconds=0.02,
            ping_interval_seconds=None,
        )

        await manager.connect()
        await asyncio.wait_for(ws.entered.wait(), timeout=0.2)
        await manager.subscribe(["tok_a", "tok_b", "tok_c"])
        await manager.unsubscribe(["tok_b"])

        self.assertEqual(len(ws.sent), 2)
        self.assertEqual(json.loads(ws.sent[0])["assets_ids"], ["tok_a", "tok_b", "tok_c"])
        self.assertEqual(json.loads(ws.sent[1])["assets_ids"], ["tok_a", "tok_c"])

        await manager.close()


if __name__ == "__main__":
    unittest.main()
