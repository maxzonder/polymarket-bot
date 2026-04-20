from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.venue_polymarket import (
    ExecutionClientConfigError,
    PolymarketExecutionClient,
    VenueOrderRequest,
)


class _FakeVenueClient:
    def __init__(self, *, host, key, chain_id):
        self.host = host
        self.key = key
        self.chain_id = chain_id
        self.api_creds = None
        self.create_and_post_order_calls = []
        self.cancel_calls = []
        self.get_order_calls = []
        self.get_orders_calls = []

    def create_or_derive_api_creds(self):
        return {"api_key": "derived"}

    def set_api_creds(self, api_creds):
        self.api_creds = api_creds

    def create_and_post_order(self, order_args):
        self.create_and_post_order_calls.append(order_args)
        return {"orderID": "venue-123", "status": "live"}

    def cancel(self, order_id):
        self.cancel_calls.append(order_id)
        return {"success": True, "status": "canceled"}

    def get_order(self, order_id):
        self.get_order_calls.append(order_id)
        return {
            "id": order_id,
            "status": "ORDER_STATUS_LIVE",
            "asset_id": "tok_yes",
            "side": "BUY",
            "price": "0.55",
            "size": "18.181818",
            "matched_size": "0",
        }

    def get_orders(self, **kwargs):
        self.get_orders_calls.append(kwargs)
        return [
            {"id": "ord_live", "status": "ORDER_STATUS_LIVE", "asset_id": "tok_yes", "size": "10", "matched_size": "0"},
            {"id": "ord_dead", "status": "ORDER_STATUS_CANCELED", "asset_id": "tok_yes", "size": "10", "matched_size": "0"},
        ]


class _FakeOrderArgs:
    def __init__(self, *, price, size, side, token_id, client_order_id=None, time_in_force=None, post_only=None):
        self.kwargs = {
            "price": price,
            "size": size,
            "side": side,
            "token_id": token_id,
            "client_order_id": client_order_id,
            "time_in_force": time_in_force,
            "post_only": post_only,
        }


class VenuePolymarketExecutionClientTest(unittest.TestCase):
    def test_startup_uses_env_private_key_and_healthchecks(self) -> None:
        captured = {}

        def client_factory(*, host, key, chain_id):
            captured["host"] = host
            captured["key"] = key
            captured["chain_id"] = chain_id
            captured["client"] = _FakeVenueClient(host=host, key=key, chain_id=chain_id)
            return captured["client"]

        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            client = PolymarketExecutionClient(client_factory=client_factory, order_args_factory=_FakeOrderArgs)
            client.startup()

        self.assertEqual(captured["host"], "https://clob.polymarket.com")
        self.assertEqual(captured["key"], "env-secret")
        self.assertEqual(captured["chain_id"], 137)
        self.assertEqual(captured["client"].api_creds, {"api_key": "derived"})
        self.assertEqual(captured["client"].get_orders_calls, [{}])

    def test_missing_env_private_key_raises_before_initialization(self) -> None:
        calls = []

        def client_factory(*, host, key, chain_id):
            calls.append((host, key, chain_id))
            return _FakeVenueClient(host=host, key=key, chain_id=chain_id)

        with patch.dict(os.environ, {}, clear=True):
            client = PolymarketExecutionClient(client_factory=client_factory, order_args_factory=_FakeOrderArgs)
            with self.assertRaises(ExecutionClientConfigError) as ctx:
                client.startup()

        self.assertIn("POLY_PRIVATE_KEY", str(ctx.exception))
        self.assertEqual(calls, [])

    def test_place_order_translates_request_into_sdk_kwargs(self) -> None:
        fake_client = _FakeVenueClient(host="https://clob.polymarket.com", key="env-secret", chain_id=137)
        client = PolymarketExecutionClient(
            client_factory=lambda **kwargs: fake_client,
            order_args_factory=_FakeOrderArgs,
        )
        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            client.startup()

        result = client.place_order(
            VenueOrderRequest(
                token_id="tok_yes",
                side="BUY",
                price=0.55,
                size=18.181818,
                client_order_id="client-1",
                time_in_force="GTC",
                post_only=False,
            )
        )

        self.assertEqual(result.order_id, "venue-123")
        self.assertEqual(result.status, "live")
        self.assertEqual(result.client_order_id, "client-1")
        sent = fake_client.create_and_post_order_calls[0]
        self.assertEqual(
            sent.kwargs,
            {
                "price": 0.55,
                "size": 18.181818,
                "side": "BUY",
                "token_id": "tok_yes",
                "client_order_id": "client-1",
                "time_in_force": "GTC",
                "post_only": False,
            },
        )

    def test_list_open_orders_filters_terminal_statuses(self) -> None:
        fake_client = _FakeVenueClient(host="https://clob.polymarket.com", key="env-secret", chain_id=137)
        client = PolymarketExecutionClient(
            client_factory=lambda **kwargs: fake_client,
            order_args_factory=_FakeOrderArgs,
        )
        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            client.startup()

        orders = client.list_open_orders(market_id="market-1")

        self.assertEqual([order.order_id for order in orders], ["ord_live"])
        self.assertEqual(fake_client.get_orders_calls[-1], {"market": "market-1"})

    @unittest.skipUnless(os.getenv("POLYMARKET_RUN_VENUE_TESTS") == "1", "set POLYMARKET_RUN_VENUE_TESTS=1 to hit real venue")
    def test_startup_and_list_open_orders_against_real_venue(self) -> None:
        try:
            import py_clob_client  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            self.skipTest(f"py-clob-client unavailable: {exc}")

        client = PolymarketExecutionClient()
        client.startup()
        orders = client.list_open_orders()
        self.assertIsInstance(orders, list)


if __name__ == "__main__":
    unittest.main()
