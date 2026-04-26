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
    MAX_UINT256,
    DEFAULT_DATA_API_HOST,
    POLYGON_NATIVE_USDC_TOKEN,
    POLYMARKET_CTF_TOKEN,
    POLYMARKET_SPENDER_ADDRESSES,
    POLYMARKET_USDC_TOKEN,
    ExecutionClientConfigError,
    PolymarketExecutionClient,
    PolygonUsdcBridgeResult,
    ZERO_BYTES32,
    VenueApiCredentials,
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
        self.get_open_orders_calls = []

    def create_or_derive_api_key(self):
        return {"api_key": "derived", "secret": "derived-secret", "passphrase": "derived-pass"}

    def set_api_creds(self, api_creds):
        self.api_creds = api_creds

    def create_and_post_order(self, order_args):
        self.create_and_post_order_calls.append(order_args)
        return {"orderID": "venue-123", "status": "live"}

    def cancel_order(self, payload):
        self.cancel_calls.append(payload)
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

    def get_open_orders(self, params=None, only_first_page=False, next_cursor=None):
        self.get_open_orders_calls.append(params)
        return [
            {"id": "ord_live", "status": "ORDER_STATUS_LIVE", "asset_id": "tok_yes", "size": "10", "matched_size": "0"},
            {"id": "ord_dead", "status": "ORDER_STATUS_CANCELED", "asset_id": "tok_yes", "size": "10", "matched_size": "0"},
        ]


class _ApiCredsShape:
    def __init__(self, *, api_key: str, api_secret: str, api_passphrase: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase


class _FakeVenueClientApiCredsObject(_FakeVenueClient):
    def create_or_derive_api_key(self):
        return _ApiCredsShape(api_key="derived", api_secret="derived-secret", api_passphrase="derived-pass")


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


class _FakeOrderPayload:
    def __init__(self, *, orderID):
        self.orderID = orderID


class _FakeOpenOrderParams:
    def __init__(self, *, id=None, market=None, asset_id=None):
        self.id = id
        self.market = market
        self.asset_id = asset_id


def _build_client(client_factory, **overrides):
    return PolymarketExecutionClient(
        client_factory=client_factory,
        order_args_factory=overrides.get("order_args_factory", _FakeOrderArgs),
        cancel_payload_factory=overrides.get("cancel_payload_factory", _FakeOrderPayload),
        open_order_params_factory=overrides.get("open_order_params_factory", _FakeOpenOrderParams),
    )


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
            client = PolymarketExecutionClient(client_factory=client_factory, order_args_factory=_FakeOrderArgs, cancel_payload_factory=_FakeOrderPayload, open_order_params_factory=_FakeOpenOrderParams)
            client.startup()

        self.assertEqual(captured["host"], "https://clob.polymarket.com")
        self.assertEqual(captured["key"], "env-secret")
        self.assertEqual(captured["chain_id"], 137)
        self.assertEqual(
            captured["client"].api_creds,
            {"api_key": "derived", "secret": "derived-secret", "passphrase": "derived-pass"},
        )
        self.assertEqual(captured["client"].get_open_orders_calls, [None])

    def test_api_credentials_exposed_after_startup(self) -> None:
        fake_client = _FakeVenueClient(host="https://clob.polymarket.com", key="env-secret", chain_id=137)
        client = PolymarketExecutionClient(
            client_factory=lambda **kwargs: fake_client,
            order_args_factory=_FakeOrderArgs,
            cancel_payload_factory=_FakeOrderPayload,
            open_order_params_factory=_FakeOpenOrderParams,
        )

        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            client.startup()

        self.assertEqual(
            client.api_credentials(),
            VenueApiCredentials(api_key="derived", secret="derived-secret", passphrase="derived-pass"),
        )

    def test_startup_accepts_prod_style_api_creds_object(self) -> None:
        fake_client = _FakeVenueClientApiCredsObject(host="https://clob.polymarket.com", key="env-secret", chain_id=137)
        client = PolymarketExecutionClient(
            client_factory=lambda **kwargs: fake_client,
            order_args_factory=_FakeOrderArgs,
            cancel_payload_factory=_FakeOrderPayload,
            open_order_params_factory=_FakeOpenOrderParams,
        )

        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            client.startup()

        self.assertEqual(
            client.api_credentials(),
            VenueApiCredentials(api_key="derived", secret="derived-secret", passphrase="derived-pass"),
        )

    def test_missing_env_private_key_raises_before_initialization(self) -> None:
        calls = []

        def client_factory(*, host, key, chain_id):
            calls.append((host, key, chain_id))
            return _FakeVenueClient(host=host, key=key, chain_id=chain_id)

        with patch.dict(os.environ, {}, clear=True):
            client = PolymarketExecutionClient(client_factory=client_factory, order_args_factory=_FakeOrderArgs, cancel_payload_factory=_FakeOrderPayload, open_order_params_factory=_FakeOpenOrderParams)
            with self.assertRaises(ExecutionClientConfigError) as ctx:
                client.startup()

        self.assertIn("POLY_PRIVATE_KEY", str(ctx.exception))
        self.assertEqual(calls, [])

    def test_place_order_translates_request_into_sdk_kwargs(self) -> None:
        fake_client = _FakeVenueClient(host="https://clob.polymarket.com", key="env-secret", chain_id=137)
        client = PolymarketExecutionClient(
            client_factory=lambda **kwargs: fake_client,
            order_args_factory=_FakeOrderArgs,
            cancel_payload_factory=_FakeOrderPayload,
            open_order_params_factory=_FakeOpenOrderParams,
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
            cancel_payload_factory=_FakeOrderPayload,
            open_order_params_factory=_FakeOpenOrderParams,
        )
        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            client.startup()

        orders = client.list_open_orders(market_id="market-1")

        self.assertEqual([order.order_id for order in orders], ["ord_live"])
        last_params = fake_client.get_open_orders_calls[-1]
        self.assertIsInstance(last_params, _FakeOpenOrderParams)
        self.assertEqual(last_params.market, "market-1")

    def test_approve_allowances_submits_missing_usdc_and_conditional_approvals(self) -> None:
        client = PolymarketExecutionClient(client_factory=lambda **kwargs: _FakeVenueClient(**kwargs), order_args_factory=_FakeOrderArgs, cancel_payload_factory=_FakeOrderPayload, open_order_params_factory=_FakeOpenOrderParams)

        sent_raw = []
        tx_counter = 0

        def sign_transaction(tx):
            nonlocal tx_counter
            tx_counter += 1
            sent_raw.append(tx)
            return f"0xsigned{tx_counter}"

        def rpc_call(method, params):
            if method == "eth_call":
                return "0x0"
            if method == "eth_getTransactionCount":
                return hex(len(sent_raw))
            if method == "eth_gasPrice":
                return hex(100)
            if method == "eth_estimateGas":
                return hex(50_000)
            if method == "eth_sendRawTransaction":
                return f"0xhash{len(sent_raw)}"
            if method == "eth_getTransactionReceipt":
                return {"status": "0x1"}
            raise AssertionError(f"unexpected rpc method: {method}")

        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            results = client.approve_allowances(
                rpc_call=rpc_call,
                sign_transaction=sign_transaction,
                signer_address="0x0000000000000000000000000000000000000abc",
                sleep_func=lambda _: None,
            )

        self.assertEqual(len(results), 6)
        self.assertTrue(all(result.status == "approved" for result in results))
        self.assertEqual([result.asset_type for result in results], ["collateral", "conditional"] * 3)
        self.assertEqual([result.spender for result in results], [spender for spender in POLYMARKET_SPENDER_ADDRESSES for _ in (0, 1)])
        self.assertEqual(len(sent_raw), 6)
        self.assertEqual(sent_raw[0]["to"], POLYMARKET_USDC_TOKEN)
        self.assertEqual(sent_raw[1]["to"], POLYMARKET_CTF_TOKEN)
        self.assertIn(hex(MAX_UINT256)[2:].lower(), sent_raw[0]["data"].lower())

    def test_approve_allowances_skips_when_already_approved(self) -> None:
        client = PolymarketExecutionClient(client_factory=lambda **kwargs: _FakeVenueClient(**kwargs), order_args_factory=_FakeOrderArgs, cancel_payload_factory=_FakeOrderPayload, open_order_params_factory=_FakeOpenOrderParams)

        def rpc_call(method, params):
            if method == "eth_call":
                return "0x1"
            raise AssertionError(f"unexpected rpc method: {method}")

        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            results = client.approve_allowances(
                rpc_call=rpc_call,
                sign_transaction=lambda tx: (_ for _ in ()).throw(AssertionError("should not sign tx")),
                signer_address="0x0000000000000000000000000000000000000abc",
                sleep_func=lambda _: None,
            )

        self.assertEqual(len(results), 6)
        self.assertTrue(all(result.status == "already_approved" for result in results))

    def test_bridge_polygon_usdc_to_usdce_transfers_into_polymarket_bridge_and_waits_for_completion(self) -> None:
        client = PolymarketExecutionClient(client_factory=lambda **kwargs: _FakeVenueClient(**kwargs), order_args_factory=_FakeOrderArgs, cancel_payload_factory=_FakeOrderPayload, open_order_params_factory=_FakeOpenOrderParams)

        sent_raw = []
        statuses = [
            {"transactions": []},
            {"transactions": []},
            {
                "transactions": [
                    {
                        "fromChainId": "137",
                        "fromTokenAddress": POLYGON_NATIVE_USDC_TOKEN,
                        "fromAmountBaseUnit": "2097983",
                        "toChainId": "137",
                        "toTokenAddress": POLYMARKET_USDC_TOKEN,
                        "status": "PROCESSING",
                        "createdTimeMs": 111,
                    }
                ]
            },
            {
                "transactions": [
                    {
                        "fromChainId": "137",
                        "fromTokenAddress": POLYGON_NATIVE_USDC_TOKEN,
                        "fromAmountBaseUnit": "2097983",
                        "toChainId": "137",
                        "toTokenAddress": POLYMARKET_USDC_TOKEN,
                        "status": "COMPLETED",
                        "createdTimeMs": 222,
                        "txHash": "0xbridgehash",
                    }
                ]
            },
        ]
        balance_calls = []

        def sign_transaction(tx):
            sent_raw.append(tx)
            return "0xsigned-bridge"

        def rpc_call(method, params):
            if method == "eth_call":
                to = (params[0] or {}).get("to")
                data = str((params[0] or {}).get("data") or "")
                if to == POLYGON_NATIVE_USDC_TOKEN and data.startswith("0x70a08231"):
                    return hex(2_097_983)
                if to == POLYMARKET_USDC_TOKEN and data.startswith("0x70a08231"):
                    balance_calls.append(1)
                    return hex(0 if len(balance_calls) == 1 else 2_094_000)
                raise AssertionError(f"unexpected eth_call payload: {params}")
            if method == "eth_getTransactionCount":
                return "0x0"
            if method == "eth_gasPrice":
                return hex(100)
            if method == "eth_estimateGas":
                return hex(60_000)
            if method == "eth_sendRawTransaction":
                return "0xwallettransfer"
            if method == "eth_getTransactionReceipt":
                return {"status": "0x1"}
            raise AssertionError(f"unexpected rpc method: {method}")

        def http_post(url, payload):
            if url.endswith("/deposit"):
                self.assertEqual(payload, {"address": "0x0000000000000000000000000000000000000abc"})
                return {"address": {"evm": "0x0000000000000000000000000000000000000def"}}
            if url.endswith("/quote"):
                self.assertEqual(payload["fromTokenAddress"], POLYGON_NATIVE_USDC_TOKEN)
                self.assertEqual(payload["toTokenAddress"], POLYMARKET_USDC_TOKEN)
                return {"estToTokenBaseUnit": "2094000", "quoteId": "quote-1"}
            raise AssertionError(f"unexpected POST url: {url}")

        def http_get(url):
            if url.endswith("/supported-assets"):
                return {
                    "supportedAssets": [
                        {
                            "chainId": "137",
                            "token": {"address": POLYGON_NATIVE_USDC_TOKEN},
                            "minCheckoutUsd": 2,
                        }
                    ]
                }
            if "/status/" in url:
                return statuses.pop(0)
            raise AssertionError(f"unexpected GET url: {url}")

        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            result = client.bridge_polygon_usdc_to_usdce(
                rpc_call=rpc_call,
                sign_transaction=sign_transaction,
                signer_address="0x0000000000000000000000000000000000000abc",
                http_post=http_post,
                http_get=http_get,
                poll_interval_seconds=0,
                sleep_func=lambda _: None,
            )

        self.assertEqual(
            result,
            PolygonUsdcBridgeResult(
                deposit_address="0x0000000000000000000000000000000000000def",
                source_amount_base_unit=2_097_983,
                quoted_target_amount_base_unit=2_094_000,
                wallet_transfer_tx_hash="0xwallettransfer",
                bridge_status="COMPLETED",
                bridge_tx_hash="0xbridgehash",
                initial_target_balance_base_unit=0,
                final_target_balance_base_unit=2_094_000,
            ),
        )
        self.assertEqual(len(sent_raw), 1)
        self.assertEqual(sent_raw[0]["to"], POLYGON_NATIVE_USDC_TOKEN)
        self.assertTrue(str(sent_raw[0]["data"]).startswith("0xa9059cbb"))

    def test_bridge_polygon_usdc_to_usdce_rejects_amount_below_bridge_minimum(self) -> None:
        client = PolymarketExecutionClient(client_factory=lambda **kwargs: _FakeVenueClient(**kwargs), order_args_factory=_FakeOrderArgs, cancel_payload_factory=_FakeOrderPayload, open_order_params_factory=_FakeOpenOrderParams)

        def rpc_call(method, params):
            if method == "eth_call":
                return hex(2_097_983)
            raise AssertionError(f"unexpected rpc method: {method}")

        def http_get(url):
            if url.endswith("/supported-assets"):
                return {
                    "supportedAssets": [
                        {
                            "chainId": "137",
                            "token": {"address": POLYGON_NATIVE_USDC_TOKEN},
                            "minCheckoutUsd": 2,
                        }
                    ]
                }
            raise AssertionError(f"unexpected GET url: {url}")

        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            with self.assertRaises(ExecutionClientConfigError) as ctx:
                client.bridge_polygon_usdc_to_usdce(
                    amount_base_unit=1_500_000,
                    rpc_call=rpc_call,
                    signer_address="0x0000000000000000000000000000000000000abc",
                    http_get=http_get,
                    http_post=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not POST")),
                    sleep_func=lambda _: None,
                )

        self.assertIn("below the documented Polymarket bridge minimum", str(ctx.exception))

    def test_redeem_positions_encodes_ctf_call_and_waits_for_receipt(self) -> None:
        client = PolymarketExecutionClient(client_factory=lambda **kwargs: _FakeVenueClient(**kwargs), order_args_factory=_FakeOrderArgs, cancel_payload_factory=_FakeOrderPayload, open_order_params_factory=_FakeOpenOrderParams)

        sent_raw = []

        def sign_transaction(tx):
            sent_raw.append(tx)
            return "0xsigned-redeem"

        def rpc_call(method, params):
            if method == "eth_getTransactionCount":
                return "0x0"
            if method == "eth_gasPrice":
                return hex(100)
            if method == "eth_estimateGas":
                return hex(90_000)
            if method == "eth_sendRawTransaction":
                return "0xredeemhash"
            if method == "eth_getTransactionReceipt":
                return {"status": "0x1"}
            raise AssertionError(f"unexpected rpc method: {method}")

        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            tx_hash = client.redeem_positions(
                condition_id="0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917",
                rpc_call=rpc_call,
                sign_transaction=sign_transaction,
                signer_address="0x0000000000000000000000000000000000000abc",
                sleep_func=lambda _: None,
            )

        self.assertEqual(tx_hash, "0xredeemhash")
        self.assertEqual(len(sent_raw), 1)
        self.assertEqual(sent_raw[0]["to"], POLYMARKET_CTF_TOKEN)
        self.assertTrue(str(sent_raw[0]["data"]).startswith("0x01b7037c"))
        self.assertIn(POLYMARKET_USDC_TOKEN.lower().replace("0x", ""), str(sent_raw[0]["data"]).lower())
        self.assertIn(ZERO_BYTES32.replace("0x", ""), str(sent_raw[0]["data"]).lower())

    def test_redeem_resolved_positions_groups_conditions_and_skips_unsupported_wallet_shapes(self) -> None:
        client = PolymarketExecutionClient(client_factory=lambda **kwargs: _FakeVenueClient(**kwargs), order_args_factory=_FakeOrderArgs, cancel_payload_factory=_FakeOrderPayload, open_order_params_factory=_FakeOpenOrderParams)

        sent_raw = []

        def sign_transaction(tx):
            sent_raw.append(tx)
            return "0xsigned-redeem"

        def rpc_call(method, params):
            if method == "eth_getTransactionCount":
                return hex(len(sent_raw))
            if method == "eth_gasPrice":
                return hex(100)
            if method == "eth_estimateGas":
                return hex(90_000)
            if method == "eth_sendRawTransaction":
                return f"0xredeemhash{len(sent_raw)}"
            if method == "eth_getTransactionReceipt":
                return {"status": "0x1"}
            raise AssertionError(f"unexpected rpc method: {method}")

        def http_get(url):
            self.assertEqual(
                url,
                f"{DEFAULT_DATA_API_HOST}/positions?user=0x0000000000000000000000000000000000000abc&sizeThreshold=0",
            )
            return [
                {
                    "conditionId": "0x1111111111111111111111111111111111111111111111111111111111111111",
                    "size": 1.2,
                    "redeemable": True,
                    "negativeRisk": False,
                    "proxyWallet": "0x0000000000000000000000000000000000000abc",
                    "outcome": "Yes",
                    "outcomeIndex": 0,
                },
                {
                    "conditionId": "0x1111111111111111111111111111111111111111111111111111111111111111",
                    "size": 0.4,
                    "redeemable": True,
                    "negativeRisk": False,
                    "proxyWallet": "0x0000000000000000000000000000000000000abc",
                    "outcome": "No",
                    "outcomeIndex": 1,
                },
                {
                    "conditionId": "0x2222222222222222222222222222222222222222222222222222222222222222",
                    "size": 0.8,
                    "redeemable": True,
                    "negativeRisk": True,
                },
                {
                    "conditionId": "0x3333333333333333333333333333333333333333333333333333333333333333",
                    "size": 0.6,
                    "redeemable": True,
                    "negativeRisk": False,
                    "proxyWallet": "0x0000000000000000000000000000000000000def",
                },
            ]

        with patch.dict(os.environ, {"POLY_PRIVATE_KEY": "env-secret"}, clear=False):
            results = client.redeem_resolved_positions(
                http_get=http_get,
                rpc_call=rpc_call,
                sign_transaction=sign_transaction,
                signer_address="0x0000000000000000000000000000000000000abc",
                sleep_func=lambda _: None,
            )

        self.assertEqual([result.status for result in results], ["redeemed", "skipped_negative_risk", "skipped_proxy_wallet"])
        self.assertEqual(results[0].condition_id, "0x1111111111111111111111111111111111111111111111111111111111111111")
        self.assertEqual(results[0].position_count, 2)
        self.assertEqual(results[0].tx_hash, "0xredeemhash1")
        self.assertEqual(results[1].condition_id, "0x2222222222222222222222222222222222222222222222222222222222222222")
        self.assertEqual(results[2].proxy_wallet, "0x0000000000000000000000000000000000000def")
        self.assertEqual(len(sent_raw), 1)

    @unittest.skipUnless(os.getenv("POLYMARKET_RUN_VENUE_TESTS") == "1", "set POLYMARKET_RUN_VENUE_TESTS=1 to hit real venue")
    def test_startup_and_list_open_orders_against_real_venue(self) -> None:
        try:
            import py_clob_client_v2  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            self.skipTest(f"py-clob-client-v2 unavailable: {exc}")

        client = PolymarketExecutionClient()
        client.startup()
        orders = client.list_open_orders()
        self.assertIsInstance(orders, list)


if __name__ == "__main__":
    unittest.main()
