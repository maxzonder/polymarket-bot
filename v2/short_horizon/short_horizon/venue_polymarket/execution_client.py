from __future__ import annotations

import importlib
import inspect
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from typing import Any, Callable


DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
DEFAULT_CHAIN_ID = 137
DEFAULT_POLYGON_RPC_URL = "https://polygon-bor-rpc.publicnode.com"
DEFAULT_BRIDGE_HOST = "https://bridge.polymarket.com"
PRIVATE_KEY_ENV_VAR = "POLY_PRIVATE_KEY"
POLYGON_NATIVE_USDC_TOKEN = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
POLYMARKET_USDC_TOKEN = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYMARKET_CTF_TOKEN = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
POLYMARKET_SPENDER_ADDRESSES = (
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "0xC5d563A36AE78145C45a50134d48A1215220f80a",
    "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",
)
MAX_UINT256 = (1 << 256) - 1
KNOWN_SELECTORS = {
    "allowance(address,address)": "dd62ed3e",
    "approve(address,uint256)": "095ea7b3",
    "balanceOf(address)": "70a08231",
    "isApprovedForAll(address,address)": "e985e9c5",
    "setApprovalForAll(address,bool)": "a22cb465",
    "transfer(address,uint256)": "a9059cbb",
}


class ExecutionClientConfigError(RuntimeError):
    pass


class ExecutionClientNotStartedError(RuntimeError):
    pass


@dataclass(frozen=True)
class AllowanceApprovalResult:
    asset_type: str
    spender: str
    tx_hash: str | None
    status: str


@dataclass(frozen=True)
class PolygonUsdcBridgeResult:
    deposit_address: str
    source_amount_base_unit: int
    quoted_target_amount_base_unit: int | None
    wallet_transfer_tx_hash: str
    bridge_status: str
    bridge_tx_hash: str | None
    initial_target_balance_base_unit: int
    final_target_balance_base_unit: int


@dataclass(frozen=True)
class VenueApiCredentials:
    api_key: str
    secret: str
    passphrase: str


@dataclass(frozen=True)
class VenueOrderRequest:
    token_id: str
    side: str
    price: float
    size: float
    client_order_id: str | None = None
    time_in_force: str | None = None
    post_only: bool | None = None


@dataclass(frozen=True)
class VenuePlaceResult:
    order_id: str
    status: str
    client_order_id: str | None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VenueCancelResult:
    order_id: str
    success: bool
    status: str
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VenueOrderState:
    order_id: str
    status: str
    client_order_id: str | None = None
    market_id: str | None = None
    token_id: str | None = None
    side: str | None = None
    price: float | None = None
    original_size: float | None = None
    cumulative_filled_size: float | None = None
    remaining_size: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)


class PolymarketExecutionClient:
    def __init__(
        self,
        private_key: str | None = None,
        host: str = DEFAULT_CLOB_HOST,
        chain_id: int = DEFAULT_CHAIN_ID,
        *,
        private_key_env_var: str = PRIVATE_KEY_ENV_VAR,
        client_factory: Callable[..., Any] | None = None,
        order_args_factory: Callable[..., Any] | None = None,
    ):
        self._private_key = private_key
        self.host = host
        self.chain_id = int(chain_id)
        self.private_key_env_var = private_key_env_var
        self._client_factory = client_factory
        self._order_args_factory = order_args_factory
        self._client = None
        self._api_credentials: VenueApiCredentials | None = None

    def startup(self) -> None:
        private_key = self._resolve_private_key()
        client_factory = self._client_factory
        order_args_factory = self._order_args_factory
        if client_factory is None or order_args_factory is None:
            sdk_client_factory, sdk_order_args_factory = self._load_sdk_factories()
            client_factory = client_factory or sdk_client_factory
            order_args_factory = order_args_factory or sdk_order_args_factory
        client = client_factory(host=self.host, key=private_key, chain_id=self.chain_id)
        api_creds = client.create_or_derive_api_creds()
        self._api_credentials = _normalize_api_creds(api_creds)
        client.set_api_creds(api_creds)
        self._client = client
        self._client_factory = client_factory
        self._order_args_factory = order_args_factory
        self.list_open_orders()

    def place_order(self, order_request: VenueOrderRequest) -> VenuePlaceResult:
        client = self._require_client()
        order_args = self._build_order_args(order_request)
        raw = client.create_and_post_order(order_args)
        payload = raw if isinstance(raw, dict) else {"value": raw}
        order_id = str(payload.get("orderID") or payload.get("id") or payload.get("order_id") or "")
        status = str(payload.get("status") or payload.get("orderStatus") or "unknown")
        return VenuePlaceResult(
            order_id=order_id,
            status=status,
            client_order_id=order_request.client_order_id,
            raw=payload,
        )

    def cancel_order(self, order_id: str) -> VenueCancelResult:
        client = self._require_client()
        raw = client.cancel(order_id)
        payload = raw if isinstance(raw, dict) else {"value": raw}
        success = bool(payload.get("success", True))
        status = str(payload.get("status") or ("canceled" if success else "error"))
        return VenueCancelResult(order_id=order_id, success=success, status=status, raw=payload)

    def get_order(self, order_id: str) -> VenueOrderState:
        client = self._require_client()
        raw = client.get_order(order_id)
        return self._normalize_order_state(raw)

    def list_open_orders(self, market_id: str | None = None) -> list[VenueOrderState]:
        client = self._require_client()
        kwargs = {"market": market_id} if market_id else {}
        raw = client.get_orders(**kwargs)
        items = raw if isinstance(raw, list) else raw.get("data", [])
        results = [self._normalize_order_state(item) for item in items]
        return [row for row in results if _is_live_status(row.status)]

    def approve_allowances(
        self,
        *,
        rpc_url: str = DEFAULT_POLYGON_RPC_URL,
        rpc_call: Callable[[str, list[Any]], Any] | None = None,
        sign_transaction: Callable[[dict[str, Any]], str] | None = None,
        signer_address: str | None = None,
        wait_timeout_seconds: float = 300.0,
        sleep_func: Callable[[float], None] = time.sleep,
    ) -> list[AllowanceApprovalResult]:
        private_key = self._resolve_private_key()
        rpc = rpc_call or _build_rpc_call(rpc_url)
        resolved_signer_address = signer_address or _address_from_private_key(private_key)
        resolved_sign_transaction = sign_transaction or (lambda tx: _sign_transaction(tx, private_key))

        results: list[AllowanceApprovalResult] = []
        for spender in POLYMARKET_SPENDER_ADDRESSES:
            allowance = _read_erc20_allowance(
                rpc,
                token_address=POLYMARKET_USDC_TOKEN,
                owner=resolved_signer_address,
                spender=spender,
            )
            if allowance > 0:
                results.append(AllowanceApprovalResult(asset_type="collateral", spender=spender, tx_hash=None, status="already_approved"))
            else:
                tx_hash = _send_contract_transaction(
                    rpc,
                    signer_address=resolved_signer_address,
                    sign_transaction=resolved_sign_transaction,
                    chain_id=self.chain_id,
                    to=POLYMARKET_USDC_TOKEN,
                    data=_encode_approve_calldata(spender, MAX_UINT256),
                    wait_timeout_seconds=wait_timeout_seconds,
                    sleep_func=sleep_func,
                )
                results.append(AllowanceApprovalResult(asset_type="collateral", spender=spender, tx_hash=tx_hash, status="approved"))

            approved = _read_erc1155_approval_for_all(
                rpc,
                token_address=POLYMARKET_CTF_TOKEN,
                owner=resolved_signer_address,
                operator=spender,
            )
            if approved:
                results.append(AllowanceApprovalResult(asset_type="conditional", spender=spender, tx_hash=None, status="already_approved"))
            else:
                tx_hash = _send_contract_transaction(
                    rpc,
                    signer_address=resolved_signer_address,
                    sign_transaction=resolved_sign_transaction,
                    chain_id=self.chain_id,
                    to=POLYMARKET_CTF_TOKEN,
                    data=_encode_set_approval_for_all_calldata(spender, True),
                    wait_timeout_seconds=wait_timeout_seconds,
                    sleep_func=sleep_func,
                )
                results.append(AllowanceApprovalResult(asset_type="conditional", spender=spender, tx_hash=tx_hash, status="approved"))
        return results

    def bridge_polygon_usdc_to_usdce(
        self,
        *,
        amount_base_unit: int | None = None,
        rpc_url: str = DEFAULT_POLYGON_RPC_URL,
        bridge_host: str = DEFAULT_BRIDGE_HOST,
        rpc_call: Callable[[str, list[Any]], Any] | None = None,
        sign_transaction: Callable[[dict[str, Any]], str] | None = None,
        signer_address: str | None = None,
        http_post: Callable[[str, dict[str, Any]], Any] | None = None,
        http_get: Callable[[str], Any] | None = None,
        wait_timeout_seconds: float = 900.0,
        poll_interval_seconds: float = 10.0,
        sleep_func: Callable[[float], None] = time.sleep,
    ) -> PolygonUsdcBridgeResult:
        private_key = self._resolve_private_key()
        rpc = rpc_call or _build_rpc_call(rpc_url)
        resolved_signer_address = signer_address or _address_from_private_key(private_key)
        resolved_sign_transaction = sign_transaction or (lambda tx: _sign_transaction(tx, private_key))
        post_json = http_post or _build_http_post_json()
        get_json = http_get or _build_http_get_json()

        native_usdc_balance = _read_erc20_balance(
            rpc,
            token_address=POLYGON_NATIVE_USDC_TOKEN,
            owner=resolved_signer_address,
        )
        resolved_amount = native_usdc_balance if amount_base_unit is None else int(amount_base_unit)
        if resolved_amount <= 0:
            raise ExecutionClientConfigError("Polygon native USDC balance is zero, nothing to bridge")
        if resolved_amount > native_usdc_balance:
            raise ExecutionClientConfigError(
                f"requested Polygon native USDC bridge amount exceeds wallet balance: requested={resolved_amount} balance={native_usdc_balance}"
            )

        min_checkout_base_unit = _load_bridge_min_checkout_base_unit(
            get_json,
            bridge_host=bridge_host,
            chain_id=str(self.chain_id),
            token_address=POLYGON_NATIVE_USDC_TOKEN,
        )
        if min_checkout_base_unit is not None and resolved_amount < min_checkout_base_unit:
            raise ExecutionClientConfigError(
                "requested Polygon native USDC bridge amount is below the documented Polymarket bridge minimum: "
                f"requested={resolved_amount} minimum={min_checkout_base_unit}"
            )

        initial_target_balance = _read_erc20_balance(
            rpc,
            token_address=POLYMARKET_USDC_TOKEN,
            owner=resolved_signer_address,
        )
        deposit_payload = post_json(
            f"{bridge_host.rstrip('/')}/deposit",
            {"address": resolved_signer_address},
        )
        deposit_address = str(((deposit_payload or {}).get("address") or {}).get("evm") or "").strip()
        if not deposit_address:
            raise ExecutionClientConfigError("Polymarket bridge deposit response did not include an EVM deposit address")

        quoted_target_amount: int | None = None
        try:
            quote_payload = post_json(
                f"{bridge_host.rstrip('/')}/quote",
                {
                    "fromAmountBaseUnit": str(resolved_amount),
                    "fromChainId": str(self.chain_id),
                    "fromTokenAddress": POLYGON_NATIVE_USDC_TOKEN,
                    "recipientAddress": resolved_signer_address,
                    "toChainId": str(self.chain_id),
                    "toTokenAddress": POLYMARKET_USDC_TOKEN,
                },
            )
            raw_estimate = (quote_payload or {}).get("estToTokenBaseUnit")
            if raw_estimate not in (None, ""):
                quoted_target_amount = int(str(raw_estimate))
        except Exception:
            quoted_target_amount = None

        initial_matches = _select_bridge_transactions(
            (get_json(f"{bridge_host.rstrip('/')}/status/{deposit_address}") or {}).get("transactions"),
            source_chain_id=str(self.chain_id),
            source_token_address=POLYGON_NATIVE_USDC_TOKEN,
            source_amount_base_unit=str(resolved_amount),
        )

        wallet_transfer_tx_hash = _send_contract_transaction(
            rpc,
            signer_address=resolved_signer_address,
            sign_transaction=resolved_sign_transaction,
            chain_id=self.chain_id,
            to=POLYGON_NATIVE_USDC_TOKEN,
            data=_encode_transfer_calldata(deposit_address, resolved_amount),
            wait_timeout_seconds=wait_timeout_seconds,
            sleep_func=sleep_func,
        )

        deadline = time.time() + float(wait_timeout_seconds)
        initial_match_count = len(initial_matches)
        last_status = "SUBMITTED"
        bridge_tx_hash: str | None = None
        matched = False
        while time.time() < deadline:
            payload = get_json(f"{bridge_host.rstrip('/')}/status/{deposit_address}") or {}
            matches = _select_bridge_transactions(
                payload.get("transactions"),
                source_chain_id=str(self.chain_id),
                source_token_address=POLYGON_NATIVE_USDC_TOKEN,
                source_amount_base_unit=str(resolved_amount),
            )
            if len(matches) <= initial_match_count:
                sleep_func(float(poll_interval_seconds))
                continue
            current = matches[-1]
            matched = True
            last_status = str(current.get("status") or "PROCESSING")
            bridge_tx_hash = _as_optional_str(current.get("txHash"))
            if last_status == "COMPLETED":
                final_target_balance = _read_erc20_balance(
                    rpc,
                    token_address=POLYMARKET_USDC_TOKEN,
                    owner=resolved_signer_address,
                )
                return PolygonUsdcBridgeResult(
                    deposit_address=deposit_address,
                    source_amount_base_unit=resolved_amount,
                    quoted_target_amount_base_unit=quoted_target_amount,
                    wallet_transfer_tx_hash=wallet_transfer_tx_hash,
                    bridge_status=last_status,
                    bridge_tx_hash=bridge_tx_hash,
                    initial_target_balance_base_unit=initial_target_balance,
                    final_target_balance_base_unit=final_target_balance,
                )
            if last_status == "FAILED":
                raise ExecutionClientConfigError(
                    "Polymarket bridge reported FAILED while converting Polygon native USDC to USDC.e"
                )
            sleep_func(float(poll_interval_seconds))

        reason = "bridge transaction never appeared" if not matched else f"bridge remained in non-terminal status {last_status}"
        raise ExecutionClientConfigError(
            f"Timed out waiting for Polymarket bridge completion after Polygon USDC transfer: {reason}; tx={wallet_transfer_tx_hash}"
        )

    def api_credentials(self) -> VenueApiCredentials:
        if self._api_credentials is None:
            raise ExecutionClientNotStartedError("Execution client API credentials unavailable before startup()")
        return self._api_credentials

    def _build_order_args(self, order_request: VenueOrderRequest):
        order_args_factory = self._order_args_factory
        if order_args_factory is None:
            raise ExecutionClientNotStartedError("Execution client order-args factory is not initialized")
        kwargs: dict[str, Any] = {
            "price": order_request.price,
            "size": order_request.size,
            "side": order_request.side,
            "token_id": order_request.token_id,
        }
        signature = _safe_signature(order_args_factory)
        if order_request.client_order_id and _supports_parameter(signature, "client_order_id"):
            kwargs["client_order_id"] = order_request.client_order_id
        if order_request.time_in_force and _supports_parameter(signature, "time_in_force"):
            kwargs["time_in_force"] = order_request.time_in_force
        if order_request.post_only is not None and _supports_parameter(signature, "post_only"):
            kwargs["post_only"] = order_request.post_only
        return order_args_factory(**kwargs)

    def _normalize_order_state(self, raw: Any) -> VenueOrderState:
        payload = raw if isinstance(raw, dict) else {"value": raw}
        order_id = str(payload.get("id") or payload.get("orderID") or payload.get("order_id") or "")
        status = str(payload.get("status") or payload.get("orderStatus") or "unknown")
        size = _as_optional_float(payload.get("original_size") or payload.get("size"))
        matched = _as_optional_float(
            payload.get("matched_size")
            or payload.get("filled_size")
            or payload.get("cumulative_filled_size")
        )
        remaining = _as_optional_float(payload.get("remaining_size"))
        if remaining is None and size is not None and matched is not None:
            remaining = max(size - matched, 0.0)
        return VenueOrderState(
            order_id=order_id,
            status=status,
            client_order_id=_as_optional_str(payload.get("client_order_id")),
            market_id=_as_optional_str(payload.get("market")),
            token_id=_as_optional_str(payload.get("asset_id") or payload.get("token_id")),
            side=_as_optional_str(payload.get("side")),
            price=_as_optional_float(payload.get("price")),
            original_size=size,
            cumulative_filled_size=matched,
            remaining_size=remaining,
            raw=payload,
        )

    def _resolve_private_key(self) -> str:
        private_key = self._private_key or os.getenv(self.private_key_env_var)
        if not private_key:
            raise ExecutionClientConfigError(
                f"{self.private_key_env_var} is required for Polymarket execution and must come from env"
            )
        return private_key

    def _load_sdk_factories(self) -> tuple[Callable[..., Any], Callable[..., Any]]:
        try:
            client_module = importlib.import_module("py_clob_client.client")
            types_module = importlib.import_module("py_clob_client.clob_types")
        except ImportError as exc:
            raise ExecutionClientConfigError(
                "py-clob-client is required for live Polymarket execution"
            ) from exc
        return client_module.ClobClient, types_module.OrderArgs

    def _require_client(self):
        if self._client is None:
            raise ExecutionClientNotStartedError("Execution client not started, call startup() first")
        return self._client


def _safe_signature(factory: Callable[..., Any] | None) -> inspect.Signature | None:
    if factory is None:
        return None
    try:
        return inspect.signature(factory)
    except (TypeError, ValueError):
        return None


def _supports_parameter(signature: inspect.Signature | None, name: str) -> bool:
    if signature is None:
        return False
    params = signature.parameters
    return any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()) or name in params


def _as_optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _as_optional_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _is_live_status(status: str) -> bool:
    normalized = status.strip().lower()
    return normalized in {"live", "open", "accepted", "order_status_live"}


def _normalize_api_creds(raw: Any) -> VenueApiCredentials:
    if hasattr(raw, "api_key") and hasattr(raw, "secret") and hasattr(raw, "passphrase"):
        api_key = getattr(raw, "api_key")
        secret = getattr(raw, "secret")
        passphrase = getattr(raw, "passphrase")
    elif hasattr(raw, "api_key") and hasattr(raw, "api_secret") and hasattr(raw, "api_passphrase"):
        api_key = getattr(raw, "api_key")
        secret = getattr(raw, "api_secret")
        passphrase = getattr(raw, "api_passphrase")
    elif isinstance(raw, dict):
        api_key = raw.get("api_key") or raw.get("apiKey")
        secret = raw.get("secret") or raw.get("api_secret") or raw.get("apiSecret")
        passphrase = raw.get("passphrase") or raw.get("api_passphrase") or raw.get("apiPassphrase")
    else:
        raise ExecutionClientConfigError("Unsupported API credentials shape returned by py-clob-client")
    if not api_key or not secret or not passphrase:
        raise ExecutionClientConfigError("Incomplete API credentials returned by py-clob-client")
    return VenueApiCredentials(api_key=str(api_key), secret=str(secret), passphrase=str(passphrase))


def _build_rpc_call(rpc_url: str) -> Callable[[str, list[Any]], Any]:
    def _rpc(method: str, params: list[Any]) -> Any:
        try:
            import requests
        except ImportError as exc:  # pragma: no cover
            raise ExecutionClientConfigError("requests is required for Polygon allowance approvals") from exc
        response = requests.post(
            rpc_url,
            json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1},
            headers={"User-Agent": "polymarket-bot/short_horizon", "Content-Type": "application/json"},
            timeout=45,
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise ExecutionClientConfigError(f"Polygon RPC {method} failed: {payload['error']}")
        return payload["result"]

    return _rpc


def _build_http_post_json() -> Callable[[str, dict[str, Any]], Any]:
    def _post(url: str, payload: dict[str, Any]) -> Any:
        try:
            import requests
        except ImportError as exc:  # pragma: no cover
            raise ExecutionClientConfigError("requests is required for Polymarket bridge operations") from exc
        response = requests.post(
            url,
            json=payload,
            headers={
                "User-Agent": "polymarket-bot/short_horizon",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=45,
        )
        response.raise_for_status()
        return response.json()

    return _post


def _build_http_get_json() -> Callable[[str], Any]:
    def _get(url: str) -> Any:
        try:
            import requests
        except ImportError as exc:  # pragma: no cover
            raise ExecutionClientConfigError("requests is required for Polymarket bridge operations") from exc
        response = requests.get(
            url,
            headers={
                "User-Agent": "polymarket-bot/short_horizon",
                "Accept": "application/json",
            },
            timeout=45,
        )
        response.raise_for_status()
        return response.json()

    return _get


def _address_from_private_key(private_key: str) -> str:
    try:
        from eth_account import Account
    except ImportError as exc:  # pragma: no cover
        raise ExecutionClientConfigError("eth-account is required for Polygon allowance approvals") from exc
    return str(Account.from_key(private_key).address)


def _sign_transaction(tx: dict[str, Any], private_key: str) -> str:
    try:
        from eth_account import Account
    except ImportError as exc:  # pragma: no cover
        raise ExecutionClientConfigError("eth-account is required for Polygon allowance approvals") from exc
    signed = Account.sign_transaction(tx, private_key)
    raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction", None)
    if raw is None:
        raise ExecutionClientConfigError("eth-account returned no signed raw transaction")
    return "0x" + raw.hex()


def _selector(signature: str) -> str:
    known = KNOWN_SELECTORS.get(signature)
    if known is not None:
        return known
    try:
        from eth_utils import keccak
    except ImportError as exc:  # pragma: no cover
        raise ExecutionClientConfigError("eth-utils is required for Polygon allowance approvals") from exc
    return keccak(text=signature)[:4].hex()


def _encode_address(address: str) -> str:
    return str(address).lower().replace("0x", "").rjust(64, "0")


def _encode_uint(value: int) -> str:
    return hex(int(value))[2:].rjust(64, "0")


def _encode_bool(value: bool) -> str:
    return ("1" if value else "0").rjust(64, "0")


def _encode_approve_calldata(spender: str, amount: int) -> str:
    return "0x" + _selector("approve(address,uint256)") + _encode_address(spender) + _encode_uint(amount)


def _encode_transfer_calldata(recipient: str, amount: int) -> str:
    return "0x" + _selector("transfer(address,uint256)") + _encode_address(recipient) + _encode_uint(amount)


def _encode_set_approval_for_all_calldata(operator: str, approved: bool) -> str:
    return "0x" + _selector("setApprovalForAll(address,bool)") + _encode_address(operator) + _encode_bool(approved)


def _read_erc20_allowance(rpc: Callable[[str, list[Any]], Any], *, token_address: str, owner: str, spender: str) -> int:
    result = rpc(
        "eth_call",
        [
            {"to": token_address, "data": "0x" + _selector("allowance(address,address)") + _encode_address(owner) + _encode_address(spender)},
            "latest",
        ],
    )
    return int(str(result), 16)


def _read_erc20_balance(rpc: Callable[[str, list[Any]], Any], *, token_address: str, owner: str) -> int:
    result = rpc(
        "eth_call",
        [
            {"to": token_address, "data": "0x" + _selector("balanceOf(address)") + _encode_address(owner)},
            "latest",
        ],
    )
    return int(str(result), 16)


def _load_bridge_min_checkout_base_unit(
    get_json: Callable[[str], Any],
    *,
    bridge_host: str,
    chain_id: str,
    token_address: str,
) -> int | None:
    payload = get_json(f"{bridge_host.rstrip('/')}/supported-assets") or {}
    rows = payload.get("supportedAssets") or []
    normalized_target = str(token_address).lower()
    for row in rows:
        if str(row.get("chainId") or "") != str(chain_id):
            continue
        token = row.get("token") or {}
        if str(token.get("address") or "").lower() != normalized_target:
            continue
        min_checkout_usd = row.get("minCheckoutUsd")
        if min_checkout_usd in (None, ""):
            return None
        scaled = (Decimal(str(min_checkout_usd)) * Decimal("1000000")).quantize(Decimal("1"), rounding=ROUND_DOWN)
        return int(scaled)
    return None


def _select_bridge_transactions(
    transactions: Any,
    *,
    source_chain_id: str,
    source_token_address: str,
    source_amount_base_unit: str,
) -> list[dict[str, Any]]:
    rows = transactions if isinstance(transactions, list) else []
    target_token = str(source_token_address).lower()
    matches = [
        row
        for row in rows
        if isinstance(row, dict)
        and str(row.get("fromChainId") or "") == str(source_chain_id)
        and str(row.get("fromTokenAddress") or "").lower() == target_token
        and str(row.get("fromAmountBaseUnit") or "") == str(source_amount_base_unit)
    ]
    return sorted(
        matches,
        key=lambda row: (
            int(row.get("createdTimeMs") or 0),
            str(row.get("txHash") or ""),
            str(row.get("status") or ""),
        ),
    )


def _read_erc1155_approval_for_all(rpc: Callable[[str, list[Any]], Any], *, token_address: str, owner: str, operator: str) -> bool:
    result = rpc(
        "eth_call",
        [
            {"to": token_address, "data": "0x" + _selector("isApprovedForAll(address,address)") + _encode_address(owner) + _encode_address(operator)},
            "latest",
        ],
    )
    return bool(int(str(result), 16))


def _send_contract_transaction(
    rpc: Callable[[str, list[Any]], Any],
    *,
    signer_address: str,
    sign_transaction: Callable[[dict[str, Any]], str],
    chain_id: int,
    to: str,
    data: str,
    wait_timeout_seconds: float,
    sleep_func: Callable[[float], None],
) -> str:
    nonce = int(str(rpc("eth_getTransactionCount", [signer_address, "pending"])), 16)
    gas_price = int(str(rpc("eth_gasPrice", [])), 16)
    try:
        gas_estimate = int(str(rpc("eth_estimateGas", [{"from": signer_address, "to": to, "data": data}])), 16)
    except Exception:
        gas_estimate = 120_000
    tx = {
        "chainId": int(chain_id),
        "nonce": nonce,
        "to": to,
        "data": data,
        "value": 0,
        "gas": max(21_000, int(gas_estimate * 1.2)),
        "gasPrice": max(1, int(gas_price * 1.2)),
    }
    tx_hash = str(rpc("eth_sendRawTransaction", [sign_transaction(tx)]))
    deadline = time.time() + float(wait_timeout_seconds)
    while time.time() < deadline:
        receipt = rpc("eth_getTransactionReceipt", [tx_hash])
        if receipt is not None:
            status = int(str(receipt.get("status") or "0x0"), 16)
            if status != 1:
                raise ExecutionClientConfigError(f"Polygon approval transaction failed: {tx_hash}")
            return tx_hash
        sleep_func(3.0)
    raise ExecutionClientConfigError(f"Timed out waiting for Polygon approval receipt: {tx_hash}")


__all__ = [
    "AllowanceApprovalResult",
    "DEFAULT_CHAIN_ID",
    "DEFAULT_CLOB_HOST",
    "DEFAULT_BRIDGE_HOST",
    "DEFAULT_POLYGON_RPC_URL",
    "ExecutionClientConfigError",
    "ExecutionClientNotStartedError",
    "MAX_UINT256",
    "PolymarketExecutionClient",
    "POLYGON_NATIVE_USDC_TOKEN",
    "POLYMARKET_CTF_TOKEN",
    "POLYMARKET_SPENDER_ADDRESSES",
    "POLYMARKET_USDC_TOKEN",
    "PolygonUsdcBridgeResult",
    "PRIVATE_KEY_ENV_VAR",
    "VenueApiCredentials",
    "VenueCancelResult",
    "VenueOrderRequest",
    "VenueOrderState",
    "VenuePlaceResult",
]
