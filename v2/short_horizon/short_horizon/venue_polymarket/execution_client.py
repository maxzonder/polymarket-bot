from __future__ import annotations

import importlib
import inspect
import os
from dataclasses import dataclass, field
from typing import Any, Callable


DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
DEFAULT_CHAIN_ID = 137
PRIVATE_KEY_ENV_VAR = "POLY_PRIVATE_KEY"


class ExecutionClientConfigError(RuntimeError):
    pass


class ExecutionClientNotStartedError(RuntimeError):
    pass


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


__all__ = [
    "DEFAULT_CHAIN_ID",
    "DEFAULT_CLOB_HOST",
    "ExecutionClientConfigError",
    "ExecutionClientNotStartedError",
    "PolymarketExecutionClient",
    "PRIVATE_KEY_ENV_VAR",
    "VenueCancelResult",
    "VenueOrderRequest",
    "VenueOrderState",
    "VenuePlaceResult",
]
