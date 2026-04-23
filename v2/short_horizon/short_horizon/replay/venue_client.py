from __future__ import annotations

from typing import Any, Callable

from ..venue_polymarket.execution_client import VenueCancelResult, VenueOrderRequest, VenueOrderState, VenuePlaceResult


class ReplayFidelityError(RuntimeError):
    """Raised when replay execution diverges from the captured venue interaction trace."""


class CapturedResponseExecutionClient:
    """Offline execution client backed entirely by captured venue request/response records."""

    def __init__(self, venue_responses: list[dict[str, Any]]):
        self._records: list[dict[str, Any]] = []
        self._cursor = 0
        for record in venue_responses:
            kind = str(record.get("kind") or "").strip()
            if not kind:
                raise ValueError("Captured venue response is missing kind")
            self._records.append(record)

    def place_order(self, order_request: VenueOrderRequest) -> VenuePlaceResult:
        record = self._match_record(
            "place_order",
            lambda item: _match_place_order_request(item.get("request"), order_request),
            describe_expected=lambda: (
                f"client_order_id={getattr(order_request, 'client_order_id', None)!r}, "
                f"token_id={getattr(order_request, 'token_id', None)!r}, "
                f"side={getattr(order_request, 'side', None)!r}, "
                f"price={getattr(order_request, 'price', None)!r}, "
                f"size={getattr(order_request, 'size', None)!r}"
            ),
        )
        _raise_captured_error_if_present("place_order", record)
        response = record.get("response")
        payload = response if isinstance(response, dict) else {"value": response}
        return VenuePlaceResult(
            order_id=str(payload.get("order_id") or payload.get("orderID") or payload.get("id") or ""),
            status=str(payload.get("status") or payload.get("orderStatus") or "unknown"),
            client_order_id=_optional_str(payload.get("client_order_id")) or order_request.client_order_id,
            raw=payload,
        )

    def cancel_order(self, order_id: str) -> VenueCancelResult:
        record = self._match_record(
            "cancel_order",
            lambda item: _match_named_value(item, expected=order_id, key_name="venue_order_id"),
            describe_expected=lambda: f"venue_order_id={order_id!r}",
        )
        _raise_captured_error_if_present("cancel_order", record)
        response = record.get("response")
        payload = response if isinstance(response, dict) else {"value": response}
        success = bool(payload.get("success", True))
        status = str(payload.get("status") or ("canceled" if success else "error"))
        return VenueCancelResult(order_id=order_id, success=success, status=status, raw=payload)

    def get_order(self, order_id: str) -> VenueOrderState:
        record = self._match_record(
            "get_order",
            lambda item: _match_named_value(item, expected=order_id, key_name="venue_order_id"),
            describe_expected=lambda: f"venue_order_id={order_id!r}",
        )
        _raise_captured_error_if_present("get_order", record)
        return _parse_venue_order_state(record.get("response"))

    def list_open_orders(self, market_id: str | None = None) -> list[VenueOrderState]:
        record = self._match_record(
            "list_open_orders",
            lambda item: _match_named_value(item, expected=market_id, key_name="market_id"),
            describe_expected=lambda: f"market_id={market_id!r}",
        )
        _raise_captured_error_if_present("list_open_orders", record)
        response = record.get("response")
        if response is None:
            return []
        if not isinstance(response, list):
            raise ValueError(f"Captured list_open_orders response must be a list, got {type(response)!r}")
        return [_parse_venue_order_state(item) for item in response]

    def assert_all_records_consumed(self) -> None:
        if self._cursor >= len(self._records):
            return
        remaining = len(self._records) - self._cursor
        next_record = self._records[self._cursor]
        next_kind = str(next_record.get("kind") or "<missing>")
        raise ReplayFidelityError(
            "Replay fidelity mismatch: replay finished before consuming the full captured execution trace "
            f"({remaining} record(s) remained, next={next_kind}: {_describe_record(next_record)})"
        )

    def _match_record(self, kind: str, predicate: Callable[[dict[str, Any]], bool], *, describe_expected: Callable[[], str]) -> dict[str, Any]:
        if self._cursor >= len(self._records):
            raise ReplayFidelityError(
                f"Replay fidelity mismatch for {kind}: no captured record remained ({describe_expected()}); "
                "captured trace already exhausted"
            )
        record = self._records[self._cursor]
        captured_kind = str(record.get("kind") or "")
        if captured_kind != kind:
            raise ReplayFidelityError(
                f"Replay fidelity mismatch for {kind}: expected next captured call to be {kind}, "
                f"but next record was {captured_kind or '<missing>'} ({_describe_record(record)})"
            )
        if not predicate(record):
            raise ReplayFidelityError(
                f"Replay fidelity mismatch for {kind}: next captured record did not match "
                f"({describe_expected()}); captured=({_describe_record(record)})"
            )
        self._cursor += 1
        return record


def _match_place_order_request(request_payload: Any, order_request: VenueOrderRequest) -> bool:
    if not isinstance(request_payload, dict):
        return False
    payload_client_order_id = _optional_str(request_payload.get("client_order_id"))
    request_client_order_id = _optional_str(getattr(order_request, "client_order_id", None))
    if payload_client_order_id is not None and request_client_order_id is not None:
        return payload_client_order_id == request_client_order_id
    if (payload_client_order_id is None) != (request_client_order_id is None):
        return False
    return (
        str(request_payload.get("token_id")) == str(getattr(order_request, "token_id", None))
        and str(request_payload.get("side")) == str(getattr(order_request, "side", None))
        and _float_matches(request_payload.get("price"), getattr(order_request, "price", None))
        and _float_matches(request_payload.get("size"), getattr(order_request, "size", None))
        and _optional_str(request_payload.get("time_in_force")) == _optional_str(getattr(order_request, "time_in_force", None))
        and _optional_bool(request_payload.get("post_only")) == _optional_bool(getattr(order_request, "post_only", None))
    )


def _match_named_value(record: dict[str, Any], *, expected: Any, key_name: str) -> bool:
    request = record.get("request")
    if isinstance(request, dict) and key_name in request:
        return _optional_str(request.get(key_name)) == _optional_str(expected)
    key = record.get("key")
    if isinstance(key, dict) and key_name in key:
        return _optional_str(key.get(key_name)) == _optional_str(expected)
    return False


def _describe_record(record: dict[str, Any]) -> str:
    request = record.get("request")
    key = record.get("key")
    if isinstance(request, dict) and request:
        return f"request={request!r}"
    if isinstance(key, dict) and key:
        return f"key={key!r}"
    return repr(record)


def _raise_captured_error_if_present(kind: str, record: dict[str, Any]) -> None:
    error = record.get("error")
    if not isinstance(error, dict) or not error:
        return
    error_type = _optional_str(error.get("type")) or "RuntimeError"
    error_message = _optional_str(error.get("message")) or f"captured {kind} failed"
    raise ReplayFidelityError(f"Captured {kind} error {error_type}: {error_message}")


def _parse_venue_order_state(payload: Any) -> VenueOrderState:
    if not isinstance(payload, dict):
        raise ValueError(f"Captured venue order state must be an object, got {type(payload)!r}")
    return VenueOrderState(
        order_id=str(payload.get("order_id") or payload.get("orderID") or payload.get("id") or ""),
        status=str(payload.get("status") or payload.get("orderStatus") or "unknown"),
        client_order_id=_optional_str(payload.get("client_order_id")),
        market_id=_optional_str(payload.get("market_id")),
        token_id=_optional_str(payload.get("token_id")),
        side=_optional_str(payload.get("side")),
        price=_optional_float(payload.get("price")),
        original_size=_optional_float(payload.get("original_size", payload.get("size"))),
        cumulative_filled_size=_optional_float(payload.get("cumulative_filled_size")),
        remaining_size=_optional_float(payload.get("remaining_size")),
        raw=payload,
    )


def _float_matches(left: Any, right: Any, *, tolerance: float = 1e-5) -> bool:
    left_value = _optional_float(left)
    right_value = _optional_float(right)
    if left_value is None or right_value is None:
        return left_value is right_value
    return abs(left_value - right_value) <= tolerance


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return bool(value)


__all__ = ["CapturedResponseExecutionClient", "ReplayFidelityError"]
