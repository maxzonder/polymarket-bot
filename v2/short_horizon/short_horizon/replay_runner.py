from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .config import ShortHorizonConfig
from .core.clock import ReplayClock
from .core.events import OrderAccepted, OrderCanceled, OrderFilled, OrderRejected
from .core.runtime import StrategyRuntime
from .replay import ReplayEventSource
from .runner import RunnerSummary, drive_runtime_events
from .storage import RunContext, SQLiteRuntimeStore
from .strategies import ShortHorizon15mTouchStrategy
from .telemetry import configure_logging, get_logger
from .venue_polymarket.execution_client import VenueCancelResult, VenueOrderState, VenuePlaceResult


def build_replay_runtime(*, db_path: str | Path, run_id: str | None = None, config: ShortHorizonConfig | None = None, config_hash: str = "dev") -> StrategyRuntime:
    config = config or ShortHorizonConfig()
    run_context = RunContext(
        run_id=run_id or generate_run_id(),
        strategy_id=config.strategy_id,
        mode="replay",
        config_hash=config_hash,
    )
    store = SQLiteRuntimeStore(db_path, run=run_context)
    clock = ReplayClock()
    strategy = ShortHorizon15mTouchStrategy(config=config, clock=clock)
    return StrategyRuntime(strategy=strategy, intent_store=store, clock=clock)


@dataclass(frozen=True)
class ReplayBundle:
    bundle_dir: Path
    manifest: dict[str, Any]
    events: list
    input_events: list
    market_state_snapshots: list[dict[str, Any]]
    venue_responses: list[dict[str, Any]]
    orders_final: list[dict[str, Any]]
    fills_final: list[dict[str, Any]]


class ReplayVenueClient:
    """Offline execution client backed by captured venue request/response records."""

    def __init__(self, venue_responses: list[dict[str, Any]]):
        self._records_by_kind: dict[str, list[dict[str, Any]]] = {}
        self._cursor_by_kind: dict[str, int] = {}
        for record in venue_responses:
            kind = str(record.get("kind") or "").strip()
            if not kind:
                raise ValueError("Captured venue response is missing kind")
            self._records_by_kind.setdefault(kind, []).append(record)
        for kind in self._records_by_kind:
            self._cursor_by_kind[kind] = 0

    def place_order(self, order_request) -> VenuePlaceResult:
        record = self._match_record(
            "place_order",
            lambda item: _match_place_order_request(item.get("request"), order_request),
            describe_expected=lambda: (
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
            client_order_id=getattr(order_request, "client_order_id", None),
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

    def _match_record(self, kind: str, predicate: Callable[[dict[str, Any]], bool], *, describe_expected: Callable[[], str]) -> dict[str, Any]:
        records = self._records_by_kind.get(kind, [])
        start = self._cursor_by_kind.get(kind, 0)
        for index in range(start, len(records)):
            record = records[index]
            if predicate(record):
                self._cursor_by_kind[kind] = index + 1
                return record
        raise RuntimeError(
            f"No captured {kind} record matched replay request ({describe_expected()}); "
            f"searched {len(records) - start} remaining record(s)"
        )


def replay_file(*, event_log_path: str | Path, db_path: str | Path, run_id: str | None = None, config: ShortHorizonConfig | None = None, config_hash: str = "dev") -> RunnerSummary:
    runtime = build_replay_runtime(db_path=db_path, run_id=run_id, config=config, config_hash=config_hash)
    try:
        events = ReplayEventSource(event_log_path).load()
        return drive_runtime_events(
            events=events,
            runtime=runtime,
            logger_name="short_horizon.replay_runner",
            completed_event_name="replay_run_completed",
        )
    finally:
        store = runtime.store
        close = getattr(store, "close", None)
        if callable(close):
            close()


def load_replay_bundle(bundle_dir: str | Path) -> ReplayBundle:
    bundle_path = Path(bundle_dir)
    manifest_path = bundle_path / "manifest.json"
    if not manifest_path.exists():
        raise ValueError(f"Replay bundle manifest not found: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise ValueError(f"Replay bundle manifest is missing files map: {manifest_path}")

    events_path = _resolve_bundle_file(bundle_path, files, "events_log")
    market_states_path = _resolve_bundle_file(bundle_path, files, "market_state_snapshots")
    venue_path = _resolve_bundle_file(bundle_path, files, "venue_responses")
    orders_path = _resolve_bundle_file(bundle_path, files, "orders_final")
    fills_path = _resolve_bundle_file(bundle_path, files, "fills_final")

    events = ReplayEventSource(events_path).load()
    market_state_snapshots = _load_jsonl_objects(market_states_path)
    venue_responses = _load_jsonl_objects(venue_path)
    orders_final = _load_jsonl_objects(orders_path)
    fills_final = _load_jsonl_objects(fills_path)

    _validate_manifest_count(files, "events_log", len(events))
    _validate_manifest_count(files, "market_state_snapshots", len(market_state_snapshots))
    _validate_manifest_count(files, "venue_responses", len(venue_responses))
    _validate_manifest_count(files, "orders_final", len(orders_final))
    _validate_manifest_count(files, "fills_final", len(fills_final))

    return ReplayBundle(
        bundle_dir=bundle_path,
        manifest=manifest,
        events=events,
        input_events=[event for event in events if _is_replay_input_event(event)],
        market_state_snapshots=market_state_snapshots,
        venue_responses=venue_responses,
        orders_final=orders_final,
        fills_final=fills_final,
    )


def replay_bundle(
    *,
    bundle_dir: str | Path,
    db_path: str | Path,
    run_id: str | None = None,
    config: ShortHorizonConfig | None = None,
    config_hash: str | None = None,
) -> RunnerSummary:
    bundle = load_replay_bundle(bundle_dir)
    effective_run_id = run_id or _optional_str(bundle.manifest.get("run_id")) or generate_run_id()
    effective_config_hash = config_hash or _optional_str(bundle.manifest.get("config_hash")) or "dev"
    runtime = build_replay_runtime(
        db_path=db_path,
        run_id=effective_run_id,
        config=config,
        config_hash=effective_config_hash,
    )
    try:
        return drive_runtime_events(
            events=bundle.input_events,
            runtime=runtime,
            logger_name="short_horizon.replay_runner",
            completed_event_name="replay_run_completed",
            execution_mode="live",
            execution_client=ReplayVenueClient(bundle.venue_responses),
        )
    finally:
        store = runtime.store
        close = getattr(store, "close", None)
        if callable(close):
            close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay a captured short-horizon bundle into a fresh SQLite run")
    parser.add_argument("bundle_dir", help="Path to a Phase 4 replay capture bundle directory")
    parser.add_argument("db_path", help="SQLite DB path for replay outputs")
    parser.add_argument("--run-id", default=None, help="Optional explicit run_id; defaults to the captured bundle run_id")
    parser.add_argument("--config-hash", default=None, help="Optional config hash override; defaults to the captured bundle config_hash")
    return parser


def generate_run_id() -> str:
    return f"replay_{uuid.uuid4().hex[:12]}"


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging()
    summary = replay_bundle(
        bundle_dir=args.bundle_dir,
        db_path=args.db_path,
        run_id=args.run_id,
        config_hash=args.config_hash,
    )
    logger = get_logger("short_horizon.replay_runner", run_id=summary.run_id)
    logger.info(
        "replay_runner_completed",
        run_id=summary.run_id,
        input_events=summary.event_count,
        order_intents=summary.order_intents,
        synthetic_order_events=summary.synthetic_order_events,
        db_path=str(summary.db_path),
    )


def _resolve_bundle_file(bundle_dir: Path, files: dict[str, Any], key: str) -> Path:
    spec = files.get(key)
    if not isinstance(spec, dict):
        raise ValueError(f"Replay bundle manifest is missing file spec for {key!r}")
    relative_path = spec.get("path")
    if not isinstance(relative_path, str) or not relative_path.strip():
        raise ValueError(f"Replay bundle file spec for {key!r} is missing path")
    path = bundle_dir / relative_path
    if not path.exists():
        raise ValueError(f"Replay bundle file for {key!r} was not found: {path}")
    return path


def _load_jsonl_objects(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"Replay bundle line {line_no} in {path} is not a JSON object")
            rows.append(payload)
    return rows


def _validate_manifest_count(files: dict[str, Any], key: str, actual_count: int) -> None:
    spec = files.get(key)
    expected = None if not isinstance(spec, dict) else spec.get("count")
    if expected is None:
        return
    if int(expected) != int(actual_count):
        raise ValueError(
            f"Replay bundle count mismatch for {key!r}: manifest={expected}, actual={actual_count}"
        )


def _is_replay_input_event(event: object) -> bool:
    if isinstance(event, (OrderAccepted, OrderRejected, OrderCanceled)):
        source = str(getattr(event, "source", "") or "")
        return not source.startswith("execution.")
    if isinstance(event, OrderFilled):
        source = str(getattr(event, "source", "") or "")
        return not source.startswith("execution.")
    return True


def _match_place_order_request(request_payload: Any, order_request: Any) -> bool:
    if not isinstance(request_payload, dict):
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
    return expected is None


def _raise_captured_error_if_present(kind: str, record: dict[str, Any]) -> None:
    error = record.get("error")
    if not isinstance(error, dict) or not error:
        return
    error_type = _optional_str(error.get("type")) or "RuntimeError"
    error_message = _optional_str(error.get("message")) or f"captured {kind} failed"
    raise RuntimeError(f"Captured {kind} error {error_type}: {error_message}")


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


if __name__ == "__main__":
    main()
