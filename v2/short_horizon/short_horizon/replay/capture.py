from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from ..core.order_state import OrderState
from ..telemetry import get_logger


TERMINAL_ORDER_STATES = {
    OrderState.FILLED.value,
    OrderState.REJECTED.value,
    OrderState.CANCEL_CONFIRMED.value,
    OrderState.EXPIRED.value,
    OrderState.REPLACED.value,
}


class ReplayCaptureWriter:
    def __init__(self, capture_dir: str | Path, *, json_dumps: Callable[..., str] | None = None):
        self.capture_dir = Path(capture_dir)
        self.capture_dir.mkdir(parents=True, exist_ok=True)
        self._json_dumps = json_dumps or json.dumps
        self._venue_records: list[dict[str, Any]] = []
        self.logger = get_logger("short_horizon.replay.capture")

    def wrap_execution_client(self, client: Any) -> Any:
        if client is None or getattr(client, "_replay_capture_wrapped", False):
            return client

        original_place_order = getattr(client, "place_order")
        original_cancel_order = getattr(client, "cancel_order")
        original_get_order = getattr(client, "get_order")
        original_list_open_orders = getattr(client, "list_open_orders")

        def place_order(order_request):
            request_payload = _json_ready(order_request)
            key = {"client_order_id": getattr(order_request, "client_order_id", None)}
            try:
                result = original_place_order(order_request)
            except Exception as exc:
                self._safe_record(
                    kind="place_order",
                    key=key,
                    request=request_payload,
                    error={"type": type(exc).__name__, "message": str(exc)},
                )
                raise
            self._safe_record(
                kind="place_order",
                key=key,
                request=request_payload,
                response=_json_ready(result),
            )
            return result

        def cancel_order(order_id: str):
            key = {"venue_order_id": order_id}
            try:
                result = original_cancel_order(order_id)
            except Exception as exc:
                self._safe_record(
                    kind="cancel_order",
                    key=key,
                    request={"venue_order_id": order_id},
                    error={"type": type(exc).__name__, "message": str(exc)},
                )
                raise
            self._safe_record(
                kind="cancel_order",
                key=key,
                request={"venue_order_id": order_id},
                response=_json_ready(result),
            )
            return result

        def get_order(order_id: str):
            key = {"venue_order_id": order_id}
            try:
                result = original_get_order(order_id)
            except Exception as exc:
                self._safe_record(
                    kind="get_order",
                    key=key,
                    request={"venue_order_id": order_id},
                    error={"type": type(exc).__name__, "message": str(exc)},
                )
                raise
            self._safe_record(
                kind="get_order",
                key=key,
                request={"venue_order_id": order_id},
                response=_json_ready(result),
            )
            return result

        def list_open_orders(market_id: str | None = None):
            key = {"market_id": market_id}
            try:
                result = original_list_open_orders(market_id)
            except Exception as exc:
                self._safe_record(
                    kind="list_open_orders",
                    key=key,
                    request={"market_id": market_id},
                    error={"type": type(exc).__name__, "message": str(exc)},
                )
                raise
            self._safe_record(
                kind="list_open_orders",
                key=key,
                request={"market_id": market_id},
                response=_json_ready(result),
            )
            return result

        setattr(client, "place_order", place_order)
        setattr(client, "cancel_order", cancel_order)
        setattr(client, "get_order", get_order)
        setattr(client, "list_open_orders", list_open_orders)
        setattr(client, "_replay_capture_wrapped", True)
        return client

    def captured_venue_records(self) -> list[dict[str, Any]]:
        return list(self._venue_records)

    def write_bundle(self, *, db_path: str | Path, run_id: str) -> dict[str, Any]:
        db_path = Path(db_path)
        bundle = _load_bundle_rows(db_path=db_path, run_id=run_id)

        events_path = self.capture_dir / "events_log.jsonl"
        market_states_path = self.capture_dir / "market_state_snapshots.jsonl"
        venue_path = self.capture_dir / "venue_responses.jsonl"
        orders_path = self.capture_dir / "orders_final.jsonl"
        fills_path = self.capture_dir / "fills_final.jsonl"
        manifest_path = self.capture_dir / "manifest.json"

        _write_jsonl(events_path, bundle["events_log"], json_dumps=self._json_dumps)
        _write_jsonl(market_states_path, bundle["market_state_snapshots"], json_dumps=self._json_dumps)
        _write_jsonl(venue_path, self._venue_records, json_dumps=self._json_dumps)
        _write_jsonl(orders_path, bundle["orders_final"], json_dumps=self._json_dumps)
        _write_jsonl(fills_path, bundle["fills_final"], json_dumps=self._json_dumps)

        manifest = {
            "run_id": bundle["run"]["run_id"],
            "strategy_id": bundle["run"].get("strategy_id"),
            "config_hash": bundle["run"].get("config_hash"),
            "wall_clock_started_at": bundle["run"].get("started_at"),
            "wall_clock_finished_at": bundle["run"].get("finished_at"),
            "source_db_path": str(db_path),
            "files": {
                "events_log": {"path": events_path.name, "count": len(bundle["events_log"])},
                "market_state_snapshots": {"path": market_states_path.name, "count": len(bundle["market_state_snapshots"])},
                "venue_responses": {"path": venue_path.name, "count": len(self._venue_records)},
                "orders_final": {"path": orders_path.name, "count": len(bundle["orders_final"])},
                "fills_final": {"path": fills_path.name, "count": len(bundle["fills_final"])},
            },
        }
        manifest_path.write_text(self._json_dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return manifest

    def _safe_record(
        self,
        *,
        kind: str,
        key: dict[str, Any],
        request: dict[str, Any],
        response: Any | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        try:
            self._record_venue_response(kind=kind, key=key, request=request, response=response, error=error)
        except Exception as exc:
            self.logger.warning(
                "replay_capture_record_failed",
                capture_dir=str(self.capture_dir),
                kind=kind,
                error=str(exc),
            )

    def _record_venue_response(
        self,
        *,
        kind: str,
        key: dict[str, Any],
        request: dict[str, Any],
        response: Any | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        record = {
            "seq": len(self._venue_records) + 1,
            "kind": kind,
            "key": _json_ready(key),
            "request": _json_ready(request),
            "response": _json_ready(response),
            "error": _json_ready(error),
        }
        self._venue_records.append(record)


def _load_bundle_rows(*, db_path: Path, run_id: str) -> dict[str, Any]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        run_row = conn.execute(
            "SELECT run_id, strategy_id, config_hash, started_at, finished_at FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if run_row is None:
            raise ValueError(f"Run {run_id!r} was not found in {db_path}")

        event_rows = conn.execute(
            "SELECT payload_json FROM events_log WHERE run_id = ? ORDER BY seq ASC",
            (run_id,),
        ).fetchall()
        events_log = [json.loads(str(row["payload_json"])) for row in event_rows]
        market_state_snapshots = [row for row in events_log if row.get("event_type") == "MarketStateUpdate"]

        order_rows = conn.execute(
            """
            SELECT * FROM orders
            WHERE run_id = ? AND state IN ({states})
            ORDER BY last_state_change_at ASC, intent_created_at ASC, order_id ASC
            """.format(states=",".join("?" for _ in TERMINAL_ORDER_STATES)),
            (run_id, *sorted(TERMINAL_ORDER_STATES)),
        ).fetchall()
        fill_rows = conn.execute(
            "SELECT * FROM fills WHERE run_id = ? ORDER BY filled_at ASC, fill_id ASC",
            (run_id,),
        ).fetchall()

        return {
            "run": dict(run_row),
            "events_log": [dict(row) for row in events_log],
            "market_state_snapshots": [dict(row) for row in market_state_snapshots],
            "orders_final": [dict(row) for row in order_rows],
            "fills_final": [dict(row) for row in fill_rows],
        }
    finally:
        conn.close()


def _write_jsonl(path: Path, rows: list[dict[str, Any]], *, json_dumps: Callable[..., str]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json_dumps(row, sort_keys=True))
            handle.write("\n")


def _json_ready(value: Any) -> Any:
    if value is None:
        return None
    if is_dataclass(value):
        return _json_ready(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return value


__all__ = ["ReplayCaptureWriter"]
