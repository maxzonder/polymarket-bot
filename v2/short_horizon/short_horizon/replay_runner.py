from __future__ import annotations

import argparse
import json
import sqlite3
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import ShortHorizonConfig
from .core.clock import ReplayClock
from .core.events import OrderAccepted, OrderCanceled, OrderFilled, OrderRejected
from .core.runtime import StrategyRuntime
from .replay import ReplayEventSource
from .replay.venue_client import CapturedResponseExecutionClient
from .runner import RunnerSummary, drive_runtime_events
from .storage import RunContext, SQLiteRuntimeStore
from .strategies import ShortHorizon15mTouchStrategy
from .telemetry import configure_logging, get_logger


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


@dataclass(frozen=True)
class ReplayComparisonReport:
    live_run_id: str
    replay_run_id: str
    order_intent_diffs: tuple[str, ...] = ()
    skip_decision_diffs: tuple[str, ...] = ()
    cancel_intent_diffs: tuple[str, ...] = ()
    terminal_outcome_diffs: tuple[str, ...] = ()

    @property
    def matched(self) -> bool:
        return not any(
            (
                self.order_intent_diffs,
                self.skip_decision_diffs,
                self.cancel_intent_diffs,
                self.terminal_outcome_diffs,
            )
        )

    @property
    def diff_count(self) -> int:
        return sum(
            len(section)
            for section in (
                self.order_intent_diffs,
                self.skip_decision_diffs,
                self.cancel_intent_diffs,
                self.terminal_outcome_diffs,
            )
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
            execution_client=CapturedResponseExecutionClient(bundle.venue_responses),
        )
    finally:
        store = runtime.store
        close = getattr(store, "close", None)
        if callable(close):
            close()


def compare_bundle_to_replay(
    *,
    bundle_dir: str | Path,
    db_path: str | Path,
    replay_run_id: str | None = None,
) -> ReplayComparisonReport:
    bundle = load_replay_bundle(bundle_dir)
    effective_replay_run_id = replay_run_id or _optional_str(bundle.manifest.get("run_id")) or bundle.manifest.get("run_id") or "replay"
    replay_truth = _load_replay_db_truth(db_path=Path(db_path), run_id=str(effective_replay_run_id))

    live_order_intents = _extract_order_intents(bundle.orders_final)
    replay_order_intents = _extract_order_intents(replay_truth["orders"])
    live_skip_decisions = _extract_skip_decisions(bundle.events)
    replay_skip_decisions = _extract_skip_decisions(replay_truth["events"])
    live_cancel_intents = _extract_cancel_intents(events=bundle.events, orders=bundle.orders_final)
    replay_cancel_intents = _extract_cancel_intents(events=replay_truth["events"], orders=replay_truth["orders"])
    live_terminal_outcomes = _extract_terminal_outcomes(bundle.orders_final)
    replay_terminal_outcomes = _extract_terminal_outcomes(replay_truth["orders"])

    return ReplayComparisonReport(
        live_run_id=_optional_str(bundle.manifest.get("run_id")) or "live",
        replay_run_id=str(effective_replay_run_id),
        order_intent_diffs=tuple(
            _compare_bucketed_sequences(
                category="order_intents",
                live_records=live_order_intents,
                replay_records=replay_order_intents,
            )
        ),
        skip_decision_diffs=tuple(
            _compare_bucketed_sequences(
                category="skip_decisions",
                live_records=live_skip_decisions,
                replay_records=replay_skip_decisions,
            )
        ),
        cancel_intent_diffs=tuple(
            _compare_bucketed_sequences(
                category="cancel_intents",
                live_records=live_cancel_intents,
                replay_records=replay_cancel_intents,
            )
        ),
        terminal_outcome_diffs=tuple(
            _compare_terminal_outcomes(
                live_outcomes=live_terminal_outcomes,
                replay_outcomes=replay_terminal_outcomes,
            )
        ),
    )


def render_comparison_report(report: ReplayComparisonReport) -> str:
    lines = [
        f"Replay comparison: live_run_id={report.live_run_id} replay_run_id={report.replay_run_id}",
        f"Result: {'MATCH' if report.matched else 'DIFF'}",
    ]
    sections = (
        ("Order intents", report.order_intent_diffs),
        ("Skip decisions", report.skip_decision_diffs),
        ("Cancel intents", report.cancel_intent_diffs),
        ("Terminal outcomes", report.terminal_outcome_diffs),
    )
    for title, diffs in sections:
        lines.append(f"{title}: {'ok' if not diffs else f'{len(diffs)} diff(s)'}")
        for diff in diffs:
            lines.append(f"  - {diff}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay a captured short-horizon bundle into a fresh SQLite run")
    parser.add_argument("bundle_dir", help="Path to a Phase 4 replay capture bundle directory")
    parser.add_argument("db_path", help="SQLite DB path for replay outputs")
    parser.add_argument("--run-id", default=None, help="Optional explicit run_id; defaults to the captured bundle run_id")
    parser.add_argument("--config-hash", default=None, help="Optional config hash override; defaults to the captured bundle config_hash")
    parser.add_argument("--compare", action="store_true", help="Compare replay outputs against the captured live truth bundle and exit nonzero on diff")
    parser.add_argument("--comparison-report-path", default=None, help="Optional path to write the human-readable comparison report")
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
    if args.compare:
        report = compare_bundle_to_replay(
            bundle_dir=args.bundle_dir,
            db_path=args.db_path,
            replay_run_id=summary.run_id,
        )
        rendered = render_comparison_report(report)
        print(rendered)
        if args.comparison_report_path:
            Path(args.comparison_report_path).write_text(rendered + "\n", encoding="utf-8")
        logger.info(
            "replay_comparison_completed",
            live_run_id=report.live_run_id,
            replay_run_id=report.replay_run_id,
            matched=report.matched,
            diff_count=report.diff_count,
        )
        if not report.matched:
            raise SystemExit(1)


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


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


@dataclass(frozen=True)
class _TimedRecord:
    timestamp: str
    label: str


def _load_replay_db_truth(*, db_path: Path, run_id: str) -> dict[str, list[dict[str, Any]]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        run_row = conn.execute("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if run_row is None:
            raise ValueError(f"Replay run {run_id!r} was not found in {db_path}")
        orders = [
            dict(row)
            for row in conn.execute(
                """
                SELECT * FROM orders
                WHERE run_id = ?
                ORDER BY intent_created_at ASC, order_id ASC
                """,
                (run_id,),
            ).fetchall()
        ]
        events = []
        for row in conn.execute(
            """
            SELECT seq, payload_json FROM events_log
            WHERE run_id = ?
            ORDER BY seq ASC
            """,
            (run_id,),
        ).fetchall():
            payload = json.loads(str(row["payload_json"]))
            if isinstance(payload, dict):
                payload.setdefault("seq", row["seq"])
                events.append(payload)
        return {"orders": orders, "events": events}
    finally:
        conn.close()


def _extract_order_intents(orders: list[dict[str, Any]]) -> list[_TimedRecord]:
    records: list[_TimedRecord] = []
    for row in sorted(orders, key=lambda item: (str(item.get("intent_created_at") or ""), str(item.get("order_id") or ""))):
        price = _optional_float(row.get("price"))
        size = _optional_float(row.get("size"))
        records.append(
            _TimedRecord(
                timestamp=_normalize_timestamp(row.get("intent_created_at") or row.get("last_state_change_at")),
                label=(
                    f"market_id={row.get('market_id')} token_id={row.get('token_id')} "
                    f"level={_fmt_float(price)} entry_price={_fmt_float(price)} "
                    f"notional_usdc={_fmt_float(None if price is None or size is None else price * size)}"
                ),
            )
        )
    return records


def _extract_skip_decisions(events: list[Any]) -> list[_TimedRecord]:
    records: list[_TimedRecord] = []
    for payload in events:
        if isinstance(payload, dict):
            if str(payload.get("event_type") or "") != "SkipDecision":
                continue
            timestamp = str(payload.get("event_time") or payload.get("event_time_ms") or "")
            reason = payload.get("reason")
            market_id = payload.get("market_id")
            token_id = payload.get("token_id")
            level = _optional_float(payload.get("level"))
        else:
            if type(payload).__name__ != "SkipDecision":
                continue
            timestamp = str(getattr(payload, "event_time_ms", ""))
            reason = getattr(payload, "reason", None)
            market_id = getattr(payload, "market_id", None)
            token_id = getattr(payload, "token_id", None)
            level = _optional_float(getattr(payload, "level", None))
        records.append(
            _TimedRecord(
                timestamp=_normalize_timestamp(timestamp),
                label=(
                    f"reason={reason} market_id={market_id} "
                    f"token_id={token_id} level={_fmt_float(level)}"
                ),
            )
        )
    return records


def _extract_cancel_intents(*, events: list[Any], orders: list[dict[str, Any]]) -> list[_TimedRecord]:
    orders_by_order_id = {
        str(row.get("order_id")): row
        for row in orders
        if row.get("order_id") is not None
    }
    orders_by_client_order_id = {
        str(row.get("client_order_id")): row
        for row in orders
        if row.get("client_order_id") is not None
    }
    records: list[_TimedRecord] = []
    for payload in events:
        if isinstance(payload, dict):
            if str(payload.get("event_type") or "") != "OrderCanceled":
                continue
            order_id = payload.get("order_id")
            client_order_id = payload.get("client_order_id")
            timestamp = str(payload.get("event_time") or payload.get("event_time_ms") or "")
            cancel_reason = _optional_str(payload.get("cancel_reason"))
        else:
            if not isinstance(payload, OrderCanceled):
                continue
            order_id = payload.order_id
            client_order_id = payload.client_order_id
            timestamp = str(payload.event_time_ms)
            cancel_reason = _optional_str(payload.cancel_reason)
        order_row = orders_by_order_id.get(str(order_id))
        if order_row is None and client_order_id is not None:
            order_row = orders_by_client_order_id.get(str(client_order_id))
        records.append(
            _TimedRecord(
                timestamp=_normalize_timestamp(timestamp),
                label=(
                    f"venue_order_id={_optional_str(None if order_row is None else order_row.get('venue_order_id'))} "
                    f"reason={cancel_reason}"
                ),
            )
        )
    return records


def _extract_terminal_outcomes(orders: list[dict[str, Any]]) -> dict[str, tuple[str | None, float | None]]:
    outcomes: dict[str, tuple[str | None, float | None]] = {}
    for row in sorted(orders, key=lambda item: (str(item.get("last_state_change_at") or ""), str(item.get("order_id") or ""))):
        outcomes[_stable_order_identity(row)] = (
            _optional_str(row.get("state")),
            _optional_float(row.get("cumulative_filled_size")),
        )
    return outcomes


def _compare_bucketed_sequences(*, category: str, live_records: list[_TimedRecord], replay_records: list[_TimedRecord]) -> list[str]:
    live_buckets = _bucketize_records(live_records)
    replay_buckets = _bucketize_records(replay_records)
    diffs: list[str] = []
    max_len = max(len(live_buckets), len(replay_buckets))
    for index in range(max_len):
        live_bucket = live_buckets[index] if index < len(live_buckets) else None
        replay_bucket = replay_buckets[index] if index < len(replay_buckets) else None
        if live_bucket is None:
            diffs.append(
                f"{category}: extra replay bucket at index={index} timestamp={replay_bucket[0]} values={replay_bucket[1]}"
            )
            continue
        if replay_bucket is None:
            diffs.append(
                f"{category}: missing replay bucket at index={index} timestamp={live_bucket[0]} values={live_bucket[1]}"
            )
            continue
        if live_bucket[0] != replay_bucket[0]:
            diffs.append(
                f"{category}: timestamp mismatch at index={index} live={live_bucket[0]} replay={replay_bucket[0]}"
            )
            continue
        if live_bucket[1] != replay_bucket[1]:
            diffs.append(
                f"{category}: values mismatch at timestamp={live_bucket[0]} live={live_bucket[1]} replay={replay_bucket[1]}"
            )
    return diffs


def _compare_terminal_outcomes(
    *,
    live_outcomes: dict[str, tuple[str | None, float | None]],
    replay_outcomes: dict[str, tuple[str | None, float | None]],
) -> list[str]:
    diffs: list[str] = []
    all_keys = sorted(set(live_outcomes) | set(replay_outcomes))
    for key in all_keys:
        live_value = live_outcomes.get(key)
        replay_value = replay_outcomes.get(key)
        if live_value is None:
            diffs.append(
                f"terminal_outcomes: extra replay order {key} state={replay_value[0]} filled_qty={_fmt_float(replay_value[1])}"
            )
            continue
        if replay_value is None:
            diffs.append(
                f"terminal_outcomes: missing replay order {key} expected_state={live_value[0]} expected_filled_qty={_fmt_float(live_value[1])}"
            )
            continue
        if live_value != replay_value:
            diffs.append(
                f"terminal_outcomes: mismatch for {key} live=(state={live_value[0]}, filled_qty={_fmt_float(live_value[1])}) "
                f"replay=(state={replay_value[0]}, filled_qty={_fmt_float(replay_value[1])})"
            )
    return diffs


def _bucketize_records(records: list[_TimedRecord]) -> list[tuple[str, tuple[str, ...]]]:
    buckets: list[tuple[str, tuple[str, ...]]] = []
    grouped: defaultdict[str, list[str]] = defaultdict(list)
    order: list[str] = []
    for record in records:
        if record.timestamp not in grouped:
            order.append(record.timestamp)
        grouped[record.timestamp].append(record.label)
    for timestamp in order:
        buckets.append((timestamp, tuple(sorted(grouped[timestamp]))))
    return buckets


def _stable_order_identity(row: dict[str, Any]) -> str:
    client_order_id = _optional_str(row.get("client_order_id"))
    if client_order_id:
        return client_order_id
    order_id = _optional_str(row.get("order_id"))
    if order_id:
        return order_id
    return "<unknown-order>"


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _normalize_timestamp(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(int(value))
    text = str(value).strip()
    if not text:
        return ""
    try:
        return str(int(float(text)))
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return str(int(dt.timestamp() * 1000))


def _fmt_float(value: float | None) -> str:
    if value is None:
        return "None"
    return f"{float(value):.6f}"


if __name__ == "__main__":
    main()
