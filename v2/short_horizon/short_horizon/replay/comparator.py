from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.events import OrderCanceled, OrderIntentEvent, SkipDecisionEvent
from ..telemetry import configure_logging, get_logger
from ..replay_runner import load_replay_bundle


@dataclass(frozen=True)
class ReplayComparisonEntry:
    key: str
    values: tuple[str, ...]


@dataclass(frozen=True)
class ReplayComparisonMismatch:
    key: str
    live: tuple[str, ...]
    replay: tuple[str, ...]


@dataclass(frozen=True)
class ReplayComparisonSection:
    matched: tuple[ReplayComparisonEntry, ...] = ()
    mismatched: tuple[ReplayComparisonMismatch, ...] = ()
    live_only: tuple[ReplayComparisonEntry, ...] = ()
    replay_only: tuple[ReplayComparisonEntry, ...] = ()

    @property
    def diff_count(self) -> int:
        return len(self.mismatched) + len(self.live_only) + len(self.replay_only)


@dataclass(frozen=True)
class ReplayComparisonReport:
    live_run_id: str
    replay_run_id: str
    order_intents: ReplayComparisonSection = ReplayComparisonSection()
    skip_decisions: ReplayComparisonSection = ReplayComparisonSection()
    cancel_intents: ReplayComparisonSection = ReplayComparisonSection()
    terminal_outcomes: ReplayComparisonSection = ReplayComparisonSection()

    @property
    def matched(self) -> bool:
        return self.diff_count == 0

    @property
    def diff_count(self) -> int:
        return sum(
            section.diff_count
            for section in (
                self.order_intents,
                self.skip_decisions,
                self.cancel_intents,
                self.terminal_outcomes,
            )
        )


@dataclass(frozen=True)
class _TimedRecord:
    key: str
    values: tuple[str, ...]


def compare_bundle_to_replay(
    *,
    bundle_dir: str | Path,
    db_path: str | Path,
    replay_run_id: str | None = None,
) -> ReplayComparisonReport:
    bundle = load_replay_bundle(bundle_dir)
    effective_replay_run_id = replay_run_id or _optional_str(bundle.manifest.get("run_id")) or "replay"
    replay_truth = _load_replay_db_truth(db_path=Path(db_path), run_id=str(effective_replay_run_id))

    live_has_order_intents = _contains_event_type(bundle.events, "OrderIntent", OrderIntentEvent)
    live_has_skip_decisions = _contains_event_type(bundle.events, "SkipDecision", SkipDecisionEvent)

    if live_has_order_intents:
        live_order_intents = _extract_order_intents_from_events(bundle.events)
        replay_order_intents = _extract_order_intents_from_events(replay_truth["events"])
        if not replay_order_intents:
            replay_order_intents = _extract_order_intents_from_orders(replay_truth["orders"])
    else:
        live_order_intents = _extract_order_intents_from_orders(bundle.orders_final)
        replay_order_intents = _extract_order_intents_from_orders(replay_truth["orders"])

    if live_has_skip_decisions:
        live_skip_decisions = _extract_skip_decisions(bundle.events)
        replay_skip_decisions = _extract_skip_decisions(replay_truth["events"])
    else:
        live_skip_decisions = []
        replay_skip_decisions = []

    return ReplayComparisonReport(
        live_run_id=_optional_str(bundle.manifest.get("run_id")) or "live",
        replay_run_id=str(effective_replay_run_id),
        order_intents=_compare_timed_sequences(
            live_records=live_order_intents,
            replay_records=replay_order_intents,
        ),
        skip_decisions=_compare_timed_sequences(
            live_records=live_skip_decisions,
            replay_records=replay_skip_decisions,
        ),
        cancel_intents=_compare_timed_sequences(
            live_records=_extract_cancel_intents(events=bundle.events, orders=bundle.orders_final),
            replay_records=_extract_cancel_intents(events=replay_truth["events"], orders=replay_truth["orders"]),
        ),
        terminal_outcomes=_compare_terminal_outcomes(
            live_outcomes=_extract_terminal_outcomes(bundle.orders_final),
            replay_outcomes=_extract_terminal_outcomes(replay_truth["orders"]),
        ),
    )


def render_comparison_report(report: ReplayComparisonReport) -> str:
    lines = [
        f"Replay comparison: live_run_id={report.live_run_id} replay_run_id={report.replay_run_id}",
        f"Result: {'MATCH' if report.matched else 'DIFF'}",
    ]
    sections = (
        ("Order intents", report.order_intents, "event_time"),
        ("Skip decisions", report.skip_decisions, "event_time"),
        ("Cancel intents", report.cancel_intents, "event_time"),
        ("Terminal outcomes", report.terminal_outcomes, "order"),
    )
    for title, section, key_label in sections:
        lines.append(
            f"{title}: {'ok' if section.diff_count == 0 else f'{section.diff_count} diff(s)'} "
            f"(matched={len(section.matched)} mismatched={len(section.mismatched)} "
            f"live_only={len(section.live_only)} replay_only={len(section.replay_only)})"
        )
        if section.mismatched:
            for item in section.mismatched:
                lines.append(
                    f"  - mismatch {key_label}={_format_report_key(item.key, key_label)}"
                    f": live[{_format_report_values(item.live)}] replay[{_format_report_values(item.replay)}]"
                )
        if section.live_only:
            for item in section.live_only:
                lines.append(
                    f"  - live-only {key_label}={_format_report_key(item.key, key_label)}"
                    f": {_format_report_values(item.values)}"
                )
        if section.replay_only:
            for item in section.replay_only:
                lines.append(
                    f"  - replay-only {key_label}={_format_report_key(item.key, key_label)}"
                    f": {_format_report_values(item.values)}"
                )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare a captured replay bundle against replay SQLite outputs")
    parser.add_argument("bundle_dir", help="Path to a Phase 4 replay capture bundle directory")
    parser.add_argument("db_path", help="SQLite DB path containing replay outputs")
    parser.add_argument("--replay-run-id", default=None, help="Optional explicit replay run_id; defaults to the captured bundle run_id")
    parser.add_argument("--report-path", default=None, help="Optional path to write the human-readable comparison report")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging()
    report = compare_bundle_to_replay(
        bundle_dir=args.bundle_dir,
        db_path=args.db_path,
        replay_run_id=args.replay_run_id,
    )
    rendered = render_comparison_report(report)
    print(rendered)
    if args.report_path:
        Path(args.report_path).write_text(rendered + "\n", encoding="utf-8")
    logger = get_logger("short_horizon.replay.comparator", run_id=report.replay_run_id)
    logger.info(
        "replay_comparison_completed",
        live_run_id=report.live_run_id,
        replay_run_id=report.replay_run_id,
        matched=report.matched,
        diff_count=report.diff_count,
    )
    if not report.matched:
        raise SystemExit(1)


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


def _extract_order_intents_from_events(events: list[Any]) -> list[_TimedRecord]:
    records: list[_TimedRecord] = []
    for payload in events:
        if isinstance(payload, dict):
            if str(payload.get("event_type") or "") != "OrderIntent":
                continue
            timestamp = payload.get("event_time") or payload.get("event_time_ms")
            market_id = payload.get("market_id")
            token_id = payload.get("token_id")
            level = _optional_float(payload.get("level"))
            entry_price = _optional_float(payload.get("entry_price"))
            notional_usdc = _optional_float(payload.get("notional_usdc"))
            lifecycle_fraction = _optional_float(payload.get("lifecycle_fraction"))
            reason = _optional_str(payload.get("reason"))
        elif isinstance(payload, OrderIntentEvent):
            timestamp = payload.event_time_ms
            market_id = payload.market_id
            token_id = payload.token_id
            level = _optional_float(payload.level)
            entry_price = _optional_float(payload.entry_price)
            notional_usdc = _optional_float(payload.notional_usdc)
            lifecycle_fraction = _optional_float(payload.lifecycle_fraction)
            reason = _optional_str(payload.reason)
        else:
            continue
        records.append(
            _TimedRecord(
                key=_normalize_timestamp(timestamp),
                values=(
                    f"market_id={market_id}",
                    f"token_id={token_id}",
                    f"level={_fmt_float(level)}",
                    f"entry_price={_fmt_float(entry_price)}",
                    f"notional_usdc={_fmt_float(notional_usdc)}",
                    f"lifecycle_fraction={_fmt_float(lifecycle_fraction)}",
                    f"reason={reason}",
                ),
            )
        )
    return records

def _extract_order_intents_from_orders(orders: list[dict[str, Any]]) -> list[_TimedRecord]:
    fallback: list[_TimedRecord] = []
    for row in sorted(orders, key=lambda item: (str(item.get("intent_created_at") or ""), str(item.get("order_id") or ""))):
        price = _optional_float(row.get("price"))
        size = _optional_float(row.get("size"))
        level = _extract_legacy_level(row)
        fallback.append(
            _TimedRecord(
                key=_normalize_timestamp(row.get("intent_created_at") or row.get("last_state_change_at")),
                values=(
                    f"market_id={row.get('market_id')}",
                    f"token_id={row.get('token_id')}",
                    f"level={_fmt_float(level)}",
                    f"entry_price={_fmt_float(price)}",
                    f"notional_usdc={_fmt_float(None if price is None or size is None else price * size)}",
                    "lifecycle_fraction=None",
                    "reason=None",
                ),
            )
        )
    return fallback


def _contains_event_type(events: list[Any], event_type: str, cls: type[object]) -> bool:
    for payload in events:
        if isinstance(payload, dict) and str(payload.get("event_type") or "") == event_type:
            return True
        if isinstance(payload, cls):
            return True
    return False


def _extract_skip_decisions(events: list[Any]) -> list[_TimedRecord]:
    records: list[_TimedRecord] = []
    for payload in events:
        if isinstance(payload, dict):
            if str(payload.get("event_type") or "") != "SkipDecision":
                continue
            timestamp = payload.get("event_time") or payload.get("event_time_ms")
            reason = payload.get("reason")
            market_id = payload.get("market_id")
            token_id = payload.get("token_id")
            level = _optional_float(payload.get("level"))
            details = _optional_str(payload.get("details"))
        elif isinstance(payload, SkipDecisionEvent):
            timestamp = payload.event_time_ms
            reason = payload.reason
            market_id = payload.market_id
            token_id = payload.token_id
            level = _optional_float(payload.level)
            details = _optional_str(payload.details)
        else:
            continue
        records.append(
            _TimedRecord(
                key=_normalize_timestamp(timestamp),
                values=(
                    f"reason={reason}",
                    f"market_id={market_id}",
                    f"token_id={token_id}",
                    f"level={_fmt_float(level)}",
                    f"details={details}",
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
            timestamp = payload.get("event_time") or payload.get("event_time_ms")
            cancel_reason = _optional_str(payload.get("cancel_reason"))
        elif isinstance(payload, OrderCanceled):
            order_id = payload.order_id
            client_order_id = payload.client_order_id
            timestamp = payload.event_time_ms
            cancel_reason = _optional_str(payload.cancel_reason)
        else:
            continue
        order_row = orders_by_order_id.get(str(order_id))
        if order_row is None and client_order_id is not None:
            order_row = orders_by_client_order_id.get(str(client_order_id))
        records.append(
            _TimedRecord(
                key=_normalize_timestamp(timestamp),
                values=(
                    f"venue_order_id={_optional_str(None if order_row is None else order_row.get('venue_order_id'))}",
                    f"reason={cancel_reason}",
                ),
            )
        )
    return records


def _extract_terminal_outcomes(orders: list[dict[str, Any]]) -> dict[str, tuple[str, ...]]:
    outcomes: dict[str, tuple[str, ...]] = {}
    for row in sorted(orders, key=lambda item: (str(item.get("last_state_change_at") or ""), str(item.get("order_id") or ""))):
        outcomes[_stable_order_identity(row)] = (
            f"state={_optional_str(row.get('state'))}",
            f"filled_qty={_fmt_float(_optional_float(row.get('cumulative_filled_size')))}",
        )
    return outcomes


def _compare_timed_sequences(*, live_records: list[_TimedRecord], replay_records: list[_TimedRecord]) -> ReplayComparisonSection:
    live_buckets = _bucketize_records(live_records)
    replay_buckets = _bucketize_records(replay_records)
    matched: list[ReplayComparisonEntry] = []
    mismatched: list[ReplayComparisonMismatch] = []
    live_only: list[ReplayComparisonEntry] = []
    replay_only: list[ReplayComparisonEntry] = []

    max_len = max(len(live_buckets), len(replay_buckets))
    for index in range(max_len):
        live_bucket = live_buckets[index] if index < len(live_buckets) else None
        replay_bucket = replay_buckets[index] if index < len(replay_buckets) else None
        if live_bucket is None:
            replay_only.append(ReplayComparisonEntry(key=replay_bucket[0], values=replay_bucket[1]))
            continue
        if replay_bucket is None:
            live_only.append(ReplayComparisonEntry(key=live_bucket[0], values=live_bucket[1]))
            continue
        if live_bucket[0] != replay_bucket[0]:
            live_only.append(ReplayComparisonEntry(key=live_bucket[0], values=live_bucket[1]))
            replay_only.append(ReplayComparisonEntry(key=replay_bucket[0], values=replay_bucket[1]))
            continue
        if live_bucket[1] == replay_bucket[1]:
            matched.append(ReplayComparisonEntry(key=live_bucket[0], values=live_bucket[1]))
            continue
        mismatched.append(
            ReplayComparisonMismatch(
                key=live_bucket[0],
                live=live_bucket[1],
                replay=replay_bucket[1],
            )
        )

    return ReplayComparisonSection(
        matched=tuple(matched),
        mismatched=tuple(mismatched),
        live_only=tuple(live_only),
        replay_only=tuple(replay_only),
    )


def _compare_terminal_outcomes(
    *,
    live_outcomes: dict[str, tuple[str, ...]],
    replay_outcomes: dict[str, tuple[str, ...]],
) -> ReplayComparisonSection:
    matched: list[ReplayComparisonEntry] = []
    mismatched: list[ReplayComparisonMismatch] = []
    live_only: list[ReplayComparisonEntry] = []
    replay_only: list[ReplayComparisonEntry] = []
    for key in sorted(set(live_outcomes) | set(replay_outcomes)):
        live_value = live_outcomes.get(key)
        replay_value = replay_outcomes.get(key)
        if live_value is None:
            replay_only.append(ReplayComparisonEntry(key=key, values=replay_value))
            continue
        if replay_value is None:
            live_only.append(ReplayComparisonEntry(key=key, values=live_value))
            continue
        if live_value == replay_value:
            matched.append(ReplayComparisonEntry(key=key, values=live_value))
            continue
        mismatched.append(ReplayComparisonMismatch(key=key, live=live_value, replay=replay_value))
    return ReplayComparisonSection(
        matched=tuple(matched),
        mismatched=tuple(mismatched),
        live_only=tuple(live_only),
        replay_only=tuple(replay_only),
    )


def _bucketize_records(records: list[_TimedRecord]) -> list[tuple[str, tuple[str, ...]]]:
    buckets: list[tuple[str, tuple[str, ...]]] = []
    grouped: defaultdict[str, list[tuple[str, ...]]] = defaultdict(list)
    order: list[str] = []
    for record in records:
        if record.key not in grouped:
            order.append(record.key)
        grouped[record.key].append(record.values)
    for key in order:
        buckets.append((key, tuple(sorted(" | ".join(values) for values in grouped[key]))))
    return buckets


def _stable_order_identity(row: dict[str, Any]) -> str:
    client_order_id = _optional_str(row.get("client_order_id"))
    if client_order_id:
        return client_order_id
    order_id = _optional_str(row.get("order_id"))
    if order_id:
        return order_id
    return "<unknown-order>"


def _extract_legacy_level(row: dict[str, Any]) -> float | None:
    order_id = _optional_str(row.get("order_id"))
    if order_id:
        parts = order_id.split(":")
        if len(parts) >= 3:
            try:
                return float(parts[2])
            except ValueError:
                pass
    return _optional_float(row.get("price"))


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


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


def _format_report_values(values: tuple[str, ...]) -> str:
    return ", ".join(values)


def _format_report_key(key: str, key_label: str) -> str:
    if key_label != "event_time":
        return key
    iso_value = _timestamp_ms_to_iso(key)
    if iso_value is None:
        return key
    return f"{key} ({iso_value})"


def _timestamp_ms_to_iso(value: str) -> str | None:
    text = str(value).strip()
    if not text or not text.isdigit():
        return None
    try:
        return datetime.fromtimestamp(int(text) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (OverflowError, ValueError):
        return None


if __name__ == "__main__":
    main()
