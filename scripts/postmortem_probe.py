#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ProbePostmortem:
    db_path: Path
    log_path: Path | None
    run_id: str | None
    started_at: str | None
    finished_at: str | None
    first_event_time: str | None
    last_event_time: str | None
    runtime_minutes: float | None
    event_counts: Counter[str]
    order_state_counts: Counter[str]
    intents: int
    accepted: int
    fills: int
    rejects: int
    cumulative_stake_usdc: float
    resolved_pnl_usdc: float
    unresolved_cost_usdc: float
    skip_histogram: Counter[str]
    anti_chase_violations: dict[str, int]
    ladder_violations: dict[str, list[float]]
    ws_warning_counts: Counter[str]
    log_warning_counts: Counter[str]
    log_error_count: int
    closed_cleanly: bool

    def as_github_comment(self) -> str:
        lines = [
            f"## Probe postmortem: `{self.run_id or self.db_path.name}`",
            "",
            "Artifacts:",
            f"- DB: `{self.db_path}`",
        ]
        if self.log_path is not None:
            lines.append(f"- log: `{self.log_path}`")
        lines.extend(
            [
                "",
                "Runtime:",
                f"- started_at: `{self.started_at or 'unknown'}`",
                f"- finished_at: `{self.finished_at or 'still_open/unknown'}`",
                f"- event window: `{self.first_event_time or 'unknown'}` -> `{self.last_event_time or 'unknown'}`",
                f"- runtime_minutes: `{_fmt_float(self.runtime_minutes)}`",
                f"- clean completion: `{self.closed_cleanly}`",
                "",
                "Execution:",
                f"- intents: `{self.intents}`",
                f"- accepted: `{self.accepted}`",
                f"- fills: `{self.fills}`",
                f"- rejects: `{self.rejects}`",
                f"- order states: `{_format_counter(self.order_state_counts)}`",
                f"- cumulative filled stake: `{self.cumulative_stake_usdc:.4f} USDC`",
                f"- resolved-position PnL: `{self.resolved_pnl_usdc:.4f} USDC`",
                f"- unresolved filled cost: `{self.unresolved_cost_usdc:.4f} USDC`",
                "",
                "Skip decisions:",
            ]
        )
        lines.extend(_format_bullets(self.skip_histogram))
        lines.extend(
            [
                "",
                "Anti-chase check:",
                f"- <=1 order per market: `{'PASS' if not self.anti_chase_violations else 'FAIL'}`",
                f"- ladder price check: `{'PASS' if not self.ladder_violations else 'FAIL'}`",
            ]
        )
        if self.anti_chase_violations:
            for market_id, count in sorted(self.anti_chase_violations.items()):
                lines.append(f"  - market `{market_id}` has `{count}` orders")
        if self.ladder_violations:
            for market_id, prices in sorted(self.ladder_violations.items()):
                lines.append(f"  - market `{market_id}` prices `{prices}`")
        lines.extend(
            [
                "",
                "WS health:",
            ]
        )
        lines.extend(_format_bullets(self.ws_warning_counts))
        lines.extend(
            [
                f"- log warnings total: `{sum(self.log_warning_counts.values())}`",
                f"- log errors total: `{self.log_error_count}`",
                "",
                "Top event counts:",
            ]
        )
        for event, count in self.event_counts.most_common(12):
            lines.append(f"- `{event}`: `{count}`")
        return "\n".join(lines)


def analyze_probe(db_path: str | Path, *, log_path: str | Path | None = None) -> ProbePostmortem:
    db = Path(db_path)
    log = Path(log_path) if log_path is not None else infer_log_path(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    run_row = _fetch_one(conn, "SELECT * FROM runs ORDER BY started_at LIMIT 1")
    run_id = run_row["run_id"] if run_row is not None and "run_id" in run_row.keys() else None
    started_at = run_row["started_at"] if run_row is not None and "started_at" in run_row.keys() else None
    finished_at = run_row["finished_at"] if run_row is not None and "finished_at" in run_row.keys() else None

    first_event_time, last_event_time = _event_window(conn, run_id)
    runtime_minutes = _minutes_between(started_at or first_event_time, finished_at or last_event_time)
    event_counts = _event_counts(conn, run_id)
    skip_histogram = _skip_histogram(conn, run_id)
    orders = _rows(conn, "SELECT * FROM orders WHERE (? IS NULL OR run_id = ?) ORDER BY intent_created_at", (run_id, run_id))
    fills = _rows(conn, "SELECT * FROM fills WHERE (? IS NULL OR run_id = ?) ORDER BY filled_at", (run_id, run_id))
    resolved = _resolved_inventory_events(conn, run_id)

    order_state_counts = Counter(str(row["state"]) for row in orders)
    order_count_by_market = Counter(str(row["market_id"]) for row in orders)
    anti_chase_violations = {market_id: count for market_id, count in order_count_by_market.items() if count > 1}
    prices_by_market: dict[str, set[float]] = defaultdict(set)
    for row in orders:
        if row["price"] is not None:
            prices_by_market[str(row["market_id"])].add(float(row["price"]))
    ladder_violations = {market_id: sorted(prices) for market_id, prices in prices_by_market.items() if len(prices) > 1}

    cumulative_stake = sum(float(row["price"] or 0.0) * float(row["size"] or 0.0) + float(row["fee_paid_usdc"] or 0.0) for row in fills)
    resolved_pnl = sum(float(item.get("estimated_pnl_usdc") or 0.0) for item in resolved)
    resolved_keys = {(str(item.get("market_id")), str(item.get("token_id"))) for item in resolved}
    unresolved_cost = sum(
        float(row["price"] or 0.0) * float(row["size"] or 0.0) + float(row["fee_paid_usdc"] or 0.0)
        for row in fills
        if (str(row["market_id"]), str(row["token_id"])) not in resolved_keys
    )

    ws_warning_counts, log_warning_counts, log_error_count = _log_health(log)
    closed_cleanly = bool(event_counts.get("live_run_completed") or event_counts.get("live_stub_run_completed") or event_counts.get("replay_capture_bundle_written"))
    if log is not None and log.exists():
        # live_run_completed is a log event, not persisted in events_log.
        log_counts = _log_event_counts(log)
        closed_cleanly = closed_cleanly or bool(log_counts.get("live_run_completed") or log_counts.get("live_runner_completed"))
        event_counts.update(log_counts)

    return ProbePostmortem(
        db_path=db,
        log_path=log if log is not None and log.exists() else None,
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        first_event_time=first_event_time,
        last_event_time=last_event_time,
        runtime_minutes=runtime_minutes,
        event_counts=event_counts,
        order_state_counts=order_state_counts,
        intents=int(event_counts.get("OrderIntent", 0) + event_counts.get("order_intent_created", 0)),
        accepted=int(event_counts.get("OrderAccepted", 0) + event_counts.get("live_order_accepted", 0)),
        fills=len(fills),
        rejects=int(event_counts.get("OrderRejected", 0) + event_counts.get("order_rejected", 0)),
        cumulative_stake_usdc=cumulative_stake,
        resolved_pnl_usdc=resolved_pnl,
        unresolved_cost_usdc=unresolved_cost,
        skip_histogram=skip_histogram,
        anti_chase_violations=anti_chase_violations,
        ladder_violations=ladder_violations,
        ws_warning_counts=ws_warning_counts,
        log_warning_counts=log_warning_counts,
        log_error_count=log_error_count,
        closed_cleanly=closed_cleanly,
    )


def infer_log_path(db_path: Path) -> Path | None:
    name = db_path.name
    if name.endswith(".sqlite3"):
        candidate = db_path.parent.parent / "logs" / f"{name[:-8]}.log"
        if candidate.exists():
            return candidate
    return None


def _fetch_one(conn: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
    return conn.execute(query, params).fetchone()


def _rows(conn: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    return list(conn.execute(query, params).fetchall())


def _event_window(conn: sqlite3.Connection, run_id: str | None) -> tuple[str | None, str | None]:
    row = _fetch_one(
        conn,
        "SELECT MIN(event_time) AS first_event_time, MAX(event_time) AS last_event_time FROM events_log WHERE (? IS NULL OR run_id = ?)",
        (run_id, run_id),
    )
    if row is None:
        return None, None
    return row["first_event_time"], row["last_event_time"]


def _event_counts(conn: sqlite3.Connection, run_id: str | None) -> Counter[str]:
    counter: Counter[str] = Counter()
    for row in conn.execute(
        "SELECT event_type, COUNT(*) AS count FROM events_log WHERE (? IS NULL OR run_id = ?) GROUP BY event_type",
        (run_id, run_id),
    ):
        counter[str(row["event_type"])] = int(row["count"])
    return counter


def _skip_histogram(conn: sqlite3.Connection, run_id: str | None) -> Counter[str]:
    counter: Counter[str] = Counter()
    for row in conn.execute(
        "SELECT payload_json FROM events_log WHERE event_type = 'SkipDecision' AND (? IS NULL OR run_id = ?)",
        (run_id, run_id),
    ):
        try:
            payload = json.loads(row["payload_json"])
        except Exception:
            continue
        counter[str(payload.get("reason") or "unknown")] += 1
    return counter


def _resolved_inventory_events(conn: sqlite3.Connection, run_id: str | None) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in conn.execute(
        "SELECT payload_json FROM events_log WHERE event_type = 'MarketResolvedWithInventory' AND (? IS NULL OR run_id = ?)",
        (run_id, run_id),
    ):
        try:
            items.append(json.loads(row["payload_json"]))
        except Exception:
            continue
    return items


def _log_health(log_path: Path | None) -> tuple[Counter[str], Counter[str], int]:
    ws_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    error_count = 0
    if log_path is None or not log_path.exists():
        return ws_counts, warning_counts, error_count
    for event in _iter_log_json(log_path):
        name = str(event.get("event") or "unknown")
        if event.get("level") == "warning":
            warning_counts[name] += 1
        if event.get("level") == "error":
            error_count += 1
        if name in {
            "ws_disconnected",
            "user_ws_disconnected",
            "ws_subscribe_send_failed",
            "ws_unsubscribe_send_failed",
            "user_ws_subscribe_send_failed",
            "user_ws_unsubscribe_send_failed",
            "live_source_component_failed",
        }:
            ws_counts[name] += 1
    return ws_counts, warning_counts, error_count


def _log_event_counts(log_path: Path) -> Counter[str]:
    counter: Counter[str] = Counter()
    for event in _iter_log_json(log_path):
        name = event.get("event")
        if name:
            counter[str(name)] += 1
    return counter


def _iter_log_json(log_path: Path):
    with log_path.open(errors="replace") as fh:
        for line in fh:
            if not line.startswith("{"):
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _minutes_between(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    from datetime import datetime

    def parse(value: str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    try:
        return (parse(end) - parse(start)).total_seconds() / 60.0
    except Exception:
        return None


def _format_counter(counter: Counter[str]) -> str:
    if not counter:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counter.items()))


def _format_bullets(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- `{key}`: `{value}`" for key, value in counter.most_common()]


def _fmt_float(value: float | None) -> str:
    return "unknown" if value is None else f"{value:.2f}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a GitHub-comment-style postmortem for a short-horizon probe DB")
    parser.add_argument("db_path", help="Probe SQLite DB path")
    parser.add_argument("--log-path", default=None, help="Optional live_runner JSON log path; inferred from DB name when omitted")
    parser.add_argument("--json", action="store_true", help="Emit compact machine-readable summary instead of Markdown")
    args = parser.parse_args()
    report = analyze_probe(args.db_path, log_path=args.log_path)
    if args.json:
        print(json.dumps({
            "run_id": report.run_id,
            "runtime_minutes": report.runtime_minutes,
            "intents": report.intents,
            "fills": report.fills,
            "rejects": report.rejects,
            "cumulative_stake_usdc": report.cumulative_stake_usdc,
            "resolved_pnl_usdc": report.resolved_pnl_usdc,
            "unresolved_cost_usdc": report.unresolved_cost_usdc,
            "skip_histogram": dict(report.skip_histogram),
            "anti_chase_violations": report.anti_chase_violations,
            "ladder_violations": report.ladder_violations,
            "ws_warning_counts": dict(report.ws_warning_counts),
            "log_error_count": report.log_error_count,
            "closed_cleanly": report.closed_cleanly,
        }, sort_keys=True))
        return
    print(report.as_github_comment())


if __name__ == "__main__":
    main()
