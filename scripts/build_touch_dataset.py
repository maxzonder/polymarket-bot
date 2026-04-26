#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

DEFAULT_COLLECTOR_GLOB = "/home/polybot/.polybot/short_horizon/phase0/live_depth_survival_*.csv"
DEFAULT_RESOLUTIONS_PATH = Path("v2/short_horizon/data/market_resolutions.sqlite3")
DEFAULT_OUTPUT_PATH = Path("v2/short_horizon/data/touch_dataset.sqlite3")
DEFAULT_VENUE_MIN_ORDER_SHARES = 5.0

REQUIRED_CSV_COLUMNS = [
    "probe_id",
    "recorded_at",
    "market_id",
    "condition_id",
    "token_id",
    "outcome",
    "question",
    "touch_level",
    "touch_time_iso",
    "duration_seconds",
    "start_time_iso",
    "end_time_iso",
    "fees_enabled",
    "fee_rate_bps",
    "tick_size",
    "best_bid_at_touch",
    "best_ask_at_touch",
    "ask_level_1_price",
    "ask_level_1_size",
    "ask_level_2_price",
    "ask_level_2_size",
    "ask_level_3_price",
    "ask_level_3_size",
    "ask_level_4_price",
    "ask_level_4_size",
    "ask_level_5_price",
    "ask_level_5_size",
    "ask_size_at_touch_level",
    "fit_10_usdc",
    "fit_50_usdc",
    "fit_100_usdc",
    "survived_ms",
    "end_reason",
]

TOUCH_DATASET_COLUMNS: list[tuple[str, str]] = [
    ("probe_id", "TEXT PRIMARY KEY"),
    ("recorded_at", "TEXT"),
    ("market_id", "TEXT NOT NULL"),
    ("condition_id", "TEXT"),
    ("token_id", "TEXT NOT NULL"),
    ("asset_slug", "TEXT NOT NULL"),
    ("direction", "TEXT NOT NULL"),
    ("question", "TEXT"),
    ("touch_level", "REAL"),
    ("touch_time_iso", "TEXT NOT NULL"),
    ("start_time_iso", "TEXT"),
    ("end_time_iso", "TEXT"),
    ("lifecycle_fraction", "REAL"),
    ("duration_seconds", "REAL"),
    ("fees_enabled", "INTEGER"),
    ("fee_rate_bps", "REAL"),
    ("tick_size", "REAL"),
    ("best_bid_at_touch", "REAL"),
    ("best_ask_at_touch", "REAL"),
    ("ask_level_1_price", "REAL"),
    ("ask_level_1_size", "REAL"),
    ("ask_level_2_price", "REAL"),
    ("ask_level_2_size", "REAL"),
    ("ask_level_3_price", "REAL"),
    ("ask_level_3_size", "REAL"),
    ("ask_level_4_price", "REAL"),
    ("ask_level_4_size", "REAL"),
    ("ask_level_5_price", "REAL"),
    ("ask_level_5_size", "REAL"),
    ("ask_size_at_touch_level", "REAL"),
    ("fit_10_usdc", "TEXT"),
    ("fit_50_usdc", "TEXT"),
    ("fit_100_usdc", "TEXT"),
    ("survived_ms", "REAL"),
    ("end_reason", "TEXT"),
    ("resolves_yes", "INTEGER NOT NULL"),
    ("outcome_resolved_at", "TEXT"),
    ("time_to_resolution_ms", "REAL"),
    ("outcome_token_id", "TEXT NOT NULL"),
    ("would_be_size_at_min_shares", "REAL NOT NULL"),
    ("would_be_cost_after_min_shares", "REAL"),
    ("would_be_estimated_fee_usdc", "REAL"),
]

NUMERIC_CSV_COLUMNS = {
    "touch_level",
    "duration_seconds",
    "fee_rate_bps",
    "tick_size",
    "best_bid_at_touch",
    "best_ask_at_touch",
    "ask_level_1_price",
    "ask_level_1_size",
    "ask_level_2_price",
    "ask_level_2_size",
    "ask_level_3_price",
    "ask_level_3_size",
    "ask_level_4_price",
    "ask_level_4_size",
    "ask_level_5_price",
    "ask_level_5_size",
    "ask_size_at_touch_level",
    "survived_ms",
}


@dataclass(frozen=True)
class ResolutionRow:
    market_id: str
    condition_id: str | None
    token_yes_id: str | None
    token_no_id: str | None
    outcome_resolved_at: str | None
    outcome_token_id: str | None
    outcome_price_yes: float | None
    outcome_price_no: float | None
    question: str | None


@dataclass(frozen=True)
class CrossCheckSummary:
    executed_trades: int
    found: int
    strict_found: int
    relaxed_found: int
    outcome_matches: int
    outcome_mismatches: int
    cost_matches: int
    cost_mismatches: int
    missing: int
    report_path: str | None = None


@dataclass(frozen=True)
class BuildSummary:
    csv_path: str
    resolutions_path: str
    output_path: str
    input_rows: int
    resolved_market_rows: int
    output_rows: int
    resolves_yes_rows: int
    resolves_no_rows: int
    dropped_unresolved: int
    dropped_token_mismatch: int
    dropped_duplicate_probe_id: int
    join_hit_rate: float | None
    cross_check: CrossCheckSummary | None = None

    def as_json(self) -> dict[str, Any]:
        payload = asdict(self)
        return payload


def build_touch_dataset(
    csv_path: str | Path,
    resolutions_path: str | Path,
    *,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    venue_min_order_shares: float = DEFAULT_VENUE_MIN_ORDER_SHARES,
    executed_db_paths: Iterable[str | Path] = (),
    cross_check_window_seconds: float = 5.0,
    cross_check_report_path: str | Path | None = None,
    csv_export_path: str | Path | None = None,
) -> BuildSummary:
    csv_path = Path(csv_path)
    resolutions_path = Path(resolutions_path)
    output_path = Path(output_path)
    resolutions = load_resolutions(resolutions_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(output_path)
    _reset_output_schema(conn)

    input_rows = 0
    output_rows = 0
    resolves_yes_rows = 0
    resolves_no_rows = 0
    dropped_unresolved = 0
    dropped_token_mismatch = 0
    dropped_duplicate_probe_id = 0
    resolved_market_rows = 0
    seen_probe_ids: set[str] = set()

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        _validate_csv_columns(csv_path, reader.fieldnames or [])
        for raw in reader:
            input_rows += 1
            probe_id = str(raw.get("probe_id") or "").strip()
            if not probe_id:
                raise ValueError(f"CSV {csv_path} row {input_rows} has empty probe_id")
            if probe_id in seen_probe_ids:
                dropped_duplicate_probe_id += 1
                continue
            seen_probe_ids.add(probe_id)

            market_id = str(raw.get("market_id") or "").strip()
            token_id = str(raw.get("token_id") or "").strip()
            resolution = resolutions.get(market_id)
            if resolution is None or not resolution.outcome_token_id:
                dropped_unresolved += 1
                continue
            resolved_market_rows += 1
            expected_tokens = {value for value in (resolution.token_yes_id, resolution.token_no_id) if value}
            if expected_tokens and token_id not in expected_tokens:
                dropped_token_mismatch += 1
                continue

            row = build_dataset_row(raw, resolution, venue_min_order_shares=venue_min_order_shares)
            _insert_touch_row(conn, row)
            output_rows += 1
            if int(row["resolves_yes"]) == 1:
                resolves_yes_rows += 1
            else:
                resolves_no_rows += 1

    conn.commit()
    conn.execute("CREATE INDEX IF NOT EXISTS idx_touch_dataset_market_token_time ON touch_dataset(market_id, token_id, touch_time_iso)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_touch_dataset_resolves_yes ON touch_dataset(resolves_yes)")
    conn.commit()

    if csv_export_path is not None:
        export_touch_dataset_csv(conn, csv_export_path)

    cross_check = None
    executed_paths = [Path(path) for path in executed_db_paths]
    if executed_paths:
        cross_check = cross_check_executed_trades(
            conn,
            executed_paths,
            window_seconds=cross_check_window_seconds,
            report_path=cross_check_report_path,
        )

    conn.close()
    return BuildSummary(
        csv_path=str(csv_path),
        resolutions_path=str(resolutions_path),
        output_path=str(output_path),
        input_rows=input_rows,
        resolved_market_rows=resolved_market_rows,
        output_rows=output_rows,
        resolves_yes_rows=resolves_yes_rows,
        resolves_no_rows=resolves_no_rows,
        dropped_unresolved=dropped_unresolved,
        dropped_token_mismatch=dropped_token_mismatch,
        dropped_duplicate_probe_id=dropped_duplicate_probe_id,
        join_hit_rate=(output_rows / resolved_market_rows) if resolved_market_rows else None,
        cross_check=cross_check,
    )


def build_dataset_row(raw: dict[str, str], resolution: ResolutionRow, *, venue_min_order_shares: float) -> dict[str, Any]:
    token_id = str(raw.get("token_id") or "").strip()
    question = _clean_text(raw.get("question")) or resolution.question
    touch_time_iso = _clean_text(raw.get("touch_time_iso"))
    start_time_iso = _clean_text(raw.get("start_time_iso"))
    end_time_iso = _clean_text(raw.get("end_time_iso"))
    touch_dt = _parse_iso(touch_time_iso)
    start_dt = _parse_iso(start_time_iso)
    end_dt = _parse_iso(end_time_iso)
    resolved_dt = _parse_iso(resolution.outcome_resolved_at)
    lifecycle_fraction = _lifecycle_fraction(touch_dt, start_dt, end_dt)
    time_to_resolution_ms = _duration_ms(touch_dt, resolved_dt or end_dt)
    best_ask = _float_or_none(raw.get("best_ask_at_touch"))
    fee_rate_bps = _float_or_none(raw.get("fee_rate_bps"))
    would_cost = (venue_min_order_shares * best_ask) if best_ask is not None else None
    would_fee = (would_cost * fee_rate_bps / 10000.0) if would_cost is not None and fee_rate_bps is not None else None

    row: dict[str, Any] = {
        "probe_id": _required_text(raw, "probe_id"),
        "recorded_at": _clean_text(raw.get("recorded_at")),
        "market_id": resolution.market_id,
        "condition_id": resolution.condition_id or _clean_text(raw.get("condition_id")),
        "token_id": token_id,
        "asset_slug": asset_slug_from_question(question),
        "direction": direction_from_outcome(raw.get("outcome")),
        "question": question,
        "touch_time_iso": touch_time_iso,
        "start_time_iso": start_time_iso,
        "end_time_iso": end_time_iso,
        "lifecycle_fraction": lifecycle_fraction,
        "fees_enabled": _bool_int(raw.get("fees_enabled")),
        "resolves_yes": 1 if token_id == resolution.outcome_token_id else 0,
        "outcome_resolved_at": resolution.outcome_resolved_at,
        "time_to_resolution_ms": time_to_resolution_ms,
        "outcome_token_id": resolution.outcome_token_id,
        "would_be_size_at_min_shares": float(venue_min_order_shares),
        "would_be_cost_after_min_shares": would_cost,
        "would_be_estimated_fee_usdc": would_fee,
    }
    for column in NUMERIC_CSV_COLUMNS:
        row[column] = _float_or_none(raw.get(column))
    for column in ("fit_10_usdc", "fit_50_usdc", "fit_100_usdc", "end_reason"):
        row[column] = _clean_text(raw.get(column))
    return row


def load_resolutions(path: str | Path) -> dict[str, ResolutionRow]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    rows: dict[str, ResolutionRow] = {}
    try:
        for row in conn.execute("SELECT * FROM market_resolutions"):
            item = ResolutionRow(
                market_id=str(row["market_id"]),
                condition_id=_clean_text(row["condition_id"]),
                token_yes_id=_clean_text(row["token_yes_id"]),
                token_no_id=_clean_text(row["token_no_id"]),
                outcome_resolved_at=_clean_text(row["outcome_resolved_at"]),
                outcome_token_id=_clean_text(row["outcome_token_id"]),
                outcome_price_yes=_float_or_none(row["outcome_price_yes"]),
                outcome_price_no=_float_or_none(row["outcome_price_no"]),
                question=_clean_text(row["question"]),
            )
            rows[item.market_id] = item
    finally:
        conn.close()
    return rows


def latest_collector_csv(glob_pattern: str = DEFAULT_COLLECTOR_GLOB) -> Path:
    paths = [path for path in Path("/").glob(glob_pattern.lstrip("/")) if path.is_file() and "probe" not in path.name]
    if not paths:
        raise FileNotFoundError(f"no collector CSV found for pattern {glob_pattern}")
    return max(paths, key=lambda path: path.stat().st_mtime)


def cross_check_executed_trades(
    dataset_conn: sqlite3.Connection,
    executed_db_paths: Iterable[Path],
    *,
    window_seconds: float = 5.0,
    report_path: str | Path | None = None,
    cost_tolerance_usdc: float = 0.05,
) -> CrossCheckSummary:
    report_rows: list[dict[str, Any]] = []
    executed = 0
    found = 0
    strict_found = 0
    relaxed_found = 0
    outcome_matches = 0
    outcome_mismatches = 0
    cost_matches = 0
    cost_mismatches = 0
    missing = 0

    for db_path in executed_db_paths:
        for trade in _extract_executed_trades(db_path):
            executed += 1
            match_info = _find_dataset_match(dataset_conn, trade, window_seconds=window_seconds)
            result: dict[str, Any] = {"db_path": str(db_path), **trade}
            if match_info is None:
                missing += 1
                result.update({"status": "missing"})
                report_rows.append(result)
                continue

            match, match_mode, match_delta_seconds = match_info
            found += 1
            if match_mode == "strict_time_window":
                strict_found += 1
            else:
                relaxed_found += 1

            expected_resolves_yes = trade.get("expected_resolves_yes")
            outcome_ok = expected_resolves_yes is None or int(match["resolves_yes"]) == int(expected_resolves_yes)
            if outcome_ok:
                outcome_matches += 1
            else:
                outcome_mismatches += 1
            cost_ok: bool | None = None
            fill_cost = _float_or_none(trade.get("fill_cost_usdc"))
            would_cost = _float_or_none(match["would_be_cost_after_min_shares"])
            if fill_cost is not None and would_cost is not None:
                cost_ok = abs(fill_cost - would_cost) <= cost_tolerance_usdc
                if cost_ok:
                    cost_matches += 1
                else:
                    cost_mismatches += 1
            result.update(
                {
                    "status": "found",
                    "matched_probe_id": match["probe_id"],
                    "matched_touch_time_iso": match["touch_time_iso"],
                    "matched_resolves_yes": match["resolves_yes"],
                    "matched_would_be_cost_after_min_shares": match["would_be_cost_after_min_shares"],
                    "match_mode": match_mode,
                    "match_delta_seconds": match_delta_seconds,
                    "outcome_ok": outcome_ok,
                    "cost_ok": cost_ok,
                }
            )
            report_rows.append(result)

    report_path_text = None
    if report_path is not None:
        path = Path(report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report_rows, indent=2, sort_keys=True) + "\n")
        report_path_text = str(path)

    return CrossCheckSummary(
        executed_trades=executed,
        found=found,
        strict_found=strict_found,
        relaxed_found=relaxed_found,
        outcome_matches=outcome_matches,
        outcome_mismatches=outcome_mismatches,
        cost_matches=cost_matches,
        cost_mismatches=cost_mismatches,
        missing=missing,
        report_path=report_path_text,
    )


def export_touch_dataset_csv(conn: sqlite3.Connection, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    column_names = [name for name, _ in TOUCH_DATASET_COLUMNS]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=column_names)
        writer.writeheader()
        for row in conn.execute(f"SELECT {', '.join(column_names)} FROM touch_dataset ORDER BY touch_time_iso, probe_id"):
            writer.writerow(dict(zip(column_names, row)))


def _extract_executed_trades(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        run_id = _first_value(conn, "SELECT run_id FROM runs ORDER BY started_at LIMIT 1") or db_path.stem
        intents = _order_intents(conn, str(run_id))
        fills = _fill_costs(conn, str(run_id))
        resolved = _resolved_outcomes(conn, str(run_id))
        trades: list[dict[str, Any]] = []
        for key, fill in fills.items():
            market_id, token_id = key
            intent = intents.get(key, {})
            outcome_price = resolved.get(key)
            # Probe DB resolution payloads may store final proxy prices like 0.99, 0.73, or 0.55,
            # not only exact binary 0/1 payouts. For the executed-trade sanity check, any
            # outcome price above 0.5 means the touched token was the winning side.
            expected_resolves_yes = None if outcome_price is None else int(float(outcome_price) > 0.5)
            trades.append(
                {
                    "run_id": str(run_id),
                    "market_id": market_id,
                    "token_id": token_id,
                    "intent_event_time": intent.get("event_time"),
                    "intent_level": intent.get("level"),
                    "expected_resolves_yes": expected_resolves_yes,
                    "outcome_price": outcome_price,
                    "fill_cost_usdc": fill.get("cost_usdc"),
                    "fill_size": fill.get("size"),
                }
            )
        return trades
    finally:
        conn.close()


def _order_intents(conn: sqlite3.Connection, run_id: str) -> dict[tuple[str, str], dict[str, Any]]:
    if not _table_exists(conn, "events_log"):
        return {}
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in conn.execute(
        "SELECT event_time, market_id, token_id, payload_json FROM events_log WHERE run_id = ? AND event_type = 'OrderIntent' ORDER BY event_time",
        (run_id,),
    ):
        payload = _loads(row["payload_json"])
        payload["event_time"] = row["event_time"]
        result[(str(row["market_id"]), str(row["token_id"]))] = payload
    return result


def _fill_costs(conn: sqlite3.Connection, run_id: str) -> dict[tuple[str, str], dict[str, float]]:
    if not _table_exists(conn, "fills"):
        return {}
    result: dict[tuple[str, str], dict[str, float]] = {}
    for row in conn.execute("SELECT market_id, token_id, price, size FROM fills WHERE run_id = ?", (run_id,)):
        key = (str(row["market_id"]), str(row["token_id"]))
        item = result.setdefault(key, {"cost_usdc": 0.0, "size": 0.0})
        price = _float_or_none(row["price"]) or 0.0
        size = _float_or_none(row["size"]) or 0.0
        item["cost_usdc"] += price * size
        item["size"] += size
    return result


def _resolved_outcomes(conn: sqlite3.Connection, run_id: str) -> dict[tuple[str, str], float | None]:
    if not _table_exists(conn, "events_log"):
        return {}
    result: dict[tuple[str, str], float | None] = {}
    for row in conn.execute(
        "SELECT market_id, token_id, payload_json FROM events_log WHERE run_id = ? AND event_type = 'MarketResolvedWithInventory' ORDER BY event_time",
        (run_id,),
    ):
        payload = _loads(row["payload_json"])
        market_id = str(payload.get("market_id") or row["market_id"] or "")
        token_id = str(payload.get("token_id") or row["token_id"] or "")
        if market_id and token_id:
            result[(market_id, token_id)] = _float_or_none(payload.get("outcome_price"))
    return result


def _find_dataset_match(conn: sqlite3.Connection, trade: dict[str, Any], *, window_seconds: float) -> tuple[sqlite3.Row, str, float | None] | None:
    conn.row_factory = sqlite3.Row
    event_time = _parse_iso(trade.get("intent_event_time"))
    intent_level = _float_or_none(trade.get("intent_level"))
    candidates = list(
        conn.execute(
            "SELECT * FROM touch_dataset WHERE market_id = ? AND token_id = ?",
            (str(trade.get("market_id") or ""), str(trade.get("token_id") or "")),
        )
    )
    if not candidates:
        return None
    if event_time is None:
        return (candidates[0], "no_intent_time", None)
    best: tuple[float, sqlite3.Row] | None = None
    for row in candidates:
        touch_time = _parse_iso(row["touch_time_iso"])
        if touch_time is None:
            continue
        delta = abs((touch_time - event_time).total_seconds())
        if delta <= window_seconds and (best is None or delta < best[0]):
            best = (delta, row)
    if best is not None:
        return (best[1], "strict_time_window", best[0])

    # The collector fires once per token/level and the trading runtime can submit later
    # against the same already-touched level. Keep those visible as relaxed matches
    # instead of marking them as silently missing.
    relaxed: tuple[float, sqlite3.Row] | None = None
    for row in candidates:
        if intent_level is not None:
            row_level = _float_or_none(row["touch_level"])
            if row_level is None or abs(row_level - intent_level) > 1e-9:
                continue
        touch_time = _parse_iso(row["touch_time_iso"])
        if touch_time is None:
            continue
        delta = abs((touch_time - event_time).total_seconds())
        if relaxed is None or delta < relaxed[0]:
            relaxed = (delta, row)
    if relaxed is not None:
        return (relaxed[1], "relaxed_same_market_token_level", relaxed[0])
    return None


def _reset_output_schema(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS touch_dataset")
    columns_sql = ",\n            ".join(f"{name} {definition}" for name, definition in TOUCH_DATASET_COLUMNS)
    conn.execute(f"CREATE TABLE touch_dataset (\n            {columns_sql}\n        )")


def _insert_touch_row(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    columns = [name for name, _ in TOUCH_DATASET_COLUMNS]
    placeholders = ", ".join("?" for _ in columns)
    conn.execute(
        f"INSERT INTO touch_dataset ({', '.join(columns)}) VALUES ({placeholders})",
        [row.get(column) for column in columns],
    )


def _validate_csv_columns(path: Path, fieldnames: list[str]) -> None:
    missing = [column for column in REQUIRED_CSV_COLUMNS if column not in fieldnames]
    if missing:
        raise ValueError(f"CSV {path} is missing required columns: {', '.join(missing)}")


def asset_slug_from_question(question: str | None) -> str:
    text = (question or "").strip().lower()
    if "bitcoin" in text or "btc" in text:
        return "btc"
    if "ethereum" in text or "eth" in text:
        return "eth"
    if "bnb" in text:
        return "bnb"
    if "dogecoin" in text or "doge" in text:
        return "doge"
    if "solana" in text or "sol" in text:
        return "sol"
    if "xrp" in text:
        return "xrp"
    if text:
        return text.split()[0].strip(" :-_/()").lower() or "unknown"
    return "unknown"


def direction_from_outcome(outcome: Any) -> str:
    text = str(outcome or "").strip().lower()
    if text in {"up", "yes"}:
        return "UP/YES"
    if text in {"down", "no"}:
        return "DOWN/NO"
    return "unknown"


def _lifecycle_fraction(touch: datetime | None, start: datetime | None, end: datetime | None) -> float | None:
    if touch is None or start is None or end is None:
        return None
    total = (end - start).total_seconds()
    if total <= 0:
        return None
    return (touch - start).total_seconds() / total


def _duration_ms(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return (end - start).total_seconds() * 1000.0


def _parse_iso(value: Any) -> datetime | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if str(value).strip() == "":
            return None
        num = float(value)
        return num if math.isfinite(num) else None
    except Exception:
        return None


def _bool_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return 1
    if text in {"0", "false", "no", "n"}:
        return 0
    parsed = _float_or_none(value)
    return int(parsed) if parsed is not None else None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_text(row: dict[str, str], column: str) -> str:
    value = _clean_text(row.get(column))
    if value is None:
        raise ValueError(f"required column {column} is empty")
    return value


def _loads(value: str | bytes | None) -> dict[str, Any]:
    if value is None:
        return {}
    try:
        item = json.loads(value)
    except Exception:
        return {}
    return item if isinstance(item, dict) else {}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _first_value(conn: sqlite3.Connection, sql: str) -> Any:
    try:
        row = conn.execute(sql).fetchone()
    except sqlite3.Error:
        return None
    return None if row is None else row[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build canonical P6-2a touch dataset from collector CSV and market resolutions")
    parser.add_argument("--csv-path", default=None, help="Collector CSV path; defaults to latest non-probe live_depth_survival_*.csv")
    parser.add_argument("--csv-glob", default=DEFAULT_COLLECTOR_GLOB, help="Collector CSV glob used when --csv-path is omitted")
    parser.add_argument("--resolutions", default=str(DEFAULT_RESOLUTIONS_PATH), help="SQLite market_resolutions DB from P6-2a-2")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="SQLite output path for touch_dataset")
    parser.add_argument("--csv-export", default=None, help="Optional CSV export of the canonical dataset")
    parser.add_argument("--venue-min-order-shares", type=float, default=DEFAULT_VENUE_MIN_ORDER_SHARES)
    parser.add_argument("--executed-db", action="append", default=[], help="Probe DB to cross-check executed trades; can be repeated")
    parser.add_argument("--cross-check-window-seconds", type=float, default=5.0)
    parser.add_argument("--cross-check-report", default=None, help="Optional JSON report path for executed-trade cross-check rows")
    parser.add_argument("--summary-out", default=None, help="Optional JSON summary path")
    args = parser.parse_args()

    csv_path = Path(args.csv_path) if args.csv_path else latest_collector_csv(args.csv_glob)
    summary = build_touch_dataset(
        csv_path,
        args.resolutions,
        output_path=args.output,
        venue_min_order_shares=args.venue_min_order_shares,
        executed_db_paths=args.executed_db,
        cross_check_window_seconds=args.cross_check_window_seconds,
        cross_check_report_path=args.cross_check_report,
        csv_export_path=args.csv_export,
    )
    payload = summary.as_json()
    output = json.dumps(payload, indent=2, sort_keys=True)
    if args.summary_out:
        path = Path(args.summary_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(output + "\n")
    print(output)


if __name__ == "__main__":
    main()
