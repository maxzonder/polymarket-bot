#!/usr/bin/env python3
"""Enrich multilevel collector touch rows with post-touch book movement metrics.

Reads a collector SQLite sidecar with `touch_events` and `book_snapshots`, then
writes a derived `post_touch_enrichment` table plus optional Markdown/JSON
summaries. This is research infrastructure only.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.paths import DATA_DIR

DEFAULT_INPUT = DATA_DIR / "short_horizon" / "phase0" / "live_depth_survival.sqlite3"
DEFAULT_OUTPUT_MD = DATA_DIR / "short_horizon" / "phase0" / "post_touch_enrichment.md"
DEFAULT_OUTPUT_JSON = DATA_DIR / "short_horizon" / "phase0" / "post_touch_enrichment.json"
DEFAULT_HORIZONS_MS = (1_000, 5_000, 15_000, 60_000)


@dataclass(frozen=True)
class EnrichmentSummaryRow:
    group: dict[str, Any]
    rows: int
    avg_move_1s: float | None
    avg_move_5s: float | None
    avg_move_15s: float | None
    avg_move_60s: float | None
    avg_max_favorable_60s: float | None
    avg_max_adverse_60s: float | None
    held_1s_rate: float | None
    held_5s_rate: float | None
    held_15s_rate: float | None
    held_60s_rate: float | None
    reversal_1s_rate: float | None
    reversal_5s_rate: float | None
    reversal_15s_rate: float | None
    reversal_60s_rate: float | None


def enrich_post_touch(
    db_path: Path,
    *,
    horizons_ms: Sequence[int] = DEFAULT_HORIZONS_MS,
    replace: bool = True,
) -> int:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_enrichment_table(conn, horizons_ms, replace=replace)
        touches = [dict(row) for row in conn.execute("SELECT * FROM touch_events ORDER BY token_id, touch_time_iso")]
        written = 0
        for touch in touches:
            enrichment = _enrich_one(conn, touch, horizons_ms)
            _insert_enrichment(conn, enrichment, horizons_ms)
            written += 1
        conn.commit()
        return written
    finally:
        conn.close()


def build_enrichment_summary(
    db_path: Path,
    *,
    axes: Sequence[str] = ("touch_level", "asset_slug", "outcome"),
    min_rows: int = 1,
) -> list[EnrichmentSummaryRow]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        _validate_axes(conn, axes)
        rows = [dict(row) for row in conn.execute("SELECT * FROM post_touch_enrichment")]
    finally:
        conn.close()

    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = tuple(row.get(axis) for axis in axes)
        groups.setdefault(key, []).append(row)

    out: list[EnrichmentSummaryRow] = []
    for key, group_rows in sorted(groups.items(), key=lambda item: item[0]):
        if len(group_rows) < min_rows:
            continue
        out.append(
            EnrichmentSummaryRow(
                group={axis: value for axis, value in zip(axes, key)},
                rows=len(group_rows),
                avg_move_1s=_avg(group_rows, "move_1000ms"),
                avg_move_5s=_avg(group_rows, "move_5000ms"),
                avg_move_15s=_avg(group_rows, "move_15000ms"),
                avg_move_60s=_avg(group_rows, "move_60000ms"),
                avg_max_favorable_60s=_avg(group_rows, "max_favorable_60000ms"),
                avg_max_adverse_60s=_avg(group_rows, "max_adverse_60000ms"),
                held_1s_rate=_rate(group_rows, "held_1000ms"),
                held_5s_rate=_rate(group_rows, "held_5000ms"),
                held_15s_rate=_rate(group_rows, "held_15000ms"),
                held_60s_rate=_rate(group_rows, "held_60000ms"),
                reversal_1s_rate=_rate(group_rows, "reversal_1000ms"),
                reversal_5s_rate=_rate(group_rows, "reversal_5000ms"),
                reversal_15s_rate=_rate(group_rows, "reversal_15000ms"),
                reversal_60s_rate=_rate(group_rows, "reversal_60000ms"),
            )
        )
    return out


def _ensure_enrichment_table(conn: sqlite3.Connection, horizons_ms: Sequence[int], *, replace: bool) -> None:
    if replace:
        conn.execute("DROP TABLE IF EXISTS post_touch_enrichment")
    dynamic_columns: list[str] = []
    for horizon in horizons_ms:
        suffix = f"{int(horizon)}ms"
        dynamic_columns.extend(
            [
                f"best_ask_{suffix} REAL",
                f"move_{suffix} REAL",
                f"max_favorable_{suffix} REAL",
                f"max_adverse_{suffix} REAL",
                f"held_{suffix} INTEGER",
                f"reversal_{suffix} INTEGER",
                f"snapshot_count_{suffix} INTEGER",
            ]
        )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS post_touch_enrichment (
            probe_id TEXT PRIMARY KEY,
            run_id TEXT,
            token_id TEXT,
            market_id TEXT,
            asset_slug TEXT,
            outcome TEXT,
            touch_level REAL,
            touch_ts_ms INTEGER,
            initial_best_ask REAL,
            {', '.join(dynamic_columns)}
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_post_touch_group ON post_touch_enrichment(asset_slug, outcome, touch_level)")
    conn.commit()


def _enrich_one(conn: sqlite3.Connection, touch: dict[str, Any], horizons_ms: Sequence[int]) -> dict[str, Any]:
    touch_ts_ms = _parse_touch_ts_ms(touch)
    token_id = str(touch["token_id"])
    initial = _to_float(touch.get("best_ask_at_touch"))
    level = _to_float(touch.get("touch_level"))
    max_horizon = max(horizons_ms) if horizons_ms else 0
    snapshots = [
        dict(row)
        for row in conn.execute(
            """
            SELECT event_ts_ms, best_ask
            FROM book_snapshots
            WHERE token_id = ? AND event_ts_ms >= ? AND event_ts_ms <= ? AND best_ask IS NOT NULL
            ORDER BY event_ts_ms
            """,
            (token_id, touch_ts_ms, touch_ts_ms + max_horizon),
        )
    ]
    out: dict[str, Any] = {
        "probe_id": touch["probe_id"],
        "run_id": touch.get("run_id"),
        "token_id": token_id,
        "market_id": touch.get("market_id"),
        "asset_slug": touch.get("asset_slug"),
        "outcome": touch.get("outcome"),
        "touch_level": level,
        "touch_ts_ms": touch_ts_ms,
        "initial_best_ask": initial,
    }
    for horizon in horizons_ms:
        suffix = f"{int(horizon)}ms"
        window = [row for row in snapshots if row["event_ts_ms"] <= touch_ts_ms + horizon]
        best_at_horizon = _best_ask_at_or_before(window, touch_ts_ms + horizon)
        moves = [_to_float(row.get("best_ask")) - initial for row in window if initial is not None and row.get("best_ask") is not None]
        max_fav = max(moves) if moves else None
        max_adv = min(moves) if moves else None
        move = best_at_horizon - initial if best_at_horizon is not None and initial is not None else None
        held = None
        reversal = None
        if level is not None and window:
            held = int(all(_to_float(row.get("best_ask")) is not None and _to_float(row.get("best_ask")) >= level - 1e-9 for row in window))
            reversal = int(any(_to_float(row.get("best_ask")) is not None and _to_float(row.get("best_ask")) < level - 1e-9 for row in window))
        out.update(
            {
                f"best_ask_{suffix}": best_at_horizon,
                f"move_{suffix}": move,
                f"max_favorable_{suffix}": max_fav,
                f"max_adverse_{suffix}": max_adv,
                f"held_{suffix}": held,
                f"reversal_{suffix}": reversal,
                f"snapshot_count_{suffix}": len(window),
            }
        )
    return out


def _insert_enrichment(conn: sqlite3.Connection, row: dict[str, Any], horizons_ms: Sequence[int]) -> None:
    columns = list(row.keys())
    placeholders = ", ".join(f":{column}" for column in columns)
    conn.execute(
        f"INSERT OR REPLACE INTO post_touch_enrichment({', '.join(columns)}) VALUES ({placeholders})",
        row,
    )


def _best_ask_at_or_before(rows: list[dict[str, Any]], target_ts_ms: int) -> float | None:
    best = None
    for row in rows:
        if row["event_ts_ms"] <= target_ts_ms:
            best = _to_float(row.get("best_ask"))
        else:
            break
    return best


def _parse_touch_ts_ms(touch: dict[str, Any]) -> int:
    # Probe ids are token:level:event_ts_ms in the live collector. Prefer that because
    # SQLite stores touch_time_iso as text and old rows may not have a numeric column.
    probe_id = str(touch.get("probe_id") or "")
    try:
        return int(probe_id.rsplit(":", 1)[1])
    except Exception:
        from datetime import datetime

        return int(datetime.fromisoformat(str(touch["touch_time_iso"]).replace("Z", "+00:00")).timestamp() * 1000)


def _validate_axes(conn: sqlite3.Connection, axes: Sequence[str]) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(post_touch_enrichment)")}
    missing = [axis for axis in axes if axis not in columns]
    if missing:
        raise ValueError(f"Unknown enrichment axis/axes: {', '.join(missing)}")


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _avg(rows: Sequence[dict[str, Any]], column: str) -> float | None:
    values = [_to_float(row.get(column)) for row in rows]
    clean = [value for value in values if value is not None]
    return sum(clean) / len(clean) if clean else None


def _rate(rows: Sequence[dict[str, Any]], column: str) -> float | None:
    values = [row.get(column) for row in rows if row.get(column) is not None]
    if not values:
        return None
    return sum(1 for value in values if bool(value)) / len(values)


def write_json(rows: Sequence[EnrichmentSummaryRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(row) for row in rows], indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(rows: Sequence[EnrichmentSummaryRow], path: Path, *, source: Path, axes: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Post-touch enrichment summary",
        "",
        f"Source: `{source}`",
        f"Axes: `{', '.join(axes)}`",
        "",
        "Metrics are derived from collector `book_snapshots`, not from market resolutions.",
        "",
    ]
    if not rows:
        lines.append("No groups matched the filters.")
    for row in rows:
        title = ", ".join(f"{key}={value}" for key, value in row.group.items())
        lines.extend(
            [
                f"## {title}",
                "",
                f"- rows: `{row.rows}`",
                f"- avg move 1s / 5s / 15s / 60s: `{_fmt_float(row.avg_move_1s)}` / `{_fmt_float(row.avg_move_5s)}` / `{_fmt_float(row.avg_move_15s)}` / `{_fmt_float(row.avg_move_60s)}`",
                f"- avg max favorable 60s: `{_fmt_float(row.avg_max_favorable_60s)}`",
                f"- avg max adverse 60s: `{_fmt_float(row.avg_max_adverse_60s)}`",
                f"- held 1s / 5s / 15s / 60s: `{_fmt_pct(row.held_1s_rate)}` / `{_fmt_pct(row.held_5s_rate)}` / `{_fmt_pct(row.held_15s_rate)}` / `{_fmt_pct(row.held_60s_rate)}`",
                f"- reversal 1s / 5s / 15s / 60s: `{_fmt_pct(row.reversal_1s_rate)}` / `{_fmt_pct(row.reversal_5s_rate)}` / `{_fmt_pct(row.reversal_15s_rate)}` / `{_fmt_pct(row.reversal_60s_rate)}`",
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def _fmt_float(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.6f}"


def _fmt_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.2f}%"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-sqlite", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--axis", action="append", dest="axes", default=None)
    parser.add_argument("--min-rows", type=int, default=1)
    parser.add_argument("--no-replace", action="store_true", help="Do not drop an existing post_touch_enrichment table first")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    axes = tuple(args.axes) if args.axes else ("touch_level", "asset_slug", "outcome")
    count = enrich_post_touch(args.input_sqlite, replace=not args.no_replace)
    rows = build_enrichment_summary(args.input_sqlite, axes=axes, min_rows=args.min_rows)
    write_markdown(rows, args.output_md, source=args.input_sqlite, axes=axes)
    write_json(rows, args.output_json)
    print(f"enriched {count} touches; wrote {len(rows)} groups to {args.output_md} and {args.output_json}")


if __name__ == "__main__":
    main()
