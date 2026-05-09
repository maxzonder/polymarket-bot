#!/usr/bin/env python3
"""Build executable-aware heatmaps from the multilevel collector SQLite sidecar.

This is research infrastructure only: it summarizes collector `touch_events`
rows and does not make strategy GO/NO-GO decisions by itself.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.paths import DATA_DIR

DEFAULT_INPUT = DATA_DIR / "short_horizon" / "phase0" / "live_depth_survival.sqlite3"
DEFAULT_OUTPUT_MD = DATA_DIR / "short_horizon" / "phase0" / "collector_heatmap.md"
DEFAULT_OUTPUT_JSON = DATA_DIR / "short_horizon" / "phase0" / "collector_heatmap.json"
DEFAULT_AXES = ("touch_level", "asset_slug", "outcome", "horizon_bucket")


@dataclass(frozen=True)
class HeatmapRow:
    group: dict[str, Any]
    rows: int
    executable_rows: int
    stale_or_bad_rows: int
    fit_5_share_rate: float | None
    fit_10_usdc_rate: float | None
    avg_spread: float | None
    avg_imbalance_top1: float | None
    avg_required_hit_rate_5_shares: float | None
    avg_spot_return_30s: float | None
    immediate_reversal_rate: float | None
    held_at_or_above_rate: float | None
    avg_max_favorable_move: float | None
    avg_max_adverse_move: float | None


FIT_OK = {"+0_tick", "+1_tick", "+2plus_ticks"}
BAD_FLAG_COLUMNS = (
    "missing_depth_flag_at_touch",
    "crossed_book_flag_at_touch",
    "book_stale_flag_at_touch",
)


def default_input_path() -> Path:
    return DEFAULT_INPUT


def build_collector_heatmap(
    db_path: Path,
    *,
    axes: Sequence[str] = DEFAULT_AXES,
    min_rows: int = 1,
    executable_only: bool = False,
) -> list[HeatmapRow]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        _validate_axes(conn, axes)
        query = "SELECT * FROM touch_events"
        rows = [dict(row) for row in conn.execute(query)]
    finally:
        conn.close()

    if executable_only:
        rows = [row for row in rows if _is_executable(row)]

    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = tuple(row.get(axis) for axis in axes)
        groups.setdefault(key, []).append(row)

    out: list[HeatmapRow] = []
    for key, group_rows in sorted(groups.items(), key=lambda item: (item[0], len(item[1]))):
        if len(group_rows) < min_rows:
            continue
        executable_rows = [row for row in group_rows if _is_executable(row)]
        stale_or_bad = [row for row in group_rows if _has_bad_flags(row)]
        out.append(
            HeatmapRow(
                group={axis: value for axis, value in zip(axes, key)},
                rows=len(group_rows),
                executable_rows=len(executable_rows),
                stale_or_bad_rows=len(stale_or_bad),
                fit_5_share_rate=_rate(group_rows, lambda row: row.get("fit_5_shares") in FIT_OK),
                fit_10_usdc_rate=_rate(group_rows, lambda row: row.get("fit_10_usdc") in FIT_OK),
                avg_spread=_avg(group_rows, "spread_at_touch"),
                avg_imbalance_top1=_avg(group_rows, "imbalance_top1"),
                avg_required_hit_rate_5_shares=_avg(group_rows, "required_hit_rate_for_5_shares"),
                avg_spot_return_30s=_avg(group_rows, "spot_return_30s"),
                immediate_reversal_rate=_rate(group_rows, lambda row: _truthy(row.get("immediate_reversal_flag"))),
                held_at_or_above_rate=_rate(group_rows, lambda row: _truthy(row.get("held_at_or_above_level"))),
                avg_max_favorable_move=_avg(group_rows, "max_favorable_move"),
                avg_max_adverse_move=_avg(group_rows, "max_adverse_move"),
            )
        )
    out.sort(key=lambda row: (row.group_key(), -row.rows) if hasattr(row, "group_key") else (tuple(row.group.values()), -row.rows))
    return out


def _validate_axes(conn: sqlite3.Connection, axes: Sequence[str]) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(touch_events)")}
    missing = [axis for axis in axes if axis not in columns]
    if missing:
        raise ValueError(f"Unknown heatmap axis/axes: {', '.join(missing)}")


def _is_executable(row: dict[str, Any]) -> bool:
    return row.get("fit_5_shares") in FIT_OK and not _has_bad_flags(row)


def _has_bad_flags(row: dict[str, Any]) -> bool:
    return any(_truthy(row.get(column)) for column in BAD_FLAG_COLUMNS)


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.lower() not in {"", "0", "false", "none"}
    return bool(value)


def _avg(rows: Iterable[dict[str, Any]], column: str) -> float | None:
    values: list[float] = []
    for row in rows:
        value = row.get(column)
        if value is None or value == "":
            continue
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            continue
    if not values:
        return None
    return sum(values) / len(values)


def _rate(rows: Sequence[dict[str, Any]], predicate: Any) -> float | None:
    if not rows:
        return None
    return sum(1 for row in rows if predicate(row)) / len(rows)


def write_json(rows: Sequence[HeatmapRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(row) for row in rows], indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(rows: Sequence[HeatmapRow], path: Path, *, axes: Sequence[str], source: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Collector heatmap",
        "",
        f"Source: `{source}`",
        f"Axes: `{', '.join(axes)}`",
        "",
        "This is an executable-context summary, not a strategy GO/NO-GO decision.",
        "",
    ]
    if not rows:
        lines.append("No rows matched the requested filters.")
    else:
        for row in rows:
            title = ", ".join(f"{key}={value}" for key, value in row.group.items())
            lines.extend(
                [
                    f"## {title}",
                    "",
                    f"- rows: `{row.rows}`",
                    f"- executable rows: `{row.executable_rows}`",
                    f"- stale/bad rows: `{row.stale_or_bad_rows}`",
                    f"- fit 5-share rate: `{_fmt_pct(row.fit_5_share_rate)}`",
                    f"- fit 10 USDC rate: `{_fmt_pct(row.fit_10_usdc_rate)}`",
                    f"- avg spread: `{_fmt_float(row.avg_spread)}`",
                    f"- avg imbalance top1: `{_fmt_float(row.avg_imbalance_top1)}`",
                    f"- avg required hit rate, 5 shares: `{_fmt_pct(row.avg_required_hit_rate_5_shares)}`",
                    f"- avg spot return 30s: `{_fmt_pct(row.avg_spot_return_30s)}`",
                    f"- immediate reversal rate: `{_fmt_pct(row.immediate_reversal_rate)}`",
                    f"- held at/above rate: `{_fmt_pct(row.held_at_or_above_rate)}`",
                    f"- avg max favorable move: `{_fmt_float(row.avg_max_favorable_move)}`",
                    f"- avg max adverse move: `{_fmt_float(row.avg_max_adverse_move)}`",
                    "",
                ]
            )
    path.write_text("\n".join(lines), encoding="utf-8")


def _fmt_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.6f}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-sqlite", type=Path, default=default_input_path())
    parser.add_argument("--output-md", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--axis", action="append", dest="axes", default=None, help="Repeatable group-by axis")
    parser.add_argument("--min-rows", type=int, default=1)
    parser.add_argument("--executable-only", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    axes = tuple(args.axes) if args.axes else DEFAULT_AXES
    rows = build_collector_heatmap(
        args.input_sqlite,
        axes=axes,
        min_rows=args.min_rows,
        executable_only=args.executable_only,
    )
    write_markdown(rows, args.output_md, axes=axes, source=args.input_sqlite)
    write_json(rows, args.output_json)
    print(f"wrote {len(rows)} heatmap groups to {args.output_md} and {args.output_json}")


if __name__ == "__main__":
    main()
