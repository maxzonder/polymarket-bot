#!/usr/bin/env python3
"""Build a compact post-run summary for a multilevel collector SQLite DB.

This is intentionally lightweight: it gives the operator one artifact to inspect
right after a tmux collector run before diving into the heatmap/post-touch/signal
reports.
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

from utils.paths import DATA_DIR  # noqa: E402

DEFAULT_INPUT = DATA_DIR / "short_horizon" / "phase0" / "live_depth_survival.sqlite3"
DEFAULT_OUTPUT_MD = DATA_DIR / "short_horizon" / "phase0" / "collector_run_summary.md"
DEFAULT_OUTPUT_JSON = DATA_DIR / "short_horizon" / "phase0" / "collector_run_summary.json"


@dataclass(frozen=True)
class CollectorRunSummary:
    source: str
    table_counts: dict[str, int | None]
    run_ids: list[str]
    horizon_counts: list[dict[str, Any]]
    asset_counts: list[dict[str, Any]]
    level_counts: list[dict[str, Any]]
    quality_counts: dict[str, int]
    best_signal_slices: list[dict[str, Any]]
    worst_signal_slices: list[dict[str, Any]]
    recommendation: str


def build_summary(
    db_path: Path,
    *,
    signal_json: Path | None = None,
    top_n: int = 5,
) -> CollectorRunSummary:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        counts = {name: _count_table(conn, name) for name in ("collection_runs", "touch_events", "book_snapshots", "spot_snapshots", "post_touch_enrichment")}
        run_ids = _run_ids(conn)
        horizon_counts = _group_counts(conn, "horizon_bucket", top_n=top_n)
        asset_counts = _group_counts(conn, "asset_slug", top_n=top_n)
        level_counts = _group_counts(conn, "touch_level", top_n=top_n)
        quality_counts = _quality_counts(conn)
    finally:
        conn.close()

    signal_rows = _load_signal_rows(signal_json) if signal_json else []
    best = _rank_signal_rows(signal_rows, reverse=True, top_n=top_n)
    worst = _rank_signal_rows(signal_rows, reverse=False, top_n=top_n)
    recommendation = _recommendation(counts, best, signal_rows)
    return CollectorRunSummary(
        source=str(db_path),
        table_counts=counts,
        run_ids=run_ids,
        horizon_counts=horizon_counts,
        asset_counts=asset_counts,
        level_counts=level_counts,
        quality_counts=quality_counts,
        best_signal_slices=best,
        worst_signal_slices=worst,
        recommendation=recommendation,
    )


def write_json(summary: CollectorRunSummary, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(summary), indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(summary: CollectorRunSummary, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Collector run summary")
    lines.append("")
    lines.append(f"Source: `{summary.source}`")
    if summary.run_ids:
        lines.append(f"Run ids: {', '.join(f'`{run_id}`' for run_id in summary.run_ids)}")
    lines.append("")
    lines.append("## Counts")
    for name, count in summary.table_counts.items():
        value = "missing" if count is None else str(count)
        lines.append(f"- {name}: {value}")
    lines.append("")
    lines.append("## Quality")
    for name, count in summary.quality_counts.items():
        lines.append(f"- {name}: {count}")
    lines.append("")
    _append_group(lines, "Horizons", summary.horizon_counts, "horizon_bucket")
    _append_group(lines, "Assets", summary.asset_counts, "asset_slug")
    _append_group(lines, "Touch levels", summary.level_counts, "touch_level")
    _append_signal(lines, "Best signal slices", summary.best_signal_slices)
    _append_signal(lines, "Worst signal slices", summary.worst_signal_slices)
    lines.append("## Recommendation")
    lines.append(f"- {summary.recommendation}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _count_table(conn: sqlite3.Connection, table: str) -> int | None:
    if not _has_table(conn, table):
        return None
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not _has_table(conn, table):
        return False
    return any(str(row[1]) == column for row in conn.execute(f"PRAGMA table_info({table})"))


def _run_ids(conn: sqlite3.Connection) -> list[str]:
    if not _has_table(conn, "collection_runs") or not _has_column(conn, "collection_runs", "run_id"):
        return []
    order_column = "started_at" if _has_column(conn, "collection_runs", "started_at") else "run_id"
    return [str(row[0]) for row in conn.execute(f"SELECT run_id FROM collection_runs ORDER BY {order_column}, run_id")]


def _group_counts(conn: sqlite3.Connection, column: str, *, top_n: int) -> list[dict[str, Any]]:
    if not _has_column(conn, "touch_events", column):
        return []
    rows = conn.execute(
        f"""
        SELECT {column} AS value, COUNT(*) AS rows
        FROM touch_events
        GROUP BY {column}
        ORDER BY rows DESC, value
        LIMIT ?
        """,
        (top_n,),
    ).fetchall()
    return [{"value": row["value"], "rows": int(row["rows"])} for row in rows]


def _quality_counts(conn: sqlite3.Connection) -> dict[str, int]:
    if not _has_table(conn, "touch_events"):
        return {}
    out: dict[str, int] = {}
    for name in (
        "missing_depth_flag_at_touch",
        "crossed_book_flag_at_touch",
        "book_stale_flag_at_touch",
        "wide_spread_flag_at_touch",
        "zero_size_level_flag_at_touch",
    ):
        if _has_column(conn, "touch_events", name):
            out[name] = int(conn.execute(f"SELECT COUNT(*) FROM touch_events WHERE COALESCE({name}, 0) != 0").fetchone()[0])
    if _has_column(conn, "touch_events", "fit_5_shares"):
        out["fit_5_executable_rows"] = int(
            conn.execute("SELECT COUNT(*) FROM touch_events WHERE fit_5_shares IN ('+0_tick', '+1_tick', '+2plus_ticks')").fetchone()[0]
        )
    return out


def _load_signal_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def _rank_signal_rows(rows: Sequence[dict[str, Any]], *, reverse: bool, top_n: int) -> list[dict[str, Any]]:
    scored = [row for row in rows if _number(row.get("total_ev_5_shares")) is not None]
    scored.sort(key=lambda row: (_number(row.get("total_ev_5_shares")) or 0.0, row.get("rows_after_filters") or 0), reverse=reverse)
    return [_compact_signal_row(row) for row in scored[:top_n]]


def _compact_signal_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "group": row.get("group") or {},
        "rows_after_filters": row.get("rows_after_filters"),
        "win_rate": row.get("win_rate"),
        "avg_required_hit_rate_5_shares": row.get("avg_required_hit_rate_5_shares"),
        "total_ev_5_shares": row.get("total_ev_5_shares"),
        "roi_on_cost_5_shares": row.get("roi_on_cost_5_shares"),
        "avg_spread": row.get("avg_spread"),
        "held_5s_rate": row.get("held_5s_rate"),
        "reversal_5s_rate": row.get("reversal_5s_rate"),
        "score": row.get("score"),
    }


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _recommendation(counts: dict[str, int | None], best: Sequence[dict[str, Any]], signal_rows: Sequence[dict[str, Any]]) -> str:
    touches = counts.get("touch_events") or 0
    if touches == 0:
        return "No touch events collected; treat this as an ingest/schema smoke only."
    if not signal_rows:
        return "Signal report has no joined slices yet; inspect heatmap/post-touch now and rerun outcome EV after markets resolve."
    positives = [row for row in best if (_number(row.get("total_ev_5_shares")) or 0.0) > 0]
    if positives:
        return "Positive slices exist; validate them on another run/day before considering any live strategy work."
    return "No positive signal slice after filters; use this run as negative evidence and adjust universe/filters."


def _append_group(lines: list[str], title: str, rows: Sequence[dict[str, Any]], key_name: str) -> None:
    lines.append(f"## {title}")
    if not rows:
        lines.append("- none")
    for row in rows:
        lines.append(f"- {row.get(key_name, row.get('value'))}: {row.get('rows')}")
    lines.append("")


def _append_signal(lines: list[str], title: str, rows: Sequence[dict[str, Any]]) -> None:
    lines.append(f"## {title}")
    if not rows:
        lines.append("- none")
    for row in rows:
        group = row.get("group") or {}
        group_text = ", ".join(f"{key}={value}" for key, value in group.items()) or "all"
        lines.append(
            "- "
            f"{group_text}: rows={row.get('rows_after_filters')}, "
            f"ev_5={_fmt(row.get('total_ev_5_shares'))}, "
            f"roi_5={_fmt_pct(row.get('roi_on_cost_5_shares'))}, "
            f"wr={_fmt_pct(row.get('win_rate'))}, "
            f"req={_fmt_pct(row.get('avg_required_hit_rate_5_shares'))}, "
            f"spread={_fmt(row.get('avg_spread'))}"
        )
    lines.append("")


def _fmt(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "n/a"
    return f"{number:.4g}"


def _fmt_pct(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "n/a"
    return f"{number * 100:.1f}%"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-sqlite", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--signal-json", type=Path, default=None)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--top-n", type=int, default=5)
    args = parser.parse_args()

    summary = build_summary(args.input_sqlite, signal_json=args.signal_json, top_n=args.top_n)
    write_markdown(summary, args.output_md)
    write_json(summary, args.output_json)


if __name__ == "__main__":
    main()
