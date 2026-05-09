#!/usr/bin/env python3
"""Build a combined signal report from collector EV + post-touch context.

This is an offline research evaluator, not trading code. It answers whether a
slice remains attractive after basic executable-trade filters, resolved outcome
EV, and post-touch hold/reversal context are considered together.
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

from scripts.backfill_market_resolutions import backfill_market_resolutions  # noqa: E402
from scripts.enrich_collector_post_touch import enrich_post_touch  # noqa: E402
from utils.paths import DATA_DIR  # noqa: E402

DEFAULT_INPUT = DATA_DIR / "short_horizon" / "phase0" / "live_depth_survival.sqlite3"
DEFAULT_RESOLUTIONS = DATA_DIR / "short_horizon" / "data" / "market_resolutions.sqlite3"
DEFAULT_OUTPUT_MD = DATA_DIR / "short_horizon" / "phase0" / "collector_signal_report.md"
DEFAULT_OUTPUT_JSON = DATA_DIR / "short_horizon" / "phase0" / "collector_signal_report.json"
DEFAULT_AXES = ("duration_seconds", "asset_slug", "outcome")
FIT_OK = {"+0_tick", "+1_tick", "+2plus_ticks"}
BAD_FLAG_COLUMNS = (
    "missing_depth_flag_at_touch",
    "crossed_book_flag_at_touch",
    "book_stale_flag_at_touch",
)


@dataclass(frozen=True)
class SignalReportRow:
    group: dict[str, Any]
    rows_seen: int
    rows_joined: int
    rows_after_filters: int
    winners: int
    win_rate: float | None
    avg_required_hit_rate_5_shares: float | None
    edge_vs_required_hit_rate_5_shares: float | None
    total_ev_5_shares: float | None
    avg_ev_5_shares: float | None
    roi_on_cost_5_shares: float | None
    fit_10_usdc_rows: int
    total_ev_10_usdc: float | None
    avg_ev_10_usdc: float | None
    roi_on_cost_10_usdc: float | None
    avg_spread: float | None
    avg_entry_price_5_shares: float | None
    stale_or_bad_rows: int
    wide_spread_rows: int
    held_1s_rate: float | None
    held_5s_rate: float | None
    held_15s_rate: float | None
    held_60s_rate: float | None
    reversal_1s_rate: float | None
    reversal_5s_rate: float | None
    reversal_15s_rate: float | None
    reversal_60s_rate: float | None
    avg_move_5s: float | None
    avg_move_60s: float | None
    score: float | None


@dataclass(frozen=True)
class JoinedRow:
    group_values: tuple[Any, ...]
    joined: bool
    passed_filters: bool
    winner: bool | None
    cost_5: float | None
    fee_5: float
    avg_entry_5: float | None
    required_hit_5: float | None
    ev_5: float | None
    fit_10: str | None
    avg_entry_10: float | None
    ev_10: float | None
    spread: float | None
    stale_or_bad: bool
    wide_spread: bool
    held_1s: bool | None
    held_5s: bool | None
    held_15s: bool | None
    held_60s: bool | None
    reversal_1s: bool | None
    reversal_5s: bool | None
    reversal_15s: bool | None
    reversal_60s: bool | None
    move_5s: float | None
    move_60s: float | None


def build_signal_report(
    db_path: Path,
    resolutions_path: Path,
    *,
    axes: Sequence[str] = DEFAULT_AXES,
    min_rows: int = 10,
    max_spread: float | None = 0.10,
    include_bad_flags: bool = False,
    require_fit_5_shares: bool = True,
    require_joined: bool = True,
    refresh_enrichment: bool = False,
    asset_slug_filter: Sequence[str] = (),
    outcome_filter: Sequence[str] = (),
    duration_seconds_filter: Sequence[int] = (),
    require_held_ms: Sequence[int] = (),
    reject_reversal_ms: Sequence[int] = (),
) -> list[SignalReportRow]:
    if refresh_enrichment or not _has_table(db_path, "post_touch_enrichment"):
        enrich_post_touch(db_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        _validate_axes(conn, axes)
        touch_rows = [dict(row) for row in conn.execute("SELECT * FROM touch_events")]
        post_rows = {
            str(row["probe_id"]): dict(row)
            for row in conn.execute("SELECT * FROM post_touch_enrichment")
        }
    finally:
        conn.close()

    resolutions = _load_resolutions(resolutions_path)
    touch_rows = [
        row
        for row in touch_rows
        if _matches_value_filter(row.get("asset_slug"), asset_slug_filter)
        and _matches_value_filter(row.get("outcome"), outcome_filter)
        and _matches_value_filter(row.get("duration_seconds"), duration_seconds_filter)
    ]

    joined = [
        _join_row(
            row,
            post_rows.get(str(row.get("probe_id") or "")),
            resolutions.get(str(row.get("market_id") or "")),
            axes,
            max_spread=max_spread,
            include_bad_flags=include_bad_flags,
            require_fit_5_shares=require_fit_5_shares,
            require_joined=require_joined,
            require_held_ms=require_held_ms,
            reject_reversal_ms=reject_reversal_ms,
        )
        for row in touch_rows
    ]

    groups: dict[tuple[Any, ...], list[JoinedRow]] = {}
    for row in joined:
        groups.setdefault(row.group_values, []).append(row)

    out: list[SignalReportRow] = []
    for key, group_rows in sorted(groups.items(), key=lambda item: item[0]):
        rows_joined = [row for row in group_rows if row.joined]
        filtered = [row for row in group_rows if row.passed_filters]
        if len(filtered) < min_rows:
            continue
        winners = sum(1 for row in filtered if row.winner)
        win_rate = winners / len(filtered) if filtered else None
        avg_required = _avg(row.required_hit_5 for row in filtered)
        ev_5_values = [row.ev_5 for row in filtered if row.ev_5 is not None]
        cost_5_values = [(row.cost_5 or 0.0) + row.fee_5 for row in filtered if row.cost_5 is not None]
        fit_10_rows = [row for row in filtered if row.fit_10 in FIT_OK and row.ev_10 is not None]
        ev_10_values = [row.ev_10 for row in fit_10_rows if row.ev_10 is not None]
        roi_5 = (sum(ev_5_values) / sum(cost_5_values)) if ev_5_values and sum(cost_5_values) else None
        held_5s = _rate(filtered, lambda row: row.held_5s is True)
        reversal_5s = _rate(filtered, lambda row: row.reversal_5s is True)
        edge = (win_rate - avg_required) if win_rate is not None and avg_required is not None else None
        # Lightweight rank score: prefer positive realized edge, low reversal, and decent 5s hold.
        score = None
        if edge is not None:
            score = edge + (held_5s or 0.0) * 0.05 - (reversal_5s or 0.0) * 0.05
        out.append(
            SignalReportRow(
                group={axis: value for axis, value in zip(axes, key)},
                rows_seen=len(group_rows),
                rows_joined=len(rows_joined),
                rows_after_filters=len(filtered),
                winners=winners,
                win_rate=win_rate,
                avg_required_hit_rate_5_shares=avg_required,
                edge_vs_required_hit_rate_5_shares=edge,
                total_ev_5_shares=sum(ev_5_values) if ev_5_values else None,
                avg_ev_5_shares=_avg(ev_5_values),
                roi_on_cost_5_shares=roi_5,
                fit_10_usdc_rows=len(fit_10_rows),
                total_ev_10_usdc=sum(ev_10_values) if ev_10_values else None,
                avg_ev_10_usdc=_avg(ev_10_values),
                roi_on_cost_10_usdc=(sum(ev_10_values) / (10.0 * len(ev_10_values))) if ev_10_values else None,
                avg_spread=_avg(row.spread for row in filtered),
                avg_entry_price_5_shares=_avg(row.avg_entry_5 for row in filtered),
                stale_or_bad_rows=sum(1 for row in group_rows if row.stale_or_bad),
                wide_spread_rows=sum(1 for row in group_rows if row.wide_spread),
                held_1s_rate=_rate(filtered, lambda row: row.held_1s is True),
                held_5s_rate=held_5s,
                held_15s_rate=_rate(filtered, lambda row: row.held_15s is True),
                held_60s_rate=_rate(filtered, lambda row: row.held_60s is True),
                reversal_1s_rate=_rate(filtered, lambda row: row.reversal_1s is True),
                reversal_5s_rate=reversal_5s,
                reversal_15s_rate=_rate(filtered, lambda row: row.reversal_15s is True),
                reversal_60s_rate=_rate(filtered, lambda row: row.reversal_60s is True),
                avg_move_5s=_avg(row.move_5s for row in filtered),
                avg_move_60s=_avg(row.move_60s for row in filtered),
                score=score,
            )
        )
    out.sort(key=lambda row: (row.score if row.score is not None else -999.0, row.total_ev_5_shares or -999.0), reverse=True)
    return out


def distinct_market_ids(db_path: Path) -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        return [str(row[0]) for row in conn.execute("SELECT DISTINCT market_id FROM touch_events WHERE market_id IS NOT NULL")]
    finally:
        conn.close()


def write_json(rows: Sequence[SignalReportRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(row) for row in rows], indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(
    rows: Sequence[SignalReportRow],
    path: Path,
    *,
    source: Path,
    resolutions: Path,
    axes: Sequence[str],
    max_spread: float | None,
    min_rows: int,
    extra_filters: str = "none",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Collector signal report",
        "",
        f"Source: `{source}`",
        f"Resolutions: `{resolutions}`",
        f"Axes: `{', '.join(axes)}`",
        f"Filters: min_rows=`{min_rows}`, max_spread=`{max_spread}`, require_good_flags=`true`, require_fit_5_shares=`true`",
        f"Extra filters: `{extra_filters}`",
        "",
        "This is an offline signal report, not trading code. Rows are sorted by a simple edge/hold/reversal score.",
        "",
    ]
    if not rows:
        lines.append("No slices passed filters.")
    else:
        lines.append("## Top slices")
        lines.append("")
        for row in rows[:20]:
            title = ", ".join(f"{key}={value}" for key, value in row.group.items())
            lines.extend(_row_lines(title, row))
        lines.append("## Bottom slices")
        lines.append("")
        bottom = sorted(rows, key=lambda row: (row.total_ev_5_shares if row.total_ev_5_shares is not None else 999.0))[:10]
        for row in bottom:
            title = ", ".join(f"{key}={value}" for key, value in row.group.items())
            lines.extend(_row_lines(title, row))
    path.write_text("\n".join(lines), encoding="utf-8")


def _row_lines(title: str, row: SignalReportRow) -> list[str]:
    return [
        f"### {title}",
        "",
        f"- rows seen / after filters: `{row.rows_seen}` / `{row.rows_after_filters}`",
        f"- winners / win rate: `{row.winners}` / `{_fmt_pct(row.win_rate)}`",
        f"- required hit / edge: `{_fmt_pct(row.avg_required_hit_rate_5_shares)}` / `{_fmt_pct(row.edge_vs_required_hit_rate_5_shares, signed=True)}`",
        f"- 5-share EV / ROI: `{_fmt_money(row.total_ev_5_shares)}` / `{_fmt_pct(row.roi_on_cost_5_shares, signed=True)}`",
        f"- 10 USDC rows / EV / ROI: `{row.fit_10_usdc_rows}` / `{_fmt_money(row.total_ev_10_usdc)}` / `{_fmt_pct(row.roi_on_cost_10_usdc, signed=True)}`",
        f"- avg spread / avg entry: `{_fmt_float(row.avg_spread)}` / `{_fmt_float(row.avg_entry_price_5_shares)}`",
        f"- held 1s/5s/15s/60s: `{_fmt_pct(row.held_1s_rate)}` / `{_fmt_pct(row.held_5s_rate)}` / `{_fmt_pct(row.held_15s_rate)}` / `{_fmt_pct(row.held_60s_rate)}`",
        f"- reversal 1s/5s/15s/60s: `{_fmt_pct(row.reversal_1s_rate)}` / `{_fmt_pct(row.reversal_5s_rate)}` / `{_fmt_pct(row.reversal_15s_rate)}` / `{_fmt_pct(row.reversal_60s_rate)}`",
        f"- avg move 5s / 60s: `{_fmt_float(row.avg_move_5s)}` / `{_fmt_float(row.avg_move_60s)}`",
        f"- score: `{_fmt_float(row.score)}`",
        "",
    ]


def _join_row(
    row: dict[str, Any],
    post: dict[str, Any] | None,
    resolution: dict[str, Any] | None,
    axes: Sequence[str],
    *,
    max_spread: float | None,
    include_bad_flags: bool,
    require_fit_5_shares: bool,
    require_joined: bool,
    require_held_ms: Sequence[int],
    reject_reversal_ms: Sequence[int],
) -> JoinedRow:
    joined = bool(resolution and resolution.get("outcome_token_id"))
    token_id = str(row.get("token_id") or "")
    winner = None if not joined else token_id == str(resolution.get("outcome_token_id"))
    fit_5 = _clean_text(row.get("fit_5_shares"))
    fit_10 = _clean_text(row.get("fit_10_usdc"))
    spread = _float_or_none(row.get("spread_at_touch"))
    stale_or_bad = _has_bad_flags(row)
    wide_spread = max_spread is not None and spread is not None and spread > max_spread
    cost_5 = _float_or_none(row.get("cost_for_5_shares"))
    avg_entry_5 = _float_or_none(row.get("avg_entry_price_for_5_shares"))
    if cost_5 is None and avg_entry_5 is not None:
        cost_5 = avg_entry_5 * 5.0
    fee_5 = _float_or_none(row.get("estimated_fee_for_5_shares")) or 0.0
    passed = True
    if require_joined and not joined:
        passed = False
    if require_fit_5_shares and fit_5 not in FIT_OK:
        passed = False
    if not include_bad_flags and stale_or_bad:
        passed = False
    if wide_spread:
        passed = False
    for horizon in require_held_ms:
        if _bool_or_none((post or {}).get(f"held_{int(horizon)}ms")) is not True:
            passed = False
    for horizon in reject_reversal_ms:
        if _bool_or_none((post or {}).get(f"reversal_{int(horizon)}ms")) is True:
            passed = False
    ev_5 = None
    if joined and fit_5 in FIT_OK and cost_5 is not None and winner is not None:
        ev_5 = (5.0 if winner else 0.0) - cost_5 - fee_5
    avg_entry_10 = _float_or_none(row.get("avg_entry_price_for_10_usdc"))
    ev_10 = None
    if joined and fit_10 in FIT_OK and avg_entry_10 is not None and avg_entry_10 > 0 and winner is not None:
        ev_10 = (10.0 / avg_entry_10 if winner else 0.0) - 10.0
    merged = dict(row)
    if post:
        merged.update(post)
    return JoinedRow(
        group_values=tuple(merged.get(axis) for axis in axes),
        joined=joined,
        passed_filters=passed,
        winner=winner,
        cost_5=cost_5,
        fee_5=fee_5,
        avg_entry_5=avg_entry_5,
        required_hit_5=_float_or_none(row.get("required_hit_rate_for_5_shares")),
        ev_5=ev_5 if passed else None,
        fit_10=fit_10,
        avg_entry_10=avg_entry_10,
        ev_10=ev_10 if passed else None,
        spread=spread,
        stale_or_bad=stale_or_bad,
        wide_spread=wide_spread,
        held_1s=_bool_or_none((post or {}).get("held_1000ms")),
        held_5s=_bool_or_none((post or {}).get("held_5000ms")),
        held_15s=_bool_or_none((post or {}).get("held_15000ms")),
        held_60s=_bool_or_none((post or {}).get("held_60000ms")),
        reversal_1s=_bool_or_none((post or {}).get("reversal_1000ms")),
        reversal_5s=_bool_or_none((post or {}).get("reversal_5000ms")),
        reversal_15s=_bool_or_none((post or {}).get("reversal_15000ms")),
        reversal_60s=_bool_or_none((post or {}).get("reversal_60000ms")),
        move_5s=_float_or_none((post or {}).get("move_5000ms")),
        move_60s=_float_or_none((post or {}).get("move_60000ms")),
    )


def _validate_axes(conn: sqlite3.Connection, axes: Sequence[str]) -> None:
    touch_columns = {row[1] for row in conn.execute("PRAGMA table_info(touch_events)")}
    post_columns = {row[1] for row in conn.execute("PRAGMA table_info(post_touch_enrichment)")}
    columns = touch_columns | post_columns
    missing = [axis for axis in axes if axis not in columns]
    if missing:
        raise ValueError(f"Unknown axis/axes: {', '.join(missing)}")


def _matches_value_filter(value: Any, allowed: Sequence[Any]) -> bool:
    if not allowed:
        return True
    normalized = str(value or "").lower()
    return normalized in {str(item).lower() for item in allowed}


def _load_resolutions(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        if not _table_exists(conn, "market_resolutions"):
            return {}
        return {str(row["market_id"]): dict(row) for row in conn.execute("SELECT * FROM market_resolutions")}
    finally:
        conn.close()


def _has_table(db_path: Path, table: str) -> bool:
    if not db_path.exists():
        return False
    conn = sqlite3.connect(db_path)
    try:
        return _table_exists(conn, table)
    finally:
        conn.close()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _has_bad_flags(row: dict[str, Any]) -> bool:
    return any(_truthy(row.get(column)) for column in BAD_FLAG_COLUMNS)


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.lower() not in {"", "0", "false", "none"}
    return bool(value)


def _bool_or_none(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    return _truthy(value)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _avg(values: Iterable[float | None]) -> float | None:
    nums = [float(value) for value in values if value is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def _rate(rows: Sequence[JoinedRow], predicate: Any) -> float | None:
    if not rows:
        return None
    return sum(1 for row in rows if predicate(row)) / len(rows)


def _fmt_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.6f}"


def _fmt_money(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.4f} USDC"


def _fmt_pct(value: float | None, *, signed: bool = False) -> str:
    if value is None:
        return "n/a"
    prefix = "+" if signed and value > 0 else ""
    return f"{prefix}{value * 100:.2f}%"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-sqlite", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--resolutions-sqlite", type=Path, default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--axis", action="append", dest="axes", default=None)
    parser.add_argument("--min-rows", type=int, default=10)
    parser.add_argument("--max-spread", type=float, default=0.10)
    parser.add_argument("--no-max-spread", action="store_true")
    parser.add_argument("--include-bad-flags", action="store_true")
    parser.add_argument("--include-non-fit-5-shares", action="store_true")
    parser.add_argument("--include-unresolved", action="store_true")
    parser.add_argument("--refresh-enrichment", action="store_true")
    parser.add_argument("--asset-slug-filter", action="append", default=None)
    parser.add_argument("--outcome-filter", action="append", default=None)
    parser.add_argument("--duration-seconds-filter", action="append", type=int, default=None)
    parser.add_argument("--require-held-ms", action="append", type=int, default=None, help="Require post-touch held_<ms>ms=1, e.g. 5000")
    parser.add_argument("--reject-reversal-ms", action="append", type=int, default=None, help="Reject rows with reversal_<ms>ms=1, e.g. 5000")
    parser.add_argument("--backfill-resolutions", action="store_true")
    parser.add_argument("--force-backfill", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    axes = tuple(args.axes) if args.axes else DEFAULT_AXES
    if args.backfill_resolutions:
        market_ids = distinct_market_ids(args.input_sqlite)
        summary = backfill_market_resolutions(
            market_ids,
            output_path=args.resolutions_sqlite,
            sleep_seconds=args.sleep_seconds,
            force=args.force_backfill,
        )
        print(
            "backfilled resolutions: "
            f"requested={summary.requested} already_present={summary.already_present} "
            f"fetched={summary.fetched} resolved={summary.resolved} unresolved={summary.unresolved}"
        )
    max_spread = None if args.no_max_spread else args.max_spread
    rows = build_signal_report(
        args.input_sqlite,
        args.resolutions_sqlite,
        axes=axes,
        min_rows=args.min_rows,
        max_spread=max_spread,
        include_bad_flags=args.include_bad_flags,
        require_fit_5_shares=not args.include_non_fit_5_shares,
        require_joined=not args.include_unresolved,
        refresh_enrichment=args.refresh_enrichment,
        asset_slug_filter=args.asset_slug_filter or (),
        outcome_filter=args.outcome_filter or (),
        duration_seconds_filter=args.duration_seconds_filter or (),
        require_held_ms=args.require_held_ms or (),
        reject_reversal_ms=args.reject_reversal_ms or (),
    )
    extra_filters = ", ".join(
        part
        for part in (
            f"asset={args.asset_slug_filter}" if args.asset_slug_filter else "",
            f"outcome={args.outcome_filter}" if args.outcome_filter else "",
            f"duration={args.duration_seconds_filter}" if args.duration_seconds_filter else "",
            f"require_held_ms={args.require_held_ms}" if args.require_held_ms else "",
            f"reject_reversal_ms={args.reject_reversal_ms}" if args.reject_reversal_ms else "",
        )
        if part
    ) or "none"
    write_markdown(
        rows,
        args.output_md,
        source=args.input_sqlite,
        resolutions=args.resolutions_sqlite,
        axes=axes,
        max_spread=max_spread,
        min_rows=args.min_rows,
        extra_filters=extra_filters,
    )
    write_json(rows, args.output_json)
    print(f"wrote {len(rows)} signal slices to {args.output_md} and {args.output_json}")


if __name__ == "__main__":
    main()
