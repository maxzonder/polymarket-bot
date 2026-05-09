#!/usr/bin/env python3
"""Join multilevel collector touches with resolved outcomes and summarize EV.

This is research infrastructure only. It answers: if we hypothetically bought
the touched token at live ask-depth context, which slices had positive or
negative realized outcome EV after resolution?
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
from utils.paths import DATA_DIR  # noqa: E402

DEFAULT_INPUT = DATA_DIR / "short_horizon" / "phase0" / "live_depth_survival.sqlite3"
DEFAULT_RESOLUTIONS = DATA_DIR / "short_horizon" / "data" / "market_resolutions.sqlite3"
DEFAULT_OUTPUT_MD = DATA_DIR / "short_horizon" / "phase0" / "collector_outcome_ev.md"
DEFAULT_OUTPUT_JSON = DATA_DIR / "short_horizon" / "phase0" / "collector_outcome_ev.json"
DEFAULT_AXES = ("touch_level", "asset_slug", "outcome", "horizon_bucket")
FIT_OK = {"+0_tick", "+1_tick", "+2plus_ticks"}
BAD_FLAG_COLUMNS = (
    "missing_depth_flag_at_touch",
    "crossed_book_flag_at_touch",
    "book_stale_flag_at_touch",
)


@dataclass(frozen=True)
class OutcomeEvRow:
    group: dict[str, Any]
    rows: int
    joined_rows: int
    executable_rows: int
    winners: int
    win_rate: float | None
    avg_entry_price_5_shares: float | None
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
    immediate_reversal_rate: float | None
    held_at_or_above_rate: float | None


@dataclass(frozen=True)
class JoinedTouch:
    group_values: tuple[Any, ...]
    is_joined: bool
    is_executable_5_shares: bool
    is_winner: bool | None
    entry_price_5_shares: float | None
    required_hit_rate_5_shares: float | None
    cost_5_shares: float | None
    fee_5_shares: float
    ev_5_shares: float | None
    fit_10_usdc: str | None
    avg_entry_price_10_usdc: float | None
    ev_10_usdc: float | None
    spread: float | None
    immediate_reversal: bool | None
    held_at_or_above: bool | None


def build_collector_outcome_ev(
    db_path: Path,
    resolutions_path: Path,
    *,
    axes: Sequence[str] = DEFAULT_AXES,
    min_rows: int = 1,
    executable_only: bool = False,
) -> list[OutcomeEvRow]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        _validate_axes(conn, axes)
        touches = [dict(row) for row in conn.execute("SELECT * FROM touch_events")]
    finally:
        conn.close()

    resolutions = load_resolutions(resolutions_path)
    joined = [_join_touch(row, resolutions.get(str(row.get("market_id") or "")), axes) for row in touches]
    if executable_only:
        joined = [row for row in joined if row.is_executable_5_shares]

    groups: dict[tuple[Any, ...], list[JoinedTouch]] = {}
    for row in joined:
        groups.setdefault(row.group_values, []).append(row)

    out: list[OutcomeEvRow] = []
    for key, group_rows in sorted(groups.items(), key=lambda item: _safe_sort_key(item[0])):
        if len(group_rows) < min_rows:
            continue
        joined_rows = [row for row in group_rows if row.is_joined]
        if not joined_rows:
            continue
        executable_rows = [row for row in joined_rows if row.is_executable_5_shares]
        winners = sum(1 for row in joined_rows if row.is_winner)
        win_rate = winners / len(joined_rows) if joined_rows else None
        avg_required = _avg_values(row.required_hit_rate_5_shares for row in executable_rows)
        ev_5_values = [row.ev_5_shares for row in executable_rows if row.ev_5_shares is not None]
        cost_5_values = [
            (row.cost_5_shares or 0.0) + row.fee_5_shares
            for row in executable_rows
            if row.cost_5_shares is not None
        ]
        fit_10_rows = [row for row in joined_rows if row.fit_10_usdc in FIT_OK and row.ev_10_usdc is not None]
        ev_10_values = [row.ev_10_usdc for row in fit_10_rows if row.ev_10_usdc is not None]
        out.append(
            OutcomeEvRow(
                group={axis: value for axis, value in zip(axes, key)},
                rows=len(group_rows),
                joined_rows=len(joined_rows),
                executable_rows=len(executable_rows),
                winners=winners,
                win_rate=win_rate,
                avg_entry_price_5_shares=_avg_values(row.entry_price_5_shares for row in executable_rows),
                avg_required_hit_rate_5_shares=avg_required,
                edge_vs_required_hit_rate_5_shares=(win_rate - avg_required) if win_rate is not None and avg_required is not None else None,
                total_ev_5_shares=sum(ev_5_values) if ev_5_values else None,
                avg_ev_5_shares=_avg_values(ev_5_values),
                roi_on_cost_5_shares=(sum(ev_5_values) / sum(cost_5_values)) if ev_5_values and sum(cost_5_values) else None,
                fit_10_usdc_rows=len(fit_10_rows),
                total_ev_10_usdc=sum(ev_10_values) if ev_10_values else None,
                avg_ev_10_usdc=_avg_values(ev_10_values),
                roi_on_cost_10_usdc=(sum(ev_10_values) / (10.0 * len(ev_10_values))) if ev_10_values else None,
                avg_spread=_avg_values(row.spread for row in joined_rows),
                immediate_reversal_rate=_rate(joined_rows, lambda row: row.immediate_reversal is True),
                held_at_or_above_rate=_rate(joined_rows, lambda row: row.held_at_or_above is True),
            )
        )
    out.sort(key=lambda row: _safe_sort_key(tuple(row.group.values())))
    return out


def _safe_sort_key(values: tuple[Any, ...]) -> tuple[tuple[str, str], ...]:
    return tuple(("" if value is None else type(value).__name__, "" if value is None else str(value)) for value in values)


def distinct_market_ids(db_path: Path) -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        return [str(row[0]) for row in conn.execute("SELECT DISTINCT market_id FROM touch_events WHERE market_id IS NOT NULL")]
    finally:
        conn.close()


def load_resolutions(path: Path) -> dict[str, dict[str, Any]]:
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


def write_json(rows: Sequence[OutcomeEvRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(row) for row in rows], indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(rows: Sequence[OutcomeEvRow], path: Path, *, axes: Sequence[str], source: Path, resolutions: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Collector outcome/EV join",
        "",
        f"Source: `{source}`",
        f"Resolutions: `{resolutions}`",
        f"Axes: `{', '.join(axes)}`",
        "",
        "This is a resolved-outcome research join over collector touches, not a trading decision.",
        "EV assumes buying the touched token at observed ask-depth context and holding to resolution.",
        "",
    ]
    if not rows:
        lines.append("No resolved joined rows matched the requested filters.")
    else:
        for row in rows:
            title = ", ".join(f"{key}={value}" for key, value in row.group.items())
            lines.extend(
                [
                    f"## {title}",
                    "",
                    f"- rows / joined / executable 5-share rows: `{row.rows}` / `{row.joined_rows}` / `{row.executable_rows}`",
                    f"- winners / win rate: `{row.winners}` / `{_fmt_pct(row.win_rate)}`",
                    f"- avg entry 5 shares: `{_fmt_float(row.avg_entry_price_5_shares)}`",
                    f"- avg required hit rate 5 shares: `{_fmt_pct(row.avg_required_hit_rate_5_shares)}`",
                    f"- edge vs required hit rate 5 shares: `{_fmt_pct(row.edge_vs_required_hit_rate_5_shares, signed=True)}`",
                    f"- total / avg EV 5 shares: `{_fmt_money(row.total_ev_5_shares)}` / `{_fmt_money(row.avg_ev_5_shares)}`",
                    f"- ROI on cost 5 shares: `{_fmt_pct(row.roi_on_cost_5_shares, signed=True)}`",
                    f"- fit 10 USDC rows: `{row.fit_10_usdc_rows}`",
                    f"- total / avg EV 10 USDC: `{_fmt_money(row.total_ev_10_usdc)}` / `{_fmt_money(row.avg_ev_10_usdc)}`",
                    f"- ROI on cost 10 USDC: `{_fmt_pct(row.roi_on_cost_10_usdc, signed=True)}`",
                    f"- avg spread: `{_fmt_float(row.avg_spread)}`",
                    f"- reversal / held rate: `{_fmt_pct(row.immediate_reversal_rate)}` / `{_fmt_pct(row.held_at_or_above_rate)}`",
                    "",
                ]
            )
    path.write_text("\n".join(lines), encoding="utf-8")


def _join_touch(row: dict[str, Any], resolution: dict[str, Any] | None, axes: Sequence[str]) -> JoinedTouch:
    is_joined = bool(resolution and resolution.get("outcome_token_id"))
    token_id = str(row.get("token_id") or "")
    is_winner = None if not is_joined else token_id == str(resolution.get("outcome_token_id"))
    is_executable = row.get("fit_5_shares") in FIT_OK and not _has_bad_flags(row)
    cost_5 = _float_or_none(row.get("cost_for_5_shares"))
    fee_5 = _float_or_none(row.get("estimated_fee_for_5_shares")) or 0.0
    avg_entry_5 = _float_or_none(row.get("avg_entry_price_for_5_shares"))
    if cost_5 is None and avg_entry_5 is not None:
        cost_5 = avg_entry_5 * 5.0
    ev_5 = None
    if is_joined and is_executable and cost_5 is not None and is_winner is not None:
        payout = 5.0 if is_winner else 0.0
        ev_5 = payout - cost_5 - fee_5
    fit_10 = _clean_text(row.get("fit_10_usdc"))
    avg_entry_10 = _float_or_none(row.get("avg_entry_price_for_10_usdc"))
    ev_10 = None
    if is_joined and fit_10 in FIT_OK and avg_entry_10 is not None and avg_entry_10 > 0 and is_winner is not None:
        shares = 10.0 / avg_entry_10
        payout = shares if is_winner else 0.0
        ev_10 = payout - 10.0
    return JoinedTouch(
        group_values=tuple(row.get(axis) for axis in axes),
        is_joined=is_joined,
        is_executable_5_shares=is_executable and is_joined,
        is_winner=is_winner,
        entry_price_5_shares=avg_entry_5,
        required_hit_rate_5_shares=_float_or_none(row.get("required_hit_rate_for_5_shares")),
        cost_5_shares=cost_5,
        fee_5_shares=fee_5,
        ev_5_shares=ev_5,
        fit_10_usdc=fit_10,
        avg_entry_price_10_usdc=avg_entry_10,
        ev_10_usdc=ev_10,
        spread=_float_or_none(row.get("spread_at_touch")),
        immediate_reversal=_bool_or_none(row.get("immediate_reversal_flag")),
        held_at_or_above=_bool_or_none(row.get("held_at_or_above_level")),
    )


def _validate_axes(conn: sqlite3.Connection, axes: Sequence[str]) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(touch_events)")}
    missing = [axis for axis in axes if axis not in columns]
    if missing:
        raise ValueError(f"Unknown axis/axes: {', '.join(missing)}")


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


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


def _avg_values(values: Iterable[float | None]) -> float | None:
    nums = [float(value) for value in values if value is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def _rate(rows: Sequence[JoinedTouch], predicate: Any) -> float | None:
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
    parser.add_argument("--axis", action="append", dest="axes", default=None, help="Repeatable group-by axis")
    parser.add_argument("--min-rows", type=int, default=1)
    parser.add_argument("--executable-only", action="store_true")
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
    rows = build_collector_outcome_ev(
        args.input_sqlite,
        args.resolutions_sqlite,
        axes=axes,
        min_rows=args.min_rows,
        executable_only=args.executable_only,
    )
    write_markdown(rows, args.output_md, axes=axes, source=args.input_sqlite, resolutions=args.resolutions_sqlite)
    write_json(rows, args.output_json)
    print(f"wrote {len(rows)} outcome/EV groups to {args.output_md} and {args.output_json}")


if __name__ == "__main__":
    main()
