#!/usr/bin/env python3
"""
Offline path-level optimizer for big_swan strategy parameters.

This script intentionally uses post-hoc historical swan paths from `swans_v2`.
It is NOT a causal or capital-realistic simulator.

Use it for:
- cheap exploration of buy levels;
- comparing binary-native TP ladders (`progress/fraction`);
- estimating resolution-vs-TP tradeoffs;
- ranking candidate buy-set / ladder presets before honest replay.

Do NOT interpret the output as final live expectancy.
Final validation should go through `scripts/run_tape_dryrun.py`.

Examples:
    python3 scripts/optimize_big_swan_params.py
    python3 scripts/optimize_big_swan_params.py --entry-levels 0.01,0.05,0.10
    python3 scripts/optimize_big_swan_params.py --tp-levels 0.10:0.10,0.50:0.20 --moonbag 0.70
    python3 scripts/optimize_big_swan_params.py --from-date 2026-01-01 --to-date 2026-01-31 --group-by month
    python3 scripts/optimize_big_swan_params.py --search --top 20
"""

from __future__ import annotations

import argparse
import itertools
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import BIG_SWAN_MODE, TPLevel
from strategy.big_swan_optimizer import (
    EntryPlanEvaluation,
    SwanPath,
    combine_entry_evaluations,
    evaluate_entry_plan,
    evaluate_ladder,
    validate_ladder,
)
from utils.paths import DB_PATH

EPS = 1e-9
DEFAULT_SEARCH_ENTRY_CANDIDATES = (0.01, 0.02, 0.03, 0.05, 0.07, 0.10, 0.12, 0.15, 0.20)
DEFAULT_SEARCH_ENTRY_SET_SIZES = (1, 2, 3)
DEFAULT_SEARCH_PROGRESS_CANDIDATES = (0.05, 0.10, 0.20, 0.35, 0.50, 0.75, 0.90)
DEFAULT_SEARCH_TP_LEG_COUNTS = (0, 1, 2)
DEFAULT_SEARCH_MOONBAGS = (0.40, 0.50, 0.60, 0.70, 0.80, 1.00)


@dataclass(frozen=True)
class SearchResult:
    entry_plan: EntryPlanEvaluation
    tp_levels: tuple[TPLevel, ...]
    moonbag: float
    sort_value: float


def parse_entry_levels(raw: str) -> tuple[float, ...]:
    levels = tuple(sorted({float(x.strip()) for x in raw.split(",") if x.strip()}))
    if not levels:
        raise ValueError("No entry levels provided")
    for level in levels:
        if level <= 0.0 or level >= 1.0:
            raise ValueError(f"Entry levels must be in (0, 1), got {level}")
    return levels


def parse_float_list(raw: str) -> tuple[float, ...]:
    values = tuple(float(x.strip()) for x in raw.split(",") if x.strip())
    if not values:
        raise ValueError("No float values provided")
    return values


def parse_int_list(raw: str) -> tuple[int, ...]:
    values = tuple(int(x.strip()) for x in raw.split(",") if x.strip())
    if not values:
        raise ValueError("No integer values provided")
    return values


def parse_tp_levels(raw: str) -> tuple[TPLevel, ...]:
    if not raw.strip():
        return tuple()
    levels: list[TPLevel] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        progress_str, fraction_str = chunk.split(":", 1)
        levels.append(TPLevel(progress=float(progress_str), fraction=float(fraction_str)))
    return tuple(sorted(levels, key=lambda level: level.progress))


def positive_compositions(total_units: int, parts: int) -> Iterable[tuple[int, ...]]:
    if parts == 0:
        if total_units == 0:
            yield ()
        return
    if parts == 1:
        if total_units > 0:
            yield (total_units,)
        return
    for first in range(1, total_units - parts + 2):
        for tail in positive_compositions(total_units - first, parts - 1):
            yield (first,) + tail


def generate_fraction_vectors(total_fraction: float, leg_count: int, step: float) -> Iterable[tuple[float, ...]]:
    if leg_count == 0:
        if abs(total_fraction) <= EPS:
            yield ()
        return
    total_units = round(total_fraction / step)
    if abs(total_units * step - total_fraction) > 1e-9:
        return
    if total_units < leg_count:
        return
    for composition in positive_compositions(total_units, leg_count):
        yield tuple(unit * step for unit in composition)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline big_swan parameter optimizer")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to polymarket_dataset.db")
    parser.add_argument(
        "--entry-levels",
        default=",".join(f"{x:g}" for x in BIG_SWAN_MODE.entry_price_levels),
        help="Comma-separated entry levels, e.g. 0.01,0.10,0.15",
    )
    parser.add_argument(
        "--tp-levels",
        default=",".join(f"{level.progress:.2f}:{level.fraction:.2f}" for level in BIG_SWAN_MODE.tp_levels),
        help="Comma-separated progress:fraction ladder, e.g. 0.10:0.10,0.50:0.20",
    )
    parser.add_argument(
        "--moonbag",
        type=float,
        default=BIG_SWAN_MODE.moonbag_fraction,
        help="Moonbag fraction held to resolution",
    )
    parser.add_argument("--from-date", default=None, help="Filter swans_v2.date >= YYYY-MM-DD")
    parser.add_argument("--to-date", default=None, help="Filter swans_v2.date <= YYYY-MM-DD")
    parser.add_argument(
        "--category",
        action="append",
        default=[],
        help="Optional category filter. Repeatable.",
    )
    parser.add_argument(
        "--group-by",
        choices=["none", "month", "category"],
        default="none",
        help="Optional breakdown under each entry level",
    )
    parser.add_argument(
        "--top-groups",
        type=int,
        default=8,
        help="How many groups to print when --group-by is enabled",
    )
    parser.add_argument("--search", action="store_true", help="Run grid search over candidate entry sets / ladders")
    parser.add_argument(
        "--candidate-entry-levels",
        default=",".join(f"{x:g}" for x in DEFAULT_SEARCH_ENTRY_CANDIDATES),
        help="Candidate entry levels for search mode",
    )
    parser.add_argument(
        "--entry-set-sizes",
        default=",".join(str(x) for x in DEFAULT_SEARCH_ENTRY_SET_SIZES),
        help="Comma-separated entry-set sizes for search mode, e.g. 1,2,3",
    )
    parser.add_argument(
        "--candidate-progress-levels",
        default=",".join(f"{x:g}" for x in DEFAULT_SEARCH_PROGRESS_CANDIDATES),
        help="Candidate TP progress levels for search mode",
    )
    parser.add_argument(
        "--tp-leg-counts",
        default=",".join(str(x) for x in DEFAULT_SEARCH_TP_LEG_COUNTS),
        help="Comma-separated TP leg counts for search mode, e.g. 0,1,2",
    )
    parser.add_argument(
        "--moonbag-options",
        default=",".join(f"{x:g}" for x in DEFAULT_SEARCH_MOONBAGS),
        help="Comma-separated moonbag candidates for search mode",
    )
    parser.add_argument(
        "--fraction-step",
        type=float,
        default=0.10,
        help="Fraction grid step for search mode, e.g. 0.10 or 0.05",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Top search results to print",
    )
    parser.add_argument(
        "--sort-by",
        choices=["total_ev", "avg_ev_per_filled_share", "roi_on_stake", "total_fill_count"],
        default="total_ev",
        help="Search ranking metric",
    )
    return parser


def load_paths(
    db_path: Path,
    from_date: str | None,
    to_date: str | None,
    categories: list[str],
) -> list[SwanPath]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    table_names = {
        row[0]
        for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    required = {"swans_v2", "markets"}
    missing = sorted(required - table_names)
    if missing:
        conn.close()
        raise SystemExit(
            f"Database {db_path} is missing required tables: {missing}. "
            "This optimizer expects a populated polymarket_dataset.db."
        )

    sql = """
        SELECT
            s.market_id,
            s.token_id,
            s.date,
            COALESCE(m.category, 'unknown') AS category,
            s.buy_min_price,
            COALESCE(s.max_price_in_history, 0.0) AS max_traded_price,
            COALESCE(s.is_winner, 0) AS is_winner,
            COALESCE(s.buy_volume, 0.0) AS buy_volume,
            COALESCE(s.sell_volume, 0.0) AS sell_volume,
            CASE
                WHEN m.end_date IS NOT NULL AND s.buy_ts_last IS NOT NULL
                THEN (m.end_date - s.buy_ts_last) / 3600.0
                ELSE NULL
            END AS hours_to_close_from_floor
        FROM swans_v2 s
        JOIN markets m ON m.id = s.market_id
        WHERE s.buy_min_price IS NOT NULL
          AND s.buy_min_price > 0
    """
    params: list[object] = []

    if from_date is not None:
        sql += " AND s.date >= ?"
        params.append(from_date)
    if to_date is not None:
        sql += " AND s.date <= ?"
        params.append(to_date)
    if categories:
        sql += " AND COALESCE(m.category, 'unknown') IN ({})".format(
            ",".join("?" for _ in categories)
        )
        params.extend(categories)

    sql += " ORDER BY s.date, s.market_id, s.token_id"
    rows = cur.execute(sql, tuple(params)).fetchall()
    conn.close()

    return [
        SwanPath(
            market_id=str(row["market_id"]),
            token_id=str(row["token_id"]),
            event_date=str(row["date"]),
            category=str(row["category"]),
            buy_min_price=float(row["buy_min_price"]),
            max_traded_price=float(row["max_traded_price"]),
            is_winner=bool(row["is_winner"]),
            buy_volume=float(row["buy_volume"]),
            sell_volume=float(row["sell_volume"]),
            hours_to_close_from_floor=(
                float(row["hours_to_close_from_floor"])
                if row["hours_to_close_from_floor"] is not None
                else None
            ),
        )
        for row in rows
    ]


def group_key(path: SwanPath, mode: str) -> str:
    if mode == "month":
        return path.event_date[:7]
    if mode == "category":
        return path.category
    return "all"


def iter_grouped(paths: Iterable[SwanPath], mode: str) -> list[tuple[str, list[SwanPath]]]:
    buckets: dict[str, list[SwanPath]] = defaultdict(list)
    for path in paths:
        buckets[group_key(path, mode)].append(path)
    return sorted(buckets.items(), key=lambda item: (-len(item[1]), item[0]))


def fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def fmt_ratio(value: float) -> str:
    return f"{value:.4f}"


def format_entry_levels(levels: Sequence[float]) -> str:
    return "[" + ", ".join(f"{level:.3f}" for level in levels) + "]"


def format_tp_levels(levels: Sequence[TPLevel]) -> str:
    if not levels:
        return "[]"
    return "[" + ", ".join(f"{level.progress:.2f}:{level.fraction:.2f}" for level in levels) + "]"


def print_header(args: argparse.Namespace, paths: list[SwanPath], tp_levels: tuple[TPLevel, ...]) -> None:
    print("big_swan offline path optimizer")
    print(f"db={Path(args.db).expanduser().resolve()}")
    print(f"paths={len(paths)}")
    print(f"entry_levels={[float(f'{x:.6f}') for x in parse_entry_levels(args.entry_levels)]}")
    print(
        "tp_levels="
        + str([
            {"progress": round(level.progress, 4), "fraction": round(level.fraction, 4)}
            for level in tp_levels
        ])
    )
    print(f"moonbag={args.moonbag:.4f}")
    print(f"date_filter=({args.from_date}, {args.to_date})")
    if args.category:
        print(f"category_filter={args.category}")
    print("note=post-hoc path analysis only; use honest replay for realism check")
    print()


def print_evaluation_block(
    label: str,
    entry_price: float,
    paths: list[SwanPath],
    tp_levels: tuple[TPLevel, ...],
    moonbag: float,
) -> None:
    summary = evaluate_ladder(paths=paths, entry_price=entry_price, tp_levels=tp_levels, moonbag_fraction=moonbag)
    print(f"entry={entry_price:.4f} [{label}]")
    print(f"  fills={summary.path_count}")
    print(f"  win_rate={fmt_pct(summary.win_rate)}")
    print(f"  avg_peak_price={fmt_ratio(summary.avg_peak_price)}")
    print(f"  avg_buy_volume={summary.avg_buy_volume:.2f}")
    print(f"  avg_sell_volume={summary.avg_sell_volume:.2f}")
    if summary.avg_hours_to_close_from_floor is not None:
        print(f"  avg_hours_to_close_from_floor={summary.avg_hours_to_close_from_floor:.2f}")
    print(f"  resolution_payout/share={summary.avg_resolution_payout_per_share:.4f}")
    print(f"  tp_payout/share={summary.avg_tp_payout_per_share:.4f}")
    print(f"  total_payout/share={summary.avg_total_payout_per_share:.4f}")
    print(f"  ev/share={summary.ev_per_share:.4f}")
    print(f"  roi_on_stake={fmt_pct(summary.roi_on_stake)}")
    if summary.leg_results:
        print("  leg_hit_rates:")
        for leg in summary.leg_results:
            print(
                "    "
                f"progress={leg.progress:.2f} fraction={leg.fraction:.2f} "
                f"target={leg.target_price:.4f} hit_rate={fmt_pct(leg.hit_rate)}"
            )
    print()


def generate_search_ladders(
    progress_candidates: Sequence[float],
    tp_leg_counts: Sequence[int],
    moonbag_options: Sequence[float],
    fraction_step: float,
) -> list[tuple[tuple[TPLevel, ...], float]]:
    ladders: list[tuple[tuple[TPLevel, ...], float]] = []
    for moonbag in moonbag_options:
        if moonbag < 0.0 or moonbag > 1.0:
            continue
        for leg_count in tp_leg_counts:
            if leg_count < 0:
                continue
            if leg_count == 0:
                if abs(moonbag - 1.0) <= EPS:
                    ladders.append((tuple(), moonbag))
                continue

            remaining_fraction = 1.0 - moonbag
            if remaining_fraction <= 0.0:
                continue

            for progress_combo in itertools.combinations(sorted(set(progress_candidates)), leg_count):
                for fractions in generate_fraction_vectors(remaining_fraction, leg_count, fraction_step):
                    tp_levels = tuple(
                        TPLevel(progress=progress, fraction=fraction)
                        for progress, fraction in zip(progress_combo, fractions)
                    )
                    validate_ladder(tp_levels, moonbag)
                    ladders.append((tp_levels, moonbag))
    return ladders


def plan_sort_value(plan: EntryPlanEvaluation, sort_by: str) -> float:
    if sort_by == "total_ev":
        return plan.total_ev
    if sort_by == "avg_ev_per_filled_share":
        return plan.avg_ev_per_filled_share
    if sort_by == "roi_on_stake":
        return plan.roi_on_stake
    if sort_by == "total_fill_count":
        return float(plan.total_fill_count)
    raise ValueError(f"Unsupported sort_by: {sort_by}")


def run_search(args: argparse.Namespace, paths: list[SwanPath]) -> None:
    entry_candidates = parse_entry_levels(args.candidate_entry_levels)
    entry_set_sizes = tuple(sorted(set(parse_int_list(args.entry_set_sizes))))
    progress_candidates = parse_float_list(args.candidate_progress_levels)
    tp_leg_counts = tuple(sorted(set(parse_int_list(args.tp_leg_counts))))
    moonbag_options = tuple(sorted(set(parse_float_list(args.moonbag_options))))
    fraction_step = float(args.fraction_step)
    if fraction_step <= 0.0 or fraction_step >= 1.0:
        raise ValueError("fraction_step must be in (0, 1)")

    ladders = generate_search_ladders(
        progress_candidates=progress_candidates,
        tp_leg_counts=tp_leg_counts,
        moonbag_options=moonbag_options,
        fraction_step=fraction_step,
    )
    if not ladders:
        raise SystemExit("Search space is empty: no valid ladder candidates generated")

    entry_sets = [
        entry_set
        for size in entry_set_sizes
        for entry_set in itertools.combinations(entry_candidates, size)
    ]
    if not entry_sets:
        raise SystemExit("Search space is empty: no entry sets generated")

    print("search_mode=on")
    print(f"candidate_entry_levels={format_entry_levels(entry_candidates)}")
    print(f"entry_set_sizes={list(entry_set_sizes)}")
    print(f"candidate_progress_levels={list(progress_candidates)}")
    print(f"tp_leg_counts={list(tp_leg_counts)}")
    print(f"moonbag_options={list(moonbag_options)}")
    print(f"fraction_step={fraction_step:.4f}")
    print(f"generated_entry_sets={len(entry_sets)}")
    print(f"generated_ladders={len(ladders)}")
    print(f"sort_by={args.sort_by}")
    print()

    per_entry_cache: dict[tuple[float, int], object] = {}
    for ladder_idx, (tp_levels, moonbag) in enumerate(ladders):
        for entry in entry_candidates:
            per_entry_cache[(entry, ladder_idx)] = evaluate_ladder(
                paths=paths,
                entry_price=entry,
                tp_levels=tp_levels,
                moonbag_fraction=moonbag,
            )

    results: list[SearchResult] = []
    for ladder_idx, (tp_levels, moonbag) in enumerate(ladders):
        for entry_set in entry_sets:
            plan = combine_entry_evaluations(
                tuple(per_entry_cache[(entry, ladder_idx)] for entry in entry_set)
            )
            results.append(
                SearchResult(
                    entry_plan=plan,
                    tp_levels=tp_levels,
                    moonbag=moonbag,
                    sort_value=plan_sort_value(plan, args.sort_by),
                )
            )

    results.sort(
        key=lambda result: (
            result.sort_value,
            result.entry_plan.total_ev,
            result.entry_plan.roi_on_stake,
            result.entry_plan.total_fill_count,
        ),
        reverse=True,
    )

    print(f"evaluated_parameter_sets={len(results)}")
    print(f"top_{args.top}:")
    for idx, result in enumerate(results[: args.top], start=1):
        plan = result.entry_plan
        print(
            f"{idx:>2}. sort={result.sort_value:.6f} "
            f"entries={format_entry_levels(plan.entry_levels)} "
            f"tp={format_tp_levels(result.tp_levels)} "
            f"moonbag={result.moonbag:.2f}"
        )
        print(
            f"    fills={plan.total_fill_count} "
            f"win_rate={fmt_pct(plan.weighted_win_rate)} "
            f"total_cost={plan.total_cost:.4f} "
            f"total_payout={plan.total_payout:.4f} "
            f"total_ev={plan.total_ev:.4f}"
        )
        print(
            f"    avg_cost/fill={plan.avg_cost_per_filled_share:.4f} "
            f"avg_payout/fill={plan.avg_total_payout_per_filled_share:.4f} "
            f"avg_ev/fill={plan.avg_ev_per_filled_share:.4f} "
            f"roi={fmt_pct(plan.roi_on_stake)} "
            f"resolution_share={fmt_pct(plan.resolution_share_of_payout)}"
        )
        per_entry_bits = ", ".join(
            f"{summary.entry_price:.3f}:fills={summary.path_count}:ev/share={summary.ev_per_share:.4f}"
            for summary in plan.per_entry
        )
        print(f"    per_entry: {per_entry_bits}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    entry_levels = parse_entry_levels(args.entry_levels)
    tp_levels = parse_tp_levels(args.tp_levels)
    validate_ladder(tp_levels, args.moonbag)

    paths = load_paths(
        db_path=db_path,
        from_date=args.from_date,
        to_date=args.to_date,
        categories=args.category,
    )
    if not paths:
        raise SystemExit("No swan paths found for the given filters")

    print_header(args, paths, tp_levels)

    if args.search:
        run_search(args, paths)
        return

    plan = evaluate_entry_plan(
        paths=paths,
        entry_levels=entry_levels,
        tp_levels=tp_levels,
        moonbag_fraction=args.moonbag,
    )
    print("plan_summary")
    print(f"  entries={format_entry_levels(plan.entry_levels)}")
    print(f"  fills={plan.total_fill_count}")
    print(f"  weighted_win_rate={fmt_pct(plan.weighted_win_rate)}")
    print(f"  total_cost={plan.total_cost:.4f}")
    print(f"  total_tp_payout={plan.total_tp_payout:.4f}")
    print(f"  total_resolution_payout={plan.total_resolution_payout:.4f}")
    print(f"  total_payout={plan.total_payout:.4f}")
    print(f"  total_ev={plan.total_ev:.4f}")
    print(f"  avg_cost/fill={plan.avg_cost_per_filled_share:.4f}")
    print(f"  avg_payout/fill={plan.avg_total_payout_per_filled_share:.4f}")
    print(f"  avg_ev/fill={plan.avg_ev_per_filled_share:.4f}")
    print(f"  roi_on_stake={fmt_pct(plan.roi_on_stake)}")
    print(f"  resolution_share_of_payout={fmt_pct(plan.resolution_share_of_payout)}")
    print()

    for entry_price in entry_levels:
        print_evaluation_block("global", entry_price, paths, tp_levels, args.moonbag)

        if args.group_by != "none":
            grouped = iter_grouped(paths, args.group_by)
            print(f"  top_{args.top_groups}_{args.group_by}_groups for entry={entry_price:.4f}")
            print()
            for label, group_paths_list in grouped[: args.top_groups]:
                print_evaluation_block(label, entry_price, group_paths_list, tp_levels, args.moonbag)


if __name__ == "__main__":
    main()
