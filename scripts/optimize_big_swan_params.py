#!/usr/bin/env python3
"""
Offline path-level optimizer for big_swan strategy parameters.

This script intentionally uses post-hoc historical swan paths from `swans_v2`.
It is NOT a causal or capital-realistic simulator.

Use it for:
- cheap exploration of buy levels;
- comparing binary-native TP ladders (`progress/fraction`);
- estimating resolution-vs-TP tradeoffs;
- generating candidate presets for later honest-replay validation.

Do NOT interpret the output as final live expectancy.
Final validation should go through `scripts/run_honest_replay.py`.

Examples:
    python3 scripts/optimize_big_swan_params.py
    python3 scripts/optimize_big_swan_params.py --entry-levels 0.01,0.05,0.10
    python3 scripts/optimize_big_swan_params.py --tp-levels 0.10:0.10,0.50:0.20 --moonbag 0.70
    python3 scripts/optimize_big_swan_params.py --from-date 2026-01-01 --to-date 2026-01-31 --group-by month
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import BIG_SWAN_MODE, TPLevel
from strategy.big_swan_optimizer import SwanPath, evaluate_ladder, validate_ladder
from utils.paths import DB_PATH


def parse_entry_levels(raw: str) -> tuple[float, ...]:
    levels = tuple(sorted({float(x.strip()) for x in raw.split(",") if x.strip()}))
    if not levels:
        raise ValueError("No entry levels provided")
    for level in levels:
        if level <= 0.0 or level >= 1.0:
            raise ValueError(f"Entry levels must be in (0, 1), got {level}")
    return levels


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
    return f"{value:.3f}"


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


def print_evaluation_block(label: str, entry_price: float, paths: list[SwanPath], tp_levels: tuple[TPLevel, ...], moonbag: float) -> None:
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
