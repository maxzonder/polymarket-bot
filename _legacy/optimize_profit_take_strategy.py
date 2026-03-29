#!/usr/bin/env python3
"""
Optimize exit schemes for one trading day using token_swans.

Variants supported:
1. full_exit_single:
   - sell 100% at one target from the candidate list.
2. ascending_ladder:
   - exactly N strictly increasing steps.
   - weights are optimized and must sum to 100%.
3. fixed_tail_500:
   - 90% is sold at one optimized single target.
   - final 10% is always sold at 500x.

Execution model:
- A swan is buyable if entry_volume_usdc >= stake.
- Data source: token_swans (real swans only).
- A tranche at target X is fillable only if:
  1) possible_x >= X
  2) exit_volume_usdc is enough to cover cumulative gross proceeds sold up to that step.
- Steps are processed in order; if one step fails, later steps do not fill.

This is a liquidity-aware approximation, not a full order-book simulator.
"""

from __future__ import annotations

import argparse
import itertools
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.paths import DB_PATH

DEFAULT_TARGETS = (5, 10, 20, 30, 40, 50, 100, 200, 500, 1000)
DEFAULT_STEPS = 5
DEFAULT_WEIGHT_INCREMENT = 10
DEFAULT_TOP_N = 10
DEFAULT_MODES = ("full_exit_single", "ascending_ladder", "fixed_tail_500")


@dataclass(frozen=True)
class Swan:
    market_id: str
    token_id: str
    possible_x: float
    entry_volume_usdc: float
    exit_volume_usdc: float


@dataclass(frozen=True)
class Step:
    target_x: int
    weight: float


@dataclass
class SchemeResult:
    mode: str
    steps: Tuple[Step, ...]
    buyable_swans: int
    total_cost_usd: float
    total_revenue_usd: float
    total_profit_usd: float
    roi_pct: float
    per_step_fill_counts: Tuple[int, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Optimize exit schemes for one day")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to SQLite DB")
    parser.add_argument("--date", required=True, help="Trading day, e.g. 2026-02-28")
    parser.add_argument("--stake", type=float, default=1.0, help="Stake per swan in USDC")
    parser.add_argument(
        "--targets",
        default=",".join(str(x) for x in DEFAULT_TARGETS),
        help="Comma-separated candidate take-profit multipliers",
    )
    parser.add_argument("--steps", type=int, default=DEFAULT_STEPS, help="Number of ladder steps")
    parser.add_argument(
        "--weight-increment",
        type=int,
        default=DEFAULT_WEIGHT_INCREMENT,
        help="Weight increment in percent. Default 10 means weights like 10%%, 20%%, ...",
    )
    parser.add_argument("--top", type=int, default=DEFAULT_TOP_N, help="How many best schemes to print per mode")
    parser.add_argument(
        "--modes",
        default=",".join(DEFAULT_MODES),
        help="Comma-separated modes: full_exit_single,ascending_ladder,fixed_tail_500",
    )
    return parser.parse_args()


def parse_targets(raw: str) -> Tuple[int, ...]:
    targets = tuple(sorted({int(x.strip()) for x in raw.split(",") if x.strip()}))
    if not targets:
        raise ValueError("No targets provided")
    return targets


def parse_modes(raw: str) -> Tuple[str, ...]:
    modes = tuple(x.strip() for x in raw.split(",") if x.strip())
    allowed = set(DEFAULT_MODES)
    bad = [m for m in modes if m not in allowed]
    if bad:
        raise ValueError(f"Unknown mode(s): {bad}")
    return modes


def load_swans(db_path: Path, date: str, stake: float) -> List[Swan]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT market_id, token_id, possible_x, entry_volume_usdc, exit_volume_usdc
        FROM token_swans
        WHERE date = ? AND entry_volume_usdc >= ?
        """,
        (date, stake),
    ).fetchall()
    conn.close()
    return [
        Swan(
            market_id=row["market_id"],
            token_id=row["token_id"],
            possible_x=float(row["possible_x"]),
            entry_volume_usdc=float(row["entry_volume_usdc"]),
            exit_volume_usdc=float(row["exit_volume_usdc"]),
        )
        for row in rows
    ]


def generate_weight_vectors(steps: int, increment_pct: int, total_weight: float = 1.0) -> Iterable[Tuple[float, ...]]:
    if 100 % increment_pct != 0:
        raise ValueError("weight_increment must divide 100 exactly")
    units_total = round(total_weight * 100 / increment_pct)
    if units_total < steps:
        raise ValueError("Not enough weight units to allocate at least one unit per step")
    for cuts in itertools.combinations(range(1, units_total), steps - 1):
        prev = 0
        parts = []
        for cut in cuts:
            parts.append(cut - prev)
            prev = cut
        parts.append(units_total - prev)
        yield tuple(part * increment_pct / 100.0 for part in parts)


def evaluate_scheme(swans: Sequence[Swan], stake: float, steps: Sequence[Step], mode: str) -> SchemeResult:
    fill_counts = [0] * len(steps)
    total_revenue = 0.0

    for swan in swans:
        cumulative_required = 0.0
        for idx, step in enumerate(steps):
            tranche_gross = stake * step.weight * step.target_x
            required_after_fill = cumulative_required + tranche_gross
            if swan.possible_x >= step.target_x and swan.exit_volume_usdc >= required_after_fill:
                total_revenue += tranche_gross
                cumulative_required = required_after_fill
                fill_counts[idx] += 1
            else:
                break

    total_cost = len(swans) * stake
    total_profit = total_revenue - total_cost
    roi_pct = (total_profit / total_cost * 100.0) if total_cost else 0.0
    return SchemeResult(
        mode=mode,
        steps=tuple(steps),
        buyable_swans=len(swans),
        total_cost_usd=total_cost,
        total_revenue_usd=total_revenue,
        total_profit_usd=total_profit,
        roi_pct=roi_pct,
        per_step_fill_counts=tuple(fill_counts),
    )


def optimize_full_exit_single(swans: Sequence[Swan], stake: float, targets: Sequence[int]) -> List[SchemeResult]:
    results = []
    for target in targets:
        steps = (Step(target_x=target, weight=1.0),)
        results.append(evaluate_scheme(swans, stake, steps, mode="full_exit_single"))
    results.sort(key=lambda r: (r.total_profit_usd, r.total_revenue_usd, r.roi_pct), reverse=True)
    return results


def optimize_ascending_ladder(
    swans: Sequence[Swan],
    stake: float,
    targets: Sequence[int],
    steps_count: int,
    weight_increment: int,
) -> List[SchemeResult]:
    results = []
    weight_vectors = list(generate_weight_vectors(steps_count, weight_increment, total_weight=1.0))
    for target_vector in itertools.combinations(targets, steps_count):
        for weight_vector in weight_vectors:
            steps = tuple(Step(target_x=t, weight=w) for t, w in zip(target_vector, weight_vector))
            results.append(evaluate_scheme(swans, stake, steps, mode="ascending_ladder"))
    results.sort(key=lambda r: (r.total_profit_usd, r.total_revenue_usd, r.roi_pct), reverse=True)
    return results


def optimize_fixed_tail_500(
    swans: Sequence[Swan],
    stake: float,
    targets: Sequence[int],
    steps_count: int,
    weight_increment: int,
) -> List[SchemeResult]:
    if 500 not in targets:
        raise ValueError("fixed_tail_500 mode requires 500 in --targets")
    front_targets = tuple(t for t in targets if t != 500)
    results = []
    for target in front_targets:
        steps = (
            Step(target_x=target, weight=0.9),
            Step(target_x=500, weight=0.1),
        )
        results.append(evaluate_scheme(swans, stake, steps, mode="fixed_tail_500"))
    results.sort(key=lambda r: (r.total_profit_usd, r.total_revenue_usd, r.roi_pct), reverse=True)
    return results


def format_steps(steps: Sequence[Step]) -> str:
    return " | ".join(f"{int(round(step.weight * 100))}% @ {step.target_x}x" for step in steps)


def print_mode_results(mode: str, results: Sequence[SchemeResult], top: int) -> None:
    print(f"MODE={mode}")
    if not results:
        print("  no valid schemes for the given constraints")
        print()
        return
    best = results[0]
    print(f"  best_steps: {format_steps(best.steps)}")
    print(f"  cost_usd: {best.total_cost_usd:.2f}")
    print(f"  revenue_usd: {best.total_revenue_usd:.2f}")
    print(f"  profit_usd: {best.total_profit_usd:.2f}")
    print(f"  roi_pct: {best.roi_pct:.2f}")
    print(f"  fills_by_step: {list(best.per_step_fill_counts)}")
    print(f"  top_{top}:")
    for idx, result in enumerate(results[:top], start=1):
        print(
            f"    {idx}. {format_steps(result.steps)} | "
            f"profit=${result.total_profit_usd:.2f} | "
            f"revenue=${result.total_revenue_usd:.2f} | "
            f"roi={result.roi_pct:.2f}% | "
            f"fills={list(result.per_step_fill_counts)}"
        )
    print()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    targets = parse_targets(args.targets)
    modes = parse_modes(args.modes)

    swans = load_swans(db_path, args.date, args.stake)
    if not swans:
        raise SystemExit(f"No buyable swans found for {args.date} with stake={args.stake}")

    print(f"date={args.date}")
    print(f"stake={args.stake}")
    print(f"buyable_swans={len(swans)}")
    print(f"candidate_targets={list(targets)}")
    print(f"steps={args.steps}")
    print(f"weight_increment={args.weight_increment}%")
    print(f"modes={list(modes)}")
    print()

    if "full_exit_single" in modes:
        print_mode_results(
            "full_exit_single",
            optimize_full_exit_single(swans, args.stake, targets),
            args.top,
        )

    if "ascending_ladder" in modes:
        print_mode_results(
            "ascending_ladder",
            optimize_ascending_ladder(swans, args.stake, targets, args.steps, args.weight_increment),
            args.top,
        )

    if "fixed_tail_500" in modes:
        print_mode_results(
            "fixed_tail_500",
            optimize_fixed_tail_500(swans, args.stake, targets, args.steps, args.weight_increment),
            args.top,
        )


if __name__ == "__main__":
    main()
