from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from config import TPLevel

EPS = 1e-9


@dataclass(frozen=True)
class SwanPath:
    market_id: str
    token_id: str
    event_date: str
    category: str
    buy_min_price: float
    max_traded_price: float
    is_winner: bool
    buy_volume: float
    sell_volume: float
    hours_to_close_from_floor: float | None


@dataclass(frozen=True)
class LegResult:
    progress: float
    fraction: float
    target_price: float
    hit_rate: float


@dataclass(frozen=True)
class LadderEvaluation:
    entry_price: float
    path_count: int
    win_rate: float
    avg_peak_price: float
    avg_resolution_payout_per_share: float
    avg_tp_payout_per_share: float
    avg_total_payout_per_share: float
    ev_per_share: float
    roi_on_stake: float
    avg_buy_volume: float
    avg_sell_volume: float
    avg_hours_to_close_from_floor: float | None
    leg_results: tuple[LegResult, ...]


def progress_to_target_price(entry_price: float, progress: float) -> float:
    if entry_price <= 0.0 or entry_price >= 1.0:
        raise ValueError(f"entry_price must be in (0, 1), got {entry_price}")
    if progress < 0.0 or progress > 1.0:
        raise ValueError(f"progress must be in [0, 1], got {progress}")
    return entry_price + progress * (1.0 - entry_price)


def validate_ladder(tp_levels: Sequence[TPLevel], moonbag_fraction: float) -> None:
    if moonbag_fraction < 0.0 or moonbag_fraction > 1.0:
        raise ValueError(f"moonbag_fraction must be in [0, 1], got {moonbag_fraction}")

    total_tp = 0.0
    prev_progress = -1.0
    for level in tp_levels:
        if level.progress < 0.0 or level.progress > 1.0:
            raise ValueError(f"TP progress must be in [0, 1], got {level.progress}")
        if level.fraction < 0.0 or level.fraction > 1.0:
            raise ValueError(f"TP fraction must be in [0, 1], got {level.fraction}")
        if level.progress + EPS < prev_progress:
            raise ValueError("TP levels must be sorted by progress ascending")
        prev_progress = level.progress
        total_tp += level.fraction

    if total_tp + moonbag_fraction > 1.0 + EPS:
        raise ValueError(
            f"TP fractions ({total_tp:.4f}) + moonbag ({moonbag_fraction:.4f}) exceed 1.0"
        )


def path_fills_entry(path: SwanPath, entry_price: float) -> bool:
    return path.buy_min_price <= entry_price + EPS


def evaluate_path_ladder(
    path: SwanPath,
    entry_price: float,
    tp_levels: Sequence[TPLevel],
    moonbag_fraction: float,
) -> tuple[float, float, tuple[bool, ...]]:
    """
    Returns:
      tp_payout_per_share,
      resolution_payout_per_share,
      tp_hit_flags

    Semantics:
    - If a TP leg trades before resolution, that fraction exits at target_price.
    - If a TP leg never trades, that fraction remains economically live and resolves
      with the rest of the position.
    - Resolution pays $1.00/share for winners, $0.00/share for losers.

    `moonbag_fraction` is validated for config-compatibility but the payout math is
    driven by what actually gets sold by TP. Any unfilled TP fractions naturally flow
    into the resolution bucket, which mirrors the current execution/accounting model.
    """
    validate_ladder(tp_levels, moonbag_fraction)

    winner_payout = 1.0 if path.is_winner else 0.0
    remaining_fraction = 1.0
    tp_payout = 0.0
    hit_flags: list[bool] = []

    for level in tp_levels:
        target_price = progress_to_target_price(entry_price, level.progress)
        hit = path.max_traded_price >= target_price - EPS
        hit_flags.append(hit)
        if hit:
            tp_payout += level.fraction * target_price
            remaining_fraction -= level.fraction

    resolution_payout = remaining_fraction * winner_payout
    return tp_payout, resolution_payout, tuple(hit_flags)


def evaluate_ladder(
    paths: Iterable[SwanPath],
    entry_price: float,
    tp_levels: Sequence[TPLevel],
    moonbag_fraction: float,
) -> LadderEvaluation:
    validate_ladder(tp_levels, moonbag_fraction)

    filled_paths = [path for path in paths if path_fills_entry(path, entry_price)]
    if not filled_paths:
        return LadderEvaluation(
            entry_price=entry_price,
            path_count=0,
            win_rate=0.0,
            avg_peak_price=0.0,
            avg_resolution_payout_per_share=0.0,
            avg_tp_payout_per_share=0.0,
            avg_total_payout_per_share=0.0,
            ev_per_share=-entry_price,
            roi_on_stake=-1.0,
            avg_buy_volume=0.0,
            avg_sell_volume=0.0,
            avg_hours_to_close_from_floor=None,
            leg_results=tuple(
                LegResult(
                    progress=level.progress,
                    fraction=level.fraction,
                    target_price=progress_to_target_price(entry_price, level.progress),
                    hit_rate=0.0,
                )
                for level in tp_levels
            ),
        )

    tp_payout_total = 0.0
    resolution_payout_total = 0.0
    winners = 0
    peak_total = 0.0
    buy_volume_total = 0.0
    sell_volume_total = 0.0
    hours_values: list[float] = []
    leg_hits = [0] * len(tp_levels)

    for path in filled_paths:
        tp_payout, resolution_payout, hit_flags = evaluate_path_ladder(
            path=path,
            entry_price=entry_price,
            tp_levels=tp_levels,
            moonbag_fraction=moonbag_fraction,
        )
        tp_payout_total += tp_payout
        resolution_payout_total += resolution_payout
        winners += int(path.is_winner)
        peak_total += path.max_traded_price
        buy_volume_total += path.buy_volume
        sell_volume_total += path.sell_volume
        if path.hours_to_close_from_floor is not None:
            hours_values.append(path.hours_to_close_from_floor)
        for idx, hit in enumerate(hit_flags):
            if hit:
                leg_hits[idx] += 1

    path_count = len(filled_paths)
    avg_tp_payout = tp_payout_total / path_count
    avg_resolution_payout = resolution_payout_total / path_count
    avg_total_payout = avg_tp_payout + avg_resolution_payout
    ev_per_share = avg_total_payout - entry_price
    roi_on_stake = (avg_total_payout / entry_price) - 1.0

    leg_results = tuple(
        LegResult(
            progress=level.progress,
            fraction=level.fraction,
            target_price=progress_to_target_price(entry_price, level.progress),
            hit_rate=leg_hits[idx] / path_count,
        )
        for idx, level in enumerate(tp_levels)
    )

    avg_hours = sum(hours_values) / len(hours_values) if hours_values else None

    return LadderEvaluation(
        entry_price=entry_price,
        path_count=path_count,
        win_rate=winners / path_count,
        avg_peak_price=peak_total / path_count,
        avg_resolution_payout_per_share=avg_resolution_payout,
        avg_tp_payout_per_share=avg_tp_payout,
        avg_total_payout_per_share=avg_total_payout,
        ev_per_share=ev_per_share,
        roi_on_stake=roi_on_stake,
        avg_buy_volume=buy_volume_total / path_count,
        avg_sell_volume=sell_volume_total / path_count,
        avg_hours_to_close_from_floor=avg_hours,
        leg_results=leg_results,
    )
