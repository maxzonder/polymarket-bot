from __future__ import annotations

from typing import Iterable

EPS = 1e-9


def suggested_entry_levels(
    current_price: float,
    configured_levels: Iterable[float],
    scanner_entry: bool,
) -> list[float]:
    """
    Return candidate entry buckets using "max acceptable price" semantics.

    Important meaning:
    - each configured level is the MAX price we are willing to pay for one tier;
    - if the market is already cheaper than a configured level, that tier remains valid;
    - lower-than-market tiers naturally become resting bids; higher/equal tiers become
      immediately marketable limit buys.

    This intentionally differs from the old "strictly below current price" logic,
    which incorrectly dropped already-cheap markets as `rejected_no_entry_levels`.
    """
    levels = sorted({float(level) for level in configured_levels if float(level) > 0.0})
    if levels:
        return levels

    if scanner_entry and current_price > EPS:
        return [float(current_price)]

    return []


def partition_entry_levels(current_price: float, entry_levels: Iterable[float]) -> tuple[list[float], list[float]]:
    """Split configured levels into marketable-now vs resting-below-current buckets."""
    marketable: list[float] = []
    resting: list[float] = []
    for level in sorted({float(level) for level in entry_levels if float(level) > 0.0}):
        if level + EPS >= current_price:
            marketable.append(level)
        else:
            resting.append(level)
    return marketable, resting
