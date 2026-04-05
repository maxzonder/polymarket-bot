from __future__ import annotations

import unittest

from config import TPLevel
from strategy.big_swan_optimizer import SwanPath, evaluate_ladder, evaluate_path_ladder, progress_to_target_price


def make_path(**overrides) -> SwanPath:
    base = dict(
        market_id="m1",
        token_id="t1",
        event_date="2026-01-01",
        category="politics",
        buy_min_price=0.01,
        max_traded_price=0.60,
        is_winner=True,
        buy_volume=10.0,
        sell_volume=100.0,
        hours_to_close_from_floor=24.0,
    )
    base.update(overrides)
    return SwanPath(**base)


class BigSwanOptimizerTests(unittest.TestCase):
    def test_progress_to_target_price_is_binary_native(self) -> None:
        self.assertEqual(progress_to_target_price(0.10, 0.0), 0.10)
        self.assertEqual(progress_to_target_price(0.10, 1.0), 1.0)
        self.assertEqual(round(progress_to_target_price(0.10, 0.50), 6), 0.55)

    def test_unhit_tp_fraction_flows_to_resolution_for_winner(self) -> None:
        path = make_path(max_traded_price=0.15, is_winner=True)
        tp_levels = (
            TPLevel(progress=0.10, fraction=0.10),
            TPLevel(progress=0.50, fraction=0.20),
        )

        tp_payout, resolution_payout, hit_flags = evaluate_path_ladder(
            path=path,
            entry_price=0.01,
            tp_levels=tp_levels,
            moonbag_fraction=0.70,
        )

        self.assertEqual(hit_flags, (True, False))
        self.assertEqual(round(tp_payout, 4), 0.0109)
        self.assertEqual(round(resolution_payout, 4), 0.9000)

    def test_loser_with_no_tp_hits_goes_to_zero(self) -> None:
        path = make_path(max_traded_price=0.02, is_winner=False)
        tp_levels = (
            TPLevel(progress=0.10, fraction=0.10),
            TPLevel(progress=0.50, fraction=0.20),
        )

        summary = evaluate_ladder(
            paths=[path],
            entry_price=0.01,
            tp_levels=tp_levels,
            moonbag_fraction=0.70,
        )

        self.assertEqual(summary.path_count, 1)
        self.assertEqual(summary.avg_tp_payout_per_share, 0.0)
        self.assertEqual(summary.avg_resolution_payout_per_share, 0.0)
        self.assertEqual(summary.avg_total_payout_per_share, 0.0)
        self.assertEqual(summary.ev_per_share, -0.01)
        self.assertEqual(summary.roi_on_stake, -1.0)

    def test_tp_hit_rate_is_aggregated_per_leg(self) -> None:
        tp_levels = (
            TPLevel(progress=0.10, fraction=0.10),
            TPLevel(progress=0.50, fraction=0.20),
        )
        paths = [
            make_path(max_traded_price=0.12, is_winner=True),
            make_path(max_traded_price=0.70, is_winner=False, token_id="t2"),
        ]

        summary = evaluate_ladder(
            paths=paths,
            entry_price=0.01,
            tp_levels=tp_levels,
            moonbag_fraction=0.70,
        )

        self.assertEqual(summary.path_count, 2)
        self.assertEqual(round(summary.leg_results[0].hit_rate, 6), 1.0)
        self.assertEqual(round(summary.leg_results[1].hit_rate, 6), 0.5)


if __name__ == "__main__":
    unittest.main()
