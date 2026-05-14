from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.venue_polymarket.markets import MarketMetadata
from short_horizon.venue_polymarket.universe_selector import UniverseSelectorConfig, build_subscription_plan


def _market(
    market_id: str,
    *,
    active: bool = True,
    yes: str | None = None,
    no: str | None = None,
    end_time_ms: int | None = 1_000_000,
    duration_seconds: int | None = 3600,
) -> MarketMetadata:
    return MarketMetadata(
        market_id=market_id,
        condition_id=f"cond-{market_id}",
        question=f"Question {market_id}?",
        token_yes_id=yes if yes is not None else f"yes-{market_id}",
        token_no_id=no if no is not None else f"no-{market_id}",
        start_time_ms=0,
        end_time_ms=end_time_ms,  # type: ignore[arg-type]
        asset_slug="bitcoin",
        is_active=active,
        duration_seconds=duration_seconds,
        fees_enabled=False,
        fee_rate_bps=None,
        tick_size=0.01,
    )


class UniverseSelectorTests(unittest.TestCase):
    def test_builds_token_maps_for_selected_markets(self) -> None:
        plan = build_subscription_plan([_market("m1"), _market("m2")])

        self.assertEqual(plan.selected_market_ids, ("m1", "m2"))
        self.assertEqual(plan.selected_token_ids, ("yes-m1", "no-m1", "yes-m2", "no-m2"))
        self.assertEqual(plan.token_to_market_id["yes-m1"], "m1")
        self.assertEqual(plan.token_to_market_id["no-m2"], "m2")
        self.assertEqual(plan.token_to_side_index["yes-m1"], 0)
        self.assertEqual(plan.token_to_side_index["no-m1"], 1)
        self.assertEqual([d.stage for d in plan.decisions], ["selected", "selected"])

    def test_rejects_technically_invalid_markets_with_reasons(self) -> None:
        plan = build_subscription_plan([
            _market("inactive", active=False),
            _market("missing-yes", yes="", no=""),
            _market("missing-end", end_time_ms=None),
        ])

        self.assertEqual(plan.selected_market_ids, ())
        self.assertEqual(plan.rejection_counts["inactive"], 1)
        self.assertEqual(plan.rejection_counts["missing_token_ids"], 1)
        self.assertEqual(plan.rejection_counts["missing_end_time"], 1)
        self.assertEqual([d.reject_reason for d in plan.decisions], [
            "inactive",
            "missing_token_ids",
            "missing_end_time",
        ])

    def test_applies_global_market_and_token_caps_before_subscription(self) -> None:
        plan = build_subscription_plan(
            [_market("m1"), _market("m2"), _market("m3")],
            config=UniverseSelectorConfig(max_markets=2),
        )

        self.assertEqual(plan.selected_market_ids, ("m1", "m2"))
        self.assertEqual(plan.rejection_counts["cap_markets"], 1)
        self.assertEqual(plan.decisions[-1].reject_reason, "cap_markets")

        token_capped = build_subscription_plan(
            [_market("m1"), _market("m2")],
            config=UniverseSelectorConfig(max_tokens=2),
        )

        self.assertEqual(token_capped.selected_market_ids, ("m1",))
        self.assertEqual(token_capped.selected_token_ids, ("yes-m1", "no-m1"))
        self.assertEqual(token_capped.rejection_counts["cap_tokens"], 1)

    def test_can_select_only_one_side_for_subscription(self) -> None:
        plan = build_subscription_plan(
            [_market("m1")],
            config=UniverseSelectorConfig(include_yes_token=False, include_no_token=True),
        )

        self.assertEqual(plan.selected_token_ids, ("no-m1",))
        self.assertNotIn("yes-m1", plan.token_to_market_id)
        self.assertEqual(plan.token_to_side_index["no-m1"], 1)


if __name__ == "__main__":
    unittest.main()
