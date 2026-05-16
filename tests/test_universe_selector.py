from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.venue_polymarket.markets import MarketMetadata
from short_horizon.venue_polymarket.universe_selector import (
    UniverseSelectorConfig,
    black_swan_universe_config,
    build_subscription_plan,
    classify_catalyst,
)


def _market(
    market_id: str,
    *,
    active: bool = True,
    yes: str | None = None,
    no: str | None = None,
    end_time_ms: int | None = 1_000_000,
    duration_seconds: int | None = 3600,
    volume_usdc: float = 1_000.0,
    total_duration_seconds: int | None = 3600,
    fees_enabled: bool = False,
    fee_rate_bps: float | None = None,
    category: str | None = "crypto",
    slug: str | None = None,
    question: str | None = None,
) -> MarketMetadata:
    return MarketMetadata(
        market_id=market_id,
        condition_id=f"cond-{market_id}",
        question=question or f"Question {market_id}?",
        token_yes_id=yes if yes is not None else f"yes-{market_id}",
        token_no_id=no if no is not None else f"no-{market_id}",
        start_time_ms=0,
        end_time_ms=end_time_ms,  # type: ignore[arg-type]
        asset_slug="bitcoin",
        is_active=active,
        duration_seconds=duration_seconds,
        tick_size=0.01,
        category=category,
        slug=slug or f"slug-{market_id}",
        volume_usdc=volume_usdc,
        liquidity_usdc=250.0,
        fees_enabled=fees_enabled,
        fee_rate_bps=fee_rate_bps,
        total_duration_seconds=total_duration_seconds,
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

    def test_defers_capacity_overflow_instead_of_rejecting_it(self) -> None:
        plan = build_subscription_plan(
            [_market("m1"), _market("m2"), _market("m3")],
            config=UniverseSelectorConfig(max_markets=2),
        )

        self.assertEqual(plan.selected_market_ids, ("m1", "m2"))
        self.assertEqual(plan.rejection_counts, {})
        self.assertEqual(plan.deferred_counts["capacity"], 1)
        self.assertEqual(plan.decisions[-1].stage, "deferred")
        self.assertEqual(plan.decisions[-1].reject_reason, "capacity")

        token_capped = build_subscription_plan(
            [_market("m1"), _market("m2")],
            config=UniverseSelectorConfig(max_tokens=2),
        )

        self.assertEqual(token_capped.selected_market_ids, ("m1",))
        self.assertEqual(token_capped.selected_token_ids, ("yes-m1", "no-m1"))
        self.assertEqual(token_capped.rejection_counts, {})
        self.assertEqual(token_capped.deferred_counts["capacity"], 1)

    def test_can_select_only_one_side_for_subscription(self) -> None:
        plan = build_subscription_plan(
            [_market("m1")],
            config=UniverseSelectorConfig(include_yes_token=False, include_no_token=True),
        )

        self.assertEqual(plan.selected_token_ids, ("no-m1",))
        self.assertNotIn("yes-m1", plan.token_to_market_id)
        self.assertEqual(plan.token_to_side_index["no-m1"], 1)

    def test_applies_cheap_metadata_gates_without_categoric_policy(self) -> None:
        plan = build_subscription_plan(
            [
                _market("thin", volume_usdc=10.0),
                _market("short", total_duration_seconds=300),
                _market("fees", fees_enabled=True, fee_rate_bps=4.0),
                _market("expensive-fee", fees_enabled=False, fee_rate_bps=40.0),
                _market("ok", volume_usdc=1_000.0, total_duration_seconds=3600, fee_rate_bps=4.0),
            ],
            config=UniverseSelectorConfig(
                min_volume_usdc=50.0,
                min_total_duration_seconds=900,
                reject_fees_enabled=True,
                max_fee_rate_bps=10.0,
            ),
        )

        self.assertEqual(plan.selected_market_ids, ("ok",))
        self.assertEqual(plan.rejection_counts["volume_below_min"], 1)
        self.assertEqual(plan.rejection_counts["total_duration_below_min"], 1)
        self.assertEqual(plan.rejection_counts["fees_enabled"], 1)
        self.assertEqual(plan.rejection_counts["fee_rate_above_max"], 1)

        ok_decision = plan.decisions[-1]
        self.assertEqual(ok_decision.stage, "selected")
        self.assertEqual(ok_decision.category, "crypto")
        self.assertEqual(ok_decision.slug, "slug-ok")
        self.assertEqual(ok_decision.duration_bucket, "1h")
        self.assertEqual(ok_decision.volume_usdc, 1_000.0)

    def test_classifies_cheap_catalyst_and_random_walk_markets(self) -> None:
        catalyst = classify_catalyst(_market(
            "election",
            category="politics",
            question="Will the opposition win the election this weekend?",
        ))
        random_walk = classify_catalyst(_market(
            "btc",
            category="crypto",
            question="Will the price of Bitcoin close above $100,000 today?",
        ))
        ambiguous = classify_catalyst(_market(
            "mention",
            category="entertainment",
            question="Will Bob be mentioned in a tweet this week?",
        ))

        self.assertEqual(catalyst.kind, "catalyst")
        self.assertEqual(catalyst.reason, "election")
        self.assertEqual(random_walk.kind, "random_walk")
        self.assertEqual(random_walk.reason, "price of")
        self.assertEqual(ambiguous.kind, "ambiguous")
        self.assertEqual(ambiguous.reason, "mentioned")

    def test_black_swan_policy_knobs_do_not_hard_filter_before_ws(self) -> None:
        plan = build_subscription_plan(
            [
                _market(
                    "hurricane",
                    category="weather",
                    question="Will a hurricane make landfall in Florida by Friday?",
                ),
                _market(
                    "btc-random",
                    category="crypto",
                    question="Will the price of Bitcoin close above $100,000 today?",
                ),
                _market(
                    "sports",
                    category="sports",
                    question="Will Team A win the match tonight?",
                ),
                _market(
                    "tech",
                    category="tech",
                    question="Will a new phone launch this week?",
                ),
            ],
            config=black_swan_universe_config(
                allowed_categories=("weather", "crypto", "sports"),
                blocked_categories=("sports",),
                reject_random_walk=True,
            ),
        )

        self.assertEqual(plan.selected_market_ids, ("btc-random", "sports", "tech", "hurricane"))
        self.assertEqual(plan.rejection_counts, {})
        self.assertEqual(plan.deferred_counts, {})

        selected = plan.decisions[0]
        self.assertEqual(selected.catalyst_kind, "catalyst")
        self.assertEqual(selected.catalyst_reason, "hurricane")
        self.assertEqual(selected.subscription_score, 1.0)

        random_walk = plan.decisions[1]
        self.assertEqual(random_walk.catalyst_kind, "random_walk")
        self.assertEqual(random_walk.subscription_score, 1.0)

    def test_uses_fair_category_order_before_applying_global_caps(self) -> None:
        plan = build_subscription_plan(
            [
                _market(
                    "crypto-a",
                    category="crypto",
                    question="Will the price of Bitcoin close above $100,000 today?",
                ),
                _market(
                    "crypto-b",
                    category="crypto",
                    question="Will ETH hit a new all-time high this week?",
                ),
                _market(
                    "weather-a",
                    category="weather",
                    question="Will a hurricane make landfall in Florida by Friday?",
                ),
            ],
            config=black_swan_universe_config(max_markets=2),
        )

        self.assertEqual(plan.selected_market_ids, ("crypto-a", "weather-a"))

    def test_can_apply_explicit_subscription_scores_without_fair_category_reordering(self) -> None:
        plan = build_subscription_plan(
            [
                _market("a-low", category="crypto"),
                _market("b-mid", category="weather"),
                _market("c-high", category="sports"),
            ],
            config=black_swan_universe_config(
                max_markets=2,
                subscription_scores={"a-low": 0.1, "b-mid": 0.5, "c-high": 0.9},
                fair_category_allocation=False,
            ),
        )

        self.assertEqual(plan.selected_market_ids, ("c-high", "b-mid"))
        self.assertEqual(plan.decisions[0].stage, "deferred")
        self.assertEqual(plan.decisions[0].subscription_score, 0.1)
        self.assertEqual(plan.decisions[2].stage, "selected")
        self.assertEqual(plan.decisions[2].subscription_score, 0.9)
        self.assertEqual(plan.deferred_counts["capacity"], 1)

    def test_retained_market_hysteresis_hooks_without_category_hard_caps(self) -> None:
        plan = build_subscription_plan(
            [
                _market("weather-a", category="weather", question="Will a hurricane hit Florida?"),
                _market("weather-b", category="weather", question="Will a tornado hit Texas?"),
                _market("legal", category="legal", question="Will the court issue a verdict tomorrow?"),
            ],
            config=black_swan_universe_config(
                max_markets_per_category=1,
                retained_market_ids=("weather-b",),
                retained_score_bonus=0.01,
            ),
        )

        self.assertEqual(plan.selected_market_ids, ("weather-b", "legal", "weather-a"))
        self.assertEqual(plan.rejection_counts, {})
        self.assertEqual(plan.deferred_counts, {})
        self.assertTrue(plan.decisions[1].retained_market)
        self.assertGreater(plan.decisions[1].subscription_score, plan.decisions[0].subscription_score)

    def test_builds_observability_summary_from_decisions(self) -> None:
        plan = build_subscription_plan(
            [
                _market("thin", category="crypto", volume_usdc=5.0),
                _market("weather", category="weather", question="Will a hurricane hit Florida?"),
                _market("legal", category="legal", question="Will the court issue a verdict tomorrow?"),
                _market("random", category="crypto", question="Will the price of Bitcoin close above $100,000 today?"),
            ],
            config=black_swan_universe_config(
                max_markets=2,
                min_volume_usdc=10.0,
                retained_market_ids=("legal",),
                retained_score_bonus=0.01,
            ),
        )

        summary = plan.summary(top_n=1)

        self.assertEqual(summary.discovered_markets, 4)
        self.assertEqual(summary.selected_markets, 2)
        self.assertEqual(summary.selected_tokens, 4)
        self.assertEqual(summary.deferred_markets, 1)
        self.assertEqual(summary.rejected_markets, 1)
        self.assertEqual(summary.rejection_counts, {"volume_below_min": 1})
        self.assertEqual(summary.deferred_counts, {"capacity": 1})
        self.assertEqual(summary.selected_by_category, {"crypto": 1, "legal": 1})
        self.assertEqual(summary.selected_by_catalyst, {"catalyst": 1, "random_walk": 1})
        self.assertEqual(summary.rejected_by_catalyst, {"none": 1})
        self.assertEqual(summary.deferred_by_catalyst, {"catalyst": 1})
        self.assertEqual(summary.selected_by_duration_bucket, {"1h": 2})
        self.assertEqual(summary.retained_selected_markets, 1)
        self.assertEqual(summary.top_selected_market_ids, ("legal",))
        self.assertGreaterEqual(summary.max_selected_score, summary.min_selected_score)


if __name__ == "__main__":
    unittest.main()
