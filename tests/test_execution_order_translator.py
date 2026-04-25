from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.core.models import OrderIntent
from short_horizon.execution import (
    VenueConstraints,
    VenueTranslationError,
    estimate_effective_buy_notional,
    translate_place_order,
)
from short_horizon.venue_polymarket.markets import MarketMetadata


class ExecutionOrderTranslatorTest(unittest.TestCase):
    def _market(self) -> MarketMetadata:
        return MarketMetadata(
            market_id="m1",
            condition_id="c1",
            question="Bitcoin Up or Down?",
            token_yes_id="tok_yes",
            token_no_id="tok_no",
            start_time_ms=1_700_000_000_000,
            end_time_ms=1_700_000_900_000,
            asset_slug="bitcoin",
            is_active=True,
            duration_seconds=900,
            fees_enabled=True,
            fee_rate_bps=10.0,
            tick_size=0.01,
        )

    def _intent(self, **overrides) -> OrderIntent:
        payload = {
            "intent_id": "intent-1",
            "strategy_id": "short_horizon_15m_touch_v1",
            "market_id": "m1",
            "token_id": "tok_yes",
            "condition_id": "c1",
            "question": "Bitcoin Up or Down?",
            "asset_slug": "bitcoin",
            "level": 0.55,
            "entry_price": 0.551,
            "notional_usdc": 10.0,
            "lifecycle_fraction": 0.25,
            "event_time_ms": 225_000,
        }
        payload.update(overrides)
        return OrderIntent(**payload)

    def test_translate_valid_buy_intent_rounds_price_down_and_generates_client_order_id(self) -> None:
        request = translate_place_order(
            self._intent(),
            self._market(),
            VenueConstraints(tick_size=0.01, min_order_size=1.0),
            client_order_id_seed="seed-1",
        )

        self.assertEqual(request.token_id, "tok_yes")
        self.assertEqual(request.side, "BUY")
        self.assertAlmostEqual(request.price, 0.55)
        self.assertAlmostEqual(request.size, 18.181819)
        self.assertEqual(request.time_in_force, "GTC")
        self.assertFalse(request.post_only)
        self.assertIsNotNone(request.client_order_id)

    def test_translate_bumps_one_dollar_buy_above_minimum_notional_floor(self) -> None:
        request = translate_place_order(
            self._intent(entry_price=0.65, notional_usdc=1.0),
            self._market(),
            VenueConstraints(tick_size=0.01, min_order_size=1.0),
        )

        self.assertAlmostEqual(request.price, 0.65)
        self.assertAlmostEqual(request.size, 1.553847)
        self.assertGreaterEqual(request.price * request.size, 1.009999)

    def test_translate_scales_buy_to_market_minimum_share_size(self) -> None:
        request = translate_place_order(
            self._intent(entry_price=0.55, notional_usdc=1.0),
            self._market(),
            VenueConstraints(tick_size=0.01, min_order_size=1.0, min_order_shares=5.0),
        )

        self.assertAlmostEqual(request.price, 0.55)
        self.assertAlmostEqual(request.size, 5.0)
        self.assertGreaterEqual(request.price * request.size, 2.75)

    def test_translate_rejects_below_minimum_size_after_rounding(self) -> None:
        with self.assertRaises(VenueTranslationError) as ctx:
            translate_place_order(
                self._intent(entry_price=0.55, notional_usdc=0.20),
                self._market(),
                VenueConstraints(tick_size=0.01, min_order_size=1.0),
            )

        self.assertIn("below venue minimum", str(ctx.exception))

    def test_translate_rejects_mismatched_token(self) -> None:
        with self.assertRaises(VenueTranslationError) as ctx:
            translate_place_order(
                self._intent(token_id="tok_other"),
                self._market(),
                VenueConstraints(tick_size=0.01, min_order_size=1.0),
            )

        self.assertIn("does not belong to market", str(ctx.exception))

    def test_translate_same_seed_produces_same_client_order_id(self) -> None:
        request_a = translate_place_order(
            self._intent(intent_id="intent-seeded"),
            self._market(),
            VenueConstraints(tick_size=0.01, min_order_size=1.0),
            client_order_id_seed="stable-seed",
        )
        request_b = translate_place_order(
            self._intent(intent_id="intent-seeded"),
            self._market(),
            VenueConstraints(tick_size=0.01, min_order_size=1.0),
            client_order_id_seed="stable-seed",
        )

        self.assertEqual(request_a.client_order_id, request_b.client_order_id)


class EstimateEffectiveBuyNotionalTest(unittest.TestCase):
    def test_matches_translator_output_for_plain_one_dollar_intent(self) -> None:
        constraints = VenueConstraints(tick_size=0.01, min_order_size=1.0)
        notional = estimate_effective_buy_notional(
            notional_usdc=1.0,
            entry_price=0.65,
            venue_constraints=constraints,
        )
        self.assertGreaterEqual(notional, 1.009999)
        self.assertLess(notional, 1.02)

    def test_matches_translator_upscale_when_min_shares_binds(self) -> None:
        constraints = VenueConstraints(tick_size=0.01, min_order_size=1.0, min_order_shares=5.0)
        notional = estimate_effective_buy_notional(
            notional_usdc=1.0,
            entry_price=0.55,
            venue_constraints=constraints,
        )
        self.assertAlmostEqual(notional, 2.75, places=6)

    def test_falls_back_to_intent_on_non_positive_price(self) -> None:
        constraints = VenueConstraints(tick_size=0.01, min_order_size=1.0)
        notional = estimate_effective_buy_notional(
            notional_usdc=1.0,
            entry_price=0.0,
            venue_constraints=constraints,
        )
        self.assertEqual(notional, 1.0)


class StrategyRuntimeVenueMinSharesFallbackTest(unittest.TestCase):
    """Covers the live-only fallback that upscales orders to the venue minimum
    when Gamma's fee_refresh response omits min_order_size, so we stop sending
    sub-minimum orders that the venue rejects with "Size lower than minimum: 5".
    """

    def _build_runtime(self, *, venue_min_order_shares_fallback: float):
        from short_horizon.config import RiskConfig, ShortHorizonConfig
        from short_horizon.core.clock import SystemClock
        from short_horizon.core.runtime import StrategyRuntime
        from short_horizon.storage.runtime import InMemoryIntentStore
        from short_horizon.strategies import ShortHorizon15mTouchStrategy

        config = ShortHorizonConfig(
            risk=RiskConfig(
                micro_live_cumulative_stake_cap_usdc=15.0,
                max_orders_per_market_per_run=10,
            )
        )
        clock = SystemClock()
        strategy = ShortHorizon15mTouchStrategy(config=config, clock=clock)
        return StrategyRuntime(
            strategy=strategy,
            intent_store=InMemoryIntentStore(),
            clock=clock,
            venue_min_order_shares_fallback=venue_min_order_shares_fallback,
        )

    def _intent(self, *, notional_usdc: float = 1.0, entry_price: float = 0.55):
        from short_horizon.core.models import OrderIntent

        return OrderIntent(
            intent_id="intent-1",
            strategy_id="short_horizon_15m_touch_v1",
            market_id="m1",
            token_id="tok_yes",
            condition_id="c1",
            question="Bitcoin Up or Down?",
            asset_slug="bitcoin",
            level=0.55,
            entry_price=entry_price,
            notional_usdc=notional_usdc,
            lifecycle_fraction=0.25,
            event_time_ms=225_000,
        )

    def test_disabled_fallback_keeps_effective_notional_close_to_intent(self) -> None:
        runtime = self._build_runtime(venue_min_order_shares_fallback=0.0)
        notional = runtime._effective_notional_usdc(self._intent())
        self.assertLess(notional, 1.05)

    def test_enabled_fallback_upscales_to_five_shares_when_meta_missing(self) -> None:
        runtime = self._build_runtime(venue_min_order_shares_fallback=5.0)
        notional = runtime._effective_notional_usdc(self._intent())
        self.assertAlmostEqual(notional, 2.75, places=6)

    def test_cumulative_stake_cap_projects_upscaled_notional_and_skips_early(self) -> None:
        from short_horizon.core.models import SkipDecision

        runtime = self._build_runtime(venue_min_order_shares_fallback=5.0)
        runtime.store.insert_order(
            order_id="o_prev",
            market_id="m_other",
            token_id="t_other",
            side="BUY",
            price=0.55,
            size=23.0,
            state="filled",
            client_order_id="c_prev",
            intent_created_at_ms=210_000,
            last_state_change_at_ms=210_000,
            remaining_size=0.0,
        )

        decision = runtime._apply_runtime_guards(self._intent())

        self.assertIsInstance(decision, SkipDecision)
        self.assertEqual(decision.reason, "micro_live_cumulative_stake_cap_reached")


if __name__ == "__main__":
    unittest.main()
