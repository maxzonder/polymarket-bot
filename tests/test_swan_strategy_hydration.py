from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.core.events import OrderAccepted, OrderFilled, OrderSide, TimerEvent
from short_horizon.strategies.swan_strategy_v1 import (
    TIMER_SCREENER_REFRESH,
    SwanCandidate,
    SwanConfig,
    SwanStrategyV1,
)
from short_horizon.strategy_api import CancelOrder, PlaceOrder


class SwanStrategyHydrationTests(unittest.TestCase):
    def _timer(self) -> TimerEvent:
        return TimerEvent(
            event_time_ms=1_000,
            ingest_time_ms=1_000,
            timer_kind=TIMER_SCREENER_REFRESH,
            source="test",
        )

    def test_hydrated_open_order_is_canceled_when_market_drops_from_screener(self) -> None:
        strategy = SwanStrategyV1()
        strategy.hydrate_open_orders([
            {
                "state": "accepted",
                "order_id": "ord-1",
                "market_id": "m-1",
                "token_id": "tok-1",
                "price": 0.01,
                "size": 100.0,
            }
        ])

        strategy.update_candidates([])
        intents = strategy.on_timer(self._timer())

        self.assertEqual(len(intents), 1)
        self.assertIsInstance(intents[0], CancelOrder)
        self.assertEqual(intents[0].market_id, "m-1")
        self.assertEqual(intents[0].token_id, "tok-1")
        self.assertEqual(intents[0].reason, "dropped_from_screener")

    def test_hydrated_open_order_prevents_duplicate_bid_when_candidate_remains(self) -> None:
        strategy = SwanStrategyV1()
        strategy.hydrate_open_orders([
            {
                "state": "accepted",
                "order_id": "ord-1",
                "market_id": "m-1",
                "token_id": "tok-1",
                "price": 0.01,
                "size": 100.0,
            }
        ])

        strategy.update_candidates([
            SwanCandidate(
                market_id="m-1",
                condition_id="c-1",
                token_id="tok-1",
                question="Still a candidate?",
                asset_slug="crypto",
                entry_levels=(0.01,),
                notional_usdc_per_level=1.0,
            )
        ])
        intents = strategy.on_timer(self._timer())

        self.assertFalse(any(isinstance(intent, PlaceOrder) for intent in intents))
        self.assertEqual(intents, [])

    def test_swan_resting_bid_intents_are_post_only(self) -> None:
        strategy = SwanStrategyV1()
        strategy.update_candidates([
            SwanCandidate(
                market_id="m-2",
                condition_id="c-2",
                token_id="tok-2",
                question="New candidate?",
                asset_slug="crypto",
                entry_levels=(0.01,),
                notional_usdc_per_level=1.0,
            )
        ])

        intents = strategy.on_timer(self._timer())

        self.assertEqual(len(intents), 1)
        self.assertIsInstance(intents[0], PlaceOrder)
        self.assertTrue(intents[0].intent.post_only)

    def test_filled_entry_level_is_not_reopened_on_next_refresh(self) -> None:
        strategy = SwanStrategyV1()
        candidate = SwanCandidate(
            market_id="m-3",
            condition_id="c-3",
            token_id="tok-3",
            question="Still cheap?",
            asset_slug="crypto",
            entry_levels=(0.01,),
            notional_usdc_per_level=1.0,
        )
        strategy.update_candidates([candidate])
        [place] = strategy.on_timer(self._timer())
        order_id = place.intent.intent_id
        strategy.on_order_event(OrderAccepted(
            event_time_ms=1_001,
            ingest_time_ms=1_001,
            order_id=order_id,
            market_id="m-3",
            token_id="tok-3",
            side=OrderSide.BUY,
            price=0.01,
            size=100.0,
            source="test",
        ))
        strategy.on_order_event(OrderFilled(
            event_time_ms=1_002,
            ingest_time_ms=1_002,
            order_id=order_id,
            market_id="m-3",
            token_id="tok-3",
            side=OrderSide.BUY,
            fill_price=0.01,
            fill_size=100.0,
            cumulative_filled_size=100.0,
            remaining_size=0.0,
            source="test",
        ))

        strategy.update_candidates([candidate])
        intents = strategy.on_timer(self._timer())

        self.assertEqual(intents, [])

    def test_total_stake_cap_counts_filled_and_open_notional(self) -> None:
        strategy = SwanStrategyV1(config=SwanConfig(max_total_stake_usdc=2.0))
        strategy.update_candidates([
            SwanCandidate(
                market_id="m-4",
                condition_id="c-4",
                token_id="tok-4",
                question="Cap test?",
                asset_slug="crypto",
                entry_levels=(0.01, 0.02, 0.03),
                notional_usdc_per_level=1.0,
            )
        ])

        intents = strategy.on_timer(self._timer())

        self.assertEqual(len(intents), 2)
        self.assertTrue(all(isinstance(intent, PlaceOrder) for intent in intents))

    def test_hydrated_filled_level_prevents_reopen_after_restart(self) -> None:
        strategy = SwanStrategyV1()
        strategy.hydrate_open_orders([
            {
                "state": "filled",
                "order_id": "ord-filled",
                "market_id": "m-5",
                "token_id": "tok-5",
                "side": "BUY",
                "price": 0.01,
                "size": 100.0,
                "cumulative_filled_size": 100.0,
                "remaining_size": 0.0,
            }
        ])
        strategy.update_candidates([
            SwanCandidate(
                market_id="m-5",
                condition_id="c-5",
                token_id="tok-5",
                question="Restart duplicate?",
                asset_slug="crypto",
                entry_levels=(0.01,),
                notional_usdc_per_level=1.0,
            )
        ])

        intents = strategy.on_timer(self._timer())

        self.assertEqual(intents, [])


if __name__ == "__main__":
    unittest.main()
