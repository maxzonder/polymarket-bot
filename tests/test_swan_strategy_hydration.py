from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.core.events import TimerEvent
from short_horizon.strategies.swan_strategy_v1 import (
    TIMER_SCREENER_REFRESH,
    SwanCandidate,
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


if __name__ == "__main__":
    unittest.main()
