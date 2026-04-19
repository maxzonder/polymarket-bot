from __future__ import annotations

import unittest
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.config import ShortHorizonConfig
from short_horizon.engine import ShortHorizonEngine
from short_horizon.events import BookUpdate, MarketStateUpdate
from short_horizon.models import OrderIntent, SkipDecision
from short_horizon.storage import InMemoryIntentStore


class ShortHorizonEngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = InMemoryIntentStore()
        self.engine = ShortHorizonEngine(config=ShortHorizonConfig(), intent_store=self.store)

    def _market_state(self, *, token_id: str = "tok_yes", asset_slug: str = "bitcoin") -> MarketStateUpdate:
        return MarketStateUpdate(
            event_time_ms=200_000,
            ingest_time_ms=200_050,
            market_id="m1",
            token_id=token_id,
            condition_id="c1",
            question="Bitcoin Up or Down?",
            asset_slug=asset_slug,
            start_time_ms=0,
            end_time_ms=900_000,
            is_active=True,
            metadata_is_fresh=True,
            fee_rate_bps=10.0,
            fee_metadata_age_ms=1_000,
        )

    def _book(self, *, event_time_ms: int, best_ask: float) -> BookUpdate:
        return BookUpdate(
            event_time_ms=event_time_ms,
            ingest_time_ms=event_time_ms + 20,
            market_id="m1",
            token_id="tok_yes",
            best_bid=best_ask - 0.01,
            best_ask=best_ask,
        )

    def test_persists_single_intent_on_first_touch_in_bucket(self) -> None:
        self.engine.on_market_state(self._market_state())

        self.assertEqual(self.engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54)), [])
        outputs = self.engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], OrderIntent)
        self.assertEqual(len(self.store.intents), 1)
        self.assertEqual(self.store.intents[0].entry_price, 0.55)
        self.assertEqual(self.store.intents[0].level, 0.55)

    def test_non_btc_eth_touch_is_skipped(self) -> None:
        self.engine.on_market_state(self._market_state(asset_slug="dogecoin"))
        self.engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54))
        outputs = self.engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "asset_not_allowed")
        self.assertEqual(len(self.store.intents), 0)

    def test_touch_does_not_rearm_after_wrong_bucket_skip(self) -> None:
        self.engine.on_market_state(self._market_state())

        self.engine.on_book_update(self._book(event_time_ms=80_000, best_ask=0.54))
        early_outputs = self.engine.on_book_update(self._book(event_time_ms=90_000, best_ask=0.55))
        self.assertEqual(len(early_outputs), 1)
        self.assertIsInstance(early_outputs[0], SkipDecision)
        self.assertEqual(early_outputs[0].reason, "wrong_lifecycle_bucket")

        self.engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54))
        later_outputs = self.engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))
        self.assertEqual(later_outputs, [])
        self.assertEqual(len(self.store.intents), 0)


if __name__ == "__main__":
    unittest.main()
