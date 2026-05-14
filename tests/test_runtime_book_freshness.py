from __future__ import annotations

import sys
import unittest
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_V2 = _REPO / "v2" / "short_horizon"
for p in (_REPO, _V2):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from short_horizon.core.clock import ReplayClock
from short_horizon.core.events import BookUpdate
from short_horizon.core.runtime import StrategyRuntime, _compute_daily_pnl_usdc
from short_horizon.storage import InMemoryIntentStore


class _NoopStrategy:
    clock = None

    def on_market_state(self, event):
        return []

    def detect_touches(self, event):
        return []

    def decide_on_touch(self, *, event, touch):
        return None


class RuntimeBookFreshnessTest(unittest.TestCase):
    def test_runtime_tracks_book_age_and_freshness(self) -> None:
        clock = ReplayClock(current_time_ms=1_000)
        runtime = StrategyRuntime(
            strategy=_NoopStrategy(),
            intent_store=InMemoryIntentStore(),
            clock=clock,
            book_freshness_max_age_ms=500,
        )

        runtime.on_book_update(
            BookUpdate(
                event_time_ms=900,
                ingest_time_ms=900,
                market_id="m1",
                token_id="tok_yes",
                best_bid=0.1,
                best_ask=0.2,
            )
        )

        self.assertEqual(runtime.book_age_ms("m1", "tok_yes", now_ms=1_000), 100)
        self.assertTrue(runtime.is_book_fresh("m1", "tok_yes", now_ms=1_000))
        self.assertFalse(runtime.is_book_fresh("m1", "tok_yes", now_ms=1_500))
        self.assertIsNone(runtime.book_age_ms("missing", "tok", now_ms=1_000))

    def test_daily_pnl_ignores_stale_unresolved_marks(self) -> None:
        orders = [{"order_id": "buy-1", "side": "BUY"}]
        fills = [{
            "fill_id": "fill-1",
            "order_id": "buy-1",
            "market_id": "m1",
            "token_id": "tok_yes",
            "price": 0.05,
            "size": 10.0,
            "filled_at": "1970-01-01T00:00:01.000Z",
            "fee_paid_usdc": 0.0,
        }]

        fresh_pnl = _compute_daily_pnl_usdc(
            orders=orders,
            fills=fills,
            event_time_ms=2_000,
            latest_best_bid_by_market_token={("m1", "tok_yes"): 0.01},
            latest_book_ingest_ms_by_market_token={("m1", "tok_yes"): 1_900},
            book_freshness_max_age_ms=500,
            resolved_inventory_marks_by_market_token={},
        )
        stale_pnl = _compute_daily_pnl_usdc(
            orders=orders,
            fills=fills,
            event_time_ms=2_000,
            latest_best_bid_by_market_token={("m1", "tok_yes"): 0.01},
            latest_book_ingest_ms_by_market_token={("m1", "tok_yes"): 1_000},
            book_freshness_max_age_ms=500,
            resolved_inventory_marks_by_market_token={},
        )

        self.assertAlmostEqual(fresh_pnl, -0.4)
        self.assertAlmostEqual(stale_pnl, 0.0)


if __name__ == "__main__":
    unittest.main()
