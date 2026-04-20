from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.core import EventType, MarketStatus
from short_horizon.venue_polymarket import MarketMetadata, MarketRefreshLoop


class MarketRefreshLoopTest(unittest.IsolatedAsyncioTestCase):
    def _market(self, market_id: str, *, asset_slug: str, end_time_ms: int) -> MarketMetadata:
        return MarketMetadata(
            market_id=market_id,
            condition_id=f"cond_{market_id}",
            question=f"{asset_slug.title()} Up or Down?",
            token_yes_id=f"{market_id}_yes",
            token_no_id=f"{market_id}_no",
            start_time_ms=1_776_693_600_000,
            end_time_ms=end_time_ms,
            asset_slug=asset_slug,
            is_active=True,
            duration_seconds=900,
            fees_enabled=True,
            fee_rate_bps=10.0,
            tick_size=0.01,
            recurrence="15m",
            series_slug=f"{asset_slug}-up-or-down-15m",
            yes_outcome_name="Yes",
            no_outcome_name="No",
        )

    async def test_market_refresh_loop_emits_initial_then_only_material_changes(self) -> None:
        snapshots = [
            [
                self._market("m1", asset_slug="bitcoin", end_time_ms=1_776_694_500_000),
                self._market("m2", asset_slug="ethereum", end_time_ms=1_776_694_560_000),
            ],
            [
                self._market("m1", asset_slug="bitcoin", end_time_ms=1_776_694_560_000),
                self._market("m3", asset_slug="bitcoin", end_time_ms=1_776_694_620_000),
            ],
        ]

        async def fake_discovery(*_args, **_kwargs):
            return snapshots.pop(0)

        loop = MarketRefreshLoop(discovery_fn=fake_discovery, refresh_interval_seconds=999)

        first_events = await loop.refresh_once()
        self.assertEqual(len(first_events), 2)
        self.assertEqual({event.market_id for event in first_events}, {"m1", "m2"})
        self.assertTrue(all(event.metadata_is_fresh for event in first_events))
        self.assertTrue(all(event.event_type == EventType.MARKET_STATE_UPDATE for event in first_events))
        self.assertTrue(all(event.status == MarketStatus.ACTIVE for event in first_events))
        self.assertTrue(all(event.fee_metadata_age_ms == 0 for event in first_events))
        self.assertTrue(all(event.fee_fetched_at_ms is not None for event in first_events))
        self.assertTrue(all(event.fees_enabled is True for event in first_events))
        self.assertTrue(all(event.fee_rate_bps == 10.0 for event in first_events))

        queued_initial = [await loop.__anext__(), await loop.__anext__()]
        self.assertEqual({event.market_id for event in queued_initial}, {"m1", "m2"})

        second_events = await loop.refresh_once()
        self.assertEqual(len(second_events), 3)

        updated = next(event for event in second_events if event.market_id == "m1")
        new_market = next(event for event in second_events if event.market_id == "m3")
        dropped = next(event for event in second_events if event.market_id == "m2")

        self.assertEqual(updated.end_time_ms, 1_776_694_560_000)
        self.assertEqual(updated.status, MarketStatus.ACTIVE)
        self.assertEqual(updated.fee_rate_bps, 10.0)
        self.assertEqual(new_market.status, MarketStatus.ACTIVE)
        self.assertEqual(new_market.fee_metadata_age_ms, 0)
        self.assertEqual(dropped.status, MarketStatus.CLOSED)
        self.assertFalse(dropped.is_active)
        self.assertEqual(dropped.asset_slug, "ethereum")

        queued_second = [await loop.__anext__(), await loop.__anext__(), await loop.__anext__()]
        self.assertEqual({event.market_id for event in queued_second}, {"m1", "m2", "m3"})


if __name__ == "__main__":
    unittest.main()
