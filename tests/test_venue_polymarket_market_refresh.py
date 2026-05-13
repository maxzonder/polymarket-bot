from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.core import EventType, MarketStatus
from short_horizon.venue_polymarket import MarketMetadata, MarketRefreshLoop
from short_horizon.venue_polymarket.market_refresh import MarketResolutionSnapshot


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

        async def no_resolution_snapshot(_market_id: str):
            return None

        loop = MarketRefreshLoop(
            discovery_fn=fake_discovery,
            refresh_interval_seconds=999,
            resolution_snapshot_fn=no_resolution_snapshot,
        )

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

    async def test_market_refresh_loop_attaches_binary_settlement_for_dropped_resolved_market(self) -> None:
        snapshots = [
            [self._market("m1", asset_slug="bitcoin", end_time_ms=1_776_694_500_000)],
            [],
        ]

        async def fake_discovery(*_args, **_kwargs):
            return snapshots.pop(0)

        async def fake_resolution_snapshot(market_id: str):
            self.assertEqual(market_id, "m1")
            return MarketResolutionSnapshot(
                is_active=False,
                status=MarketStatus.RESOLVED,
                settlement_prices={"m1_yes": 0.0, "m1_no": 1.0},
                resolved_token_id="m1_no",
            )

        loop = MarketRefreshLoop(
            discovery_fn=fake_discovery,
            refresh_interval_seconds=999,
            resolution_snapshot_fn=fake_resolution_snapshot,
        )

        await loop.refresh_once()
        dropped_events = await loop.refresh_once()

        self.assertEqual(len(dropped_events), 1)
        dropped = dropped_events[0]
        self.assertEqual(dropped.market_id, "m1")
        self.assertEqual(dropped.status, MarketStatus.RESOLVED)
        self.assertFalse(dropped.is_active)
        self.assertEqual(dropped.resolved_token_id, "m1_no")
        self.assertEqual(dropped.settlement_prices, {"m1_yes": 0.0, "m1_no": 1.0})

    async def test_dropped_active_market_stays_pending_until_settlement(self) -> None:
        snapshots = [
            [self._market("m1", asset_slug="bitcoin", end_time_ms=1_776_694_500_000)],
            [],
            [],
        ]
        resolution_snapshots = [
            MarketResolutionSnapshot(is_active=True, status=MarketStatus.ACTIVE),
            MarketResolutionSnapshot(
                is_active=False,
                status=MarketStatus.RESOLVED,
                settlement_prices={"m1_yes": 1.0, "m1_no": 0.0},
                resolved_token_id="m1_yes",
            ),
        ]

        async def fake_discovery(*_args, **_kwargs):
            return snapshots.pop(0)

        async def fake_resolution_snapshot(market_id: str):
            self.assertEqual(market_id, "m1")
            return resolution_snapshots.pop(0)

        loop = MarketRefreshLoop(
            discovery_fn=fake_discovery,
            refresh_interval_seconds=999,
            resolution_snapshot_fn=fake_resolution_snapshot,
        )

        await loop.refresh_once()
        dropped_active_events = await loop.refresh_once()

        self.assertEqual(len(dropped_active_events), 1)
        self.assertEqual(dropped_active_events[0].status, MarketStatus.ACTIVE)
        self.assertIn("m1", loop._previous_markets)

        resolved_events = await loop.refresh_once()

        self.assertEqual(len(resolved_events), 1)
        self.assertEqual(resolved_events[0].status, MarketStatus.RESOLVED)
        self.assertEqual(resolved_events[0].resolved_token_id, "m1_yes")
        self.assertEqual(resolved_events[0].settlement_prices, {"m1_yes": 1.0, "m1_no": 0.0})
        self.assertNotIn("m1", loop._previous_markets)

    async def test_dropped_closed_market_without_binary_settlement_stays_pending(self) -> None:
        snapshots = [
            [self._market("m1", asset_slug="bitcoin", end_time_ms=1_776_694_500_000)],
            [],
        ]

        async def fake_discovery(*_args, **_kwargs):
            return snapshots.pop(0)

        async def fake_resolution_snapshot(market_id: str):
            self.assertEqual(market_id, "m1")
            return MarketResolutionSnapshot(is_active=False, status=MarketStatus.CLOSED)

        loop = MarketRefreshLoop(
            discovery_fn=fake_discovery,
            refresh_interval_seconds=999,
            resolution_snapshot_fn=fake_resolution_snapshot,
        )

        await loop.refresh_once()
        dropped_closed_events = await loop.refresh_once()

        self.assertEqual(len(dropped_closed_events), 1)
        self.assertEqual(dropped_closed_events[0].status, MarketStatus.CLOSED)
        self.assertIsNone(dropped_closed_events[0].settlement_prices)
        self.assertIn("m1", loop._previous_markets)

    async def test_market_refresh_loop_retries_after_transient_failure(self) -> None:
        calls = 0

        async def fake_discovery(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("429 Too Many Requests")
            return [self._market("m1", asset_slug="bitcoin", end_time_ms=1_776_694_500_000)]

        loop = MarketRefreshLoop(
            discovery_fn=fake_discovery,
            refresh_interval_seconds=999,
            retry_backoff_initial_seconds=0.01,
            retry_backoff_max_seconds=0.01,
        )

        await loop.start()
        try:
            event = await asyncio.wait_for(loop.__anext__(), timeout=0.2)
        finally:
            await loop.stop()

        self.assertEqual(calls, 2)
        self.assertEqual(event.market_id, "m1")
        self.assertFalse(loop.failed())

    async def test_market_refresh_loop_exposes_failure_after_repeated_errors(self) -> None:
        async def fake_discovery(*_args, **_kwargs):
            raise RuntimeError("429 Too Many Requests")

        loop = MarketRefreshLoop(
            discovery_fn=fake_discovery,
            refresh_interval_seconds=999,
            retry_backoff_initial_seconds=0.01,
            retry_backoff_max_seconds=0.01,
            max_consecutive_failures=2,
        )

        await loop.start()
        try:
            await asyncio.sleep(0.05)
            self.assertTrue(loop.failed())
            self.assertIsInstance(loop.failure_exception(), RuntimeError)
        finally:
            await loop.stop()


if __name__ == "__main__":
    unittest.main()
