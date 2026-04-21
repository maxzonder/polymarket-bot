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
from short_horizon.venue_polymarket import FeeMetadataRefreshLoop, MarketMetadata


class FeeMetadataRefreshLoopTest(unittest.IsolatedAsyncioTestCase):
    def _market(self, market_id: str, *, fee_rate_bps: float | None, fees_enabled: bool = True) -> MarketMetadata:
        return MarketMetadata(
            market_id=market_id,
            condition_id=f"cond_{market_id}",
            question="Bitcoin Up or Down?",
            token_yes_id=f"{market_id}_yes",
            token_no_id=f"{market_id}_no",
            start_time_ms=1_776_693_600_000,
            end_time_ms=1_776_694_500_000,
            asset_slug="bitcoin",
            is_active=True,
            duration_seconds=900,
            fees_enabled=fees_enabled,
            fee_rate_bps=fee_rate_bps,
            tick_size=0.01,
            recurrence="15m",
            series_slug="bitcoin-up-or-down-15m",
            yes_outcome_name="Yes",
            no_outcome_name="No",
        )

    async def test_fee_refresh_loop_emits_fresh_fee_snapshots_for_all_markets(self) -> None:
        snapshots = [[self._market("m1", fee_rate_bps=35.0), self._market("m2", fee_rate_bps=8.0, fees_enabled=False)]]
        clock_values = iter([1_776_694_000_000])

        async def fake_discovery(*_args, **_kwargs):
            return snapshots.pop(0)

        loop = FeeMetadataRefreshLoop(discovery_fn=fake_discovery, clock_ms=lambda: next(clock_values))

        events = await loop.refresh_once()
        self.assertEqual(len(events), 2)
        self.assertTrue(all(event.event_type == EventType.MARKET_STATE_UPDATE for event in events))
        self.assertTrue(all(event.fee_metadata_age_ms == 0 for event in events))
        self.assertTrue(all(event.fee_fetched_at_ms == 1_776_694_000_000 for event in events))
        self.assertEqual([event.market_id for event in events], ["m1", "m2"])
        self.assertEqual([event.fee_rate_bps for event in events], [35.0, 8.0])
        self.assertEqual([event.fees_enabled for event in events], [True, False])
        self.assertTrue(all(event.status == MarketStatus.ACTIVE for event in events))
        self.assertTrue(all(event.source == "polymarket.gamma.fee_refresh" for event in events))

        queued = [await loop.__anext__(), await loop.__anext__()]
        self.assertEqual([event.market_id for event in queued], ["m1", "m2"])

    async def test_fee_refresh_loop_reemits_same_market_to_keep_ttl_fresh(self) -> None:
        snapshots = [[self._market("m1", fee_rate_bps=35.0)], [self._market("m1", fee_rate_bps=35.0)]]
        clock_values = iter([1_776_694_000_000, 1_776_694_030_000])

        async def fake_discovery(*_args, **_kwargs):
            return snapshots.pop(0)

        loop = FeeMetadataRefreshLoop(
            discovery_fn=fake_discovery,
            refresh_interval_seconds=30,
            fee_metadata_ttl_seconds=60,
            clock_ms=lambda: next(clock_values),
        )

        first = await loop.refresh_once()
        second = await loop.refresh_once()

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertEqual(first[0].market_id, "m1")
        self.assertEqual(second[0].market_id, "m1")
        self.assertEqual(first[0].fee_rate_bps, 35.0)
        self.assertEqual(second[0].fee_rate_bps, 35.0)
        self.assertEqual(first[0].fee_fetched_at_ms, 1_776_694_000_000)
        self.assertEqual(second[0].fee_fetched_at_ms, 1_776_694_030_000)
        self.assertEqual(first[0].fee_metadata_age_ms, 0)
        self.assertEqual(second[0].fee_metadata_age_ms, 0)

    async def test_fee_refresh_loop_retries_after_transient_failure(self) -> None:
        calls = 0

        async def fake_discovery(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("429 Too Many Requests")
            return [self._market("m1", fee_rate_bps=35.0)]

        loop = FeeMetadataRefreshLoop(
            discovery_fn=fake_discovery,
            refresh_interval_seconds=30,
            fee_metadata_ttl_seconds=60,
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


if __name__ == "__main__":
    unittest.main()
