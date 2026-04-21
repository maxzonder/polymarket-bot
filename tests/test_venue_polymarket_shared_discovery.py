from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.venue_polymarket import DurationWindow, MarketMetadata, SharedMarketDiscovery, UniverseFilter


class SharedMarketDiscoveryTest(unittest.IsolatedAsyncioTestCase):
    def _market(self, market_id: str) -> MarketMetadata:
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
            fees_enabled=True,
            fee_rate_bps=10.0,
            tick_size=0.01,
            recurrence="15m",
            series_slug="bitcoin-up-or-down-15m",
            yes_outcome_name="Yes",
            no_outcome_name="No",
        )

    async def test_shared_discovery_reuses_snapshot_within_refresh_window(self) -> None:
        calls: list[int] = []
        clock = {"now": 100.0}

        async def fake_discovery(universe_filter, duration_window, max_rows):
            calls.append(max_rows)
            self.assertEqual(universe_filter, UniverseFilter())
            self.assertEqual(duration_window, DurationWindow())
            return [self._market("m1")]

        shared = SharedMarketDiscovery(
            discovery_fn=fake_discovery,
            refresh_interval_seconds=30,
            max_rows=500,
            clock_s=lambda: clock["now"],
        )

        first = await shared()
        second = await shared()

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertEqual(calls, [500])

        clock["now"] = 131.0
        third = await shared()

        self.assertEqual(len(third), 1)
        self.assertEqual(calls, [500, 500])


if __name__ == "__main__":
    unittest.main()
