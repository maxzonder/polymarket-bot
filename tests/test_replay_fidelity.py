from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.replay_runner import _is_replay_input_event, replay_bundle
from short_horizon.replay.comparator import compare_bundle_to_replay
from short_horizon.replay.venue_client import ReplayFidelityError
from short_horizon.config import ShortHorizonConfig, ExecutionConfig
from short_horizon.core import MarketResolvedWithInventory, OrderSide, SpotPriceUpdate

FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "replay_fidelity" / "minimal_bundle"

class ReplayFidelityGateTest(unittest.TestCase):
    def test_fidelity_gate_minimal_bundle_matches_perfectly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "replay.sqlite3"
            config = ShortHorizonConfig(execution=ExecutionConfig(hold_to_resolution=False))
            replay_bundle(bundle_dir=FIXTURE_DIR, db_path=db_path, config=config)
            
            report = compare_bundle_to_replay(bundle_dir=FIXTURE_DIR, db_path=db_path)
            self.assertTrue(report.matched, "Fidelity comparison failed on minimal bundle")


    def test_spot_price_update_is_replay_input(self) -> None:
        event = SpotPriceUpdate(
            event_time_ms=1,
            ingest_time_ms=1,
            source="fixture.spot",
            asset_slug="btc",
            spot_price=75123.45,
        )

        self.assertTrue(_is_replay_input_event(event))

    def test_resolved_inventory_events_are_not_replay_inputs(self) -> None:
        event = MarketResolvedWithInventory(
            event_time_ms=1,
            ingest_time_ms=1,
            market_id="m1",
            token_id="tok1",
            side=OrderSide.BUY,
            size=1.0,
            outcome_price=0.0,
            average_entry_price=0.5,
            estimated_pnl_usdc=-0.5,
        )

        self.assertFalse(_is_replay_input_event(event))

    def test_fidelity_gate_fails_on_deliberate_strategy_perturbation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "replay.sqlite3"
            config = ShortHorizonConfig(execution=ExecutionConfig(target_trade_size_usdc=1000.0, hold_to_resolution=False))
            
            with self.assertRaises(ReplayFidelityError):
                replay_bundle(bundle_dir=FIXTURE_DIR, db_path=db_path, config=config)

if __name__ == "__main__":
    unittest.main()
