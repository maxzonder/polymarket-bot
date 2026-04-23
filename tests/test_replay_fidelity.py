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

from short_horizon.replay_runner import replay_bundle
from short_horizon.replay.comparator import compare_bundle_to_replay
from short_horizon.replay.venue_client import ReplayFidelityError
from short_horizon.config import ShortHorizonConfig, ExecutionConfig

FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "replay_fidelity" / "minimal_bundle"

class ReplayFidelityGateTest(unittest.TestCase):
    def test_fidelity_gate_minimal_bundle_matches_perfectly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "replay.sqlite3"
            config = ShortHorizonConfig(execution=ExecutionConfig(hold_to_resolution=False))
            replay_bundle(bundle_dir=FIXTURE_DIR, db_path=db_path, config=config)
            
            report = compare_bundle_to_replay(bundle_dir=FIXTURE_DIR, db_path=db_path)
            self.assertTrue(report.matched, "Fidelity comparison failed on minimal bundle")

    def test_fidelity_gate_fails_on_deliberate_strategy_perturbation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "replay.sqlite3"
            config = ShortHorizonConfig(execution=ExecutionConfig(target_trade_size_usdc=1000.0, hold_to_resolution=False))
            
            with self.assertRaises(ReplayFidelityError):
                replay_bundle(bundle_dir=FIXTURE_DIR, db_path=db_path, config=config)

if __name__ == "__main__":
    unittest.main()
