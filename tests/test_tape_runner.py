from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from replay.tape_runner import TapeDrivenDryRunRunner


class TapeRunnerSmokeTests(unittest.TestCase):
    def test_runner_constructs_and_runs_empty_tape(self) -> None:
        with TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            fake_rows = [
                {
                    "market_id": "m1",
                    "question": "Question?",
                    "category": "sports",
                    "volume": 1000.0,
                    "comment_count": 1,
                    "end_date": 1_700_000_000,
                    "start_date": 1_699_000_000,
                    "neg_risk": 0,
                    "neg_risk_market_id": None,
                    "token_id": "tok1",
                    "outcome_name": "Yes",
                }
            ]
            with patch("replay.tape_runner.load_all_markets", return_value=fake_rows), patch(
                "replay.tape_runner.iter_tape_batches", return_value=[]
            ):
                runner = TapeDrivenDryRunRunner(
                    start=date(2026, 1, 1),
                    end=date(2026, 1, 1),
                    mode="big_swan_mode",
                    output_dir=out,
                    limit_markets=1,
                    batch_seconds=300,
                )
                result = runner.run()

            self.assertEqual(result["batches"], 0)
            self.assertEqual(result["trades"], 0)
            self.assertEqual(result["orders_placed"], 0)
            self.assertTrue(out.exists())


if __name__ == "__main__":
    unittest.main()
