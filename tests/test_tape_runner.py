from __future__ import annotations

import json
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
            out.mkdir(parents=True, exist_ok=True)
            tape_db = out / "historical_tape.db"
            tape_db.write_text("", encoding="utf-8")
            with patch("replay.tape_runner.load_all_markets", return_value=fake_rows), patch(
                "replay.tape_runner.iter_tape_batches_db", return_value=[]
            ):
                runner = TapeDrivenDryRunRunner(
                    start=date(2026, 1, 1),
                    end=date(2026, 1, 1),
                    mode="big_swan_mode",
                    output_dir=out,
                    limit_markets=1,
                    batch_seconds=300,
                    tape_db_path=tape_db,
                )
                result = runner.run()

            self.assertEqual(result["batches"], 288)
            self.assertEqual(result["empty_batches"], 288)
            self.assertEqual(result["trades"], 0)
            self.assertEqual(result["orders_placed"], 0)
            self.assertTrue(out.exists())
            snapshot = json.loads((out / "config_snapshot.json").read_text(encoding="utf-8"))
            self.assertEqual(snapshot["run"]["kind"], "tape_dryrun")
            self.assertEqual(snapshot["run"]["batch_seconds"], 300)
            self.assertEqual(snapshot["run"]["limit_markets"], 1)
            self.assertEqual(snapshot["run"]["mode"], "big_swan_mode")

    def test_runner_falls_back_to_json_batches_for_invalid_tape_db(self) -> None:
        with TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.mkdir(parents=True, exist_ok=True)
            tape_db = out / "historical_tape.db"
            tape_db.write_text("", encoding="utf-8")
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
            ) as json_iter, patch("replay.tape_runner.iter_tape_batches_db") as sqlite_iter:
                runner = TapeDrivenDryRunRunner(
                    start=date(2026, 1, 1),
                    end=date(2026, 1, 1),
                    mode="big_swan_mode",
                    output_dir=out,
                    limit_markets=1,
                    batch_seconds=300,
                    tape_db_path=tape_db,
                )
                runner.run()

            json_iter.assert_called_once()
            sqlite_iter.assert_not_called()

    def test_runner_skips_scan_and_monitor_for_empty_batches(self) -> None:
        from replay.tape_feed import TapeBatch, TapeTrade

        with TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.mkdir(parents=True, exist_ok=True)
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
            batches = [
                TapeBatch(batch_start_ts=0, batch_end_ts=299, trades=()),
                TapeBatch(
                    batch_start_ts=300,
                    batch_end_ts=599,
                    trades=(TapeTrade(300, "m1", "tok1", 0.1, 1.0, "BUY", {}),),
                ),
            ]
            with patch("replay.tape_runner.load_all_markets", return_value=fake_rows), patch(
                "replay.tape_runner.has_valid_tape_db", return_value=False
            ), patch("replay.tape_runner.iter_tape_batches", return_value=batches), patch(
                "strategy.screener.Screener.scan", return_value=[]
            ) as scan, patch.object(
                TapeDrivenDryRunRunner, "_patched_runtime"
            ) as patched_runtime:
                patched_runtime.return_value.__enter__.return_value = None
                patched_runtime.return_value.__exit__.return_value = False
                runner = TapeDrivenDryRunRunner(
                    start=date(2026, 1, 1),
                    end=date(2026, 1, 1),
                    mode="big_swan_mode",
                    output_dir=out,
                    limit_markets=1,
                    batch_seconds=300,
                )
                with patch.object(runner.monitor, "_check_fills_dry_run") as fill_check, patch.object(
                    runner.monitor, "_update_peak_prices"
                ) as peak_check:
                    result = runner.run()

            self.assertEqual(result["batches"], 2)
            self.assertEqual(result["empty_batches"], 1)
            scan.assert_called_once()
            fill_check.assert_called_once()
            peak_check.assert_called_once()


if __name__ == "__main__":
    unittest.main()
