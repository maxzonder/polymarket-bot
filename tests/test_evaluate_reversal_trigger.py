from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from evaluate_reversal_trigger import (
    TouchRow,
    evaluate_reversal_trigger,
    evaluate_touch,
    lifecycle_bucket,
    realized_ev_per_usdc,
)


class ReversalTriggerTest(unittest.TestCase):
    def test_realized_ev_uses_inverse_price_payout_and_taker_fee(self) -> None:
        self.assertAlmostEqual(realized_ev_per_usdc(0.50, True, 720.0), 0.928)
        self.assertAlmostEqual(realized_ev_per_usdc(0.50, False, 720.0), -1.072)
        self.assertAlmostEqual(realized_ev_per_usdc(0.30, True, 100.0), (1.0 / 0.30) - 1.0 - 0.01)

    def test_evaluate_touch_flips_winner_and_price_for_reversal(self) -> None:
        row = TouchRow(
            probe_id="p1",
            market_id="m1",
            token_id="yes",
            asset_slug="btc",
            direction="UP/YES",
            touch_level=0.65,
            lifecycle_fraction=0.62,
            resolves_yes=1,
            fee_rate_bps=720.0,
        )
        item = evaluate_touch(row, fee_rate_bps=720.0)
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item.lifecycle_bucket, "0.60-0.75")
        self.assertAlmostEqual(item.continuation_entry_price, 0.65)
        self.assertAlmostEqual(item.reversal_entry_price, 0.35)
        self.assertGreater(item.continuation_ev_per_usdc, 0.0)
        self.assertAlmostEqual(item.reversal_ev_per_usdc, -1.072)

    def test_lifecycle_buckets_match_issue_contract(self) -> None:
        self.assertEqual(lifecycle_bucket(0.10), "<0.40")
        self.assertEqual(lifecycle_bucket(0.40), "0.40-0.60")
        self.assertEqual(lifecycle_bucket(0.60), "0.60-0.75")
        self.assertEqual(lifecycle_bucket(0.75), ">=0.75")
        self.assertIsNone(lifecycle_bucket(None))

    def test_pipeline_writes_report_and_marks_positive_reversal_segment_go(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            touch_db = tmp_path / "touch_dataset.sqlite3"
            report = tmp_path / "report.md"
            _write_touch_db(touch_db, n=60)

            summary = evaluate_reversal_trigger(
                touch_db,
                report_path=report,
                fee_rate_bps=720.0,
                bootstrap_samples=50,
                min_segment_n=50,
                seed=7,
            )

            self.assertEqual(summary.input_rows, 60)
            self.assertEqual(summary.evaluated_rows, 60)
            self.assertEqual(summary.go_gate, "GO")
            self.assertTrue(report.exists())
            self.assertIn("T1 reversal trigger", report.read_text(encoding="utf-8"))
            best = summary.best_reversal_segment
            self.assertIsNotNone(best)
            assert best is not None
            self.assertEqual(best.asset_slug, "btc")
            self.assertEqual(best.direction, "UP/YES")
            self.assertEqual(best.lifecycle_bucket, "0.40-0.60")
            self.assertAlmostEqual(best.level, 0.65)
            self.assertEqual(best.n, 60)
            self.assertEqual(best.gate, "GO")
            self.assertGreater(best.reversal_ev_per_usdc, 0.0)
            self.assertLess(best.continuation_ev_per_usdc, 0.0)


def _write_touch_db(path: Path, *, n: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE touch_dataset (
            probe_id TEXT PRIMARY KEY,
            market_id TEXT,
            token_id TEXT,
            asset_slug TEXT,
            direction TEXT,
            touch_level REAL,
            lifecycle_fraction REAL,
            resolves_yes INTEGER,
            fee_rate_bps REAL,
            touch_time_iso TEXT
        )
        """
    )
    rows = []
    for i in range(n):
        # Continuation loses every row; reversal wins every row at 0.35 entry.
        rows.append((f"p{i}", f"m{i}", f"t{i}", "btc", "UP/YES", 0.65, 0.50, 0, 720.0, f"2026-04-20T00:{i:02d}:00Z"))
    conn.executemany("INSERT INTO touch_dataset VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    unittest.main()
