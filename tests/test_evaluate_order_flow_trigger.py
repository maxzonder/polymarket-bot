from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from evaluate_order_flow_trigger import evaluate_order_flow_trigger, realized_ev_per_usdc
from _order_flow_features import build_order_flow_features


class EvaluateOrderFlowTriggerTest(unittest.TestCase):
    def test_realized_ev_uses_executable_ask_and_fee(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            touch_db, capture_glob = _write_fixture(Path(tmp))
            feature = build_order_flow_features(touch_db, capture_glob)[0]
            expected = (1.0 / 0.65) - 1.0 - 0.072
            self.assertAlmostEqual(realized_ev_per_usdc(feature, fee_rate_bps=720.0), expected)

    def test_evaluator_writes_report_and_output_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            touch_db, capture_glob = _write_fixture(root)
            report = root / "report.md"
            output_db = root / "order_flow.sqlite3"

            summary = evaluate_order_flow_trigger(
                touch_db,
                capture_glob,
                report_path=report,
                output_db_path=output_db,
                bootstrap_samples=20,
                null_samples=10,
                min_segment_n=1,
                max_null_p_value=1.0,
                asset_allowlist={"btc"},
                seed=5,
            )

            self.assertEqual(summary.raw_feature_rows, 1)
            self.assertEqual(summary.deduped_feature_rows, 1)
            self.assertGreater(summary.segment_count, 0)
            self.assertEqual(summary.go_gate, "GO")
            self.assertTrue(report.exists())
            self.assertIn("T2 order-flow trigger", report.read_text(encoding="utf-8"))
            conn = sqlite3.connect(output_db)
            try:
                feature_count = conn.execute("SELECT COUNT(*) FROM order_flow_features").fetchone()[0]
                segment_count = conn.execute("SELECT COUNT(*) FROM order_flow_segments").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(feature_count, 1)
            self.assertEqual(segment_count, summary.segment_count)


def _write_fixture(root: Path) -> tuple[Path, str]:
    touch_db = root / "touch.sqlite3"
    capture_dir = root / "bundle_a"
    capture_dir.mkdir()
    events = capture_dir / "events_log.jsonl"
    conn = sqlite3.connect(touch_db)
    conn.execute(
        """
        CREATE TABLE touch_dataset (
            probe_id TEXT,
            market_id TEXT,
            token_id TEXT,
            asset_slug TEXT,
            direction TEXT,
            touch_level REAL,
            touch_time_iso TEXT,
            lifecycle_fraction REAL,
            resolves_yes INTEGER,
            fee_rate_bps REAL,
            best_ask_at_touch REAL,
            best_bid_at_touch REAL,
            tick_size REAL
        )
        """
    )
    conn.execute(
        "INSERT INTO touch_dataset VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("p1", "m1", "tok", "btc", "DOWN/NO", 0.65, "2026-04-26T00:00:10Z", 0.7, 1, 720.0, 0.65, 0.64, 0.01),
    )
    conn.commit()
    conn.close()
    rows = [
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:05Z", "market_id": "m1", "token_id": "tok", "best_bid": 0.61, "best_ask": 0.62},
        {"event_type": "TradeTick", "event_time": "2026-04-26T00:00:06Z", "market_id": "m1", "token_id": "tok", "price": 0.62, "size": 5.0, "aggressor_side": "buy"},
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:09Z", "market_id": "m1", "token_id": "tok", "best_bid": 0.64, "best_ask": 0.65},
        {"event_type": "TradeTick", "event_time": "2026-04-26T00:00:09.500Z", "market_id": "m1", "token_id": "tok", "price": 0.65, "size": 10.0, "aggressor_side": "buy"},
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:10Z", "market_id": "m1", "token_id": "tok", "best_bid": 0.64, "best_ask": 0.65},
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:10.500Z", "market_id": "m1", "token_id": "tok", "best_bid": 0.65, "best_ask": 0.66},
    ]
    with events.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
    return touch_db, str(root / "*" / "events_log.jsonl")


if __name__ == "__main__":
    unittest.main()
