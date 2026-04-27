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

from evaluate_pre_touch_maker import (
    MakerConfig,
    evaluate_pre_touch_maker,
    find_trigger_book,
    load_bundle_slice,
    load_touch_rows,
    replay_pre_touch,
)


class EvaluatePreTouchMakerTest(unittest.TestCase):
    def test_trigger_requires_post_only_safe_near_ask(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            touch_db, capture_glob, events = _write_fixture(Path(tmp))
            touch = load_touch_rows(touch_db)[0]
            bundle = load_bundle_slice(events, [touch], max_lookback_ms=120_000)
            assert bundle is not None
            config = MakerConfig(1.0, 1.0, 2, 60_000, 60_000, 30_000, False, False)
            trigger = find_trigger_book(touch, bundle.books_by_token[touch.token_id], config)
            self.assertIsNotNone(trigger)
            assert trigger is not None
            self.assertGreater(trigger.best_ask or 0.0, touch.touch_level)
            self.assertLessEqual(trigger.best_ask or 9.0, touch.touch_level + 0.02 + 1e-9)

    def test_replay_fills_when_ask_reaches_and_sell_volume_clears_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            touch_db, _capture_glob, events = _write_fixture(Path(tmp))
            touch = load_touch_rows(touch_db)[0]
            bundle = load_bundle_slice(events, [touch], max_lookback_ms=120_000)
            assert bundle is not None
            config = MakerConfig(1.0, 1.0, 2, 60_000, 60_000, 30_000, False, False)
            replay = replay_pre_touch(touch, bundle_name="bundle", books=bundle.books_by_token[touch.token_id], trades=bundle.trades_by_token[touch.token_id], config=config)
            self.assertEqual(replay.status, "filled")
            self.assertAlmostEqual(replay.cost_usdc, 2.75)
            self.assertAlmostEqual(replay.pnl_usdc, 2.25)
            self.assertTrue(replay.ask_reached_level)

    def test_evaluator_writes_report_and_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            touch_db, capture_glob, _events = _write_fixture(root)
            report = root / "report.md"
            out_db = root / "pre_touch.sqlite3"
            summary = evaluate_pre_touch_maker(touch_db, capture_glob, report_path=report, output_db=out_db, bootstrap_samples=10, seed=5)
            self.assertEqual(summary.bundle_count, 1)
            self.assertGreater(summary.replay_rows, 0)
            self.assertTrue(report.exists())
            self.assertIn("T3 pre-touch maker", report.read_text(encoding="utf-8"))
            conn = sqlite3.connect(out_db)
            try:
                self.assertGreater(conn.execute("SELECT COUNT(*) FROM pre_touch_replays").fetchone()[0], 0)
                self.assertGreater(conn.execute("SELECT COUNT(*) FROM pre_touch_segments").fetchone()[0], 0)
            finally:
                conn.close()


def _write_fixture(root: Path) -> tuple[Path, str, Path]:
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
            touch_time_iso TEXT,
            asset_slug TEXT,
            direction TEXT,
            touch_level REAL,
            lifecycle_fraction REAL,
            resolves_yes INTEGER,
            tick_size REAL
        )
        """
    )
    conn.execute(
        "INSERT INTO touch_dataset VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("p1", "m1", "tok", "2026-04-26T00:01:00Z", "btc", "UP/YES", 0.55, 0.7, 1, 0.01),
    )
    conn.commit()
    conn.close()
    rows = [
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:00Z", "token_id": "tok", "best_bid": 0.50, "best_ask": 0.60},
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:20Z", "token_id": "tok", "best_bid": 0.53, "best_ask": 0.57},
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:35Z", "token_id": "tok", "best_bid": 0.54, "best_ask": 0.56},
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:50Z", "token_id": "tok", "best_bid": 0.54, "best_ask": 0.55},
        {"event_type": "TradeTick", "event_time": "2026-04-26T00:00:52Z", "token_id": "tok", "price": 0.55, "size": 5.0, "aggressor_side": "sell"},
    ]
    with events.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
    return touch_db, str(root / "*" / "events_log.jsonl"), events


if __name__ == "__main__":
    unittest.main()
