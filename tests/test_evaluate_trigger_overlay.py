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

from evaluate_trigger_overlay import evaluate_trigger_overlay


class EvaluateTriggerOverlayTest(unittest.TestCase):
    def test_matches_trigger_to_touch_and_scores_executable_ask_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trigger_db = root / "triggers.sqlite3"
            touch_db = root / "touch.sqlite3"
            output_db = root / "overlay.sqlite3"
            report = root / "overlay.md"
            _write_trigger_db(trigger_db)
            _write_touch_db(touch_db)

            summary = evaluate_trigger_overlay(
                trigger_db,
                touch_db,
                output_db_path=output_db,
                report_path=report,
                assets={"btc"},
                trigger_labels={"compression_breakout_up"},
                match_window_seconds=30,
                max_ask_slippage_ticks=1,
                fit_allowlist=("+0_tick", "+1_tick"),
                bootstrap_samples=10,
            )

            self.assertEqual(summary.triggers_loaded, 2)
            self.assertEqual(summary.matched_rows, 1)
            self.assertEqual(summary.executable_rows, 1)
            self.assertTrue(report.exists())
            text = report.read_text(encoding="utf-8")
            self.assertIn("Trigger executable overlay report", text)
            self.assertIn("Executable trigger candidates", text)

            conn = sqlite3.connect(output_db)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute("SELECT * FROM overlay_matches").fetchone()
                self.assertEqual(row["executable"], 1)
                self.assertAlmostEqual(row["best_ask_at_touch"], 0.56)
                self.assertAlmostEqual(row["ask_slippage_ticks"], 1.0)
                self.assertGreater(row["net_pnl_per_share"], 0.0)
                agg = conn.execute(
                    "SELECT * FROM overlay_aggregates WHERE group_type='executable_asset_direction_trigger'"
                ).fetchone()
                self.assertEqual(agg["group_key"], "btc|UP/YES|compression_breakout_up")
            finally:
                conn.close()

    def test_rejects_non_executable_depth_label(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trigger_db = root / "triggers.sqlite3"
            touch_db = root / "touch.sqlite3"
            output_db = root / "overlay.sqlite3"
            report = root / "overlay.md"
            _write_trigger_db(trigger_db)
            _write_touch_db(touch_db, fit_10="+2plus_ticks")

            summary = evaluate_trigger_overlay(
                trigger_db,
                touch_db,
                output_db_path=output_db,
                report_path=report,
                assets={"btc"},
                trigger_labels={"compression_breakout_up"},
                match_window_seconds=30,
                max_ask_slippage_ticks=1,
                fit_allowlist=("+0_tick", "+1_tick"),
                bootstrap_samples=10,
            )

            self.assertEqual(summary.matched_rows, 1)
            self.assertEqual(summary.executable_rows, 0)
            conn = sqlite3.connect(output_db)
            try:
                reason = conn.execute("SELECT reject_reason FROM overlay_matches").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(reason, "fit_10_not_allowed")


def _write_trigger_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE trigger_features(
            token_id TEXT,
            market_id TEXT,
            question TEXT,
            asset_slug TEXT,
            direction TEXT,
            split TEXT,
            utc_hour INTEGER,
            trading_session TEXT,
            start_ts INTEGER,
            closed_ts INTEGER,
            trigger_ts INTEGER,
            trigger_fraction REAL,
            trigger_price REAL,
            trigger_label TEXT,
            trigger_tags TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO trigger_features VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "yes1",
                "m1",
                "Bitcoin Up or Down - test",
                "btc",
                "UP/YES",
                "test",
                13,
                "us_morning_13_16utc",
                1_000,
                1_900,
                1_360,
                0.40,
                0.55,
                "compression_breakout_up",
                "positive_acceleration",
            ),
            (
                "yes1",
                "m1",
                "Bitcoin Up or Down - test",
                "btc",
                "UP/YES",
                "test",
                13,
                "us_morning_13_16utc",
                1_000,
                1_900,
                1_720,
                0.80,
                0.70,
                "compression_breakout_up",
                "positive_acceleration",
            ),
        ],
    )
    conn.commit()
    conn.close()


def _write_touch_db(path: Path, *, fit_10: str = "+1_tick") -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE touch_dataset(
            probe_id TEXT,
            token_id TEXT,
            market_id TEXT,
            asset_slug TEXT,
            direction TEXT,
            touch_time_iso TEXT,
            lifecycle_fraction REAL,
            touch_level REAL,
            best_bid_at_touch REAL,
            best_ask_at_touch REAL,
            tick_size REAL,
            fee_rate_bps REAL,
            fees_enabled INTEGER,
            ask_size_at_touch_level REAL,
            fit_10_usdc TEXT,
            fit_50_usdc TEXT,
            fit_100_usdc TEXT,
            survived_ms REAL,
            end_reason TEXT,
            resolves_yes INTEGER,
            would_be_size_at_min_shares REAL,
            would_be_cost_after_min_shares REAL,
            would_be_estimated_fee_usdc REAL
        )
        """
    )
    conn.execute(
        "INSERT INTO touch_dataset VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "p1",
            "yes1",
            "m1",
            "btc",
            "UP/YES",
            "1970-01-01T00:22:45Z",
            0.41,
            0.55,
            0.54,
            0.56,
            0.01,
            720.0,
            1,
            10.0,
            fit_10,
            "+1_tick",
            "insufficient_depth",
            2000.0,
            "window_complete_level_still_present",
            1,
            5.0,
            2.80,
            0.177408,
        ),
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    unittest.main()
