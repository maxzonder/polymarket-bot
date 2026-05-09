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

from build_collector_heatmap import build_collector_heatmap, write_json, write_markdown  # noqa: E402


class BuildCollectorHeatmapTest(unittest.TestCase):
    def test_builds_executable_context_heatmap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "collector.sqlite3"
            _write_touch_events(db_path)

            rows = build_collector_heatmap(db_path, axes=("touch_level", "asset_slug"))

            self.assertEqual(len(rows), 2)
            btc = next(row for row in rows if row.group == {"touch_level": 0.55, "asset_slug": "btc"})
            self.assertEqual(btc.rows, 2)
            self.assertEqual(btc.executable_rows, 1)
            self.assertEqual(btc.stale_or_bad_rows, 1)
            self.assertAlmostEqual(btc.fit_5_share_rate or 0.0, 0.5)
            self.assertAlmostEqual(btc.avg_spread or 0.0, 0.02)
            self.assertAlmostEqual(btc.immediate_reversal_rate or 0.0, 0.5)
            self.assertAlmostEqual(btc.held_at_or_above_rate or 0.0, 0.5)

            executable = build_collector_heatmap(db_path, axes=("touch_level", "asset_slug"), executable_only=True)
            btc_executable = next(row for row in executable if row.group == {"touch_level": 0.55, "asset_slug": "btc"})
            self.assertEqual(btc_executable.rows, 1)
            self.assertEqual(btc_executable.executable_rows, 1)

    def test_writes_markdown_and_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "collector.sqlite3"
            md_path = tmp_path / "heatmap.md"
            json_path = tmp_path / "heatmap.json"
            _write_touch_events(db_path)

            rows = build_collector_heatmap(db_path, axes=("touch_level", "asset_slug"))
            write_markdown(rows, md_path, axes=("touch_level", "asset_slug"), source=db_path)
            write_json(rows, json_path)

            self.assertIn("Collector heatmap", md_path.read_text())
            payload = json.loads(json_path.read_text())
            self.assertEqual(len(payload), 2)
            self.assertIn("executable_rows", payload[0])


def _write_touch_events(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE touch_events (
                probe_id TEXT PRIMARY KEY,
                touch_level REAL,
                asset_slug TEXT,
                outcome TEXT,
                horizon_bucket TEXT,
                fit_5_shares TEXT,
                fit_10_usdc TEXT,
                spread_at_touch REAL,
                imbalance_top1 REAL,
                required_hit_rate_for_5_shares REAL,
                spot_return_30s REAL,
                immediate_reversal_flag INTEGER,
                held_at_or_above_level INTEGER,
                max_favorable_move REAL,
                max_adverse_move REAL,
                missing_depth_flag_at_touch INTEGER,
                crossed_book_flag_at_touch INTEGER,
                book_stale_flag_at_touch INTEGER
            )
            """
        )
        rows = [
            (
                "p1",
                0.55,
                "btc",
                "Up",
                "15m",
                "+0_tick",
                "+0_tick",
                0.01,
                0.6,
                0.56,
                0.01,
                0,
                1,
                0.02,
                -0.01,
                0,
                0,
                0,
            ),
            (
                "p2",
                0.55,
                "btc",
                "Up",
                "15m",
                "insufficient_depth",
                "insufficient_depth",
                0.03,
                0.4,
                None,
                -0.02,
                1,
                0,
                0.01,
                -0.03,
                1,
                0,
                0,
            ),
            (
                "p3",
                0.65,
                "eth",
                "Down",
                "15m",
                "+1_tick",
                "+2plus_ticks",
                0.02,
                0.5,
                0.66,
                0.0,
                0,
                1,
                0.03,
                -0.01,
                0,
                0,
                0,
            ),
        ]
        conn.executemany("INSERT INTO touch_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    unittest.main()
