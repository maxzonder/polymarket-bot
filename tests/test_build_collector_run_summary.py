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

from build_collector_run_summary import build_summary, write_json, write_markdown  # noqa: E402


class BuildCollectorRunSummaryTest(unittest.TestCase):
    def test_builds_compact_counts_and_signal_ranking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "collector.sqlite3"
            signal_path = tmp_path / "signal.json"
            _write_collector(db_path)
            signal_path.write_text(
                json.dumps(
                    [
                        {
                            "group": {"asset_slug": "eth", "outcome": "Down"},
                            "rows_after_filters": 12,
                            "win_rate": 0.75,
                            "avg_required_hit_rate_5_shares": 0.65,
                            "total_ev_5_shares": 10.0,
                            "roi_on_cost_5_shares": 0.12,
                            "avg_spread": 0.03,
                        },
                        {
                            "group": {"asset_slug": "btc", "outcome": "Down"},
                            "rows_after_filters": 8,
                            "win_rate": 0.25,
                            "avg_required_hit_rate_5_shares": 0.60,
                            "total_ev_5_shares": -5.0,
                            "roi_on_cost_5_shares": -0.08,
                            "avg_spread": 0.04,
                        },
                    ]
                ),
                encoding="utf-8",
            )

            summary = build_summary(db_path, signal_json=signal_path, top_n=3)

            self.assertEqual(summary.table_counts["touch_events"], 2)
            self.assertEqual(summary.table_counts["book_snapshots"], 5)
            self.assertEqual(summary.run_ids, ["run1"])
            self.assertIn({"value": "5m", "rows": 1}, summary.horizon_counts)
            self.assertEqual(summary.quality_counts["fit_5_executable_rows"], 1)
            self.assertEqual(summary.best_signal_slices[0]["group"], {"asset_slug": "eth", "outcome": "Down"})
            self.assertIn("Positive slices exist", summary.recommendation)

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "collector.sqlite3"
            md_path = tmp_path / "summary.md"
            json_path = tmp_path / "summary.json"
            _write_collector(db_path)

            summary = build_summary(db_path)
            write_markdown(summary, md_path)
            write_json(summary, json_path)

            self.assertIn("Collector run summary", md_path.read_text(encoding="utf-8"))
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["table_counts"]["touch_events"], 2)


def _write_collector(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE collection_runs (
                run_id TEXT PRIMARY KEY,
                started_at_iso TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE touch_events (
                probe_id TEXT PRIMARY KEY,
                run_id TEXT,
                horizon_bucket TEXT,
                asset_slug TEXT,
                outcome TEXT,
                touch_level REAL,
                fit_5_shares TEXT,
                missing_depth_flag_at_touch INTEGER,
                crossed_book_flag_at_touch INTEGER,
                book_stale_flag_at_touch INTEGER,
                wide_spread_flag_at_touch INTEGER,
                zero_size_level_flag_at_touch INTEGER
            )
            """
        )
        conn.execute("CREATE TABLE book_snapshots (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE spot_snapshots (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO collection_runs VALUES ('run1', '2026-05-09T00:00:00Z')")
        for _ in range(5):
            conn.execute("INSERT INTO book_snapshots DEFAULT VALUES")
        conn.executemany(
            "INSERT INTO touch_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("p1", "run1", "5m", "eth", "Down", 0.55, "+0_tick", 0, 0, 0, 0, 0),
                ("p2", "run1", "15m", "btc", "Down", 0.60, "insufficient_depth", 1, 0, 0, 1, 0),
            ],
        )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    unittest.main()
