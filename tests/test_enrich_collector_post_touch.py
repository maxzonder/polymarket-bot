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

from enrich_collector_post_touch import (  # noqa: E402
    build_enrichment_summary,
    enrich_post_touch,
    write_json,
    write_markdown,
)


class EnrichCollectorPostTouchTest(unittest.TestCase):
    def test_enriches_touch_with_horizon_movements(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db = tmp_path / "collector.sqlite3"
            _write_fixture(db)

            count = enrich_post_touch(db, horizons_ms=(1000, 5000, 15000, 60000))

            self.assertEqual(count, 2)
            conn = sqlite3.connect(db)
            try:
                row = conn.execute(
                    "SELECT move_1000ms, move_5000ms, max_favorable_60000ms, max_adverse_60000ms, held_5000ms, reversal_5000ms FROM post_touch_enrichment WHERE probe_id='tok1:0.55:100000'"
                ).fetchone()
            finally:
                conn.close()

            self.assertAlmostEqual(row[0], 0.01)
            self.assertAlmostEqual(row[1], -0.02)
            self.assertAlmostEqual(row[2], 0.03)
            self.assertAlmostEqual(row[3], -0.02)
            self.assertEqual(row[4], 0)
            self.assertEqual(row[5], 1)

    def test_summary_and_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db = tmp_path / "collector.sqlite3"
            md = tmp_path / "summary.md"
            js = tmp_path / "summary.json"
            _write_fixture(db)
            enrich_post_touch(db)

            rows = build_enrichment_summary(db, axes=("touch_level", "asset_slug"))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].rows, 2)
            self.assertIsNotNone(rows[0].held_5s_rate)

            write_markdown(rows, md, source=db, axes=("touch_level", "asset_slug"))
            write_json(rows, js)
            self.assertIn("Post-touch enrichment", md.read_text())
            self.assertEqual(len(json.loads(js.read_text())), 1)


def _write_fixture(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE touch_events (
                probe_id TEXT PRIMARY KEY,
                run_id TEXT,
                token_id TEXT,
                market_id TEXT,
                asset_slug TEXT,
                outcome TEXT,
                touch_level REAL,
                touch_time_iso TEXT,
                best_ask_at_touch REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE book_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                run_id TEXT,
                token_id TEXT,
                market_id TEXT,
                event_ts_ms INTEGER,
                best_ask REAL
            )
            """
        )
        conn.executemany(
            "INSERT INTO touch_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("tok1:0.55:100000", "run1", "tok1", "m1", "btc", "Up", 0.55, "1970-01-01T00:01:40.000Z", 0.55),
                ("tok1:0.55:200000", "run1", "tok1", "m1", "btc", "Up", 0.55, "1970-01-01T00:03:20.000Z", 0.55),
            ],
        )
        snapshots = [
            ("s0", "run1", "tok1", "m1", 100000, 0.55),
            ("s1", "run1", "tok1", "m1", 101000, 0.56),
            ("s2", "run1", "tok1", "m1", 105000, 0.53),
            ("s3", "run1", "tok1", "m1", 115000, 0.58),
            ("s4", "run1", "tok1", "m1", 160000, 0.57),
            ("s5", "run1", "tok1", "m1", 200000, 0.55),
            ("s6", "run1", "tok1", "m1", 201000, 0.56),
            ("s7", "run1", "tok1", "m1", 205000, 0.57),
            ("s8", "run1", "tok1", "m1", 260000, 0.58),
        ]
        conn.executemany("INSERT INTO book_snapshots VALUES (?, ?, ?, ?, ?, ?)", snapshots)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    unittest.main()
