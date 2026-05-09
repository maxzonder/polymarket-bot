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

from build_collector_signal_report import _safe_sort_key, build_signal_report, write_json, write_markdown  # noqa: E402


class BuildCollectorSignalReportTest(unittest.TestCase):
    def test_applies_normal_trade_filters_and_combines_post_touch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "collector.sqlite3"
            resolutions_path = tmp_path / "resolutions.sqlite3"
            _write_collector(db_path)
            _write_resolutions(resolutions_path)

            rows = build_signal_report(
                db_path,
                resolutions_path,
                axes=("duration_seconds", "asset_slug", "outcome"),
                min_rows=1,
                max_spread=0.10,
            )

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row.group, {"duration_seconds": 300, "asset_slug": "eth", "outcome": "Down"})
            self.assertEqual(row.rows_seen, 3)
            self.assertEqual(row.rows_after_filters, 2)
            self.assertEqual(row.winners, 1)
            self.assertAlmostEqual(row.win_rate or 0.0, 0.5)
            self.assertAlmostEqual(row.total_ev_5_shares or 0.0, -0.5)
            self.assertEqual(row.fit_10_usdc_rows, 2)
            self.assertAlmostEqual(row.held_5s_rate or 0.0, 0.5)
            self.assertAlmostEqual(row.reversal_5s_rate or 0.0, 0.5)

    def test_safe_sort_key_handles_mixed_none_and_strings(self) -> None:
        values = [("total",), (None,), ("temperature_exact",)]
        self.assertEqual(sorted(values, key=_safe_sort_key), [(None,), ("temperature_exact",), ("total",)])

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "collector.sqlite3"
            resolutions_path = tmp_path / "resolutions.sqlite3"
            md_path = tmp_path / "signal.md"
            json_path = tmp_path / "signal.json"
            _write_collector(db_path)
            _write_resolutions(resolutions_path)

            rows = build_signal_report(db_path, resolutions_path, min_rows=1)
            write_markdown(rows, md_path, source=db_path, resolutions=resolutions_path, axes=("duration_seconds", "asset_slug", "outcome"), max_spread=0.10, min_rows=1)
            write_json(rows, json_path)

            self.assertIn("Collector signal report", md_path.read_text())
            payload = json.loads(json_path.read_text())
            self.assertEqual(len(payload), 1)
            self.assertIn("held_5s_rate", payload[0])


def _write_collector(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE touch_events (
                probe_id TEXT PRIMARY KEY,
                market_id TEXT,
                token_id TEXT,
                duration_seconds INTEGER,
                touch_level REAL,
                asset_slug TEXT,
                outcome TEXT,
                fit_5_shares TEXT,
                avg_entry_price_for_5_shares REAL,
                cost_for_5_shares REAL,
                estimated_fee_for_5_shares REAL,
                required_hit_rate_for_5_shares REAL,
                fit_10_usdc TEXT,
                avg_entry_price_for_10_usdc REAL,
                spread_at_touch REAL,
                missing_depth_flag_at_touch INTEGER,
                crossed_book_flag_at_touch INTEGER,
                book_stale_flag_at_touch INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE post_touch_enrichment (
                probe_id TEXT PRIMARY KEY,
                duration_seconds INTEGER,
                asset_slug TEXT,
                outcome TEXT,
                touch_level REAL,
                held_1000ms INTEGER,
                held_5000ms INTEGER,
                held_15000ms INTEGER,
                held_60000ms INTEGER,
                reversal_1000ms INTEGER,
                reversal_5000ms INTEGER,
                reversal_15000ms INTEGER,
                reversal_60000ms INTEGER,
                move_5000ms REAL,
                move_60000ms REAL
            )
            """
        )
        touches = [
            ("p1", "m1", "yes1", 300, 0.55, "eth", "Down", "+0_tick", 0.55, 2.75, 0.0, 0.55, "+0_tick", 0.60, 0.02, 0, 0, 0),
            ("p2", "m2", "yes2", 300, 0.55, "eth", "Down", "+0_tick", 0.55, 2.75, 0.0, 0.55, "+0_tick", 0.60, 0.03, 0, 0, 0),
            ("p3", "m3", "yes3", 300, 0.55, "eth", "Down", "+0_tick", 0.55, 2.75, 0.0, 0.55, "+0_tick", 0.60, 0.20, 0, 0, 0),
        ]
        post = [
            ("p1", 300, "eth", "Down", 0.55, 1, 1, 1, 0, 0, 0, 0, 1, 0.02, -0.01),
            ("p2", 300, "eth", "Down", 0.55, 1, 0, 0, 0, 0, 1, 1, 1, -0.02, -0.05),
            ("p3", 300, "eth", "Down", 0.55, 1, 1, 1, 1, 0, 0, 0, 0, 0.01, 0.02),
        ]
        conn.executemany("INSERT INTO touch_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", touches)
        conn.executemany("INSERT INTO post_touch_enrichment VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", post)
        conn.commit()
    finally:
        conn.close()


def _write_resolutions(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE market_resolutions (
                market_id TEXT PRIMARY KEY,
                condition_id TEXT,
                token_yes_id TEXT,
                token_no_id TEXT,
                outcome_resolved_at TEXT,
                outcome_token_id TEXT,
                outcome_price_yes REAL,
                outcome_price_no REAL,
                question TEXT,
                raw_json TEXT,
                fetched_at TEXT NOT NULL
            )
            """
        )
        rows = [
            ("m1", None, "yes1", "no1", "2026-05-09T01:00:00Z", "yes1", 1.0, 0.0, "ETH Down", None, "2026-05-09T01:01:00Z"),
            ("m2", None, "yes2", "no2", "2026-05-09T01:00:00Z", "no2", 0.0, 1.0, "ETH Down", None, "2026-05-09T01:01:00Z"),
            ("m3", None, "yes3", "no3", "2026-05-09T01:00:00Z", "yes3", 1.0, 0.0, "ETH Down", None, "2026-05-09T01:01:00Z"),
        ]
        conn.executemany("INSERT INTO market_resolutions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    unittest.main()
