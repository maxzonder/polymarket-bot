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

from build_collector_outcome_ev import build_collector_outcome_ev, write_json, write_markdown  # noqa: E402


class BuildCollectorOutcomeEvTest(unittest.TestCase):
    def test_joins_outcomes_and_computes_ev(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            collector_path = tmp_path / "collector.sqlite3"
            resolutions_path = tmp_path / "resolutions.sqlite3"
            _write_collector(collector_path)
            _write_resolutions(resolutions_path)

            rows = build_collector_outcome_ev(collector_path, resolutions_path, axes=("touch_level", "asset_slug"))

            self.assertEqual(len(rows), 2)
            btc = next(row for row in rows if row.group == {"touch_level": 0.55, "asset_slug": "btc"})
            self.assertEqual(btc.rows, 2)
            self.assertEqual(btc.joined_rows, 2)
            self.assertEqual(btc.executable_rows, 2)
            self.assertEqual(btc.winners, 1)
            self.assertAlmostEqual(btc.win_rate or 0.0, 0.5)
            self.assertAlmostEqual(btc.total_ev_5_shares or 0.0, -0.6)
            self.assertAlmostEqual(btc.avg_ev_5_shares or 0.0, -0.3)
            self.assertAlmostEqual(btc.roi_on_cost_5_shares or 0.0, -0.6 / 5.6)
            self.assertAlmostEqual(btc.total_ev_10_usdc or 0.0, -3.3333333333)

            eth = next(row for row in rows if row.group == {"touch_level": 0.65, "asset_slug": "eth"})
            self.assertEqual(eth.joined_rows, 1)
            self.assertEqual(eth.executable_rows, 0)
            self.assertIsNone(eth.total_ev_5_shares)

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            collector_path = tmp_path / "collector.sqlite3"
            resolutions_path = tmp_path / "resolutions.sqlite3"
            md_path = tmp_path / "ev.md"
            json_path = tmp_path / "ev.json"
            _write_collector(collector_path)
            _write_resolutions(resolutions_path)

            rows = build_collector_outcome_ev(collector_path, resolutions_path, axes=("touch_level", "asset_slug"))
            write_markdown(rows, md_path, axes=("touch_level", "asset_slug"), source=collector_path, resolutions=resolutions_path)
            write_json(rows, json_path)

            self.assertIn("Collector outcome/EV join", md_path.read_text())
            payload = json.loads(json_path.read_text())
            self.assertEqual(len(payload), 2)
            self.assertIn("total_ev_5_shares", payload[0])


def _write_collector(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE touch_events (
                probe_id TEXT PRIMARY KEY,
                market_id TEXT,
                token_id TEXT,
                touch_level REAL,
                asset_slug TEXT,
                outcome TEXT,
                horizon_bucket TEXT,
                fit_5_shares TEXT,
                avg_entry_price_for_5_shares REAL,
                cost_for_5_shares REAL,
                estimated_fee_for_5_shares REAL,
                required_hit_rate_for_5_shares REAL,
                fit_10_usdc TEXT,
                avg_entry_price_for_10_usdc REAL,
                spread_at_touch REAL,
                immediate_reversal_flag INTEGER,
                held_at_or_above_level INTEGER,
                missing_depth_flag_at_touch INTEGER,
                crossed_book_flag_at_touch INTEGER,
                book_stale_flag_at_touch INTEGER
            )
            """
        )
        rows = [
            ("p1", "m1", "yes1", 0.55, "btc", "Up", "15m", "+0_tick", 0.55, 2.75, 0.0, 0.55, "+0_tick", 0.60, 0.02, 0, 1, 0, 0, 0),
            ("p2", "m2", "yes2", 0.55, "btc", "Up", "15m", "+0_tick", 0.57, 2.85, 0.0, 0.57, "+0_tick", 0.60, 0.03, 1, 0, 0, 0, 0),
            ("p3", "m3", "yes3", 0.65, "eth", "Down", "15m", "insufficient_depth", 0.65, None, 0.0, 0.65, "insufficient_depth", None, 0.04, 0, 1, 0, 0, 0),
        ]
        conn.executemany("INSERT INTO touch_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
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
            ("m1", None, "yes1", "no1", "2026-05-09T01:00:00Z", "yes1", 1.0, 0.0, "BTC Up", None, "2026-05-09T01:01:00Z"),
            ("m2", None, "yes2", "no2", "2026-05-09T01:00:00Z", "no2", 0.0, 1.0, "BTC Up", None, "2026-05-09T01:01:00Z"),
            ("m3", None, "yes3", "no3", "2026-05-09T01:00:00Z", "yes3", 1.0, 0.0, "ETH Down", None, "2026-05-09T01:01:00Z"),
        ]
        conn.executemany("INSERT INTO market_resolutions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    unittest.main()
