from __future__ import annotations

import csv
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from build_touch_dataset import build_touch_dataset


FIELDNAMES = [
    "probe_id",
    "recorded_at",
    "market_id",
    "condition_id",
    "token_id",
    "outcome",
    "question",
    "touch_level",
    "touch_time_iso",
    "duration_seconds",
    "start_time_iso",
    "end_time_iso",
    "fees_enabled",
    "fee_rate_bps",
    "tick_size",
    "best_bid_at_touch",
    "best_ask_at_touch",
    "ask_level_1_price",
    "ask_level_1_size",
    "ask_level_2_price",
    "ask_level_2_size",
    "ask_level_3_price",
    "ask_level_3_size",
    "ask_level_4_price",
    "ask_level_4_size",
    "ask_level_5_price",
    "ask_level_5_size",
    "ask_size_at_touch_level",
    "fit_10_usdc",
    "fit_50_usdc",
    "fit_100_usdc",
    "survived_ms",
    "end_reason",
]


class BuildTouchDatasetTest(unittest.TestCase):
    def test_builds_dataset_drops_unresolved_and_labels_by_token_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            csv_path = tmp_path / "touches.csv"
            resolutions_path = tmp_path / "market_resolutions.sqlite3"
            output_path = tmp_path / "touch_dataset.sqlite3"
            _write_collector_csv(
                csv_path,
                [
                    _touch_row("p1", "m1", "yes1", "Up", question="Bitcoin Up or Down - test"),
                    _touch_row("p2", "m1", "no1", "Down", question="Bitcoin Up or Down - test"),
                    _touch_row("p3", "m2", "yes2", "Up", question="Ethereum Up or Down - test"),
                ],
            )
            _write_resolutions(
                resolutions_path,
                [
                    ("m1", "c1", "yes1", "no1", "2026-04-20T12:15:05Z", "yes1", "Bitcoin Up or Down - test"),
                    ("m2", "c2", "yes2", "no2", None, None, "Ethereum Up or Down - test"),
                ],
            )

            summary = build_touch_dataset(csv_path, resolutions_path, output_path=output_path)

            self.assertEqual(summary.input_rows, 3)
            self.assertEqual(summary.output_rows, 2)
            self.assertEqual(summary.dropped_unresolved, 1)
            self.assertEqual(summary.dropped_token_mismatch, 0)
            self.assertEqual(summary.join_hit_rate, 1.0)
            conn = sqlite3.connect(output_path)
            conn.row_factory = sqlite3.Row
            rows = list(conn.execute("SELECT * FROM touch_dataset ORDER BY probe_id"))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["resolves_yes"], 1)
            self.assertEqual(rows[1]["resolves_yes"], 0)
            self.assertEqual(rows[0]["asset_slug"], "btc")
            self.assertEqual(rows[0]["direction"], "UP/YES")
            self.assertAlmostEqual(rows[0]["lifecycle_fraction"], 0.0)
            self.assertAlmostEqual(rows[0]["would_be_cost_after_min_shares"], 2.75)
            self.assertAlmostEqual(rows[0]["would_be_estimated_fee_usdc"], 0.198)
            conn.close()

    def test_token_mismatch_is_reported_not_silently_labeled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            csv_path = tmp_path / "touches.csv"
            resolutions_path = tmp_path / "market_resolutions.sqlite3"
            output_path = tmp_path / "touch_dataset.sqlite3"
            _write_collector_csv(csv_path, [_touch_row("p1", "m1", "surprise-token", "Up")])
            _write_resolutions(resolutions_path, [("m1", "c1", "yes1", "no1", "2026-04-20T12:15:05Z", "yes1", "Bitcoin Up or Down - test")])

            summary = build_touch_dataset(csv_path, resolutions_path, output_path=output_path)

            self.assertEqual(summary.output_rows, 0)
            self.assertEqual(summary.dropped_token_mismatch, 1)
            self.assertEqual(summary.join_hit_rate, 0.0)

    def test_missing_required_csv_field_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            csv_path = tmp_path / "touches.csv"
            resolutions_path = tmp_path / "market_resolutions.sqlite3"
            output_path = tmp_path / "touch_dataset.sqlite3"
            with csv_path.open("w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=["probe_id", "market_id"])
                writer.writeheader()
                writer.writerow({"probe_id": "p1", "market_id": "m1"})
            _write_resolutions(resolutions_path, [])

            with self.assertRaisesRegex(ValueError, "missing required columns"):
                build_touch_dataset(csv_path, resolutions_path, output_path=output_path)

    def test_cross_check_matches_synthetic_executed_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            csv_path = tmp_path / "touches.csv"
            resolutions_path = tmp_path / "market_resolutions.sqlite3"
            output_path = tmp_path / "touch_dataset.sqlite3"
            executed_db = tmp_path / "probe.sqlite3"
            report_path = tmp_path / "cross_check.json"
            _write_collector_csv(csv_path, [_touch_row("p1", "m1", "yes1", "Up")])
            _write_resolutions(resolutions_path, [("m1", "c1", "yes1", "no1", "2026-04-20T12:15:05Z", "yes1", "Bitcoin Up or Down - test")])
            _write_executed_probe_db(executed_db)

            summary = build_touch_dataset(
                csv_path,
                resolutions_path,
                output_path=output_path,
                executed_db_paths=[executed_db],
                cross_check_report_path=report_path,
            )

            assert summary.cross_check is not None
            self.assertEqual(summary.cross_check.executed_trades, 1)
            self.assertEqual(summary.cross_check.found, 1)
            self.assertEqual(summary.cross_check.outcome_matches, 1)
            self.assertEqual(summary.cross_check.cost_matches, 1)
            self.assertTrue(report_path.exists())


def _touch_row(probe_id: str, market_id: str, token_id: str, outcome: str, *, question: str = "Bitcoin Up or Down - test") -> dict[str, str]:
    return {
        "probe_id": probe_id,
        "recorded_at": "2026-04-20T12:00:01Z",
        "market_id": market_id,
        "condition_id": f"c-{market_id}",
        "token_id": token_id,
        "outcome": outcome,
        "question": question,
        "touch_level": "0.55",
        "touch_time_iso": "2026-04-20T12:00:00Z",
        "duration_seconds": "900",
        "start_time_iso": "2026-04-20T12:00:00Z",
        "end_time_iso": "2026-04-20T12:15:00Z",
        "fees_enabled": "1",
        "fee_rate_bps": "720.0",
        "tick_size": "0.01",
        "best_bid_at_touch": "0.53",
        "best_ask_at_touch": "0.55",
        "ask_level_1_price": "0.55",
        "ask_level_1_size": "5.0",
        "ask_level_2_price": "0.56",
        "ask_level_2_size": "5.0",
        "ask_level_3_price": "0.57",
        "ask_level_3_size": "5.0",
        "ask_level_4_price": "0.58",
        "ask_level_4_size": "5.0",
        "ask_level_5_price": "0.59",
        "ask_level_5_size": "5.0",
        "ask_size_at_touch_level": "5.0",
        "fit_10_usdc": "+2plus_ticks",
        "fit_50_usdc": "insufficient_depth",
        "fit_100_usdc": "insufficient_depth",
        "survived_ms": "2000",
        "end_reason": "window_complete_level_still_present",
    }


def _write_collector_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def _write_resolutions(path: Path, rows: list[tuple[str, str, str, str, str | None, str | None, str]]) -> None:
    conn = sqlite3.connect(path)
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
    for market_id, condition_id, yes, no, resolved_at, winner, question in rows:
        conn.execute(
            """
            INSERT INTO market_resolutions (
                market_id, condition_id, token_yes_id, token_no_id, outcome_resolved_at,
                outcome_token_id, outcome_price_yes, outcome_price_no, question, raw_json, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (market_id, condition_id, yes, no, resolved_at, winner, 1.0 if winner == yes else 0.0, 1.0 if winner == no else 0.0, question, "{}", "2026-04-20T12:16:00Z"),
        )
    conn.commit()
    conn.close()


def _write_executed_probe_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE runs (run_id TEXT, started_at TEXT)")
    conn.execute("INSERT INTO runs VALUES ('run1', '2026-04-20T12:00:00Z')")
    conn.execute(
        "CREATE TABLE fills (run_id TEXT, market_id TEXT, token_id TEXT, price REAL, size REAL)"
    )
    conn.execute("INSERT INTO fills VALUES ('run1', 'm1', 'yes1', 0.55, 5.0)")
    conn.execute(
        "CREATE TABLE events_log (run_id TEXT, event_type TEXT, event_time TEXT, market_id TEXT, token_id TEXT, payload_json TEXT)"
    )
    conn.execute(
        "INSERT INTO events_log VALUES (?, ?, ?, ?, ?, ?)",
        ('run1', 'OrderIntent', '2026-04-20T12:00:02Z', 'm1', 'yes1', '{"level": 0.55}'),
    )
    conn.execute(
        "INSERT INTO events_log VALUES (?, ?, ?, ?, ?, ?)",
        ('run1', 'MarketResolvedWithInventory', '2026-04-20T12:15:05Z', 'm1', 'yes1', '{"market_id": "m1", "token_id": "yes1", "outcome_price": 1.0}'),
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    unittest.main()
