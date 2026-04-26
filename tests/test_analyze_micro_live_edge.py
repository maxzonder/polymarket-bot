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

from analyze_micro_live_edge import analyze_edge


SCHEMA = """
CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    mode TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    git_sha TEXT,
    config_hash TEXT NOT NULL,
    notes TEXT
);
CREATE TABLE markets (
    market_id TEXT PRIMARY KEY,
    condition_id TEXT,
    token_yes_id TEXT,
    token_no_id TEXT,
    question TEXT,
    market_status TEXT,
    duration_seconds_snapshot INTEGER,
    start_time_latest TEXT,
    end_time_latest TEXT,
    fee_rate_bps_latest REAL,
    fee_fetched_at TEXT,
    fees_enabled INTEGER,
    market_source_revision TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    price REAL NOT NULL,
    size REAL NOT NULL,
    fee_paid_usdc REAL,
    liquidity_role TEXT,
    filled_at TEXT NOT NULL,
    source TEXT NOT NULL,
    venue_fill_id TEXT
);
CREATE TABLE events_log (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_time TEXT NOT NULL,
    ingest_time TEXT NOT NULL,
    source TEXT NOT NULL,
    market_id TEXT,
    token_id TEXT,
    order_id TEXT,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (run_id, seq)
);
"""


class AnalyzeMicroLiveEdgeTest(unittest.TestCase):
    def test_analyze_edge_uses_actual_fill_cost_and_breakdowns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "probe.sqlite3"
            con = sqlite3.connect(db)
            con.executescript(SCHEMA)
            con.execute(
                "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("run-1", "2026-04-25T00:00:00.000Z", "2026-04-25T01:00:00.000Z", "live", "s", None, "cfg", None),
            )
            con.execute(
                "INSERT INTO markets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("m1", None, "yes-token", "no-token", "Bitcoin Up or Down - test", None, None, None, None, 720.0, None, None, None, "2026-04-25T00:00:00.000Z"),
            )
            con.execute(
                "INSERT INTO fills VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("f1", "o1", "run-1", "m1", "yes-token", 0.55, 5.0, None, None, "2026-04-25T00:10:00.000Z", "venue_ws", None),
            )
            events = [
                (
                    "OrderIntent",
                    {
                        "market_id": "m1",
                        "token_id": "yes-token",
                        "order_id": "o1",
                        "entry_price": 0.55,
                        "level": 0.55,
                        "lifecycle_fraction": 0.31,
                    },
                ),
                (
                    "MarketResolvedWithInventory",
                    {
                        "market_id": "m1",
                        "token_id": "yes-token",
                        "average_entry_price": 0.55,
                        "size": 5.0,
                        "outcome_price": 0.99,
                        "estimated_pnl_usdc": 2.2,
                    },
                ),
            ]
            for seq, (event_type, payload) in enumerate(events, start=1):
                payload.setdefault("event_type", event_type)
                payload.setdefault("event_time", f"2026-04-25T00:{seq:02d}:00.000Z")
                payload.setdefault("ingest_time", f"2026-04-25T00:{seq:02d}:00.000Z")
                payload.setdefault("source", "test")
                con.execute(
                    "INSERT INTO events_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ("run-1", seq, event_type, payload["event_time"], payload["ingest_time"], payload["source"], payload.get("market_id"), payload.get("token_id"), payload.get("order_id"), json.dumps(payload)),
                )
            con.commit()

            report = analyze_edge([db], min_trades_for_go=1)

            self.assertEqual(report.overall.trades, 1)
            self.assertEqual(report.overall.wins, 1)
            self.assertAlmostEqual(report.overall.cost_usdc, 2.75)
            self.assertAlmostEqual(report.overall.gross_pnl_usdc, 2.2)
            self.assertAlmostEqual(report.overall.estimated_fee_usdc, 0.198)
            self.assertAlmostEqual(report.overall.net_pnl_after_estimated_fees, 2.002)
            self.assertAlmostEqual(report.overall.pnl_usdc, 2.002)
            self.assertAlmostEqual(report.overall.break_even_hit_rate or 0.0, 0.5896)
            self.assertEqual(report.by_asset[0].name, "BTC")
            self.assertEqual(report.by_direction[0].name, "UP/YES")
            self.assertEqual(report.by_price_level[0].name, "0.55")
            self.assertEqual(report.by_lifecycle_bucket[0].name, "0.3-0.4")
            self.assertIn("GO", report.recommendation)
            self.assertIn("Phase 6 P6-1 edge report", report.as_markdown())

    def test_analyze_edge_rejects_missing_fee_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "probe.sqlite3"
            con = sqlite3.connect(db)
            con.executescript(SCHEMA)
            con.execute(
                "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("run-1", "2026-04-25T00:00:00.000Z", "2026-04-25T01:00:00.000Z", "live", "s", None, "cfg", None),
            )
            con.execute(
                "INSERT INTO markets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("m1", None, "yes-token", "no-token", "Bitcoin Up or Down - test", None, None, None, None, None, None, None, None, "2026-04-25T00:00:00.000Z"),
            )
            con.execute(
                "INSERT INTO fills VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("f1", "o1", "run-1", "m1", "yes-token", 0.55, 5.0, None, None, "2026-04-25T00:10:00.000Z", "venue_ws", None),
            )
            for seq, (event_type, payload) in enumerate(
                [
                    ("OrderIntent", {"market_id": "m1", "token_id": "yes-token", "entry_price": 0.55, "level": 0.55}),
                    ("MarketResolvedWithInventory", {"market_id": "m1", "token_id": "yes-token", "average_entry_price": 0.55, "size": 5.0, "outcome_price": 0.99, "estimated_pnl_usdc": 2.2}),
                ],
                start=1,
            ):
                payload.setdefault("event_type", event_type)
                payload.setdefault("event_time", f"2026-04-25T00:{seq:02d}:00.000Z")
                payload.setdefault("ingest_time", f"2026-04-25T00:{seq:02d}:00.000Z")
                payload.setdefault("source", "test")
                con.execute(
                    "INSERT INTO events_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ("run-1", seq, event_type, payload["event_time"], payload["ingest_time"], payload["source"], payload.get("market_id"), payload.get("token_id"), payload.get("order_id"), json.dumps(payload)),
                )
            con.commit()

            with self.assertRaisesRegex(ValueError, "missing fee_rate_bps_latest"):
                analyze_edge([db], min_trades_for_go=1)


if __name__ == "__main__":
    unittest.main()
