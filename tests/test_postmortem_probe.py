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

from postmortem_probe import analyze_probe
from notify_probe_completed import build_telegram_summary


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
CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL,
    price REAL,
    size REAL,
    state TEXT NOT NULL,
    client_order_id TEXT,
    venue_order_id TEXT,
    parent_order_id TEXT,
    intent_created_at TEXT NOT NULL,
    last_state_change_at TEXT NOT NULL,
    venue_order_status TEXT,
    cumulative_filled_size REAL NOT NULL DEFAULT 0,
    remaining_size REAL,
    last_reject_code TEXT,
    last_reject_reason TEXT,
    reconciliation_required INTEGER NOT NULL DEFAULT 0
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


class PostmortemProbeTest(unittest.TestCase):
    def test_analyze_probe_outputs_requested_summary_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "micro_live_test.sqlite3"
            log = Path(tmp) / "micro_live_test.log"
            con = sqlite3.connect(db)
            con.executescript(SCHEMA)
            con.execute(
                "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("run-1", "2026-04-25T00:00:00.000Z", "2026-04-25T00:10:00.000Z", "live", "s", None, "cfg", None),
            )
            con.execute(
                "INSERT INTO orders (order_id, run_id, market_id, token_id, side, price, size, state, intent_created_at, last_state_change_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("ord-1", "run-1", "m1", "tok1", "BUY", 0.5, 2.0, "filled", "2026-04-25T00:01:00.000Z", "2026-04-25T00:02:00.000Z"),
            )
            con.execute(
                "INSERT INTO fills VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("fill-1", "ord-1", "run-1", "m1", "tok1", 0.5, 2.0, None, None, "2026-04-25T00:02:00.000Z", "venue_ws", None),
            )
            events = [
                ("OrderIntent", {"reason": "ascending_first_touch"}),
                ("SkipDecision", {"reason": "wrong_lifecycle_bucket"}),
                ("MarketResolvedWithInventory", {"market_id": "m1", "token_id": "tok1", "estimated_pnl_usdc": 0.25}),
            ]
            for seq, (event_type, payload) in enumerate(events, start=1):
                payload.setdefault("event_type", event_type)
                payload.setdefault("event_time", f"2026-04-25T00:0{seq}:00.000Z")
                payload.setdefault("ingest_time", f"2026-04-25T00:0{seq}:00.000Z")
                payload.setdefault("source", "test")
                con.execute(
                    "INSERT INTO events_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ("run-1", seq, event_type, payload["event_time"], payload["ingest_time"], payload["source"], payload.get("market_id"), payload.get("token_id"), None, json.dumps(payload)),
                )
            con.commit()
            log.write_text(
                json.dumps({"event": "ws_subscribe_send_failed", "level": "warning"}) + "\n"
                + json.dumps({"event": "live_runner_completed", "level": "info"}) + "\n"
            )

            report = analyze_probe(db, log_path=log)

            self.assertEqual(report.intents, 1)
            self.assertEqual(report.fills, 1)
            self.assertEqual(report.cumulative_stake_usdc, 1.0)
            self.assertEqual(report.resolved_pnl_usdc, 0.25)
            self.assertEqual(report.skip_histogram["wrong_lifecycle_bucket"], 1)
            self.assertFalse(report.anti_chase_violations)
            self.assertEqual(report.ws_warning_counts["ws_subscribe_send_failed"], 1)
            self.assertTrue(report.closed_cleanly)
            self.assertIn("Probe postmortem", report.as_github_comment())
            self.assertIn("Probe completed", build_telegram_summary(db, log_path=log))


if __name__ == "__main__":
    unittest.main()
