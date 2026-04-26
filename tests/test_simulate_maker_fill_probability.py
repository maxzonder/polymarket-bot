from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.simulate_maker_fill_probability import (
    SimConfig,
    TouchRow,
    load_touch_rows,
    maker_fill_decision,
    order_shares_for_stake,
    run_sweep,
    simulate_config,
)


class SimulateMakerFillProbabilityTests(unittest.TestCase):
    def test_order_size_respects_venue_min_shares(self) -> None:
        self.assertAlmostEqual(order_shares_for_stake(stake_usdc=1.0, entry_price=0.50), 5.0)
        self.assertAlmostEqual(order_shares_for_stake(stake_usdc=10.0, entry_price=0.50), 20.0)

    def test_fill_requires_size_and_survival_time(self) -> None:
        row = TouchRow(
            probe_id="p1",
            market_id="m1",
            touch_time_iso="2026-04-26T00:00:00Z",
            asset_slug="btc",
            direction="DOWN/NO",
            touch_level=0.50,
            lifecycle_fraction=0.70,
            ask_size_at_touch_level=10.0,
            survived_ms=500.0,
            resolves_yes=1,
        )

        filled = maker_fill_decision(row, stake_usdc=1.0, queue_factor=2.0, min_survived_ms=500.0)
        self.assertTrue(filled.accepted)
        self.assertAlmostEqual(filled.order_shares, 5.0)
        self.assertAlmostEqual(filled.net_pnl, 2.5)

        no_size = maker_fill_decision(row, stake_usdc=1.0, queue_factor=5.0, min_survived_ms=500.0)
        self.assertFalse(no_size.accepted)

        no_time = maker_fill_decision(row, stake_usdc=1.0, queue_factor=2.0, min_survived_ms=1000.0)
        self.assertFalse(no_time.accepted)

    def test_simulate_config_summarizes_ev_and_ci(self) -> None:
        rows = [
            TouchRow("win", "m1", "2026-04-26T00:00:00Z", "btc", "DOWN/NO", 0.50, 0.70, 10.0, 1000.0, 1),
            TouchRow("loss", "m2", "2026-04-26T00:01:00Z", "btc", "DOWN/NO", 0.50, 0.70, 10.0, 1000.0, 0),
            TouchRow("nofill", "m3", "2026-04-26T00:02:00Z", "btc", "DOWN/NO", 0.50, 0.70, 1.0, 1000.0, 1),
        ]
        summary = simulate_config(rows, SimConfig("a2_down_no", stake_usdc=1.0, queue_factor=1.0, min_survived_ms=0.0), seed=1)
        self.assertEqual(summary.candidate_n, 3)
        self.assertEqual(summary.accepted_n, 2)
        self.assertAlmostEqual(summary.gross_cost_usdc, 5.0)
        self.assertAlmostEqual(summary.net_pnl_usdc, 0.0)
        self.assertAlmostEqual(summary.net_ev_per_usdc or 0.0, 0.0)
        self.assertEqual(len(summary.per_day_rows), 1)

    def test_load_touch_rows_filters_a2_and_baseline(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "touch.sqlite3"
            self._write_touch_db(db)

            a2_rows = load_touch_rows(db, universe="a2_down_no")
            baseline_rows = load_touch_rows(db, universe="hres_baseline")

            self.assertEqual([row.probe_id for row in a2_rows], ["a2"])
            self.assertEqual([row.probe_id for row in baseline_rows], ["a2", "up"])

            summaries = run_sweep(db, seed=1)
            self.assertEqual(len(summaries), 96)

    def _write_touch_db(self, path: Path) -> None:
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE touch_dataset (
                probe_id TEXT,
                market_id TEXT,
                touch_time_iso TEXT,
                asset_slug TEXT,
                direction TEXT,
                touch_level REAL,
                lifecycle_fraction REAL,
                ask_size_at_touch_level REAL,
                survived_ms REAL,
                resolves_yes INTEGER
            );
            INSERT INTO touch_dataset VALUES
                ('a2', 'm1', '2026-04-26T00:00:00Z', 'btc', 'DOWN/NO', 0.55, 0.70, 10.0, 1000.0, 1),
                ('up', 'm2', '2026-04-26T00:01:00Z', 'btc', 'UP/YES', 0.65, 0.70, 10.0, 1000.0, 0),
                ('early', 'm3', '2026-04-26T00:02:00Z', 'btc', 'DOWN/NO', 0.55, 0.50, 10.0, 1000.0, 1),
                ('bnb', 'm4', '2026-04-26T00:03:00Z', 'bnb', 'DOWN/NO', 0.55, 0.70, 10.0, 1000.0, 1),
                ('other_level', 'm5', '2026-04-26T00:04:00Z', 'btc', 'DOWN/NO', 0.60, 0.70, 10.0, 1000.0, 1);
            """
        )
        conn.commit()
        conn.close()


if __name__ == "__main__":
    unittest.main()
