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

from evaluate_touch_policy import PolicyCandidate, evaluate_candidates, realized_net_pnl
from train_touch_model import TouchModelRow, load_artifact, train_touch_model, walk_forward_split


class TouchModelPolicyTest(unittest.TestCase):
    def test_train_pipeline_writes_artifact_scores_and_walk_forward_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            touch_db = tmp_path / "touch_dataset.sqlite3"
            spot_db = tmp_path / "spot_features.sqlite3"
            artifact_path = tmp_path / "artifact.json"
            scores_path = tmp_path / "scores.sqlite3"
            report_path = tmp_path / "report.md"
            _write_training_dbs(touch_db, spot_db)

            summary = train_touch_model(
                touch_db,
                spot_db,
                artifact_path=artifact_path,
                scores_path=scores_path,
                report_path=report_path,
                train_fraction=0.5,
                seed=7,
                epochs=250,
                learning_rate=0.1,
            )

            self.assertEqual(summary.joined_rows, 20)
            self.assertEqual(summary.train_rows, 10)
            self.assertEqual(summary.test_rows, 10)
            self.assertTrue(artifact_path.exists())
            self.assertTrue(scores_path.exists())
            self.assertTrue(report_path.exists())
            artifact = load_artifact(artifact_path)
            self.assertEqual(artifact.model_type, "logistic_regression_stdlib")
            self.assertIsNotNone(artifact.metrics["test_auc"])
            conn = sqlite3.connect(scores_path)
            count = conn.execute("SELECT COUNT(*) FROM touch_model_scores").fetchone()[0]
            splits = dict(conn.execute("SELECT split, COUNT(*) FROM touch_model_scores GROUP BY split").fetchall())
            conn.close()
            self.assertEqual(count, 20)
            self.assertEqual(splits, {"test": 10, "train": 10})

    def test_walk_forward_split_uses_time_order_not_random_shuffle(self) -> None:
        rows = [
            _row(f"p{i}", event_time_ms=1000 + i, resolves_yes=0 if i < 5 else 1)
            for i in reversed(range(10))
        ]
        train, test = walk_forward_split(rows, train_fraction=0.6)
        self.assertEqual([row.probe_id for row in train], ["p0", "p1", "p2", "p3", "p4", "p5"])
        self.assertEqual([row.probe_id for row in test], ["p6", "p7", "p8", "p9"])
        self.assertLess(max(row.event_time_ms for row in train), min(row.event_time_ms for row in test))

    def test_policy_ev_matches_closed_form_realized_pnl(self) -> None:
        candidate = PolicyCandidate(
            probe_id="p1",
            market_id="m1",
            touch_time_iso="2026-04-26T00:00:00Z",
            split="test",
            resolves_yes=1,
            market_price=0.50,
            model_prob=0.70,
            fee_rate_bps=100.0,
            would_be_cost_after_min_shares=5.0,
        )
        decisions = evaluate_candidates(
            [candidate],
            stake_usdc=10.0,
            edge_buffer_bps=0.0,
            min_size_policy="upscale",
            max_orders_per_market_per_run=1,
            daily_loss_cap_usdc=30.0,
            min_survived_ms=0.0,
        )
        expected = (10.0 / 0.50) - 10.0 - (10.0 * 0.01)
        self.assertEqual(decisions[0].action, "accept")
        self.assertAlmostEqual(decisions[0].net_pnl_usdc, expected)
        self.assertAlmostEqual(realized_net_pnl(candidate, 10.0), expected)

    def test_policy_applies_fee_min_size_and_daily_loss_cap(self) -> None:
        candidates = [
            PolicyCandidate(
                probe_id="loss1",
                market_id="m1",
                touch_time_iso="2026-04-26T00:00:00Z",
                split="test",
                resolves_yes=0,
                market_price=0.50,
                model_prob=0.95,
                fee_rate_bps=0.0,
                would_be_cost_after_min_shares=1.0,
            ),
            PolicyCandidate(
                probe_id="loss2",
                market_id="m2",
                touch_time_iso="2026-04-26T00:01:00Z",
                split="test",
                resolves_yes=0,
                market_price=0.50,
                model_prob=0.95,
                fee_rate_bps=0.0,
                would_be_cost_after_min_shares=1.0,
            ),
            PolicyCandidate(
                probe_id="minskip",
                market_id="m3",
                touch_time_iso="2026-04-27T00:00:00Z",
                split="test",
                resolves_yes=1,
                market_price=0.50,
                model_prob=0.95,
                fee_rate_bps=0.0,
                would_be_cost_after_min_shares=5.0,
            ),
        ]
        decisions = evaluate_candidates(
            candidates,
            stake_usdc=1.0,
            edge_buffer_bps=0.0,
            min_size_policy="skip",
            max_orders_per_market_per_run=1,
            daily_loss_cap_usdc=0.5,
            min_survived_ms=0.0,
        )
        self.assertEqual(decisions[0].action, "accept")
        self.assertEqual(decisions[1].reason, "daily_loss_cap_reached")
        self.assertEqual(decisions[2].reason, "below_min_order_shares")


def _write_training_dbs(touch_db: Path, spot_db: Path) -> None:
    touch = sqlite3.connect(touch_db)
    touch.execute(
        """
        CREATE TABLE touch_dataset (
            probe_id TEXT PRIMARY KEY,
            touch_time_iso TEXT NOT NULL,
            market_id TEXT,
            asset_slug TEXT,
            direction TEXT,
            resolves_yes INTEGER,
            best_bid_at_touch REAL,
            best_ask_at_touch REAL,
            ask_level_1_size REAL,
            ask_level_2_size REAL,
            ask_level_3_size REAL,
            ask_level_4_size REAL,
            ask_level_5_size REAL,
            ask_size_at_touch_level REAL,
            touch_level REAL,
            lifecycle_fraction REAL,
            fee_rate_bps REAL,
            would_be_cost_after_min_shares REAL,
            survived_ms REAL
        )
        """
    )
    spot = sqlite3.connect(spot_db)
    spot.execute(
        """
        CREATE TABLE spot_features (
            probe_id TEXT PRIMARY KEY,
            spot_return_since_window_start REAL,
            spot_realized_vol_recent_60s REAL,
            spot_realized_vol_recent_300s REAL,
            spot_velocity_recent_30s REAL,
            seconds_to_resolution REAL,
            spot_implied_prob_minus_market_prob REAL
        )
        """
    )
    for idx in range(20):
        signal = -0.08 if idx % 2 == 0 else 0.08
        label = 1 if signal > 0 else 0
        probe_id = f"p{idx:02d}"
        touch.execute(
            "INSERT INTO touch_dataset VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                probe_id,
                f"2026-04-26T00:{idx:02d}:00Z",
                f"m{idx // 2}",
                "btc" if idx < 14 else "eth",
                "UP/YES" if idx % 3 else "DOWN/NO",
                label,
                0.49,
                0.50,
                10.0 + idx,
                8.0,
                6.0,
                4.0,
                2.0,
                10.0,
                0.55,
                idx / 20.0,
                100.0,
                5.0,
                1000.0,
            ),
        )
        spot.execute(
            "INSERT INTO spot_features VALUES (?, ?, ?, ?, ?, ?, ?)",
            (probe_id, signal, 0.01, 0.02, signal / 30.0, 600.0, signal),
        )
    touch.commit()
    touch.close()
    spot.commit()
    spot.close()


def _row(probe_id: str, *, event_time_ms: int, resolves_yes: int) -> TouchModelRow:
    return TouchModelRow(
        probe_id=probe_id,
        market_id="m",
        touch_time_iso="2026-04-26T00:00:00Z",
        event_time_ms=event_time_ms,
        asset_slug="btc",
        direction="UP/YES",
        resolves_yes=resolves_yes,
        market_price=0.5,
        fee_rate_bps=0.0,
        would_be_cost_after_min_shares=5.0,
        survived_ms=1000.0,
        features={},
    )


if __name__ == "__main__":
    unittest.main()
