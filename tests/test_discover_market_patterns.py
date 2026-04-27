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

from discover_market_patterns import (
    classify_pattern,
    classify_trigger_event,
    discover_market_patterns,
    feature_for_token,
    TokenMeta,
    TradePoint,
)


class DiscoverMarketPatternsTest(unittest.TestCase):
    def test_classifies_basic_prefix_shapes(self) -> None:
        self.assertEqual(
            classify_pattern(
                prefix_return=0.20,
                prefix_range=0.20,
                efficiency=0.90,
                early_return=0.10,
                late_return=0.10,
                max_runup=0.20,
                max_drawdown=0.0,
                crossing_count=3,
                volatility=0.01,
            )[0],
            "monotonic_uptrend",
        )
        self.assertEqual(
            classify_pattern(
                prefix_return=0.01,
                prefix_range=0.16,
                efficiency=0.10,
                early_return=-0.12,
                late_return=0.13,
                max_runup=0.01,
                max_drawdown=0.12,
                crossing_count=2,
                volatility=0.03,
            )[0],
            "dip_then_reversal_up",
        )

    def test_feature_for_token_uses_prefix_and_fee_aware_pnl(self) -> None:
        meta = TokenMeta(
            token_id="yes1",
            market_id="m1",
            question="Bitcoin Up or Down - test",
            asset_slug="btc",
            direction="UP/YES",
            is_winner=1,
            token_order=0,
            category="crypto",
            fees_enabled=1,
            duration_hours=0.25,
            start_ts=1_000,
            closed_ts=1_900,
        )
        trades = [
            TradePoint(1_000, 0.50, 10, "m1", "yes1", 0, 0),
            TradePoint(1_120, 0.52, 10, "m1", "yes1", 0, 1),
            TradePoint(1_240, 0.55, 10, "m1", "yes1", 0, 2),
            TradePoint(1_360, 0.66, 10, "m1", "yes1", 0, 3),
            TradePoint(1_900, 0.99, 10, "m1", "yes1", 0, 4),
        ]
        feature = feature_for_token(meta=meta, trades=trades, grid=(0.0, 0.5, 1.0), decision_fraction=0.40)
        self.assertIsNotNone(feature)
        assert feature is not None
        self.assertEqual(feature.primary_label, "monotonic_uptrend")
        self.assertAlmostEqual(feature.decision_price, 0.66)
        self.assertGreater(feature.gross_pnl_per_share, 0.0)
        self.assertGreater(feature.fee_per_share, 0.0)
        self.assertLess(feature.net_pnl_per_share, feature.gross_pnl_per_share)

    def test_classifies_point_in_time_trigger_events(self) -> None:
        label, tags = classify_trigger_event(
            prefix_return=0.08,
            recent_return=0.07,
            previous_return=0.01,
            acceleration=0.06,
            prefix_efficiency=0.55,
            prefix_range=0.09,
            prefix_volatility=0.01,
            distance_from_prefix_high=0.0,
            distance_from_prefix_low=0.08,
            crossing_count=1,
        )
        self.assertEqual(label, "compression_breakout_up")
        self.assertIn("positive_acceleration", tags)

    def test_discovery_writes_features_aggregates_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset_db = root / "dataset.sqlite3"
            tape_db = root / "tape.sqlite3"
            output_db = root / "patterns.sqlite3"
            report = root / "report.md"
            _write_dataset(dataset_db)
            _write_tape(tape_db)

            summary = discover_market_patterns(
                dataset_db,
                tape_db,
                output_db_path=output_db,
                report_path=report,
                market_duration_minutes=15,
                assets={"btc", "eth"},
                time_grid=(0.0, 0.5, 1.0),
                decision_fraction=0.40,
                trigger_fractions=(0.50,),
                trigger_lookback_fraction=0.40,
                bootstrap_samples=25,
            )

            self.assertEqual(summary.eligible_tokens, 4)
            self.assertEqual(summary.processed_tokens, 4)
            self.assertGreater(summary.processed_triggers, 0)
            self.assertTrue(report.exists())
            text = report.read_text(encoding="utf-8")
            self.assertIn("Pattern discovery report", text)
            self.assertIn("Trigger discovery leaderboard", text)
            self.assertIn("Trading-day / UTC session view", text)
            self.assertIn("BTC vs ETH", text)

            conn = sqlite3.connect(output_db)
            try:
                feature_count = conn.execute("SELECT COUNT(*) FROM pattern_features").fetchone()[0]
                trigger_count = conn.execute("SELECT COUNT(*) FROM trigger_features").fetchone()[0]
                aggregate_count = conn.execute("SELECT COUNT(*) FROM pattern_aggregates").fetchone()[0]
                trigger_aggregate_count = conn.execute("SELECT COUNT(*) FROM trigger_aggregates").fetchone()[0]
                best = conn.execute(
                    "SELECT group_key FROM pattern_aggregates WHERE group_type='pattern' ORDER BY avg_net_pnl_per_share DESC LIMIT 1"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(feature_count, 4)
            self.assertGreater(trigger_count, 0)
            self.assertGreater(aggregate_count, 0)
            self.assertGreater(trigger_aggregate_count, 0)
            self.assertIsInstance(best, str)


def _write_dataset(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE markets (
            id TEXT PRIMARY KEY,
            question TEXT,
            category TEXT,
            fees_enabled INTEGER,
            duration_hours REAL,
            closed_time INTEGER
        );
        CREATE TABLE tokens (
            token_id TEXT PRIMARY KEY,
            market_id TEXT,
            is_winner INTEGER,
            token_order INTEGER
        );
        """
    )
    conn.executemany(
        "INSERT INTO markets VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("m1", "Bitcoin Up or Down - test", "crypto", 1, 0.25, 1_900),
            ("m2", "Ethereum Up or Down - test", "crypto", 1, 0.25, 2_900),
        ],
    )
    conn.executemany(
        "INSERT INTO tokens VALUES (?, ?, ?, ?)",
        [
            ("yes1", "m1", 1, 0),
            ("no1", "m1", 0, 1),
            ("yes2", "m2", 0, 0),
            ("no2", "m2", 1, 1),
        ],
    )
    conn.commit()
    conn.close()


def _write_tape(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE tape (
            source_file_id INTEGER,
            seq INTEGER,
            timestamp INTEGER,
            market_id TEXT,
            token_id TEXT,
            price REAL,
            size REAL
        )
        """
    )
    rows = []
    seq = 0
    for token_id, market_id, start, prices in [
        ("yes1", "m1", 1_000, [0.50, 0.54, 0.60, 0.68, 0.99]),
        ("no1", "m1", 1_000, [0.50, 0.46, 0.40, 0.32, 0.01]),
        ("yes2", "m2", 2_000, [0.52, 0.58, 0.50, 0.48, 0.01]),
        ("no2", "m2", 2_000, [0.48, 0.42, 0.50, 0.52, 0.99]),
    ]:
        for idx, price in enumerate(prices):
            rows.append((0, seq, start + idx * 180, market_id, token_id, price, 10.0))
            seq += 1
    conn.executemany("INSERT INTO tape VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    unittest.main()
