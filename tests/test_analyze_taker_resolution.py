from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.analyze_taker_resolution import (
    build_taker_resolution_research,
    _fee_rate_for_category,
    _taker_entry_fee_per_share,
)


class AnalyzeTakerResolutionTests(unittest.TestCase):
    def test_fee_helpers_follow_v2_mapping(self) -> None:
        self.assertAlmostEqual(_fee_rate_for_category("Crypto", fees_enabled=1, default_fee_rate=0.05), 0.072)
        self.assertAlmostEqual(_fee_rate_for_category("Politics", fees_enabled=1, default_fee_rate=0.05), 0.04)
        self.assertAlmostEqual(_fee_rate_for_category("Unknown", fees_enabled=1, default_fee_rate=0.05), 0.05)
        self.assertAlmostEqual(_fee_rate_for_category("Crypto", fees_enabled=0, default_fee_rate=0.05), 0.0)
        self.assertAlmostEqual(_taker_entry_fee_per_share(0.82, 0.04, fees_enabled=1), 0.005904)
        self.assertAlmostEqual(_taker_entry_fee_per_share(0.82, 0.04, fees_enabled=0), 0.0)

    def test_build_taker_resolution_research_materializes_fee_aware_summary(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset_db = root / "polymarket_dataset.db"
            tape_db = root / "historical_tape.db"
            output_db = root / "taker_resolution_research.db"

            self._create_dataset_db(dataset_db)
            self._create_tape_db(tape_db)

            stats = build_taker_resolution_research(
                dataset_db_path=dataset_db,
                tape_db_path=tape_db,
                output_db_path=output_db,
                price_step=0.1,
                direction_gap=0.1,
                direction_lookback_sec=300,
                touch_volume_lookback_sec=60,
                order_size_shares=100.0,
                min_duration_hours=0.0,
                max_duration_hours=0.0,
                default_fee_rate=0.05,
                min_touch_volume_usdc=0.0,
            )

            self.assertEqual(stats["resolution_events"], 16)
            self.assertGreater(stats["summary_rows"], 0)

            conn = sqlite3.connect(output_db)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute(
                    """
                    SELECT *
                    FROM taker_resolution_summary
                    WHERE price_level=0.8
                      AND touch_direction='ascending'
                      AND time_to_close_bucket='5m_15m'
                      AND market_type='binary'
                      AND token_side='yes'
                      AND category='politics'
                      AND fees_enabled=1
                    """
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(int(row["n_tokens"]), 2)
                self.assertEqual(int(row["winner_count"]), 1)
                self.assertAlmostEqual(float(row["win_rate"]), 0.5)
                self.assertAlmostEqual(float(row["avg_entry_price"]), 0.815)
                self.assertAlmostEqual(float(row["avg_entry_fee_per_share"]), 0.00603, places=5)
                self.assertAlmostEqual(float(row["avg_gross_edge"]), -0.315, places=6)
                self.assertAlmostEqual(float(row["avg_net_edge"]), -0.32103, places=5)
                self.assertAlmostEqual(float(row["avg_net_pnl_per_order"]), -32.103, places=3)
            finally:
                conn.close()

    def test_duration_filter_can_isolate_exact_15m_markets(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset_db = root / "polymarket_dataset.db"
            tape_db = root / "historical_tape.db"
            output_db = root / "taker_resolution_research.db"

            self._create_dataset_db(dataset_db)
            self._create_tape_db(tape_db)

            stats = build_taker_resolution_research(
                dataset_db_path=dataset_db,
                tape_db_path=tape_db,
                output_db_path=output_db,
                price_step=0.1,
                direction_gap=0.1,
                direction_lookback_sec=300,
                touch_volume_lookback_sec=60,
                order_size_shares=100.0,
                min_duration_hours=0.25,
                max_duration_hours=0.25,
                default_fee_rate=0.05,
                min_touch_volume_usdc=0.0,
            )

            self.assertEqual(stats["resolution_events"], 8)

            conn = sqlite3.connect(output_db)
            conn.row_factory = sqlite3.Row
            try:
                token_ids = conn.execute(
                    "SELECT DISTINCT token_id FROM taker_resolution_events ORDER BY token_id"
                ).fetchall()
                self.assertEqual([row["token_id"] for row in token_ids], ["tokB"])
            finally:
                conn.close()

    def test_relative_time_bucket_mode_materializes_relative_buckets(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset_db = root / "polymarket_dataset.db"
            tape_db = root / "historical_tape.db"
            output_db = root / "taker_resolution_research.db"

            self._create_dataset_db(dataset_db)
            self._create_tape_db(tape_db)

            stats = build_taker_resolution_research(
                dataset_db_path=dataset_db,
                tape_db_path=tape_db,
                output_db_path=output_db,
                price_step=0.1,
                direction_gap=0.1,
                direction_lookback_sec=300,
                touch_volume_lookback_sec=60,
                order_size_shares=100.0,
                min_duration_hours=0.25,
                max_duration_hours=0.25,
                default_fee_rate=0.05,
                min_touch_volume_usdc=0.0,
                time_bucket_mode="relative",
            )

            self.assertEqual(stats["resolution_events"], 8)

            conn = sqlite3.connect(output_db)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    """
                    SELECT DISTINCT time_to_close_bucket, time_bucket_mode
                    FROM taker_resolution_summary
                    ORDER BY time_to_close_bucket
                    """
                ).fetchall()
                self.assertEqual(
                    [(row["time_to_close_bucket"], row["time_bucket_mode"]) for row in rows],
                    [("40_60pct", "relative")],
                )
            finally:
                conn.close()

    def _create_dataset_db(self, path: Path) -> None:
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE markets (
                id TEXT PRIMARY KEY,
                category TEXT,
                neg_risk INTEGER,
                fees_enabled INTEGER,
                duration_hours REAL,
                closed_time INTEGER
            );
            CREATE TABLE tokens (
                token_id TEXT PRIMARY KEY,
                market_id TEXT NOT NULL,
                is_winner INTEGER,
                token_order INTEGER
            );
            """
        )
        conn.executemany(
            "INSERT INTO markets(id, category, neg_risk, fees_enabled, duration_hours, closed_time) VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("m1", "politics", 0, 1, 1.0, 400),
                ("m2", "politics", 0, 1, 0.25, 400),
            ],
        )
        conn.executemany(
            "INSERT INTO tokens(token_id, market_id, is_winner, token_order) VALUES (?, ?, ?, ?)",
            [
                ("tokA", "m1", 1, 0),
                ("tokB", "m2", 0, 0),
            ],
        )
        conn.commit()
        conn.close()

    def _create_tape_db(self, path: Path) -> None:
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE tape (
                source_file_id INTEGER NOT NULL,
                seq INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                market_id TEXT NOT NULL,
                token_id TEXT NOT NULL,
                price REAL NOT NULL,
                size REAL NOT NULL,
                side TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (source_file_id, seq)
            );
            CREATE INDEX idx_tape_token_ts ON tape (token_id, timestamp);
            """
        )
        conn.executemany(
            "INSERT INTO tape(source_file_id, seq, timestamp, market_id, token_id, price, size, side) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (1, 0, 0, "m1", "tokA", 0.10, 10.0, "BUY"),
                (1, 1, 10, "m1", "tokA", 0.30, 10.0, "BUY"),
                (1, 2, 20, "m1", "tokA", 0.82, 10.0, "BUY"),
                (1, 3, 30, "m1", "tokA", 0.92, 10.0, "BUY"),
                (1, 4, 40, "m1", "tokA", 1.00, 10.0, "BUY"),
                (2, 0, 0, "m2", "tokB", 0.10, 10.0, "BUY"),
                (2, 1, 10, "m2", "tokB", 0.30, 10.0, "BUY"),
                (2, 2, 20, "m2", "tokB", 0.81, 10.0, "BUY"),
                (2, 3, 30, "m2", "tokB", 0.85, 10.0, "BUY"),
                (2, 4, 40, "m2", "tokB", 0.70, 10.0, "SELL"),
            ],
        )
        conn.commit()
        conn.close()


if __name__ == "__main__":
    unittest.main()
