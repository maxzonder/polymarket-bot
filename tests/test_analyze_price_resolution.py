from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.analyze_price_resolution import (
    TokenMeta,
    TradePoint,
    _resolve_time_bucket,
    build_price_resolution_research,
    build_touch_events_for_token,
)


class AnalyzePriceResolutionTests(unittest.TestCase):
    def test_resolve_time_bucket_supports_standard_short_and_relative_modes(self) -> None:
        self.assertEqual(_resolve_time_bucket(50, 5.0 / 60.0, "standard"), "lt_1m")
        self.assertEqual(_resolve_time_bucket(50, 5.0 / 60.0, "short"), "30s_1m")
        self.assertEqual(_resolve_time_bucket(50, 5.0 / 60.0, "relative"), "0_20pct")
        self.assertEqual(_resolve_time_bucket(170, 5.0 / 60.0, "relative"), "40_60pct")

    def test_build_touch_events_detects_ascending_and_descending(self) -> None:
        meta = TokenMeta(
            token_id="tok1",
            market_id="m1",
            is_winner=1,
            token_order=0,
            category="politics",
            neg_risk=0,
            duration_hours=1.0,
            closed_time=1000,
        )
        trades = [
            TradePoint(timestamp=0, market_id="m1", token_id="tok1", price=0.10, size=10.0, source_file_id=1, seq=0),
            TradePoint(timestamp=10, market_id="m1", token_id="tok1", price=0.30, size=10.0, source_file_id=1, seq=1),
            TradePoint(timestamp=20, market_id="m1", token_id="tok1", price=0.82, size=10.0, source_file_id=1, seq=2),
            TradePoint(timestamp=30, market_id="m1", token_id="tok1", price=0.95, size=10.0, source_file_id=1, seq=3),
            TradePoint(timestamp=40, market_id="m1", token_id="tok1", price=0.75, size=10.0, source_file_id=1, seq=4),
            TradePoint(timestamp=50, market_id="m1", token_id="tok1", price=0.60, size=10.0, source_file_id=1, seq=5),
        ]

        events = build_touch_events_for_token(
            meta=meta,
            trades=trades,
            price_levels=(0.8, 0.9),
            direction_gap=0.10,
            direction_lookback_sec=300,
            touch_volume_lookback_sec=60,
        )

        keyed = {(event.price_level, event.touch_direction): event for event in events}
        self.assertIn((0.8, "ascending"), keyed)
        self.assertIn((0.9, "ascending"), keyed)
        self.assertIn((0.8, "descending"), keyed)
        self.assertNotIn((0.9, "descending"), keyed)

        asc_08 = keyed[(0.8, "ascending")]
        self.assertEqual(asc_08.first_touch_ts, 20)
        self.assertAlmostEqual(asc_08.max_price_after_touch, 0.95)
        self.assertEqual(asc_08.time_to_close_sec, 980)

        desc_08 = keyed[(0.8, "descending")]
        self.assertEqual(desc_08.first_touch_ts, 40)
        self.assertAlmostEqual(desc_08.min_price_after_touch, 0.60)

    def test_build_price_resolution_research_materializes_core_tables(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset_db = root / "polymarket_dataset.db"
            tape_db = root / "historical_tape.db"
            output_db = root / "price_resolution_research.db"

            self._create_dataset_db(dataset_db)
            self._create_tape_db(tape_db)

            stats = build_price_resolution_research(
                dataset_db_path=dataset_db,
                tape_db_path=tape_db,
                output_db_path=output_db,
                price_step=0.1,
                reaction_windows=(60, 300),
                direction_gap=0.1,
                direction_lookback_sec=300,
                touch_volume_lookback_sec=60,
                min_touch_volume_usdc=0.0,
                min_duration_hours=0.0,
                max_duration_hours=0.0,
            )

            self.assertEqual(stats["resolution_events"], 16)
            self.assertGreater(stats["heatmap_rows"], 0)
            self.assertGreater(stats["transition_rows"], 0)
            self.assertGreater(stats["regret_rows"], 0)
            self.assertGreater(stats["diagnostic_rows"], 0)

            conn = sqlite3.connect(output_db)
            conn.row_factory = sqlite3.Row
            try:
                heatmap_row = conn.execute(
                    """
                    SELECT *
                    FROM price_resolution_heatmap
                    WHERE reaction_window_sec=60
                      AND price_level=0.8
                      AND touch_direction='ascending'
                      AND time_to_close_bucket='5m_15m'
                      AND market_type='all'
                      AND token_side='all'
                    """
                ).fetchone()
                self.assertIsNotNone(heatmap_row)
                self.assertEqual(int(heatmap_row["n_tokens"]), 2)
                self.assertEqual(int(heatmap_row["winner_count"]), 1)
                self.assertAlmostEqual(float(heatmap_row["win_rate"]), 0.5)
                self.assertAlmostEqual(float(heatmap_row["avg_touch_price"]), 0.815)
                self.assertAlmostEqual(float(heatmap_row["avg_gross_edge"]), -0.315)

                transition_row = conn.execute(
                    """
                    SELECT *
                    FROM price_level_transition_matrix
                    WHERE reaction_window_sec=60
                      AND from_price_level=0.8
                      AND to_price_level=0.9
                      AND touch_direction='ascending'
                      AND time_to_close_bucket='5m_15m'
                      AND market_type='all'
                      AND token_side='all'
                    """
                ).fetchone()
                self.assertIsNotNone(transition_row)
                self.assertEqual(int(transition_row["n_tokens"]), 2)
                self.assertEqual(int(transition_row["reached_count"]), 1)
                self.assertAlmostEqual(float(transition_row["reach_rate"]), 0.5)
                self.assertAlmostEqual(float(transition_row["avg_minutes_to_target"]), 10.0 / 60.0)
                self.assertAlmostEqual(float(transition_row["median_minutes_to_target"]), 10.0 / 60.0)

                regret_row = conn.execute(
                    """
                    SELECT *
                    FROM price_level_regret_stats
                    WHERE reaction_window_sec=60
                      AND price_level=0.8
                      AND touch_direction='ascending'
                      AND time_to_close_bucket='5m_15m'
                      AND market_type='all'
                      AND token_side='all'
                    """
                ).fetchone()
                self.assertIsNotNone(regret_row)
                self.assertEqual(int(regret_row["winner_samples"]), 1)
                self.assertAlmostEqual(float(regret_row["avg_max_price_after_touch"]), 1.0)

                diag_winner = conn.execute(
                    """
                    SELECT *
                    FROM price_resolution_touch_diagnostics
                    WHERE reaction_window_sec=60
                      AND price_level=0.8
                      AND touch_direction='ascending'
                      AND time_to_close_bucket='5m_15m'
                      AND market_type='all'
                      AND token_side='all'
                      AND outcome_group='winner'
                    """
                ).fetchone()
                self.assertIsNotNone(diag_winner)
                self.assertEqual(int(diag_winner["samples"]), 1)
                self.assertAlmostEqual(float(diag_winner["avg_touch_price"]), 0.82)

                diag_loser = conn.execute(
                    """
                    SELECT *
                    FROM price_resolution_touch_diagnostics
                    WHERE reaction_window_sec=60
                      AND price_level=0.8
                      AND touch_direction='ascending'
                      AND time_to_close_bucket='5m_15m'
                      AND market_type='all'
                      AND token_side='all'
                      AND outcome_group='loser'
                    """
                ).fetchone()
                self.assertIsNotNone(diag_loser)
                self.assertEqual(int(diag_loser["samples"]), 1)
                self.assertAlmostEqual(float(diag_loser["avg_touch_price"]), 0.81)
            finally:
                conn.close()

    def test_build_price_resolution_research_can_use_short_and_relative_time_buckets(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset_db = root / "polymarket_dataset.db"
            tape_db = root / "historical_tape.db"
            short_output_db = root / "price_resolution_short.db"
            relative_output_db = root / "price_resolution_relative.db"

            self._create_dataset_db(dataset_db)
            self._create_tape_db(tape_db)

            build_price_resolution_research(
                dataset_db_path=dataset_db,
                tape_db_path=tape_db,
                output_db_path=short_output_db,
                price_step=0.1,
                reaction_windows=(60,),
                direction_gap=0.1,
                direction_lookback_sec=300,
                touch_volume_lookback_sec=60,
                min_touch_volume_usdc=0.0,
                min_duration_hours=0.0,
                max_duration_hours=0.0,
                time_bucket_mode="short",
            )

            build_price_resolution_research(
                dataset_db_path=dataset_db,
                tape_db_path=tape_db,
                output_db_path=relative_output_db,
                price_step=0.1,
                reaction_windows=(60,),
                direction_gap=0.1,
                direction_lookback_sec=300,
                touch_volume_lookback_sec=60,
                min_touch_volume_usdc=0.0,
                min_duration_hours=0.0,
                max_duration_hours=0.0,
                time_bucket_mode="relative",
            )

            short_conn = sqlite3.connect(short_output_db)
            short_conn.row_factory = sqlite3.Row
            relative_conn = sqlite3.connect(relative_output_db)
            relative_conn.row_factory = sqlite3.Row
            try:
                short_row = short_conn.execute(
                    """
                    SELECT *
                    FROM price_resolution_heatmap
                    WHERE reaction_window_sec=60
                      AND price_level=0.8
                      AND touch_direction='ascending'
                      AND time_to_close_bucket='5m_15m'
                      AND market_type='all'
                      AND token_side='all'
                    """
                ).fetchone()
                self.assertIsNotNone(short_row)

                relative_row = relative_conn.execute(
                    """
                    SELECT *
                    FROM price_resolution_heatmap
                    WHERE reaction_window_sec=60
                      AND price_level=0.8
                      AND touch_direction='ascending'
                      AND time_to_close_bucket='40_60pct'
                      AND market_type='all'
                      AND token_side='all'
                    """
                ).fetchone()
                self.assertIsNotNone(relative_row)

                meta_rows = relative_conn.execute(
                    "SELECT key, value FROM research_meta WHERE key='time_bucket_mode'"
                ).fetchall()
                self.assertEqual([(row["key"], row["value"]) for row in meta_rows], [("time_bucket_mode", "relative")])
            finally:
                short_conn.close()
                relative_conn.close()

    def _create_dataset_db(self, path: Path) -> None:
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE markets (
                id TEXT PRIMARY KEY,
                category TEXT,
                neg_risk INTEGER,
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
            "INSERT INTO markets(id, category, neg_risk, duration_hours, closed_time) VALUES (?, ?, ?, ?, ?)",
            [
                ("m1", "politics", 0, 1.0, 400),
                ("m2", "politics", 0, 0.25, 400),
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

    def test_duration_filter_can_isolate_exact_15m_markets(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset_db = root / "polymarket_dataset.db"
            tape_db = root / "historical_tape.db"
            output_db = root / "price_resolution_research.db"

            self._create_dataset_db(dataset_db)
            self._create_tape_db(tape_db)

            stats = build_price_resolution_research(
                dataset_db_path=dataset_db,
                tape_db_path=tape_db,
                output_db_path=output_db,
                price_step=0.1,
                reaction_windows=(60,),
                direction_gap=0.1,
                direction_lookback_sec=300,
                touch_volume_lookback_sec=60,
                min_touch_volume_usdc=0.0,
                min_duration_hours=0.25,
                max_duration_hours=0.25,
            )

            self.assertEqual(stats["resolution_events"], 8)

            conn = sqlite3.connect(output_db)
            conn.row_factory = sqlite3.Row
            try:
                token_ids = conn.execute(
                    "SELECT DISTINCT token_id FROM token_price_resolution_events ORDER BY token_id"
                ).fetchall()
                self.assertEqual([row["token_id"] for row in token_ids], ["tokB"])

                meta_rows = conn.execute(
                    "SELECT key, value FROM research_meta WHERE key IN ('min_duration_hours', 'max_duration_hours') ORDER BY key"
                ).fetchall()
                self.assertEqual(
                    [(row["key"], row["value"]) for row in meta_rows],
                    [("max_duration_hours", "0.250000"), ("min_duration_hours", "0.250000")],
                )
            finally:
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
