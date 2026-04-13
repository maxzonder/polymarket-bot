from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.analyze_maker_postonly_proxy import build_maker_postonly_proxy_research


class AnalyzeMakerPostonlyProxyTests(unittest.TestCase):
    def test_materializes_candidate_fill_and_summary_tables(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset_db = root / "polymarket_dataset.db"
            tape_db = root / "historical_tape.db"
            output_db = root / "maker_postonly_proxy.db"

            self._create_dataset_db(dataset_db)
            self._create_tape_db(tape_db)

            stats = build_maker_postonly_proxy_research(
                dataset_db_path=dataset_db,
                tape_db_path=tape_db,
                output_db_path=output_db,
                trigger_price_min=0.70,
                trigger_price_max=0.75,
                precursor_max_price=0.70,
                precursor_lookback_sec=900,
                lookback_volume_sec=60,
                min_lookback_volume_usdc=0.0,
                max_time_to_close_sec=1800,
                bid_price=0.80,
                order_size_shares=10.0,
                cancel_after_sec=600,
                rest_delay_sec=0,
                conservative_queue_multiplier=2.0,
            )

            self.assertEqual(stats["candidates"], 3)
            self.assertEqual(stats["fill_rows"], 6)
            self.assertGreater(stats["summary_rows"], 0)

            conn = sqlite3.connect(output_db)
            conn.row_factory = sqlite3.Row
            try:
                candidates = conn.execute("SELECT * FROM maker_entry_candidates ORDER BY token_id").fetchall()
                self.assertEqual(len(candidates), 3)
                self.assertEqual(candidates[0]["token_id"], "tokA")
                self.assertEqual(candidates[0]["time_to_close_bucket"], "15m_60m")
                self.assertEqual(candidates[0]["relative_time_bucket"], "20_30pct")
                self.assertAlmostEqual(float(candidates[0]["remaining_frac"]), 1680 / 7200, places=4)

                optimistic_fill = conn.execute(
                    """
                    SELECT * FROM maker_entry_proxy_fills
                    WHERE token_id='tokA' AND proxy_mode='optimistic'
                    """
                ).fetchone()
                self.assertIsNotNone(optimistic_fill)
                self.assertEqual(optimistic_fill["fill_status"], "filled")
                self.assertEqual(int(optimistic_fill["first_fill_ts"]), 130)
                self.assertAlmostEqual(float(optimistic_fill["pnl_per_share"]), 0.20)

                conservative_fill = conn.execute(
                    """
                    SELECT * FROM maker_entry_proxy_fills
                    WHERE token_id='tokA' AND proxy_mode='conservative'
                    """
                ).fetchone()
                self.assertIsNotNone(conservative_fill)
                self.assertEqual(conservative_fill["fill_status"], "unfilled")
                self.assertAlmostEqual(float(conservative_fill["pnl_per_share"]), 0.0)

                loser_fill = conn.execute(
                    """
                    SELECT * FROM maker_entry_proxy_fills
                    WHERE token_id='tokB' AND proxy_mode='optimistic'
                    """
                ).fetchone()
                self.assertIsNotNone(loser_fill)
                self.assertEqual(loser_fill["fill_status"], "unfilled")

                buy_side_only_flow = conn.execute(
                    """
                    SELECT * FROM maker_entry_proxy_fills
                    WHERE token_id='tokC' AND proxy_mode='optimistic'
                    """
                ).fetchone()
                self.assertIsNotNone(buy_side_only_flow)
                self.assertEqual(buy_side_only_flow["fill_status"], "unfilled")
                self.assertAlmostEqual(float(buy_side_only_flow["cumulative_fillable_size"]), 0.0)

                summary = conn.execute(
                    """
                    SELECT * FROM maker_entry_proxy_summary
                    WHERE proxy_mode='optimistic'
                      AND bid_price=0.8
                      AND time_to_close_bucket='15m_60m'
                      AND relative_time_bucket='20_30pct'
                      AND category='crypto'
                      AND fees_enabled=1
                    """
                ).fetchone()
                self.assertIsNotNone(summary)
                self.assertEqual(int(summary["n_candidates"]), 2)
                self.assertEqual(int(summary["filled_count"]), 1)
                self.assertAlmostEqual(float(summary["avg_ev_per_filled_share"]), 0.20)
                self.assertAlmostEqual(float(summary["avg_ev_per_placed_share"]), 0.10)
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
                ("m1", "crypto", 0, 1, 2.0, 1800),
                ("m2", "politics", 0, 0, 1.0, 1800),
                ("m3", "crypto", 0, 1, 2.0, 1800),
            ],
        )
        conn.executemany(
            "INSERT INTO tokens(token_id, market_id, is_winner, token_order) VALUES (?, ?, ?, ?)",
            [
                ("tokA", "m1", 1, 0),
                ("tokB", "m2", 0, 0),
                ("tokC", "m3", 0, 0),
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
                (1, 0, 0, "m1", "tokA", 0.60, 4.0, "BUY"),
                (1, 1, 60, "m1", "tokA", 0.68, 4.0, "BUY"),
                (1, 2, 120, "m1", "tokA", 0.72, 2.0, "BUY"),
                (1, 3, 130, "m1", "tokA", 0.80, 10.0, "SELL"),
                (1, 4, 150, "m1", "tokA", 0.79, 5.0, "SELL"),
                (2, 0, 0, "m2", "tokB", 0.65, 4.0, "BUY"),
                (2, 1, 60, "m2", "tokB", 0.69, 4.0, "BUY"),
                (2, 2, 120, "m2", "tokB", 0.74, 2.0, "BUY"),
                (2, 3, 220, "m2", "tokB", 0.85, 1.0, "BUY"),
                (3, 0, 0, "m3", "tokC", 0.60, 4.0, "BUY"),
                (3, 1, 60, "m3", "tokC", 0.68, 4.0, "BUY"),
                (3, 2, 120, "m3", "tokC", 0.72, 2.0, "BUY"),
                (3, 3, 130, "m3", "tokC", 0.79, 10.0, "BUY"),
            ],
        )
        conn.commit()
        conn.close()


if __name__ == "__main__":
    unittest.main()
