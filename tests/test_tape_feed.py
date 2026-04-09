from __future__ import annotations

import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from replay.tape_feed import (
    discover_trade_files,
    has_valid_tape_db,
    iter_global_tape,
    iter_tape_batches,
    iter_tape_batches_db,
)


class TapeFeedTests(unittest.TestCase):
    def test_global_tape_orders_trades_across_files(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            market_dir = root / "2026-01-14" / "m1_trades"
            market_dir.mkdir(parents=True)
            (market_dir / "tokA.json").write_text(
                json.dumps(
                    [
                        {"timestamp": 100, "price": 0.1, "size": 1, "side": "BUY"},
                        {"timestamp": 130, "price": 0.2, "size": 1, "side": "SELL"},
                    ]
                ),
                encoding="utf-8",
            )
            (market_dir / "tokB.json").write_text(
                json.dumps(
                    [
                        {"timestamp": 110, "price": 0.3, "size": 2, "side": "BUY"},
                        {"timestamp": 120, "price": 0.4, "size": 2, "side": "SELL"},
                    ]
                ),
                encoding="utf-8",
            )

            trades = list(iter_global_tape(start_ts=100, end_ts=130, database_dir=root))
            self.assertEqual([t.timestamp for t in trades], [100, 110, 120, 130])
            self.assertEqual([t.token_id for t in trades], ["tokA", "tokB", "tokB", "tokA"])

    def test_tape_batches_are_time_bucketed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            market_dir = root / "2026-01-14" / "m1_trades"
            market_dir.mkdir(parents=True)
            (market_dir / "tokA.json").write_text(
                json.dumps(
                    [
                        {"timestamp": 100, "price": 0.1, "size": 1, "side": "BUY"},
                        {"timestamp": 301, "price": 0.2, "size": 1, "side": "SELL"},
                    ]
                ),
                encoding="utf-8",
            )

            batches = list(
                iter_tape_batches(
                    batch_seconds=300,
                    start_ts=100,
                    end_ts=699,
                    database_dir=root,
                )
            )
            self.assertEqual(len(batches), 2)
            self.assertEqual((batches[0].batch_start_ts, batches[0].batch_end_ts), (100, 399))
            self.assertEqual([t.timestamp for t in batches[0].trades], [100, 301])
            self.assertEqual((batches[1].batch_start_ts, batches[1].batch_end_ts), (400, 699))
            self.assertEqual(len(batches[1].trades), 0)

    def test_discover_trade_files_reads_market_token_layout(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            market_dir = root / "2026-01-15" / "999_trades"
            market_dir.mkdir(parents=True)
            (market_dir / "abc.json").write_text("[]", encoding="utf-8")

            refs = discover_trade_files(root)
            self.assertEqual(len(refs), 1)
            self.assertEqual(refs[0].market_id, "999")
            self.assertEqual(refs[0].token_id, "abc")

    def test_tape_batches_db_orders_and_filters(self) -> None:
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "historical_tape.db"
            conn = sqlite3.connect(db_path)
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
                CREATE INDEX idx_tape_ts ON tape (timestamp);
                """
            )
            conn.executemany(
                "INSERT INTO tape(source_file_id, seq, timestamp, market_id, token_id, price, size, side) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (1, 0, 100, "m1", "tokA", 0.1, 1.0, "BUY"),
                    (1, 1, 130, "m1", "tokA", 0.2, 1.0, "SELL"),
                    (2, 0, 110, "m1", "tokB", 0.3, 2.0, "BUY"),
                    (2, 1, 120, "m2", "tokC", 0.4, 2.0, "SELL"),
                ],
            )
            conn.commit()
            conn.close()

            batches = list(
                iter_tape_batches_db(
                    batch_seconds=50,
                    start_ts=100,
                    end_ts=150,
                    tape_db_path=db_path,
                    selected_markets={"m1"},
                    selected_tokens={"tokA", "tokB"},
                )
            )
            self.assertEqual(len(batches), 2)
            self.assertEqual([t.timestamp for t in batches[0].trades], [100, 110, 130])
            self.assertEqual([t.token_id for t in batches[0].trades], ["tokA", "tokB", "tokA"])
            self.assertEqual(len(batches[1].trades), 0)

    def test_has_valid_tape_db_rejects_empty_file(self) -> None:
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "historical_tape.db"
            db_path.write_text("", encoding="utf-8")
            self.assertFalse(has_valid_tape_db(db_path))

    def test_has_valid_tape_db_accepts_expected_schema(self) -> None:
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "historical_tape.db"
            conn = sqlite3.connect(db_path)
            conn.execute(
                "CREATE TABLE tape (timestamp INTEGER, market_id TEXT, token_id TEXT, price REAL, size REAL, side TEXT, source_file_id INTEGER, seq INTEGER)"
            )
            conn.commit()
            conn.close()
            self.assertTrue(has_valid_tape_db(db_path))


if __name__ == "__main__":
    unittest.main()
