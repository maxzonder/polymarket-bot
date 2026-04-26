from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.analyze_maker_fees_and_adverse_selection import (
    analyze_adverse_selection,
    bootstrap_ci_bernoulli,
    infer_fee_observation,
)


class MakerFeesAndAdverseSelectionTests(unittest.TestCase):
    def test_infers_taker_only_crypto_fee_schedule(self) -> None:
        observation = infer_fee_observation(
            {
                "id": "m1",
                "conditionId": "cond1",
                "question": "Bitcoin Up or Down - April 26, 7:00PM-7:15PM ET",
                "feesEnabled": True,
                "makerBaseFee": 1000,
                "takerBaseFee": 1000,
                "makerRebatesFeeShareBps": 10000,
                "feeType": "crypto_fees_v2",
                "feeSchedule": {"exponent": 1, "rate": 0.072, "takerOnly": True, "rebateRate": 0.2},
            },
            {"condition_id": "cond1", "maker_base_fee": 1000, "taker_base_fee": 1000},
        )

        self.assertEqual(observation.inferred_taker_fee_rate, 0.072)
        self.assertEqual(observation.inferred_maker_fee_rate, 0.0)
        self.assertEqual(observation.inferred_maker_rebate_rate, 0.2)
        self.assertTrue(observation.gamma_taker_only)

    def test_bootstrap_ci_handles_empty_and_full_samples(self) -> None:
        self.assertEqual(bootstrap_ci_bernoulli(0, 0), (None, None))
        low, high = bootstrap_ci_bernoulli(10, 10, iterations=100, seed=1)
        self.assertEqual(low, 1.0)
        self.assertEqual(high, 1.0)

    def test_adverse_selection_joins_historical_tape_and_touch_baseline(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset_db = root / "dataset.db"
            tape_db = root / "tape.db"
            touch_db = root / "touch.db"
            self._create_dataset_db(dataset_db)
            self._create_tape_db(tape_db)
            self._create_touch_db(touch_db)

            result = analyze_adverse_selection(dataset_db=dataset_db, tape_db=tape_db, touch_db=touch_db)

            self.assertEqual(result.historical_a2_all.n, 3)
            self.assertEqual(result.historical_a2_all.wins, 2)
            self.assertEqual(result.historical_a2_sell.n, 2)
            self.assertEqual(result.historical_a2_sell.wins, 1)
            self.assertEqual(result.touch_a2.n, 2)
            self.assertEqual(result.touch_a2.wins, 1)
            self.assertEqual([(row["level"], row["n"]) for row in result.historical_by_level], [(0.55, 1), (0.65, 1), (0.7, 1)])

    def _create_dataset_db(self, path: Path) -> None:
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE markets (
                id TEXT PRIMARY KEY,
                question TEXT,
                event_start_time INTEGER,
                end_date INTEGER,
                duration_hours REAL
            );
            CREATE TABLE tokens (
                token_id TEXT PRIMARY KEY,
                market_id TEXT,
                outcome_name TEXT,
                is_winner INTEGER,
                token_order INTEGER
            );
            INSERT INTO markets VALUES
                ('m1', 'Bitcoin Up or Down - April 1, 7:30PM-7:45PM ET', 1000, 1900, 0.25),
                ('m2', 'Ethereum Up or Down - April 1, 7:30PM-7:45PM ET', 1000, 1900, 0.25),
                ('m3', 'Random non-crypto market', 1000, 1900, 0.25);
            INSERT INTO tokens VALUES
                ('m1_down', 'm1', 'Down', 1, 1),
                ('m1_up', 'm1', 'Up', 0, 0),
                ('m2_down', 'm2', 'Down', 0, 1),
                ('m3_down', 'm3', 'Down', 1, 1);
            """
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
            ) WITHOUT ROWID;
            CREATE INDEX idx_tape_token_ts ON tape (token_id, timestamp);
            INSERT INTO tape VALUES
                (1, 1, 1540, 'm1', 'm1_down', 0.55, 10.0, 'SELL'),
                (1, 2, 1600, 'm1', 'm1_down', 0.65, 10.0, 'BUY'),
                (2, 1, 1700, 'm2', 'm2_down', 0.70, 10.0, 'SELL'),
                (2, 2, 1200, 'm2', 'm2_down', 0.55, 10.0, 'SELL'),
                (3, 1, 1700, 'm3', 'm3_down', 0.55, 10.0, 'SELL');
            """
        )
        conn.commit()
        conn.close()

    def _create_touch_db(self, path: Path) -> None:
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE touch_dataset (
                asset_slug TEXT,
                direction TEXT,
                lifecycle_fraction REAL,
                touch_level REAL,
                resolves_yes INTEGER
            );
            INSERT INTO touch_dataset VALUES
                ('btc', 'DOWN/NO', 0.70, 0.55, 1),
                ('eth', 'DOWN/NO', 0.80, 0.65, 0),
                ('xrp', 'UP/YES', 0.80, 0.70, 1),
                ('bnb', 'DOWN/NO', 0.80, 0.70, 1);
            """
        )
        conn.commit()
        conn.close()


if __name__ == "__main__":
    unittest.main()
