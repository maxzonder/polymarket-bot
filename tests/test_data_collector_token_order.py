from __future__ import annotations

import sqlite3
import unittest

from data_collector.data_collector_and_parsing import _init_db, _parse_token_rows


class DataCollectorTokenOrderTests(unittest.TestCase):
    def test_parse_token_rows_persists_gamma_token_order(self) -> None:
        rows = _parse_token_rows(
            {
                "id": "m1",
                "clobTokenIds": '["yes_tok", "no_tok"]',
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["1.0", "0.0"]',
            }
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["token_id"], "yes_tok")
        self.assertEqual(rows[0]["token_order"], 0)
        self.assertEqual(rows[0]["outcome_name"], "Yes")
        self.assertEqual(rows[0]["is_winner"], 1)
        self.assertEqual(rows[1]["token_id"], "no_tok")
        self.assertEqual(rows[1]["token_order"], 1)
        self.assertEqual(rows[1]["outcome_name"], "No")
        self.assertEqual(rows[1]["is_winner"], 0)

    def test_init_db_adds_and_backfills_token_order_for_existing_tokens(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE markets (id TEXT PRIMARY KEY)")
        conn.execute(
            """
            CREATE TABLE tokens (
                token_id TEXT PRIMARY KEY,
                market_id TEXT,
                outcome_name TEXT,
                is_winner INTEGER,
                FOREIGN KEY (market_id) REFERENCES markets(id)
            )
            """
        )
        conn.execute("INSERT INTO markets (id) VALUES ('m1')")
        conn.executemany(
            "INSERT INTO tokens (token_id, market_id, outcome_name, is_winner) VALUES (?, ?, ?, ?)",
            [
                ("yes_tok", "m1", "Yes", 1),
                ("no_tok", "m1", "No", 0),
            ],
        )
        conn.commit()

        _init_db(conn)

        cols = {row[1] for row in conn.execute("PRAGMA table_info(tokens)").fetchall()}
        self.assertIn("token_order", cols)
        rows = conn.execute(
            "SELECT token_id, token_order FROM tokens ORDER BY token_id"
        ).fetchall()
        self.assertEqual(rows, [("no_tok", 1), ("yes_tok", 0)])


if __name__ == "__main__":
    unittest.main()
