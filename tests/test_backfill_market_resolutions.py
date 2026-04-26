from __future__ import annotations

import csv
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from backfill_market_resolutions import backfill_market_resolutions, load_market_ids_from_csv, normalize_gamma_market


class BackfillMarketResolutionsTest(unittest.TestCase):
    def test_normalize_gamma_market_extracts_winner_and_schema_fields(self) -> None:
        row = {
            "id": "2031669",
            "question": "Ethereum Up or Down - test",
            "conditionId": "0xabc",
            "clobTokenIds": '["up-token", "down-token"]',
            "outcomes": '["Up", "Down"]',
            "outcomePrices": '["0", "1"]',
            "closedTime": "2026-04-21T09:00:05Z",
            "umaResolutionStatus": "resolved",
        }

        item = normalize_gamma_market("2031669", row)

        self.assertEqual(item.market_id, "2031669")
        self.assertEqual(item.condition_id, "0xabc")
        self.assertEqual(item.token_yes_id, "up-token")
        self.assertEqual(item.token_no_id, "down-token")
        self.assertEqual(item.outcome_token_id, "down-token")
        self.assertEqual(item.outcome_price_yes, 0.0)
        self.assertEqual(item.outcome_price_no, 1.0)

    def test_backfill_is_idempotent_and_adds_new_markets(self) -> None:
        rows = {
            "m1": {
                "id": "m1",
                "conditionId": "c1",
                "question": "Bitcoin Up or Down - test",
                "clobTokenIds": '["yes1", "no1"]',
                "outcomes": '["Up", "Down"]',
                "outcomePrices": '["1", "0"]',
                "closedTime": "2026-04-21T09:00:05Z",
            },
            "m2": {
                "id": "m2",
                "conditionId": "c2",
                "question": "Ethereum Up or Down - test",
                "clobTokenIds": '["yes2", "no2"]',
                "outcomes": '["Up", "Down"]',
                "outcomePrices": '["0", "1"]',
                "closedTime": "2026-04-21T09:15:05Z",
            },
        }
        calls: list[str] = []

        def fetch(market_id: str):
            calls.append(market_id)
            return rows.get(market_id)

        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "market_resolutions.sqlite3"
            first = backfill_market_resolutions(["m1", "m1"], output_path=db, sleep_seconds=0, fetch_market=fetch)
            second = backfill_market_resolutions(["m1", "m2"], output_path=db, sleep_seconds=0, fetch_market=fetch)

            self.assertEqual(first.requested, 1)
            self.assertEqual(first.fetched, 1)
            self.assertEqual(second.already_present, 1)
            self.assertEqual(second.fetched, 1)
            self.assertEqual(calls, ["m1", "m2"])
            conn = sqlite3.connect(db)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM market_resolutions").fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT outcome_token_id FROM market_resolutions WHERE market_id='m2'").fetchone()[0], "no2")
            conn.close()

    def test_load_market_ids_from_csv_dedupes_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "touches.csv"
            with path.open("w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=["market_id", "token_id"])
                writer.writeheader()
                writer.writerow({"market_id": "m1", "token_id": "a"})
                writer.writerow({"market_id": "m1", "token_id": "b"})
                writer.writerow({"market_id": "m2", "token_id": "c"})

            self.assertEqual(load_market_ids_from_csv(path), ["m1", "m2"])


if __name__ == "__main__":
    unittest.main()
