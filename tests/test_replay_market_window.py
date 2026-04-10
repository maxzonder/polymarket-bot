from __future__ import annotations

import unittest

from replay.honest_replay import slice_rows_by_market


class ReplayMarketWindowTests(unittest.TestCase):
    def test_slice_rows_by_market_skips_closed_and_preserves_whole_markets(self) -> None:
        rows = [
            {"market_id": "m0", "token_id": "m0_yes", "end_date": 100, "outcome_name": "Yes"},
            {"market_id": "m0", "token_id": "m0_no", "end_date": 100, "outcome_name": "No"},
            {"market_id": "m1", "token_id": "m1_yes", "end_date": 101, "outcome_name": "Yes"},
            {"market_id": "m1", "token_id": "m1_no", "end_date": 101, "outcome_name": "No"},
            {"market_id": "m2", "token_id": "m2_yes", "end_date": 102, "outcome_name": "Yes"},
            {"market_id": "m2", "token_id": "m2_no", "end_date": 102, "outcome_name": "No"},
        ]

        sliced = slice_rows_by_market(
            rows,
            skip_closed_at_start_ts=100,
            market_offset=1,
            market_limit=1,
        )

        self.assertEqual([row["market_id"] for row in sliced], ["m2", "m2"])
        self.assertEqual([row["token_id"] for row in sliced], ["m2_yes", "m2_no"])


if __name__ == "__main__":
    unittest.main()
