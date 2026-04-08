from __future__ import annotations

import unittest

from replay.offline_dryrun import OfflineDryRunState
from replay.tape_feed import TapeBatch, TapeTrade


class OfflineDryRunStateTests(unittest.TestCase):
    def test_apply_batch_updates_prices_and_recent_trades(self) -> None:
        rows = [
            {
                "market_id": "m1",
                "question": "Question?",
                "category": "sports",
                "volume": 1000.0,
                "comment_count": 3,
                "end_date": 1000,
                "start_date": 10,
                "neg_risk": 0,
                "neg_risk_market_id": None,
                "token_id": "yes_tok",
                "outcome_name": "Yes",
            },
            {
                "market_id": "m1",
                "question": "Question?",
                "category": "sports",
                "volume": 1000.0,
                "comment_count": 3,
                "end_date": 1000,
                "start_date": 10,
                "neg_risk": 0,
                "neg_risk_market_id": None,
                "token_id": "no_tok",
                "outcome_name": "No",
            },
        ]
        state = OfflineDryRunState.from_rows(rows)
        batch = TapeBatch(
            batch_start_ts=100,
            batch_end_ts=399,
            trades=(
                TapeTrade(100, "m1", "yes_tok", 0.81, 10.0, "BUY", {}),
                TapeTrade(110, "m1", "no_tok", 0.19, 8.0, "SELL", {}),
            ),
        )

        state.apply_batch(batch)

        self.assertEqual(state.now_ts, 399)
        self.assertAlmostEqual(state.token_price("yes_tok"), 0.81, places=9)
        self.assertEqual(state.get_last_trade_ts("m1"), 110)
        self.assertEqual(len(state.get_recent_trades("m1", limit=10)), 2)

    def test_fetch_open_markets_uses_applied_snapshot(self) -> None:
        rows = [
            {
                "market_id": "m1",
                "question": "Question?",
                "category": "sports",
                "volume": 1000.0,
                "comment_count": 3,
                "end_date": 10_000,
                "start_date": 10,
                "neg_risk": 0,
                "neg_risk_market_id": None,
                "token_id": "yes_tok",
                "outcome_name": "Yes",
            },
            {
                "market_id": "m1",
                "question": "Question?",
                "category": "sports",
                "volume": 1000.0,
                "comment_count": 3,
                "end_date": 10_000,
                "start_date": 10,
                "neg_risk": 0,
                "neg_risk_market_id": None,
                "token_id": "no_tok",
                "outcome_name": "No",
            },
        ]
        state = OfflineDryRunState.from_rows(rows)
        state.apply_batch(
            TapeBatch(
                batch_start_ts=100,
                batch_end_ts=399,
                trades=(
                    TapeTrade(100, "m1", "yes_tok", 0.82, 10.0, "BUY", {}),
                    TapeTrade(120, "m1", "no_tok", 0.18, 11.0, "SELL", {}),
                ),
            )
        )

        markets = state.fetch_open_markets(price_max=0.20)
        self.assertEqual(len(markets), 1)
        market = markets[0]
        self.assertEqual(market.market_id, "m1")
        self.assertEqual(market.token_ids, ["yes_tok", "no_tok"])
        self.assertAlmostEqual(float(market.best_ask), 0.82, places=9)

        ob = state.get_orderbook("no_tok")
        self.assertAlmostEqual(float(ob.best_ask), 0.18, places=9)
        self.assertIsNotNone(ob.best_bid)


if __name__ == "__main__":
    unittest.main()
