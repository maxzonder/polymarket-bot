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

    def test_fetch_open_markets_tracks_active_market_boundaries(self) -> None:
        rows = [
            {
                "market_id": "m1",
                "question": "Q1",
                "category": "weather",
                "volume": 1000.0,
                "comment_count": 0,
                "end_date": 200,
                "start_date": 100,
                "neg_risk": 0,
                "neg_risk_market_id": None,
                "token_id": "m1_yes",
                "outcome_name": "Yes",
            },
            {
                "market_id": "m2",
                "question": "Q2",
                "category": "weather",
                "volume": 1000.0,
                "comment_count": 0,
                "end_date": 400,
                "start_date": 300,
                "neg_risk": 0,
                "neg_risk_market_id": None,
                "token_id": "m2_yes",
                "outcome_name": "Yes",
            },
        ]
        state = OfflineDryRunState.from_rows(rows)

        state.apply_batch(
            TapeBatch(
                batch_start_ts=100,
                batch_end_ts=100,
                trades=(TapeTrade(100, "m1", "m1_yes", 0.2, 1.0, "BUY", {}),),
            )
        )
        self.assertEqual([m.market_id for m in state.fetch_open_markets()], ["m1"])

        state.apply_batch(
            TapeBatch(
                batch_start_ts=300,
                batch_end_ts=300,
                trades=(TapeTrade(300, "m2", "m2_yes", 0.3, 1.0, "BUY", {}),),
            )
        )
        self.assertEqual([m.market_id for m in state.fetch_open_markets()], ["m2"])

    def test_from_rows_prefers_token_order_over_outcome_name_sort(self) -> None:
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
                "token_id": "tok_second",
                "token_order": 1,
                "outcome_name": "Alpha",
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
                "token_id": "tok_first",
                "token_order": 0,
                "outcome_name": "Zulu",
            },
        ]
        state = OfflineDryRunState.from_rows(rows)
        market = state.markets["m1"]

        self.assertEqual(market.token_ids, ("tok_first", "tok_second"))
        self.assertEqual(market.outcome_names, ("Zulu", "Alpha"))

    def test_fetch_open_markets_skips_large_neg_risk_cohorts_by_full_group_size(self) -> None:
        rows = []
        for idx in range(6):
            market_id = f"m{idx}"
            rows.append(
                {
                    "market_id": market_id,
                    "question": f"Q{idx}",
                    "category": "crypto",
                    "volume": 1000.0,
                    "comment_count": 0,
                    "end_date": 10_000,
                    "start_date": 10,
                    "neg_risk": 1,
                    "neg_risk_market_id": "gid_big",
                    "token_id": f"{market_id}_yes",
                    "outcome_name": "Yes",
                }
            )
        state = OfflineDryRunState.from_rows(rows, max_cohort_size=5)
        state.apply_batch(
            TapeBatch(
                batch_start_ts=100,
                batch_end_ts=399,
                trades=tuple(
                    TapeTrade(100 + idx, f"m{idx}", f"m{idx}_yes", 0.1, 1.0, "BUY", {})
                    for idx in range(6)
                ),
            )
        )

        markets = state.fetch_open_markets(price_max=0.20)
        self.assertEqual(markets, [])

    def test_fetch_open_markets_keeps_small_neg_risk_cohorts(self) -> None:
        rows = []
        for idx in range(5):
            market_id = f"m{idx}"
            rows.append(
                {
                    "market_id": market_id,
                    "question": f"Q{idx}",
                    "category": "crypto",
                    "volume": 1000.0,
                    "comment_count": 0,
                    "end_date": 10_000,
                    "start_date": 10,
                    "neg_risk": 1,
                    "neg_risk_market_id": "gid_ok",
                    "token_id": f"{market_id}_yes",
                    "outcome_name": "Yes",
                }
            )
        state = OfflineDryRunState.from_rows(rows, max_cohort_size=5)
        state.apply_batch(
            TapeBatch(
                batch_start_ts=100,
                batch_end_ts=399,
                trades=tuple(
                    TapeTrade(100 + idx, f"m{idx}", f"m{idx}_yes", 0.1, 1.0, "BUY", {})
                    for idx in range(5)
                ),
            )
        )

        markets = state.fetch_open_markets(price_max=0.20)
        self.assertEqual(len(markets), 5)
        self.assertTrue(all(m.neg_risk_group_id == "gid_ok" for m in markets))

    def test_orderbook_cache_invalidates_only_dirty_token(self) -> None:
        rows = [
            {
                "market_id": "m1",
                "question": "Q1",
                "category": "weather",
                "volume": 1000.0,
                "comment_count": 0,
                "end_date": 1000,
                "start_date": 0,
                "neg_risk": 0,
                "neg_risk_market_id": None,
                "token_id": "yes_tok",
                "outcome_name": "Yes",
            },
            {
                "market_id": "m2",
                "question": "Q2",
                "category": "weather",
                "volume": 1000.0,
                "comment_count": 0,
                "end_date": 1000,
                "start_date": 0,
                "neg_risk": 0,
                "neg_risk_market_id": None,
                "token_id": "other_tok",
                "outcome_name": "Yes",
            },
        ]
        state = OfflineDryRunState.from_rows(rows)
        state.apply_batch(
            TapeBatch(
                batch_start_ts=10,
                batch_end_ts=10,
                trades=(
                    TapeTrade(10, "m1", "yes_tok", 0.2, 1.0, "BUY", {}),
                    TapeTrade(10, "m2", "other_tok", 0.4, 1.0, "BUY", {}),
                ),
            )
        )

        book_a = state.get_orderbook("yes_tok")
        book_b = state.get_orderbook("other_tok")

        state.apply_batch(
            TapeBatch(
                batch_start_ts=20,
                batch_end_ts=20,
                trades=(TapeTrade(20, "m1", "yes_tok", 0.3, 1.0, "BUY", {}),),
            )
        )

        self.assertEqual(state.dirty_tokens, {"yes_tok"})
        self.assertEqual(state.dirty_markets, {"m1"})
        self.assertIsNot(state.get_orderbook("yes_tok"), book_a)
        self.assertIs(state.get_orderbook("other_tok"), book_b)


if __name__ == "__main__":
    unittest.main()
