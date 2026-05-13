import unittest

from api.gamma_client import MarketInfo
from config import BotConfig
from strategy.screener import Screener


class ScreenerTotalDurationGateTests(unittest.TestCase):
    def test_rejects_markets_below_min_total_duration(self) -> None:
        screener = Screener(BotConfig(mode="black_swan_mode"), market_scorer=None, skip_logging=True)
        market = MarketInfo(
            market_id="m-short",
            condition_id="c-short",
            question="Short market?",
            category="crypto",
            token_ids=["yes", "no"],
            outcome_names=["Yes", "No"],
            best_ask=0.01,
            best_bid=0.005,
            last_trade_price=0.01,
            volume_usdc=100.0,
            liquidity_usdc=50.0,
            comment_count=0,
            fees_enabled=False,
            end_date_ts=None,
            hours_to_close=1.0,
            total_duration_hours=1.5,
        )
        log_entries = []

        candidates = screener._evaluate_market(market, log_entries)

        self.assertEqual(candidates, [])
        self.assertEqual(log_entries[-1][8], "rejected_total_duration_min")

    def test_parse_market_computes_total_duration_hours(self) -> None:
        from api.gamma_client import _parse_market

        market = _parse_market(
            {
                "id": "m1",
                "conditionId": "c1",
                "active": True,
                "closed": False,
                "archived": False,
                "question": "Will the price of Ethereum be above $2,600 on May 14?",
                "clobTokenIds": '["yes", "no"]',
                "outcomes": '["Yes", "No"]',
                "volume": "100",
                "liquidity": "50",
                "bestAsk": "0.05",
                "bestBid": "0.04",
                "lastTradePrice": "0.05",
                "startDate": "2026-05-14T10:00:00Z",
                "endDate": "2026-05-14T11:30:00Z",
                "events": [],
                "tags": [],
            },
            now_ts=0,
        )
        self.assertIsNotNone(market)
        self.assertEqual(market.total_duration_hours, 1.5)


if __name__ == "__main__":
    unittest.main()
