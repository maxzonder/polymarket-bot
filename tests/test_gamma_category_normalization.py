import unittest

from api.gamma_client import _parse_market
from market_classifier import infer_market_category


class GammaCategoryNormalizationTests(unittest.TestCase):
    def test_weather_question_overrides_wrong_raw_category(self) -> None:
        self.assertEqual(
            infer_market_category({
                "category": "politics",
                "question": "Will the highest temperature in Buenos Aires be 15°C on May 14?",
            }),
            "weather",
        )

    def test_eurovision_question_overrides_wrong_raw_category(self) -> None:
        self.assertEqual(
            infer_market_category({
                "category": "crypto",
                "question": "Will Lithuania be in the top 3 at Eurovision 2026?",
            }),
            "entertainment",
        )

    def test_crypto_price_question_remains_crypto(self) -> None:
        self.assertEqual(
            infer_market_category({
                "question": "Will the price of Ethereum be above $2,600 on May 14?",
            }),
            "crypto",
        )

    def test_falls_back_to_tag_when_no_question_override(self) -> None:
        self.assertEqual(
            infer_market_category({
                "question": "Will candidate X win?",
                "tags": [{"slug": "politics", "label": "Politics"}],
            }),
            "politics",
        )

    def test_parse_market_applies_question_override(self) -> None:
        market = _parse_market(
            {
                "id": "m1",
                "conditionId": "c1",
                "active": True,
                "closed": False,
                "archived": False,
                "question": "Will the highest temperature in Miami be between 84-85°F on May 15?",
                "category": "politics",
                "clobTokenIds": '["yes", "no"]',
                "outcomes": '["Yes", "No"]',
                "volume": "100",
                "liquidity": "50",
                "bestAsk": "0.05",
                "bestBid": "0.04",
                "lastTradePrice": "0.05",
                "endDate": "2026-05-15T12:00:00Z",
                "events": [],
                "tags": [{"slug": "politics", "label": "Politics"}],
            },
            now_ts=0,
        )
        self.assertIsNotNone(market)
        self.assertEqual(market.category, "weather")


if __name__ == "__main__":
    unittest.main()
