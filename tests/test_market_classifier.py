import time
import unittest

from api.gamma_client import _parse_market
from market_classifier import infer_market_category
from scripts.measure_live_depth_and_survival import _extract_category
from data_collector.data_collector_and_parsing import _infer_category


class MarketClassifierTests(unittest.TestCase):
    def test_shared_classifier_recognizes_untagged_sports(self) -> None:
        raw = {
            "id": "m1",
            "question": "Will Justin Rose win the 2026 PGA Championship?",
            "slug": "will-justin-rose-win-the-2026-pga-championship",
        }
        self.assertEqual(infer_market_category(raw), "sports")
        self.assertEqual(_extract_category(raw, None), "sports")
        self.assertEqual(_infer_category(raw), "sports")

    def test_shared_classifier_recognizes_untagged_crypto_and_weather(self) -> None:
        self.assertEqual(
            infer_market_category({"question": "Will Bitcoin dip to $78,000 on May 12?"}),
            "crypto",
        )
        self.assertEqual(
            infer_market_category({"question": "Will the highest temperature in Taipei be 23°C on May 14?"}),
            "weather",
        )

    def test_gamma_client_uses_shared_classifier_for_sports_filters(self) -> None:
        raw = {
            "id": "m1",
            "conditionId": "c1",
            "active": True,
            "closed": False,
            "archived": False,
            "question": "Dota 2: Two Move vs Power Rangers - Game 4 Winner",
            "slug": "dota2-tm6-pr1-2026-05-12-game4",
            "clobTokenIds": '["yes-token", "no-token"]',
            "outcomes": '["Yes", "No"]',
            "bestAsk": "0.05",
            "volumeNum": "1000",
            "liquidityNum": "100",
            "endDate": "2099-01-01T00:00:00Z",
        }
        market = _parse_market(raw, time.time())
        self.assertIsNotNone(market)
        assert market is not None
        self.assertEqual(market.category, "sports")

    def test_tag_mapping_preserves_finance_as_historical_crypto_bucket(self) -> None:
        raw = {
            "question": "Will SPY close above 600?",
            "tags": [{"slug": "stocks", "label": "Stocks"}],
        }
        self.assertEqual(infer_market_category(raw), "crypto")


if __name__ == "__main__":
    unittest.main()
