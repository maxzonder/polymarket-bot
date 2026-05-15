import unittest

from api.gamma_client import MarketInfo
from config import BotConfig
from strategy.screener import Screener


def _sports_market(question: str, slug: str) -> MarketInfo:
    return MarketInfo(
        market_id="m-sports",
        condition_id="c-sports",
        question=question,
        category="sports",
        token_ids=["yes", "no"],
        outcome_names=["Yes", "No"],
        best_ask=0.01,
        best_bid=0.005,
        last_trade_price=0.01,
        volume_usdc=100_000.0,
        liquidity_usdc=10_000.0,
        comment_count=0,
        fees_enabled=False,
        end_date_ts=None,
        hours_to_close=3.0,
        total_duration_hours=6.0,
        slug=slug,
    )


class ScreenerSportsSubtypeGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.screener = Screener(BotConfig(mode="black_swan_mode"), market_scorer=None, skip_logging=True)

    def test_rejects_only_explicit_bottom_tier_sports_subtypes(self) -> None:
        for question, slug, outcome in [
            ("Formula 1: Monaco Grand Prix winner", "f1-monaco-grand-prix-2026", "rejected_sports_subtype_formula_1"),
            ("Valorant: Evil Geniuses vs Team Envy", "ewc-eg-envy-2026", "rejected_sports_subtype_valorant"),
        ]:
            with self.subTest(slug=slug):
                log_entries = []
                candidates = self.screener._evaluate_market(_sports_market(question, slug), log_entries)

                self.assertEqual(candidates, [])
                self.assertEqual(log_entries[-1][8], outcome)

    def test_keeps_non_allowlisted_sports_for_normal_downstream_gates(self) -> None:
        for question, slug in [
            ("Dota 2: Team Spirit vs Natus Vincere (BO3)", "dota2-ts8-navi-2026-05-15"),
            ("UFC Fight Night: Marcin Tybura vs. Tyrell Fortune", "ufc-mar12-tfortu-2026-05-15"),
            ("ICC Cricket World Cup League Two: Scotland vs USA", "crint-sco-usa-2026-05-15"),
            ("NCAA Basketball: Duke vs North Carolina", "cbb-duke-unc-2026-05-15"),
            ("Spanish Segunda: Zaragoza vs Cordoba", "es2-zar-cor-2026-05-15"),
            ("Valencia vs Real Madrid", "valencia-real-madrid-2026-05-15"),
            ("Internazionali BNL d'Italia: Casper Ruud vs Karen Khachanov", "atp-ruud-khachanov-2026-05-15"),
            ("Counter-Strike: Spirit vs G2 (BO3) - PGL Astana Playoffs", "cs2-spirit-g2-2026-05-15"),
        ]:
            with self.subTest(slug=slug):
                log_entries = []
                candidates = self.screener._evaluate_market(_sports_market(question, slug), log_entries)

                self.assertGreater(len(candidates), 0)
                self.assertIn("passed_to_order_manager", [entry[8] for entry in log_entries])


if __name__ == "__main__":
    unittest.main()
