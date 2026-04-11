import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from analyzer.market_level_features_v1_1 import build
from api.gamma_client import MarketInfo
from config import BotConfig
from strategy.market_scorer import MarketScorer
from strategy.screener import Screener


class _StubCategoryScorer:
    def __init__(self, score: float = 0.5):
        self.score = score
        self.p_winner = 0.5
        self.p_20x = 0.1
        self.tail_ev = 1.0


class _StubScorerRegistry:
    def __init__(self, score: float = 0.5):
        self._obj = _StubCategoryScorer(score)

    def get(self, _category: str):
        return self._obj


class _StubMarketScorer:
    is_ready = True

    def score(self, market: MarketInfo, hours_to_close=None, is_no_token=None):
        if market.neg_risk and is_no_token is False:
            return SimpleNamespace(total=0.9, tier="pass", rationale="yes-pass")
        if market.neg_risk and is_no_token is True:
            return SimpleNamespace(total=0.1, tier="reject", rationale="no-reject")
        return SimpleNamespace(total=0.1, tier="reject", rationale="reject")


class FeatureMartNegRiskYesOnlyTests(unittest.TestCase):
    def test_build_persists_best_swan_is_yes_token(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE markets (
                id TEXT PRIMARY KEY,
                category TEXT,
                volume REAL,
                liquidity REAL,
                duration_hours REAL,
                comment_count INTEGER,
                neg_risk INTEGER,
                closed_time INTEGER,
                neg_risk_market_id TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE tokens (
                token_id TEXT PRIMARY KEY,
                market_id TEXT,
                outcome_name TEXT,
                is_winner INTEGER,
                token_order INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE swans_v2 (
                token_id TEXT,
                buy_min_price REAL,
                max_traded_x REAL,
                buy_volume REAL,
                buy_trade_count INTEGER,
                buy_ts_first INTEGER,
                buy_ts_last INTEGER,
                is_winner INTEGER
            )
            """
        )

        closed_time = 1760000000
        rows = [
            ("m_yes", "weather", 1000.0, 500.0, 24.0, 1, 1, closed_time, "gid-1"),
            ("m_no", "weather", 1000.0, 500.0, 24.0, 1, 1, closed_time, "gid-1"),
        ]
        conn.executemany(
            "INSERT INTO markets (id, category, volume, liquidity, duration_hours, comment_count, neg_risk, closed_time, neg_risk_market_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.executemany(
            "INSERT INTO tokens (token_id, market_id, outcome_name, is_winner, token_order) VALUES (?, ?, ?, ?, ?)",
            [
                ("yes_yes", "m_yes", "Yes", 1, 0),
                ("yes_no", "m_yes", "No", 0, 1),
                ("no_yes", "m_no", "Yes", 0, 0),
                ("no_no", "m_no", "No", 1, 1),
            ],
        )
        conn.executemany(
            "INSERT INTO swans_v2 (token_id, buy_min_price, max_traded_x, buy_volume, buy_trade_count, buy_ts_first, buy_ts_last, is_winner) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("yes_yes", 0.05, 30.0, 100.0, 5, closed_time - 1000, closed_time - 900, 1),
                ("yes_no", 0.05, 10.0, 80.0, 4, closed_time - 800, closed_time - 700, 0),
                ("no_yes", 0.05, 10.0, 80.0, 4, closed_time - 800, closed_time - 700, 0),
                ("no_no", 0.05, 25.0, 90.0, 5, closed_time - 1000, closed_time - 900, 1),
            ],
        )

        build(conn, recompute=True)

        got = dict(
            conn.execute(
                "SELECT market_id, best_swan_is_yes_token FROM feature_mart_v1_1 ORDER BY market_id"
            ).fetchall()
        )
        self.assertEqual(got["m_no"], 0)
        self.assertEqual(got["m_yes"], 1)


class MarketScorerNegRiskYesOnlyTests(unittest.TestCase):
    def test_neg_risk_yes_uses_dedicated_lookup_but_binary_does_not(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "dataset.db"
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE feature_mart_v1_1 (
                    market_id TEXT PRIMARY KEY,
                    category TEXT,
                    volume_usdc REAL,
                    log_volume REAL,
                    was_swan INTEGER,
                    swan_is_winner INTEGER,
                    best_swan_is_yes_token INTEGER,
                    label_20x INTEGER,
                    neg_risk INTEGER
                )
                """
            )
            rows = []
            for i in range(20):
                rows.append((f"bin_{i}", "sports", 20000.0, 1.0, 0, None, None, 0, 0))
            for i in range(20):
                rows.append((f"nr_yes_{i}", "sports", 20000.0, 1.0, 1, 1, 1, 1, 1))
            for i in range(20):
                rows.append((f"nr_no_{i}", "sports", 20000.0, 1.0, 1, 0, 0, 0, 1))
            conn.executemany(
                "INSERT INTO feature_mart_v1_1 (market_id, category, volume_usdc, log_volume, was_swan, swan_is_winner, best_swan_is_yes_token, label_20x, neg_risk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            conn.commit()
            conn.close()

            scorer = MarketScorer(db_path=db_path)
            yes_score = scorer.score_from_db(
                market_id="live_nr_yes",
                volume=20000.0,
                comment_count=0,
                hours_to_close=24.0,
                category="sports",
                neg_risk=True,
                is_no_token=False,
            )
            no_score = scorer.score_from_db(
                market_id="live_nr_no",
                volume=20000.0,
                comment_count=0,
                hours_to_close=24.0,
                category="sports",
                neg_risk=True,
                is_no_token=True,
            )
            binary_score = scorer.score_from_db(
                market_id="live_bin_yes",
                volume=20000.0,
                comment_count=0,
                hours_to_close=24.0,
                category="sports",
                neg_risk=False,
                is_no_token=False,
            )

            self.assertAlmostEqual(yes_score.analogy_score, 1.0, places=4)
            self.assertAlmostEqual(no_score.analogy_score, 1.0 / 3.0, places=4)
            self.assertAlmostEqual(binary_score.analogy_score, 1.0 / 3.0, places=4)

    def test_screener_keeps_neg_risk_yes_and_rejects_neg_risk_no(self) -> None:
        config = BotConfig()

        screener = Screener(
            config=config,
            entry_fill_scorer=_StubScorerRegistry(),
            resolution_scorer=_StubScorerRegistry(),
            market_scorer=_StubMarketScorer(),
            skip_logging=True,
        )
        market = MarketInfo(
            market_id="m1",
            condition_id="c1",
            question="Q?",
            category="sports",
            token_ids=["yes_token", "no_token"],
            outcome_names=["Yes", "No"],
            best_ask=0.05,
            best_bid=0.04,
            last_trade_price=0.05,
            volume_usdc=20000.0,
            liquidity_usdc=10000.0,
            comment_count=0,
            fees_enabled=False,
            end_date_ts=None,
            hours_to_close=24.0,
            neg_risk=True,
            neg_risk_group_id="gid-1",
        )

        with patch("strategy.screener.get_last_trade_ts", return_value=None):
            candidates = screener._evaluate_market(market, log_entries=[])

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].token_id, "yes_token")
        self.assertEqual(candidates[0].outcome_name, "Yes")


if __name__ == "__main__":
    unittest.main()
