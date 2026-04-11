from __future__ import annotations

import math
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from api.gamma_client import MarketInfo
from config import BotConfig
from strategy.market_scorer import MarketScore, MarketScorer
from strategy.screener import Screener


class _DummyScoreObj:
    def __init__(self, score: float = 0.5) -> None:
        self.score = score
        self.p_winner = 0.1
        self.p_20x = 0.05
        self.tail_ev = 1.0


class _DummyCategoryScorer:
    def get(self, _category: str | None) -> _DummyScoreObj:
        return _DummyScoreObj()


class _StubSideAwareMarketScorer:
    is_ready = True

    def score(self, _market: MarketInfo, hours_to_close=None, token_order=None) -> MarketScore:
        if token_order == 0:
            total = 0.8
            tier = "top10"
        else:
            total = 0.1
            tier = "reject"
        return MarketScore(
            market_id="m1",
            liquidity_score=0.5,
            niche_score=0.5,
            time_score=0.5,
            analogy_score=total,
            context_score=0.2,
            total=total,
            tier=tier,
            rationale=f"side={token_order} total={total}",
        )


class MarketScorerSideAwareTests(unittest.TestCase):
    def _build_temp_db(self) -> str:
        fd, path = tempfile.mkstemp(suffix=".db")
        Path(path).unlink(missing_ok=True)
        conn = sqlite3.connect(path)
        try:
            conn.execute(
                """
                CREATE TABLE feature_mart_v1_1 (
                    market_id TEXT,
                    category TEXT,
                    volume_usdc REAL,
                    log_volume REAL,
                    was_swan INTEGER,
                    swan_is_winner INTEGER,
                    label_20x INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE markets (
                    id TEXT PRIMARY KEY,
                    category TEXT,
                    volume REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE tokens (
                    token_id TEXT PRIMARY KEY,
                    market_id TEXT,
                    token_order INTEGER,
                    outcome_name TEXT,
                    is_winner INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE swans_v2 (
                    token_id TEXT,
                    max_traded_x REAL
                )
                """
            )

            for idx in range(20):
                market_id = f"m{idx}"
                conn.execute(
                    "INSERT INTO markets (id, category, volume) VALUES (?, ?, ?)",
                    (market_id, "sports", 50_000.0),
                )
                conn.execute(
                    "INSERT INTO feature_mart_v1_1 (market_id, category, volume_usdc, log_volume, was_swan, swan_is_winner, label_20x) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (market_id, "sports", 50_000.0, math.log1p(50_000.0), 1, 1, 1 if idx < 10 else 0),
                )
                conn.execute(
                    "INSERT INTO tokens (token_id, market_id, token_order, outcome_name, is_winner) VALUES (?, ?, ?, ?, ?)",
                    (f"yes_{idx}", market_id, 0, "Yes", 1),
                )
                conn.execute(
                    "INSERT INTO tokens (token_id, market_id, token_order, outcome_name, is_winner) VALUES (?, ?, ?, ?, ?)",
                    (f"no_{idx}", market_id, 1, "No", 0),
                )
                conn.execute(
                    "INSERT INTO swans_v2 (token_id, max_traded_x) VALUES (?, ?)",
                    (f"yes_{idx}", 25.0),
                )
            conn.commit()
        finally:
            conn.close()
        return path

    def test_market_scorer_uses_side_aware_analogy_when_available(self) -> None:
        db_path = self._build_temp_db()
        self.addCleanup(lambda: Path(db_path).unlink(missing_ok=True))
        scorer = MarketScorer(db_path=db_path, min_score=0.0)

        yes_score = scorer.score_from_db(
            market_id="live_m",
            volume=50_000.0,
            comment_count=0,
            hours_to_close=24.0,
            category="sports",
            token_order=0,
        )
        no_score = scorer.score_from_db(
            market_id="live_m",
            volume=50_000.0,
            comment_count=0,
            hours_to_close=24.0,
            category="sports",
            token_order=1,
        )
        fallback_score = scorer.score_from_db(
            market_id="live_m",
            volume=50_000.0,
            comment_count=0,
            hours_to_close=24.0,
            category="sports",
            token_order=2,
        )

        self.assertTrue(scorer.is_ready)
        self.assertAlmostEqual(yes_score.analogy_score, 1.0, places=4)
        self.assertAlmostEqual(no_score.analogy_score, 0.0, places=4)
        self.assertAlmostEqual(fallback_score.analogy_score, 0.5, places=4)
        self.assertGreater(yes_score.total, no_score.total)

    def test_screener_can_keep_yes_and_reject_no_by_market_score(self) -> None:
        base_cfg = BotConfig()
        cfg = SimpleNamespace(
            mode_config=replace(base_cfg.mode_config, entry_price_max=0.95),
            dead_market_hours=base_cfg.dead_market_hours,
            category_weights=base_cfg.category_weights,
        )
        screener = Screener(
            config=cfg,
            entry_fill_scorer=_DummyCategoryScorer(),
            resolution_scorer=_DummyCategoryScorer(),
            market_scorer=_StubSideAwareMarketScorer(),
            skip_logging=True,
        )
        market = MarketInfo(
            market_id="m1",
            condition_id="c1",
            question="Will something happen?",
            category="sports",
            token_ids=["yes_tok", "no_tok"],
            outcome_names=["Yes", "No"],
            best_ask=0.1,
            best_bid=0.09,
            last_trade_price=0.1,
            volume_usdc=50_000.0,
            liquidity_usdc=10_000.0,
            comment_count=0,
            fees_enabled=False,
            end_date_ts=None,
            hours_to_close=24.0,
            neg_risk=False,
            neg_risk_group_id=None,
        )
        log_entries: list[tuple] = []

        with patch("strategy.screener.get_last_trade_ts", return_value=None), patch(
            "strategy.screener.get_orderbook"
        ):
            candidates = screener._evaluate_market(market, log_entries)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].token_id, "yes_tok")
        self.assertEqual(candidates[0].outcome_name, "Yes")
        rejected_no = [row for row in log_entries if row[2] == "no_tok" and row[10] == "rejected_market_score"]
        self.assertTrue(rejected_no)


if __name__ == "__main__":
    unittest.main()
