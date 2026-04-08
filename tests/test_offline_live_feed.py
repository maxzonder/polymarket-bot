from __future__ import annotations

import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import execution.order_manager as om_module
from api.clob_client import ClobClient, Orderbook, OrderbookLevel
from api.gamma_client import MarketInfo
from config import BotConfig
from execution.order_manager import OrderManager
from replay.offline_live import HistoricalMarketFeed
from strategy.risk_manager import RiskManager
from strategy.screener import EntryCandidate


class OfflineLiveFeedTests(unittest.TestCase):
    def test_feed_exposes_tick_based_market_snapshot(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            yes_path = tmp_path / "yes.json"
            no_path = tmp_path / "no.json"
            base_ts = 1_700_000_000

            yes_path.write_text(
                json.dumps(
                    [
                        {"timestamp": base_ts, "price": 0.80, "size": 100.0, "side": "BUY"},
                        {"timestamp": base_ts + 300, "price": 0.70, "size": 100.0, "side": "SELL"},
                    ]
                ),
                encoding="utf-8",
            )
            no_path.write_text(
                json.dumps(
                    [
                        {"timestamp": base_ts, "price": 0.20, "size": 100.0, "side": "SELL"},
                        {"timestamp": base_ts + 300, "price": 0.30, "size": 100.0, "side": "BUY"},
                    ]
                ),
                encoding="utf-8",
            )

            rows = [
                {
                    "market_id": "mkt1",
                    "question": "Will something happen?",
                    "category": "crypto",
                    "volume": 1000.0,
                    "end_date": base_ts + 86_400,
                    "start_date": base_ts - 3600,
                    "neg_risk": 0,
                    "neg_risk_market_id": None,
                    "comment_count": 3,
                    "token_id": "yes_tok",
                    "outcome_name": "Yes",
                    "is_winner": 1,
                },
                {
                    "market_id": "mkt1",
                    "question": "Will something happen?",
                    "category": "crypto",
                    "volume": 1000.0,
                    "end_date": base_ts + 86_400,
                    "start_date": base_ts - 3600,
                    "neg_risk": 0,
                    "neg_risk_market_id": None,
                    "comment_count": 3,
                    "token_id": "no_tok",
                    "outcome_name": "No",
                    "is_winner": 0,
                },
            ]
            trade_index = {
                ("mkt1", "yes_tok"): str(yes_path),
                ("mkt1", "no_tok"): str(no_path),
            }

            feed = HistoricalMarketFeed.from_rows(rows, trade_index, trade_cache_size=8)
            feed.set_now(base_ts + 100)

            markets = feed.fetch_open_markets(price_max=0.25)
            self.assertEqual(len(markets), 1)
            market = markets[0]
            self.assertEqual(market.market_id, "mkt1")
            self.assertEqual(market.token_ids, ["yes_tok", "no_tok"])
            self.assertAlmostEqual(float(market.best_ask), 0.80, places=9)
            self.assertAlmostEqual(float(feed.get_orderbook("no_tok").best_ask), 0.20, places=9)
            self.assertEqual(feed.get_last_trade_ts("mkt1"), base_ts)
            self.assertEqual(feed.get_recent_trades("mkt1", limit=1)[0]["timestamp"], base_ts)

    def test_resolve_due_markets_cancels_live_resting_orders(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            positions_db = tmp_path / "positions.db"
            paper_db = tmp_path / "paper.db"
            trades_path = tmp_path / "tok.json"
            base_ts = 1_700_000_000

            trades_path.write_text(
                json.dumps(
                    [
                        {"timestamp": base_ts, "price": 0.10, "size": 100.0, "side": "BUY"},
                        {"timestamp": base_ts + 60, "price": 0.08, "size": 100.0, "side": "SELL"},
                    ]
                ),
                encoding="utf-8",
            )

            rows = [
                {
                    "market_id": "mkt1",
                    "question": "Resolve soon?",
                    "category": "crypto",
                    "volume": 1000.0,
                    "end_date": base_ts + 120,
                    "start_date": base_ts - 3600,
                    "neg_risk": 0,
                    "neg_risk_market_id": None,
                    "comment_count": 0,
                    "token_id": "tok1",
                    "outcome_name": "Yes",
                    "is_winner": 1,
                }
            ]
            trade_index = {("mkt1", "tok1"): str(trades_path)}
            feed = HistoricalMarketFeed.from_rows(rows, trade_index, trade_cache_size=8)

            cfg = BotConfig(
                mode="big_swan_mode",
                dry_run=True,
                private_key="fake",
                paper_initial_balance_usdc=1000.0,
            )
            clob = ClobClient(private_key="fake", dry_run=True, paper_db_path=paper_db)
            risk = RiskManager(cfg.mode_config, balance_usdc=cfg.paper_initial_balance_usdc)
            om_module.POSITIONS_DB = positions_db
            order_manager = OrderManager(cfg, clob, risk)

            candidate = EntryCandidate(
                market_info=MarketInfo(
                    market_id="mkt1",
                    condition_id="mkt1",
                    question="Resolve soon?",
                    category="crypto",
                    token_ids=["tok1"],
                    outcome_names=["Yes"],
                    best_ask=0.10,
                    best_bid=0.09,
                    last_trade_price=0.10,
                    volume_usdc=1000.0,
                    liquidity_usdc=0.0,
                    comment_count=0,
                    fees_enabled=False,
                    end_date_ts=base_ts + 120,
                    hours_to_close=1.0,
                    neg_risk=False,
                    neg_risk_group_id=None,
                ),
                token_id="tok1",
                outcome_name="Yes",
                current_price=0.10,
                entry_fill_score=0.0,
                resolution_score=0.0,
                total_score=1.0,
                suggested_entry_levels=[0.05],
                candidate_id="cand1",
                rationale="test",
                market_score=None,
            )

            synthetic_book = Orderbook(
                token_id="tok1",
                bids=[OrderbookLevel(price=0.095, size=100.0)],
                asks=[OrderbookLevel(price=0.10, size=100.0)],
                best_bid=0.095,
                best_ask=0.10,
            )
            with patch("execution.order_manager.get_orderbook", return_value=synthetic_book):
                results = order_manager.process_candidate(candidate)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].status, "live")

            conn = sqlite3.connect(positions_db)
            status_before = conn.execute(
                "SELECT status FROM resting_orders WHERE order_id=?",
                (results[0].order_id,),
            ).fetchone()[0]
            conn.close()
            self.assertEqual(status_before, "live")

            feed.set_now(base_ts + 180)
            resolved_markets, resolved_tokens = feed.resolve_due_markets(order_manager)
            self.assertEqual(resolved_markets, 1)
            self.assertEqual(resolved_tokens, 1)

            conn = sqlite3.connect(positions_db)
            status_after = conn.execute(
                "SELECT status FROM resting_orders WHERE order_id=?",
                (results[0].order_id,),
            ).fetchone()[0]
            conn.close()
            self.assertEqual(status_after, "cancelled")


if __name__ == "__main__":
    unittest.main()
