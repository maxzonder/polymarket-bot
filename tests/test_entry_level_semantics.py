from __future__ import annotations

import json
import sqlite3
import unittest
from dataclasses import replace
from pathlib import Path

import config as config_module
import execution.order_manager as om_module
from api.clob_client import ClobClient
from config import BotConfig
from execution.order_manager import OrderManager
from replay.honest_replay import simulate_token
from strategy.entry_levels import partition_entry_levels, suggested_entry_levels
from strategy.risk_manager import RiskManager


class EntryLevelSemanticsTests(unittest.TestCase):
    def test_suggested_entry_levels_keep_already_cheap_buckets(self) -> None:
        levels = suggested_entry_levels(
            current_price=0.01,
            configured_levels=(0.12, 0.15, 0.20),
            scanner_entry=False,
        )
        self.assertEqual(levels, [0.12, 0.15, 0.20])

        marketable, resting = partition_entry_levels(0.01, levels)
        self.assertEqual(marketable, [0.12, 0.15, 0.20])
        self.assertEqual(resting, [])

    def test_honest_replay_uses_better_trade_price_than_bucket(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            positions_db = tmp_path / "positions.db"
            paper_db = tmp_path / "paper.db"
            trades_path = tmp_path / "tok1.json"

            trades = [
                {"timestamp": 1_700_000_000, "price": 0.19, "size": 100.0, "side": "BUY"},
                {"timestamp": 1_700_000_100, "price": 0.01, "size": 500.0, "side": "SELL"},
            ]
            trades_path.write_text(json.dumps(trades), encoding="utf-8")

            original_mode = config_module.MODES["big_swan_mode"]
            config_module.MODES["big_swan_mode"] = replace(
                original_mode,
                entry_price_levels=(0.12, 0.15),
            )
            try:
                cfg = BotConfig(
                    mode="big_swan_mode",
                    dry_run=True,
                    private_key="fake",
                    paper_initial_balance_usdc=1000.0,
                )
                mc = cfg.mode_config
                clob = ClobClient(private_key="fake", dry_run=True, paper_db_path=paper_db)
                risk = RiskManager(mc, balance_usdc=cfg.paper_initial_balance_usdc)
                om_module.POSITIONS_DB = positions_db
                om = OrderManager(cfg, clob, risk)

                row = {
                    "token_id": "tok1",
                    "market_id": "mkt1",
                    "question": "Replay better fill price?",
                    "outcome_name": "Yes",
                    "end_date": 1_700_100_000,
                    "start_date": 0,
                    "category": "crypto",
                    "volume": 1000.0,
                    "comment_count": 0,
                    "neg_risk": 0,
                    "neg_risk_market_id": None,
                    "is_winner": 0,
                }

                result = simulate_token(
                    row=row,
                    om=om,
                    clob=clob,
                    mc=mc,
                    risk=risk,
                    scorer=None,
                    positions_db_path=str(positions_db),
                    start_ts=1_700_000_000,
                    end_ts=1_700_050_000,
                    dead_market_hours=48.0,
                    trade_index={("mkt1", "tok1"): str(trades_path)},
                    activation_delay=0,
                    fill_fraction=1.0,
                    group_boost=1.0,
                )

                self.assertEqual(result["status"], "ok")

                conn = sqlite3.connect(positions_db)
                conn.row_factory = sqlite3.Row
                pos = conn.execute(
                    "SELECT entry_price, entry_size_usdc, token_quantity FROM positions LIMIT 1"
                ).fetchone()
                conn.close()

                self.assertIsNotNone(pos)
                self.assertAlmostEqual(float(pos["entry_price"]), 0.01, places=9)
                self.assertGreater(float(pos["entry_size_usdc"]), 0.0)
                self.assertGreater(float(pos["token_quantity"]), 0.0)
            finally:
                config_module.MODES["big_swan_mode"] = original_mode


if __name__ == "__main__":
    unittest.main()
