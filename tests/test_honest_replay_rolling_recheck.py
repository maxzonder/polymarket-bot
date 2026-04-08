from __future__ import annotations

import json
import sqlite3
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

import config as config_module
import execution.order_manager as om_module
from api.clob_client import ClobClient
from config import BotConfig
from execution.order_manager import OrderManager
from scripts.run_honest_replay import simulate_token
from strategy.risk_manager import RiskManager


class HonestReplayRollingRecheckTests(unittest.TestCase):
    def test_token_can_reenter_scope_on_later_poll(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            positions_db = tmp_path / "positions.db"
            paper_db = tmp_path / "paper.db"
            trades_path = tmp_path / "tok1.json"

            start_ts = 1_700_000_000
            trades = [
                {"timestamp": start_ts, "price": 0.30, "size": 100.0, "side": "BUY"},
                {"timestamp": start_ts + 301, "price": 0.10, "size": 500.0, "side": "SELL"},
            ]
            trades_path.write_text(json.dumps(trades), encoding="utf-8")

            original_mode = config_module.MODES["big_swan_mode"]
            config_module.MODES["big_swan_mode"] = replace(
                original_mode,
                entry_price_levels=(0.12,),
                entry_price_max=0.20,
                tp_levels=(),
                moonbag_fraction=1.0,
            )
            try:
                cfg = BotConfig(
                    mode="big_swan_mode",
                    dry_run=True,
                    private_key="fake",
                    paper_initial_balance_usdc=1000.0,
                )
                cfg.screener_interval = 300
                mc = cfg.mode_config
                clob = ClobClient(private_key="fake", dry_run=True, paper_db_path=paper_db)
                risk = RiskManager(mc, balance_usdc=cfg.paper_initial_balance_usdc)
                om_module.POSITIONS_DB = positions_db
                om = OrderManager(cfg, clob, risk)

                row = {
                    "token_id": "tok1",
                    "market_id": "mkt1",
                    "question": "Rolling re-check market?",
                    "outcome_name": "Yes",
                    "end_date": start_ts + 86_400,
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
                    start_ts=start_ts,
                    end_ts=start_ts + 10_000,
                    dead_market_hours=48.0,
                    trade_index={("mkt1", "tok1"): str(trades_path)},
                    activation_delay=0,
                    fill_fraction=1.0,
                    group_boost=1.0,
                    screener_interval=cfg.screener_interval,
                )

                self.assertEqual(result["status"], "ok")
                self.assertEqual(result["accepted_recheck_ts"], start_ts + 600)
                self.assertEqual(result["recheck_count"], 2)

                conn = sqlite3.connect(positions_db)
                conn.row_factory = sqlite3.Row
                pos = conn.execute(
                    "SELECT entry_price, entry_size_usdc, token_quantity FROM positions LIMIT 1"
                ).fetchone()
                conn.close()

                self.assertIsNotNone(pos)
                self.assertAlmostEqual(float(pos["entry_price"]), 0.10, places=9)
                self.assertGreater(float(pos["entry_size_usdc"]), 0.0)
                self.assertGreater(float(pos["token_quantity"]), 0.0)
            finally:
                config_module.MODES["big_swan_mode"] = original_mode


if __name__ == "__main__":
    unittest.main()
