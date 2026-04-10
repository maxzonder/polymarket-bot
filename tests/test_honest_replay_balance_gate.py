"""Regression test: honest replay should reuse OrderManager balance gate."""

import json
import sqlite3
from pathlib import Path

import execution.order_manager as om_module
from api.clob_client import ClobClient
from config import BotConfig
from execution.order_manager import OrderManager
from replay.honest_replay import simulate_token
from strategy.risk_manager import RiskManager


def test_honest_replay_reuses_order_manager_balance_gate(tmp_path):
    positions_db = tmp_path / "positions.db"
    paper_db = tmp_path / "paper.db"
    trades_path = tmp_path / "tok1.json"

    trades = [
        {"timestamp": 1_700_000_000, "price": 0.20, "size": 100.0, "side": "BUY"},
        {"timestamp": 1_700_000_100, "price": 0.20, "size": 100.0, "side": "BUY"},
    ]
    trades_path.write_text(json.dumps(trades), encoding="utf-8")

    cfg = BotConfig(
        mode="big_swan_mode",
        dry_run=True,
        private_key="fake",
        paper_initial_balance_usdc=0.06,
    )
    mc = cfg.mode_config
    clob = ClobClient(private_key="fake", dry_run=True, paper_db_path=paper_db)
    risk = RiskManager(mc, balance_usdc=cfg.paper_initial_balance_usdc)
    om_module.POSITIONS_DB = positions_db
    om = OrderManager(cfg, clob, risk)

    row = {
        "token_id": "tok1",
        "market_id": "mkt1",
        "question": "Test market?",
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

    assert result["status"] == "ok"

    conn = sqlite3.connect(paper_db)
    paper_orders = conn.execute("SELECT count(*) FROM paper_orders").fetchone()[0]
    conn.close()

    conn = sqlite3.connect(positions_db)
    conn.row_factory = sqlite3.Row
    bal = conn.execute("SELECT cash_balance FROM paper_balance WHERE id=1").fetchone()[0]
    resting = conn.execute("SELECT count(*) FROM resting_orders").fetchone()[0]
    conn.close()

    assert paper_orders == 1
    assert resting == 1
    assert abs(float(bal) - 0.06) < 1e-9
