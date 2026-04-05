"""Regression test: terminal replay cleanup must cancel paper BUY orders too."""

import json
import sqlite3
from pathlib import Path

import execution.order_manager as om_module
from api.clob_client import ClobClient
from config import BotConfig
from execution.order_manager import OrderManager
from scripts.run_honest_replay import simulate_token
from strategy.risk_manager import RiskManager


def test_honest_replay_terminal_cleanup_cancels_paper_buy_orders(tmp_path):
    positions_db = tmp_path / "positions.db"
    paper_db = tmp_path / "paper.db"
    trades_path = tmp_path / "tok1.json"

    # Trade prices stay far above our resting bids, so orders never fill.
    trades = [
        {"timestamp": 1_700_000_000, "price": 0.15, "size": 100.0, "side": "BUY"},
        {"timestamp": 1_700_000_100, "price": 0.15, "size": 100.0, "side": "SELL"},
    ]
    trades_path.write_text(json.dumps(trades), encoding="utf-8")

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
        "question": "Replay cancel sync?",
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

    conn = sqlite3.connect(positions_db)
    cancelled_resting = conn.execute(
        "SELECT COUNT(*) FROM resting_orders WHERE status='cancelled'"
    ).fetchone()[0]
    live_resting = conn.execute(
        "SELECT COUNT(*) FROM resting_orders WHERE status='live'"
    ).fetchone()[0]
    conn.close()

    conn = sqlite3.connect(paper_db)
    cancelled_paper_buys = conn.execute(
        "SELECT COUNT(*) FROM paper_orders WHERE side='BUY' AND status='cancelled'"
    ).fetchone()[0]
    live_paper_buys = conn.execute(
        "SELECT COUNT(*) FROM paper_orders WHERE side='BUY' AND status='live'"
    ).fetchone()[0]
    conn.close()

    assert cancelled_resting > 0
    assert live_resting == 0
    assert cancelled_paper_buys == cancelled_resting
    assert live_paper_buys == 0
