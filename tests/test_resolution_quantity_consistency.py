"""Regression test: execution must use actual resolution leg quantity."""

import sqlite3
import time
from unittest.mock import patch

from api.clob_client import ClobClient
from config import BotConfig
from execution.order_manager import OrderManager
from strategy.risk_manager import RiskManager


def test_on_entry_filled_uses_actual_resolution_quantity(tmp_path):
    positions_db = tmp_path / "positions.db"
    paper_db = tmp_path / "paper.db"

    cfg = BotConfig(mode="big_swan_mode", dry_run=True, private_key="fake")
    clob = ClobClient(private_key="fake", dry_run=True, paper_db_path=paper_db)
    risk = RiskManager(cfg.mode_config)

    with patch("execution.order_manager.POSITIONS_DB", positions_db):
        om = OrderManager(cfg, clob, risk)

    now = int(time.time())
    conn = sqlite3.connect(positions_db)
    conn.execute(
        "INSERT INTO resting_orders (order_id, token_id, market_id, side, price, size, status, created_at, expires_at, mode) "
        "VALUES (?, ?, ?, 'BUY', ?, ?, 'live', ?, 0, ?)",
        ("entry1", "tok1", "mkt1", 0.10, 100.0, now, "big_swan_mode"),
    )
    conn.commit()
    conn.close()

    om.on_entry_filled("entry1", "tok1", "mkt1", 0.10, 100.0, "Yes")
    om.on_market_resolved("tok1", True)

    conn = sqlite3.connect(positions_db)
    conn.row_factory = sqlite3.Row
    pos = conn.execute(
        "SELECT moonbag_quantity, realized_pnl FROM positions WHERE token_id='tok1'"
    ).fetchone()
    moonbag = conn.execute(
        "SELECT sell_quantity FROM tp_orders WHERE token_id='tok1' AND label='moonbag_resolution'"
    ).fetchone()
    conn.close()

    assert pos is not None and moonbag is not None
    assert abs(float(pos["moonbag_quantity"]) - 70.0) < 1e-9
    assert abs(float(moonbag["sell_quantity"]) - 70.0) < 1e-9
    assert abs(float(pos["realized_pnl"]) - 90.0) < 1e-9
