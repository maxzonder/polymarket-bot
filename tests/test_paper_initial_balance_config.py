"""Tests for configurable paper initial balance wiring."""

import sqlite3
from unittest.mock import MagicMock, patch

from config import BotConfig
from execution.order_manager import OrderManager


def test_order_manager_seeds_paper_balance_from_config(tmp_path):
    db_path = tmp_path / "positions.db"
    config = BotConfig(
        mode="big_swan_mode",
        dry_run=True,
        private_key="fake_key",
        paper_initial_balance_usdc=321.5,
    )
    clob = MagicMock()
    risk = MagicMock()

    with patch("execution.order_manager.POSITIONS_DB", db_path):
        om = OrderManager(config, clob, risk)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT cash_balance FROM paper_balance WHERE id=1").fetchone()
    event = conn.execute(
        "SELECT delta_usdc, note FROM paper_balance_events ORDER BY id LIMIT 1"
    ).fetchone()
    conn.close()

    assert row is not None
    assert abs(float(row["cash_balance"]) - 321.5) < 1e-9
    assert event is not None
    assert abs(float(event["delta_usdc"]) - 321.5) < 1e-9
    assert event["note"] == "initial balance"
    assert abs(om.get_cash_balance() - 321.5) < 1e-9
