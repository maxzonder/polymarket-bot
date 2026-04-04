"""Regression test: OrderManager exposure manager must use the same patched DB."""

from unittest.mock import MagicMock, patch

from config import BotConfig
from execution.order_manager import OrderManager


def test_order_manager_exposure_manager_uses_patched_positions_db(tmp_path):
    db_path = tmp_path / "positions.db"
    config = BotConfig(
        mode="big_swan_mode",
        dry_run=True,
        private_key="fake_key",
        paper_initial_balance_usdc=1.0,
    )
    clob = MagicMock()
    risk = MagicMock()

    with patch("execution.order_manager.POSITIONS_DB", db_path):
        om = OrderManager(config, clob, risk)

    assert om._db_path == str(db_path)
    assert om.em._db_path == str(db_path)
