import sqlite3
import time
from unittest.mock import patch

from api.clob_client import ClobClient
from config import BotConfig
from execution.order_manager import OrderManager
from strategy.risk_manager import RiskManager


def _make_om(tmp_path):
    positions_db = tmp_path / "positions.db"
    paper_db = tmp_path / "paper.db"
    cfg = BotConfig(mode="big_swan_mode", dry_run=True, private_key="fake")
    clob = ClobClient(private_key="fake", dry_run=True, paper_db_path=paper_db)
    risk = RiskManager(cfg.mode_config)
    with patch("execution.order_manager.POSITIONS_DB", positions_db):
        om = OrderManager(cfg, clob, risk)
    return om, positions_db


def _insert_resting(conn, order_id, token_id, market_id, price, size, filled_quantity=0.0):
    now = int(time.time())
    conn.execute(
        "INSERT INTO resting_orders (order_id, token_id, market_id, side, price, size, status, created_at, expires_at, mode, filled_quantity) "
        "VALUES (?, ?, ?, 'BUY', ?, ?, 'live', ?, 0, 'big_swan_mode', ?)",
        (order_id, token_id, market_id, price, size, now, filled_quantity),
    )


def test_multitier_entry_fills_accumulate_full_cost_basis_and_exposure(tmp_path):
    om, positions_db = _make_om(tmp_path)

    conn = sqlite3.connect(positions_db)
    _insert_resting(conn, "ord_low", "tok1", "mkt1", 0.01, 33.3333, 33.3333)
    _insert_resting(conn, "ord_mid", "tok1", "mkt1", 0.10, 3.33333, 3.33333)
    _insert_resting(conn, "ord_hi", "tok1", "mkt1", 0.15, 2.22222, 2.22222)
    conn.commit()
    conn.close()

    om.on_entry_filled("ord_low", "tok1", "mkt1", 0.01, 33.3333, "Yes")
    om.on_entry_filled("ord_mid", "tok1", "mkt1", 0.10, 3.33333, "Yes")
    om.on_entry_filled("ord_hi", "tok1", "mkt1", 0.15, 2.22222, "Yes")

    conn = sqlite3.connect(positions_db)
    conn.row_factory = sqlite3.Row
    pos = conn.execute(
        "SELECT entry_size_usdc, token_quantity, entry_price FROM positions WHERE token_id='tok1'"
    ).fetchone()
    exp = conn.execute(
        "SELECT total_stake_usdc, total_qty, fill_count, avg_entry_price FROM exposure_v1_1 WHERE token_id='tok1'"
    ).fetchone()
    resting = conn.execute(
        "SELECT order_id, filled_quantity, status FROM resting_orders WHERE token_id='tok1' ORDER BY order_id"
    ).fetchall()
    conn.close()

    assert pos is not None
    assert abs(float(pos["entry_size_usdc"]) - 0.999999) < 1e-6
    assert abs(float(pos["token_quantity"]) - 38.88885) < 1e-6
    assert abs(float(pos["entry_price"]) - (0.999999 / 38.88885)) < 1e-6

    assert exp is not None
    assert abs(float(exp["total_stake_usdc"]) - 0.999999) < 1e-6
    assert abs(float(exp["total_qty"]) - 38.88885) < 1e-6
    assert int(exp["fill_count"]) == 3
    assert abs(float(exp["avg_entry_price"]) - (0.999999 / 38.88885)) < 1e-6

    assert [(r["order_id"], round(float(r["filled_quantity"]), 6), r["status"]) for r in resting] == [
        ("ord_hi", 2.22222, "matched"),
        ("ord_low", 33.3333, "matched"),
        ("ord_mid", 3.33333, "matched"),
    ]
