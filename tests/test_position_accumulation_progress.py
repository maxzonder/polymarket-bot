import sqlite3
import time
from pathlib import Path
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
    return om, positions_db, paper_db


def _insert_resting(conn, order_id, token_id, market_id, price, size, filled_quantity=0.0):
    now = int(time.time())
    conn.execute(
        "INSERT INTO resting_orders (order_id, token_id, market_id, side, price, size, status, created_at, expires_at, mode, filled_quantity) "
        "VALUES (?, ?, ?, 'BUY', ?, ?, 'live', ?, 0, 'big_swan_mode', ?)",
        (order_id, token_id, market_id, price, size, now, filled_quantity),
    )


def test_buy_partial_fills_accumulate_into_one_open_position(tmp_path):
    om, positions_db, _ = _make_om(tmp_path)

    conn = sqlite3.connect(positions_db)
    _insert_resting(conn, "ord1", "tok1", "mkt1", 0.10, 100.0, 30.0)
    _insert_resting(conn, "ord2", "tok1", "mkt1", 0.15, 20.0, 10.0)
    conn.commit()
    conn.close()

    om.on_entry_filled("ord1", "tok1", "mkt1", 0.10, 30.0, "Yes")
    om.on_entry_filled("ord2", "tok1", "mkt1", 0.15, 10.0, "Yes")

    conn = sqlite3.connect(positions_db)
    conn.row_factory = sqlite3.Row
    pos = conn.execute(
        "SELECT token_quantity, entry_size_usdc, entry_price, status FROM positions"
    ).fetchall()
    conn.close()

    assert len(pos) == 1
    assert abs(float(pos[0]["token_quantity"]) - 40.0) < 1e-9
    assert abs(float(pos[0]["entry_size_usdc"]) - 4.5) < 1e-9
    assert abs(float(pos[0]["entry_price"]) - 0.1125) < 1e-9
    assert pos[0]["status"] == "open"


def test_tp_activation_gate_blocks_below_25pct_then_activates(tmp_path):
    om, positions_db, _ = _make_om(tmp_path)

    conn = sqlite3.connect(positions_db)
    _insert_resting(conn, "ord1", "tok1", "mkt1", 0.10, 100.0, 24.0)
    conn.commit()
    conn.close()

    om.on_entry_filled("ord1", "tok1", "mkt1", 0.10, 24.0, "Yes")

    conn = sqlite3.connect(positions_db)
    live_before = conn.execute(
        "SELECT COUNT(*) FROM tp_orders WHERE status='live'"
    ).fetchone()[0]
    conn.close()
    assert live_before == 0

    conn = sqlite3.connect(positions_db)
    conn.execute("UPDATE resting_orders SET filled_quantity=25.0 WHERE order_id='ord1'")
    conn.commit()
    conn.close()

    om.on_entry_filled("ord1", "tok1", "mkt1", 0.10, 1.0, "Yes")

    conn = sqlite3.connect(positions_db)
    conn.row_factory = sqlite3.Row
    live_after = conn.execute(
        "SELECT label, sell_quantity FROM tp_orders WHERE status='live' ORDER BY label"
    ).fetchall()
    conn.close()

    assert [row["label"] for row in live_after] == ["tp_p10", "tp_p50"]
    assert abs(float(live_after[0]["sell_quantity"]) - 2.5) < 1e-9
    assert abs(float(live_after[1]["sell_quantity"]) - 5.0) < 1e-9


def test_tp_reconcile_ignores_growth_below_10pct_threshold(tmp_path):
    om, positions_db, _ = _make_om(tmp_path)

    conn = sqlite3.connect(positions_db)
    _insert_resting(conn, "ord1", "tok1", "mkt1", 0.10, 100.0, 30.0)
    conn.commit()
    conn.close()

    om.on_entry_filled("ord1", "tok1", "mkt1", 0.10, 30.0, "Yes")

    conn = sqlite3.connect(positions_db)
    live_before = conn.execute(
        "SELECT COUNT(*) FROM tp_orders WHERE status='live'"
    ).fetchone()[0]
    conn.execute("UPDATE resting_orders SET filled_quantity=32.0 WHERE order_id='ord1'")
    conn.commit()
    conn.close()

    om.on_entry_filled("ord1", "tok1", "mkt1", 0.10, 2.0, "Yes")

    conn = sqlite3.connect(positions_db)
    live_after = conn.execute(
        "SELECT COUNT(*) FROM tp_orders WHERE status='live'"
    ).fetchone()[0]
    conn.close()

    assert live_before == 2
    assert live_after == 2



def test_resolution_settles_partial_only_accumulated_inventory(tmp_path):
    om, positions_db, _ = _make_om(tmp_path)

    conn = sqlite3.connect(positions_db)
    _insert_resting(conn, "ord1", "tok1", "mkt1", 0.10, 100.0, 40.0)
    conn.commit()
    conn.close()

    om.on_entry_filled("ord1", "tok1", "mkt1", 0.10, 40.0, "Yes")
    om.on_market_resolved("tok1", True)

    conn = sqlite3.connect(positions_db)
    conn.row_factory = sqlite3.Row
    pos = conn.execute(
        "SELECT token_quantity, realized_pnl, status FROM positions WHERE token_id='tok1'"
    ).fetchone()
    conn.close()

    assert pos is not None
    assert pos["status"] == "resolved"
    assert abs(float(pos["token_quantity"]) - 40.0) < 1e-9
    assert abs(float(pos["realized_pnl"]) - 36.0) < 1e-9
