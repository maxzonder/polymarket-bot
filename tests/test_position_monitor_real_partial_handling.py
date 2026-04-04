"""Regression tests for real-mode partial fill handling."""

import sqlite3
import time
from unittest.mock import MagicMock

from config import BotConfig
from execution.position_monitor import PositionMonitor


class DummyOM:
    def __init__(self, db_path):
        self._db_path = str(db_path)
        self.entry_calls = []
        self.tp_calls = []

    def on_entry_filled(self, **kwargs):
        self.entry_calls.append(kwargs)

    def on_tp_filled(self, order_id, fill_price, fill_quantity):
        self.tp_calls.append(
            {"order_id": order_id, "fill_price": fill_price, "fill_quantity": fill_quantity}
        )


def _make_positions_db(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS resting_orders (
            order_id TEXT PRIMARY KEY,
            token_id TEXT NOT NULL,
            market_id TEXT NOT NULL,
            outcome_name TEXT,
            price REAL NOT NULL,
            size REAL NOT NULL,
            filled_quantity REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'live'
        );
        CREATE TABLE IF NOT EXISTS tp_orders (
            order_id TEXT PRIMARY KEY,
            position_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            sell_price REAL NOT NULL,
            sell_quantity REAL NOT NULL,
            filled_quantity REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'live'
        );
        """
    )
    conn.commit()
    return conn


def test_check_fills_real_accumulates_partial_buy_without_opening_position(tmp_path):
    db_path = tmp_path / "positions.db"
    conn = _make_positions_db(db_path)
    conn.execute(
        "INSERT INTO resting_orders (order_id, token_id, market_id, outcome_name, price, size, status) VALUES (?,?,?,?,?,?, 'live')",
        ("ord1", "tok1", "mkt1", "Yes", 0.01, 2.0),
    )
    conn.commit()
    conn.close()

    cfg = BotConfig(mode="big_swan_mode", dry_run=False, private_key="fake")
    clob = MagicMock()
    clob.get_all_orders.return_value = [
        {"id": "ord1", "status": "PARTIAL", "price": 0.01, "size_matched": 0.5}
    ]
    om = DummyOM(db_path)
    pm = PositionMonitor(cfg, clob, om)
    pm._db_path = str(db_path)

    pm._check_fills_real()

    conn = sqlite3.connect(db_path)
    filled = conn.execute("SELECT filled_quantity FROM resting_orders WHERE order_id='ord1'").fetchone()[0]
    conn.close()

    assert abs(float(filled) - 0.5) < 1e-9
    assert om.entry_calls == []


def test_check_fills_real_passes_tp_partial_delta(tmp_path):
    db_path = tmp_path / "positions.db"
    conn = _make_positions_db(db_path)
    conn.execute(
        "INSERT INTO tp_orders (order_id, position_id, token_id, sell_price, sell_quantity, filled_quantity, status) VALUES (?,?,?,?,?,?, 'live')",
        ("tp1", "pos1", "tok1", 0.50, 10.0, 4.0),
    )
    conn.commit()
    conn.close()

    cfg = BotConfig(mode="big_swan_mode", dry_run=False, private_key="fake")
    clob = MagicMock()
    clob.get_all_orders.return_value = [
        {"id": "tp1", "status": "PARTIAL", "price": 0.50, "size_matched": 7.5}
    ]
    om = DummyOM(db_path)
    pm = PositionMonitor(cfg, clob, om)
    pm._db_path = str(db_path)

    pm._check_fills_real()

    assert len(om.tp_calls) == 1
    assert om.tp_calls[0]["order_id"] == "tp1"
    assert abs(om.tp_calls[0]["fill_quantity"] - 3.5) < 1e-9
