"""Tests for on_tp_filled() partial-fill handling — issue #71 finding 3."""

import sqlite3
import time
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _make_db(path: str) -> sqlite3.Connection:
    """Create a minimal positions DB with required tables."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS positions (
            position_id    TEXT PRIMARY KEY,
            token_id       TEXT NOT NULL,
            market_id      TEXT NOT NULL,
            entry_price    REAL NOT NULL,
            realized_pnl   REAL
        );
        CREATE TABLE IF NOT EXISTS tp_orders (
            order_id         TEXT PRIMARY KEY,
            position_id      TEXT NOT NULL,
            token_id         TEXT NOT NULL,
            sell_price       REAL NOT NULL,
            sell_quantity    REAL NOT NULL,
            filled_quantity  REAL NOT NULL DEFAULT 0,
            label            TEXT,
            status           TEXT NOT NULL DEFAULT 'live',
            created_at       INTEGER NOT NULL,
            filled_at        INTEGER
        );
    """)
    conn.commit()
    return conn


def _seed(conn: sqlite3.Connection, sell_qty: float) -> tuple[str, str]:
    """Insert one position and one TP order. Returns (tp_id, position_id)."""
    now = int(time.time())
    pos_id = "pos-001"
    tp_id = "tp-001"
    conn.execute(
        "INSERT INTO positions (position_id, token_id, market_id, entry_price) VALUES (?,?,?,?)",
        (pos_id, "tok1", "mkt1", 0.10),
    )
    conn.execute(
        "INSERT INTO tp_orders (order_id, position_id, token_id, sell_price, sell_quantity, label, status, created_at) "
        "VALUES (?,?,?,?,?,'tp_5x','live',?)",
        (tp_id, pos_id, "tok1", 0.50, sell_qty, now),
    )
    conn.commit()
    return tp_id, pos_id


def _make_om(db_path: str):
    """Build a minimal OrderManager-like object with only on_tp_filled wired up."""
    from execution.order_manager import OrderManager

    config = MagicMock()
    config.mode_config.max_exposure_per_market = 0.0
    config.dry_run = False

    clob = MagicMock()
    risk = MagicMock()

    with patch("execution.order_manager._init_positions_db"), \
         patch("execution.order_manager.pb.ensure_seeded"), \
         patch("execution.order_manager.pb.init_tables"), \
         patch.object(OrderManager, "_init_db", lambda self: None):
        om = OrderManager.__new__(OrderManager)
        om.config = config
        om.mc = config.mode_config
        om.clob = clob
        om.risk = risk
        om._db_path = db_path
        om._last_balance_alert_ts = 0
        from execution.exposure_manager import ExposureManager
        om.em = ExposureManager(max_per_market=float("inf"))
    return om


@pytest.fixture
def setup(tmp_path):
    db_path = str(tmp_path / "positions.db")
    conn = _make_db(db_path)
    om = _make_om(db_path)
    return om, conn, db_path


def _fetch_tp(conn, tp_id):
    return conn.execute(
        "SELECT status, filled_quantity FROM tp_orders WHERE order_id=?", (tp_id,)
    ).fetchone()


def test_partial_fill_stays_live(setup):
    om, conn, _ = setup
    tp_id, _ = _seed(conn, sell_qty=100.0)

    om.on_tp_filled(tp_id, fill_price=0.50, fill_quantity=40.0)

    conn2 = sqlite3.connect(om._db_path)
    conn2.row_factory = sqlite3.Row
    row = conn2.execute("SELECT status, filled_quantity FROM tp_orders WHERE order_id=?", (tp_id,)).fetchone()
    assert row["status"] == "live"
    assert abs(row["filled_quantity"] - 40.0) < 1e-9


def test_second_partial_still_live(setup):
    om, conn, _ = setup
    tp_id, _ = _seed(conn, sell_qty=100.0)

    om.on_tp_filled(tp_id, fill_price=0.50, fill_quantity=40.0)
    om.on_tp_filled(tp_id, fill_price=0.50, fill_quantity=40.0)

    conn2 = sqlite3.connect(om._db_path)
    conn2.row_factory = sqlite3.Row
    row = conn2.execute("SELECT status, filled_quantity FROM tp_orders WHERE order_id=?", (tp_id,)).fetchone()
    assert row["status"] == "live"
    assert abs(row["filled_quantity"] - 80.0) < 1e-9


def test_full_fill_becomes_matched(setup):
    om, conn, _ = setup
    tp_id, _ = _seed(conn, sell_qty=100.0)

    om.on_tp_filled(tp_id, fill_price=0.50, fill_quantity=100.0)

    conn2 = sqlite3.connect(om._db_path)
    conn2.row_factory = sqlite3.Row
    row = conn2.execute("SELECT status, filled_quantity FROM tp_orders WHERE order_id=?", (tp_id,)).fetchone()
    assert row["status"] == "matched"
    assert abs(row["filled_quantity"] - 100.0) < 1e-9


def test_partial_then_final_becomes_matched(setup):
    om, conn, _ = setup
    tp_id, _ = _seed(conn, sell_qty=100.0)

    om.on_tp_filled(tp_id, fill_price=0.50, fill_quantity=60.0)
    om.on_tp_filled(tp_id, fill_price=0.50, fill_quantity=40.0)

    conn2 = sqlite3.connect(om._db_path)
    conn2.row_factory = sqlite3.Row
    row = conn2.execute("SELECT status, filled_quantity FROM tp_orders WHERE order_id=?", (tp_id,)).fetchone()
    assert row["status"] == "matched"
    assert abs(row["filled_quantity"] - 100.0) < 1e-9


def test_pnl_accumulates_across_partial_fills(setup):
    om, conn, _ = setup
    tp_id, pos_id = _seed(conn, sell_qty=100.0)

    # entry_price=0.10, sell_price=0.50 → pnl per token = 0.40
    om.on_tp_filled(tp_id, fill_price=0.50, fill_quantity=40.0)
    om.on_tp_filled(tp_id, fill_price=0.50, fill_quantity=60.0)

    conn2 = sqlite3.connect(om._db_path)
    conn2.row_factory = sqlite3.Row
    row = conn2.execute("SELECT realized_pnl FROM positions WHERE position_id=?", (pos_id,)).fetchone()
    assert abs(row["realized_pnl"] - 40.0) < 1e-6  # 100 tokens * 0.40
