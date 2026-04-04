"""Regression tests for cumulative partial fill sync in paper_orders."""

import sqlite3

from api.clob_client import ClobClient


def test_paper_simulate_fill_keeps_partial_orders_live(tmp_path):
    paper_db = tmp_path / "paper.db"
    clob = ClobClient(private_key="fake", dry_run=True, paper_db_path=paper_db)

    result = clob.place_limit_order("tok1", "SELL", 0.50, 10.0, label="tp_100x")
    clob.paper_simulate_fill(result.order_id, 0.50, 4.0)

    conn = sqlite3.connect(paper_db)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT status, filled_size, size FROM paper_orders WHERE order_id=?", (result.order_id,)).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "live"
    assert abs(float(row["filled_size"]) - 4.0) < 1e-9
    assert abs(float(row["size"]) - 10.0) < 1e-9


def test_paper_simulate_fill_marks_matched_at_cumulative_full_fill(tmp_path):
    paper_db = tmp_path / "paper.db"
    clob = ClobClient(private_key="fake", dry_run=True, paper_db_path=paper_db)

    result = clob.place_limit_order("tok1", "SELL", 0.50, 10.0, label="tp_100x")
    clob.paper_simulate_fill(result.order_id, 0.50, 4.0)
    clob.paper_simulate_fill(result.order_id, 0.50, 10.0)

    conn = sqlite3.connect(paper_db)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT status, filled_size FROM paper_orders WHERE order_id=?", (result.order_id,)).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "matched"
    assert abs(float(row["filled_size"]) - 10.0) < 1e-9
