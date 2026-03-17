"""
Dry-run lifecycle validation harness for the polymarket trading bot.

Validates 5 scenarios without hitting live APIs.
Run with: python3 scripts/validate_dry_run.py
"""

import os
import sys
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.clob_client import ClobClient, Orderbook, OrderbookLevel
from api.gamma_client import MarketInfo
from config import BotConfig, FAST_TP_MODE, BIG_SWAN_MODE
from execution.order_manager import OrderManager
from execution.position_monitor import PositionMonitor
from strategy.risk_manager import RiskManager
from strategy.screener import EntryCandidate


# ── Helpers ───────────────────────────────────────────────────────────────────

def fake_book(best_ask, best_bid, ask_depth=50.0, bid_depth=50.0):
    asks = [OrderbookLevel(price=best_ask, size=ask_depth)] if best_ask else []
    bids = [OrderbookLevel(price=best_bid, size=bid_depth)] if best_bid else []
    return Orderbook(
        token_id="fake",
        bids=bids,
        asks=asks,
        best_bid=best_bid,
        best_ask=best_ask,
    )


def fake_market():
    return MarketInfo(
        market_id="mkt_test",
        condition_id="cond_test",
        question="Test market?",
        category="crypto",
        token_ids=["tok_yes", "tok_no"],
        outcome_names=["Yes", "No"],
        best_ask=0.04,
        best_bid=0.03,
        last_trade_price=0.04,
        volume_usdc=1000.0,
        liquidity_usdc=500.0,
        comment_count=0,
        fees_enabled=False,
        end_date_ts=None,
        hours_to_close=24.0,
    )


def fake_candidate(price, entry_levels, mode_config):
    return EntryCandidate(
        market_info=fake_market(),
        token_id="tok_yes",
        outcome_name="Yes",
        current_price=price,
        entry_fill_score=0.5,
        resolution_score=0.3,
        total_score=0.4,
        suggested_entry_levels=entry_levels,
    )


def make_om(tmp_dir: Path, mode_config, balance=100.0):
    """Build an OrderManager wired to a temp DB with a paper ClobClient.

    POSITIONS_DB is patched at the module level before construction so the
    OrderManager constructor (which calls _init_db()) never touches the real
    data directory — not even once.
    """
    config = BotConfig(
        mode=mode_config.name,
        dry_run=True,
        private_key="fake_key",
    )
    clob = ClobClient(
        private_key="fake_key",
        dry_run=True,
        paper_db_path=tmp_dir / "paper.db",
    )
    risk = RiskManager(mode_config, balance_usdc=balance)
    tmp_positions_db = tmp_dir / "positions.db"
    with patch("execution.order_manager.POSITIONS_DB", tmp_positions_db):
        om = OrderManager(config, clob, risk)
    # _db_path is already set to tmp path by the patched constructor
    return om, config, clob


def make_pm(tmp_dir: Path, config, clob, om):
    """Build a PositionMonitor pointing at the same temp DB."""
    pm = PositionMonitor(config, clob, om)
    pm._db_path = str(tmp_dir / "positions.db")
    return pm


def read_position(db_path: str, token_id: str) -> dict | None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM positions WHERE token_id=? ORDER BY opened_at DESC LIMIT 1",
        (token_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def read_paper_order(paper_db_path: str, order_id: str) -> dict | None:
    conn = sqlite3.connect(paper_db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM paper_orders WHERE order_id=?", (order_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def read_resting_orders(db_path: str, token_id: str, status: str = "live") -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM resting_orders WHERE token_id=? AND status=?",
        (token_id, status),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Scenario 1: scanner_entry ─────────────────────────────────────────────────

def scenario_scanner_entry() -> tuple[bool, str]:
    """
    fast_tp_mode, price=0.04 inside zone (<=0.05).
    Call om.process_scanner_entry(candidate).
    Verify: order placed at best_ask (0.04), NOT at resting ladder levels.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, config, clob = make_om(tmp_dir, FAST_TP_MODE, balance=100.0)

        # book: best_ask=0.04, best_bid=0.03, deep liquidity
        book = fake_book(best_ask=0.04, best_bid=0.03, ask_depth=50.0, bid_depth=50.0)
        candidate = fake_candidate(
            price=0.04,
            entry_levels=list(FAST_TP_MODE.entry_price_levels),
            mode_config=FAST_TP_MODE,
        )

        with patch("execution.order_manager.get_orderbook", return_value=book), \
             patch("execution.position_monitor.get_orderbook", return_value=book):
            results = om.process_scanner_entry(candidate)

        if not results:
            return False, "No orders returned from process_scanner_entry"

        result = results[0]
        # Should be placed at best_ask (0.04)
        if abs(result.price - 0.04) > 1e-9:
            return False, f"Expected order price 0.04, got {result.price}"

        # Make sure the price is NOT one of the ladder levels (0.005/0.01/0.02/0.03/0.05)
        ladder_levels = set(FAST_TP_MODE.entry_price_levels)
        # 0.04 is NOT in the ladder (ladder has 0.005, 0.01, 0.02, 0.03, 0.05)
        if result.price in ladder_levels:
            return False, f"Order was placed at a ladder level {result.price}, should be at best_ask"

        # Confirm resting order saved in DB at 0.04
        orders = read_resting_orders(str(tmp_dir / "positions.db"), "tok_yes")
        if not orders:
            return False, "No resting order saved in DB"
        if abs(orders[0]["price"] - 0.04) > 1e-9:
            return False, f"DB resting order price={orders[0]['price']}, expected 0.04"

        return True, ""


# ── Scenario 2: resting_bid ───────────────────────────────────────────────────

def scenario_resting_bid() -> tuple[bool, str]:
    """
    big_swan_mode, price=0.15 above zone max (0.30 is the screen max, but
    entry_price_levels are (0.001, 0.002, 0.005, 0.01) — all below 0.15).
    Call om.process_candidate(candidate).
    Verify: resting bids placed at levels < 0.15.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, config, clob = make_om(tmp_dir, BIG_SWAN_MODE, balance=100.0)

        # book: current market price is 0.15; deep top-of-book
        book = fake_book(best_ask=0.15, best_bid=0.14, ask_depth=50.0, bid_depth=50.0)
        candidate = fake_candidate(
            price=0.15,
            entry_levels=list(BIG_SWAN_MODE.entry_price_levels),
            mode_config=BIG_SWAN_MODE,
        )

        with patch("execution.order_manager.get_orderbook", return_value=book), \
             patch("execution.position_monitor.get_orderbook", return_value=book):
            results = om.process_candidate(candidate)

        if not results:
            return False, "No orders returned from process_candidate"

        # All placed prices must be < 0.15 (the current market price)
        for r in results:
            if r.price >= 0.15:
                return False, f"Resting bid at {r.price} >= current price 0.15"

        # Prices should match entry_price_levels (0.001, 0.002, 0.005, 0.01)
        placed_prices = {r.price for r in results}
        expected_levels = set(BIG_SWAN_MODE.entry_price_levels)
        unexpected = placed_prices - expected_levels
        if unexpected:
            return False, f"Unexpected bid prices placed: {unexpected}"

        return True, ""


# ── Scenario 3: partial_fill_realism ─────────────────────────────────────────

def scenario_partial_fill_realism() -> tuple[bool, str]:
    """
    Resting bid at 0.01, best_ask=0.008 (would fill), ask_depth_at(0.01)=0.5
    while order size=2.0. Trigger pm._check_fills_dry_run().
    Verify fill_quantity=0.5 propagates to position.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, config, clob = make_om(tmp_dir, BIG_SWAN_MODE, balance=100.0)
        pm = make_pm(tmp_dir, config, clob, om)

        # First, manually insert a resting order at 0.01 with size=2.0
        import time as _time
        now = int(_time.time())
        conn = sqlite3.connect(str(tmp_dir / "positions.db"))
        order_id = "test_order_partial_001"
        conn.execute(
            "INSERT INTO resting_orders "
            "(order_id, token_id, market_id, side, price, size, status, created_at, expires_at, mode) "
            "VALUES (?, ?, ?, 'BUY', ?, ?, 'live', ?, ?, ?)",
            (order_id, "tok_yes", "mkt_test", 0.01, 2.0, now, now + 86400, "big_swan_mode"),
        )
        conn.commit()
        conn.close()

        # Also insert the order into paper_trades.db so paper_simulate_fill can find it
        paper_conn = sqlite3.connect(str(tmp_dir / "paper.db"))
        paper_conn.execute(
            "INSERT INTO paper_orders "
            "(order_id, token_id, side, price, size, status, created_at, updated_at, label) "
            "VALUES (?, 'tok_yes', 'BUY', 0.01, 2.0, 'live', ?, ?, 'test')",
            (order_id, now, now),
        )
        paper_conn.commit()
        paper_conn.close()

        # book: best_ask=0.008 (<= 0.01 so fills), but only 0.5 depth at or below 0.01
        book = fake_book(best_ask=0.008, best_bid=0.006, ask_depth=0.5, bid_depth=50.0)

        with patch("execution.order_manager.get_orderbook", return_value=book), \
             patch("execution.position_monitor.get_orderbook", return_value=book):
            pm._check_fills_dry_run()

        # Check position was created with fill_quantity=0.5 (depth-capped)
        pos = read_position(str(tmp_dir / "positions.db"), "tok_yes")
        if pos is None:
            return False, "No position created after simulated fill"

        actual_qty = float(pos["token_quantity"])
        if abs(actual_qty - 0.5) > 1e-6:
            return False, f"Expected token_quantity=0.5 (depth-capped), got {actual_qty}"

        # Also verify paper_orders.filled_size is consistent with position state.
        # This is the full consistency claim: paper DB and position DB must agree.
        paper_order = read_paper_order(str(tmp_dir / "paper.db"), order_id)
        if paper_order is None:
            return False, "Paper order not found after fill"
        actual_filled_size = float(paper_order.get("filled_size") or 0.0)
        if abs(actual_filled_size - 0.5) > 1e-6:
            return False, (
                f"paper_orders.filled_size={actual_filled_size}, "
                f"expected 0.5 (should match position token_quantity)"
            )

        return True, ""


# ── Scenario 4: tp_pnl_accounting ────────────────────────────────────────────

def scenario_tp_pnl_accounting() -> tuple[bool, str]:
    """
    Entry filled at 0.01 for 2.0 tokens.
    TP fill at 0.05 for 1.0 tokens → realized_pnl = (0.05 - 0.01) * 1.0 = 0.04.
    Market resolves as winner (price=1.0).
    Verify final realized_pnl = 0.04 + (1.0 - 0.01) * moonbag_qty (additive).
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, config, clob = make_om(tmp_dir, BIG_SWAN_MODE, balance=100.0)

        # First, create a resting order entry so on_entry_filled can mark it matched
        import time as _time
        now = int(_time.time())
        conn = sqlite3.connect(str(tmp_dir / "positions.db"))
        entry_order_id = "entry_order_tp_test"
        conn.execute(
            "INSERT INTO resting_orders "
            "(order_id, token_id, market_id, side, price, size, status, created_at, expires_at, mode) "
            "VALUES (?, ?, ?, 'BUY', 0.01, 2.0, 'live', ?, ?, ?)",
            (entry_order_id, "tok_yes", "mkt_test", now, now + 86400, "big_swan_mode"),
        )
        conn.commit()
        conn.close()

        # Trigger entry fill at 0.01 for 2.0 tokens
        om.on_entry_filled(
            order_id=entry_order_id,
            token_id="tok_yes",
            market_id="mkt_test",
            fill_price=0.01,
            fill_quantity=2.0,
            outcome_name="Yes",
        )

        # Find the TP order in the DB (label starts with "tp_")
        db_path = str(tmp_dir / "positions.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        tp_rows = conn.execute(
            "SELECT * FROM tp_orders WHERE token_id='tok_yes' AND status='live'"
        ).fetchall()
        conn.close()

        if not tp_rows:
            return False, "No live TP orders found after on_entry_filled"

        # Pick first live TP order (not moonbag)
        tp_order = None
        for row in tp_rows:
            if row["label"] != "moonbag_resolution":
                tp_order = dict(row)
                break

        if tp_order is None:
            return False, "No non-moonbag TP order found"

        # Use the actual TP order quantity from the system — not a hardcoded value.
        # For big_swan_mode entry at 0.01, 2.0 tokens: tp_5x sell_qty = 2.0 * 0.20 = 0.40
        tp_fill_qty = float(tp_order["sell_quantity"])
        tp_fill_price = float(tp_order["sell_price"])

        om.on_tp_filled(
            order_id=tp_order["order_id"],
            fill_price=tp_fill_price,
            fill_quantity=tp_fill_qty,
        )

        # Check intermediate PnL
        pos = read_position(db_path, "tok_yes")
        if pos is None:
            return False, "Position not found after TP fill"

        expected_tp_pnl = (tp_fill_price - 0.01) * tp_fill_qty
        actual_pnl = float(pos["realized_pnl"] or 0.0)
        if abs(actual_pnl - expected_tp_pnl) > 1e-6:
            return False, (
                f"After TP fill, expected realized_pnl={expected_tp_pnl:.6f} "
                f"(price={tp_fill_price} qty={tp_fill_qty}), got {actual_pnl:.6f}"
            )

        # Now market resolves as winner
        om.on_market_resolved(token_id="tok_yes", is_winner=True)

        pos_after = read_position(db_path, "tok_yes")
        if pos_after is None:
            return False, "Position not found after market resolution"

        # moonbag_fraction for big_swan_mode = 0.60
        moonbag_qty = 2.0 * BIG_SWAN_MODE.moonbag_fraction  # 1.2
        moonbag_pnl = (1.0 - 0.01) * moonbag_qty
        expected_final_pnl = expected_tp_pnl + moonbag_pnl

        actual_final_pnl = float(pos_after["realized_pnl"] or 0.0)
        if abs(actual_final_pnl - expected_final_pnl) > 1e-6:
            return (
                False,
                f"After resolution, expected realized_pnl={expected_final_pnl:.6f} "
                f"(tp={expected_tp_pnl:.4f} + moonbag={moonbag_pnl:.4f}), "
                f"got {actual_final_pnl:.6f}",
            )

        if pos_after["status"] != "resolved":
            return False, f"Expected position status='resolved', got {pos_after['status']!r}"

        return True, ""


# ── Scenario 5: losing_resolution ────────────────────────────────────────────

def scenario_losing_resolution() -> tuple[bool, str]:
    """
    Entry filled at 0.02 for 3.0 tokens.
    Market resolves to 0 (loser).
    Verify realized_pnl = (0 - 0.02) * moonbag_qty (negative).
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, config, clob = make_om(tmp_dir, BIG_SWAN_MODE, balance=100.0)

        import time as _time
        now = int(_time.time())
        conn = sqlite3.connect(str(tmp_dir / "positions.db"))
        entry_order_id = "entry_order_loser_test"
        conn.execute(
            "INSERT INTO resting_orders "
            "(order_id, token_id, market_id, side, price, size, status, created_at, expires_at, mode) "
            "VALUES (?, ?, ?, 'BUY', 0.02, 3.0, 'live', ?, ?, ?)",
            (entry_order_id, "tok_yes", "mkt_test", now, now + 86400, "big_swan_mode"),
        )
        conn.commit()
        conn.close()

        # Entry filled at 0.02 for 3.0 tokens
        om.on_entry_filled(
            order_id=entry_order_id,
            token_id="tok_yes",
            market_id="mkt_test",
            fill_price=0.02,
            fill_quantity=3.0,
            outcome_name="Yes",
        )

        # Market resolves as loser (price=0)
        om.on_market_resolved(token_id="tok_yes", is_winner=False)

        db_path = str(tmp_dir / "positions.db")
        pos = read_position(db_path, "tok_yes")
        if pos is None:
            return False, "Position not found after losing resolution"

        if pos["status"] != "resolved":
            return False, f"Expected status='resolved', got {pos['status']!r}"

        # moonbag_qty = 3.0 * 0.60 = 1.8
        moonbag_qty = 3.0 * BIG_SWAN_MODE.moonbag_fraction
        # resolution_price=0.0, so moonbag_pnl = (0.0 - 0.02) * 1.8 = -0.036
        expected_pnl = (0.0 - 0.02) * moonbag_qty

        actual_pnl = float(pos["realized_pnl"] or 0.0)
        if abs(actual_pnl - expected_pnl) > 1e-6:
            return (
                False,
                f"Expected realized_pnl={expected_pnl:.6f} (negative), got {actual_pnl:.6f}",
            )

        if actual_pnl >= 0:
            return False, f"Expected negative PnL for losing position, got {actual_pnl}"

        return True, ""


# ── Runner ────────────────────────────────────────────────────────────────────

SCENARIOS = [
    ("scanner_entry", scenario_scanner_entry),
    ("resting_bid", scenario_resting_bid),
    ("partial_fill_realism", scenario_partial_fill_realism),
    ("tp_pnl_accounting", scenario_tp_pnl_accounting),
    ("losing_resolution", scenario_losing_resolution),
]


def main():
    passed = 0
    failed = 0

    for name, fn in SCENARIOS:
        try:
            ok, reason = fn()
        except Exception as exc:
            ok = False
            reason = f"Unexpected exception: {exc}"

        if ok:
            print(f"[PASS] {name}")
            passed += 1
        else:
            print(f"[FAIL] {name}: {reason}")
            failed += 1

    total = passed + failed
    print(f"\nResults: {passed}/{total} passed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
