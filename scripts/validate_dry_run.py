"""Current execution validation harness for the polymarket trading bot.

Focus: mechanism correctness, not strategy quality.

Run with:
    python3 scripts/validate_dry_run.py
"""

import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import execution.order_manager as om_module
from api.clob_client import ClobClient, Orderbook, OrderbookLevel
from api.gamma_client import MarketInfo
from config import BotConfig, FAST_TP_MODE, BIG_SWAN_MODE
from execution.order_manager import OrderManager
from execution.position_monitor import PositionMonitor
from scripts.run_honest_replay import simulate_token
from strategy.risk_manager import RiskManager
from strategy.screener import EntryCandidate


# ── Helpers ───────────────────────────────────────────────────────────────────

def fake_book(best_ask, best_bid, ask_depth=50.0, bid_depth=50.0):
    asks = [OrderbookLevel(price=best_ask, size=ask_depth)] if best_ask is not None else []
    bids = [OrderbookLevel(price=best_bid, size=bid_depth)] if best_bid is not None else []
    return Orderbook(
        token_id="fake",
        bids=bids,
        asks=asks,
        best_bid=best_bid,
        best_ask=best_ask,
    )


def fake_market(question: str = "Test market?") -> MarketInfo:
    return MarketInfo(
        market_id="mkt_test",
        condition_id="cond_test",
        question=question,
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


def fake_candidate(price, entry_levels, market=None):
    return EntryCandidate(
        market_info=market or fake_market(),
        token_id="tok_yes",
        outcome_name="Yes",
        current_price=price,
        entry_fill_score=0.5,
        resolution_score=0.3,
        total_score=0.4,
        suggested_entry_levels=entry_levels,
        candidate_id="cand_test",
    )


def make_om(tmp_dir: Path, mode_config, balance: float | None = None):
    config = BotConfig(
        mode=mode_config.name,
        dry_run=True,
        private_key="fake_key",
        paper_initial_balance_usdc=(balance if balance is not None else BotConfig().paper_initial_balance_usdc),
    )
    if balance is None:
        balance = config.paper_initial_balance_usdc
    clob = ClobClient(
        private_key="fake_key",
        dry_run=True,
        paper_db_path=tmp_dir / "paper.db",
    )
    risk = RiskManager(mode_config, balance_usdc=balance)
    tmp_positions_db = tmp_dir / "positions.db"
    with patch("execution.order_manager.POSITIONS_DB", tmp_positions_db):
        om = OrderManager(config, clob, risk)
    return om, config, clob


def make_pm(tmp_dir: Path, config, clob, om):
    pm = PositionMonitor(config, clob, om)
    pm._db_path = str(tmp_dir / "positions.db")
    return pm


def read_one(db_path: str, query: str, params=()):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(query, params).fetchone()
    conn.close()
    return dict(row) if row else None


def count_rows(db_path: str, table: str, where: str = "", params=()):
    conn = sqlite3.connect(db_path)
    sql = f"SELECT COUNT(*) FROM {table}"
    if where:
        sql += f" WHERE {where}"
    count = conn.execute(sql, params).fetchone()[0]
    conn.close()
    return int(count)


def insert_resting_order(db_path: str, order_id: str, price: float, size: float, mode: str, token_id: str = "tok_yes", market_id: str = "mkt_test", outcome_name: str = "Yes"):
    now = int(time.time())
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO resting_orders "
        "(order_id, token_id, market_id, outcome_name, side, price, size, status, created_at, expires_at, mode) "
        "VALUES (?, ?, ?, ?, 'BUY', ?, ?, 'live', ?, 0, ?)",
        (order_id, token_id, market_id, outcome_name, price, size, now, mode),
    )
    conn.commit()
    conn.close()


def insert_paper_order(paper_db_path: str, order_id: str, side: str, price: float, size: float, token_id: str = "tok_yes", label: str = "test"):
    now = int(time.time())
    conn = sqlite3.connect(paper_db_path)
    conn.execute(
        "INSERT INTO paper_orders "
        "(order_id, token_id, side, price, size, status, created_at, updated_at, label) "
        "VALUES (?, ?, ?, ?, ?, 'live', ?, ?, ?)",
        (order_id, token_id, side, price, size, now, now, label),
    )
    conn.commit()
    conn.close()


# ── Scenario 1: scanner entry path ───────────────────────────────────────────

def scenario_scanner_entry_best_ask() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, _, _ = make_om(tmp_dir, FAST_TP_MODE)
        book = fake_book(best_ask=0.04, best_bid=0.03, ask_depth=50.0, bid_depth=50.0)
        candidate = fake_candidate(0.04, list(FAST_TP_MODE.entry_price_levels))

        with patch("execution.order_manager.get_orderbook", return_value=book):
            results = om.process_scanner_entry(candidate)

        if not results:
            return False, "No scanner-entry order placed"
        if abs(results[0].price - 0.04) > 1e-9:
            return False, f"Expected best_ask execution at 0.04, got {results[0].price}"

        row = read_one(str(tmp_dir / "positions.db"), "SELECT price FROM resting_orders WHERE order_id=?", (results[0].order_id,))
        if row is None or abs(float(row["price"]) - 0.04) > 1e-9:
            return False, "Resting order record not saved at best_ask"
        return True, ""


# ── Scenario 2: resting bid ladder placement ────────────────────────────────

def scenario_resting_bid_uses_entry_levels() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, _, _ = make_om(tmp_dir, BIG_SWAN_MODE)
        book = fake_book(best_ask=0.15, best_bid=0.14, ask_depth=50.0, bid_depth=50.0)
        candidate = fake_candidate(0.15, list(BIG_SWAN_MODE.entry_price_levels))

        with patch("execution.order_manager.get_orderbook", return_value=book):
            results = om.process_candidate(candidate)

        if not results:
            return False, "No resting bids placed"
        placed_prices = {float(r.price) for r in results}
        expected_prices = set(BIG_SWAN_MODE.entry_price_levels)
        if placed_prices != expected_prices:
            return False, f"Placed prices {placed_prices}, expected {expected_prices}"
        return True, ""


# ── Scenario 2b: already-cheap market must still use max-price tiers ─────────

def scenario_resting_bid_below_all_levels_buys_now() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, _, _ = make_om(tmp_dir, BIG_SWAN_MODE)
        book = fake_book(best_ask=0.01, best_bid=0.009, ask_depth=200.0, bid_depth=50.0)
        candidate = fake_candidate(0.01, list(BIG_SWAN_MODE.entry_price_levels))

        with patch("execution.order_manager.get_orderbook", return_value=book):
            results = om.process_candidate(candidate)

        if len(results) != len(BIG_SWAN_MODE.entry_price_levels):
            return False, f"Expected {len(BIG_SWAN_MODE.entry_price_levels)} tier orders, got {len(results)}"
        conn = sqlite3.connect(str(tmp_dir / "positions.db"))
        conn.row_factory = sqlite3.Row
        pos = conn.execute(
            "SELECT entry_price, entry_size_usdc, token_quantity FROM positions LIMIT 1"
        ).fetchone()
        conn.close()
        if pos is None:
            return False, "Expected immediate accumulated position for already-cheap market"
        if abs(float(pos["entry_price"]) - 0.01) > 1e-9:
            return False, f"Expected better fill price 0.01, got {pos['entry_price']}"
        expected_total_stake = BIG_SWAN_MODE.stake_usdc * len(BIG_SWAN_MODE.entry_price_levels)
        if float(pos["entry_size_usdc"]) + 1e-9 < expected_total_stake:
            return False, f"Expected at least configured tier budget {expected_total_stake}, got {pos['entry_size_usdc']}"
        return True, ""


# ── Scenario 3: balance gate ─────────────────────────────────────────────────

def scenario_balance_gate_blocks_extra_resting_bids() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, _, _ = make_om(tmp_dir, BIG_SWAN_MODE, balance=0.06)
        book = fake_book(best_ask=0.20, best_bid=0.19, ask_depth=50.0, bid_depth=50.0)
        candidate = fake_candidate(0.20, list(BIG_SWAN_MODE.entry_price_levels))

        with patch("execution.order_manager.get_orderbook", return_value=book):
            results = om.process_candidate(candidate)

        if len(results) != 1:
            return False, f"Expected 1 resting bid due to balance gate, got {len(results)}"
        if count_rows(str(tmp_dir / "positions.db"), "resting_orders") != 1:
            return False, "Expected exactly 1 resting order in DB"
        bal = read_one(str(tmp_dir / "positions.db"), "SELECT cash_balance FROM paper_balance WHERE id=1")
        if bal is None or abs(float(bal["cash_balance"]) - 0.06) > 1e-9:
            return False, "Cash balance should not be debited before entry fill"
        return True, ""


# ── Scenario 4: dry partial BUY accumulation ────────────────────────────────

def scenario_dry_partial_buy_accumulates_into_position() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, config, clob = make_om(tmp_dir, BIG_SWAN_MODE)
        pm = make_pm(tmp_dir, config, clob, om)

        insert_resting_order(str(tmp_dir / "positions.db"), "ord_partial_buy", 0.01, 2.0, BIG_SWAN_MODE.name)
        insert_paper_order(str(tmp_dir / "paper.db"), "ord_partial_buy", "BUY", 0.01, 2.0)

        book = fake_book(best_ask=0.008, best_bid=0.006, ask_depth=0.5, bid_depth=50.0)
        with patch("execution.position_monitor.get_orderbook", return_value=book):
            pm._check_fills_dry_run()

        resting = read_one(str(tmp_dir / "positions.db"), "SELECT filled_quantity, status FROM resting_orders WHERE order_id='ord_partial_buy'")
        paper = read_one(str(tmp_dir / "paper.db"), "SELECT filled_size, status FROM paper_orders WHERE order_id='ord_partial_buy'")
        pos = read_one(
            str(tmp_dir / "positions.db"),
            "SELECT token_quantity, entry_size_usdc, entry_price, status FROM positions LIMIT 1",
        )

        if resting is None or abs(float(resting["filled_quantity"]) - 0.5) > 1e-9:
            return False, f"Expected resting filled_quantity=0.5, got {resting}"
        if paper is None or abs(float(paper["filled_size"]) - 0.5) > 1e-9 or paper["status"] != "live":
            return False, f"Expected paper order live with filled_size=0.5, got {paper}"
        if pos is None:
            return False, "Expected accumulated position row after partial BUY"
        if pos["status"] != "open":
            return False, f"Expected open accumulated position, got {pos}"
        if abs(float(pos["token_quantity"]) - 0.5) > 1e-9:
            return False, f"Expected token_quantity=0.5, got {pos}"
        if abs(float(pos["entry_size_usdc"]) - 0.005) > 1e-9:
            return False, f"Expected entry_size_usdc=0.005, got {pos}"
        if abs(float(pos["entry_price"]) - 0.01) > 1e-9:
            return False, f"Expected entry_price=0.01, got {pos}"
        return True, ""


# ── Scenario 5: TP partial sync ──────────────────────────────────────────────

def scenario_tp_partial_fill_keeps_orders_live_until_full() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, _, clob = make_om(tmp_dir, BIG_SWAN_MODE)
        insert_resting_order(str(tmp_dir / "positions.db"), "entry_tp_sync", 0.01, 100.0, BIG_SWAN_MODE.name)
        insert_paper_order(str(tmp_dir / "paper.db"), "entry_tp_sync", "BUY", 0.01, 100.0)

        om.on_entry_filled("entry_tp_sync", "tok_yes", "mkt_test", 0.01, 100.0, "Yes")
        tp = read_one(
            str(tmp_dir / "positions.db"),
            "SELECT order_id, sell_price, sell_quantity FROM tp_orders WHERE label='tp_p10' LIMIT 1",
        )
        if tp is None:
            return False, "No tp_p10 order created"

        clob.paper_simulate_fill(tp["order_id"], float(tp["sell_price"]), 4.0)
        om.on_tp_filled(tp["order_id"], float(tp["sell_price"]), 4.0)
        row_partial = read_one(str(tmp_dir / "positions.db"), "SELECT status, filled_quantity FROM tp_orders WHERE order_id=?", (tp["order_id"],))
        paper_partial = read_one(str(tmp_dir / "paper.db"), "SELECT status, filled_size FROM paper_orders WHERE order_id=?", (tp["order_id"],))

        clob.paper_simulate_fill(tp["order_id"], float(tp["sell_price"]), 10.0)
        om.on_tp_filled(tp["order_id"], float(tp["sell_price"]), 6.0)
        row_full = read_one(str(tmp_dir / "positions.db"), "SELECT status, filled_quantity FROM tp_orders WHERE order_id=?", (tp["order_id"],))
        paper_full = read_one(str(tmp_dir / "paper.db"), "SELECT status, filled_size FROM paper_orders WHERE order_id=?", (tp["order_id"],))

        if row_partial is None or row_partial["status"] != "live" or abs(float(row_partial["filled_quantity"]) - 4.0) > 1e-9:
            return False, f"Expected TP partial to stay live at 4.0, got {row_partial}"
        if paper_partial is None or paper_partial["status"] != "live" or abs(float(paper_partial["filled_size"]) - 4.0) > 1e-9:
            return False, f"Expected paper TP partial to stay live at 4.0, got {paper_partial}"
        if row_full is None or row_full["status"] != "matched" or abs(float(row_full["filled_quantity"]) - 10.0) > 1e-9:
            return False, f"Expected TP full to be matched at 10.0, got {row_full}"
        if paper_full is None or paper_full["status"] != "matched" or abs(float(paper_full["filled_size"]) - 10.0) > 1e-9:
            return False, f"Expected paper TP full to be matched at 10.0, got {paper_full}"
        return True, ""


# ── Scenario 6: resolution quantity consistency ─────────────────────────────

def scenario_resolution_uses_actual_moonbag_leg() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        om, _, _ = make_om(tmp_dir, BIG_SWAN_MODE)
        insert_resting_order(str(tmp_dir / "positions.db"), "entry_resolution", 0.10, 100.0, BIG_SWAN_MODE.name)

        om.on_entry_filled("entry_resolution", "tok_yes", "mkt_test", 0.10, 100.0, "Yes")
        om.on_market_resolved("tok_yes", True)

        pos = read_one(str(tmp_dir / "positions.db"), "SELECT moonbag_quantity, realized_pnl FROM positions WHERE token_id='tok_yes'")
        moonbag = read_one(str(tmp_dir / "positions.db"), "SELECT sell_quantity FROM tp_orders WHERE token_id='tok_yes' AND label='moonbag_resolution'")
        if pos is None or moonbag is None:
            return False, "Missing position or moonbag row"
        if abs(float(pos["moonbag_quantity"]) - 70.0) > 1e-9:
            return False, f"Expected moonbag_quantity=70.0, got {pos['moonbag_quantity']}"
        if abs(float(moonbag["sell_quantity"]) - 70.0) > 1e-9:
            return False, f"Expected moonbag_resolution sell_quantity=70.0, got {moonbag['sell_quantity']}"
        if abs(float(pos["realized_pnl"]) - 90.0) > 1e-9:
            return False, f"Expected winner resolution pnl=90.0, got {pos['realized_pnl']}"
        return True, ""


# ── Scenario 7: real-mode partial handling ──────────────────────────────────

def scenario_real_mode_monitor_handles_partial_cumulative_fills() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        db_path = tmp_dir / "positions.db"
        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE resting_orders (
                order_id TEXT PRIMARY KEY,
                token_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                outcome_name TEXT,
                price REAL NOT NULL,
                size REAL NOT NULL,
                filled_quantity REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'live'
            );
            CREATE TABLE tp_orders (
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
        conn.execute(
            "INSERT INTO resting_orders (order_id, token_id, market_id, outcome_name, price, size, status) VALUES (?,?,?,?,?,?, 'live')",
            ("ord_real_partial", "tok_yes", "mkt_test", "Yes", 0.01, 2.0),
        )
        conn.execute(
            "INSERT INTO tp_orders (order_id, position_id, token_id, sell_price, sell_quantity, filled_quantity, status) VALUES (?,?,?,?,?,?, 'live')",
            ("tp_real_partial", "pos1", "tok_yes", 0.50, 10.0, 4.0),
        )
        conn.commit()
        conn.close()

        cfg = BotConfig(mode="big_swan_mode", dry_run=False, private_key="fake")
        clob = MagicMock()
        clob.get_all_orders.return_value = [
            {"id": "ord_real_partial", "status": "PARTIAL", "price": 0.01, "size_matched": 0.5},
            {"id": "tp_real_partial", "status": "PARTIAL", "price": 0.50, "size_matched": 7.5},
        ]

        class DummyOM:
            def __init__(self):
                self.entry_calls = []
                self.tp_calls = []

            def on_entry_filled(self, **kwargs):
                self.entry_calls.append(kwargs)

            def on_tp_filled(self, order_id, fill_price, fill_quantity):
                self.tp_calls.append((order_id, fill_price, fill_quantity))

        om = DummyOM()
        pm = PositionMonitor(cfg, clob, om)
        pm._db_path = str(db_path)
        pm._check_fills_real()

        resting = read_one(str(db_path), "SELECT filled_quantity FROM resting_orders WHERE order_id='ord_real_partial'")
        if resting is None or abs(float(resting["filled_quantity"]) - 0.5) > 1e-9:
            return False, f"Expected recorded real BUY partial=0.5, got {resting}"
        if len(om.entry_calls) != 1:
            return False, f"Expected one BUY delta callback on partial fill, got {om.entry_calls}"
        if abs(float(om.entry_calls[0]["fill_quantity"]) - 0.5) > 1e-9:
            return False, f"Expected BUY delta fill=0.5, got {om.entry_calls}"
        if len(om.tp_calls) != 1 or abs(float(om.tp_calls[0][2]) - 3.5) > 1e-9:
            return False, f"Expected TP delta fill=3.5, got {om.tp_calls}"
        return True, ""


# ── Scenario 8: honest replay entry path + balance gate ─────────────────────

def scenario_honest_replay_respects_balance_gate() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        positions_db = tmp_dir / "positions.db"
        paper_db = tmp_dir / "paper.db"
        trades_path = tmp_dir / "tok1.json"
        trades_path.write_text(
            json.dumps(
                [
                    {"timestamp": 1_700_000_000, "price": 0.20, "size": 100.0, "side": "BUY"},
                    {"timestamp": 1_700_000_100, "price": 0.20, "size": 100.0, "side": "BUY"},
                ]
            ),
            encoding="utf-8",
        )

        cfg = BotConfig(
            mode="big_swan_mode",
            dry_run=True,
            private_key="fake",
            paper_initial_balance_usdc=0.06,
        )
        clob = ClobClient(private_key="fake", dry_run=True, paper_db_path=paper_db)
        risk = RiskManager(cfg.mode_config, balance_usdc=cfg.paper_initial_balance_usdc)
        om_module.POSITIONS_DB = positions_db
        om = OrderManager(cfg, clob, risk)

        row = {
            "token_id": "tok1",
            "market_id": "mkt1",
            "question": "Replay test?",
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
            mc=cfg.mode_config,
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

        if result["status"] != "ok":
            return False, f"Expected replay status=ok, got {result}"
        if count_rows(str(positions_db), "resting_orders") != 1:
            return False, "Expected replay to place exactly one resting bid under tight balance"
        if count_rows(str(paper_db), "paper_orders") != 1:
            return False, "Expected replay to create exactly one paper order under tight balance"
        return True, ""


def scenario_honest_replay_terminal_cleanup_cancels_paper_orders() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        positions_db = tmp_dir / "positions.db"
        paper_db = tmp_dir / "paper.db"
        trades_path = tmp_dir / "tok1.json"
        trades_path.write_text(
            json.dumps(
                [
                    {"timestamp": 1_700_000_000, "price": 0.20, "size": 100.0, "side": "BUY"},
                    {"timestamp": 1_700_000_100, "price": 0.20, "size": 100.0, "side": "SELL"},
                ]
            ),
            encoding="utf-8",
        )

        cfg = BotConfig(
            mode="big_swan_mode",
            dry_run=True,
            private_key="fake",
            paper_initial_balance_usdc=1000.0,
        )
        clob = ClobClient(private_key="fake", dry_run=True, paper_db_path=paper_db)
        risk = RiskManager(cfg.mode_config, balance_usdc=cfg.paper_initial_balance_usdc)
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
            mc=cfg.mode_config,
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

        if result["status"] != "ok":
            return False, f"Expected replay status=ok, got {result}"
        cancelled_resting = count_rows(str(positions_db), "resting_orders", "status='cancelled'")
        live_resting = count_rows(str(positions_db), "resting_orders", "status='live'")
        cancelled_paper = count_rows(str(paper_db), "paper_orders", "side='BUY' AND status='cancelled'")
        live_paper = count_rows(str(paper_db), "paper_orders", "side='BUY' AND status='live'")
        if cancelled_resting == 0:
            return False, "Expected terminal replay cleanup to cancel resting buys"
        if live_resting != 0:
            return False, f"Expected no live resting buys after cleanup, got {live_resting}"
        if cancelled_paper != cancelled_resting:
            return False, f"Expected cancelled paper BUY count={cancelled_resting}, got {cancelled_paper}"
        if live_paper != 0:
            return False, f"Expected no live paper BUY orders after cleanup, got {live_paper}"
        return True, ""


SCENARIOS = [
    ("scanner_entry_best_ask", scenario_scanner_entry_best_ask),
    ("resting_bid_uses_entry_levels", scenario_resting_bid_uses_entry_levels),
    ("resting_bid_below_all_levels_buys_now", scenario_resting_bid_below_all_levels_buys_now),
    ("balance_gate_blocks_extra_resting_bids", scenario_balance_gate_blocks_extra_resting_bids),
    ("dry_partial_buy_accumulates_into_position", scenario_dry_partial_buy_accumulates_into_position),
    ("tp_partial_fill_keeps_orders_live_until_full", scenario_tp_partial_fill_keeps_orders_live_until_full),
    ("resolution_uses_actual_moonbag_leg", scenario_resolution_uses_actual_moonbag_leg),
    ("real_mode_monitor_handles_partial_cumulative_fills", scenario_real_mode_monitor_handles_partial_cumulative_fills),
    ("honest_replay_respects_balance_gate", scenario_honest_replay_respects_balance_gate),
    ("honest_replay_terminal_cleanup_cancels_paper_orders", scenario_honest_replay_terminal_cleanup_cancels_paper_orders),
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
