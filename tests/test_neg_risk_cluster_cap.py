import sqlite3
import time
from unittest.mock import MagicMock, patch

from api.clob_client import OrderResult, Orderbook, OrderbookLevel
from api.gamma_client import MarketInfo
from config import BotConfig
from execution.order_manager import OrderManager
from strategy.screener import EntryCandidate


def _make_order_manager(tmp_path):
    positions_db = tmp_path / "positions.db"
    config = BotConfig(
        mode="big_swan_mode",
        dry_run=True,
        private_key="fake_key",
        paper_initial_balance_usdc=10.0,
    )
    clob = MagicMock()
    risk = MagicMock()
    with patch("execution.order_manager.POSITIONS_DB", positions_db):
        om = OrderManager(config, clob, risk, disable_scan_log=True)
    return om, positions_db, clob, risk


def _insert_resting_order(
    conn: sqlite3.Connection,
    *,
    order_id: str,
    token_id: str,
    market_id: str,
    group_id: str,
    status: str,
):
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO resting_orders
            (order_id, token_id, market_id, side, price, size, status, created_at,
             expires_at, mode, candidate_id, outcome_name, neg_risk_group_id)
        VALUES (?, ?, ?, 'BUY', 0.10, 1.0, ?, ?, 0, 'big_swan_mode', NULL, 'Yes', ?)
        """,
        (order_id, token_id, market_id, status, now, group_id),
    )


def _insert_open_position(
    conn: sqlite3.Connection,
    *,
    position_id: str,
    token_id: str,
    market_id: str,
    entry_order_id: str,
    group_id: str,
):
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO positions
            (position_id, token_id, market_id, outcome_name, entry_order_id,
             entry_price, entry_size_usdc, token_quantity, status, opened_at, neg_risk_group_id)
        VALUES (?, ?, ?, 'Yes', ?, 0.10, 0.10, 1.0, 'open', ?, ?)
        """,
        (position_id, token_id, market_id, entry_order_id, now, group_id),
    )


def _candidate(group_id: str, market_id: str, token_id: str) -> EntryCandidate:
    market = MarketInfo(
        market_id=market_id,
        condition_id=f"cond-{market_id}",
        question=f"Question for {market_id}",
        category="politics",
        token_ids=[token_id, f"no-{token_id}"],
        outcome_names=["Yes", "No"],
        best_ask=0.08,
        best_bid=0.07,
        last_trade_price=0.08,
        volume_usdc=1000.0,
        liquidity_usdc=500.0,
        comment_count=0,
        fees_enabled=False,
        end_date_ts=None,
        hours_to_close=24.0,
        neg_risk=True,
        neg_risk_group_id=group_id,
    )
    return EntryCandidate(
        market_info=market,
        token_id=token_id,
        outcome_name="Yes",
        current_price=0.08,
        total_score=0.5,
        suggested_entry_levels=[0.05],
        candidate_id=f"cand-{market_id}",
        rationale="test",
        market_score=None,
    )


def test_build_cycle_context_counts_open_positions_toward_neg_risk_cluster_cap(tmp_path):
    om, positions_db, _, _ = _make_order_manager(tmp_path)

    conn = sqlite3.connect(positions_db)
    _insert_open_position(
        conn,
        position_id="pos-open",
        token_id="tok-open",
        market_id="mkt-open",
        entry_order_id="ord-open",
        group_id="grp-1",
    )
    conn.commit()
    conn.close()

    context = om.build_cycle_context()

    assert context.open_position_keys == {("mkt-open", "tok-open")}
    assert context.live_cluster_count("grp-1") == 1


def test_same_market_can_still_place_multiple_price_levels_when_cluster_cap_is_one(tmp_path):
    om, _, clob, risk = _make_order_manager(tmp_path)
    candidate = _candidate(group_id="grp-1", market_id="mkt-same", token_id="tok-same")
    candidate.current_price = 0.20
    candidate.suggested_entry_levels = [0.05, 0.10]

    risk.size_position.side_effect = [
        MagicMock(token_quantity=1.0, stake_usdc=0.05),
        MagicMock(token_quantity=1.0, stake_usdc=0.10),
    ]
    clob.place_limit_order.side_effect = [
        OrderResult(order_id="ord-1", status="live", token_id="tok-same", side="BUY", price=0.05, size=1.0),
        OrderResult(order_id="ord-2", status="live", token_id="tok-same", side="BUY", price=0.10, size=1.0),
    ]

    book = Orderbook(
        token_id="tok-same",
        bids=[OrderbookLevel(price=0.19, size=10.0)],
        asks=[OrderbookLevel(price=0.20, size=10.0)],
        best_bid=0.19,
        best_ask=0.20,
    )

    with patch("execution.order_manager.get_orderbook", return_value=book):
        results = om.process_candidate(candidate)

    assert [r.order_id for r in results] == ["ord-1", "ord-2"]
    assert clob.place_limit_order.call_count == 2



def test_scanner_entry_respects_cluster_cap_from_existing_open_position(tmp_path):
    om, positions_db, clob, risk = _make_order_manager(tmp_path)

    conn = sqlite3.connect(positions_db)
    _insert_resting_order(
        conn,
        order_id="ord-open",
        token_id="tok-open",
        market_id="mkt-open",
        group_id="grp-1",
        status="matched",
    )
    _insert_open_position(
        conn,
        position_id="pos-open",
        token_id="tok-open",
        market_id="mkt-open",
        entry_order_id="ord-open",
        group_id="grp-1",
    )
    conn.commit()
    conn.close()

    context = om.build_cycle_context()
    candidate = _candidate(group_id="grp-1", market_id="mkt-other", token_id="tok-other")

    with patch("execution.order_manager.get_orderbook") as get_orderbook:
        results = om.process_scanner_entry(candidate, context=context)

    assert results == []
    get_orderbook.assert_not_called()
    risk.size_position.assert_not_called()
    clob.place_limit_order.assert_not_called()


def test_scanner_matched_fill_persists_neg_risk_group_id_in_positions(tmp_path):
    om, positions_db, clob, risk = _make_order_manager(tmp_path)
    candidate = _candidate(group_id="grp-2", market_id="mkt-scan", token_id="tok-scan")

    risk.size_position.return_value = MagicMock(token_quantity=2.0, stake_usdc=0.16)
    clob.place_limit_order.return_value = OrderResult(
        order_id="ord-scan",
        status="matched",
        token_id="tok-scan",
        side="BUY",
        price=0.08,
        size=2.0,
        filled_size=2.0,
    )
    book = Orderbook(
        token_id="tok-scan",
        bids=[OrderbookLevel(price=0.07, size=10.0)],
        asks=[OrderbookLevel(price=0.08, size=10.0)],
        best_bid=0.07,
        best_ask=0.08,
    )

    with patch("execution.order_manager.get_orderbook", return_value=book):
        results = om.process_scanner_entry(candidate)

    assert len(results) == 1
    assert results[0].status == "matched"

    conn = sqlite3.connect(positions_db)
    conn.row_factory = sqlite3.Row
    pos = conn.execute(
        "SELECT market_id, token_id, entry_order_id, neg_risk_group_id, status FROM positions"
    ).fetchone()
    conn.close()

    assert pos is not None
    assert pos["market_id"] == "mkt-scan"
    assert pos["token_id"] == "tok-scan"
    assert pos["entry_order_id"] == "ord-scan"
    assert pos["neg_risk_group_id"] == "grp-2"
    assert pos["status"] == "open"

    rebuilt = om.build_cycle_context()
    assert rebuilt.live_cluster_count("grp-2") == 1
