from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.config import ShortHorizonConfig
from short_horizon.core import OrderState
from short_horizon.core.events import AggressorSide
from short_horizon.events import BookLevel, BookUpdate, MarketStateUpdate, TradeTick
from short_horizon.execution import ExecutionEngine, ExecutionMode
from short_horizon.execution.paper_fill import PaperFillSimulator
from short_horizon.models import OrderIntent
from short_horizon.runner import drive_runtime_events
from short_horizon.storage import RunContext, SQLiteRuntimeStore
from short_horizon.strategies import ShortHorizon15mTouchStrategy


class PaperFillSimulatorTest(unittest.TestCase):
    def _market_state(self) -> MarketStateUpdate:
        return MarketStateUpdate(
            event_time_ms=200_000,
            ingest_time_ms=200_050,
            market_id="m1",
            token_id="tok_yes",
            condition_id="c1",
            question="Bitcoin Up or Down?",
            asset_slug="bitcoin",
            start_time_ms=0,
            end_time_ms=900_000,
            is_active=True,
            metadata_is_fresh=True,
            fee_rate_bps=10.0,
            fee_metadata_age_ms=1_000,
        )

    def _intent(self, *, price: float = 0.05, notional: float = 2.5) -> OrderIntent:
        return OrderIntent(
            intent_id="paper_ord_001",
            strategy_id="paper_test",
            market_id="m1",
            token_id="tok_yes",
            condition_id="c1",
            question="Bitcoin Up or Down?",
            asset_slug="bitcoin",
            level=price,
            entry_price=price,
            notional_usdc=notional,
            lifecycle_fraction=0.25,
            event_time_ms=225_000,
            reason="test_resting_bid",
        )

    def test_book_cross_partially_fills_buy_order_and_does_not_duplicate_same_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "paper.sqlite3"
            store = SQLiteRuntimeStore(db_path, run=RunContext(run_id="paper_fill_001", strategy_id="paper_test", mode="dry_run"))
            try:
                store.upsert_market_state(self._market_state())
                intent = self._intent(price=0.05, notional=2.5)  # 50 shares
                store.persist_intent(intent)
                execution = ExecutionEngine(store=store, mode=ExecutionMode.DRY_RUN)
                execution.submit(intent)

                simulator = PaperFillSimulator()
                book = BookUpdate(
                    event_time_ms=226_000,
                    ingest_time_ms=226_010,
                    market_id="m1",
                    token_id="tok_yes",
                    best_bid=0.03,
                    best_ask=0.04,
                    ask_levels=(BookLevel(price=0.04, size=10.0),),
                )

                fills = simulator.on_book_update(book, execution=execution)
                duplicate_fills = simulator.on_book_update(book, execution=execution)

                self.assertEqual(len(fills), 1)
                self.assertEqual(duplicate_fills, [])
                self.assertAlmostEqual(fills[0].fill_size, 10.0)
                self.assertAlmostEqual(fills[0].fill_price, 0.04)
                self.assertAlmostEqual(fills[0].remaining_size, 40.0)

                order = store.load_order("paper_ord_001")
                assert order is not None
                self.assertEqual(order["state"], OrderState.PARTIALLY_FILLED.value)
                self.assertAlmostEqual(order["cumulative_filled_size"], 10.0)
                self.assertAlmostEqual(order["remaining_size"], 40.0)
            finally:
                store.close()

    def test_trade_cross_fills_sell_order_for_exit_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "paper_sell.sqlite3"
            store = SQLiteRuntimeStore(db_path, run=RunContext(run_id="paper_fill_sell_001", strategy_id="paper_test", mode="dry_run"))
            try:
                store.upsert_market_state(self._market_state())
                store.insert_order(
                    order_id="sell_ord_001",
                    market_id="m1",
                    token_id="tok_yes",
                    side="SELL",
                    price=0.20,
                    size=15.0,
                    state=OrderState.ACCEPTED,
                    client_order_id="sell_ord_001",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_001,
                    remaining_size=15.0,
                    venue_order_status="accepted",
                )
                execution = ExecutionEngine(store=store, mode=ExecutionMode.DRY_RUN)
                simulator = PaperFillSimulator()

                fills = simulator.on_trade_tick(
                    TradeTick(
                        event_time_ms=226_000,
                        ingest_time_ms=226_010,
                        market_id="m1",
                        token_id="tok_yes",
                        price=0.21,
                        size=7.0,
                        source="test.trade",
                    ),
                    execution=execution,
                )

                self.assertEqual(len(fills), 1)
                self.assertEqual(fills[0].side, "SELL")
                self.assertAlmostEqual(fills[0].fill_size, 7.0)
                self.assertAlmostEqual(fills[0].fill_price, 0.21)
                order = store.load_order("sell_ord_001")
                assert order is not None
                self.assertEqual(order["state"], OrderState.PARTIALLY_FILLED.value)
                self.assertAlmostEqual(order["remaining_size"], 8.0)
            finally:
                store.close()

    def test_post_only_buy_fills_as_maker_on_book_cross(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "paper_post_only.sqlite3"
            store = SQLiteRuntimeStore(db_path, run=RunContext(run_id="paper_post_only_001", strategy_id="paper_test", mode="dry_run"))
            try:
                store.upsert_market_state(self._market_state())
                store.insert_order(
                    order_id="maker_buy_001",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.05,
                    size=20.0,
                    state=OrderState.ACCEPTED,
                    client_order_id="maker_buy_001",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_001,
                    remaining_size=20.0,
                    venue_order_status="accepted",
                    post_only=True,
                )
                execution = ExecutionEngine(store=store, mode=ExecutionMode.DRY_RUN)
                simulator = PaperFillSimulator()

                book_fills = simulator.on_book_update(
                    BookUpdate(
                        event_time_ms=226_000,
                        ingest_time_ms=226_010,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.04,
                        best_ask=0.04,
                        ask_levels=(BookLevel(price=0.04, size=20.0),),
                    ),
                    execution=execution,
                )
                trade_fills = simulator.on_trade_tick(
                    TradeTick(
                        event_time_ms=226_100,
                        ingest_time_ms=226_110,
                        market_id="m1",
                        token_id="tok_yes",
                        price=0.05,
                        size=6.0,
                        source="test.trade",
                        aggressor_side=AggressorSide.SELL,
                    ),
                    execution=execution,
                )

                self.assertEqual(len(book_fills), 1)
                self.assertEqual(book_fills[0].liquidity_role.value, "maker")
                self.assertAlmostEqual(book_fills[0].fill_size, 20.0)
                self.assertAlmostEqual(book_fills[0].fill_price, 0.05)
                self.assertEqual(trade_fills, [])
            finally:
                store.close()

    def test_post_only_book_cross_consumes_visible_depth_by_best_bid_priority(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "paper_post_only_book_queue.sqlite3"
            store = SQLiteRuntimeStore(db_path, run=RunContext(run_id="paper_post_only_book_queue_001", strategy_id="paper_test", mode="dry_run"))
            try:
                store.upsert_market_state(self._market_state())
                for idx, price in enumerate((0.01, 0.03, 0.05)):
                    store.insert_order(
                        order_id=f"maker_buy_book_{idx}",
                        market_id="m1",
                        token_id="tok_yes",
                        side="BUY",
                        price=price,
                        size=10.0,
                        state=OrderState.ACCEPTED,
                        client_order_id=f"maker_buy_book_{idx}",
                        intent_created_at_ms=225_000 + idx,
                        last_state_change_at_ms=225_001 + idx,
                        remaining_size=10.0,
                        venue_order_status="accepted",
                        post_only=True,
                    )
                execution = ExecutionEngine(store=store, mode=ExecutionMode.DRY_RUN)
                simulator = PaperFillSimulator()

                fills = simulator.on_book_update(
                    BookUpdate(
                        event_time_ms=226_000,
                        ingest_time_ms=226_010,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.04,
                        best_ask=0.01,
                        ask_levels=(BookLevel(price=0.01, size=15.0),),
                    ),
                    execution=execution,
                )

                self.assertEqual(len(fills), 2)
                self.assertEqual(fills[0].order_id, "maker_buy_book_2")
                self.assertEqual(fills[1].order_id, "maker_buy_book_1")
                self.assertAlmostEqual(fills[0].fill_size, 10.0)
                self.assertAlmostEqual(fills[1].fill_size, 5.0)
                self.assertAlmostEqual(fills[0].fill_price, 0.05)
                self.assertAlmostEqual(fills[1].fill_price, 0.03)
                self.assertEqual(store.load_order("maker_buy_book_2")["state"], OrderState.FILLED.value)
                self.assertEqual(store.load_order("maker_buy_book_1")["state"], OrderState.PARTIALLY_FILLED.value)
                self.assertEqual(store.load_order("maker_buy_book_0")["state"], OrderState.ACCEPTED.value)
            finally:
                store.close()

    def test_post_only_buy_requires_known_sell_aggressor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "paper_post_only_unknown.sqlite3"
            store = SQLiteRuntimeStore(db_path, run=RunContext(run_id="paper_post_only_unknown_001", strategy_id="paper_test", mode="dry_run"))
            try:
                store.upsert_market_state(self._market_state())
                store.insert_order(
                    order_id="maker_buy_unknown_001",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.05,
                    size=20.0,
                    state=OrderState.ACCEPTED,
                    client_order_id="maker_buy_unknown_001",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_001,
                    remaining_size=20.0,
                    venue_order_status="accepted",
                    post_only=True,
                )
                execution = ExecutionEngine(store=store, mode=ExecutionMode.DRY_RUN)
                simulator = PaperFillSimulator()

                fills = simulator.on_trade_tick(
                    TradeTick(
                        event_time_ms=226_100,
                        ingest_time_ms=226_110,
                        market_id="m1",
                        token_id="tok_yes",
                        price=0.05,
                        size=6.0,
                        source="test.trade",
                        aggressor_side=None,
                    ),
                    execution=execution,
                )

                self.assertEqual(fills, [])
                order = store.load_order("maker_buy_unknown_001")
                assert order is not None
                self.assertEqual(order["state"], OrderState.ACCEPTED.value)
                self.assertAlmostEqual(order["remaining_size"], 20.0)
            finally:
                store.close()

    def test_post_only_trade_print_consumes_size_once_across_same_price_orders(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "paper_post_only_queue.sqlite3"
            store = SQLiteRuntimeStore(db_path, run=RunContext(run_id="paper_post_only_queue_001", strategy_id="paper_test", mode="dry_run"))
            try:
                store.upsert_market_state(self._market_state())
                for idx in range(3):
                    store.insert_order(
                        order_id=f"maker_buy_queue_{idx}",
                        market_id="m1",
                        token_id="tok_yes",
                        side="BUY",
                        price=0.05,
                        size=10.0,
                        state=OrderState.ACCEPTED,
                        client_order_id=f"maker_buy_queue_{idx}",
                        intent_created_at_ms=225_000 + idx,
                        last_state_change_at_ms=225_001 + idx,
                        remaining_size=10.0,
                        venue_order_status="accepted",
                        post_only=True,
                    )
                execution = ExecutionEngine(store=store, mode=ExecutionMode.DRY_RUN)
                simulator = PaperFillSimulator()

                fills = simulator.on_trade_tick(
                    TradeTick(
                        event_time_ms=226_100,
                        ingest_time_ms=226_110,
                        market_id="m1",
                        token_id="tok_yes",
                        price=0.05,
                        size=15.0,
                        source="test.trade",
                        aggressor_side=AggressorSide.SELL,
                    ),
                    execution=execution,
                )

                self.assertEqual(len(fills), 2)
                self.assertAlmostEqual(sum(fill.fill_size for fill in fills), 15.0)
                self.assertAlmostEqual(fills[0].fill_size, 10.0)
                self.assertAlmostEqual(fills[1].fill_size, 5.0)
                self.assertEqual(store.load_order("maker_buy_queue_0")["state"], OrderState.FILLED.value)
                self.assertEqual(store.load_order("maker_buy_queue_1")["state"], OrderState.PARTIALLY_FILLED.value)
                self.assertEqual(store.load_order("maker_buy_queue_2")["state"], OrderState.ACCEPTED.value)
            finally:
                store.close()

    def test_same_ms_distinct_trade_ticks_do_not_collide(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "paper_same_ms_trades.sqlite3"
            store = SQLiteRuntimeStore(db_path, run=RunContext(run_id="paper_same_ms_trades_001", strategy_id="paper_test", mode="dry_run"))
            try:
                store.upsert_market_state(self._market_state())
                store.insert_order(
                    order_id="maker_buy_same_ms_001",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.05,
                    size=15.0,
                    state=OrderState.ACCEPTED,
                    client_order_id="maker_buy_same_ms_001",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_001,
                    remaining_size=15.0,
                    venue_order_status="accepted",
                    post_only=True,
                )
                execution = ExecutionEngine(store=store, mode=ExecutionMode.DRY_RUN)
                simulator = PaperFillSimulator()

                first_fills = simulator.on_trade_tick(
                    TradeTick(
                        event_time_ms=226_100,
                        ingest_time_ms=226_110,
                        market_id="m1",
                        token_id="tok_yes",
                        price=0.05,
                        size=5.0,
                        source="test.trade",
                        trade_id="trade-a",
                        aggressor_side=AggressorSide.SELL,
                    ),
                    execution=execution,
                )
                second_fills = simulator.on_trade_tick(
                    TradeTick(
                        event_time_ms=226_100,
                        ingest_time_ms=226_111,
                        market_id="m1",
                        token_id="tok_yes",
                        price=0.05,
                        size=5.0,
                        source="test.trade",
                        trade_id="trade-b",
                        aggressor_side=AggressorSide.SELL,
                    ),
                    execution=execution,
                )
                duplicate_first_fills = simulator.on_trade_tick(
                    TradeTick(
                        event_time_ms=226_100,
                        ingest_time_ms=226_110,
                        market_id="m1",
                        token_id="tok_yes",
                        price=0.05,
                        size=5.0,
                        source="test.trade",
                        trade_id="trade-a",
                        aggressor_side=AggressorSide.SELL,
                    ),
                    execution=execution,
                )

                self.assertEqual(len(first_fills), 1)
                self.assertEqual(len(second_fills), 1)
                self.assertEqual(duplicate_first_fills, [])
                fill_rows = store.load_fills()
                self.assertEqual(len(fill_rows), 2)
                self.assertEqual(len({row["fill_id"] for row in fill_rows}), 2)
                self.assertTrue(any("trade_trade-a" in row["fill_id"] for row in fill_rows))
                self.assertTrue(any("trade_trade-b" in row["fill_id"] for row in fill_rows))
                order = store.load_order("maker_buy_same_ms_001")
                assert order is not None
                self.assertEqual(order["state"], OrderState.PARTIALLY_FILLED.value)
                self.assertAlmostEqual(order["cumulative_filled_size"], 10.0)
                self.assertAlmostEqual(order["remaining_size"], 5.0)
            finally:
                store.close()

    def test_post_only_buy_matches_trade_price_not_all_higher_bid_levels(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "paper_post_only_levels.sqlite3"
            store = SQLiteRuntimeStore(db_path, run=RunContext(run_id="paper_post_only_levels_001", strategy_id="paper_test", mode="dry_run"))
            try:
                store.upsert_market_state(self._market_state())
                for idx, price in enumerate((0.01, 0.02, 0.03, 0.05)):
                    store.insert_order(
                        order_id=f"maker_buy_level_{idx}",
                        market_id="m1",
                        token_id="tok_yes",
                        side="BUY",
                        price=price,
                        size=10.0,
                        state=OrderState.ACCEPTED,
                        client_order_id=f"maker_buy_level_{idx}",
                        intent_created_at_ms=225_000 + idx,
                        last_state_change_at_ms=225_001 + idx,
                        remaining_size=10.0,
                        venue_order_status="accepted",
                        post_only=True,
                    )
                execution = ExecutionEngine(store=store, mode=ExecutionMode.DRY_RUN)
                simulator = PaperFillSimulator()

                fills = simulator.on_trade_tick(
                    TradeTick(
                        event_time_ms=226_100,
                        ingest_time_ms=226_110,
                        market_id="m1",
                        token_id="tok_yes",
                        price=0.01,
                        size=25.0,
                        source="test.trade",
                        aggressor_side=AggressorSide.SELL,
                    ),
                    execution=execution,
                )

                self.assertEqual(len(fills), 1)
                self.assertAlmostEqual(fills[0].fill_size, 10.0)
                self.assertEqual(fills[0].order_id, "maker_buy_level_0")
                self.assertEqual(store.load_order("maker_buy_level_0")["state"], OrderState.FILLED.value)
                for idx in (1, 2, 3):
                    order = store.load_order(f"maker_buy_level_{idx}")
                    assert order is not None
                    self.assertEqual(order["state"], OrderState.ACCEPTED.value)
                    self.assertAlmostEqual(order["remaining_size"], 10.0)
            finally:
                store.close()

    def test_runner_applies_paper_fills_in_dry_run_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runner.sqlite3"
            config = ShortHorizonConfig()
            runtime = __import__("short_horizon.live_runner", fromlist=["build_live_runtime"]).build_live_runtime(
                db_path=db_path,
                run_id="paper_runner_fill_001",
                config=config,
            )
            try:
                events = [
                    self._market_state(),
                    BookUpdate(event_time_ms=220_000, ingest_time_ms=220_010, market_id="m1", token_id="tok_yes", best_bid=0.53, best_ask=0.54),
                    BookUpdate(
                        event_time_ms=225_000,
                        ingest_time_ms=225_010,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.54,
                        best_ask=0.55,
                        ask_levels=(BookLevel(price=0.55, size=20.0),),
                    ),
                ]

                summary = drive_runtime_events(
                    events=events,
                    runtime=runtime,
                    logger_name="test.paper_fill_runner",
                    completed_event_name="paper_fill_runner_completed",
                    execution_mode=ExecutionMode.DRY_RUN,
                )
                self.assertEqual(summary.order_intents, 1)

                conn = sqlite3.connect(db_path)
                try:
                    fill_count = conn.execute("SELECT COUNT(*) FROM fills WHERE run_id = 'paper_runner_fill_001'").fetchone()[0]
                    order_state = conn.execute("SELECT state FROM orders WHERE run_id = 'paper_runner_fill_001'").fetchone()[0]
                finally:
                    conn.close()

                self.assertEqual(fill_count, 1)
                self.assertEqual(order_state, OrderState.FILLED.value)
            finally:
                runtime.store.close()


if __name__ == "__main__":
    unittest.main()
