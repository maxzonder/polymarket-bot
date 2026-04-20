from __future__ import annotations

import io
import json
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
from short_horizon.core import EventType, OrderState
from short_horizon.engine import ShortHorizonEngine
from short_horizon.events import BookUpdate, MarketStateUpdate, TimerEvent
from short_horizon.models import OrderIntent, SkipDecision
from short_horizon.storage import InMemoryIntentStore, RunContext, SQLiteRuntimeStore
from short_horizon.strategies import ShortHorizon15mTouchStrategy
from short_horizon.strategy_api import Noop, PlaceOrder
from short_horizon.telemetry import configure_logging, event_log_fields, get_logger


class CoreTypesTest(unittest.TestCase):
    def test_order_state_values_match_phase0_contract(self) -> None:
        self.assertEqual(OrderState.INTENT, "intent")
        self.assertEqual(OrderState.PENDING_SEND, "pending_send")
        self.assertEqual(OrderState.ACCEPTED, "accepted")
        self.assertEqual(OrderState.PARTIALLY_FILLED, "partially_filled")
        self.assertEqual(OrderState.FILLED, "filled")
        self.assertEqual(OrderState.CANCEL_REQUESTED, "cancel_requested")
        self.assertEqual(OrderState.CANCEL_CONFIRMED, "cancel_confirmed")
        self.assertEqual(OrderState.REJECTED, "rejected")
        self.assertEqual(OrderState.EXPIRED, "expired")
        self.assertEqual(OrderState.UNKNOWN, "unknown")
        self.assertEqual(OrderState.REPLACE_REQUESTED, "replace_requested")
        self.assertEqual(OrderState.REPLACED, "replaced")

    def test_normalized_events_expose_canonical_event_type(self) -> None:
        market = MarketStateUpdate(
            event_time_ms=1,
            ingest_time_ms=2,
            market_id="m1",
            token_id="tok_yes",
            condition_id="c1",
            question="Bitcoin Up or Down?",
        )
        book = BookUpdate(
            event_time_ms=3,
            ingest_time_ms=4,
            market_id="m1",
            token_id="tok_yes",
            best_bid=0.54,
            best_ask=0.55,
        )
        self.assertEqual(market.event_type, EventType.MARKET_STATE_UPDATE)
        self.assertEqual(book.event_type, EventType.BOOK_UPDATE)


class StrategyApiContractTest(unittest.TestCase):
    def test_touch_strategy_implements_market_event_contract(self) -> None:
        strategy = ShortHorizon15mTouchStrategy(config=ShortHorizonConfig())
        strategy.on_market_event(
            MarketStateUpdate(
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
        )
        self.assertEqual(strategy.on_market_event(BookUpdate(event_time_ms=220_000, ingest_time_ms=220_020, market_id="m1", token_id="tok_yes", best_bid=0.53, best_ask=0.54)), [])
        outputs = strategy.on_market_event(BookUpdate(event_time_ms=225_000, ingest_time_ms=225_020, market_id="m1", token_id="tok_yes", best_bid=0.54, best_ask=0.55))
        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], PlaceOrder)
        self.assertIsInstance(outputs[0].intent, OrderIntent)

    def test_touch_strategy_timer_is_unit_testable_without_io(self) -> None:
        strategy = ShortHorizon15mTouchStrategy(config=ShortHorizonConfig())
        outputs = strategy.on_timer(TimerEvent(event_time_ms=1, ingest_time_ms=1, timer_kind="decision_tick"))
        self.assertEqual(outputs, [])
        self.assertEqual([Noop(reason="x")][0].reason, "x")


class ShortHorizonEngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = InMemoryIntentStore()
        self.engine = ShortHorizonEngine(config=ShortHorizonConfig(), intent_store=self.store)

    def _market_state(self, *, token_id: str = "tok_yes", asset_slug: str = "bitcoin") -> MarketStateUpdate:
        return MarketStateUpdate(
            event_time_ms=200_000,
            ingest_time_ms=200_050,
            market_id="m1",
            token_id=token_id,
            condition_id="c1",
            question="Bitcoin Up or Down?",
            asset_slug=asset_slug,
            start_time_ms=0,
            end_time_ms=900_000,
            is_active=True,
            metadata_is_fresh=True,
            fee_rate_bps=10.0,
            fee_metadata_age_ms=1_000,
        )

    def _book(self, *, event_time_ms: int, best_ask: float) -> BookUpdate:
        return BookUpdate(
            event_time_ms=event_time_ms,
            ingest_time_ms=event_time_ms + 20,
            market_id="m1",
            token_id="tok_yes",
            best_bid=best_ask - 0.01,
            best_ask=best_ask,
        )

    def test_persists_single_intent_on_first_touch_in_bucket(self) -> None:
        self.engine.on_market_state(self._market_state())

        self.assertEqual(self.engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54)), [])
        outputs = self.engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], OrderIntent)
        self.assertEqual(len(self.store.intents), 1)
        self.assertEqual(self.store.intents[0].entry_price, 0.55)
        self.assertEqual(self.store.intents[0].level, 0.55)
        self.assertEqual(len(self.store.events), 3)
        self.assertIn(("m1", "tok_yes", "first_touch_fired:0.55"), self.store.strategy_state)

    def test_non_btc_eth_touch_is_skipped(self) -> None:
        self.engine.on_market_state(self._market_state(asset_slug="dogecoin"))
        self.engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54))
        outputs = self.engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "asset_not_allowed")
        self.assertEqual(len(self.store.intents), 0)

    def test_touch_does_not_rearm_after_wrong_bucket_skip(self) -> None:
        self.engine.on_market_state(self._market_state())

        self.engine.on_book_update(self._book(event_time_ms=80_000, best_ask=0.54))
        early_outputs = self.engine.on_book_update(self._book(event_time_ms=90_000, best_ask=0.55))
        self.assertEqual(len(early_outputs), 1)
        self.assertIsInstance(early_outputs[0], SkipDecision)
        self.assertEqual(early_outputs[0].reason, "wrong_lifecycle_bucket")

        self.engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54))
        later_outputs = self.engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))
        self.assertEqual(later_outputs, [])
        self.assertEqual(len(self.store.intents), 0)


class TelemetryTest(unittest.TestCase):
    def test_structured_logger_emits_mandatory_fields(self) -> None:
        stream = io.StringIO()
        configure_logging(name="short_horizon.test", stream=stream)
        logger = get_logger("short_horizon.test.runtime", run_id="run_test_telemetry")
        event = BookUpdate(
            event_time_ms=3_000,
            ingest_time_ms=4_000,
            market_id="m1",
            token_id="tok_yes",
            best_bid=0.54,
            best_ask=0.55,
        )

        logger.info("book_update_ingested", **event_log_fields(event, order_id="ord_123"), best_ask=0.55)

        payload = json.loads(stream.getvalue().strip())
        self.assertEqual(payload["run_id"], "run_test_telemetry")
        self.assertEqual(payload["market_id"], "m1")
        self.assertEqual(payload["order_id"], "ord_123")
        self.assertEqual(payload["event_time"], "1970-01-01T00:00:03.000Z")
        self.assertEqual(payload["ingest_time"], "1970-01-01T00:00:04.000Z")
        self.assertEqual(payload["event"], "book_update_ingested")


class SQLiteRuntimeStoreTest(unittest.TestCase):
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

    def _book(self, *, event_time_ms: int, best_ask: float) -> BookUpdate:
        return BookUpdate(
            event_time_ms=event_time_ms,
            ingest_time_ms=event_time_ms + 20,
            market_id="m1",
            token_id="tok_yes",
            best_bid=best_ask - 0.01,
            best_ask=best_ask,
        )

    def test_sqlite_store_persists_run_market_events_and_intent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_test_001",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                engine = ShortHorizonEngine(config=ShortHorizonConfig(), intent_store=store)
                engine.on_market_state(self._market_state())
                engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54))
                outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

                self.assertEqual(len(outputs), 1)
                self.assertIsInstance(outputs[0], OrderIntent)

                conn = sqlite3.connect(db_path)
                try:
                    run_count = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
                    market_row = conn.execute(
                        "SELECT market_status, condition_id, question FROM markets WHERE market_id = 'm1'"
                    ).fetchone()
                    order_row = conn.execute(
                        "SELECT state, price, size FROM orders WHERE order_id LIKE 'm1:tok_yes:0.55:%'"
                    ).fetchone()
                    event_count = conn.execute("SELECT COUNT(*) FROM events_log WHERE run_id = 'run_test_001'").fetchone()[0]
                    state_row = conn.execute(
                        "SELECT state_key, state_value_json FROM strategy_state WHERE run_id = 'run_test_001'"
                    ).fetchone()
                finally:
                    conn.close()

                self.assertEqual(run_count, 1)
                self.assertEqual(market_row[0], "active")
                self.assertEqual(market_row[1], "c1")
                self.assertEqual(market_row[2], "Bitcoin Up or Down?")
                self.assertEqual(order_row[0], "intent")
                self.assertAlmostEqual(order_row[1], 0.55)
                self.assertAlmostEqual(order_row[2], 10.0 / 0.55)
                self.assertEqual(event_count, 3)
                self.assertEqual(state_row[0], "first_touch_fired:0.55")
                self.assertIn('"level": 0.55', state_row[1])
            finally:
                store.close()

    def test_sqlite_repository_api_handles_transition_fill_and_reconciliation_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_test_002",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                store.upsert_market_state(self._market_state())
                store.insert_order(
                    order_id="ord_001",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.55,
                    size=18.1818,
                    state=OrderState.INTENT,
                    client_order_id="cli_001",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_000,
                    remaining_size=18.1818,
                )
                store.update_order_state(
                    order_id="ord_001",
                    state=OrderState.ACCEPTED,
                    event_time_ms=225_100,
                    venue_order_status="live",
                )
                store.insert_fill(
                    fill_id="fill_001",
                    order_id="ord_001",
                    market_id="m1",
                    token_id="tok_yes",
                    price=0.55,
                    size=10.0,
                    filled_at_ms=225_200,
                    source="replay",
                    fee_paid_usdc=0.05,
                    liquidity_role="taker",
                )
                store.update_order_state(
                    order_id="ord_001",
                    state=OrderState.PARTIALLY_FILLED,
                    event_time_ms=225_200,
                    cumulative_filled_size=10.0,
                    remaining_size=8.1818,
                )
                open_orders = store.load_non_terminal_orders()

                self.assertEqual(len(open_orders), 1)
                self.assertEqual(open_orders[0]["order_id"], "ord_001")
                self.assertEqual(open_orders[0]["state"], "partially_filled")

                conn = sqlite3.connect(db_path)
                try:
                    order_row = conn.execute(
                        "SELECT state, venue_order_status, cumulative_filled_size, remaining_size FROM orders WHERE order_id = 'ord_001'"
                    ).fetchone()
                    fill_row = conn.execute(
                        "SELECT price, size, fee_paid_usdc, liquidity_role, source FROM fills WHERE fill_id = 'fill_001'"
                    ).fetchone()
                finally:
                    conn.close()

                self.assertEqual(order_row[0], "partially_filled")
                self.assertEqual(order_row[1], "live")
                self.assertAlmostEqual(order_row[2], 10.0)
                self.assertAlmostEqual(order_row[3], 8.1818)
                self.assertAlmostEqual(fill_row[0], 0.55)
                self.assertAlmostEqual(fill_row[1], 10.0)
                self.assertAlmostEqual(fill_row[2], 0.05)
                self.assertEqual(fill_row[3], "taker")
                self.assertEqual(fill_row[4], "replay")
            finally:
                store.close()

    def test_runtime_logs_are_joinable_to_events_log_by_run_and_event_time(self) -> None:
        stream = io.StringIO()
        configure_logging(name="short_horizon", stream=stream)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_test_003",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                engine = ShortHorizonEngine(config=ShortHorizonConfig(), intent_store=store)
                engine.on_market_state(self._market_state())
                engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54))
                engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

                lines = [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]
                intent_log = next(line for line in lines if line.get("event") == "order_intent_created")

                conn = sqlite3.connect(db_path)
                try:
                    event_rows = conn.execute(
                        "SELECT run_id, event_time, market_id FROM events_log WHERE run_id = ? AND event_time = ? AND market_id = ?",
                        (intent_log["run_id"], intent_log["event_time"], intent_log["market_id"]),
                    ).fetchall()
                finally:
                    conn.close()

                self.assertEqual(intent_log["run_id"], "run_test_003")
                self.assertEqual(intent_log["order_id"], "m1:tok_yes:0.55:225000")
                self.assertGreaterEqual(len(event_rows), 1)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
