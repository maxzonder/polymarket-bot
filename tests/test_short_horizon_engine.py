from __future__ import annotations

import asyncio
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.config import ShortHorizonConfig
from short_horizon.core import EventType, OrderState
from short_horizon.engine import ShortHorizonEngine
from short_horizon.execution import ExecutionEngine, ExecutionMode, ExecutionTransitionError, ExecutionValidationError, SyntheticFillRequest, estimate_fee_usdc, is_valid_tick_size
from short_horizon.events import BookUpdate, MarketStateUpdate, OrderCanceled, OrderFilled, TimerEvent
from short_horizon.live_runner import build_live_source, build_parser, run_live, run_stub_live, validate_cli_args
from short_horizon.probe import assert_min_book_updates_per_minute, cross_validate_probe_against_collector, summarize_probe_db
from short_horizon.models import OrderIntent, SkipDecision
from short_horizon.replay_runner import replay_file
from short_horizon.storage import InMemoryIntentStore, RunContext, SQLiteRuntimeStore
from short_horizon.strategies import ShortHorizon15mTouchStrategy
from short_horizon.strategy_api import Noop, PlaceOrder
from short_horizon.telemetry import configure_logging, event_log_fields, get_logger
from short_horizon.venue_polymarket.execution_client import PRIVATE_KEY_ENV_VAR, VenueApiCredentials, VenuePlaceResult


class _AsyncNormalizedSource:
    def __init__(self, events):
        self._events = list(events)
        self._queue: asyncio.Queue[object] = asyncio.Queue()
        self._sentinel = object()
        self.started = False
        self.stopped = False

    @property
    def events(self):
        return self

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self._queue.get()
        if item is self._sentinel:
            raise StopAsyncIteration
        return item

    async def start(self) -> None:
        self.started = True
        for event in self._events:
            await self._queue.put(event)
        await self._queue.put(self._sentinel)

    async def stop(self) -> None:
        self.stopped = True
        await self._queue.put(self._sentinel)


class _FakeLiveRunnerClient:
    def __init__(self) -> None:
        self.started = False
        self.place_calls = []
        self.cancel_calls = []
        self.api_credentials_calls = 0

    def startup(self) -> None:
        self.started = True

    def api_credentials(self) -> VenueApiCredentials:
        self.api_credentials_calls += 1
        return VenueApiCredentials(api_key="api-key", secret="api-secret", passphrase="api-pass")

    def place_order(self, order_request):
        self.place_calls.append(order_request)
        return VenuePlaceResult(
            order_id="venue-live-1",
            status="live",
            client_order_id=order_request.client_order_id,
        )

    def cancel_order(self, order_id):
        self.cancel_calls.append(order_id)
        from short_horizon.venue_polymarket.execution_client import VenueCancelResult

        return VenueCancelResult(order_id=order_id, success=True, status="canceled")


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

    def test_touch_uses_fee_fetched_timestamp_when_age_not_materialized(self) -> None:
        self.engine.on_market_state(
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
                fee_fetched_at_ms=100_000,
                fee_metadata_age_ms=None,
            )
        )

        self.engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54))
        outputs = self.engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "stale_fee_metadata")


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


class _FakeExecutionClient:
    def __init__(self, *, place_result=None, error: Exception | None = None):
        self.place_result = place_result or {"order_id": "venue-ord-1", "status": "live"}
        self.error = error
        self.place_calls = []
        self.cancel_calls = []

    def place_order(self, order_request):
        self.place_calls.append(order_request)
        if self.error is not None:
            raise self.error
        from short_horizon.venue_polymarket.execution_client import VenuePlaceResult

        if isinstance(self.place_result, VenuePlaceResult):
            return self.place_result
        return VenuePlaceResult(
            order_id=self.place_result.get("order_id", "venue-ord-1"),
            status=self.place_result.get("status", "live"),
            client_order_id=self.place_result.get("client_order_id"),
            raw=self.place_result,
        )

    def cancel_order(self, order_id):
        self.cancel_calls.append(order_id)
        from short_horizon.venue_polymarket.execution_client import VenueCancelResult

        if self.error is not None:
            raise self.error
        return VenueCancelResult(order_id=order_id, success=True, status="canceled")


class ExecutionEngineTest(unittest.TestCase):
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

    def _intent(self) -> OrderIntent:
        return OrderIntent(
            intent_id="ord_exec_001",
            strategy_id="short_horizon_15m_touch_v1",
            market_id="m1",
            token_id="tok_yes",
            condition_id="c1",
            question="Bitcoin Up or Down?",
            asset_slug="bitcoin",
            level=0.55,
            entry_price=0.55,
            notional_usdc=10.0,
            lifecycle_fraction=0.25,
            event_time_ms=225_000,
        )

    def test_execution_boundary_handles_synthetic_accept_and_fill(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_001",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                store.upsert_market_state(self._market_state())
                store.persist_intent(self._intent())
                execution = ExecutionEngine(store=store)

                accepted_events = execution.submit(self._intent())
                self.assertEqual(len(accepted_events), 1)
                self.assertEqual(accepted_events[0].order_id, "ord_exec_001")

                fill_event = execution.apply_fill(SyntheticFillRequest(order_id="ord_exec_001", event_time_ms=225_100))
                self.assertEqual(fill_event.order_id, "ord_exec_001")
                self.assertAlmostEqual(fill_event.fill_size, 10.0 / 0.55)
                self.assertAlmostEqual(fill_event.remaining_size, 0.0)

                conn = sqlite3.connect(db_path)
                try:
                    order_row = conn.execute(
                        "SELECT state, venue_order_status, cumulative_filled_size, remaining_size FROM orders WHERE order_id = 'ord_exec_001'"
                    ).fetchone()
                    fill_count = conn.execute("SELECT COUNT(*) FROM fills WHERE order_id = 'ord_exec_001'").fetchone()[0]
                    event_types = [
                        row[0]
                        for row in conn.execute(
                            "SELECT event_type FROM events_log WHERE run_id = 'run_exec_001' ORDER BY seq ASC"
                        ).fetchall()
                    ]
                finally:
                    conn.close()

                self.assertEqual(order_row[0], "filled")
                self.assertEqual(order_row[1], "filled")
                self.assertAlmostEqual(order_row[2], 10.0 / 0.55)
                self.assertAlmostEqual(order_row[3], 0.0)
                self.assertEqual(fill_count, 1)
                self.assertIn("OrderAccepted", event_types)
                self.assertIn("OrderFilled", event_types)
            finally:
                store.close()

    def test_execution_boundary_rejects_illegal_intent_to_filled_transition(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_002",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                store.upsert_market_state(self._market_state())
                store.persist_intent(self._intent())
                execution = ExecutionEngine(store=store)

                with self.assertRaises(ExecutionTransitionError):
                    execution.apply_fill(SyntheticFillRequest(order_id="ord_exec_001", event_time_ms=225_050))
            finally:
                store.close()

    def test_execution_boundary_rejects_fill_after_cancel_confirmed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_003",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                store.upsert_market_state(self._market_state())
                store.persist_intent(self._intent())
                execution = ExecutionEngine(store=store)
                execution.submit(self._intent())
                cancel_event = execution.cancel(market_id="m1", token_id="tok_yes", event_time_ms=225_050, reason="test_cancel")

                self.assertIsNotNone(cancel_event)
                with self.assertRaises(ExecutionTransitionError):
                    execution.apply_fill(SyntheticFillRequest(order_id="ord_exec_001", event_time_ms=225_100))
            finally:
                store.close()

    def test_fee_math_rounds_up_conservatively(self) -> None:
        self.assertAlmostEqual(
            estimate_fee_usdc(price=0.333333, size=3.0, fee_rate_bps=25.0),
            0.0025,
        )
        self.assertAlmostEqual(
            estimate_fee_usdc(price=0.55, size=18.1818181818, fee_rate_bps=10.0),
            0.01,
        )

    def test_tick_size_validation_accepts_only_tick_aligned_prices(self) -> None:
        self.assertTrue(is_valid_tick_size(0.55, 0.01))
        self.assertFalse(is_valid_tick_size(0.555, 0.01))

    def test_execution_boundary_rejects_sub_tick_price(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_004",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                store.upsert_market_state(self._market_state())
                store.insert_order(
                    order_id="ord_exec_bad_tick",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.555,
                    size=10.0,
                    state=OrderState.INTENT,
                    client_order_id="ord_exec_bad_tick",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_000,
                    remaining_size=10.0,
                )
                execution = ExecutionEngine(store=store)
                bad_intent = self._intent().__class__(
                    **{**self._intent().__dict__, "intent_id": "ord_exec_bad_tick", "entry_price": 0.555}
                )

                with self.assertRaises(ExecutionValidationError):
                    execution.submit(bad_intent)
            finally:
                store.close()

    def test_execution_boundary_rejects_sub_min_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_005",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                store.upsert_market_state(self._market_state())
                store.insert_order(
                    order_id="ord_exec_too_small",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.55,
                    size=0.5,
                    state=OrderState.INTENT,
                    client_order_id="ord_exec_too_small",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_000,
                    remaining_size=0.5,
                )
                execution = ExecutionEngine(store=store, min_order_size=1.0)
                bad_intent = self._intent().__class__(
                    **{**self._intent().__dict__, "intent_id": "ord_exec_too_small"}
                )

                with self.assertRaises(ExecutionValidationError):
                    execution.submit(bad_intent)
            finally:
                store.close()

    def test_execution_boundary_dry_run_logs_translated_order_without_hitting_venue(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_dry_001",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            client = _FakeExecutionClient()
            try:
                store.upsert_market_state(self._market_state())
                store.persist_intent(self._intent())
                execution = ExecutionEngine(store=store, client=client, mode=ExecutionMode.DRY_RUN)

                events = execution.submit(self._intent())

                self.assertEqual(len(client.place_calls), 0)
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0].event_type, EventType.ORDER_ACCEPTED)
                conn = sqlite3.connect(db_path)
                try:
                    order_row = conn.execute(
                        "SELECT state, client_order_id, venue_order_id, venue_order_status FROM orders WHERE order_id = 'ord_exec_001'"
                    ).fetchone()
                    dry_run_rows = conn.execute(
                        "SELECT event_type, payload_json FROM events_log WHERE run_id = 'run_exec_dry_001' ORDER BY seq ASC"
                    ).fetchall()
                finally:
                    conn.close()

                self.assertEqual(order_row[0], "accepted")
                self.assertIsNotNone(order_row[1])
                self.assertIsNone(order_row[2])
                self.assertEqual(order_row[3], "accepted")
                dry_payloads = [json.loads(row[1]) for row in dry_run_rows if row[0] == "TimerEvent"]
                self.assertTrue(any(payload.get("timer_kind") == "dry_run_order_logged" for payload in dry_payloads))
            finally:
                store.close()

    def test_execution_boundary_live_mode_binds_venue_order_id_on_accept(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_live_001",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            client = _FakeExecutionClient(place_result={"order_id": "venue-123", "status": "live"})
            try:
                store.upsert_market_state(self._market_state())
                store.persist_intent(self._intent())
                execution = ExecutionEngine(store=store, client=client, mode=ExecutionMode.LIVE)

                events = execution.submit(self._intent())

                self.assertEqual(len(client.place_calls), 1)
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0].order_id, "ord_exec_001")
                conn = sqlite3.connect(db_path)
                try:
                    order_row = conn.execute(
                        "SELECT state, client_order_id, venue_order_id, venue_order_status FROM orders WHERE order_id = 'ord_exec_001'"
                    ).fetchone()
                finally:
                    conn.close()

                self.assertEqual(order_row[0], "accepted")
                self.assertIsNotNone(order_row[1])
                self.assertEqual(order_row[2], "venue-123")
                self.assertEqual(order_row[3], "live")
            finally:
                store.close()

    def test_execution_boundary_live_mode_maps_venue_error_to_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_live_reject_001",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            client = _FakeExecutionClient(error=RuntimeError("insufficient balance"))
            try:
                store.upsert_market_state(self._market_state())
                store.persist_intent(self._intent())
                execution = ExecutionEngine(store=store, client=client, mode=ExecutionMode.LIVE)

                events = execution.submit(self._intent())

                self.assertEqual(len(events), 1)
                self.assertEqual(events[0].event_type, EventType.ORDER_REJECTED)
                conn = sqlite3.connect(db_path)
                try:
                    order_row = conn.execute(
                        "SELECT state, venue_order_id, last_reject_code, last_reject_reason FROM orders WHERE order_id = 'ord_exec_001'"
                    ).fetchone()
                finally:
                    conn.close()

                self.assertEqual(order_row[0], "rejected")
                self.assertIsNone(order_row[1])
                self.assertEqual(order_row[2], "VENUE_ERROR")
                self.assertIn("insufficient balance", order_row[3])
            finally:
                store.close()

    def test_execution_boundary_live_mode_cancels_by_venue_order_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_live_cancel_001",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            client = _FakeExecutionClient(place_result={"order_id": "venue-123", "status": "live"})
            try:
                store.upsert_market_state(self._market_state())
                store.persist_intent(self._intent())
                execution = ExecutionEngine(store=store, client=client, mode=ExecutionMode.LIVE)
                execution.submit(self._intent())

                cancel_event = execution.cancel(
                    market_id="m1",
                    token_id="tok_yes",
                    event_time_ms=225_050,
                    reason="test_cancel",
                )

                self.assertIsNotNone(cancel_event)
                self.assertEqual(client.cancel_calls, ["venue-123"])
                conn = sqlite3.connect(db_path)
                try:
                    order_row = conn.execute(
                        "SELECT state, venue_order_id, venue_order_status, reconciliation_required FROM orders WHERE order_id = 'ord_exec_001'"
                    ).fetchone()
                finally:
                    conn.close()

                self.assertEqual(order_row[0], "cancel_confirmed")
                self.assertEqual(order_row[1], "venue-123")
                self.assertEqual(order_row[2], "canceled")
                self.assertEqual(order_row[3], 0)
            finally:
                store.close()

    def test_execution_boundary_reconciles_user_stream_fill_by_venue_order_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_live_fill_001",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            client = _FakeExecutionClient(place_result={"order_id": "venue-123", "status": "live"})
            try:
                store.upsert_market_state(self._market_state())
                store.persist_intent(self._intent())
                execution = ExecutionEngine(store=store, client=client, mode=ExecutionMode.LIVE)
                execution.submit(self._intent())

                reconciled = execution.reconcile_order_event(
                    OrderFilled(
                        event_time_ms=225_100,
                        ingest_time_ms=225_110,
                        order_id="venue-123",
                        market_id="m1",
                        token_id="tok_yes",
                        side="BUY",
                        source="polymarket_clob_user_ws",
                        client_order_id="ord_exec_001",
                        fill_price=0.55,
                        fill_size=10.0 / 0.55,
                        cumulative_filled_size=10.0 / 0.55,
                        remaining_size=0.0,
                    )
                )

                self.assertIsNotNone(reconciled)
                self.assertEqual(reconciled.order_id, "ord_exec_001")
                conn = sqlite3.connect(db_path)
                try:
                    order_row = conn.execute(
                        "SELECT state, venue_order_id, venue_order_status, cumulative_filled_size, remaining_size FROM orders WHERE order_id = 'ord_exec_001'"
                    ).fetchone()
                    fill_count = conn.execute("SELECT COUNT(*) FROM fills WHERE order_id = 'ord_exec_001'").fetchone()[0]
                finally:
                    conn.close()

                self.assertEqual(order_row[0], "filled")
                self.assertEqual(order_row[1], "venue-123")
                self.assertEqual(order_row[2], "filled")
                self.assertAlmostEqual(order_row[3], 10.0 / 0.55)
                self.assertAlmostEqual(order_row[4], 0.0)
                self.assertEqual(fill_count, 1)
            finally:
                store.close()


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

    def test_sqlite_store_persists_venue_order_id_without_mutating_primary_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_test_venue_id",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                store.insert_order(
                    order_id="local_ord_001",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.55,
                    size=18.18,
                    state=OrderState.PENDING_SEND,
                    client_order_id="cli_ord_001",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_000,
                    remaining_size=18.18,
                )
                store.update_order_state(
                    order_id="local_ord_001",
                    state=OrderState.ACCEPTED,
                    event_time_ms=225_001,
                    venue_order_id="venue_ord_123",
                    venue_order_status="accepted",
                )

                conn = sqlite3.connect(db_path)
                try:
                    row = conn.execute(
                        "SELECT order_id, client_order_id, venue_order_id, state FROM orders WHERE order_id = 'local_ord_001'"
                    ).fetchone()
                finally:
                    conn.close()

                self.assertEqual(row[0], "local_ord_001")
                self.assertEqual(row[1], "cli_ord_001")
                self.assertEqual(row[2], "venue_ord_123")
                self.assertEqual(row[3], "accepted")
            finally:
                store.close()

    def test_sqlite_store_migrates_legacy_orders_schema_to_add_venue_order_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "legacy.sqlite3"
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE runs (
                        run_id TEXT PRIMARY KEY,
                        started_at TEXT NOT NULL,
                        finished_at TEXT,
                        mode TEXT NOT NULL,
                        strategy_id TEXT NOT NULL,
                        git_sha TEXT,
                        config_hash TEXT NOT NULL,
                        notes TEXT
                    );
                    CREATE TABLE markets (
                        market_id TEXT PRIMARY KEY,
                        market_status TEXT,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE orders (
                        order_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        market_id TEXT NOT NULL,
                        token_id TEXT NOT NULL,
                        side TEXT NOT NULL,
                        price REAL,
                        size REAL,
                        state TEXT NOT NULL,
                        client_order_id TEXT,
                        parent_order_id TEXT,
                        intent_created_at TEXT NOT NULL,
                        last_state_change_at TEXT NOT NULL,
                        venue_order_status TEXT,
                        cumulative_filled_size REAL NOT NULL DEFAULT 0,
                        remaining_size REAL,
                        last_reject_code TEXT,
                        last_reject_reason TEXT,
                        reconciliation_required INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE fills (
                        fill_id TEXT PRIMARY KEY,
                        order_id TEXT NOT NULL,
                        run_id TEXT NOT NULL,
                        market_id TEXT NOT NULL,
                        token_id TEXT NOT NULL,
                        price REAL NOT NULL,
                        size REAL NOT NULL,
                        fee_paid_usdc REAL,
                        liquidity_role TEXT,
                        filled_at TEXT NOT NULL,
                        source TEXT NOT NULL,
                        venue_fill_id TEXT
                    );
                    CREATE TABLE events_log (
                        run_id TEXT NOT NULL,
                        seq INTEGER NOT NULL,
                        event_type TEXT NOT NULL,
                        event_time TEXT NOT NULL,
                        ingest_time TEXT NOT NULL,
                        source TEXT NOT NULL,
                        market_id TEXT,
                        token_id TEXT,
                        order_id TEXT,
                        payload_json TEXT NOT NULL,
                        PRIMARY KEY (run_id, seq)
                    );
                    CREATE TABLE strategy_state (
                        run_id TEXT NOT NULL,
                        strategy_id TEXT NOT NULL,
                        market_id TEXT NOT NULL,
                        token_id TEXT,
                        state_key TEXT NOT NULL,
                        state_value_json TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (run_id, strategy_id, market_id, token_id, state_key)
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_test_migrate",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                conn = sqlite3.connect(db_path)
                try:
                    columns = [row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()]
                    indexes = [row[1] for row in conn.execute("PRAGMA index_list(orders)").fetchall()]
                finally:
                    conn.close()

                self.assertIn("venue_order_id", columns)
                self.assertIn("idx_orders_venue_order_id", indexes)
            finally:
                store.close()


class ReplayRunnerTest(unittest.TestCase):
    def _sample_replay_rows(self) -> list[dict[str, object]]:
        sample_events: list[dict[str, object]] = [
            {
                "event_type": "MarketStateUpdate",
                "event_time": 200000,
                "ingest_time": 200050,
                "market_id": "m1",
                "token_id": "tok_yes",
                "condition_id": "c1",
                "question": "Bitcoin Up or Down?",
                "asset_slug": "bitcoin",
                "status": "active",
                "start_time": 0,
                "end_time": 900000,
                "is_active": True,
                "metadata_is_fresh": True,
                "fee_rate_bps": 10.0,
                "fee_metadata_age_ms": 1000,
                "source": "sample.market_state",
            },
            {
                "event_type": "BookUpdate",
                "event_time": 220000,
                "ingest_time": 220020,
                "market_id": "m1",
                "token_id": "tok_yes",
                "best_bid": 0.53,
                "best_ask": 0.54,
                "source": "sample.book",
            },
            {
                "event_type": "BookUpdate",
                "event_time": 225000,
                "ingest_time": 225020,
                "market_id": "m1",
                "token_id": "tok_yes",
                "best_bid": 0.54,
                "best_ask": 0.55,
                "source": "sample.book",
            },
        ]
        for offset in range(3, 10):
            event_time = 225000 + offset * 1000
            sample_events.append(
                {
                    "event_type": "BookUpdate",
                    "event_time": event_time,
                    "ingest_time": event_time + 20,
                    "market_id": "m1",
                    "token_id": "tok_yes",
                    "best_bid": 0.55,
                    "best_ask": 0.56,
                    "source": "sample.book",
                }
            )
        return sample_events

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

    def test_replay_runner_replays_jsonl_into_fresh_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            event_log_path = tmp_path / "sample_events.jsonl"
            db_path = tmp_path / "replay.sqlite3"
            event_log_path.write_text("\n".join(json.dumps(row) for row in self._sample_replay_rows()) + "\n", encoding="utf-8")

            summary = replay_file(
                event_log_path=event_log_path,
                db_path=db_path,
                run_id="replay_test_001",
                config_hash="test-config",
            )

            self.assertEqual(summary.run_id, "replay_test_001")
            self.assertEqual(summary.event_count, 10)
            self.assertEqual(summary.order_intents, 1)
            self.assertEqual(summary.synthetic_order_events, 1)

            conn = sqlite3.connect(db_path)
            try:
                run_row = conn.execute(
                    "SELECT run_id, mode, strategy_id, finished_at FROM runs WHERE run_id = 'replay_test_001'"
                ).fetchone()
                event_count = conn.execute(
                    "SELECT COUNT(*) FROM events_log WHERE run_id = 'replay_test_001'"
                ).fetchone()[0]
                order_row = conn.execute(
                    "SELECT state, venue_order_status FROM orders WHERE run_id = 'replay_test_001' AND order_id = 'm1:tok_yes:0.55:225000'"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(run_row[0], "replay_test_001")
            self.assertEqual(run_row[1], "replay")
            self.assertEqual(run_row[2], "short_horizon_15m_touch_v1")
            self.assertIsNotNone(run_row[3])
            self.assertEqual(event_count, 11)
            self.assertEqual(order_row[0], "accepted")
            self.assertEqual(order_row[1], "accepted")

    def test_live_runner_shell_uses_same_stub_event_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            event_log_path = tmp_path / "stub_live_events.jsonl"
            db_path = tmp_path / "live.sqlite3"
            event_log_path.write_text("\n".join(json.dumps(row) for row in self._sample_replay_rows()) + "\n", encoding="utf-8")

            summary = run_stub_live(
                stub_event_log_path=event_log_path,
                db_path=db_path,
                run_id="live_test_001",
                config_hash="test-config",
            )

            self.assertEqual(summary.run_id, "live_test_001")
            self.assertEqual(summary.event_count, 10)
            self.assertEqual(summary.order_intents, 1)
            self.assertEqual(summary.synthetic_order_events, 1)

            conn = sqlite3.connect(db_path)
            try:
                run_row = conn.execute(
                    "SELECT run_id, mode, finished_at FROM runs WHERE run_id = 'live_test_001'"
                ).fetchone()
                event_count = conn.execute(
                    "SELECT COUNT(*) FROM events_log WHERE run_id = 'live_test_001'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(run_row[0], "live_test_001")
            self.assertEqual(run_row[1], "live")
            self.assertIsNotNone(run_row[2])
            self.assertEqual(event_count, 11)

    def test_live_runner_parser_supports_stub_and_live_modes(self) -> None:
        parser = build_parser()

        stub_args = parser.parse_args(["live.sqlite3", "--mode", "stub", "--execution-mode", "dry_run", "--stub-event-log-path", "sample.jsonl"])
        self.assertEqual(stub_args.db_path, "live.sqlite3")
        self.assertEqual(stub_args.mode, "stub")
        self.assertEqual(stub_args.execution_mode, "dry_run")
        self.assertEqual(stub_args.stub_event_log_path, "sample.jsonl")

        live_args = parser.parse_args(["live.sqlite3", "--mode", "live", "--execution-mode", "live", "--max-events", "5", "--max-runtime-seconds", "15", "--collector-csv", "collector.csv"])
        self.assertEqual(live_args.mode, "live")
        self.assertEqual(live_args.execution_mode, "live")
        self.assertEqual(live_args.max_events, 5)
        self.assertEqual(live_args.max_runtime_seconds, 15.0)
        self.assertEqual(live_args.collector_csv, "collector.csv")

    def test_live_runner_cli_validation_requires_live_input_for_live_execution(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["live.sqlite3", "--mode", "stub", "--execution-mode", "live", "--stub-event-log-path", "sample.jsonl"])

        with self.assertRaises(SystemExit):
            validate_cli_args(parser, args)

    def test_live_runner_cli_validation_requires_private_key_for_live_execution(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["live.sqlite3", "--mode", "live", "--execution-mode", "live"])

        env_backup = os.environ.get(PRIVATE_KEY_ENV_VAR)
        try:
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(SystemExit):
                    validate_cli_args(parser, args)
        finally:
            if env_backup is not None:
                os.environ[PRIVATE_KEY_ENV_VAR] = env_backup

    def test_replay_runner_is_deterministic_for_same_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            event_log_path = tmp_path / "deterministic_events.jsonl"
            db_path_a = tmp_path / "replay_a.sqlite3"
            db_path_b = tmp_path / "replay_b.sqlite3"
            event_log_path.write_text("\n".join(json.dumps(row) for row in self._sample_replay_rows()) + "\n", encoding="utf-8")

            replay_file(
                event_log_path=event_log_path,
                db_path=db_path_a,
                run_id="replay_deterministic",
                config_hash="test-config",
            )
            replay_file(
                event_log_path=event_log_path,
                db_path=db_path_b,
                run_id="replay_deterministic",
                config_hash="test-config",
            )

            conn_a = sqlite3.connect(db_path_a)
            conn_b = sqlite3.connect(db_path_b)
            try:
                events_a = conn_a.execute(
                    "SELECT seq, event_type, event_time, ingest_time, market_id, token_id, order_id, payload_json FROM events_log WHERE run_id = 'replay_deterministic' ORDER BY seq ASC"
                ).fetchall()
                events_b = conn_b.execute(
                    "SELECT seq, event_type, event_time, ingest_time, market_id, token_id, order_id, payload_json FROM events_log WHERE run_id = 'replay_deterministic' ORDER BY seq ASC"
                ).fetchall()
                orders_a = conn_a.execute(
                    "SELECT order_id, state, price, size, venue_order_status FROM orders WHERE run_id = 'replay_deterministic' ORDER BY order_id ASC"
                ).fetchall()
                orders_b = conn_b.execute(
                    "SELECT order_id, state, price, size, venue_order_status FROM orders WHERE run_id = 'replay_deterministic' ORDER BY order_id ASC"
                ).fetchall()
            finally:
                conn_a.close()
                conn_b.close()

            self.assertEqual(events_a, events_b)
            self.assertEqual(orders_a, orders_b)


class LiveRunnerAsyncTest(unittest.IsolatedAsyncioTestCase):
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

    async def test_live_runner_real_mode_uses_live_event_source_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_real.sqlite3"
            source = _AsyncNormalizedSource(
                [
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
                        fee_fetched_at_ms=200_050,
                        fee_metadata_age_ms=0,
                    ),
                    BookUpdate(
                        event_time_ms=220_000,
                        ingest_time_ms=220_020,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.53,
                        best_ask=0.54,
                    ),
                    BookUpdate(
                        event_time_ms=225_000,
                        ingest_time_ms=225_020,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.54,
                        best_ask=0.55,
                    ),
                ]
            )

            summary = await run_live(
                db_path=db_path,
                run_id="live_real_test_001",
                config_hash="test-config",
                source=source,
                max_events=3,
            )

            self.assertEqual(summary.run_id, "live_real_test_001")
            self.assertEqual(summary.event_count, 3)
            self.assertEqual(summary.order_intents, 1)
            self.assertEqual(summary.synthetic_order_events, 1)
            self.assertTrue(source.started)
            self.assertTrue(source.stopped)

            conn = sqlite3.connect(db_path)
            try:
                run_row = conn.execute(
                    "SELECT run_id, mode FROM runs WHERE run_id = 'live_real_test_001'"
                ).fetchone()
                event_count = conn.execute(
                    "SELECT COUNT(*) FROM events_log WHERE run_id = 'live_real_test_001'"
                ).fetchone()[0]
                order_row = conn.execute(
                    "SELECT state, venue_order_status FROM orders WHERE run_id = 'live_real_test_001' AND order_id = 'm1:tok_yes:0.55:225000'"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(run_row[0], "live_real_test_001")
            self.assertEqual(run_row[1], "live")
            self.assertEqual(event_count, 4)
            self.assertEqual(order_row[0], "accepted")
            self.assertEqual(order_row[1], "accepted")

    async def test_live_runner_dry_run_mode_logs_translated_order_without_hitting_venue(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_dry_run.sqlite3"
            source = _AsyncNormalizedSource(
                [
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
                        fee_fetched_at_ms=200_050,
                        fee_metadata_age_ms=0,
                    ),
                    BookUpdate(
                        event_time_ms=220_000,
                        ingest_time_ms=220_020,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.53,
                        best_ask=0.54,
                    ),
                    BookUpdate(
                        event_time_ms=225_000,
                        ingest_time_ms=225_020,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.54,
                        best_ask=0.55,
                    ),
                ]
            )

            summary = await run_live(
                db_path=db_path,
                run_id="live_dry_run_test_001",
                config_hash="test-config",
                source=source,
                max_events=3,
                execution_mode=ExecutionMode.DRY_RUN,
            )

            self.assertEqual(summary.run_id, "live_dry_run_test_001")
            self.assertEqual(summary.event_count, 3)
            self.assertEqual(summary.order_intents, 1)
            self.assertEqual(summary.synthetic_order_events, 1)

            conn = sqlite3.connect(db_path)
            try:
                event_rows = conn.execute(
                    "SELECT event_type, payload_json FROM events_log WHERE run_id = 'live_dry_run_test_001' ORDER BY seq ASC"
                ).fetchall()
                order_row = conn.execute(
                    "SELECT state, client_order_id, venue_order_id, venue_order_status FROM orders WHERE run_id = 'live_dry_run_test_001' AND order_id = 'm1:tok_yes:0.55:225000'"
                ).fetchone()
            finally:
                conn.close()

            event_types = [row[0] for row in event_rows]
            self.assertEqual(event_types.count("TimerEvent"), 1)
            self.assertEqual(event_types.count("OrderAccepted"), 1)
            self.assertEqual(order_row[0], "accepted")
            self.assertIsNotNone(order_row[1])
            self.assertIsNone(order_row[2])
            self.assertEqual(order_row[3], "accepted")
            timer_payloads = [json.loads(row[1]) for row in event_rows if row[0] == "TimerEvent"]
            self.assertTrue(any(payload.get("timer_kind") == "dry_run_order_logged" for payload in timer_payloads))

    async def test_live_runner_live_execution_mode_starts_client_and_persists_venue_order_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_exec.sqlite3"
            source = _AsyncNormalizedSource(
                [
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
                        fee_fetched_at_ms=200_050,
                        fee_metadata_age_ms=0,
                    ),
                    BookUpdate(
                        event_time_ms=220_000,
                        ingest_time_ms=220_020,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.53,
                        best_ask=0.54,
                    ),
                    BookUpdate(
                        event_time_ms=225_000,
                        ingest_time_ms=225_020,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.54,
                        best_ask=0.55,
                    ),
                ]
            )
            client = _FakeLiveRunnerClient()

            summary = await run_live(
                db_path=db_path,
                run_id="live_exec_test_001",
                config_hash="test-config",
                source=source,
                max_events=3,
                execution_mode=ExecutionMode.LIVE,
                execution_client=client,
            )

            self.assertEqual(summary.run_id, "live_exec_test_001")
            self.assertTrue(client.started)
            self.assertEqual(len(client.place_calls), 1)

            conn = sqlite3.connect(db_path)
            try:
                order_row = conn.execute(
                    "SELECT state, client_order_id, venue_order_id, venue_order_status FROM orders WHERE run_id = 'live_exec_test_001' AND order_id = 'm1:tok_yes:0.55:225000'"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(order_row[0], "accepted")
            self.assertIsNotNone(order_row[1])
            self.assertEqual(order_row[2], "venue-live-1")
            self.assertEqual(order_row[3], "live")

    async def test_live_runner_live_execution_mode_reconciles_user_stream_cancel_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_exec_cancel.sqlite3"
            source = _AsyncNormalizedSource(
                [
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
                        fee_fetched_at_ms=200_050,
                        fee_metadata_age_ms=0,
                    ),
                    BookUpdate(
                        event_time_ms=220_000,
                        ingest_time_ms=220_020,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.53,
                        best_ask=0.54,
                    ),
                    BookUpdate(
                        event_time_ms=225_000,
                        ingest_time_ms=225_020,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.54,
                        best_ask=0.55,
                    ),
                    OrderCanceled(
                        event_time_ms=225_100,
                        ingest_time_ms=225_110,
                        order_id="venue-live-1",
                        market_id="m1",
                        token_id="tok_yes",
                        source="polymarket_clob_user_ws",
                        client_order_id="m1:tok_yes:0.55:225000",
                        cancel_reason="user_canceled",
                        cumulative_filled_size=0.0,
                        remaining_size=10.0 / 0.55,
                    ),
                ]
            )
            client = _FakeLiveRunnerClient()

            summary = await run_live(
                db_path=db_path,
                run_id="live_exec_cancel_test_001",
                config_hash="test-config",
                source=source,
                max_events=4,
                execution_mode=ExecutionMode.LIVE,
                execution_client=client,
            )

            self.assertEqual(summary.run_id, "live_exec_cancel_test_001")
            conn = sqlite3.connect(db_path)
            try:
                order_row = conn.execute(
                    "SELECT state, venue_order_id, venue_order_status, cumulative_filled_size FROM orders WHERE run_id = 'live_exec_cancel_test_001' AND order_id = 'm1:tok_yes:0.55:225000'"
                ).fetchone()
                canceled_events = conn.execute(
                    "SELECT COUNT(*) FROM events_log WHERE run_id = 'live_exec_cancel_test_001' AND event_type = 'OrderCanceled' AND order_id = 'm1:tok_yes:0.55:225000'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(order_row[0], "cancel_confirmed")
            self.assertEqual(order_row[1], "venue-live-1")
            self.assertEqual(order_row[2], "canceled")
            self.assertAlmostEqual(order_row[3], 0.0)
            self.assertEqual(canceled_events, 1)

    def test_build_live_source_live_mode_requires_client_api_credentials(self) -> None:
        with self.assertRaises(ValueError):
            build_live_source(execution_mode=ExecutionMode.LIVE, execution_client=None)

        with self.assertRaises(TypeError):
            build_live_source(execution_mode=ExecutionMode.LIVE, execution_client=object())

    async def test_live_runner_live_execution_mode_builds_authenticated_user_stream_when_source_omitted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_exec_default_source.sqlite3"
            client = _FakeLiveRunnerClient()
            captured = {}
            events = [
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
                    fee_fetched_at_ms=200_050,
                    fee_metadata_age_ms=0,
                ),
                BookUpdate(
                    event_time_ms=220_000,
                    ingest_time_ms=220_020,
                    market_id="m1",
                    token_id="tok_yes",
                    best_bid=0.53,
                    best_ask=0.54,
                ),
                BookUpdate(
                    event_time_ms=225_000,
                    ingest_time_ms=225_020,
                    market_id="m1",
                    token_id="tok_yes",
                    best_bid=0.54,
                    best_ask=0.55,
                ),
            ]

            class _PatchedUserStream:
                def __init__(self, *, auth):
                    captured["auth"] = auth

            class _PatchedLiveEventSource(_AsyncNormalizedSource):
                def __init__(self, *, user_stream=None):
                    captured["user_stream"] = user_stream
                    super().__init__(events)

            with patch("short_horizon.live_runner.PolymarketUserStream", _PatchedUserStream), patch(
                "short_horizon.live_runner.LiveEventSource", _PatchedLiveEventSource
            ):
                summary = await run_live(
                    db_path=db_path,
                    run_id="live_exec_default_source_test_001",
                    config_hash="test-config",
                    source=None,
                    max_events=3,
                    execution_mode=ExecutionMode.LIVE,
                    execution_client=client,
                )

            self.assertEqual(summary.run_id, "live_exec_default_source_test_001")
            self.assertTrue(client.started)
            self.assertEqual(client.api_credentials_calls, 1)
            self.assertIsInstance(captured["user_stream"], _PatchedUserStream)
            self.assertEqual(
                captured["auth"],
                VenueApiCredentials(api_key="api-key", secret="api-secret", passphrase="api-pass"),
            )

    async def test_live_runner_probe_summary_and_collector_cross_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_probe.sqlite3"
            collector_csv = Path(tmpdir) / "collector.csv"
            collector_csv.write_text(
                "market_id,token_id\n"
                "m1,tok_yes\n"
                "m9,tok_other\n",
                encoding="utf-8",
            )
            source = _AsyncNormalizedSource(
                [
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
                        fee_fetched_at_ms=200_050,
                        fee_metadata_age_ms=0,
                    ),
                    BookUpdate(
                        event_time_ms=220_000,
                        ingest_time_ms=220_020,
                        market_id="m1",
                        token_id="tok_yes",
                        best_bid=0.53,
                        best_ask=0.54,
                    ),
                ]
            )

            summary = await run_live(
                db_path=db_path,
                run_id="live_probe_test_001",
                config_hash="test-config",
                source=source,
                max_runtime_seconds=1.0,
            )

            self.assertEqual(summary.event_count, 2)

            probe_summary = summarize_probe_db(db_path, run_id="live_probe_test_001")
            self.assertEqual(probe_summary.total_events, 2)
            self.assertEqual(probe_summary.market_state_updates, 1)
            self.assertEqual(probe_summary.book_updates, 1)
            self.assertEqual(probe_summary.trade_ticks, 0)
            self.assertEqual(probe_summary.distinct_markets, 1)
            self.assertEqual(probe_summary.distinct_tokens, 1)
            self.assertIsNotNone(probe_summary.first_event_time)
            self.assertIsNotNone(probe_summary.last_event_time)
            self.assertIsNotNone(probe_summary.window_minutes)
            self.assertIsNotNone(probe_summary.book_updates_per_minute)
            assert_min_book_updates_per_minute(probe_summary, min_rate=0.1)
            with self.assertRaises(AssertionError):
                assert_min_book_updates_per_minute(probe_summary, min_rate=100.0)

            validation = cross_validate_probe_against_collector(
                db_path,
                run_id="live_probe_test_001",
                collector_csv_path=collector_csv,
            )
            self.assertEqual(validation.collector_rows, 2)
            self.assertEqual(validation.probe_markets, 1)
            self.assertEqual(validation.collector_markets, 2)
            self.assertEqual(validation.overlapping_markets, 1)
            self.assertEqual(validation.probe_tokens, 1)
            self.assertEqual(validation.collector_tokens, 2)
            self.assertEqual(validation.overlapping_tokens, 1)

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
                by_client_order = store.load_order_by_client_order_id("cli_001")

                store.update_order_state(
                    order_id="ord_001",
                    state=OrderState.PARTIALLY_FILLED,
                    event_time_ms=225_201,
                    venue_order_id="venue_001",
                )
                by_venue_order = store.load_order_by_venue_order_id("venue_001")

                self.assertEqual(len(open_orders), 1)
                self.assertEqual(open_orders[0]["order_id"], "ord_001")
                self.assertEqual(open_orders[0]["state"], "partially_filled")
                self.assertEqual(by_client_order["order_id"], "ord_001")
                self.assertEqual(by_venue_order["order_id"], "ord_001")

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
