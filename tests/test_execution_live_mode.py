from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.core import OrderState
from short_horizon.events import MarketStateUpdate, OrderCanceled, OrderFilled
from short_horizon.execution import ExecutionEngine, ExecutionMode
from short_horizon.models import OrderIntent
from short_horizon.storage import RunContext, SQLiteRuntimeStore
from short_horizon.venue_polymarket.execution_client import VenueCancelResult, VenueOrderState, VenuePlaceResult


class _FakeExecutionClient:
    def __init__(
        self,
        *,
        place_result: VenuePlaceResult | dict | None = None,
        cancel_result: VenueCancelResult | dict | None = None,
        place_error: Exception | None = None,
        order_lookup_by_id: dict[str, VenueOrderState] | None = None,
        open_orders_by_market: dict[str | None, list[VenueOrderState]] | None = None,
    ) -> None:
        self.place_result = place_result or {"order_id": "venue-123", "status": "live"}
        self.cancel_result = cancel_result or {"order_id": "venue-123", "success": True, "status": "canceled"}
        self.place_error = place_error
        self.order_lookup_by_id = dict(order_lookup_by_id or {})
        self.open_orders_by_market = dict(open_orders_by_market or {})
        self.place_calls = []
        self.cancel_calls = []
        self.get_order_calls = []
        self.list_open_orders_calls = []

    def place_order(self, order_request):
        self.place_calls.append(order_request)
        if self.place_error is not None:
            raise self.place_error
        if isinstance(self.place_result, VenuePlaceResult):
            return self.place_result
        return VenuePlaceResult(
            order_id=str(self.place_result.get("order_id", "venue-123")),
            status=str(self.place_result.get("status", "live")),
            client_order_id=self.place_result.get("client_order_id"),
            raw=dict(self.place_result),
        )

    def cancel_order(self, order_id: str):
        self.cancel_calls.append(order_id)
        if isinstance(self.cancel_result, VenueCancelResult):
            return self.cancel_result
        payload = dict(self.cancel_result)
        return VenueCancelResult(
            order_id=str(payload.get("order_id", order_id)),
            success=bool(payload.get("success", True)),
            status=str(payload.get("status", "canceled")),
            raw=payload,
        )

    def get_order(self, order_id: str):
        self.get_order_calls.append(order_id)
        if order_id in self.order_lookup_by_id:
            return self.order_lookup_by_id[order_id]
        raise RuntimeError(f"unknown order {order_id}")

    def list_open_orders(self, market_id: str | None = None):
        self.list_open_orders_calls.append(market_id)
        return list(self.open_orders_by_market.get(market_id, []))


class ExecutionLiveModeIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._stores: list[SQLiteRuntimeStore] = []

    def tearDown(self) -> None:
        for store in self._stores:
            store.close()
        self._tmpdir.cleanup()

    def _create_store(self, run_id: str) -> SQLiteRuntimeStore:
        store = SQLiteRuntimeStore(
            Path(self._tmpdir.name) / f"{run_id}.sqlite3",
            run=RunContext(
                run_id=run_id,
                strategy_id="short_horizon_15m_touch_v1",
                config_hash="test-config",
            ),
        )
        self._stores.append(store)
        return store

    @staticmethod
    def _market_state() -> MarketStateUpdate:
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

    @staticmethod
    def _intent(intent_id: str = "ord_exec_001") -> OrderIntent:
        return OrderIntent(
            intent_id=intent_id,
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

    def _prepare_live_execution(self, *, run_id: str, client: _FakeExecutionClient):
        store = self._create_store(run_id)
        store.upsert_market_state(self._market_state())
        intent = self._intent()
        store.persist_intent(intent)
        execution = ExecutionEngine(store=store, client=client, mode=ExecutionMode.LIVE)
        return store, intent, execution

    @staticmethod
    def _fill_size() -> float:
        return 10.0 / 0.55

    def _load_order(self, store: SQLiteRuntimeStore, order_id: str = "ord_exec_001") -> dict:
        row = store.load_order(order_id)
        self.assertIsNotNone(row)
        return row

    def _fill_count(self, store: SQLiteRuntimeStore, order_id: str = "ord_exec_001") -> int:
        return int(
            store.conn.execute(
                "SELECT COUNT(*) FROM fills WHERE run_id = ? AND order_id = ?",
                (store.current_run_id, order_id),
            ).fetchone()[0]
        )

    def test_live_submit_binds_venue_order_id_on_accept(self) -> None:
        client = _FakeExecutionClient(place_result={"order_id": "venue-123", "status": "live"})
        store, intent, execution = self._prepare_live_execution(run_id="live_accept_001", client=client)

        events = execution.submit(intent)

        self.assertEqual(len(client.place_calls), 1)
        self.assertEqual(len(events), 1)
        order_row = self._load_order(store)
        self.assertEqual(order_row["state"], OrderState.ACCEPTED.value)
        self.assertEqual(order_row["venue_order_id"], "venue-123")
        self.assertEqual(order_row["venue_order_status"], "live")
        self.assertFalse(bool(order_row["reconciliation_required"]))

    def test_live_submit_then_partial_fill_transitions_to_partially_filled(self) -> None:
        client = _FakeExecutionClient(place_result={"order_id": "venue-123", "status": "live"})
        store, intent, execution = self._prepare_live_execution(run_id="live_partial_fill_001", client=client)
        execution.submit(intent)

        partial = execution.reconcile_order_event(
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
                fill_size=self._fill_size() / 2,
                cumulative_filled_size=self._fill_size() / 2,
                remaining_size=self._fill_size() / 2,
            )
        )

        self.assertIsNotNone(partial)
        order_row = self._load_order(store)
        self.assertEqual(order_row["state"], OrderState.PARTIALLY_FILLED.value)
        self.assertEqual(order_row["venue_order_status"], "partially_filled")
        self.assertAlmostEqual(float(order_row["cumulative_filled_size"]), self._fill_size() / 2)
        self.assertAlmostEqual(float(order_row["remaining_size"]), self._fill_size() / 2)
        self.assertEqual(self._fill_count(store), 1)

    def test_live_submit_then_full_fill_transitions_to_filled(self) -> None:
        client = _FakeExecutionClient(place_result={"order_id": "venue-123", "status": "live"})
        store, intent, execution = self._prepare_live_execution(run_id="live_fill_001", client=client)
        execution.submit(intent)

        filled = execution.reconcile_order_event(
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
                fill_size=self._fill_size(),
                cumulative_filled_size=self._fill_size(),
                remaining_size=0.0,
            )
        )

        self.assertIsNotNone(filled)
        self.assertEqual(filled.order_id, "ord_exec_001")
        order_row = self._load_order(store)
        self.assertEqual(order_row["state"], OrderState.FILLED.value)
        self.assertEqual(order_row["venue_order_status"], "filled")
        self.assertAlmostEqual(float(order_row["remaining_size"]), 0.0)
        self.assertEqual(self._fill_count(store), 1)

    def test_live_submit_venue_error_transitions_to_rejected(self) -> None:
        client = _FakeExecutionClient(place_error=RuntimeError("insufficient balance"))
        store, intent, execution = self._prepare_live_execution(run_id="live_reject_001", client=client)

        rejected = execution.submit(intent)

        self.assertEqual(len(rejected), 1)
        order_row = self._load_order(store)
        self.assertEqual(order_row["state"], OrderState.REJECTED.value)
        self.assertEqual(order_row["venue_order_status"], "rejected")
        self.assertEqual(order_row["last_reject_code"], "VENUE_ERROR")
        self.assertIn("insufficient balance", str(order_row["last_reject_reason"]))

    def test_live_cancel_path_reaches_cancel_confirmed_via_stream_ack(self) -> None:
        client = _FakeExecutionClient(
            place_result={"order_id": "venue-123", "status": "live"},
            cancel_result={"order_id": "venue-123", "success": True, "status": "pending_cancel"},
        )
        store, intent, execution = self._prepare_live_execution(run_id="live_cancel_001", client=client)
        execution.submit(intent)

        cancel_submit = execution.cancel(
            market_id="m1",
            token_id="tok_yes",
            event_time_ms=225_130,
            reason="operator_requested",
        )

        self.assertIsNone(cancel_submit)
        order_row = self._load_order(store)
        self.assertEqual(order_row["state"], OrderState.CANCEL_REQUESTED.value)
        self.assertEqual(order_row["venue_order_status"], "cancel_requested")
        self.assertTrue(bool(order_row["reconciliation_required"]))
        self.assertEqual(client.cancel_calls, ["venue-123"])

        canceled = execution.reconcile_order_event(
            OrderCanceled(
                event_time_ms=225_131,
                ingest_time_ms=225_132,
                order_id="venue-123",
                market_id="m1",
                token_id="tok_yes",
                source="polymarket_clob_user_ws",
                client_order_id="ord_exec_001",
                cancel_reason="operator_requested",
                cumulative_filled_size=0.0,
                remaining_size=0.0,
            )
        )

        self.assertIsNotNone(canceled)
        self.assertEqual(canceled.order_id, "ord_exec_001")
        order_row = self._load_order(store)
        self.assertEqual(order_row["state"], OrderState.CANCEL_CONFIRMED.value)
        self.assertEqual(order_row["venue_order_status"], "canceled")
        self.assertFalse(bool(order_row["reconciliation_required"]))

    def test_live_cancel_after_fill_race_keeps_fill_terminal(self) -> None:
        client = _FakeExecutionClient(place_result={"order_id": "venue-123", "status": "live"})
        store, intent, execution = self._prepare_live_execution(run_id="live_cancel_race_001", client=client)
        execution.submit(intent)
        execution.reconcile_order_event(
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
                fill_size=self._fill_size(),
                cumulative_filled_size=self._fill_size(),
                remaining_size=0.0,
            )
        )

        canceled = execution.reconcile_order_event(
            OrderCanceled(
                event_time_ms=225_140,
                ingest_time_ms=225_141,
                order_id="venue-123",
                market_id="m1",
                token_id="tok_yes",
                source="polymarket_clob_user_ws",
                client_order_id="ord_exec_001",
                cancel_reason="late_cancel",
                cumulative_filled_size=self._fill_size(),
                remaining_size=0.0,
            )
        )

        self.assertIsNone(canceled)
        order_row = self._load_order(store)
        self.assertEqual(order_row["state"], OrderState.FILLED.value)
        self.assertEqual(self._fill_count(store), 1)

    def test_reconciliation_restores_cancel_requested_order_back_to_accepted(self) -> None:
        client = _FakeExecutionClient(
            order_lookup_by_id={
                "venue-123": VenueOrderState(
                    order_id="venue-123",
                    status="live",
                    market_id="m1",
                    token_id="tok_yes",
                    side="buy",
                    price=0.55,
                    original_size=self._fill_size(),
                    cumulative_filled_size=0.0,
                    remaining_size=self._fill_size(),
                    client_order_id="cid-123",
                )
            }
        )
        store = self._create_store("live_reconcile_accept_001")
        store.insert_order(
            order_id="ord_exec_001",
            market_id="m1",
            token_id="tok_yes",
            side="BUY",
            price=0.55,
            size=self._fill_size(),
            state=OrderState.CANCEL_REQUESTED,
            client_order_id="cid-123",
            venue_order_id="venue-123",
            intent_created_at_ms=225_000,
            last_state_change_at_ms=225_050,
            remaining_size=self._fill_size(),
            venue_order_status="cancel_requested",
            reconciliation_required=True,
        )
        execution = ExecutionEngine(store=store, client=client, mode=ExecutionMode.LIVE)

        reconciled = execution.reconcile_persisted_orders(event_time_ms=225_200)

        self.assertEqual(reconciled, 1)
        order_row = self._load_order(store)
        self.assertEqual(order_row["state"], OrderState.ACCEPTED.value)
        self.assertEqual(order_row["venue_order_status"], "live")
        self.assertFalse(bool(order_row["reconciliation_required"]))
        self.assertEqual(client.get_order_calls, ["venue-123"])

    def test_reconciliation_marks_persisted_live_order_filled_and_records_fill(self) -> None:
        client = _FakeExecutionClient(
            order_lookup_by_id={
                "venue-123": VenueOrderState(
                    order_id="venue-123",
                    status="filled",
                    market_id="m1",
                    token_id="tok_yes",
                    side="buy",
                    price=0.55,
                    original_size=self._fill_size(),
                    cumulative_filled_size=self._fill_size(),
                    remaining_size=0.0,
                    client_order_id="cid-123",
                )
            }
        )
        store = self._create_store("live_reconcile_fill_001")
        store.insert_order(
            order_id="ord_exec_001",
            market_id="m1",
            token_id="tok_yes",
            side="BUY",
            price=0.55,
            size=self._fill_size(),
            state=OrderState.ACCEPTED,
            client_order_id="cid-123",
            venue_order_id="venue-123",
            intent_created_at_ms=225_000,
            last_state_change_at_ms=225_050,
            remaining_size=self._fill_size(),
            venue_order_status="live",
            reconciliation_required=True,
        )
        execution = ExecutionEngine(store=store, client=client, mode=ExecutionMode.LIVE)

        reconciled = execution.reconcile_persisted_orders(event_time_ms=225_200)

        self.assertEqual(reconciled, 1)
        order_row = self._load_order(store)
        self.assertEqual(order_row["state"], OrderState.FILLED.value)
        self.assertEqual(order_row["venue_order_status"], "filled")
        self.assertAlmostEqual(float(order_row["remaining_size"]), 0.0)
        self.assertEqual(self._fill_count(store), 1)

    def test_reconciliation_marks_missing_persisted_live_order_unknown(self) -> None:
        client = _FakeExecutionClient()
        store = self._create_store("live_reconcile_unknown_001")
        store.insert_order(
            order_id="ord_exec_001",
            market_id="m1",
            token_id="tok_yes",
            side="BUY",
            price=0.55,
            size=self._fill_size(),
            state=OrderState.ACCEPTED,
            client_order_id="cid-123",
            venue_order_id="venue-404",
            intent_created_at_ms=225_000,
            last_state_change_at_ms=225_050,
            remaining_size=self._fill_size(),
            venue_order_status="live",
            reconciliation_required=True,
        )
        execution = ExecutionEngine(store=store, client=client, mode=ExecutionMode.LIVE)

        reconciled = execution.reconcile_persisted_orders(event_time_ms=225_200)

        self.assertEqual(reconciled, 1)
        order_row = self._load_order(store)
        self.assertEqual(order_row["state"], OrderState.UNKNOWN.value)
        self.assertEqual(order_row["venue_order_status"], "not_found")
        self.assertTrue(bool(order_row["reconciliation_required"]))
        self.assertEqual(order_row["last_reject_code"], "VENUE_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
