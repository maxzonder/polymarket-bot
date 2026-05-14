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
from unittest.mock import call, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.config import ExecutionConfig, RiskConfig, ShortHorizonConfig, SpotDislocationConfig
from short_horizon.core import EventType, MarketResolvedWithInventory, OrderSide, OrderState, ReplayClock, SpotPriceUpdate
from short_horizon.engine import ShortHorizonEngine
from short_horizon.execution import ExecutionEngine, ExecutionMode, ExecutionTransitionError, ExecutionValidationError, SyntheticFillRequest, estimate_fee_usdc, is_valid_tick_size
from short_horizon.events import BookLevel, BookUpdate, MarketStateUpdate, OrderAccepted, OrderCanceled, OrderFilled, TimerEvent
from short_horizon.live_runner import AllowanceApprovalSummary, KillSwitchSummary, OperatorConfirmLiveOrderGuard, PolygonUsdcBridgeSummary, ResolvedRedeemSummary, build_live_source, build_live_runtime, build_live_submit_guard, build_parser, execute_allowance_approve, execute_kill_switch, execute_polygon_usdc_bridge, execute_resolved_redeem, main, reconcile_runtime_orders, run_live, run_stub_live, validate_cli_args
from short_horizon.probe import assert_min_book_updates_per_minute, cross_validate_probe_against_collector, maybe_cross_validate_probe_against_collector, summarize_probe_db
from short_horizon.models import OrderIntent, SkipDecision
from short_horizon.replay import ReplayCaptureWriter, ReplayFidelityError, parse_event_record
from short_horizon.replay.comparator import main as replay_comparator_main
from short_horizon.replay_runner import build_replay_runtime, compare_bundle_to_replay, main as replay_main, replay_bundle, replay_file, render_comparison_report
from short_horizon.runner import drive_runtime_events
from short_horizon.live_runner import DEFAULT_LIVE_VENUE_MIN_ORDER_SHARES_FALLBACK
from short_horizon.storage import InMemoryIntentStore, RunContext, SQLiteRuntimeStore
from short_horizon.strategies import ShortHorizon15mTouchStrategy
from short_horizon.strategy_api import CancelOrder, Noop, PlaceOrder
from short_horizon.telemetry import configure_logging, event_log_fields, get_logger
from short_horizon.venue_polymarket.execution_client import PRIVATE_KEY_ENV_VAR, PolygonUsdcBridgeResult, VenueApiCredentials, VenueOrderRequest, VenueOrderState, VenuePlaceResult
from short_horizon.venue_polymarket import RedeemResult
from short_horizon.storage.runtime import normalize_event_payload


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
        self.approve_calls = 0
        self.bridge_calls = []
        self.redeem_calls = 0
        self.api_credentials_calls = 0
        self.order_lookup_by_id = {}
        self.open_orders_by_market = {}
        self.wrap_calls = 0
        self.wrap_raises = None

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

    def approve_allowances(self):
        self.approve_calls += 1
        from short_horizon.venue_polymarket.execution_client import AllowanceApprovalResult

        return [
            AllowanceApprovalResult(asset_type="collateral", spender="spender-1", tx_hash="0xabc", status="approved"),
            AllowanceApprovalResult(asset_type="conditional", spender="spender-1", tx_hash=None, status="already_approved"),
        ]

    def bridge_polygon_usdc_to_usdce(self, *, amount_base_unit=None):
        self.bridge_calls.append(amount_base_unit)
        return PolygonUsdcBridgeResult(
            deposit_address="0xdeposit",
            source_amount_base_unit=amount_base_unit or 2_097_983,
            quoted_target_amount_base_unit=2_094_000,
            wallet_transfer_tx_hash="0xwallettransfer",
            bridge_status="COMPLETED",
            bridge_tx_hash="0xbridgehash",
            initial_target_balance_base_unit=0,
            final_target_balance_base_unit=2_094_000,
        )

    def redeem_resolved_positions(self):
        self.redeem_calls += 1
        return [
            RedeemResult(condition_id="0xcond1", status="redeemed", tx_hash="0xredeem1", position_count=2),
            RedeemResult(condition_id="0xcond2", status="skipped_negative_risk", position_count=1, reason="unsupported"),
            RedeemResult(condition_id="0xcond3", status="skipped_proxy_wallet", position_count=1, proxy_wallet="0xproxy"),
        ]

    def get_order(self, order_id):
        if order_id in self.order_lookup_by_id:
            return self.order_lookup_by_id[order_id]
        raise RuntimeError(f"unknown order {order_id}")

    def list_open_orders(self, market_id=None):
        return list(self.open_orders_by_market.get(market_id, []))

    def wrap_polygon_usdc_to_pusd(self, *, amount_base_unit=None):
        self.wrap_calls += 1
        if self.wrap_raises is not None:
            raise self.wrap_raises
        from short_horizon.venue_polymarket.execution_client import PolygonUsdcWrapResult
        return PolygonUsdcWrapResult(
            source_amount_base_unit=amount_base_unit or 10_000_000,
            usdce_token_address="0xusdce",
            onramp_address="0xonramp",
            recipient_address="0xrecipient",
            approval_tx_hash=None,
            wrap_tx_hash="0xwraptx",
        )


class ReplayCaptureWriterTest(unittest.TestCase):
    def test_records_place_and_cancel_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ReplayCaptureWriter(Path(tmpdir) / "capture")
            raw_client = _FakeLiveRunnerClient()
            client = writer.wrap_execution_client(raw_client)

            place_result = client.place_order(
                VenueOrderRequest(
                    token_id="tok_yes",
                    side="BUY",
                    price=0.55,
                    size=1.818181,
                    client_order_id="cid-123",
                )
            )
            cancel_result = client.cancel_order(place_result.order_id)

            records = writer.captured_venue_records()
            self.assertEqual(len(records), 2)
            self.assertEqual(records[0]["kind"], "place_order")
            self.assertEqual(records[0]["key"]["client_order_id"], "cid-123")
            self.assertEqual(records[0]["response"]["order_id"], "venue-live-1")
            self.assertEqual(records[1]["kind"], "cancel_order")
            self.assertEqual(records[1]["key"]["venue_order_id"], place_result.order_id)
            self.assertTrue(cancel_result.success)
            self.assertEqual(raw_client.cancel_calls, ["venue-live-1"])

    def test_capture_record_error_does_not_propagate_to_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ReplayCaptureWriter(Path(tmpdir) / "capture")
            raw_client = _FakeLiveRunnerClient()
            client = writer.wrap_execution_client(raw_client)

            with patch.object(writer, "_record_venue_response", side_effect=RuntimeError("boom")):
                result = client.place_order(
                    VenueOrderRequest(
                        token_id="tok_yes",
                        side="BUY",
                        price=0.55,
                        size=1.818181,
                        client_order_id="cid-123",
                    )
                )

            self.assertEqual(result.order_id, "venue-live-1")
            self.assertEqual(writer.captured_venue_records(), [])
            self.assertEqual(len(raw_client.place_calls), 1)

    def test_wrap_execution_client_returns_proxy_without_mutating_original_client(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ReplayCaptureWriter(Path(tmpdir) / "capture")
            raw_client = _FakeLiveRunnerClient()
            original_method = raw_client.place_order.__func__

            proxy = writer.wrap_execution_client(raw_client)

            self.assertIsNot(proxy, raw_client)
            self.assertIs(raw_client.place_order.__func__, original_method)

    def test_run_stub_live_with_capture_dir_materializes_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            event_log_path = tmp_path / "stub_live_events.jsonl"
            db_path = tmp_path / "live.sqlite3"
            capture_dir = tmp_path / "capture_bundle"
            event_log_path.write_text("\n".join(json.dumps(row) for row in self._sample_replay_rows()) + "\n", encoding="utf-8")

            summary = run_stub_live(
                stub_event_log_path=event_log_path,
                db_path=db_path,
                run_id="live_capture_test_001",
                config_hash="test-config",
                capture_dir=capture_dir,
            )

            self.assertEqual(summary.run_id, "live_capture_test_001")
            expected_files = {
                "events_log.jsonl",
                "market_state_snapshots.jsonl",
                "venue_responses.jsonl",
                "orders_final.jsonl",
                "fills_final.jsonl",
                "manifest.json",
            }
            self.assertEqual(expected_files, {path.name for path in capture_dir.iterdir()})

            manifest = json.loads((capture_dir / "manifest.json").read_text(encoding="utf-8"))
            for key, filename in {
                "events_log": "events_log.jsonl",
                "market_state_snapshots": "market_state_snapshots.jsonl",
                "venue_responses": "venue_responses.jsonl",
                "orders_final": "orders_final.jsonl",
                "fills_final": "fills_final.jsonl",
            }.items():
                file_path = capture_dir / filename
                with file_path.open("r", encoding="utf-8") as handle:
                    line_count = sum(1 for _ in handle)
                self.assertEqual(manifest["files"][key]["count"], line_count)
                self.assertEqual(manifest["files"][key]["path"], filename)

            self.assertEqual(manifest["run_id"], "live_capture_test_001")
            self.assertEqual(manifest["strategy_id"], "short_horizon_15m_touch_v1")
            self.assertEqual(manifest["config_hash"], "test-config")
            self.assertEqual(manifest["execution_mode"], "synthetic")

    @staticmethod
    def _sample_replay_rows() -> list[dict[str, object]]:
        return [
            {
                "event_type": "MarketStateUpdate",
                "event_time": "1970-01-01T00:03:20.000Z",
                "ingest_time": "1970-01-01T00:03:20.050Z",
                "source": "market_state",
                "market_id": "m1",
                "token_id": "tok_yes",
                "condition_id": "c1",
                "question": "Bitcoin Up or Down?",
                "status": "active",
                "start_time": "1970-01-01T00:00:00.000Z",
                "end_time": "1970-01-01T00:15:00.000Z",
                "duration_seconds": 900,
                "asset_slug": "bitcoin",
                "is_active": True,
                "metadata_is_fresh": True,
                "fee_rate_bps": 10.0,
                "fee_metadata_age_ms": 1000,
            },
            {
                "event_type": "BookUpdate",
                "event_time": "1970-01-01T00:03:40.000Z",
                "ingest_time": "1970-01-01T00:03:40.020Z",
                "source": "book_update",
                "market_id": "m1",
                "token_id": "tok_yes",
                "best_bid": 0.53,
                "best_ask": 0.54,
            },
            {
                "event_type": "BookUpdate",
                "event_time": "1970-01-01T00:03:45.000Z",
                "ingest_time": "1970-01-01T00:03:45.020Z",
                "source": "book_update",
                "market_id": "m1",
                "token_id": "tok_yes",
                "best_bid": 0.54,
                "best_ask": 0.55,
            },
        ]


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

    def test_spot_price_update_round_trips_through_normalizer_and_replay_parser(self) -> None:
        event = SpotPriceUpdate(
            event_time_ms=123_000,
            ingest_time_ms=123_050,
            source="coinbase.spot",
            asset_slug="btc",
            spot_price=75123.45,
            bid=75123.40,
            ask=75123.50,
            staleness_ms=50,
            venue="coinbase",
            run_id="run1",
        )

        payload = normalize_event_payload(event)
        parsed = parse_event_record(payload)

        self.assertEqual(payload["event_type"], "SpotPriceUpdate")
        self.assertEqual(parsed, event)

    def test_market_resolved_with_inventory_round_trips_through_normalizer_and_replay_parser(self) -> None:
        event = MarketResolvedWithInventory(
            event_time_ms=230_000,
            ingest_time_ms=230_050,
            market_id="m1",
            token_id="tok_yes",
            side=OrderSide.BUY,
            size=10.0,
            outcome_price=0.15,
            average_entry_price=0.62,
            estimated_pnl_usdc=-4.7,
            run_id="run1",
        )

        payload = normalize_event_payload(event)
        parsed = parse_event_record(payload)

        self.assertEqual(parsed, event)

    def test_market_state_resolution_fields_round_trip_through_replay_parser(self) -> None:
        event = MarketStateUpdate(
            event_time_ms=230_000,
            ingest_time_ms=230_050,
            market_id="m1",
            token_id="tok_yes",
            condition_id="c1",
            question="Bitcoin Up or Down?",
            start_time_ms=229_000,
            end_time_ms=230_000,
            duration_seconds=1,
            token_yes_id="tok_yes",
            token_no_id="tok_no",
            is_active=False,
            resolved_token_id="tok_yes",
            settlement_prices={"tok_yes": 1.0, "tok_no": 0.0},
            run_id="run1",
        )

        payload = normalize_event_payload(event)
        parsed = parse_event_record(payload)

        self.assertEqual(parsed, event)


class StrategyApiContractTest(unittest.TestCase):
    def test_runner_persists_spot_price_update_without_strategy_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "spot.sqlite3"
            runtime = build_live_runtime(db_path=db_path, run_id="run1", config=ShortHorizonConfig())
            event = SpotPriceUpdate(
                event_time_ms=123_000,
                ingest_time_ms=123_050,
                source="coinbase.spot",
                asset_slug="btc",
                spot_price=75123.45,
            )

            summary = drive_runtime_events(
                events=[event],
                runtime=runtime,
                logger_name="test.spot",
                completed_event_name="test_completed",
            )
            runtime.store.close()

            self.assertEqual(summary.event_count, 1)
            self.assertEqual(summary.order_intents, 0)
            conn = sqlite3.connect(db_path)
            row = conn.execute("SELECT event_type, payload_json FROM events_log").fetchone()
            conn.close()
            self.assertEqual(row[0], "SpotPriceUpdate")
            self.assertEqual(json.loads(row[1])["spot_price"], 75123.45)
            self.assertEqual(runtime.latest_spot_by_asset["btc"], event)

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

    def test_touch_strategy_with_replay_clock_is_deterministic_across_identical_runs(self) -> None:
        def run_once() -> tuple[list[PlaceOrder | Noop], int]:
            clock = ReplayClock()
            strategy = ShortHorizon15mTouchStrategy(config=ShortHorizonConfig(), clock=clock)
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
                    fee_metadata_age_ms=1_000,
                ),
                BookUpdate(event_time_ms=220_000, ingest_time_ms=220_020, market_id="m1", token_id="tok_yes", best_bid=0.53, best_ask=0.54),
                BookUpdate(event_time_ms=225_000, ingest_time_ms=225_020, market_id="m1", token_id="tok_yes", best_bid=0.54, best_ask=0.55),
            ]
            outputs: list[PlaceOrder | Noop] = []
            for event in events:
                clock.advance_to(event.event_time_ms)
                outputs.extend(strategy.on_market_event(event))
            return outputs, clock.now_ms()

        first_outputs, first_now = run_once()
        second_outputs, second_now = run_once()

        self.assertEqual(first_outputs, second_outputs)
        self.assertEqual(first_now, 225_000)
        self.assertEqual(second_now, 225_000)
        self.assertEqual(len(first_outputs), 1)
        self.assertIsInstance(first_outputs[0], PlaceOrder)
        self.assertIsInstance(first_outputs[0].intent, OrderIntent)

    def test_touch_strategy_skips_when_open_order_is_hydrated(self) -> None:
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
        strategy.hydrate_open_orders(
            [
                {
                    "order_id": "ord_live_1",
                    "market_id": "m1",
                    "token_id": "tok_yes",
                    "state": OrderState.ACCEPTED,
                }
            ]
        )
        self.assertEqual(strategy.on_market_event(BookUpdate(event_time_ms=220_000, ingest_time_ms=220_020, market_id="m1", token_id="tok_yes", best_bid=0.53, best_ask=0.54)), [])
        outputs = strategy.on_market_event(BookUpdate(event_time_ms=225_000, ingest_time_ms=225_020, market_id="m1", token_id="tok_yes", best_bid=0.54, best_ask=0.55))
        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], Noop)
        self.assertEqual(outputs[0].reason, "open_order_exists")

    def test_touch_strategy_clears_active_order_after_terminal_event(self) -> None:
        strategy = ShortHorizon15mTouchStrategy(config=ShortHorizonConfig())
        strategy.on_order_event(
            OrderAccepted(
                event_time_ms=1,
                ingest_time_ms=1,
                order_id="ord_live_1",
                market_id="m1",
                token_id="tok_yes",
                side="buy",
                price=0.55,
                size=10.0,
                source="test",
                client_order_id="cid-1",
                venue_status="live",
                run_id="run1",
            )
        )
        strategy.on_order_event(
            OrderCanceled(
                event_time_ms=2,
                ingest_time_ms=2,
                order_id="ord_live_1",
                market_id="m1",
                token_id="tok_yes",
                source="test",
                cancel_reason="done",
                run_id="run1",
            )
        )
        self.assertEqual(strategy.active_order_ids_by_market_token, {})

    def test_touch_strategy_skips_opposite_side_when_market_inventory_exists(self) -> None:
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
        strategy.on_market_event(
            MarketStateUpdate(
                event_time_ms=200_000,
                ingest_time_ms=200_050,
                market_id="m1",
                token_id="tok_no",
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
        strategy.on_order_event(
            OrderFilled(
                event_time_ms=225_001,
                ingest_time_ms=225_001,
                order_id="ord_fill_yes",
                market_id="m1",
                token_id="tok_yes",
                side="buy",
                fill_price=0.55,
                fill_size=1.0,
                cumulative_filled_size=1.0,
                remaining_size=0.0,
                source="test",
                run_id="run1",
            )
        )

        self.assertEqual(strategy.on_market_event(BookUpdate(event_time_ms=220_000, ingest_time_ms=220_020, market_id="m1", token_id="tok_no", best_bid=0.53, best_ask=0.54)), [])
        outputs = strategy.on_market_event(BookUpdate(event_time_ms=225_000, ingest_time_ms=225_020, market_id="m1", token_id="tok_no", best_bid=0.54, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], Noop)
        self.assertEqual(outputs[0].reason, "opposite_side_position_on_market")
        self.assertEqual(outputs[0].details, {"inventory_token_ids": ["tok_yes"]})

    def test_touch_strategy_emits_cancel_when_market_closes_and_hold_to_resolution_disabled(self) -> None:
        strategy = ShortHorizon15mTouchStrategy(
            config=ShortHorizonConfig(
                execution=ExecutionConfig(
                    target_trade_size_usdc=10.0,
                    stale_market_data_threshold_ms=2000,
                    hold_to_resolution=False,
                )
            )
        )
        strategy.hydrate_open_orders(
            [
                {
                    "order_id": "ord_live_1",
                    "market_id": "m1",
                    "token_id": "tok_yes",
                    "state": OrderState.ACCEPTED,
                }
            ]
        )

        outputs = strategy.on_market_event(
            MarketStateUpdate(
                event_time_ms=900_000,
                ingest_time_ms=900_050,
                market_id="m1",
                token_id="tok_yes",
                condition_id="c1",
                question="Bitcoin Up or Down?",
                asset_slug="bitcoin",
                start_time_ms=0,
                end_time_ms=900_000,
                is_active=False,
                metadata_is_fresh=True,
                fee_rate_bps=10.0,
                fee_metadata_age_ms=1_000,
            )
        )

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], CancelOrder)
        self.assertEqual(outputs[0].reason, "market_inactive_or_window_closed")

    def test_touch_strategy_does_not_repeat_cancel_once_requested(self) -> None:
        strategy = ShortHorizon15mTouchStrategy(
            config=ShortHorizonConfig(
                execution=ExecutionConfig(
                    target_trade_size_usdc=10.0,
                    stale_market_data_threshold_ms=2000,
                    hold_to_resolution=False,
                )
            )
        )
        strategy.hydrate_open_orders(
            [
                {
                    "order_id": "ord_live_1",
                    "market_id": "m1",
                    "token_id": "tok_yes",
                    "state": OrderState.ACCEPTED,
                }
            ]
        )
        event = MarketStateUpdate(
            event_time_ms=900_000,
            ingest_time_ms=900_050,
            market_id="m1",
            token_id="tok_yes",
            condition_id="c1",
            question="Bitcoin Up or Down?",
            asset_slug="bitcoin",
            start_time_ms=0,
            end_time_ms=900_000,
            is_active=False,
            metadata_is_fresh=True,
            fee_rate_bps=10.0,
            tick_size=0.01,
            token_yes_id="tok_yes",
            token_no_id="tok_no",
            fee_metadata_age_ms=1_000,
        )

        self.assertEqual(len(strategy.on_market_event(event)), 1)
        self.assertEqual(strategy.on_market_event(event), [])


class ShortHorizonEngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = InMemoryIntentStore()
        self.engine = ShortHorizonEngine(config=ShortHorizonConfig(), intent_store=self.store)

    def _market_state(
        self,
        *,
        token_id: str = "tok_yes",
        asset_slug: str = "bitcoin",
        event_time_ms: int = 200_000,
        ingest_time_ms: int | None = None,
        end_time_ms: int = 900_000,
        is_active: bool = True,
    ) -> MarketStateUpdate:
        return MarketStateUpdate(
            event_time_ms=event_time_ms,
            ingest_time_ms=event_time_ms + 50 if ingest_time_ms is None else ingest_time_ms,
            market_id="m1",
            token_id=token_id,
            condition_id="c1",
            question="Bitcoin Up or Down?",
            asset_slug=asset_slug,
            start_time_ms=0,
            end_time_ms=end_time_ms,
            is_active=is_active,
            metadata_is_fresh=True,
            fee_rate_bps=10.0,
            tick_size=0.01,
            token_yes_id="tok_yes",
            token_no_id="tok_no",
            fee_metadata_age_ms=1_000,
        )

    def _book(self, *, event_time_ms: int, best_ask: float, token_id: str = "tok_yes", best_bid: float | None = None) -> BookUpdate:
        return BookUpdate(
            event_time_ms=event_time_ms,
            ingest_time_ms=event_time_ms + 20,
            market_id="m1",
            token_id=token_id,
            best_bid=(best_ask - 0.01) if best_bid is None else best_bid,
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
        self.assertEqual(len(self.store.events), 4)
        self.assertIn(("m1", "tok_yes", "first_touch_fired:0.55"), self.store.strategy_state)

    def test_spot_dislocation_v3_accepts_down_no_touch(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                execution=ExecutionConfig(target_trade_size_usdc=5.0),
                lifecycle=type(self.engine.strategy.config.lifecycle)(bucket_start_fraction=0.0, bucket_end_fraction=1.0),
                spot_dislocation=SpotDislocationConfig(enabled=True),
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_spot_price_update(SpotPriceUpdate(event_time_ms=0, ingest_time_ms=0, source="binance.ticker", asset_slug="btc", spot_price=100.0))
        engine.on_spot_price_update(SpotPriceUpdate(event_time_ms=300_000, ingest_time_ms=300_000, source="binance.ticker", asset_slug="btc", spot_price=95.0))
        engine.on_spot_price_update(SpotPriceUpdate(event_time_ms=629_000, ingest_time_ms=629_000, source="binance.ticker", asset_slug="btc", spot_price=90.0))
        engine.on_market_state(self._market_state(token_id="tok_no", event_time_ms=620_000))
        engine.on_book_update(self._book(event_time_ms=629_000, token_id="tok_no", best_ask=0.64))
        outputs = engine.on_book_update(
            BookUpdate(
                event_time_ms=630_000,
                ingest_time_ms=630_020,
                market_id="m1",
                token_id="tok_no",
                best_bid=0.64,
                best_ask=0.65,
                ask_levels=(BookLevel(price=0.65, size=10.0),),
            )
        )

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], OrderIntent)
        self.assertEqual(outputs[0].token_id, "tok_no")
        self.assertEqual(outputs[0].entry_price, 0.65)

    def test_spot_dislocation_v3_skips_up_yes_direction(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                execution=ExecutionConfig(target_trade_size_usdc=5.0),
                lifecycle=type(self.engine.strategy.config.lifecycle)(bucket_start_fraction=0.0, bucket_end_fraction=1.0),
                spot_dislocation=SpotDislocationConfig(enabled=True),
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_spot_price_update(SpotPriceUpdate(event_time_ms=0, ingest_time_ms=0, source="binance.ticker", asset_slug="btc", spot_price=100.0))
        engine.on_spot_price_update(SpotPriceUpdate(event_time_ms=629_000, ingest_time_ms=629_000, source="binance.ticker", asset_slug="btc", spot_price=90.0))
        engine.on_market_state(self._market_state(token_id="tok_yes", event_time_ms=620_000))
        engine.on_book_update(self._book(event_time_ms=629_000, token_id="tok_yes", best_ask=0.64))
        outputs = engine.on_book_update(
            BookUpdate(
                event_time_ms=630_000,
                ingest_time_ms=630_020,
                market_id="m1",
                token_id="tok_yes",
                best_bid=0.64,
                best_ask=0.65,
                ask_levels=(BookLevel(price=0.65, size=10.0),),
            )
        )

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "spot_dislocation_direction_not_allowed")

    def test_spot_dislocation_v3_skips_touch_only_without_spot(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                execution=ExecutionConfig(target_trade_size_usdc=5.0),
                lifecycle=type(self.engine.strategy.config.lifecycle)(bucket_start_fraction=0.0, bucket_end_fraction=1.0),
                spot_dislocation=SpotDislocationConfig(enabled=True),
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_no", event_time_ms=620_000))
        engine.on_book_update(self._book(event_time_ms=629_000, token_id="tok_no", best_ask=0.64))
        outputs = engine.on_book_update(
            BookUpdate(
                event_time_ms=630_000,
                ingest_time_ms=630_020,
                market_id="m1",
                token_id="tok_no",
                best_bid=0.64,
                best_ask=0.65,
                ask_levels=(BookLevel(price=0.65, size=10.0),),
            )
        )

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "spot_dislocation_missing_spot")

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

    def test_unknown_order_blocks_new_touches_for_same_market(self) -> None:
        self.engine.on_market_state(self._market_state(token_id="tok_yes"))
        self.engine.on_market_state(self._market_state(token_id="tok_no"))
        self.store.insert_order(
            order_id="ord_unknown_001",
            market_id="m1",
            token_id="tok_yes",
            side="BUY",
            price=0.55,
            size=10.0,
            state=OrderState.UNKNOWN,
            client_order_id="cli_unknown_001",
            intent_created_at_ms=210_000,
            last_state_change_at_ms=210_000,
            remaining_size=10.0,
            reconciliation_required=True,
        )

        self.assertEqual(self.engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54, token_id="tok_no")), [])
        outputs = self.engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55, token_id="tok_no"))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "market_reconciliation_blocked")
        self.assertEqual(outputs[0].details, "unknown_order_present")
        self.assertEqual(len(self.store.intents), 0)

    def test_runtime_blocks_new_intent_when_max_open_orders_total_reached(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(risk=RiskConfig(max_open_orders_total=1, micro_live_cumulative_stake_cap_usdc=100.0)),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_yes"))
        engine.store.insert_order(
            order_id="ord_existing_001",
            market_id="m2",
            token_id="tok_other",
            side="BUY",
            price=0.55,
            size=10.0,
            state=OrderState.ACCEPTED,
            client_order_id="cli_existing_001",
            intent_created_at_ms=210_000,
            last_state_change_at_ms=210_000,
            remaining_size=10.0,
        )

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54)), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "max_open_orders_total_reached")
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_blocks_new_intent_when_max_notional_per_strategy_would_be_exceeded(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_notional_per_strategy_usdc=0.5,
                    max_open_orders_total=10,
                    max_open_orders_per_market=10,
                    micro_live_cumulative_stake_cap_usdc=100.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_yes"))

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54)), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "max_notional_per_strategy_usdc_reached")
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_cap_fires_on_effective_notional_after_min_shares_upscale(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_notional_per_strategy_usdc=2.0,
                    max_trade_notional_usdc=100.0,
                    max_open_orders_total=10,
                    max_open_orders_per_market=10,
                    micro_live_cumulative_stake_cap_usdc=100.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        market_state = self._market_state(token_id="tok_yes")
        market_state = MarketStateUpdate(
            **{**market_state.__dict__, "tick_size": 0.01, "min_order_size": 5.0}
        )
        engine.on_market_state(market_state)

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54)), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "max_notional_per_strategy_usdc_reached")
        self.assertIn("projected_usdc=2.7", outputs[0].details)
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_blocks_new_intent_when_effective_trade_notional_exceeds_cap(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_trade_notional_usdc=2.0,
                    max_notional_per_strategy_usdc=100.0,
                    max_open_orders_total=10,
                    max_open_orders_per_market=10,
                    micro_live_concurrent_open_notional_cap_usdc=100.0,
                    micro_live_cumulative_stake_cap_usdc=100.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        market_state = self._market_state(token_id="tok_yes")
        market_state = MarketStateUpdate(
            **{**market_state.__dict__, "tick_size": 0.01, "min_order_size": 5.0}
        )
        engine.on_market_state(market_state)

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54)), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "max_trade_notional_usdc_reached")
        self.assertIn("effective_notional_usdc=", outputs[0].details)
        self.assertIn("cap_usdc=2.000000", outputs[0].details)
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_blocks_new_intent_when_max_open_orders_per_market_reached(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_open_orders_total=10,
                    max_open_orders_per_market=1,
                    micro_live_cumulative_stake_cap_usdc=100.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_yes"))
        engine.store.insert_order(
            order_id="ord_existing_market_001",
            market_id="m1",
            token_id="tok_no",
            side="BUY",
            price=0.55,
            size=10.0,
            state=OrderState.ACCEPTED,
            client_order_id="cli_existing_market_001",
            intent_created_at_ms=210_000,
            last_state_change_at_ms=210_000,
            remaining_size=10.0,
        )

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54, token_id="tok_yes")), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55, token_id="tok_yes"))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "max_open_orders_per_market_reached")
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_blocks_same_side_reentry_when_prior_fill_on_market_token(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_open_orders_total=10,
                    max_open_orders_per_market=10,
                    max_orders_per_market_per_run=1,
                    micro_live_cumulative_stake_cap_usdc=100.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_yes"))
        engine.store.insert_order(
            order_id="ord_prior_fill_001",
            market_id="m1",
            token_id="tok_yes",
            side="BUY",
            price=0.55,
            size=1.83,
            state=OrderState.FILLED,
            client_order_id="cli_prior_fill_001",
            intent_created_at_ms=210_000,
            last_state_change_at_ms=210_500,
            remaining_size=0.0,
        )

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.64, token_id="tok_yes")), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.66, token_id="tok_yes"))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "max_orders_per_market_per_run_reached")
        self.assertIn("market_orders_so_far=1", outputs[0].details)
        self.assertIn("cap=1", outputs[0].details)
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_counts_rejected_order_attempts_for_per_market_cap(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_open_orders_total=10,
                    max_open_orders_per_market=10,
                    max_orders_per_market_per_run=1,
                    micro_live_cumulative_stake_cap_usdc=100.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_yes"))
        engine.store.insert_order(
            order_id="ord_prior_reject_001",
            market_id="m1",
            token_id="tok_yes",
            side="BUY",
            price=0.55,
            size=1.83,
            state=OrderState.REJECTED,
            client_order_id="cli_prior_reject_001",
            intent_created_at_ms=210_000,
            last_state_change_at_ms=210_500,
            remaining_size=0.0,
        )

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54, token_id="tok_yes")), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55, token_id="tok_yes"))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "max_orders_per_market_per_run_reached")
        self.assertIn("market_orders_so_far=1", outputs[0].details)
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_blocks_new_intent_when_max_daily_loss_usdc_is_already_breached(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_daily_loss_usdc=5.0,
                    max_open_orders_total=10,
                    max_open_orders_per_market=10,
                    max_orders_per_market_per_run=10,
                    micro_live_cumulative_stake_cap_usdc=100.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_yes"))
        engine.store.insert_order(
            order_id="ord_buy_loss_001",
            market_id="m1",
            token_id="tok_yes",
            side="BUY",
            price=0.60,
            size=10.0,
            state=OrderState.FILLED,
            client_order_id="cli_buy_loss_001",
            intent_created_at_ms=200_000,
            last_state_change_at_ms=210_000,
            remaining_size=0.0,
        )
        engine.store.insert_order(
            order_id="ord_sell_loss_001",
            market_id="m1",
            token_id="tok_yes",
            side="SELL",
            price=0.10,
            size=10.0,
            state=OrderState.FILLED,
            client_order_id="cli_sell_loss_001",
            intent_created_at_ms=211_000,
            last_state_change_at_ms=212_000,
            remaining_size=0.0,
        )
        engine.store.insert_fill(
            fill_id="fill_buy_loss_001",
            order_id="ord_buy_loss_001",
            market_id="m1",
            token_id="tok_yes",
            price=0.60,
            size=10.0,
            filled_at_ms=210_000,
            source="test",
            fee_paid_usdc=0.20,
        )
        engine.store.insert_fill(
            fill_id="fill_sell_loss_001",
            order_id="ord_sell_loss_001",
            market_id="m1",
            token_id="tok_yes",
            price=0.10,
            size=10.0,
            filled_at_ms=212_000,
            source="test",
            fee_paid_usdc=0.10,
        )

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_bid=0.10, best_ask=0.54)), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_bid=0.10, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "max_daily_loss_usdc_reached")
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_blocks_new_intent_when_buy_only_mark_to_market_loss_is_already_breached(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_daily_loss_usdc=1.0,
                    max_open_orders_total=10,
                    max_open_orders_per_market=10,
                    max_orders_per_market_per_run=10,
                    max_trade_notional_usdc=100.0,
                    micro_live_cumulative_stake_cap_usdc=100.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_yes"))
        engine.store.insert_order(
            order_id="ord_buy_loss_002",
            market_id="m1",
            token_id="tok_yes",
            side="BUY",
            price=0.60,
            size=10.0,
            state=OrderState.FILLED,
            client_order_id="cli_buy_loss_002",
            intent_created_at_ms=200_000,
            last_state_change_at_ms=210_000,
            remaining_size=0.0,
        )
        engine.store.insert_fill(
            fill_id="fill_buy_loss_002",
            order_id="ord_buy_loss_002",
            market_id="m1",
            token_id="tok_yes",
            price=0.60,
            size=10.0,
            filled_at_ms=210_000,
            source="test",
            fee_paid_usdc=0.20,
        )

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_bid=0.40, best_ask=0.54)), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_bid=0.41, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "max_daily_loss_usdc_reached")
        self.assertIn("daily_pnl_usdc=", outputs[0].details)
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_does_not_resolve_inventory_without_canonical_settlement(self) -> None:
        engine = ShortHorizonEngine(config=ShortHorizonConfig(), intent_store=InMemoryIntentStore())
        engine.on_market_state(self._market_state(token_id="tok_yes"))
        engine.store.insert_order(
            order_id="ord_buy_resolved_001",
            market_id="m1",
            token_id="tok_yes",
            side="BUY",
            price=0.60,
            size=10.0,
            state=OrderState.FILLED,
            client_order_id="cli_buy_resolved_001",
            intent_created_at_ms=200_000,
            last_state_change_at_ms=210_000,
            remaining_size=0.0,
        )
        engine.store.insert_fill(
            fill_id="fill_buy_resolved_001",
            order_id="ord_buy_resolved_001",
            market_id="m1",
            token_id="tok_yes",
            price=0.60,
            size=10.0,
            filled_at_ms=210_000,
            source="test",
            fee_paid_usdc=0.20,
        )

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_bid=0.15, best_ask=0.54)), [])
        outputs = engine.on_market_state(self._market_state(token_id="tok_yes", event_time_ms=230_000, end_time_ms=230_000, is_active=False))

        self.assertEqual(outputs, [])
        resolved_events = [event for event in engine.store.events if isinstance(event, MarketResolvedWithInventory)]
        self.assertEqual(resolved_events, [])

    def test_runtime_prefers_canonical_resolution_settlement_over_last_bid_mark(self) -> None:
        engine = ShortHorizonEngine(config=ShortHorizonConfig(), intent_store=InMemoryIntentStore())
        engine.on_market_state(self._market_state(token_id="tok_yes"))
        engine.store.insert_order(
            order_id="ord_buy_settle_001",
            market_id="m1",
            token_id="tok_yes",
            side="BUY",
            price=0.60,
            size=10.0,
            state=OrderState.FILLED,
            client_order_id="cli_buy_settle_001",
            intent_created_at_ms=200_000,
            last_state_change_at_ms=210_000,
            remaining_size=0.0,
        )
        engine.store.insert_fill(
            fill_id="fill_buy_settle_001",
            order_id="ord_buy_settle_001",
            market_id="m1",
            token_id="tok_yes",
            price=0.60,
            size=10.0,
            filled_at_ms=210_000,
            source="test",
            fee_paid_usdc=0.20,
        )
        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_bid=0.15, best_ask=0.54)), [])

        outputs = engine.on_market_state(MarketStateUpdate(
            event_time_ms=230_000,
            ingest_time_ms=230_050,
            market_id="m1",
            token_id="tok_yes",
            token_yes_id="tok_yes",
            token_no_id="tok_no",
            condition_id="c1",
            question="Bitcoin Up or Down?",
            asset_slug="bitcoin",
            status="resolved",
            is_active=False,
            end_time_ms=230_000,
            resolved_token_id="tok_yes",
        ))

        self.assertEqual(outputs, [])
        resolved_events = [event for event in engine.store.events if isinstance(event, MarketResolvedWithInventory)]
        self.assertEqual(len(resolved_events), 1)
        self.assertEqual(resolved_events[0].outcome_price, 1.0)
        self.assertEqual(resolved_events[0].source, "runtime.market_resolution_settlement")
        self.assertAlmostEqual(resolved_events[0].estimated_pnl_usdc, 3.8)

    def test_runtime_blocks_new_intent_when_max_consecutive_rejects_reached(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_consecutive_rejects=3,
                    max_open_orders_total=10,
                    max_open_orders_per_market=10,
                    max_orders_per_market_per_run=10,
                    micro_live_cumulative_stake_cap_usdc=100.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_yes"))
        for idx in range(3):
            event_time_ms = 210_000 + idx
            engine.store.insert_order(
                order_id=f"ord_reject_{idx}",
                market_id="m1",
                token_id="tok_yes",
                side="BUY",
                price=0.55,
                size=10.0,
                state=OrderState.REJECTED,
                client_order_id=f"cli_reject_{idx}",
                intent_created_at_ms=event_time_ms,
                last_state_change_at_ms=event_time_ms,
                remaining_size=0.0,
            )

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54)), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "max_consecutive_rejects_reached")
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_blocks_new_intent_in_global_safe_mode(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(risk=RiskConfig(global_safe_mode=True, max_open_orders_total=10, micro_live_cumulative_stake_cap_usdc=100.0)),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_yes"))

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54)), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "global_safe_mode")
        self.assertEqual(outputs[0].details, "operator_requested")
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_blocks_new_intent_when_concurrent_open_notional_cap_would_be_exceeded(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_open_orders_total=10,
                    micro_live_concurrent_open_notional_cap_usdc=10.0,
                    micro_live_cumulative_stake_cap_usdc=100.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_yes"))
        engine.store.insert_order(
            order_id="ord_existing_001",
            market_id="m2",
            token_id="tok_other",
            side="BUY",
            price=0.50,
            size=20.0,
            state=OrderState.ACCEPTED,
            client_order_id="cli_existing_001",
            intent_created_at_ms=210_000,
            last_state_change_at_ms=210_000,
            remaining_size=20.0,
        )

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54)), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "micro_live_concurrent_open_notional_cap_reached")
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_blocks_new_intent_when_cumulative_stake_cap_would_be_exceeded(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_open_orders_total=10,
                    micro_live_concurrent_open_notional_cap_usdc=100.0,
                    micro_live_cumulative_stake_cap_usdc=10.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_yes"))
        engine.store.insert_order(
            order_id="ord_existing_001",
            market_id="m2",
            token_id="tok_other",
            side="BUY",
            price=0.50,
            size=20.0,
            state=OrderState.FILLED,
            client_order_id="cli_existing_001",
            intent_created_at_ms=210_000,
            last_state_change_at_ms=210_000,
            remaining_size=0.0,
        )

        self.assertEqual(engine.on_book_update(self._book(event_time_ms=220_000, best_ask=0.54)), [])
        outputs = engine.on_book_update(self._book(event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "micro_live_cumulative_stake_cap_reached")
        self.assertEqual(len(engine.store.intents), 0)

    def test_runtime_blocks_new_token_when_market_exposure_cap_would_be_exceeded(self) -> None:
        engine = ShortHorizonEngine(
            config=ShortHorizonConfig(
                risk=RiskConfig(
                    max_open_orders_total=10,
                    max_orders_per_market_per_run=10,
                    max_tokens_with_exposure_per_market=1,
                    micro_live_concurrent_open_notional_cap_usdc=100.0,
                    micro_live_cumulative_stake_cap_usdc=100.0,
                    max_trade_notional_usdc=100.0,
                )
            ),
            intent_store=InMemoryIntentStore(),
        )
        engine.on_market_state(self._market_state(token_id="tok_no"))
        engine.store.insert_order(
            order_id="ord_existing_001",
            market_id="m1",
            token_id="tok_yes",
            side="BUY",
            price=0.50,
            size=20.0,
            state=OrderState.FILLED,
            client_order_id="cli_existing_001",
            intent_created_at_ms=210_000,
            last_state_change_at_ms=210_000,
            remaining_size=0.0,
        )
        engine.store.update_order_state(
            order_id="ord_existing_001",
            state=OrderState.FILLED,
            event_time_ms=210_100,
            cumulative_filled_size=20.0,
            remaining_size=0.0,
        )

        self.assertEqual(engine.on_book_update(self._book(token_id="tok_no", event_time_ms=220_000, best_ask=0.54)), [])
        outputs = engine.on_book_update(self._book(token_id="tok_no", event_time_ms=225_000, best_ask=0.55))

        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], SkipDecision)
        self.assertEqual(outputs[0].reason, "max_tokens_with_exposure_per_market_reached")
        self.assertIn("tok_yes", outputs[0].details)
        self.assertIn("tok_no", outputs[0].details)
        self.assertEqual(len(engine.store.intents), 0)


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
    def __init__(self, *, place_result=None, error: Exception | None = None, order_lookup_by_id=None, open_orders_by_market=None):
        self.place_result = place_result or {"order_id": "venue-ord-1", "status": "live"}
        self.error = error
        self.place_calls = []
        self.cancel_calls = []
        self.order_lookup_by_id = dict(order_lookup_by_id or {})
        self.open_orders_by_market = dict(open_orders_by_market or {})

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

    def get_order(self, order_id):
        if self.error is not None:
            raise self.error
        if order_id in self.order_lookup_by_id:
            return self.order_lookup_by_id[order_id]
        raise RuntimeError(f"unknown order {order_id}")

    def list_open_orders(self, market_id=None):
        if self.error is not None:
            raise self.error
        return list(self.open_orders_by_market.get(market_id, []))


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
            notional_usdc=1.0,
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
                self.assertAlmostEqual(fill_event.fill_size, 1.0 / 0.55)
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
                self.assertAlmostEqual(order_row[2], 1.0 / 0.55)
                self.assertAlmostEqual(order_row[3], 0.0)
                self.assertEqual(fill_count, 1)
                self.assertIn("OrderAccepted", event_types)
                self.assertIn("OrderFilled", event_types)
            finally:
                store.close()

    def test_synthetic_fill_is_idempotent_after_store_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            run = RunContext(
                run_id="run_exec_idempotent_fill_001",
                strategy_id="short_horizon_15m_touch_v1",
                config_hash="test-config",
            )
            store = SQLiteRuntimeStore(db_path, run=run)
            try:
                store.upsert_market_state(self._market_state())
                store.insert_order(
                    order_id="ord_exec_idempotent_001",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.05,
                    size=20.0,
                    state=OrderState.ACCEPTED,
                    client_order_id="ord_exec_idempotent_001",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_001,
                    remaining_size=20.0,
                    venue_order_status="accepted",
                    post_only=True,
                )
                execution = ExecutionEngine(store=store)
                request = SyntheticFillRequest(
                    order_id="ord_exec_idempotent_001",
                    event_time_ms=225_100,
                    fill_size=10.0,
                    fill_price=0.05,
                    source="replay",
                    liquidity_role="maker",
                )
                first_fill = execution.apply_fill(request)
                self.assertIsNotNone(first_fill)
            finally:
                store.close()

            restarted_store = SQLiteRuntimeStore(db_path, run=run)
            try:
                restarted_execution = ExecutionEngine(store=restarted_store)
                duplicate_fill = restarted_execution.apply_fill(request)
                self.assertIsNone(duplicate_fill)

                conn = sqlite3.connect(db_path)
                try:
                    order_row = conn.execute(
                        "SELECT state, cumulative_filled_size, remaining_size FROM orders WHERE order_id = 'ord_exec_idempotent_001'"
                    ).fetchone()
                    fill_count = conn.execute("SELECT COUNT(*) FROM fills WHERE order_id = 'ord_exec_idempotent_001'").fetchone()[0]
                    filled_events = conn.execute(
                        "SELECT COUNT(*) FROM events_log WHERE run_id = 'run_exec_idempotent_fill_001' AND event_type = 'OrderFilled'"
                    ).fetchone()[0]
                finally:
                    conn.close()

                self.assertEqual(order_row[0], OrderState.PARTIALLY_FILLED.value)
                self.assertAlmostEqual(order_row[1], 10.0)
                self.assertAlmostEqual(order_row[2], 10.0)
                self.assertEqual(fill_count, 1)
                self.assertEqual(filled_events, 1)
            finally:
                restarted_store.close()

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
                        fill_size=1.0 / 0.55,
                        cumulative_filled_size=1.0 / 0.55,
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
                self.assertAlmostEqual(order_row[3], 1.0 / 0.55)
                self.assertAlmostEqual(order_row[4], 0.0)
                self.assertEqual(fill_count, 1)
            finally:
                store.close()

    def test_execution_boundary_reconciles_persisted_live_order_on_startup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_live_reconcile_001",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            client = _FakeExecutionClient(
                order_lookup_by_id={
                    "venue-123": VenueOrderState(
                        order_id="venue-123",
                        status="live",
                        market_id="m1",
                        token_id="tok_yes",
                        side="buy",
                        price=0.55,
                        original_size=18.1818181818,
                        remaining_size=18.1818181818,
                        cumulative_filled_size=0.0,
                        client_order_id="cid-123",
                    )
                }
            )
            try:
                store.insert_order(
                    order_id="ord_exec_001",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.55,
                    size=18.1818181818,
                    state=OrderState.CANCEL_REQUESTED,
                    client_order_id="cid-123",
                    venue_order_id="venue-123",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_050,
                    remaining_size=18.1818181818,
                    venue_order_status="cancelling",
                    reconciliation_required=True,
                )
                execution = ExecutionEngine(store=store, client=client, mode=ExecutionMode.LIVE)

                reconciled = execution.reconcile_persisted_orders(event_time_ms=225_200)

                self.assertEqual(reconciled, 1)
                conn = sqlite3.connect(db_path)
                try:
                    order_row = conn.execute(
                        "SELECT state, venue_order_status, reconciliation_required FROM orders WHERE order_id = 'ord_exec_001'"
                    ).fetchone()
                finally:
                    conn.close()

                self.assertEqual(order_row[0], "accepted")
                self.assertEqual(order_row[1], "live")
                self.assertEqual(order_row[2], 0)
            finally:
                store.close()

    def test_execution_boundary_marks_missing_persisted_live_order_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "short_horizon.sqlite3"
            store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="run_exec_live_reconcile_002",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            client = _FakeExecutionClient()
            try:
                store.insert_order(
                    order_id="ord_exec_002",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.55,
                    size=18.1818181818,
                    state=OrderState.ACCEPTED,
                    client_order_id="cid-123",
                    venue_order_id="venue-404",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_050,
                    remaining_size=18.1818181818,
                    venue_order_status="live",
                    reconciliation_required=True,
                )
                execution = ExecutionEngine(store=store, client=client, mode=ExecutionMode.LIVE)

                reconciled = execution.reconcile_persisted_orders(event_time_ms=225_200)

                self.assertEqual(reconciled, 1)
                conn = sqlite3.connect(db_path)
                try:
                    order_row = conn.execute(
                        "SELECT state, venue_order_status, reconciliation_required, last_reject_code FROM orders WHERE order_id = 'ord_exec_002'"
                    ).fetchone()
                finally:
                    conn.close()

                self.assertEqual(order_row[0], "unknown")
                self.assertEqual(order_row[1], "not_found")
                self.assertEqual(order_row[2], 1)
                self.assertEqual(order_row[3], "VENUE_NOT_FOUND")
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
                self.assertAlmostEqual(order_row[2], 1.0 / 0.55)
                self.assertEqual(event_count, 4)
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

    def _write_bundle(
        self,
        bundle_dir: Path,
        *,
        include_execution_accept_event: bool = False,
        include_terminal_cancel_event: bool = False,
    ) -> None:
        bundle_dir.mkdir(parents=True, exist_ok=True)
        event_rows = self._sample_replay_rows()
        if include_execution_accept_event:
            event_rows.append(
                {
                    "event_type": "OrderAccepted",
                    "event_time": 225001,
                    "ingest_time": 225001,
                    "order_id": "m1:tok_yes:0.55:225000",
                    "market_id": "m1",
                    "token_id": "tok_yes",
                    "side": "BUY",
                    "price": 0.55,
                    "size": 1.836364,
                    "source": "execution.live_accept",
                    "client_order_id": "45bfb309-fdd9-563e-9f2c-9c9ace42edda",
                    "venue_status": "live",
                    "run_id": "bundle_live_001",
                }
            )

        if include_terminal_cancel_event:
            event_rows.append(
                {
                    "event_type": "OrderCanceled",
                    "event_time": 225100,
                    "ingest_time": 225100,
                    "order_id": "m1:tok_yes:0.55:225000",
                    "market_id": "m1",
                    "token_id": "tok_yes",
                    "source": "sample.user_stream",
                    "client_order_id": "45bfb309-fdd9-563e-9f2c-9c9ace42edda",
                    "cancel_reason": "market_closed",
                    "cumulative_filled_size": 0.0,
                    "remaining_size": 1.836364,
                }
            )

        market_state_rows = [row for row in event_rows if row.get("event_type") == "MarketStateUpdate"]
        venue_rows = [
            {
                "seq": 1,
                "kind": "place_order",
                "key": {"client_order_id": "45bfb309-fdd9-563e-9f2c-9c9ace42edda"},
                "request": {
                    "token_id": "tok_yes",
                    "side": "BUY",
                    "price": 0.55,
                    "size": 1.836364,
                    "client_order_id": "45bfb309-fdd9-563e-9f2c-9c9ace42edda",
                    "time_in_force": "GTC",
                    "post_only": False,
                },
                "response": {
                    "order_id": "venue-live-1",
                    "status": "live",
                    "client_order_id": "45bfb309-fdd9-563e-9f2c-9c9ace42edda",
                    "raw": {"order_id": "venue-live-1", "status": "live"},
                },
                "error": None,
            }
        ]
        orders_final_rows = []
        if include_terminal_cancel_event:
            orders_final_rows.append(
                {
                    "order_id": "m1:tok_yes:0.55:225000",
                    "run_id": "bundle_live_001",
                    "market_id": "m1",
                    "token_id": "tok_yes",
                    "side": "BUY",
                    "price": 0.55,
                    "size": 1.836364,
                    "state": "cancel_confirmed",
                    "client_order_id": "45bfb309-fdd9-563e-9f2c-9c9ace42edda",
                    "venue_order_id": "venue-live-1",
                    "parent_order_id": None,
                    "intent_created_at": "1970-01-01T00:03:45.000Z",
                    "last_state_change_at": "1970-01-01T00:03:45.100Z",
                    "venue_order_status": "canceled",
                    "cumulative_filled_size": 0.0,
                    "remaining_size": 1.836364,
                    "reconciliation_required": 0,
                    "last_reject_code": None,
                    "last_reject_reason": None,
                }
            )
        manifest = {
            "run_id": "bundle_live_001",
            "strategy_id": "short_horizon_15m_touch_v1",
            "config_hash": "test-config",
            "files": {
                "events_log": {"path": "events_log.jsonl", "count": len(event_rows)},
                "market_state_snapshots": {"path": "market_state_snapshots.jsonl", "count": len(market_state_rows)},
                "venue_responses": {"path": "venue_responses.jsonl", "count": len(venue_rows)},
                "orders_final": {"path": "orders_final.jsonl", "count": len(orders_final_rows)},
                "fills_final": {"path": "fills_final.jsonl", "count": 0},
            },
        }

        (bundle_dir / "events_log.jsonl").write_text("\n".join(json.dumps(row) for row in event_rows) + "\n", encoding="utf-8")
        (bundle_dir / "market_state_snapshots.jsonl").write_text("\n".join(json.dumps(row) for row in market_state_rows) + "\n", encoding="utf-8")
        (bundle_dir / "venue_responses.jsonl").write_text("\n".join(json.dumps(row) for row in venue_rows) + "\n", encoding="utf-8")
        (bundle_dir / "orders_final.jsonl").write_text(
            ("\n".join(json.dumps(row) for row in orders_final_rows) + "\n") if orders_final_rows else "",
            encoding="utf-8",
        )
        (bundle_dir / "fills_final.jsonl").write_text("", encoding="utf-8")
        (bundle_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def _write_skip_decision_bundle(self, bundle_dir: Path) -> None:
        bundle_dir.mkdir(parents=True, exist_ok=True)
        event_rows = [
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
            {
                "event_type": "SkipDecision",
                "event_time": 225000,
                "ingest_time": 225000,
                "reason": "global_safe_mode",
                "market_id": "m1",
                "token_id": "tok_yes",
                "level": 0.55,
                "details": "operator_requested",
                "source": "runtime.skip_decision",
                "run_id": "bundle_skip_001",
            },
        ]
        manifest = {
            "run_id": "bundle_skip_001",
            "strategy_id": "short_horizon_15m_touch_v1",
            "config_hash": "test-config",
            "files": {
                "events_log": {"path": "events_log.jsonl", "count": len(event_rows)},
                "market_state_snapshots": {"path": "market_state_snapshots.jsonl", "count": 1},
                "venue_responses": {"path": "venue_responses.jsonl", "count": 0},
                "orders_final": {"path": "orders_final.jsonl", "count": 0},
                "fills_final": {"path": "fills_final.jsonl", "count": 0},
            },
        }

        (bundle_dir / "events_log.jsonl").write_text("\n".join(json.dumps(row) for row in event_rows) + "\n", encoding="utf-8")
        (bundle_dir / "market_state_snapshots.jsonl").write_text(json.dumps(event_rows[0]) + "\n", encoding="utf-8")
        (bundle_dir / "venue_responses.jsonl").write_text("", encoding="utf-8")
        (bundle_dir / "orders_final.jsonl").write_text("", encoding="utf-8")
        (bundle_dir / "fills_final.jsonl").write_text("", encoding="utf-8")
        (bundle_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

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
            self.assertEqual(event_count, 12)
            self.assertEqual(order_row[0], "accepted")
            self.assertEqual(order_row[1], "accepted")

    def test_replay_bundle_reads_venue_min_order_shares_fallback_from_manifest(self) -> None:
        """Bundles captured with the live fallback active stamp it into the manifest;
        replay must read it back so order sizing in replay matches what live submitted.
        Bundles without the field (pre-stamp captures) keep the default 0.0 so older
        bundles continue to replay byte-for-byte against their captured behavior."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            stamped_dir = tmp_path / "stamped"
            self._write_bundle(stamped_dir)
            manifest_path = stamped_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["runtime_config"] = {"venue_min_order_shares_fallback": 5.0}
            manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            unstamped_dir = tmp_path / "unstamped"
            self._write_bundle(unstamped_dir)

            from short_horizon.replay_runner import load_replay_bundle

            stamped_bundle = load_replay_bundle(stamped_dir)
            unstamped_bundle = load_replay_bundle(unstamped_dir)

            self.assertEqual(stamped_bundle.manifest["runtime_config"]["venue_min_order_shares_fallback"], 5.0)
            self.assertNotIn("runtime_config", unstamped_bundle.manifest)

            with patch("short_horizon.replay_runner.build_replay_runtime", wraps=build_replay_runtime) as spy:
                replay_bundle(bundle_dir=stamped_dir, db_path=tmp_path / "stamped_replay.sqlite3")
                replay_bundle(bundle_dir=unstamped_dir, db_path=tmp_path / "unstamped_replay.sqlite3")

            stamped_kwargs = spy.call_args_list[0].kwargs
            unstamped_kwargs = spy.call_args_list[1].kwargs
            self.assertEqual(stamped_kwargs["venue_min_order_shares_fallback"], 5.0)
            self.assertEqual(unstamped_kwargs["venue_min_order_shares_fallback"], 0.0)

    def test_capture_writer_stamps_runtime_config_into_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            event_log_path = tmp_path / "stub_live_events.jsonl"
            db_path = tmp_path / "live.sqlite3"
            capture_dir = tmp_path / "capture_bundle"
            event_log_path.write_text(
                "\n".join(json.dumps(row) for row in self._sample_replay_rows()) + "\n", encoding="utf-8"
            )

            run_stub_live(
                stub_event_log_path=event_log_path,
                db_path=db_path,
                run_id="live_capture_stamp_001",
                config_hash="test-config",
                capture_dir=capture_dir,
            )

            manifest = json.loads((capture_dir / "manifest.json").read_text(encoding="utf-8"))
            runtime_config = manifest.get("runtime_config")
            self.assertIsNotNone(runtime_config)
            self.assertEqual(
                runtime_config.get("venue_min_order_shares_fallback"),
                DEFAULT_LIVE_VENUE_MIN_ORDER_SHARES_FALLBACK,
            )

    def test_replay_runner_replays_bundle_into_fresh_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_dir = tmp_path / "capture_bundle"
            db_path = tmp_path / "bundle_replay.sqlite3"
            self._write_bundle(bundle_dir)

            summary = replay_bundle(
                bundle_dir=bundle_dir,
                db_path=db_path,
            )

            self.assertEqual(summary.run_id, "bundle_live_001")
            self.assertEqual(summary.event_count, 10)
            self.assertEqual(summary.order_intents, 1)
            self.assertEqual(summary.synthetic_order_events, 1)

            conn = sqlite3.connect(db_path)
            try:
                run_row = conn.execute(
                    "SELECT run_id, mode, strategy_id, config_hash, finished_at FROM runs WHERE run_id = 'bundle_live_001'"
                ).fetchone()
                event_count = conn.execute(
                    "SELECT COUNT(*) FROM events_log WHERE run_id = 'bundle_live_001'"
                ).fetchone()[0]
                order_row = conn.execute(
                    "SELECT state, venue_order_id, venue_order_status FROM orders WHERE run_id = 'bundle_live_001' AND order_id = 'm1:tok_yes:0.55:225000'"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(run_row[0], "bundle_live_001")
            self.assertEqual(run_row[1], "replay")
            self.assertEqual(run_row[2], "short_horizon_15m_touch_v1")
            self.assertEqual(run_row[3], "test-config")
            self.assertIsNotNone(run_row[4])
            self.assertEqual(event_count, 12)
            self.assertEqual(order_row[0], "accepted")
            self.assertEqual(order_row[1], "venue-live-1")
            self.assertEqual(order_row[2], "live")

    def test_replay_runner_bundle_filters_execution_generated_order_events_from_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_dir = tmp_path / "capture_bundle"
            db_path = tmp_path / "bundle_replay.sqlite3"
            self._write_bundle(bundle_dir, include_execution_accept_event=True)

            summary = replay_bundle(
                bundle_dir=bundle_dir,
                db_path=db_path,
            )

            self.assertEqual(summary.event_count, 10)

            conn = sqlite3.connect(db_path)
            try:
                event_count = conn.execute(
                    "SELECT COUNT(*) FROM events_log WHERE run_id = 'bundle_live_001'"
                ).fetchone()[0]
                accepted_count = conn.execute(
                    "SELECT COUNT(*) FROM events_log WHERE run_id = 'bundle_live_001' AND event_type = 'OrderAccepted'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(event_count, 12)
            self.assertEqual(accepted_count, 1)

    def test_replay_bundle_fails_when_captured_execution_trace_has_unconsumed_calls(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_dir = tmp_path / "capture_bundle"
            db_path = tmp_path / "bundle_replay.sqlite3"
            self._write_bundle(bundle_dir)

            venue_rows = [
                json.loads(line)
                for line in (bundle_dir / "venue_responses.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            venue_rows.append(
                {
                    "seq": 2,
                    "kind": "cancel_order",
                    "key": {"venue_order_id": "venue-live-unused"},
                    "request": {"venue_order_id": "venue-live-unused"},
                    "response": {"order_id": "venue-live-unused", "success": True, "status": "canceled"},
                    "error": None,
                }
            )
            (bundle_dir / "venue_responses.jsonl").write_text(
                "\n".join(json.dumps(row) for row in venue_rows) + "\n",
                encoding="utf-8",
            )
            manifest = json.loads((bundle_dir / "manifest.json").read_text(encoding="utf-8"))
            manifest["files"]["venue_responses"]["count"] = len(venue_rows)
            (bundle_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            with self.assertRaises(ReplayFidelityError) as ctx:
                replay_bundle(
                    bundle_dir=bundle_dir,
                    db_path=db_path,
                )

            self.assertIn("full captured execution trace", str(ctx.exception))
            self.assertIn("cancel_order", str(ctx.exception))

    def test_compare_bundle_to_replay_matches_skip_decision_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_dir = tmp_path / "capture_bundle"
            db_path = tmp_path / "bundle_replay.sqlite3"
            self._write_skip_decision_bundle(bundle_dir)

            summary = replay_bundle(
                bundle_dir=bundle_dir,
                db_path=db_path,
                config=ShortHorizonConfig(risk=RiskConfig(global_safe_mode=True)),
            )
            report = compare_bundle_to_replay(
                bundle_dir=bundle_dir,
                db_path=db_path,
                replay_run_id=summary.run_id,
            )

            self.assertTrue(report.matched)
            self.assertEqual(report.diff_count, 0)
            self.assertEqual(len(report.skip_decisions.matched), 1)
            self.assertEqual(len(report.skip_decisions.live_only), 0)
            self.assertEqual(len(report.skip_decisions.replay_only), 0)

    def test_compare_bundle_to_replay_ignores_non_contract_order_intent_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_dir = tmp_path / "capture_bundle"
            db_path = tmp_path / "bundle_replay.sqlite3"
            self._write_bundle(bundle_dir, include_terminal_cancel_event=True)

            event_rows = [
                json.loads(line)
                for line in (bundle_dir / "events_log.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            event_rows.append(
                {
                    "event_type": "OrderIntent",
                    "event_time": 225000,
                    "ingest_time": 225000,
                    "order_id": "m1:tok_yes:0.55:225000",
                    "strategy_id": "short_horizon_15m_touch_v1",
                    "market_id": "m1",
                    "token_id": "tok_yes",
                    "level": 0.55,
                    "entry_price": 0.55,
                    "notional_usdc": 1.0,
                    "lifecycle_fraction": 0.99,
                    "reason": "manually_edited_reason",
                    "source": "runtime.order_intent",
                    "run_id": "bundle_live_001",
                }
            )
            (bundle_dir / "events_log.jsonl").write_text(
                "\n".join(json.dumps(row) for row in event_rows) + "\n",
                encoding="utf-8",
            )
            manifest = json.loads((bundle_dir / "manifest.json").read_text(encoding="utf-8"))
            manifest["files"]["events_log"]["count"] = len(event_rows)
            (bundle_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            summary = replay_bundle(
                bundle_dir=bundle_dir,
                db_path=db_path,
            )
            report = compare_bundle_to_replay(
                bundle_dir=bundle_dir,
                db_path=db_path,
                replay_run_id=summary.run_id,
            )

            self.assertTrue(report.matched)
            self.assertEqual(report.diff_count, 0)

    def test_compare_bundle_to_replay_ignores_non_contract_skip_decision_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_dir = tmp_path / "capture_bundle"
            db_path = tmp_path / "bundle_replay.sqlite3"
            self._write_skip_decision_bundle(bundle_dir)

            event_rows = [
                json.loads(line)
                for line in (bundle_dir / "events_log.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            for row in event_rows:
                if row.get("event_type") == "SkipDecision":
                    row["details"] = "edited_but_out_of_contract"
                    row["level"] = 0.123
                    row["market_id"] = "edited_market"
                    row["token_id"] = "edited_token"
            (bundle_dir / "events_log.jsonl").write_text(
                "\n".join(json.dumps(row) for row in event_rows) + "\n",
                encoding="utf-8",
            )

            summary = replay_bundle(
                bundle_dir=bundle_dir,
                db_path=db_path,
                config=ShortHorizonConfig(risk=RiskConfig(global_safe_mode=True)),
            )
            report = compare_bundle_to_replay(
                bundle_dir=bundle_dir,
                db_path=db_path,
                replay_run_id=summary.run_id,
            )

            self.assertTrue(report.matched)
            self.assertEqual(report.diff_count, 0)

    def test_compare_bundle_to_replay_matches_terminal_cancel_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_dir = tmp_path / "capture_bundle"
            db_path = tmp_path / "bundle_replay.sqlite3"
            self._write_bundle(bundle_dir, include_terminal_cancel_event=True)

            summary = replay_bundle(
                bundle_dir=bundle_dir,
                db_path=db_path,
            )
            report = compare_bundle_to_replay(
                bundle_dir=bundle_dir,
                db_path=db_path,
                replay_run_id=summary.run_id,
            )

            self.assertTrue(report.matched)
            self.assertEqual(report.diff_count, 0)

    def test_compare_terminal_outcomes_matches_on_order_id_ignoring_client_order_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_dir = tmp_path / "capture_bundle"
            db_path = tmp_path / "bundle_replay.sqlite3"
            self._write_bundle(bundle_dir, include_terminal_cancel_event=True)

            orders_rows = [
                json.loads(line)
                for line in (bundle_dir / "orders_final.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            orders_rows[0]["client_order_id"] = "live-side-client-order-id"
            (bundle_dir / "orders_final.jsonl").write_text(
                "\n".join(json.dumps(row) for row in orders_rows) + "\n",
                encoding="utf-8",
            )

            summary = replay_bundle(bundle_dir=bundle_dir, db_path=db_path)
            report = compare_bundle_to_replay(
                bundle_dir=bundle_dir,
                db_path=db_path,
                replay_run_id=summary.run_id,
            )

            self.assertTrue(report.matched)
            self.assertEqual(report.terminal_outcomes.matched[0].key, orders_rows[0]["order_id"])

    def test_replay_main_compare_exits_nonzero_on_decision_diff(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_dir = tmp_path / "capture_bundle"
            db_path = tmp_path / "bundle_replay.sqlite3"
            report_path = tmp_path / "comparison.txt"
            self._write_bundle(bundle_dir, include_terminal_cancel_event=True)

            event_rows = [
                json.loads(line)
                for line in (bundle_dir / "events_log.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            event_rows = [row for row in event_rows if row.get("event_type") != "OrderCanceled"]
            (bundle_dir / "events_log.jsonl").write_text(
                "\n".join(json.dumps(row) for row in event_rows) + "\n",
                encoding="utf-8",
            )
            manifest = json.loads((bundle_dir / "manifest.json").read_text(encoding="utf-8"))
            manifest["files"]["events_log"]["count"] = len(event_rows)
            (bundle_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            stdout = io.StringIO()
            with patch("sys.stdout", stdout):
                with self.assertRaises(SystemExit) as ctx:
                    replay_main(
                        [
                            str(bundle_dir),
                            str(db_path),
                            "--compare",
                            "--comparison-report-path",
                            str(report_path),
                        ]
                    )

            self.assertEqual(ctx.exception.code, 1)
            self.assertTrue(report_path.exists())
            self.assertIn("Result: DIFF", stdout.getvalue())
            self.assertIn("Terminal outcomes: 1 diff(s)", report_path.read_text(encoding="utf-8"))

    def test_replay_comparator_main_writes_report_for_matching_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_dir = tmp_path / "capture_bundle"
            db_path = tmp_path / "bundle_replay.sqlite3"
            report_path = tmp_path / "comparison.txt"
            self._write_skip_decision_bundle(bundle_dir)

            summary = replay_bundle(
                bundle_dir=bundle_dir,
                db_path=db_path,
                config=ShortHorizonConfig(risk=RiskConfig(global_safe_mode=True)),
            )

            stdout = io.StringIO()
            with patch("sys.stdout", stdout):
                replay_comparator_main(
                    [
                        str(bundle_dir),
                        str(db_path),
                        "--replay-run-id",
                        summary.run_id,
                        "--report-path",
                        str(report_path),
                    ]
                )

            self.assertTrue(report_path.exists())
            self.assertIn("Result: MATCH", stdout.getvalue())
            self.assertIn("Skip decisions: ok", report_path.read_text(encoding="utf-8"))

    def test_render_comparison_report_uses_domain_terms_for_terminal_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_dir = tmp_path / "capture_bundle"
            db_path = tmp_path / "bundle_replay.sqlite3"
            self._write_bundle(bundle_dir, include_terminal_cancel_event=True)

            summary = replay_bundle(
                bundle_dir=bundle_dir,
                db_path=db_path,
            )

            orders_rows = [
                json.loads(line)
                for line in (bundle_dir / "orders_final.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            orders_rows[0]["state"] = "rejected"
            orders_rows[0]["cumulative_filled_size"] = 0.0
            (bundle_dir / "orders_final.jsonl").write_text(
                "\n".join(json.dumps(row) for row in orders_rows) + "\n",
                encoding="utf-8",
            )

            report = compare_bundle_to_replay(
                bundle_dir=bundle_dir,
                db_path=db_path,
                replay_run_id=summary.run_id,
            )
            rendered = render_comparison_report(report)

            self.assertFalse(report.matched)
            self.assertIn("Terminal outcomes: 1 diff(s)", rendered)
            self.assertIn("mismatch order=m1:tok_yes:0.55:225000", rendered)
            self.assertIn("live[state=rejected, filled_qty=0.000000]", rendered)
            self.assertIn("replay[state=cancel_confirmed, filled_qty=0.000000]", rendered)

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
            self.assertEqual(event_count, 12)

    def test_live_runner_parser_supports_stub_and_live_modes(self) -> None:
        parser = build_parser()

        stub_args = parser.parse_args(["live.sqlite3", "--mode", "stub", "--execution-mode", "dry_run", "--stub-event-log-path", "sample.jsonl"])
        self.assertEqual(stub_args.db_path, "live.sqlite3")
        self.assertEqual(stub_args.mode, "stub")
        self.assertEqual(stub_args.execution_mode, "dry_run")
        self.assertEqual(stub_args.stub_event_log_path, "sample.jsonl")

        live_args = parser.parse_args([
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--confirm-live-order",
            "--max-live-orders-total",
            "1",
            "--safe-mode",
            "--max-events",
            "5",
            "--max-runtime-seconds",
            "15",
            "--collector-csv",
            "collector.csv",
        ])
        self.assertEqual(live_args.mode, "live")
        self.assertEqual(live_args.execution_mode, "live")
        self.assertTrue(live_args.allow_live_execution)
        self.assertTrue(live_args.confirm_live_order)
        self.assertEqual(live_args.max_live_orders_total, 1)
        self.assertTrue(live_args.safe_mode)
        self.assertEqual(live_args.max_events, 5)
        self.assertEqual(live_args.max_runtime_seconds, 15.0)
        self.assertEqual(live_args.collector_csv, "collector.csv")

        capture_args = parser.parse_args([
            "live.sqlite3",
            "--mode",
            "stub",
            "--stub-event-log-path",
            "sample.jsonl",
            "--capture-dir",
            "captures/run_001",
        ])
        self.assertEqual(capture_args.capture_dir, "captures/run_001")

        approval_args = parser.parse_args([
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--approve-allowances",
            "--max-events",
            "1",
        ])
        self.assertTrue(approval_args.approve_allowances)

        redeem_args = parser.parse_args([
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--redeem-resolved",
            "--redeem-resolved-interval-seconds",
            "60",
            "--no-final-redeem",
            "--max-events",
            "1",
        ])
        self.assertTrue(redeem_args.redeem_resolved)
        self.assertEqual(redeem_args.redeem_resolved_interval_seconds, 60.0)
        self.assertTrue(redeem_args.no_final_redeem)

        bridge_args = parser.parse_args([
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--bridge-polygon-usdc-to-usdce",
            "--bridge-polygon-usdc-amount",
            "2.05",
            "--max-events",
            "1",
        ])
        self.assertTrue(bridge_args.bridge_polygon_usdc_to_usdce)
        self.assertEqual(bridge_args.bridge_polygon_usdc_amount, "2.05")

    def test_live_runner_cli_validation_requires_live_input_for_live_execution(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["live.sqlite3", "--mode", "stub", "--execution-mode", "live", "--stub-event-log-path", "sample.jsonl"])

        with self.assertRaises(SystemExit):
            validate_cli_args(parser, args)

    def test_live_runner_cli_validation_requires_private_key_for_live_execution(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--max-events",
            "1",
        ])

        env_backup = os.environ.get(PRIVATE_KEY_ENV_VAR)
        try:
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(SystemExit):
                    validate_cli_args(parser, args)
        finally:
            if env_backup is not None:
                os.environ[PRIVATE_KEY_ENV_VAR] = env_backup

    def test_live_runner_cli_validation_requires_explicit_live_ack(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--max-events",
            "1",
        ])

        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            with self.assertRaises(SystemExit):
                validate_cli_args(parser, args)

    def test_live_runner_cli_validation_for_kill_switch(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "live.sqlite3",
            "--kill-switch",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
        ])

        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            self.assertEqual(validate_cli_args(parser, args), ExecutionMode.LIVE)

        # Missing allow-live-execution
        args = parser.parse_args(["live.sqlite3", "--kill-switch", "--mode", "live", "--execution-mode", "live"])
        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            with self.assertRaises(SystemExit):
                validate_cli_args(parser, args)

    def test_live_runner_cli_validation_for_approve_allowances(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "live.sqlite3",
            "--approve-allowances",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--max-events",
            "1",
        ])

        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            self.assertEqual(validate_cli_args(parser, args), ExecutionMode.LIVE)

        bad_args = parser.parse_args(["live.sqlite3", "--approve-allowances"])
        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            with self.assertRaises(SystemExit):
                validate_cli_args(parser, bad_args)

        bad_combo = parser.parse_args([
            "live.sqlite3",
            "--approve-allowances",
            "--kill-switch",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
        ])
        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            with self.assertRaises(SystemExit):
                validate_cli_args(parser, bad_combo)

    def test_execute_allowance_approve_uses_client_and_summarizes(self) -> None:
        client = _FakeLiveRunnerClient()

        summary = execute_allowance_approve("approve_test_001", execution_client=client)

        self.assertEqual(
            summary,
            AllowanceApprovalSummary(total_actions=2, approved_count=1, already_approved_count=1),
        )
        self.assertTrue(client.started)
        self.assertEqual(client.approve_calls, 1)

    def test_live_runner_cli_validation_for_redeem_resolved(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "live.sqlite3",
            "--redeem-resolved",
            "--redeem-resolved-interval-seconds",
            "60",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--max-events",
            "1",
        ])

        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            self.assertEqual(validate_cli_args(parser, args), ExecutionMode.LIVE)

        bad_args = parser.parse_args(["live.sqlite3", "--redeem-resolved-interval-seconds", "60"])
        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            with self.assertRaises(SystemExit):
                validate_cli_args(parser, bad_args)

        bad_interval = parser.parse_args([
            "live.sqlite3",
            "--redeem-resolved",
            "--redeem-resolved-interval-seconds",
            "0",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--max-events",
            "1",
        ])
        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            with self.assertRaises(SystemExit):
                validate_cli_args(parser, bad_interval)

    def test_execute_resolved_redeem_uses_client_and_summarizes(self) -> None:
        client = _FakeLiveRunnerClient()

        summary = execute_resolved_redeem("redeem_test_001", execution_client=client)

        self.assertEqual(
            summary,
            ResolvedRedeemSummary(
                total_actions=3,
                redeemed_count=1,
                skipped_negative_risk_count=1,
                skipped_proxy_wallet_count=1,
                error_count=0,
                auto_wrap_status="wrapped",
                auto_wrap_tx_hash="0xwraptx",
            ),
        )
        self.assertEqual(client.redeem_calls, 1)
        self.assertEqual(client.wrap_calls, 1)

    def test_execute_resolved_redeem_skips_wrap_when_no_redeems(self) -> None:
        from short_horizon.venue_polymarket.execution_client import RedeemResult

        client = _FakeLiveRunnerClient()
        client.redeem_resolved_positions = lambda: [
            RedeemResult(condition_id="0xc1", status="skipped_proxy_wallet", position_count=1, proxy_wallet="0xpx"),
        ]

        summary = execute_resolved_redeem("redeem_test_002", execution_client=client)

        self.assertEqual(summary.redeemed_count, 0)
        self.assertEqual(summary.auto_wrap_status, "skipped_no_redeems")
        self.assertIsNone(summary.auto_wrap_tx_hash)
        self.assertEqual(client.wrap_calls, 0)

    def test_execute_resolved_redeem_skips_wrap_when_balance_zero(self) -> None:
        from short_horizon.venue_polymarket.execution_client import ExecutionClientConfigError

        client = _FakeLiveRunnerClient()
        client.wrap_raises = ExecutionClientConfigError("Polygon USDC.e balance is zero, nothing to wrap")

        summary = execute_resolved_redeem("redeem_test_003", execution_client=client)

        self.assertEqual(summary.redeemed_count, 1)
        self.assertEqual(summary.auto_wrap_status, "skipped_balance_zero")
        self.assertIsNone(summary.auto_wrap_tx_hash)
        self.assertEqual(client.wrap_calls, 1)

    def test_execute_resolved_redeem_marks_wrap_failure_but_returns_summary(self) -> None:
        client = _FakeLiveRunnerClient()
        client.wrap_raises = RuntimeError("rpc connection refused")

        summary = execute_resolved_redeem("redeem_test_004", execution_client=client)

        self.assertEqual(summary.redeemed_count, 1)
        self.assertEqual(summary.auto_wrap_status, "failed")
        self.assertIsNone(summary.auto_wrap_tx_hash)

    def test_live_runner_cli_validation_for_polygon_usdc_bridge(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "live.sqlite3",
            "--bridge-polygon-usdc-to-usdce",
            "--bridge-polygon-usdc-amount",
            "2.05",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--max-events",
            "1",
        ])

        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            self.assertEqual(validate_cli_args(parser, args), ExecutionMode.LIVE)

        bad_args = parser.parse_args(["live.sqlite3", "--bridge-polygon-usdc-to-usdce"])
        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            with self.assertRaises(SystemExit):
                validate_cli_args(parser, bad_args)

        bad_amount = parser.parse_args([
            "live.sqlite3",
            "--bridge-polygon-usdc-to-usdce",
            "--bridge-polygon-usdc-amount",
            "2.0000001",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--max-events",
            "1",
        ])
        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            with self.assertRaises(SystemExit):
                validate_cli_args(parser, bad_amount)

        bad_combo = parser.parse_args([
            "live.sqlite3",
            "--bridge-polygon-usdc-to-usdce",
            "--kill-switch",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
        ])
        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            with self.assertRaises(SystemExit):
                validate_cli_args(parser, bad_combo)

    def test_execute_polygon_usdc_bridge_uses_client_and_summarizes(self) -> None:
        client = _FakeLiveRunnerClient()

        summary = execute_polygon_usdc_bridge("bridge_test_001", execution_client=client, amount_base_unit=2_050_000)

        self.assertEqual(
            summary,
            PolygonUsdcBridgeSummary(
                source_amount_base_unit=2_050_000,
                deposit_address="0xdeposit",
                wallet_transfer_tx_hash="0xwallettransfer",
                bridge_status="COMPLETED",
                bridge_tx_hash="0xbridgehash",
                quoted_target_amount_base_unit=2_094_000,
                collateral_delta_base_unit=2_094_000,
            ),
        )
        self.assertEqual(client.bridge_calls, [2_050_000])

    def test_execute_kill_switch_cancels_all_open_orders_and_returns_summary(self) -> None:
        client = _FakeLiveRunnerClient()
        client.open_orders_by_market[None] = [
            VenueOrderState(order_id="venue-live-1", status="live", market_id="m1", token_id="tok_yes"),
            VenueOrderState(order_id="venue-live-2", status="live", market_id="m2", token_id="tok_no"),
        ]

        summary = execute_kill_switch(run_id="kill_switch_test_001", execution_client=client)

        self.assertTrue(client.started)
        self.assertEqual(client.cancel_calls, ["venue-live-1", "venue-live-2"])
        self.assertEqual(summary, KillSwitchSummary(open_order_count=2, canceled_count=2, failed_count=0))

    def test_main_runs_kill_switch_and_returns_early(self) -> None:
        argv = [
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--kill-switch",
        ]

        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True), patch(
            "short_horizon.live_runner.execute_kill_switch",
            return_value=KillSwitchSummary(open_order_count=1, canceled_count=1, failed_count=0),
        ) as execute_mock, patch("short_horizon.live_runner.asyncio.run", side_effect=AssertionError("should not run live loop")):
            self.assertIsNone(main(argv))

        execute_mock.assert_called_once_with(None)

    def test_main_kill_switch_exits_nonzero_when_cancel_has_failures(self) -> None:
        argv = [
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--kill-switch",
        ]

        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True), patch(
            "short_horizon.live_runner.execute_kill_switch",
            return_value=KillSwitchSummary(open_order_count=2, canceled_count=1, failed_count=1),
        ):
            with self.assertRaises(SystemExit) as ctx:
                main(argv)

        self.assertEqual(ctx.exception.code, 1)

    def test_main_live_mode_runs_polygon_usdc_bridge_before_allowances_and_live_loop(self) -> None:
        argv = [
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--redeem-resolved",
            "--redeem-resolved-interval-seconds",
            "60",
            "--bridge-polygon-usdc-to-usdce",
            "--bridge-polygon-usdc-amount",
            "2.05",
            "--approve-allowances",
            "--max-events",
            "1",
        ]

        fake_summary = type("Summary", (), {"run_id": "live_test_001", "event_count": 0, "order_intents": 0, "synthetic_order_events": 0, "db_path": "live.sqlite3"})()
        fake_probe = type(
            "ProbeSummary",
            (),
            {
                "run_id": "live_test_001",
                "total_events": 0,
                "market_state_updates": 0,
                "book_updates": 0,
                "trade_ticks": 0,
                "order_events": 0,
                "distinct_markets": 0,
                "distinct_tokens": 0,
                "first_event_time": None,
                "last_event_time": None,
                "window_minutes": 0.0,
                "book_updates_per_minute": 0.0,
            },
        )()

        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True), patch(
            "short_horizon.live_runner.PolymarketExecutionClient",
            return_value=_FakeLiveRunnerClient(),
        ) as client_ctor, patch(
            "short_horizon.live_runner.execute_resolved_redeem",
            return_value=ResolvedRedeemSummary(
                total_actions=1,
                redeemed_count=1,
                skipped_negative_risk_count=0,
                skipped_proxy_wallet_count=0,
                error_count=0,
            ),
        ) as redeem_mock, patch(
            "short_horizon.live_runner.execute_polygon_usdc_bridge",
            return_value=PolygonUsdcBridgeSummary(
                source_amount_base_unit=2_050_000,
                deposit_address="0xdeposit",
                wallet_transfer_tx_hash="0xwallettransfer",
                bridge_status="COMPLETED",
                bridge_tx_hash="0xbridgehash",
                quoted_target_amount_base_unit=2_094_000,
                collateral_delta_base_unit=2_094_000,
            ),
        ) as bridge_mock, patch(
            "short_horizon.live_runner.execute_allowance_approve",
            return_value=AllowanceApprovalSummary(total_actions=2, approved_count=1, already_approved_count=1),
        ) as approve_mock, patch(
            "short_horizon.live_runner.run_live",
            return_value=fake_summary,
        ) as run_live_mock, patch(
            "short_horizon.live_runner.asyncio.run",
            side_effect=lambda coro: coro.close() or fake_summary,
        ), patch(
            "short_horizon.live_runner.summarize_probe_db",
            return_value=fake_probe,
        ), patch(
            "short_horizon.live_runner.assert_min_book_updates_per_minute",
            return_value=None,
        ):
            self.assertIsNone(main(argv))

        client = client_ctor.return_value
        self.assertEqual(
            redeem_mock.call_args_list,
            [
                call(None, execution_client=client),
                call("live_test_001", execution_client=client),
            ],
        )
        bridge_mock.assert_called_once_with(
            None,
            execution_client=client,
            amount_base_unit=2_050_000,
        )
        approve_mock.assert_called_once_with(None, execution_client=client, venue_version="v1")
        self.assertEqual(run_live_mock.call_args.kwargs["execution_client"], client)
        self.assertEqual(run_live_mock.call_args.kwargs["resolved_redeem_interval_seconds"], 60.0)

    def test_main_live_mode_can_disable_final_redeem(self) -> None:
        argv = [
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--no-final-redeem",
            "--max-events",
            "1",
        ]

        fake_summary = type("Summary", (), {"run_id": "live_test_002", "event_count": 0, "order_intents": 0, "synthetic_order_events": 0, "db_path": "live.sqlite3"})()
        fake_probe = type(
            "ProbeSummary",
            (),
            {
                "run_id": "live_test_002",
                "total_events": 0,
                "market_state_updates": 0,
                "book_updates": 0,
                "trade_ticks": 0,
                "order_events": 0,
                "distinct_markets": 0,
                "distinct_tokens": 0,
                "first_event_time": None,
                "last_event_time": None,
                "window_minutes": 0.0,
                "book_updates_per_minute": 0.0,
            },
        )()

        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True), patch(
            "short_horizon.live_runner.PolymarketExecutionClient",
            return_value=_FakeLiveRunnerClient(),
        ) as client_ctor, patch(
            "short_horizon.live_runner.execute_resolved_redeem",
            return_value=ResolvedRedeemSummary(
                total_actions=0,
                redeemed_count=0,
                skipped_negative_risk_count=0,
                skipped_proxy_wallet_count=0,
                error_count=0,
            ),
        ) as redeem_mock, patch(
            "short_horizon.live_runner.run_live",
            return_value=fake_summary,
        ) as run_live_mock, patch(
            "short_horizon.live_runner.asyncio.run",
            side_effect=lambda coro: coro.close() or fake_summary,
        ), patch(
            "short_horizon.live_runner.summarize_probe_db",
            return_value=fake_probe,
        ), patch(
            "short_horizon.live_runner.assert_min_book_updates_per_minute",
            return_value=None,
        ):
            self.assertIsNone(main(argv))

        client_ctor.assert_not_called()
        redeem_mock.assert_not_called()
        self.assertIsNone(run_live_mock.call_args.kwargs["execution_client"])

    def test_live_runner_cli_validation_requires_bounded_live_probe(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
        ])

        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True):
            with self.assertRaises(SystemExit):
                validate_cli_args(parser, args)

    def test_live_runner_cli_validation_rejects_non_positive_runtime_caps(self) -> None:
        parser = build_parser()

        args = parser.parse_args(["live.sqlite3", "--max-events", "0"])
        with self.assertRaises(SystemExit):
            validate_cli_args(parser, args)

        args = parser.parse_args(["live.sqlite3", "--max-runtime-seconds", "0"])
        with self.assertRaises(SystemExit):
            validate_cli_args(parser, args)

        args = parser.parse_args(["live.sqlite3", "--max-live-orders-total", "0"])
        with self.assertRaises(SystemExit):
            validate_cli_args(parser, args)

    def test_live_runner_cli_validation_rejects_live_confirmation_flags_outside_live_mode(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "live.sqlite3",
            "--mode",
            "stub",
            "--execution-mode",
            "dry_run",
            "--stub-event-log-path",
            "sample.jsonl",
            "--confirm-live-order",
        ])

        with self.assertRaises(SystemExit):
            validate_cli_args(parser, args)

    def test_live_runner_cli_validation_requires_tty_for_confirm_live_order(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--confirm-live-order",
            "--max-live-orders-total",
            "1",
            "--max-events",
            "1",
        ])

        with patch.dict(os.environ, {PRIVATE_KEY_ENV_VAR: "test-private-key"}, clear=True), patch("sys.stdin.isatty", return_value=False):
            with self.assertRaises(SystemExit):
                validate_cli_args(parser, args)

    def test_build_live_submit_guard_returns_operator_guard(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "live.sqlite3",
            "--mode",
            "live",
            "--execution-mode",
            "live",
            "--allow-live-execution",
            "--confirm-live-order",
            "--max-live-orders-total",
            "1",
            "--max-events",
            "1",
        ])

        guard = build_live_submit_guard(args)

        self.assertIsInstance(guard, OperatorConfirmLiveOrderGuard)
        self.assertTrue(guard.require_confirmation)
        self.assertEqual(guard.max_live_orders_total, 1)

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
                    BookUpdate(
                        event_time_ms=225_010,
                        ingest_time_ms=225_030,
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
            self.assertEqual(event_count, 5)
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
                    BookUpdate(
                        event_time_ms=225_010,
                        ingest_time_ms=225_030,
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
                max_events=4,
                execution_mode=ExecutionMode.DRY_RUN,
            )

            self.assertEqual(summary.run_id, "live_dry_run_test_001")
            self.assertEqual(summary.event_count, 4)
            self.assertEqual(summary.order_intents, 1)
            self.assertEqual(summary.synthetic_order_events, 2)

            conn = sqlite3.connect(db_path)
            try:
                event_rows = conn.execute(
                    "SELECT event_type, payload_json FROM events_log WHERE run_id = 'live_dry_run_test_001' ORDER BY seq ASC"
                ).fetchall()
                order_row = conn.execute(
                    "SELECT state, client_order_id, venue_order_id, venue_order_status FROM orders WHERE run_id = 'live_dry_run_test_001' AND order_id = 'm1:tok_yes:0.55:225000'"
                ).fetchone()
                fill_count = conn.execute(
                    "SELECT COUNT(*) FROM fills WHERE run_id = 'live_dry_run_test_001' AND order_id = 'm1:tok_yes:0.55:225000'"
                ).fetchone()[0]
            finally:
                conn.close()

            event_types = [row[0] for row in event_rows]
            self.assertEqual(event_types.count("TimerEvent"), 1)
            self.assertEqual(event_types.count("OrderAccepted"), 1)
            self.assertEqual(event_types.count("OrderFilled"), 1)
            self.assertEqual(fill_count, 1)
            self.assertEqual(order_row[0], "filled")
            self.assertIsNotNone(order_row[1])
            self.assertIsNone(order_row[2])
            self.assertEqual(order_row[3], "filled")
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
                        remaining_size=1.0 / 0.55,
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

    async def test_live_runner_emits_strategy_cancel_when_market_closes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_exec_strategy_cancel.sqlite3"
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
                    MarketStateUpdate(
                        event_time_ms=900_000,
                        ingest_time_ms=900_050,
                        market_id="m1",
                        token_id="tok_yes",
                        condition_id="c1",
                        question="Bitcoin Up or Down?",
                        asset_slug="bitcoin",
                        start_time_ms=0,
                        end_time_ms=900_000,
                        is_active=False,
                        metadata_is_fresh=True,
                        fee_rate_bps=10.0,
                        fee_fetched_at_ms=900_050,
                        fee_metadata_age_ms=0,
                    ),
                ]
            )
            client = _FakeLiveRunnerClient()

            summary = await run_live(
                db_path=db_path,
                run_id="live_exec_strategy_cancel_test_001",
                config=ShortHorizonConfig(
                    execution=ExecutionConfig(
                        target_trade_size_usdc=10.0,
                        stale_market_data_threshold_ms=2000,
                        hold_to_resolution=False,
                    )
                ),
                config_hash="test-config",
                source=source,
                max_events=4,
                execution_mode=ExecutionMode.LIVE,
                execution_client=client,
            )

            self.assertEqual(summary.run_id, "live_exec_strategy_cancel_test_001")
            self.assertEqual(client.cancel_calls, ["venue-live-1"])
            conn = sqlite3.connect(db_path)
            try:
                order_row = conn.execute(
                    "SELECT state, venue_order_status FROM orders WHERE run_id = 'live_exec_strategy_cancel_test_001' AND order_id = 'm1:tok_yes:0.55:225000'"
                ).fetchone()
                canceled_events = conn.execute(
                    "SELECT COUNT(*) FROM events_log WHERE run_id = 'live_exec_strategy_cancel_test_001' AND event_type = 'OrderCanceled' AND order_id = 'm1:tok_yes:0.55:225000'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(order_row[0], "cancel_confirmed")
            self.assertEqual(order_row[1], "canceled")
            self.assertEqual(canceled_events, 1)

    async def test_live_runner_blocks_market_after_startup_unknown_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_exec_unknown_block.sqlite3"
            run_id = "live_exec_unknown_block_test_001"
            seed_store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id=run_id,
                    strategy_id="short_horizon_15m_touch_v1",
                    mode="live",
                    config_hash="test-config",
                ),
            )
            try:
                seed_store.insert_order(
                    order_id="ord_unknown_001",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.55,
                    size=10.0,
                    state=OrderState.ACCEPTED,
                    client_order_id="cli_unknown_001",
                    venue_order_id="venue-missing-1",
                    intent_created_at_ms=210_000,
                    last_state_change_at_ms=210_000,
                    remaining_size=10.0,
                    venue_order_status="live",
                    reconciliation_required=True,
                )
            finally:
                seed_store.close()

            source = _AsyncNormalizedSource(
                [
                    MarketStateUpdate(
                        event_time_ms=200_000,
                        ingest_time_ms=200_050,
                        market_id="m1",
                        token_id="tok_no",
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
                        token_id="tok_no",
                        best_bid=0.53,
                        best_ask=0.54,
                    ),
                    BookUpdate(
                        event_time_ms=225_000,
                        ingest_time_ms=225_020,
                        market_id="m1",
                        token_id="tok_no",
                        best_bid=0.54,
                        best_ask=0.55,
                    ),
                ]
            )
            client = _FakeLiveRunnerClient()

            summary = await run_live(
                db_path=db_path,
                run_id=run_id,
                config_hash="test-config",
                source=source,
                max_events=3,
                execution_mode=ExecutionMode.LIVE,
                execution_client=client,
            )

            self.assertEqual(summary.run_id, run_id)
            self.assertEqual(len(client.place_calls), 0)

            conn = sqlite3.connect(db_path)
            try:
                blocked_order = conn.execute(
                    "SELECT state, reconciliation_required FROM orders WHERE run_id = ? AND order_id = 'ord_unknown_001'",
                    (run_id,),
                ).fetchone()
                other_token_orders = conn.execute(
                    "SELECT COUNT(*) FROM orders WHERE run_id = ? AND token_id = 'tok_no'",
                    (run_id,),
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(blocked_order[0], "unknown")
            self.assertEqual(blocked_order[1], 1)
            self.assertEqual(other_token_orders, 0)

    async def test_live_runner_blocks_new_order_when_open_order_cap_is_already_reached(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_exec_open_cap.sqlite3"
            run_id = "live_exec_open_cap_test_001"
            seed_store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id=run_id,
                    strategy_id="short_horizon_15m_touch_v1",
                    mode="live",
                    config_hash="test-config",
                ),
            )
            try:
                seed_store.insert_order(
                    order_id="ord_existing_001",
                    market_id="m2",
                    token_id="tok_other",
                    side="BUY",
                    price=0.55,
                    size=10.0,
                    state=OrderState.ACCEPTED,
                    client_order_id="cli_existing_001",
                    venue_order_id="venue-existing-1",
                    intent_created_at_ms=210_000,
                    last_state_change_at_ms=210_000,
                    remaining_size=10.0,
                    venue_order_status="live",
                    reconciliation_required=True,
                )
            finally:
                seed_store.close()

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
            client.order_lookup_by_id["venue-existing-1"] = VenueOrderState(
                order_id="venue-existing-1",
                status="live",
                market_id="m2",
                token_id="tok_other",
                side="buy",
                price=0.55,
                original_size=10.0,
                remaining_size=10.0,
                cumulative_filled_size=0.0,
                client_order_id="cli_existing_001",
            )

            summary = await run_live(
                db_path=db_path,
                run_id=run_id,
                config=ShortHorizonConfig(risk=RiskConfig(max_open_orders_total=1, micro_live_cumulative_stake_cap_usdc=100.0)),
                config_hash="test-config",
                source=source,
                max_events=3,
                execution_mode=ExecutionMode.LIVE,
                execution_client=client,
            )

            self.assertEqual(summary.run_id, run_id)
            self.assertEqual(len(client.place_calls), 0)

            conn = sqlite3.connect(db_path)
            try:
                order_count = conn.execute(
                    "SELECT COUNT(*) FROM orders WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                existing_row = conn.execute(
                    "SELECT state, venue_order_status FROM orders WHERE run_id = ? AND order_id = 'ord_existing_001'",
                    (run_id,),
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(order_count, 1)
            self.assertEqual(existing_row[0], "accepted")
            self.assertEqual(existing_row[1], "live")

    async def test_live_runner_global_safe_mode_blocks_new_order_but_keeps_live_path_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_exec_safe_mode.sqlite3"
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
                run_id="live_exec_safe_mode_test_001",
                config=ShortHorizonConfig(risk=RiskConfig(global_safe_mode=True, max_open_orders_total=10, micro_live_cumulative_stake_cap_usdc=100.0)),
                config_hash="test-config",
                source=source,
                max_events=3,
                execution_mode=ExecutionMode.LIVE,
                execution_client=client,
            )

            self.assertEqual(summary.run_id, "live_exec_safe_mode_test_001")
            self.assertTrue(client.started)
            self.assertEqual(len(client.place_calls), 0)

            conn = sqlite3.connect(db_path)
            try:
                order_count = conn.execute(
                    "SELECT COUNT(*) FROM orders WHERE run_id = 'live_exec_safe_mode_test_001'"
                ).fetchone()[0]
                total_events = conn.execute(
                    "SELECT COUNT(*) FROM events_log WHERE run_id = 'live_exec_safe_mode_test_001'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(order_count, 0)
            self.assertEqual(total_events, 4)

    def test_build_live_runtime_hydrates_strategy_open_orders_from_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_restart.sqlite3"
            seed_store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="live_restart_test_001",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                seed_store.insert_order(
                    order_id="ord_seed_1",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.55,
                    size=18.1818181818,
                    state=OrderState.ACCEPTED,
                    client_order_id="cid-1",
                    venue_order_id="venue-1",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_001,
                    remaining_size=18.1818181818,
                    venue_order_status="live",
                    reconciliation_required=False,
                )
            finally:
                seed_store.close()

            runtime = build_live_runtime(db_path=db_path, run_id="live_restart_test_001", config_hash="test-config")
            try:
                self.assertEqual(runtime.strategy.active_order_ids_by_market_token, {("m1", "tok_yes"): "ord_seed_1"})
                self.assertEqual(runtime.strategy.active_inventory_by_market, {})
            finally:
                runtime.store.close()

    def test_build_live_runtime_hydrates_filled_inventory_exposure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_restart_inventory.sqlite3"
            seed_store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="live_restart_test_inventory_001",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                seed_store.insert_order(
                    order_id="ord_seed_fill_1",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.55,
                    size=18.1818181818,
                    state=OrderState.FILLED,
                    client_order_id="cid-fill-1",
                    venue_order_id="venue-fill-1",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_100,
                    remaining_size=0.0,
                    venue_order_status="matched",
                    reconciliation_required=False,
                )
                seed_store.update_order_state(
                    order_id="ord_seed_fill_1",
                    state=OrderState.FILLED,
                    event_time_ms=225_100,
                    cumulative_filled_size=18.1818181818,
                    remaining_size=0.0,
                )
            finally:
                seed_store.close()

            runtime = build_live_runtime(db_path=db_path, run_id="live_restart_test_inventory_001", config_hash="test-config")
            try:
                self.assertEqual(runtime.strategy.active_order_ids_by_market_token, {})
                self.assertEqual(runtime.strategy.active_inventory_by_market, {"m1": {"tok_yes"}})
            finally:
                runtime.store.close()

    def test_reconcile_runtime_orders_refreshes_strategy_after_startup_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_restart_reconcile.sqlite3"
            seed_store = SQLiteRuntimeStore(
                db_path,
                run=RunContext(
                    run_id="live_restart_test_002",
                    strategy_id="short_horizon_15m_touch_v1",
                    config_hash="test-config",
                ),
            )
            try:
                seed_store.insert_order(
                    order_id="ord_seed_2",
                    market_id="m1",
                    token_id="tok_yes",
                    side="BUY",
                    price=0.55,
                    size=18.1818181818,
                    state=OrderState.ACCEPTED,
                    client_order_id="cid-2",
                    venue_order_id="venue-2",
                    intent_created_at_ms=225_000,
                    last_state_change_at_ms=225_001,
                    remaining_size=18.1818181818,
                    venue_order_status="live",
                    reconciliation_required=True,
                )
            finally:
                seed_store.close()

            runtime = build_live_runtime(db_path=db_path, run_id="live_restart_test_002", config_hash="test-config")
            client = _FakeLiveRunnerClient()
            client.order_lookup_by_id["venue-2"] = VenueOrderState(
                order_id="venue-2",
                status="canceled",
                market_id="m1",
                token_id="tok_yes",
                side="buy",
                price=0.55,
                original_size=18.1818181818,
                remaining_size=18.1818181818,
                cumulative_filled_size=0.0,
                client_order_id="cid-2",
            )
            try:
                reconciled = reconcile_runtime_orders(
                    runtime=runtime,
                    execution_client=client,
                    execution_mode=ExecutionMode.LIVE,
                )

                self.assertEqual(reconciled, 1)
                self.assertEqual(runtime.strategy.active_order_ids_by_market_token, {})
                self.assertEqual(runtime.strategy.active_inventory_by_market, {})
            finally:
                runtime.store.close()

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
                def __init__(self, *, user_stream=None, fee_info_fetcher=None, **_kwargs):
                    captured["user_stream"] = user_stream
                    captured["fee_info_fetcher"] = fee_info_fetcher
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

    async def test_live_runner_probe_cross_validation_missing_collector_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "live_probe.sqlite3"
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
                run_id="live_probe_missing_collector_test_001",
                config_hash="test-config",
                source=source,
                max_runtime_seconds=1.0,
            )

            self.assertEqual(summary.event_count, 2)
            validation = maybe_cross_validate_probe_against_collector(
                db_path,
                run_id="live_probe_missing_collector_test_001",
                collector_csv_path=Path(tmpdir) / "missing.csv",
            )
            self.assertIsNone(validation)

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
                self.assertFalse(store.has_unknown_order_for_market("m1"))

                store.insert_order(
                    order_id="ord_unknown_001",
                    market_id="m1",
                    token_id="tok_no",
                    side="BUY",
                    price=0.55,
                    size=5.0,
                    state=OrderState.UNKNOWN,
                    client_order_id="cli_unknown_001",
                    intent_created_at_ms=225_300,
                    last_state_change_at_ms=225_300,
                    remaining_size=5.0,
                    reconciliation_required=True,
                )

                self.assertTrue(store.has_unknown_order_for_market("m1"))
                self.assertFalse(store.has_unknown_order_for_market("m2"))

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
