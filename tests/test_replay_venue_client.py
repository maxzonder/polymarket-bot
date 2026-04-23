from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.replay import CapturedResponseExecutionClient, ReplayFidelityError
from short_horizon.replay_runner import load_replay_bundle
from short_horizon.venue_polymarket.execution_client import VenueOrderRequest


class CapturedResponseExecutionClientTest(unittest.TestCase):
    @staticmethod
    def _place_request(*, client_order_id: str = "cid-1") -> VenueOrderRequest:
        return VenueOrderRequest(
            token_id="tok_yes",
            side="BUY",
            price=0.55,
            size=1.836364,
            client_order_id=client_order_id,
            time_in_force="GTC",
            post_only=False,
        )

    @staticmethod
    def _record(kind: str, *, key=None, request=None, response=None, error=None, seq: int = 1) -> dict:
        return {
            "seq": seq,
            "kind": kind,
            "key": key,
            "request": request,
            "response": response,
            "error": error,
        }

    def test_place_order_matches_captured_client_order_id(self) -> None:
        client = CapturedResponseExecutionClient(
            [
                self._record(
                    "place_order",
                    key={"client_order_id": "cid-1"},
                    request={
                        "token_id": "tok_yes",
                        "side": "BUY",
                        "price": 0.55,
                        "size": 1.836364,
                        "client_order_id": "cid-1",
                        "time_in_force": "GTC",
                        "post_only": False,
                    },
                    response={"order_id": "venue-1", "status": "live", "client_order_id": "cid-1"},
                )
            ]
        )

        result = client.place_order(self._place_request())

        self.assertEqual(result.order_id, "venue-1")
        self.assertEqual(result.status, "live")
        self.assertEqual(result.client_order_id, "cid-1")

    def test_cancel_order_matches_captured_venue_order_id(self) -> None:
        client = CapturedResponseExecutionClient(
            [
                self._record(
                    "cancel_order",
                    key={"venue_order_id": "venue-1"},
                    request={"venue_order_id": "venue-1"},
                    response={"order_id": "venue-1", "success": True, "status": "canceled"},
                )
            ]
        )

        result = client.cancel_order("venue-1")

        self.assertEqual(result.order_id, "venue-1")
        self.assertTrue(result.success)
        self.assertEqual(result.status, "canceled")

    def test_get_order_returns_captured_state(self) -> None:
        client = CapturedResponseExecutionClient(
            [
                self._record(
                    "get_order",
                    key={"venue_order_id": "venue-1"},
                    request={"venue_order_id": "venue-1"},
                    response={
                        "order_id": "venue-1",
                        "status": "live",
                        "client_order_id": "cid-1",
                        "market_id": "m1",
                        "token_id": "tok_yes",
                        "side": "BUY",
                        "price": 0.55,
                        "original_size": 1.836364,
                        "cumulative_filled_size": 0.25,
                        "remaining_size": 1.586364,
                    },
                )
            ]
        )

        result = client.get_order("venue-1")

        self.assertEqual(result.order_id, "venue-1")
        self.assertEqual(result.status, "live")
        self.assertEqual(result.client_order_id, "cid-1")
        self.assertEqual(result.remaining_size, 1.586364)

    def test_list_open_orders_returns_captured_states(self) -> None:
        client = CapturedResponseExecutionClient(
            [
                self._record(
                    "list_open_orders",
                    key={"market_id": "m1"},
                    request={"market_id": "m1"},
                    response=[
                        {
                            "order_id": "venue-1",
                            "status": "live",
                            "client_order_id": "cid-1",
                            "market_id": "m1",
                            "token_id": "tok_yes",
                        },
                        {
                            "order_id": "venue-2",
                            "status": "partially_filled",
                            "client_order_id": "cid-2",
                            "market_id": "m1",
                            "token_id": "tok_yes",
                        },
                    ],
                )
            ]
        )

        result = client.list_open_orders("m1")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].order_id, "venue-1")
        self.assertEqual(result[1].status, "partially_filled")

    def test_unmatched_place_order_raises_replay_fidelity_error(self) -> None:
        client = CapturedResponseExecutionClient(
            [
                self._record(
                    "place_order",
                    key={"client_order_id": "cid-live"},
                    request={
                        "token_id": "tok_yes",
                        "side": "BUY",
                        "price": 0.55,
                        "size": 1.836364,
                        "client_order_id": "cid-live",
                        "time_in_force": "GTC",
                        "post_only": False,
                    },
                    response={"order_id": "venue-1", "status": "live", "client_order_id": "cid-live"},
                )
            ]
        )

        with self.assertRaises(ReplayFidelityError) as ctx:
            client.place_order(self._place_request(client_order_id="cid-replay-only"))

        self.assertIn("Replay fidelity mismatch for place_order", str(ctx.exception))
        self.assertIn("cid-replay-only", str(ctx.exception))

    def test_captured_error_raises_replay_fidelity_error(self) -> None:
        client = CapturedResponseExecutionClient(
            [
                self._record(
                    "cancel_order",
                    key={"venue_order_id": "venue-1"},
                    request={"venue_order_id": "venue-1"},
                    error={"type": "VenueError", "message": "cancel rejected"},
                )
            ]
        )

        with self.assertRaises(ReplayFidelityError) as ctx:
            client.cancel_order("venue-1")

        self.assertIn("Captured cancel_order error VenueError", str(ctx.exception))

    def test_client_serves_records_loaded_from_bundle_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle_dir = Path(tmpdir) / "bundle"
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
                    "status": "active",
                    "source": "sample.market_state",
                    "run_id": "bundle_test_001",
                }
            ]
            venue_rows = [
                self._record(
                    "place_order",
                    key={"client_order_id": "cid-1"},
                    request={
                        "token_id": "tok_yes",
                        "side": "BUY",
                        "price": 0.55,
                        "size": 1.836364,
                        "client_order_id": "cid-1",
                        "time_in_force": "GTC",
                        "post_only": False,
                    },
                    response={"order_id": "venue-1", "status": "live", "client_order_id": "cid-1"},
                    seq=1,
                ),
                self._record(
                    "get_order",
                    key={"venue_order_id": "venue-1"},
                    request={"venue_order_id": "venue-1"},
                    response={"order_id": "venue-1", "status": "live", "client_order_id": "cid-1"},
                    seq=2,
                ),
                self._record(
                    "list_open_orders",
                    key={"market_id": "m1"},
                    request={"market_id": "m1"},
                    response=[{"order_id": "venue-1", "status": "live", "client_order_id": "cid-1", "market_id": "m1"}],
                    seq=3,
                ),
                self._record(
                    "cancel_order",
                    key={"venue_order_id": "venue-1"},
                    request={"venue_order_id": "venue-1"},
                    response={"order_id": "venue-1", "success": True, "status": "canceled"},
                    seq=4,
                ),
            ]
            manifest = {
                "run_id": "bundle_test_001",
                "strategy_id": "short_horizon_15m_touch_v1",
                "config_hash": "test-config",
                "files": {
                    "events_log": {"path": "events_log.jsonl", "count": len(event_rows)},
                    "market_state_snapshots": {"path": "market_state_snapshots.jsonl", "count": len(event_rows)},
                    "venue_responses": {"path": "venue_responses.jsonl", "count": len(venue_rows)},
                    "orders_final": {"path": "orders_final.jsonl", "count": 0},
                    "fills_final": {"path": "fills_final.jsonl", "count": 0},
                },
            }

            (bundle_dir / "events_log.jsonl").write_text("\n".join(json.dumps(row) for row in event_rows) + "\n", encoding="utf-8")
            (bundle_dir / "market_state_snapshots.jsonl").write_text("\n".join(json.dumps(row) for row in event_rows) + "\n", encoding="utf-8")
            (bundle_dir / "venue_responses.jsonl").write_text("\n".join(json.dumps(row) for row in venue_rows) + "\n", encoding="utf-8")
            (bundle_dir / "orders_final.jsonl").write_text("", encoding="utf-8")
            (bundle_dir / "fills_final.jsonl").write_text("", encoding="utf-8")
            (bundle_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            bundle = load_replay_bundle(bundle_dir)
            client = CapturedResponseExecutionClient(bundle.venue_responses)

            place_result = client.place_order(self._place_request())
            order_state = client.get_order("venue-1")
            open_orders = client.list_open_orders("m1")
            cancel_result = client.cancel_order("venue-1")

            self.assertEqual(place_result.order_id, "venue-1")
            self.assertEqual(order_state.client_order_id, "cid-1")
            self.assertEqual(len(open_orders), 1)
            self.assertEqual(open_orders[0].order_id, "venue-1")
            self.assertEqual(cancel_result.status, "canceled")


if __name__ == "__main__":
    unittest.main()
