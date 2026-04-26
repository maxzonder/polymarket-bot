from __future__ import annotations

import json
import sys
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.core.events import FeeInfo, MarketStateUpdate, MarketStatus, effective_fee_rate_bps
from short_horizon.replay import _parse_fee_info, parse_event_record
from short_horizon.storage.runtime import RunContext, SQLiteRuntimeStore
from short_horizon.venue_polymarket.markets import _extract_fee_info
from short_horizon.venue_polymarket.v2_fees import fetch_fee_info


class EffectiveFeeRateBpsTest(unittest.TestCase):
    def test_prefers_fee_info_when_present(self) -> None:
        info = FeeInfo(base_fee_bps=42, rate=0.0042, exponent=1.0)
        self.assertEqual(effective_fee_rate_bps(fee_info=info, fee_rate_bps=99.0), 42.0)

    def test_falls_back_to_fee_rate_bps_when_fee_info_missing(self) -> None:
        self.assertEqual(effective_fee_rate_bps(fee_info=None, fee_rate_bps=17.5), 17.5)

    def test_returns_none_when_both_missing(self) -> None:
        self.assertIsNone(effective_fee_rate_bps(fee_info=None, fee_rate_bps=None))


class ReplayFeeInfoRoundTripTest(unittest.TestCase):
    def test_v1_era_payload_without_fee_info_parses(self) -> None:
        payload = {
            "event_type": "MarketStateUpdate",
            "event_time_ms": 1000,
            "ingest_time_ms": 1010,
            "market_id": "m1",
            "fee_rate_bps": 25.0,
        }
        event = parse_event_record(payload)
        assert isinstance(event, MarketStateUpdate)
        self.assertEqual(event.fee_rate_bps, 25.0)
        self.assertIsNone(event.fee_info)

    def test_v2_payload_with_fee_info_round_trips(self) -> None:
        info = FeeInfo(base_fee_bps=20, rate=0.002, exponent=1.5, source="v2.clob_market_info")
        payload = {
            "event_type": "MarketStateUpdate",
            "event_time_ms": 1000,
            "ingest_time_ms": 1010,
            "market_id": "m1",
            "fee_rate_bps": 25.0,
            "fee_info": asdict(info),
        }
        event = parse_event_record(payload)
        assert isinstance(event, MarketStateUpdate)
        self.assertEqual(event.fee_info, info)

    def test_parse_fee_info_returns_none_for_garbage(self) -> None:
        self.assertIsNone(_parse_fee_info(None))
        self.assertIsNone(_parse_fee_info("not-a-dict"))
        self.assertIsNone(_parse_fee_info({}))  # missing base_fee_bps

    def test_parse_fee_info_uses_default_source(self) -> None:
        info = _parse_fee_info({"base_fee_bps": 10})
        assert info is not None
        self.assertEqual(info.base_fee_bps, 10)
        self.assertEqual(info.source, "v2.clob_market_info")


class GammaFeeInfoExtractTest(unittest.TestCase):
    def test_extracts_fd_when_present(self) -> None:
        info = _extract_fee_info({"fd": {"r": 0.003, "e": 1.0}}, fee_rate_bps=30.0)
        assert info is not None
        self.assertEqual(info.base_fee_bps, 30)
        self.assertAlmostEqual(info.rate, 0.003)
        self.assertAlmostEqual(info.exponent, 1.0)
        self.assertEqual(info.source, "gamma.fd")

    def test_returns_none_when_fd_missing(self) -> None:
        self.assertIsNone(_extract_fee_info({"feesEnabled": True}, fee_rate_bps=30.0))

    def test_handles_missing_fee_rate_bps(self) -> None:
        info = _extract_fee_info({"fd": {"r": 0.001, "e": 0.5}}, fee_rate_bps=None)
        assert info is not None
        self.assertEqual(info.base_fee_bps, 0)


class StorageFeeInfoTest(unittest.TestCase):
    def test_upsert_market_state_persists_fee_info_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteRuntimeStore(
                Path(tmpdir) / "runtime.sqlite3",
                run=RunContext(run_id="r1", strategy_id="s1", config_hash="c1"),
            )
            try:
                fee_info = FeeInfo(base_fee_bps=15, rate=0.0015, exponent=1.0, source="v2.clob_market_info")
                event = MarketStateUpdate(
                    event_time_ms=1_000_000,
                    ingest_time_ms=1_000_010,
                    market_id="m_v2",
                    condition_id="cond1",
                    question="q?",
                    status=MarketStatus.ACTIVE,
                    start_time_ms=0,
                    end_time_ms=900_000,
                    is_active=True,
                    fee_rate_bps=15.0,
                    fee_info=fee_info,
                )
                store.upsert_market_state(event)
                row = store.conn.execute(
                    "SELECT fee_info_json FROM markets WHERE market_id = ?", ("m_v2",)
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(json.loads(row[0]), asdict(fee_info))
            finally:
                store.close()

    def test_upsert_market_state_writes_null_when_fee_info_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteRuntimeStore(
                Path(tmpdir) / "runtime.sqlite3",
                run=RunContext(run_id="r1", strategy_id="s1", config_hash="c1"),
            )
            try:
                event = MarketStateUpdate(
                    event_time_ms=1_000_000,
                    ingest_time_ms=1_000_010,
                    market_id="m_v1",
                    condition_id="cond1",
                    question="q?",
                    status=MarketStatus.ACTIVE,
                    start_time_ms=0,
                    end_time_ms=900_000,
                    is_active=True,
                    fee_rate_bps=20.0,
                )
                store.upsert_market_state(event)
                row = store.conn.execute(
                    "SELECT fee_info_json FROM markets WHERE market_id = ?", ("m_v1",)
                ).fetchone()
                self.assertIsNone(row[0])
            finally:
                store.close()


class V2FetchFeeInfoTest(unittest.TestCase):
    def test_fetch_pairs_each_token_with_fd_and_per_token_base_fee(self) -> None:
        class _FakeClient:
            def __init__(self) -> None:
                self.calls = []

            def get_clob_market_info(self, condition_id: str) -> dict:
                self.calls.append(("get_clob_market_info", condition_id))
                return {"t": [{"t": "tok_yes"}, {"t": "tok_no"}], "fd": {"r": 0.002, "e": 1.5}}

            def get_fee_rate_bps(self, token_id: str) -> int:
                self.calls.append(("get_fee_rate_bps", token_id))
                return 25

        client = _FakeClient()
        result = fetch_fee_info(client, "cond1")
        self.assertEqual(set(result.keys()), {"tok_yes", "tok_no"})
        self.assertEqual(result["tok_yes"].base_fee_bps, 25)
        self.assertAlmostEqual(result["tok_yes"].rate, 0.002)
        self.assertAlmostEqual(result["tok_yes"].exponent, 1.5)
        self.assertEqual(result["tok_yes"].source, "v2.clob_market_info")

    def test_fetch_returns_empty_when_payload_missing_tokens(self) -> None:
        class _FakeClient:
            def get_clob_market_info(self, condition_id: str) -> dict:
                return {"t": [], "fd": {}}

            def get_fee_rate_bps(self, token_id: str) -> int:
                raise AssertionError("should not be called")

        self.assertEqual(fetch_fee_info(_FakeClient(), "cond"), {})

    def test_fetch_swallows_per_token_fee_lookup_failures(self) -> None:
        class _FakeClient:
            def get_clob_market_info(self, condition_id: str) -> dict:
                return {"t": [{"t": "tok_yes"}], "fd": {"r": 0.001, "e": 1.0}}

            def get_fee_rate_bps(self, token_id: str) -> int:
                raise RuntimeError("rpc broken")

        result = fetch_fee_info(_FakeClient(), "cond")
        self.assertEqual(result["tok_yes"].base_fee_bps, 0)


if __name__ == "__main__":
    unittest.main()
