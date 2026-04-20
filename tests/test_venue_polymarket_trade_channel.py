from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.core import AggressorSide, EventType
from short_horizon.venue_polymarket import TradeNormalizer


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


class VenuePolymarketTradeChannelTest(unittest.TestCase):
    def _fixture_frames(self) -> list[dict]:
        fixture_path = FIXTURES_DIR / "clob_ws_sample_trades.jsonl"
        return [json.loads(line) for line in fixture_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def _replay_fixture(self):
        normalizer = TradeNormalizer()
        ticks = []
        for frame in self._fixture_frames():
            ticks.extend(normalizer.normalize_event(frame, ingest_time_ms=int(frame.get("timestamp", 0)) + 25))
        return ticks

    def test_trade_normalizer_emits_expected_trade_tick_sequence(self) -> None:
        ticks = self._replay_fixture()

        self.assertEqual(len(ticks), 2)
        self.assertTrue(all(tick.event_type == EventType.TRADE_TICK for tick in ticks))
        self.assertEqual([tick.market_id for tick in ticks], ["m1", "m1"])
        self.assertEqual([tick.token_id for tick in ticks], ["tok_yes", "tok_no"])
        self.assertEqual([tick.price for tick in ticks], [0.456, 0.544])
        self.assertEqual([tick.size for tick in ticks], [219.217767, 10.5])
        self.assertEqual([tick.aggressor_side for tick in ticks], [AggressorSide.BUY, AggressorSide.SELL])
        self.assertEqual([tick.trade_id for tick in ticks], ["tr_001", None])
        self.assertEqual([tick.venue_seq for tick in ticks], [101, None])
        self.assertEqual([tick.event_time_ms for tick in ticks], [1750428146322, 1750428147000])
        self.assertEqual([tick.ingest_time_ms for tick in ticks], [1750428146347, 1750428147025])
        self.assertTrue(all(tick.source == "polymarket_clob_ws" for tick in ticks))

    def test_trade_normalizer_is_deterministic_on_replay(self) -> None:
        first = self._serialize_ticks(self._replay_fixture())
        second = self._serialize_ticks(self._replay_fixture())
        self.assertEqual(first, second)

    @staticmethod
    def _serialize_ticks(ticks) -> str:
        payload = []
        for tick in ticks:
            payload.append(
                {
                    "event_time_ms": tick.event_time_ms,
                    "ingest_time_ms": tick.ingest_time_ms,
                    "market_id": tick.market_id,
                    "token_id": tick.token_id,
                    "price": tick.price,
                    "size": tick.size,
                    "source": tick.source,
                    "trade_id": tick.trade_id,
                    "aggressor_side": tick.aggressor_side.value if tick.aggressor_side is not None else None,
                    "venue_seq": tick.venue_seq,
                }
            )
        return json.dumps(payload, sort_keys=True)


if __name__ == "__main__":
    unittest.main()
