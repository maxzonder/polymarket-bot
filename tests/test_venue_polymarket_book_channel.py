from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.venue_polymarket import BookNormalizer


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


class VenuePolymarketBookChannelTest(unittest.TestCase):
    def _fixture_frames(self) -> list[dict]:
        fixture_path = FIXTURES_DIR / "clob_ws_sample_book.jsonl"
        return [json.loads(line) for line in fixture_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def _replay_fixture(self) -> list:
        normalizer = BookNormalizer()
        updates = []
        for frame in self._fixture_frames():
            updates.extend(normalizer.normalize_event(frame, ingest_time_ms=int(frame.get("timestamp", 0)) + 100))
        return updates

    def test_book_normalizer_emits_expected_best_ask_progression(self) -> None:
        updates = self._replay_fixture()

        self.assertEqual(len(updates), 4)
        self.assertEqual([update.best_ask for update in updates], [0.54, 0.55, 0.55, 0.56])
        self.assertEqual([update.best_bid for update in updates], [0.53, 0.53, 0.54, 0.54])
        self.assertEqual([update.is_snapshot for update in updates], [True, False, False, False])
        self.assertTrue(all(update.source == "polymarket_clob_ws" for update in updates))
        self.assertEqual([update.event_time_ms for update in updates], [1000, 1010, 1020, 1030])
        self.assertEqual([update.ingest_time_ms for update in updates], [1100, 1110, 1120, 1130])

    def test_book_normalizer_is_deterministic_on_replay(self) -> None:
        first = self._serialize_updates(self._replay_fixture())
        second = self._serialize_updates(self._replay_fixture())
        self.assertEqual(first, second)

    def test_depth_snapshot_regression_matches_collector_style_touch_capture(self) -> None:
        updates = self._replay_fixture()
        snapshot = self._depth_snapshot_at_touch(updates, level=0.55)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["best_ask"], 0.55)
        self.assertEqual(snapshot["ask_levels"], [(0.54, 12.0), (0.55, 15.0), (0.56, 8.0)])
        self.assertEqual(snapshot["ask_size_at_level"], 15.0)

    def test_out_of_order_top_of_book_frame_is_dropped(self) -> None:
        updates = self._replay_fixture()
        self.assertEqual(updates[-1].event_time_ms, 1030)
        self.assertEqual(updates[-1].best_ask, 0.56)

    @staticmethod
    def _serialize_updates(updates: list) -> str:
        payload = []
        for update in updates:
            payload.append(
                {
                    "event_time_ms": update.event_time_ms,
                    "ingest_time_ms": update.ingest_time_ms,
                    "market_id": update.market_id,
                    "token_id": update.token_id,
                    "best_bid": update.best_bid,
                    "best_ask": update.best_ask,
                    "spread": update.spread,
                    "mid_price": update.mid_price,
                    "bid_levels": [{"price": level.price, "size": level.size} for level in update.bid_levels],
                    "ask_levels": [{"price": level.price, "size": level.size} for level in update.ask_levels],
                    "is_snapshot": update.is_snapshot,
                    "source": update.source,
                }
            )
        return json.dumps(payload, sort_keys=True)

    @staticmethod
    def _depth_snapshot_at_touch(updates: list, *, level: float) -> dict | None:
        previous_best_ask = None
        for update in updates:
            current_best_ask = update.best_ask
            if previous_best_ask is not None and current_best_ask is not None and previous_best_ask < level <= current_best_ask:
                ask_levels = [(book_level.price, book_level.size) for book_level in update.ask_levels]
                ask_size_at_level = sum(size for price, size in ask_levels if abs(price - level) < 1e-9)
                return {
                    "best_ask": current_best_ask,
                    "ask_levels": ask_levels,
                    "ask_size_at_level": ask_size_at_level,
                }
            previous_best_ask = current_best_ask
        return None


if __name__ == "__main__":
    unittest.main()
