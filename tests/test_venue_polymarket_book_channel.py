from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.core import MarketStateUpdate
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

    def _mixed_batch_fixture(self):
        fixture_path = FIXTURES_DIR / "clob_ws_mixed_batch.json"
        return fixture_path.read_text(encoding="utf-8")

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

    def test_book_normalizer_accepts_batch_frame_and_ignores_irrelevant_events(self) -> None:
        normalizer = BookNormalizer()
        updates = normalizer.normalize_frame(self._mixed_batch_fixture(), ingest_time_ms=2000)

        self.assertEqual(len(updates), 2)
        self.assertEqual([update.event_time_ms for update in updates], [1000, 1020])
        self.assertEqual([update.best_ask for update in updates], [0.54, 0.54])
        self.assertEqual([update.is_snapshot for update in updates], [True, False])
        self.assertTrue(all(update.ingest_time_ms == 2000 for update in updates))

    def test_book_normalizer_maps_condition_id_market_field_back_to_numeric_market_id(self) -> None:
        normalizer = BookNormalizer()
        normalizer.register_market(
            MarketStateUpdate(
                event_time_ms=1000,
                ingest_time_ms=1000,
                market_id="2023024",
                condition_id="0x0c108ff3",
                token_yes_id="tok_yes",
                token_no_id="tok_no",
                token_id="tok_yes",
            )
        )

        updates = normalizer.normalize_event(
            {
                "event_type": "best_bid_ask",
                "market": "0x0c108ff3",
                "asset_id": "tok_yes",
                "best_bid": "0.54",
                "best_ask": "0.55",
                "timestamp": 1020,
            },
            ingest_time_ms=1100,
        )

        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0].market_id, "2023024")
        self.assertEqual(updates[0].token_id, "tok_yes")

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
