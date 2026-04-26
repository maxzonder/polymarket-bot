from __future__ import annotations

import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.replay_maker_queue_capture import (
    BookPoint,
    Candidate,
    ReplayConfig,
    TradePoint,
    load_a2_candidates,
    load_bundle_slice,
    order_shares_for_stake,
    replay_candidate,
    run_replay,
)


class ReplayMakerQueueCaptureTests(unittest.TestCase):
    def test_order_size_respects_min_shares(self) -> None:
        self.assertEqual(order_shares_for_stake(stake_usdc=1.0, entry_price=0.50), 5.0)
        self.assertEqual(order_shares_for_stake(stake_usdc=10.0, entry_price=0.50), 20.0)

    def test_post_only_cross_blocks_before_trade_fill(self) -> None:
        candidate = self._candidate()
        replay = replay_candidate(
            candidate,
            books=[BookPoint(candidate.touch_ms - 5_000, best_bid=0.54, best_ask=0.55)],
            trades=[TradePoint(candidate.touch_ms, price=0.55, size=100.0, aggressor_side="sell")],
            config=ReplayConfig(stake_usdc=1.0, queue_factor=1.0, post_before_ms=5_000, cancel_after_ms=10_000),
        )
        self.assertEqual(replay.status, "post_only_would_cross")
        self.assertEqual(replay.cost_usdc, 0.0)

    def test_sell_trade_volume_can_fill_when_post_only_safe(self) -> None:
        candidate = self._candidate(resolves_yes=1)
        replay = replay_candidate(
            candidate,
            books=[BookPoint(candidate.touch_ms - 5_000, best_bid=0.50, best_ask=0.56)],
            trades=[
                TradePoint(candidate.touch_ms - 1_000, price=0.55, size=2.0, aggressor_side="sell"),
                TradePoint(candidate.touch_ms + 1_000, price=0.55, size=3.0, aggressor_side="sell"),
            ],
            config=ReplayConfig(stake_usdc=1.0, queue_factor=1.0, post_before_ms=5_000, cancel_after_ms=10_000),
        )
        self.assertEqual(replay.status, "filled")
        self.assertAlmostEqual(replay.cost_usdc, 2.75)
        self.assertAlmostEqual(replay.pnl_usdc, 2.25)

    def test_queue_factor_requires_more_sell_volume(self) -> None:
        candidate = self._candidate(resolves_yes=1)
        replay = replay_candidate(
            candidate,
            books=[BookPoint(candidate.touch_ms - 5_000, best_bid=0.50, best_ask=0.56)],
            trades=[TradePoint(candidate.touch_ms, price=0.55, size=5.0, aggressor_side="sell")],
            config=ReplayConfig(stake_usdc=1.0, queue_factor=2.0, post_before_ms=5_000, cancel_after_ms=10_000),
        )
        self.assertEqual(replay.status, "unfilled")
        self.assertAlmostEqual(replay.observed_sell_volume, 5.0)

    def test_loads_candidates_and_bundle_slice(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            touch_db = root / "touch.sqlite3"
            capture_dir = root / "bundle"
            capture_dir.mkdir()
            events = capture_dir / "events_log.jsonl"
            self._write_touch_db(touch_db)
            with events.open("w", encoding="utf-8") as fh:
                fh.write(json.dumps({"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:04Z", "token_id": "tok", "best_bid": 0.50, "best_ask": 0.56}) + "\n")
                fh.write(json.dumps({"event_type": "TradeTick", "event_time": "2026-04-26T00:00:10Z", "token_id": "tok", "price": 0.55, "size": 5.0, "aggressor_side": "sell"}) + "\n")

            candidates = load_a2_candidates(touch_db)
            self.assertEqual(len(candidates), 1)
            loaded = load_bundle_slice(events, candidates, max_pre_ms=10_000)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(len(loaded.candidates), 1)
            self.assertFalse(loaded.has_l2_book_levels)

            slices, summaries = run_replay(touch_db=touch_db, capture_glob=str(root / "*" / "events_log.jsonl"))
            self.assertEqual(len(slices), 1)
            self.assertEqual(len(summaries), 48)
            self.assertTrue(any(summary.filled_n == 1 for summary in summaries))

    def _candidate(self, *, resolves_yes: int = 0) -> Candidate:
        return Candidate(
            probe_id="p1",
            market_id="m1",
            token_id="tok",
            touch_time_iso="2026-04-26T00:00:10Z",
            touch_ms=1_776_710_410_000,
            touch_level=0.55,
            resolves_yes=resolves_yes,
        )

    def _write_touch_db(self, path: Path) -> None:
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE touch_dataset (
                probe_id TEXT,
                market_id TEXT,
                token_id TEXT,
                touch_time_iso TEXT,
                asset_slug TEXT,
                direction TEXT,
                lifecycle_fraction REAL,
                touch_level REAL,
                resolves_yes INTEGER
            );
            INSERT INTO touch_dataset VALUES
                ('p1', 'm1', 'tok', '2026-04-26T00:00:10Z', 'btc', 'DOWN/NO', 0.7, 0.55, 1),
                ('p2', 'm2', 'tok2', '2026-04-26T00:00:10Z', 'btc', 'UP/YES', 0.7, 0.55, 1);
            """
        )
        conn.commit()
        conn.close()


if __name__ == "__main__":
    unittest.main()
