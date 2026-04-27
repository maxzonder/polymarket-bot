from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from _order_flow_features import build_order_flow_features, load_bundle_events, parse_ts_ms


class OrderFlowFeaturesTest(unittest.TestCase):
    def test_parse_ts_ms_accepts_iso_ms_and_seconds(self) -> None:
        self.assertEqual(parse_ts_ms("2026-04-26T00:00:10Z"), 1777161610000)
        self.assertEqual(parse_ts_ms(1777161610000), 1777161610000)
        self.assertEqual(parse_ts_ms(1777161610), 1777161610000)

    def test_builds_trade_and_bbo_features_from_capture_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            touch_db = root / "touch.sqlite3"
            bundle = root / "bundle_a"
            bundle.mkdir()
            events = bundle / "events_log.jsonl"
            _write_touch_db(touch_db)
            _write_events(events)

            features = build_order_flow_features(
                touch_db,
                str(root / "*" / "events_log.jsonl"),
                pre_windows_ms=(1_000, 5_000),
                post_windows_ms=(1_000, 5_000),
                asset_allowlist={"btc"},
            )

            self.assertEqual(len(features), 1)
            feature = features[0]
            self.assertEqual(feature.bundle_name, "bundle_a")
            self.assertEqual(feature.probe_id, "p1")
            self.assertEqual(feature.pre_book_n, 3)
            self.assertEqual(feature.post_book_n, 3)
            self.assertEqual(feature.pre_trade_n, 3)
            self.assertEqual(feature.post_trade_n, 0)
            self.assertTrue(feature.has_l2_levels)
            self.assertAlmostEqual(feature.best_ask_pre or 0.0, 0.65)
            self.assertAlmostEqual(feature.best_ask_at_or_after_touch or 0.0, 0.65)
            self.assertEqual(feature.last_pre_book_delta_ms, 0)
            self.assertEqual(feature.first_post_book_delta_ms, 0)

            trade_5s = feature.trade_windows[5_000]
            self.assertEqual(trade_5s.buy_trade_count, 2)
            self.assertEqual(trade_5s.sell_trade_count, 1)
            self.assertEqual(trade_5s.unknown_trade_count, 0)
            self.assertAlmostEqual(trade_5s.buy_size, 12.0)
            self.assertAlmostEqual(trade_5s.sell_size, 3.0)
            self.assertAlmostEqual(trade_5s.signed_size, 9.0)
            self.assertAlmostEqual(trade_5s.buy_share or 0.0, 0.8)
            self.assertEqual(trade_5s.last_trade_side, "buy")
            self.assertEqual(trade_5s.last_trade_delta_ms, -100)

            book_5s = feature.book_windows[5_000]
            self.assertEqual(book_5s.book_count, 3)
            self.assertAlmostEqual(book_5s.ask_delta or 0.0, 0.03)
            self.assertAlmostEqual(book_5s.bid_delta or 0.0, 0.03)
            self.assertAlmostEqual(book_5s.mid_delta or 0.0, 0.03)

            self.assertFalse(feature.post_windows[1_000].ask_reverts_below_level)
            self.assertTrue(feature.post_windows[1_000].ask_stays_ge_level)
            self.assertTrue(feature.post_windows[5_000].ask_reverts_below_level)
            self.assertFalse(feature.post_windows[5_000].ask_stays_ge_level)

    def test_unknown_trade_side_does_not_pollute_buy_share(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            touch_db = root / "touch.sqlite3"
            bundle = root / "bundle_a"
            bundle.mkdir()
            events = bundle / "events_log.jsonl"
            _write_touch_db(touch_db)
            with events.open("w", encoding="utf-8") as fh:
                fh.write(json.dumps({"event_type": "TradeTick", "event_time": "2026-04-26T00:00:09Z", "market_id": "m1", "token_id": "tok", "price": 0.64, "size": 9.0, "aggressor_side": None}) + "\n")
                fh.write(json.dumps({"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:10Z", "market_id": "m1", "token_id": "tok", "best_bid": 0.64, "best_ask": 0.65}) + "\n")

            feature = build_order_flow_features(touch_db, str(root / "*" / "events_log.jsonl"))[0]
            stats = feature.trade_windows[5_000]
            self.assertEqual(stats.unknown_trade_count, 1)
            self.assertEqual(stats.unknown_size, 9.0)
            self.assertIsNone(stats.buy_share)

    def test_load_bundle_events_tolerates_missing_depth(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            events = Path(tmp) / "events_log.jsonl"
            with events.open("w", encoding="utf-8") as fh:
                fh.write(json.dumps({"event_type": "BookUpdate", "event_time_ms": 1777161610000, "market_id": "m1", "token_id": "tok", "best_bid": 0.64, "best_ask": 0.65}) + "\n")
            bundle = load_bundle_events(events)
            self.assertFalse(bundle.has_l2_book_levels)
            self.assertIn("tok", bundle.books_by_token)


def _write_touch_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE touch_dataset (
            probe_id TEXT,
            market_id TEXT,
            token_id TEXT,
            asset_slug TEXT,
            direction TEXT,
            touch_level REAL,
            touch_time_iso TEXT,
            lifecycle_fraction REAL,
            resolves_yes INTEGER,
            fee_rate_bps REAL,
            best_ask_at_touch REAL,
            best_bid_at_touch REAL,
            tick_size REAL
        )
        """
    )
    conn.execute(
        "INSERT INTO touch_dataset VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("p1", "m1", "tok", "btc", "DOWN/NO", 0.65, "2026-04-26T00:00:10Z", 0.7, 1, 720.0, 0.65, 0.64, 0.01),
    )
    conn.commit()
    conn.close()


def _write_events(path: Path) -> None:
    events = [
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:05Z", "market_id": "m1", "token_id": "tok", "best_bid": 0.61, "best_ask": 0.62},
        {"event_type": "TradeTick", "event_time": "2026-04-26T00:00:06Z", "market_id": "m1", "token_id": "tok", "price": 0.62, "size": 5.0, "aggressor_side": "buy"},
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:08Z", "market_id": "m1", "token_id": "tok", "best_bid": 0.63, "best_ask": 0.64, "bid_levels": [{"price": 0.63, "size": 10.0}], "ask_levels": [{"price": 0.64, "size": 8.0}]},
        {"event_type": "TradeTick", "event_time": "2026-04-26T00:00:09Z", "market_id": "m1", "token_id": "tok", "price": 0.64, "size": 3.0, "aggressor_side": "sell"},
        {"event_type": "TradeTick", "event_time": "2026-04-26T00:00:09.900Z", "market_id": "m1", "token_id": "tok", "price": 0.65, "size": 7.0, "aggressor_side": "buy"},
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:10Z", "market_id": "m1", "token_id": "tok", "best_bid": 0.64, "best_ask": 0.65},
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:10.500Z", "market_id": "m1", "token_id": "tok", "best_bid": 0.65, "best_ask": 0.66},
        {"event_type": "BookUpdate", "event_time": "2026-04-26T00:00:12Z", "market_id": "m1", "token_id": "tok", "best_bid": 0.61, "best_ask": 0.62},
    ]
    with path.open("w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")


if __name__ == "__main__":
    unittest.main()
