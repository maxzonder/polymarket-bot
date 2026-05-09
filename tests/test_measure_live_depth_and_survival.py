from __future__ import annotations

import argparse
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

sys.modules.setdefault("websockets", types.SimpleNamespace(connect=None))

from measure_live_depth_and_survival import (  # noqa: E402
    LEVEL_PRESETS,
    BookState,
    MarketToken,
    SqliteSink,
    SurvivalProbe,
    PreTouchFeatures,
    SpotObservation,
    build_book_snapshot_row,
    build_touch_row,
    compute_book_metrics,
    evaluate_share_entry_details,
    monotonic_ms,
    spot_features_from_history,
)


class MeasureLiveDepthAndSurvivalTest(unittest.TestCase):
    def test_level_presets_include_wide_and_low_tail(self) -> None:
        self.assertIn(0.25, LEVEL_PRESETS["crypto_wide"])
        self.assertIn(0.90, LEVEL_PRESETS["crypto_wide"])
        self.assertIn(0.005, LEVEL_PRESETS["black_swan_low_tail"])
        self.assertIn(0.05, LEVEL_PRESETS["black_swan_low_tail"])

    def test_book_metrics_use_bid_and_ask_depth(self) -> None:
        metrics = compute_book_metrics(
            bid_levels=[(0.49, 30.0), (0.48, 20.0), (0.47, 10.0)],
            ask_levels=[(0.51, 10.0), (0.52, 10.0), (0.53, 20.0)],
        )

        self.assertAlmostEqual(metrics["spread_at_touch"], 0.02)
        self.assertAlmostEqual(metrics["mid_price_at_touch"], 0.50)
        self.assertAlmostEqual(metrics["imbalance_top1"], 30.0 / 40.0)
        self.assertAlmostEqual(metrics["imbalance_top3"], 60.0 / 100.0)
        self.assertAlmostEqual(metrics["microprice_at_touch"], (0.51 * 30.0 + 0.49 * 10.0) / 40.0)

    def test_min_share_entry_details_are_fee_aware(self) -> None:
        details = evaluate_share_entry_details(
            [(0.50, 2.0), (0.51, 10.0)],
            5.0,
            fee_rate_bps=100.0,
        )

        self.assertEqual(details["fit"], "+1_tick")
        self.assertAlmostEqual(details["entry_price"], 0.50)
        self.assertAlmostEqual(details["cost"], 0.50 * 2.0 + 0.51 * 3.0)
        self.assertAlmostEqual(details["estimated_fee"], (0.50 * 2.0 + 0.51 * 3.0) * 0.01)
        self.assertAlmostEqual(details["required_hit_rate"], ((0.50 * 2.0 + 0.51 * 3.0) * 1.01) / 5.0)

    def test_book_snapshot_row_sets_periodic_quality_flags(self) -> None:
        book = BookState(
            token_id="tok1",
            market_id="m1",
            best_bid=0.60,
            best_ask=0.55,
            bid_levels=[(0.60, 1.0)],
            ask_levels=[],
            last_event_ts_ms=1_775_000_000_000,
            last_ingest_monotonic_ms=monotonic_ms() - 10_000,
            last_book_hash="h1",
        )

        row = build_book_snapshot_row(
            book,
            run_id="run1",
            update_kind="periodic",
            stale_book_ms=1_000,
            wide_spread_threshold=0.01,
        )

        self.assertEqual(row["snapshot_id"], "run1:tok1:periodic:1775000000000")
        self.assertEqual(row["update_kind"], "periodic")
        self.assertEqual(row["missing_depth_flag"], 1)
        self.assertEqual(row["crossed_book_flag"], 1)
        self.assertEqual(row["book_stale_flag"], 1)
        self.assertEqual(row["book_hash"], "h1")

    def test_sqlite_sink_can_write_periodic_book_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "collector.sqlite3"
            args = argparse.Namespace(
                level_preset="crypto_wide",
                levels=(0.25, 0.30),
                depth_levels=5,
                output_csv="touches.csv",
                book_snapshot_interval_ms=1000,
                stale_book_ms=5000,
                wide_spread_threshold=0.10,
            )
            sink = SqliteSink(path, run_id="run1", args=args)
            try:
                sink.write_book_snapshot(
                    BookState(
                        token_id="tok1",
                        market_id="m1",
                        best_bid=0.49,
                        best_ask=0.51,
                        bid_levels=[(0.49, 10.0)],
                        ask_levels=[(0.51, 5.0)],
                        last_event_ts_ms=1_775_000_000_000,
                        last_ingest_monotonic_ms=monotonic_ms(),
                    ),
                    update_kind="periodic",
                )
            finally:
                sink.close()

            conn = sqlite3.connect(path)
            try:
                run = conn.execute(
                    "SELECT book_snapshot_interval_ms, stale_book_ms, wide_spread_threshold FROM collection_runs WHERE run_id='run1'"
                ).fetchone()
                snapshot = conn.execute(
                    "SELECT update_kind, best_bid, best_ask, missing_depth_flag FROM book_snapshots WHERE token_id='tok1'"
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(run[0], 1000)
            self.assertEqual(run[1], 5000)
            self.assertAlmostEqual(run[2], 0.10)
            self.assertEqual(snapshot[0], "periodic")
            self.assertAlmostEqual(snapshot[1], 0.49)
            self.assertAlmostEqual(snapshot[2], 0.51)
            self.assertEqual(snapshot[3], 0)

    def test_spot_features_and_sqlite_spot_snapshot(self) -> None:
        history = __import__("collections").deque(
            [
                SpotObservation("btc", 1_000, 1_005, "fixture", 100.0, 99.9, 100.1),
                SpotObservation("btc", 31_000, 31_010, "fixture", 101.0, 100.9, 101.1),
            ]
        )
        features = spot_features_from_history(history, history[-1], 31_000)
        self.assertAlmostEqual(features["spot_price_at_touch"], 101.0)
        self.assertAlmostEqual(features["spot_return_30s"], 0.01)
        self.assertEqual(features["spot_source_at_touch"], "fixture")
        self.assertEqual(features["spot_latency_ms_at_touch"], 10)

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "collector.sqlite3"
            args = argparse.Namespace(
                level_preset="crypto_wide",
                levels=(0.25, 0.30),
                depth_levels=5,
                output_csv="touches.csv",
                book_snapshot_interval_ms=0,
                stale_book_ms=5000,
                wide_spread_threshold=0.10,
            )
            sink = SqliteSink(path, run_id="run1", args=args)
            try:
                sink.write_spot_snapshot(history[-1])
            finally:
                sink.close()
            conn = sqlite3.connect(path)
            try:
                row = conn.execute(
                    "SELECT asset_slug, source, spot_price, latency_ms FROM spot_snapshots WHERE asset_slug='btc'"
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(row[0], "btc")
            self.assertEqual(row[1], "fixture")
            self.assertAlmostEqual(row[2], 101.0)
            self.assertEqual(row[3], 10)

    def test_touch_row_and_sqlite_sink_include_schema_v2_fields(self) -> None:
        token = MarketToken(
            market_id="m1",
            condition_id="c1",
            question="Bitcoin Up or Down",
            token_id="tok1",
            outcome="Up",
            end_time_iso="2026-04-20T12:15:00Z",
            start_time_iso="2026-04-20T12:00:00Z",
            duration_seconds=900,
            seconds_to_end=600,
            recurrence="15m",
            series_slug="btc-15m",
            category="crypto",
            event_slug="btc-up-down",
            asset_slug="btc",
            fees_enabled=True,
            fee_rate_bps=100.0,
            tick_size=0.01,
        )
        probe = SurvivalProbe(
            probe_id="p1",
            token=token,
            level=0.55,
            started_at_ms=1_775_000_000_000,
            initial_best_ask=0.55,
            initial_ask_size_at_level=7.0,
            ask_levels_at_touch=[(0.55, 7.0), (0.56, 10.0)],
            bid_levels_at_touch=[(0.54, 11.0), (0.53, 9.0)],
            notional_fit={"10": "+1_tick", "50": "insufficient_depth", "100": "insufficient_depth"},
            fit_details={
                "10": {"fit": "+1_tick", "avg_entry_price": 0.555},
                "50": {"fit": "insufficient_depth"},
                "100": {"fit": "insufficient_depth"},
            },
            min_share_details={
                "fit": "+0_tick",
                "entry_price": 0.55,
                "avg_entry_price": 0.55,
                "cost": 2.75,
                "worst_tick": 0,
                "estimated_fee": 0.0275,
                "required_hit_rate": 0.5555,
            },
            pre_touch_features=PreTouchFeatures(
                best_bid_1s_before=0.53,
                best_ask_1s_before=0.54,
                best_bid_delta_1s=0.01,
                best_ask_delta_1s=0.01,
            ),
            max_favorable_move=0.02,
            max_adverse_move=-0.01,
            held_at_or_above_level=False,
            immediate_reversal_flag=True,
            spot_features={
                "spot_price_at_touch": 101.0,
                "spot_source_at_touch": "fixture.spot",
                "spot_latency_ms_at_touch": 10,
                "spot_bid_at_touch": 100.9,
                "spot_ask_at_touch": 101.1,
                "spot_return_30s": 0.01,
                "spot_return_1m": None,
                "spot_return_3m": None,
                "spot_return_5m": None,
            },
            done=True,
            end_reason="window_complete_level_still_present",
            survived_ms=2000,
        )
        book = BookState(token_id="tok1", best_bid=0.54, best_ask=0.55)

        row = build_touch_row(probe, book, run_id="run1")

        self.assertEqual(row["schema_version"], 2)
        self.assertEqual(row["run_id"], "run1")
        self.assertEqual(row["bid_level_1_price"], 0.54)
        self.assertEqual(row["fit_5_shares"], "+0_tick")
        self.assertAlmostEqual(row["spread_at_touch"], 0.01)
        self.assertEqual(row["missing_depth_flag_at_touch"], 0)
        self.assertAlmostEqual(row["best_ask_delta_1s"], 0.01)
        self.assertAlmostEqual(row["max_favorable_move"], 0.02)
        self.assertEqual(row["held_at_or_above_level"], 0)
        self.assertEqual(row["immediate_reversal_flag"], 1)
        self.assertEqual(row["asset_slug"], "btc")
        self.assertAlmostEqual(row["spot_price_at_touch"], 101.0)
        self.assertAlmostEqual(row["spot_return_30s"], 0.01)

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "collector.sqlite3"
            args = argparse.Namespace(
                level_preset="crypto_wide",
                levels=(0.25, 0.30),
                depth_levels=5,
                output_csv="touches.csv",
                book_snapshot_interval_ms=0,
                stale_book_ms=5000,
                wide_spread_threshold=0.10,
            )
            sink = SqliteSink(path, run_id="run1", args=args)
            try:
                sink.write_probe(probe, book, row)
            finally:
                sink.close()

            conn = sqlite3.connect(path)
            try:
                stored = conn.execute(
                    "SELECT schema_version, run_id, bid_levels_json, spread_at_touch, best_ask_delta_1s, immediate_reversal_flag, spot_price_at_touch FROM touch_events WHERE probe_id='p1'"
                ).fetchone()
                snapshot = conn.execute(
                    "SELECT run_id, update_kind, bid_levels_json, ask_levels_json FROM book_snapshots WHERE probe_id='p1'"
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(stored[0], 2)
            self.assertEqual(stored[1], "run1")
            self.assertIn("0.54", stored[2])
            self.assertAlmostEqual(stored[3], 0.01)
            self.assertAlmostEqual(stored[4], 0.01)
            self.assertEqual(stored[5], 1)
            self.assertAlmostEqual(stored[6], 101.0)
            self.assertEqual(snapshot[0], "run1")
            self.assertEqual(snapshot[1], "touch")
            self.assertIn("0.54", snapshot[2])
            self.assertIn("0.55", snapshot[3])


if __name__ == "__main__":
    unittest.main()
