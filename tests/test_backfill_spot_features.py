from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from backfill_spot_features import SpotBar, build_spot_features


class BackfillSpotFeaturesTest(unittest.TestCase):
    def test_builds_expected_spot_features_from_synthetic_bars(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            touch_db = tmp_path / "touch_dataset.sqlite3"
            out_db = tmp_path / "spot_features.sqlite3"
            cache_db = tmp_path / "spot_cache.sqlite3"
            _write_touch_dataset(touch_db)

            def fetch(symbol: str, start_ms: int, end_ms: int) -> list[SpotBar]:
                bars: list[SpotBar] = []
                for idx, ts in enumerate(range(start_ms, end_ms + 1, 1000)):
                    price = 100.0 + ((ts - 1_800_000) / 1000.0) * 0.1
                    bars.append(
                        SpotBar(
                            asset_slug="",
                            symbol=symbol,
                            open_time_ms=ts,
                            open=price,
                            high=price + 0.01,
                            low=price - 0.01,
                            close=price,
                            volume=1.0 + idx,
                        )
                    )
                return bars

            summary = build_spot_features(
                touch_db,
                output_path=out_db,
                cache_path=cache_db,
                fetch_klines=fetch,
                sleep_seconds=0,
                asset_symbols={"btc": "BTCUSDT"},
            )

            self.assertEqual(summary.input_rows, 1)
            self.assertEqual(summary.output_rows, 1)
            self.assertEqual(summary.coverage, 1.0)
            conn = sqlite3.connect(out_db)
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM spot_features WHERE probe_id='p1'").fetchone()
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row["spot_symbol"], "BTCUSDT")
            self.assertAlmostEqual(row["nearest_spot_latency_ms"], 0.0)
            self.assertAlmostEqual(row["spot_at_window_start"], 100.0)
            self.assertAlmostEqual(row["spot_at_touch"], 103.0)
            self.assertAlmostEqual(row["spot_return_since_window_start"], 0.03)
            self.assertAlmostEqual(row["spot_velocity_recent_30s"], 0.1)
            self.assertGreaterEqual(row["spot_implied_prob"], 0.0)
            self.assertLessEqual(row["spot_implied_prob"], 1.0)
            self.assertAlmostEqual(row["spot_implied_prob_minus_market_prob"], row["spot_implied_prob"] - 0.55)
            conn.close()

    def test_unsupported_assets_are_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            touch_db = tmp_path / "touch_dataset.sqlite3"
            out_db = tmp_path / "spot_features.sqlite3"
            cache_db = tmp_path / "spot_cache.sqlite3"
            _write_touch_dataset(touch_db, asset_slug="unknowncoin")

            summary = build_spot_features(
                touch_db,
                output_path=out_db,
                cache_path=cache_db,
                fetch_klines=lambda symbol, start_ms, end_ms: [],
                sleep_seconds=0,
                asset_symbols={},
            )

            self.assertEqual(summary.input_rows, 1)
            self.assertEqual(summary.output_rows, 0)
            self.assertEqual(summary.skipped_unsupported_asset, 1)
            self.assertEqual(summary.coverage, 0.0)


def _write_touch_dataset(path: Path, *, asset_slug: str = "btc") -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE touch_dataset (
            probe_id TEXT PRIMARY KEY,
            asset_slug TEXT NOT NULL,
            direction TEXT NOT NULL,
            touch_time_iso TEXT NOT NULL,
            start_time_iso TEXT,
            end_time_iso TEXT,
            best_ask_at_touch REAL
        )
        """
    )
    conn.execute(
        "INSERT INTO touch_dataset VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            "p1",
            asset_slug,
            "UP/YES",
            "1970-01-01T00:30:30.000Z",
            "1970-01-01T00:30:00.000Z",
            "1970-01-01T00:45:00.000Z",
            0.55,
        ),
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    unittest.main()
