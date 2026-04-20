from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.venue_polymarket import DurationWindow, UniverseFilter, discover_short_horizon_markets_sync, parse_market_discovery_rows


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
NOW_TS = 1_776_693_900.0  # 2026-04-20T14:05:00Z


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []
        self.closed = False

    def get(self, url, *, params, timeout):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return _FakeResponse(self.pages.pop(0))

    def close(self) -> None:
        self.closed = True


class VenuePolymarketMarketsTest(unittest.TestCase):
    def _fixture_rows(self):
        return json.loads((FIXTURES_DIR / "gamma_short_horizon_markets.json").read_text(encoding="utf-8"))

    def test_parse_market_discovery_rows_keeps_only_expected_eligible_markets(self) -> None:
        markets, stats = parse_market_discovery_rows(
            self._fixture_rows(),
            universe_filter=UniverseFilter(allowed_assets=("bitcoin", "ethereum")),
            duration_window=DurationWindow(min_seconds=850, max_seconds=960, duration_metric="lifecycle"),
            now_ts=NOW_TS,
        )

        self.assertEqual([market.market_id for market in markets], ["m_btc_850", "m_eth_960"])
        self.assertEqual(stats.eligible_markets, 2)
        self.assertEqual(stats.skipped_duration_window, 1)
        self.assertEqual(stats.skipped_asset_filter, 1)
        self.assertEqual(stats.skipped_missing_end, 1)
        self.assertEqual(stats.skipped_duplicate, 1)

        btc_market = markets[0]
        eth_market = markets[1]

        self.assertEqual(btc_market.asset_slug, "bitcoin")
        self.assertEqual(btc_market.duration_seconds, 900)
        self.assertEqual(btc_market.start_time_ms, 1_776_693_600_000)
        self.assertEqual(btc_market.end_time_ms, 1_776_694_450_000)
        self.assertEqual(btc_market.token_yes_id, "tok_btc_yes")
        self.assertEqual(btc_market.token_no_id, "tok_btc_no")
        self.assertAlmostEqual(btc_market.fee_rate_bps, 35.0)
        self.assertAlmostEqual(btc_market.tick_size or 0.0, 0.01)

        self.assertEqual(eth_market.asset_slug, "ethereum")
        self.assertEqual(eth_market.duration_seconds, 900)
        self.assertEqual(eth_market.start_time_ms, 1_776_693_600_000)
        self.assertEqual(eth_market.end_time_ms, 1_776_694_560_000)
        self.assertAlmostEqual(eth_market.fee_rate_bps or 0.0, 8.0)

    def test_discover_short_horizon_markets_sync_uses_expected_gamma_query_shape(self) -> None:
        rows = self._fixture_rows()
        session = _FakeSession([rows, []])

        markets = discover_short_horizon_markets_sync(
            universe_filter=UniverseFilter(allowed_assets=("bitcoin", "ethereum")),
            duration_window=DurationWindow(min_seconds=850, max_seconds=960, duration_metric="lifecycle"),
            max_rows=20_000,
            order="volume24hr",
            ascending=True,
            session=session,
            now_ts=NOW_TS,
        )

        self.assertEqual(len(markets), 2)
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(session.calls[0]["params"]["active"], "true")
        self.assertEqual(session.calls[0]["params"]["closed"], "false")
        self.assertEqual(session.calls[0]["params"]["archived"], "false")
        self.assertEqual(session.calls[0]["params"]["order"], "volume24hr")
        self.assertEqual(session.calls[0]["params"]["ascending"], "true")
        self.assertFalse(session.closed)


if __name__ == "__main__":
    unittest.main()
