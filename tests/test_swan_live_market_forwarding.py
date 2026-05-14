from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_V2 = _REPO / "v2" / "short_horizon"
for p in (_REPO, _V2):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from short_horizon.core.clock import SystemClock
from short_horizon.core.events import BookUpdate, MarketStateUpdate, TradeTick
from short_horizon.market_data import LiveEventSource
from short_horizon.strategies.swan_strategy_v1 import SwanCandidate
from short_horizon.venue_polymarket import MarketMetadata, black_swan_universe_config
import v2.short_horizon.swan_live as swan_live_module
from v2.short_horizon.swan_live import (
    _WsCandidateCache,
    _build_arg_parser,
    _build_black_swan_universe_selector_config,
    _held_or_open_market_tokens,
    _ws_price_monitor_loop,
    _ws_universe_builder_loop,
)


class _FakeWs:
    def __init__(self, shutdown: asyncio.Event):
        self.shutdown = shutdown
        self.sent = False
        self.subscribed: list[list[str]] = []
        self.unsubscribed: list[list[str]] = []

    async def subscribe(self, token_ids):
        self.subscribed.append(list(token_ids))

    async def unsubscribe(self, token_ids):
        self.unsubscribed.append(list(token_ids))

    async def recv(self):
        if self.sent:
            await asyncio.sleep(0.01)
            return ""
        self.sent = True
        self.shutdown.set()
        return [
            {
                "event_type": "best_bid_ask",
                "market": "cond-1",
                "asset_id": "tok_yes",
                "timestamp": "230000",
                "best_bid": "0.04",
                "best_ask": "0.06",
            },
            {
                "event_type": "last_trade_price",
                "market": "cond-1",
                "asset_id": "tok_yes",
                "timestamp": "230001",
                "price": "0.005",
                "size": "12",
                "side": "SELL",
                "trade_id": "trade-1",
            },
        ]


class _FakeSharedDiscovery:
    def __init__(self, shutdown: asyncio.Event, markets: list[MarketMetadata]):
        self.shutdown = shutdown
        self.markets = markets

    async def get_markets(self, **kwargs):
        self.shutdown.set()
        return self.markets


class _SequencedSharedDiscovery:
    def __init__(self, shutdown: asyncio.Event, batches: list[list[MarketMetadata]]):
        self.shutdown = shutdown
        self.batches = batches
        self.calls = 0

    async def get_markets(self, **kwargs):
        batch = self.batches[min(self.calls, len(self.batches) - 1)]
        self.calls += 1
        if self.calls >= len(self.batches):
            self.shutdown.set()
        return batch


class _FakeStrategy:
    def __init__(self):
        self.candidate_updates: list[list[SwanCandidate]] = []

    def update_candidates(self, candidates):
        self.candidate_updates.append(list(candidates))


class _NoopLogger:
    def __init__(self):
        self.infos: list[tuple[tuple, dict]] = []

    def exception(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        self.infos.append((args, kwargs))


def _candidate(market_id: str, token_id: str) -> SwanCandidate:
    return SwanCandidate(
        market_id=market_id,
        condition_id=f"cond-{market_id}",
        token_id=token_id,
        question=f"Question {market_id}?",
        asset_slug="bitcoin",
        entry_levels=(0.01,),
        notional_usdc_per_level=1.0,
        candidate_id=f"candidate-{market_id}",
    )


def _market(market_id: str, *, question: str, category: str = "crypto") -> MarketMetadata:
    return MarketMetadata(
        market_id=market_id,
        condition_id=f"cond-{market_id}",
        question=question,
        token_yes_id=f"yes-{market_id}",
        token_no_id=f"no-{market_id}",
        start_time_ms=0,
        end_time_ms=1_000_000,
        asset_slug="bitcoin",
        is_active=True,
        duration_seconds=3600,
        fees_enabled=False,
        fee_rate_bps=None,
        tick_size=0.01,
        category=category,
        volume_usdc=1_000.0,
        total_duration_seconds=3600,
    )


class SwanLiveMarketForwardingTest(unittest.TestCase):
    def test_price_monitor_forwards_book_and_trade_events_to_runtime_source(self) -> None:
        async def run() -> list[object]:
            shutdown = asyncio.Event()
            source = LiveEventSource(websocket=object())
            market = MarketStateUpdate(
                event_time_ms=200_000,
                ingest_time_ms=200_050,
                market_id="m1",
                condition_id="cond-1",
                question="Q?",
                token_yes_id="tok_yes",
                token_no_id="tok_no",
                is_active=True,
            )
            source._register_market_event(market)

            await _ws_price_monitor_loop(
                ws=_FakeWs(shutdown),
                screener=object(),
                strategy=object(),
                source=source,
                clock=SystemClock(),
                stake_usdc_per_level=1.0,
                duration_stake_multipliers=(),
                price_threshold=0.01,
                token_to_market_id={"tok_yes": "m1"},
                token_to_side_index={"tok_yes": 0},
                candidate_cache=_WsCandidateCache(),
                logger=_NoopLogger(),
                shutdown=shutdown,
                forward_market_events=True,
            )
            events: list[object] = []
            while not source._queue.empty():
                events.append(source._queue.get_nowait())
            return events

        events = asyncio.run(run())

        self.assertTrue(any(isinstance(event, BookUpdate) for event in events))
        trades = [event for event in events if isinstance(event, TradeTick)]
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].market_id, "m1")
        self.assertEqual(trades[0].token_id, "tok_yes")

    def test_held_or_open_market_tokens_includes_open_orders_and_net_inventory(self) -> None:
        class Store:
            def load_non_terminal_orders(self):
                return [
                    {"order_id": "open-1", "market_id": "m1", "token_id": "tok_yes", "side": "BUY"},
                ]

            def load_all_orders(self):
                return [
                    {"order_id": "buy-1", "market_id": "m2", "token_id": "tok_no", "side": "BUY"},
                    {"order_id": "sell-1", "market_id": "m2", "token_id": "tok_no", "side": "SELL"},
                    {"order_id": "flat-buy", "market_id": "m3", "token_id": "tok_flat", "side": "BUY"},
                    {"order_id": "flat-sell", "market_id": "m3", "token_id": "tok_flat", "side": "SELL"},
                ]

            def load_fills(self):
                return [
                    {"order_id": "buy-1", "market_id": "m2", "token_id": "tok_no", "size": 10.0},
                    {"order_id": "sell-1", "market_id": "m2", "token_id": "tok_no", "size": 4.0},
                    {"order_id": "flat-buy", "market_id": "m3", "token_id": "tok_flat", "size": 5.0},
                    {"order_id": "flat-sell", "market_id": "m3", "token_id": "tok_flat", "size": 5.0},
                ]

        self.assertEqual(
            _held_or_open_market_tokens(Store()),
            [("m1", "tok_yes"), ("m2", "tok_no")],
        )

    def test_universe_selector_cli_overrides_build_policy_config(self) -> None:
        args = _build_arg_parser().parse_args([
            "--strategy", "black_swan",
            "--apply-universe-selector",
            "--universe-selector-max-markets", "25",
            "--universe-selector-max-tokens", "40",
            "--universe-selector-max-markets-per-category", "5",
            "--universe-selector-min-volume-usdc", "2500",
            "--universe-selector-reject-random-walk",
        ])
        cfg = _build_black_swan_universe_selector_config(
            max_markets=args.universe_selector_max_markets,
            max_tokens=args.universe_selector_max_tokens,
            max_markets_per_category=args.universe_selector_max_markets_per_category,
            min_volume_usdc=args.universe_selector_min_volume_usdc,
            reject_random_walk=args.universe_selector_reject_random_walk,
        )

        self.assertTrue(args.apply_universe_selector)
        self.assertEqual(cfg.max_markets, 25)
        self.assertEqual(cfg.max_tokens, 40)
        self.assertEqual(cfg.max_markets_per_category, 5)
        self.assertEqual(cfg.min_volume_usdc, 2500.0)
        self.assertTrue(cfg.reject_random_walk)
        self.assertIn("weather", cfg.category_multipliers)

    def test_universe_builder_logs_selector_observation_without_changing_subscription(self) -> None:
        async def run():
            shutdown = asyncio.Event()
            ws = _FakeWs(shutdown)
            logger = _NoopLogger()
            markets = [
                _market("random", question="Will the price of Bitcoin close above $100,000 today?"),
                _market("weather", category="weather", question="Will a hurricane hit Florida?"),
            ]
            token_to_market_id: dict[str, str] = {}
            token_to_side_index: dict[str, int] = {}

            await _ws_universe_builder_loop(
                ws=ws,
                shared_discovery=_FakeSharedDiscovery(shutdown, markets),
                token_to_market_id=token_to_market_id,
                token_to_side_index=token_to_side_index,
                duration_window=object(),
                universe_filter=object(),
                logger=logger,
                shutdown=shutdown,
                selector_observation_config=black_swan_universe_config(max_markets=1),
            )
            return ws, logger, token_to_market_id, token_to_side_index

        ws, logger, token_to_market_id, token_to_side_index = asyncio.run(run())

        # Observation mode logs the narrowed plan, but subscriptions/mappings still
        # use the legacy full discovered universe in this batch.
        self.assertEqual(set(ws.subscribed[0]), {"yes-random", "no-random", "yes-weather", "no-weather"})
        self.assertEqual(token_to_market_id["yes-random"], "random")
        self.assertEqual(token_to_side_index["no-weather"], 1)

        selector_logs = [entry for entry in logger.infos if entry[0] == ("ws_universe_selector_observation",)]
        self.assertEqual(len(selector_logs), 1)
        payload = selector_logs[0][1]
        self.assertTrue(payload["observe_only"])
        self.assertEqual(payload["discovered_markets"], 2)
        self.assertEqual(payload["selected_markets"], 1)
        self.assertEqual(payload["legacy_tokens"], 4)
        self.assertEqual(payload["selected_tokens"], 2)
        self.assertEqual(payload["token_reduction"], 2)
        self.assertEqual(payload["rejection_counts"], {"cap_markets": 1})
        self.assertEqual(payload["top_selected_market_ids"], ("weather",))

    def test_universe_builder_can_apply_selector_with_safe_unsubscribe(self) -> None:
        async def run():
            shutdown = asyncio.Event()
            ws = _FakeWs(shutdown)
            logger = _NoopLogger()
            token_to_market_id: dict[str, str] = {}
            token_to_side_index: dict[str, int] = {}
            candidate_cache = _WsCandidateCache()
            strategy = _FakeStrategy()
            await candidate_cache.upsert(strategy=strategy, candidates=[_candidate("weather", "yes-weather")])
            old_interval = swan_live_module._WS_UNIVERSE_INTERVAL_SECONDS
            swan_live_module._WS_UNIVERSE_INTERVAL_SECONDS = 0.001
            try:
                await _ws_universe_builder_loop(
                    ws=ws,
                    shared_discovery=_SequencedSharedDiscovery(
                        shutdown,
                        [
                            [
                                _market("random", question="Will the price of Bitcoin close above $100,000 today?"),
                                _market("weather", category="weather", question="Will a hurricane hit Florida?"),
                            ],
                            [_market("random", question="Will the price of Bitcoin close above $100,000 today?")],
                        ],
                    ),
                    token_to_market_id=token_to_market_id,
                    token_to_side_index=token_to_side_index,
                    duration_window=object(),
                    universe_filter=object(),
                    logger=logger,
                    shutdown=shutdown,
                    selector_observation_config=black_swan_universe_config(max_markets=1),
                    apply_selector_plan=True,
                    candidate_cache=candidate_cache,
                    strategy_for_cache_prune=strategy,
                )
            finally:
                swan_live_module._WS_UNIVERSE_INTERVAL_SECONDS = old_interval
            return ws, logger, token_to_market_id, token_to_side_index, strategy

        ws, logger, token_to_market_id, token_to_side_index, strategy = asyncio.run(run())

        self.assertEqual(set(ws.subscribed[0]), {"yes-weather", "no-weather"})
        self.assertEqual(set(ws.subscribed[1]), {"yes-random", "no-random"})
        self.assertEqual(set(ws.unsubscribed[0]), {"yes-weather", "no-weather"})
        self.assertEqual(token_to_market_id, {"yes-random": "random", "no-random": "random"})
        self.assertEqual(token_to_side_index, {"yes-random": 0, "no-random": 1})

        selector_logs = [entry for entry in logger.infos if entry[0] == ("ws_universe_selector_observation",)]
        self.assertEqual(len(selector_logs), 2)
        self.assertFalse(selector_logs[0][1]["observe_only"])
        universe_logs = [entry for entry in logger.infos if entry[0] == ("ws_universe_updated",)]
        self.assertTrue(all(entry[1]["selector_applied"] for entry in universe_logs))
        self.assertEqual(universe_logs[-1][1]["pruned_candidates"], 1)
        self.assertEqual(strategy.candidate_updates[-1], [])

    def test_universe_builder_retains_protected_held_tokens_when_selector_applies(self) -> None:
        async def run():
            shutdown = asyncio.Event()
            ws = _FakeWs(shutdown)
            logger = _NoopLogger()
            token_to_market_id: dict[str, str] = {}
            token_to_side_index: dict[str, int] = {}

            await _ws_universe_builder_loop(
                ws=ws,
                shared_discovery=_FakeSharedDiscovery(
                    shutdown,
                    [
                        _market("random", question="Will the price of Bitcoin close above $100,000 today?"),
                        _market("weather", category="weather", question="Will a hurricane hit Florida?"),
                    ],
                ),
                token_to_market_id=token_to_market_id,
                token_to_side_index=token_to_side_index,
                duration_window=object(),
                universe_filter=object(),
                logger=logger,
                shutdown=shutdown,
                selector_observation_config=black_swan_universe_config(max_markets=1),
                apply_selector_plan=True,
                protected_market_tokens_fn=lambda: [("random", "yes-random")],
            )
            return ws, logger, token_to_market_id, token_to_side_index

        ws, logger, token_to_market_id, token_to_side_index = asyncio.run(run())

        self.assertEqual(set(ws.subscribed[0]), {"yes-weather", "no-weather", "yes-random"})
        self.assertEqual(token_to_market_id["yes-random"], "random")
        self.assertEqual(token_to_side_index["yes-random"], 0)
        self.assertNotIn("no-random", token_to_market_id)
        universe_logs = [entry for entry in logger.infos if entry[0] == ("ws_universe_updated",)]
        self.assertEqual(universe_logs[-1][1]["protected_tokens"], 1)
