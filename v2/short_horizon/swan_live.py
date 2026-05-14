"""
Swan V2 Live Runner
===================

Replaces bot/main_loop.py for production trading, using the V2 execution path
(pUSD + V2 CTF Exchange spender) instead of the deprecated V1 CLOB client.

Old bot/main_loop.py remains as a backtest harness; this file owns all live
order submission going forward.

Usage:
    # dry-run (default, big_swan strategy)
    python -m v2.short_horizon.swan_live

    # black_swan strategy dry-run
    python -m v2.short_horizon.swan_live --strategy black_swan

    # live
    python -m v2.short_horizon.swan_live --execution-mode live

    # black_swan live, with 1-hour resolved redeem sweep
    python -m v2.short_horizon.swan_live --strategy black_swan --execution-mode live --redeem-interval 3600
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from dataclasses import asdict, replace as dataclass_replace
from pathlib import Path
from typing import Callable, Iterable

# Ensure repo root and v2/short_horizon/ are on path so all imports resolve.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_V2_SH_DIR = Path(__file__).resolve().parent  # v2/short_horizon/ → exposes short_horizon pkg
for _p in (_REPO_ROOT, _V2_SH_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from short_horizon.core.clock import SystemClock
from short_horizon.core.events import BookLevel, BookUpdate, TimerEvent
from short_horizon.core.runtime import StrategyRuntime
from short_horizon.execution import ExecutionEngine, ExecutionMode
from short_horizon.live_runner import (
    PeriodicResolvedRedeemSweeper,
    build_live_submit_guard,
    execute_resolved_redeem,
    generate_run_id,
    reconcile_runtime_orders,
)
from short_horizon.market_data import LiveEventSource
from short_horizon.runner import RunnerSummary, drive_runtime_event_stream
from short_horizon.storage import RunContext, SQLiteRuntimeStore
from short_horizon.strategies.swan_strategy_v1 import (
    TIMER_SCREENER_REFRESH,
    TIMER_STALE_CLEANUP,
    SwanCandidate,
    SwanConfig,
    SwanStrategyV1,
)
from short_horizon.strategies.black_swan_strategy_v1 import (
    BlackSwanConfig,
    BlackSwanStrategyV1,
)
from short_horizon.telemetry import configure_logging, get_logger
from short_horizon.venue_polymarket import PolymarketUserStream, UniverseSelectorConfig, black_swan_universe_config, build_subscription_plan
from short_horizon.venue_polymarket.execution_client import (
    PRIVATE_KEY_ENV_VAR,
    PolymarketExecutionClient,
)
from short_horizon.venue_polymarket.markets import DurationWindow, UniverseFilter
from short_horizon.venue_polymarket.book_channel import BookNormalizer
from short_horizon.venue_polymarket.websocket import PolymarketWebsocket

# Import old swan screener (reused as-is for market discovery).
from config import BIG_SWAN_MODE, BLACK_SWAN_MODE, BotConfig, load_config
from execution.order_manager import POSITIONS_DB
from strategy.market_scorer import MarketScorer
from strategy.screener import EntryCandidate, Screener
from api.gamma_client import MarketInfo, fetch_market

_DB_DIR = Path(os.environ.get("POLYBOT_DATA_DIR", Path.home() / ".polybot" / "swan_v2"))

_SCREENER_INTERVAL_SECONDS = 300   # 5 min
_CLEANUP_INTERVAL_SECONDS = 1800   # 30 min
_HELD_BOOK_REFRESH_INTERVAL_SECONDS = 60
_HELD_BOOK_STALE_AFTER_MS = 2 * 60 * 1000
_HELD_BOOK_REFRESH_MAX_TOKENS_PER_PASS = 150
_DEFAULT_STAKE_PER_LEVEL = 1.0     # target ~1 USDC notional per entry level (~5 USDC/market ladder)

_STRATEGY_REGISTRY = {
    "swan": {
        "mode":           BIG_SWAN_MODE,
        "config_class":   SwanConfig,
        "strategy_class": SwanStrategyV1,
        "strategy_id":    "swan_v1",
        "db_name":        "swan_v2_live.sqlite3",
    },
    "black_swan": {
        "mode":           BLACK_SWAN_MODE,
        "config_class":   BlackSwanConfig,
        "strategy_class": BlackSwanStrategyV1,
        "strategy_id":    "black_swan_v1",
        "db_name":        "black_swan_v1_live.sqlite3",
    },
}


class _ScreenerConfigOverride:
    """Proxies BotConfig but replaces mode_config with a given ModeConfig."""

    def __init__(self, base: BotConfig, *, mode_config) -> None:
        self._base = base
        self._mc = mode_config

    @property
    def mode_config(self):
        return self._mc

    def __getattr__(self, name: str):
        return getattr(self._base, name)


def _duration_stake_multiplier(
    hours_to_close: float | None,
    table: tuple[tuple[float, float], ...],
) -> float:
    """First-match multiplier from a (max_hours, multiplier) table.

    Empty table → 1.0 (no scaling). None hours_to_close → 1.0 (treat as
    unknown rather than apply worst-case attenuation, matches behaviour of
    other null-default fallbacks in the screener).
    """
    if not table or hours_to_close is None:
        return 1.0
    for max_h, mult in table:
        if hours_to_close <= max_h:
            return mult
    return 1.0


def _entry_candidate_to_swan(
    candidate: EntryCandidate,
    *,
    stake_usdc_per_level: float,
    duration_stake_multipliers: tuple[tuple[float, float], ...] = (),
) -> SwanCandidate:
    mi = candidate.market_info
    duration_mult = _duration_stake_multiplier(
        getattr(mi, "hours_to_close", None), duration_stake_multipliers
    )
    return SwanCandidate(
        market_id=mi.market_id,
        condition_id=mi.condition_id,
        token_id=candidate.token_id,
        question=mi.question or "",
        asset_slug=getattr(mi, "slug", None) or "",
        entry_levels=tuple(sorted(set(candidate.suggested_entry_levels))),
        notional_usdc_per_level=stake_usdc_per_level * duration_mult,
        candidate_id=candidate.candidate_id or "",
    )


async def _screener_loop(
    *,
    screener: Screener,
    strategy: SwanStrategyV1,
    source: LiveEventSource,
    interval_seconds: float,
    clock: SystemClock,
    logger,
    shutdown: asyncio.Event,
    stake_usdc_per_level: float,
    duration_stake_multipliers: tuple[tuple[float, float], ...] = (),
) -> None:
    """Runs the swan screener periodically and injects a timer event each time."""
    while not shutdown.is_set():
        try:
            active_market_ids = strategy.get_active_market_ids()
            raw_candidates = await asyncio.to_thread(screener.scan, None, active_market_ids)
            swan_candidates = [
                _entry_candidate_to_swan(
                    c,
                    stake_usdc_per_level=stake_usdc_per_level,
                    duration_stake_multipliers=duration_stake_multipliers,
                )
                for c in raw_candidates
            ]
            strategy.update_candidates(swan_candidates)
            now_ms = clock.now_ms()
            source.inject(TimerEvent(
                event_time_ms=now_ms,
                ingest_time_ms=now_ms,
                timer_kind=TIMER_SCREENER_REFRESH,
                source="swan_screener_task",
            ))
            logger.info(
                "swan_screener_refresh",
                candidates=len(swan_candidates),
                timer_kind=TIMER_SCREENER_REFRESH,
            )
        except Exception:
            logger.exception("swan_screener_error")

        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            pass


async def _cleanup_loop(
    *,
    source: LiveEventSource,
    clock: SystemClock,
    interval_seconds: float,
    shutdown: asyncio.Event,
) -> None:
    """Periodically injects a stale-order-cleanup timer event."""
    while not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            pass
        if shutdown.is_set():
            break
        now_ms = clock.now_ms()
        source.inject(TimerEvent(
            event_time_ms=now_ms,
            ingest_time_ms=now_ms,
            timer_kind=TIMER_STALE_CLEANUP,
            source="swan_cleanup_task",
        ))


_WS_UNIVERSE_INTERVAL_SECONDS = 300   # rebuild WS subscription universe every 5 min
_WS_PATTERN_RETRY_DELAY_SECONDS = 90  # fallback/max retry delay
_WS_PATTERN_RETRY_DELAY_MIN_SECONDS = 20
_WS_PATTERN_RETRY_DELAY_FRACTION_OF_REMAINING = 0.05
_WS_PATTERN_RETRY_MAX_ATTEMPTS = 3




def _ws_pattern_retry_delay_seconds(market_info: MarketInfo, *, now_s: float | None = None) -> int:
    remaining_seconds: float | None = None
    hours_to_close = getattr(market_info, "hours_to_close", None)
    if hours_to_close is not None:
        try:
            remaining_seconds = max(0.0, float(hours_to_close) * 3600.0)
        except (TypeError, ValueError):
            remaining_seconds = None
    if remaining_seconds is None:
        end_date_ts = getattr(market_info, "end_date_ts", None)
        if end_date_ts is not None:
            try:
                remaining_seconds = max(0.0, float(end_date_ts) - float(now_s if now_s is not None else time.time()))
            except (TypeError, ValueError):
                remaining_seconds = None
    if remaining_seconds is None:
        return _WS_PATTERN_RETRY_DELAY_SECONDS
    return int(round(max(
        float(_WS_PATTERN_RETRY_DELAY_MIN_SECONDS),
        min(
            float(_WS_PATTERN_RETRY_DELAY_SECONDS),
            remaining_seconds * _WS_PATTERN_RETRY_DELAY_FRACTION_OF_REMAINING,
        ),
    )))


class _WsCandidateCache:
    """Maintains the full candidate snapshot for WS-triggered screener updates.

    SwanStrategyV1.update_candidates() expects a complete universe snapshot and
    cancels markets missing from the next refresh. WS price triggers are
    incremental, so this adapter merges per-market trigger results before
    notifying the strategy.
    """

    def __init__(self) -> None:
        self._by_market: dict[str, SwanCandidate] = {}
        self._lock = asyncio.Lock()

    async def upsert(self, *, strategy: SwanStrategyV1, candidates: list[SwanCandidate]) -> int:
        async with self._lock:
            for candidate in candidates:
                self._by_market[candidate.market_id] = candidate
            strategy.update_candidates(list(self._by_market.values()))
            return len(self._by_market)

    async def prune_market_ids(self, *, strategy: SwanStrategyV1, market_ids: Iterable[str]) -> int:
        async with self._lock:
            removed = 0
            for market_id in market_ids:
                if self._by_market.pop(str(market_id), None) is not None:
                    removed += 1
            if removed:
                strategy.update_candidates(list(self._by_market.values()))
            return removed


def _market_with_trigger_price(
    market: MarketInfo, *, token_id: str, side_index: int | None, ask: float
) -> MarketInfo:
    """Return a MarketInfo snapshot whose price gate is anchored to CLOB best_ask.

    Legacy Screener stores only the YES best ask. For a NO-token trigger we set
    YES ~= 1 - NO ask so the existing synthetic-NO prefilter can pass; the
    screener then refines the NO side with the real CLOB book as before.
    """
    try:
        token_index = market.token_ids.index(token_id)
    except ValueError:
        token_index = 0 if side_index is None else side_index
    clamped_ask = max(0.0, min(1.0, float(ask)))
    synthetic_yes_ask = clamped_ask if token_index == 0 else 1.0 - clamped_ask
    return dataclass_replace(
        market,
        best_ask=synthetic_yes_ask,
        last_trade_price=synthetic_yes_ask if market.last_trade_price is None else market.last_trade_price,
    )


async def _ws_universe_builder_loop(
    *,
    ws: PolymarketWebsocket,
    shared_discovery,  # SharedMarketDiscovery — reuse existing cached snapshot
    token_to_market_id: dict[str, str],
    token_to_side_index: dict[str, int],
    duration_window: DurationWindow,
    universe_filter: UniverseFilter,
    logger,
    shutdown: asyncio.Event,
    selector_observation_config: UniverseSelectorConfig | None = None,
    apply_selector_plan: bool = False,
    protected_market_tokens_fn: Callable[[], Iterable[tuple[str, str]]] | None = None,
    candidate_cache: _WsCandidateCache | None = None,
    strategy_for_cache_prune: SwanStrategyV1 | None = None,
) -> None:
    """Periodically syncs CLOB WS subscription to the already-discovered market universe.

    Reuses SharedMarketDiscovery's cached snapshot to avoid extra Gamma API calls
    (the main MarketRefreshLoop already keeps the snapshot fresh).
    """
    subscribed: set[str] = set()
    while not shutdown.is_set():
        try:
            # Read from the shared snapshot (no extra Gamma request if cache is warm).
            markets = await shared_discovery.get_markets(
                universe_filter=universe_filter,
                duration_window=duration_window,
                max_rows=5_000,
            )
            legacy_tokens, legacy_token_to_market_id, legacy_token_to_side_index = _legacy_token_subscription_maps(markets)
            selector_plan = None
            if selector_observation_config is not None:
                selector_plan = build_subscription_plan(markets, config=selector_observation_config)
                logger.info(
                    "ws_universe_selector_observation",
                    observe_only=not apply_selector_plan,
                    legacy_tokens=len(legacy_tokens),
                    token_reduction=max(0, len(legacy_tokens) - selector_plan.selected_tokens_count),
                    **asdict(selector_plan.summary(top_n=10)),
                )

            tokens: set[str] = set()
            next_token_to_market_id: dict[str, str] = {}
            next_token_to_side_index: dict[str, int] = {}
            old_token_to_market_id = dict(token_to_market_id)
            old_token_to_side_index = dict(token_to_side_index)
            market_by_id = {str(m.market_id): m for m in markets if m.market_id}
            protected_pairs = _load_protected_market_tokens(protected_market_tokens_fn)
            protected_token_ids = {token_id for _, token_id in protected_pairs}
            if apply_selector_plan and selector_plan is not None:
                tokens = set(selector_plan.selected_token_ids)
                next_token_to_market_id.update(selector_plan.token_to_market_id)
                next_token_to_side_index.update(selector_plan.token_to_side_index)
            else:
                tokens = set(legacy_tokens)
                next_token_to_market_id.update(legacy_token_to_market_id)
                next_token_to_side_index.update(legacy_token_to_side_index)
            for market_id, token_id in protected_pairs:
                tokens.add(token_id)
                next_token_to_market_id[token_id] = market_id
                next_token_to_side_index[token_id] = _resolve_side_index(
                    token_id=token_id,
                    market=market_by_id.get(market_id),
                    existing_side_index=old_token_to_side_index.get(token_id),
                )

            to_add = tokens - subscribed
            to_remove = subscribed - tokens
            prunable_market_ids = {
                old_token_to_market_id[token_id]
                for token_id in to_remove
                if token_id in old_token_to_market_id and token_id not in protected_token_ids
            } - set(next_token_to_market_id.values())

            token_to_market_id.clear()
            token_to_market_id.update(next_token_to_market_id)
            token_to_side_index.clear()
            token_to_side_index.update(next_token_to_side_index)

            if to_add:
                await ws.subscribe(list(to_add))
            if to_remove:
                await ws.unsubscribe(list(to_remove))
            pruned_candidates = 0
            if prunable_market_ids and candidate_cache is not None and strategy_for_cache_prune is not None:
                pruned_candidates = await candidate_cache.prune_market_ids(
                    strategy=strategy_for_cache_prune,
                    market_ids=prunable_market_ids,
                )
            subscribed = tokens
            logger.info(
                "ws_universe_updated",
                markets=len(markets),
                tokens=len(tokens),
                added=len(to_add),
                removed=len(to_remove),
                selector_applied=apply_selector_plan and selector_plan is not None,
                protected_tokens=len(protected_token_ids),
                pruned_candidates=pruned_candidates,
            )
        except Exception:
            logger.exception("ws_universe_builder_error")

        try:
            await asyncio.wait_for(shutdown.wait(), timeout=_WS_UNIVERSE_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


def _legacy_token_subscription_maps(markets) -> tuple[set[str], dict[str, str], dict[str, int]]:
    tokens: set[str] = set()
    token_to_market_id: dict[str, str] = {}
    token_to_side_index: dict[str, int] = {}
    for market in markets:
        if market.token_yes_id:
            tokens.add(market.token_yes_id)
            token_to_market_id[market.token_yes_id] = market.market_id
            token_to_side_index[market.token_yes_id] = 0
        if market.token_no_id:
            tokens.add(market.token_no_id)
            token_to_market_id[market.token_no_id] = market.market_id
            token_to_side_index[market.token_no_id] = 1
    return tokens, token_to_market_id, token_to_side_index


def _load_protected_market_tokens(
    protected_market_tokens_fn: Callable[[], Iterable[tuple[str, str]]] | None,
) -> set[tuple[str, str]]:
    if protected_market_tokens_fn is None:
        return set()
    return {
        (str(market_id), str(token_id))
        for market_id, token_id in protected_market_tokens_fn()
        if market_id and token_id
    }


def _resolve_side_index(
    *,
    token_id: str,
    market,
    existing_side_index: int | None,
) -> int:
    if existing_side_index is not None:
        return int(existing_side_index)
    if market is not None:
        if token_id == getattr(market, "token_no_id", None):
            return 1
        if token_id == getattr(market, "token_yes_id", None):
            return 0
    return 0


async def _ws_price_monitor_loop(
    *,
    ws: PolymarketWebsocket,
    screener: Screener,
    strategy: SwanStrategyV1,
    source: LiveEventSource,
    clock: SystemClock,
    stake_usdc_per_level: float,
    duration_stake_multipliers: tuple[tuple[float, float], ...],
    price_threshold: float,
    token_to_market_id: dict[str, str],
    token_to_side_index: dict[str, int],
    candidate_cache: _WsCandidateCache,
    logger,
    shutdown: asyncio.Event,
    forward_market_events: bool = False,
) -> None:
    """Watches CLOB WS for best_ask crossing ≤ price_threshold; triggers per-market screener."""
    # Reuse LiveEventSource normalizers instead of a private BookNormalizer so
    # market/condition/token mappings registered by market refresh are shared.
    # In dry-run this also forwards public market events into the main runtime
    # queue, which is what the paper fill simulator consumes.
    normalizer = source.book_normalizer
    trade_normalizer = source.trade_normalizer
    below_threshold: set[str] = set()

    while not shutdown.is_set():
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
        except asyncio.TimeoutError:
            continue
        except Exception:
            logger.exception("ws_price_monitor_recv_error")
            continue

        ingest_ms = clock.now_ms()
        try:
            updates = normalizer.normalize_frame(raw, ingest_time_ms=ingest_ms)
            trade_updates = trade_normalizer.normalize_frame(raw, ingest_time_ms=ingest_ms)
        except Exception:
            continue

        if forward_market_events:
            for idx, event in enumerate([*updates, *trade_updates], start=1):
                source.inject(event)
                if idx % 500 == 0:
                    await asyncio.sleep(0)

        for idx, update in enumerate(updates, start=1):
            if idx % 500 == 0:
                await asyncio.sleep(0)
            ask = update.best_ask
            if ask is None:
                continue
            token_id = update.token_id
            was_below = token_id in below_threshold
            is_below = ask <= price_threshold

            if is_below and not was_below:
                below_threshold.add(token_id)
                resolved_market_id = token_to_market_id.get(token_id)
                side_index = token_to_side_index.get(token_id)
                if resolved_market_id is None or side_index is None:
                    logger.warning(
                        "ws_trigger_unknown_token",
                        token_id=token_id,
                        update_market_id=update.market_id,
                        ask=ask,
                    )
                    continue
                asyncio.create_task(
                    _ws_trigger_screener(
                        token_id=token_id,
                        market_id=resolved_market_id,
                        side_index=side_index,
                        ask=ask,
                        screener=screener,
                        strategy=strategy,
                        source=source,
                        clock=clock,
                        stake_usdc_per_level=stake_usdc_per_level,
                        duration_stake_multipliers=duration_stake_multipliers,
                        candidate_cache=candidate_cache,
                        logger=logger,
                        price_threshold=price_threshold,
                        retry_attempt=0,
                    ),
                    name=f"ws_trigger_{token_id[:12]}",
                )
            elif not is_below and was_below:
                below_threshold.discard(token_id)


async def _held_book_snapshot_refresh_loop(
    *,
    runtime: StrategyRuntime,
    source: LiveEventSource,
    clock: SystemClock,
    interval_seconds: float,
    stale_after_ms: int,
    max_tokens_per_pass: int,
    logger,
    shutdown: asyncio.Event,
) -> None:
    """Refresh stale/missing books for tokens where the run has exposure.

    Public CLOB WS subscriptions are broad and can reconnect or go quiet for an
    individual token.  This loop keeps marks for open/held tokens point-in-time
    fresh by injecting REST book snapshots into the same runtime event path used
    by WS BookUpdates.
    """
    while not shutdown.is_set():
        try:
            now_ms = int(clock.now_ms())
            held_tokens = _held_or_open_market_tokens(runtime.store)
            missing = 0
            stale = 0
            fresh = 0
            refresh_targets: list[tuple[str, str]] = []
            for market_id, token_id in held_tokens:
                age_ms = runtime.book_age_ms(market_id, token_id, now_ms=now_ms)
                if age_ms is None:
                    missing += 1
                    refresh_targets.append((market_id, token_id))
                elif age_ms > stale_after_ms:
                    stale += 1
                    refresh_targets.append((market_id, token_id))
                else:
                    fresh += 1

            attempted = 0
            refreshed = 0
            failed = 0
            for market_id, token_id in refresh_targets[:max_tokens_per_pass]:
                attempted += 1
                try:
                    event = await _fetch_book_snapshot_event(
                        market_id=market_id,
                        token_id=token_id,
                        clock=clock,
                    )
                except Exception as exc:
                    failed += 1
                    logger.warning(
                        "held_book_snapshot_refresh_failed",
                        market_id=market_id,
                        token_id=token_id,
                        error=str(exc),
                    )
                    continue
                source.inject(event)
                refreshed += 1

            if held_tokens or attempted:
                logger.info(
                    "held_book_freshness_check",
                    held_tokens=len(held_tokens),
                    fresh=fresh,
                    stale=stale,
                    missing=missing,
                    refresh_targets=len(refresh_targets),
                    attempted=attempted,
                    refreshed=refreshed,
                    failed=failed,
                    max_tokens_per_pass=max_tokens_per_pass,
                    stale_after_ms=stale_after_ms,
                )
        except Exception:
            logger.exception("held_book_snapshot_refresh_loop_error")

        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            pass


async def _fetch_book_snapshot_event(*, market_id: str, token_id: str, clock: SystemClock) -> BookUpdate:
    from api.clob_client import get_orderbook

    orderbook = await asyncio.to_thread(get_orderbook, token_id)
    now_ms = int(clock.now_ms())
    bid_levels = tuple(BookLevel(price=float(level.price), size=float(level.size)) for level in orderbook.bids[:10])
    ask_levels = tuple(BookLevel(price=float(level.price), size=float(level.size)) for level in orderbook.asks[:10])
    best_bid = float(orderbook.best_bid) if orderbook.best_bid is not None else None
    best_ask = float(orderbook.best_ask) if orderbook.best_ask is not None else None
    spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None
    mid_price = ((best_ask + best_bid) / 2.0) if best_bid is not None and best_ask is not None else None
    return BookUpdate(
        event_time_ms=now_ms,
        ingest_time_ms=now_ms,
        market_id=str(market_id),
        token_id=str(token_id),
        best_bid=best_bid,
        best_ask=best_ask,
        spread=spread,
        mid_price=mid_price,
        bid_levels=bid_levels,
        ask_levels=ask_levels,
        is_snapshot=True,
        source="polymarket_clob_rest_snapshot",
    )


def _held_or_open_market_tokens(store) -> list[tuple[str, str]]:
    tokens: set[tuple[str, str]] = set()
    load_non_terminal_orders = getattr(store, "load_non_terminal_orders", None)
    if callable(load_non_terminal_orders):
        for row in load_non_terminal_orders():
            market_id = row.get("market_id")
            token_id = row.get("token_id")
            if market_id and token_id:
                tokens.add((str(market_id), str(token_id)))

    load_all_orders = getattr(store, "load_all_orders", None)
    load_fills = getattr(store, "load_fills", None)
    if callable(load_all_orders) and callable(load_fills):
        order_by_id = {str(row.get("order_id")): row for row in load_all_orders() if row.get("order_id") is not None}
        net_size_by_token: dict[tuple[str, str], float] = {}
        for fill in load_fills():
            order = order_by_id.get(str(fill.get("order_id") or ""), {})
            side = str(order.get("side") or "").upper()
            market_id = fill.get("market_id")
            token_id = fill.get("token_id")
            if not market_id or not token_id:
                continue
            key = (str(market_id), str(token_id))
            size = float(fill.get("size") or 0.0)
            if side == "BUY":
                net_size_by_token[key] = net_size_by_token.get(key, 0.0) + size
            elif side == "SELL":
                net_size_by_token[key] = net_size_by_token.get(key, 0.0) - size
        for key, net_size in net_size_by_token.items():
            if net_size > 1e-12:
                tokens.add(key)

    return sorted(tokens)


async def _ws_trigger_screener(
    *,
    token_id: str,
    market_id: str | None,
    side_index: int | None,
    ask: float,
    screener: Screener,
    strategy: SwanStrategyV1,
    source: LiveEventSource,
    clock: SystemClock,
    stake_usdc_per_level: float,
    duration_stake_multipliers: tuple[tuple[float, float], ...],
    candidate_cache: _WsCandidateCache,
    logger,
    price_threshold: float,
    retry_attempt: int = 0,
) -> None:
    """Runs single-market screener evaluation after a CLOB price edge trigger."""
    if not market_id:
        return
    try:
        market_info = await asyncio.to_thread(fetch_market, market_id)
    except Exception:
        logger.warning("ws_trigger_fetch_failed", token_id=token_id, market_id=market_id)
        return
    if market_info is None:
        return
    if token_id not in market_info.token_ids and side_index is None:
        logger.warning("ws_trigger_token_not_in_market", token_id=token_id, market_id=market_id)
        return

    market_info = _market_with_trigger_price(
        market_info,
        token_id=token_id,
        side_index=side_index,
        ask=ask,
    )

    log_entries: list[tuple] = []
    try:
        candidates = [
            c for c in screener._evaluate_market(market_info, log_entries)
            if c.token_id == token_id
        ]
    except Exception:
        logger.exception("ws_trigger_eval_error", market_id=market_id)
        return
    finally:
        if log_entries:
            screener._flush_screener_log(log_entries)

    if not candidates:
        deferred_pattern = any(
            len(entry) > 8 and entry[2] == token_id and entry[8] == "deferred_pattern"
            for entry in log_entries
        )
        if deferred_pattern and retry_attempt < _WS_PATTERN_RETRY_MAX_ATTEMPTS:
            next_attempt = retry_attempt + 1
            retry_delay_seconds = _ws_pattern_retry_delay_seconds(market_info)
            logger.info(
                "ws_price_trigger_deferred_pattern_retry_scheduled",
                token_id=token_id,
                ask=ask,
                market_id=market_id,
                retry_attempt=next_attempt,
                delay_seconds=retry_delay_seconds,
            )
            await asyncio.sleep(retry_delay_seconds)
            try:
                from api.clob_client import get_orderbook
                orderbook = await asyncio.to_thread(get_orderbook, token_id)
                retry_ask = orderbook.best_ask
            except Exception:
                retry_ask = ask
            if retry_ask is None or retry_ask > price_threshold:
                logger.info(
                    "ws_price_trigger_deferred_pattern_retry_cancelled",
                    token_id=token_id,
                    ask=retry_ask,
                    market_id=market_id,
                    retry_attempt=next_attempt,
                    reason="price_above_threshold_or_missing",
                )
                return
            await _ws_trigger_screener(
                token_id=token_id,
                market_id=market_id,
                side_index=side_index,
                ask=float(retry_ask),
                screener=screener,
                strategy=strategy,
                source=source,
                clock=clock,
                stake_usdc_per_level=stake_usdc_per_level,
                duration_stake_multipliers=duration_stake_multipliers,
                candidate_cache=candidate_cache,
                logger=logger,
                price_threshold=price_threshold,
                retry_attempt=next_attempt,
            )
            return
        logger.info(
            "ws_price_trigger_no_candidates",
            token_id=token_id,
            ask=ask,
            market_id=market_id,
            deferred_pattern=deferred_pattern,
            retry_attempt=retry_attempt,
        )
        return

    swan_cands = [
        _entry_candidate_to_swan(c, stake_usdc_per_level=stake_usdc_per_level,
                                 duration_stake_multipliers=duration_stake_multipliers)
        for c in candidates
    ]
    cached_candidates = await candidate_cache.upsert(strategy=strategy, candidates=swan_cands)
    now_ms = clock.now_ms()
    source.inject(TimerEvent(
        event_time_ms=now_ms,
        ingest_time_ms=now_ms,
        timer_kind=TIMER_SCREENER_REFRESH,
        source="swan_ws_price_trigger",
    ))
    logger.info(
        "ws_price_trigger_candidates",
        token_id=token_id,
        ask=ask,
        market_id=market_id,
        candidates=len(swan_cands),
        cached_candidates=cached_candidates,
    )


def _build_black_swan_universe_selector_config(
    *,
    max_markets: int | None = None,
    max_tokens: int | None = None,
    max_markets_per_category: int | None = None,
    min_volume_usdc: float | None = None,
    reject_random_walk: bool = False,
) -> UniverseSelectorConfig:
    overrides = {}
    if max_markets is not None:
        overrides["max_markets"] = max(0, int(max_markets))
    if max_tokens is not None:
        overrides["max_tokens"] = max(0, int(max_tokens))
    if max_markets_per_category is not None:
        overrides["max_markets_per_category"] = max(0, int(max_markets_per_category))
    if min_volume_usdc is not None:
        overrides["min_volume_usdc"] = max(0.0, float(min_volume_usdc))
    if reject_random_walk:
        overrides["reject_random_walk"] = True
    return black_swan_universe_config(**overrides)


async def run_swan_live(
    *,
    db_path: Path | str | None = None,
    run_id: str | None = None,
    execution_mode: ExecutionMode | str = ExecutionMode.DRY_RUN,
    max_runtime_seconds: float | None = None,
    max_live_orders_total: int | None = None,
    redeem_interval_seconds: float | None = None,
    stake_per_level: float | None = None,
    max_hours_to_close: float | None = None,
    min_hours_to_close: float | None = None,
    min_total_duration_hours: float | None = None,
    strategy_name: str = "swan",
    apply_universe_selector: bool = False,
    universe_selector_max_markets: int | None = None,
    universe_selector_max_tokens: int | None = None,
    universe_selector_max_markets_per_category: int | None = None,
    universe_selector_min_volume_usdc: float | None = None,
    universe_selector_reject_random_walk: bool = False,
) -> RunnerSummary:
    reg = _STRATEGY_REGISTRY.get(strategy_name)
    if reg is None:
        raise ValueError(f"Unknown strategy: {strategy_name!r}. Choose from {list(_STRATEGY_REGISTRY)}")

    mode_config    = reg["mode"]
    config_class   = reg["config_class"]
    strategy_class = reg["strategy_class"]
    strategy_id    = reg["strategy_id"]

    resolved_mode = ExecutionMode(str(execution_mode))
    db_path = Path(db_path or (_DB_DIR / reg["db_name"]))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    run_id = run_id or f"{strategy_id}_{uuid.uuid4().hex[:12]}"

    configure_logging()
    logger = get_logger("swan_live", run_id=run_id, strategy=strategy_name)
    logger.info("swan_live_starting", execution_mode=resolved_mode.value, db_path=str(db_path), strategy=strategy_name)

    clock = SystemClock()

    # ── Strategy ──────────────────────────────────────────────────────────────
    # stale_order_ttl_seconds intentionally NOT overridden: each strategy class
    # owns its TTL default (SwanConfig=1h, BlackSwanConfig=6h, chosen from
    # buy_phase analytics in #180). Previously this was overridden to
    # mode_config.max_hours_to_close*3600 (168h=7d for black_swan), defeating
    # the purpose of the TTL.
    swan_config = config_class(
        strategy_id=strategy_id,
        max_open_resting_bids=mode_config.max_open_positions,
        max_resting_markets=mode_config.max_resting_markets,
        max_resting_per_cluster=mode_config.max_resting_per_cluster,
        phase_stake_multipliers=(),  # keep live/paper stake flat: --stake-per-level is final per-level notional
    )
    strategy = strategy_class(config=swan_config, clock=clock)

    # ── Storage ───────────────────────────────────────────────────────────────
    run_context = RunContext(run_id=run_id, strategy_id=strategy_id, mode=resolved_mode.value, config_hash=strategy_id)
    store = SQLiteRuntimeStore(db_path, run=run_context)
    runtime = StrategyRuntime(strategy=strategy, intent_store=store, clock=clock)

    # ── Execution client ──────────────────────────────────────────────────────
    execution_client: PolymarketExecutionClient | None = None
    if resolved_mode is ExecutionMode.LIVE:
        execution_client = PolymarketExecutionClient()
        execution_client.startup()
        reconcile_runtime_orders(runtime=runtime, execution_client=execution_client, execution_mode=resolved_mode)

    # ── Market event source ───────────────────────────────────────────────────
    _min_secs = int((min_hours_to_close if min_hours_to_close is not None else mode_config.min_hours_to_close) * 3600)
    _max_secs = int(max_hours_to_close * 3600) if max_hours_to_close is not None else int(mode_config.max_hours_to_close * 3600)
    universe_filter = UniverseFilter(allowed_assets=())  # empty = all assets
    duration_window = DurationWindow(
        min_seconds=_min_secs,
        max_seconds=_max_secs,
        require_recurrence=False,
        duration_metric="time_remaining",
        max_seconds_to_end=_max_secs,
    )

    # Price-monitoring WS: separate from LiveEventSource (which uses NoopWebsocket
    # for market-state events). This WS subscribes to the discovered universe and
    # fires the screener when best_ask crosses ≤ _WS_PRICE_THRESHOLD.
    price_monitor_ws = PolymarketWebsocket()

    # LiveEventSource still uses a noop WS — swan strategy ignores BookUpdates and
    # we don't want book frames doubling up in the main event log.
    class _NoopWebsocket:
        messages: asyncio.Queue = asyncio.Queue()
        async def connect(self) -> None: pass
        async def close(self) -> None: pass
        async def subscribe(self, token_ids) -> None: pass
        async def unsubscribe(self, token_ids) -> None: pass
        async def recv(self) -> str:
            await asyncio.sleep(3600)
            return ""

    if resolved_mode is ExecutionMode.LIVE:
        if execution_client is None:
            raise RuntimeError("execution_client required for live mode")
        credentials = execution_client.api_credentials()
        user_stream = PolymarketUserStream(auth=credentials)
        source = LiveEventSource(user_stream=user_stream, websocket=_NoopWebsocket())
    else:
        source = LiveEventSource(websocket=_NoopWebsocket())

    # Override market discovery with swan's wide universe.
    from short_horizon.venue_polymarket import MarketRefreshLoop, FeeMetadataRefreshLoop, SharedMarketDiscovery
    shared_discovery = SharedMarketDiscovery(
        universe_filter=universe_filter,
        duration_window=duration_window,
    )
    source.market_refresh = MarketRefreshLoop(
        discovery_fn=shared_discovery,
        universe_filter=universe_filter,
        duration_window=duration_window,
        max_rows=5_000,
        max_consecutive_failures=30,
        retry_backoff_max_seconds=300.0,
    )
    source.fee_refresh = FeeMetadataRefreshLoop(
        discovery_fn=shared_discovery,
        universe_filter=universe_filter,
        duration_window=duration_window,
        max_rows=5_000,
        fee_info_fetcher=None,
        max_consecutive_failures=30,
        retry_backoff_max_seconds=300.0,
    )

    # ── Old screener (reused for candidate scoring) ───────────────────────────
    bot_config = load_config()
    # Always override mode_config so the screener uses the selected strategy's
    # entry levels and price gates, not the default big_swan_mode from .env.
    effective_mc = mode_config
    _overrides: dict = {}
    if max_hours_to_close is not None:
        _overrides["max_hours_to_close"] = max_hours_to_close
    if min_hours_to_close is not None:
        _overrides["min_hours_to_close"] = min_hours_to_close
    if min_total_duration_hours is not None:
        _overrides["min_total_duration_hours"] = min_total_duration_hours
    if _overrides:
        from dataclasses import replace as _dc_replace
        effective_mc = _dc_replace(mode_config, **_overrides)
    screener_config = _ScreenerConfigOverride(bot_config, mode_config=effective_mc)
    # In black_swan mode, the analogy prior must be built from was_black_swan
    # winners (matches the stricter universe filter). Other modes keep the
    # broader label_20x prior. Issue #180 follow-up.
    market_scorer = MarketScorer(
        min_score=mode_config.min_market_score,
        use_black_swan_label=(strategy_name == "black_swan"),
    )
    # Phase E: pattern tracker is only active for black_swan strategy.
    # For swan (big_swan_mode) the wider universe makes pattern gating too
    # aggressive; black_swan already requires ≤5c + confirmed shock.
    pattern_tracker = None
    if strategy_name == "black_swan":
        from strategy.market_pattern_tracker import MarketPatternTracker
        pattern_tracker = MarketPatternTracker()
        logger.info("market_pattern_tracker_enabled", strategy=strategy_name)
    screener = Screener(
        config=screener_config,
        db_path=str(_DB_DIR / "swan_screener_log.sqlite3"),
        market_scorer=market_scorer,
        pattern_tracker=pattern_tracker,
    )
    # Per-level stake: default targets ~1 USDC per level, i.e. ~5 USDC
    # per full 5-level black_swan ladder before duration/phase multipliers.
    _stake_per_level = stake_per_level if stake_per_level is not None else _DEFAULT_STAKE_PER_LEVEL

    # ── Redeem sweeper ────────────────────────────────────────────────────────
    redeem_sweeper: PeriodicResolvedRedeemSweeper | None = None
    if resolved_mode is ExecutionMode.LIVE and redeem_interval_seconds is not None:
        assert execution_client is not None
        redeem_sweeper = PeriodicResolvedRedeemSweeper(
            interval_seconds=redeem_interval_seconds,
            run_id=run_id,
            execution_client=execution_client,
        )

    # ── Live submit guard ─────────────────────────────────────────────────────
    live_submit_guard = None
    if max_live_orders_total is not None:
        from short_horizon.live_runner import OperatorConfirmLiveOrderGuard
        live_submit_guard = OperatorConfirmLiveOrderGuard(max_live_orders_total=max_live_orders_total)

    shutdown = asyncio.Event()

    async def _run_event_stream() -> RunnerSummary:
        await source.start()
        if redeem_sweeper is not None:
            redeem_sweeper.maybe_run()
        try:
            return await drive_runtime_event_stream(
                events=source.events,
                runtime=runtime,
                logger_name="swan_live",
                completed_event_name="swan_live_run_completed",
                max_runtime_seconds=max_runtime_seconds,
                execution_mode=resolved_mode,
                execution_client=execution_client,
                live_submit_guard=live_submit_guard,
            )
        finally:
            shutdown.set()
            await source.stop()
            close = getattr(store, "close", None)
            if callable(close):
                close()

    await price_monitor_ws.connect()

    token_to_market_id: dict[str, str] = {}
    token_to_side_index: dict[str, int] = {}
    ws_candidate_cache = _WsCandidateCache()
    selector_observation_config = (
        _build_black_swan_universe_selector_config(
            max_markets=universe_selector_max_markets,
            max_tokens=universe_selector_max_tokens,
            max_markets_per_category=universe_selector_max_markets_per_category,
            min_volume_usdc=universe_selector_min_volume_usdc,
            reject_random_walk=universe_selector_reject_random_walk,
        )
        if strategy_name == "black_swan" and (resolved_mode is ExecutionMode.DRY_RUN or apply_universe_selector)
        else None
    )
    if selector_observation_config is not None:
        logger.info(
            "ws_universe_selector_configured",
            apply_selector=apply_universe_selector and strategy_name == "black_swan",
            observe_only=not (apply_universe_selector and strategy_name == "black_swan"),
            **asdict(selector_observation_config),
        )

    # Issue #190: black_swan price discovery comes from CLOB WS edge triggers.
    # Keep the old full-Gamma screener only for the legacy broad swan strategy.
    screener_task: asyncio.Task | None = None
    if strategy_name != "black_swan":
        screener_task = asyncio.create_task(
            _screener_loop(
                screener=screener,
                strategy=strategy,
                source=source,
                interval_seconds=_SCREENER_INTERVAL_SECONDS,
                clock=clock,
                logger=logger,
                shutdown=shutdown,
                stake_usdc_per_level=_stake_per_level,
                duration_stake_multipliers=(),  # keep live/paper stake flat: --stake-per-level is final per-level notional
            ),
            name="swan_screener_loop",
        )
    cleanup_task = asyncio.create_task(
        _cleanup_loop(
            source=source,
            clock=clock,
            interval_seconds=_CLEANUP_INTERVAL_SECONDS,
            shutdown=shutdown,
        ),
        name="swan_cleanup_loop",
    )
    ws_universe_task = asyncio.create_task(
        _ws_universe_builder_loop(
            ws=price_monitor_ws,
            shared_discovery=shared_discovery,
            token_to_market_id=token_to_market_id,
            token_to_side_index=token_to_side_index,
            duration_window=duration_window,
            universe_filter=universe_filter,
            logger=logger,
            shutdown=shutdown,
            selector_observation_config=selector_observation_config,
            apply_selector_plan=apply_universe_selector and strategy_name == "black_swan",
            protected_market_tokens_fn=lambda: _held_or_open_market_tokens(store),
            candidate_cache=ws_candidate_cache,
            strategy_for_cache_prune=strategy,
        ),
        name="swan_ws_universe_loop",
    )
    ws_monitor_task = asyncio.create_task(
        _ws_price_monitor_loop(
            ws=price_monitor_ws,
            screener=screener,
            strategy=strategy,
            source=source,
            clock=clock,
            stake_usdc_per_level=_stake_per_level,
            duration_stake_multipliers=(),  # keep live/paper stake flat: --stake-per-level is final per-level notional
            price_threshold=mode_config.entry_price_max,
            token_to_market_id=token_to_market_id,
            token_to_side_index=token_to_side_index,
            candidate_cache=ws_candidate_cache,
            logger=logger,
            shutdown=shutdown,
            forward_market_events=(resolved_mode is ExecutionMode.DRY_RUN),
        ),
        name="swan_ws_monitor_loop",
    )
    held_book_refresh_task = asyncio.create_task(
        _held_book_snapshot_refresh_loop(
            runtime=runtime,
            source=source,
            clock=clock,
            interval_seconds=_HELD_BOOK_REFRESH_INTERVAL_SECONDS,
            stale_after_ms=_HELD_BOOK_STALE_AFTER_MS,
            max_tokens_per_pass=_HELD_BOOK_REFRESH_MAX_TOKENS_PER_PASS,
            logger=logger,
            shutdown=shutdown,
        ),
        name="swan_held_book_snapshot_refresh_loop",
    )

    try:
        summary = await _run_event_stream()
    finally:
        tasks = [cleanup_task, ws_universe_task, ws_monitor_task, held_book_refresh_task]
        if screener_task is not None:
            tasks.append(screener_task)
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        await price_monitor_ws.close()

    logger.info("swan_live_finished", run_id=run_id, events=summary.event_count)
    return summary


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Swan V2 live runner")
    p.add_argument(
        "--strategy",
        default="swan",
        choices=list(_STRATEGY_REGISTRY),
        help="Trading strategy: 'swan' (big_swan_mode) or 'black_swan' (black_swan_mode)",
    )
    p.add_argument("--execution-mode", default="dry_run", choices=["dry_run", "live"],
                   help="dry_run = log only, live = submit real orders")
    p.add_argument("--db-path", default=None, help="SQLite DB path for run storage")
    p.add_argument("--run-id", default=None)
    p.add_argument("--max-runtime-seconds", type=float, default=None)
    p.add_argument("--max-live-orders-total", type=int, default=None,
                   help="Hard cap on total live orders submitted this run")
    p.add_argument("--redeem-interval", type=float, default=None,
                   dest="redeem_interval_seconds",
                   help="Seconds between resolved-position redeem sweeps (live mode only)")
    p.add_argument("--stake-per-level", type=float, default=None,
                   dest="stake_per_level",
                   help="USDC notional per entry level (default: 1.0)")
    p.add_argument("--max-hours-to-close", type=float, default=None,
                   dest="max_hours_to_close",
                   help="Override max hours to close from mode config")
    p.add_argument("--min-hours-to-close", type=float, default=None,
                   dest="min_hours_to_close",
                   help="Override min hours to close from mode config")
    p.add_argument("--min-total-duration-hours", type=float, default=None,
                   dest="min_total_duration_hours",
                   help="Override min total market lifespan (hours) from mode config")
    p.add_argument("--apply-universe-selector", action="store_true",
                   help="Use the black_swan universe selector to narrow CLOB WS subscriptions (default: observe/log only in dry-run)")
    p.add_argument("--universe-selector-max-markets", type=int, default=None,
                   help="Override black_swan selector max markets before WS subscription (0 = unlimited)")
    p.add_argument("--universe-selector-max-tokens", type=int, default=None,
                   help="Override black_swan selector max tokens before WS subscription (0 = unlimited)")
    p.add_argument("--universe-selector-max-markets-per-category", type=int, default=None,
                   help="Override black_swan selector per-category market cap (0 = unlimited)")
    p.add_argument("--universe-selector-min-volume-usdc", type=float, default=None,
                   help="Override black_swan selector minimum Gamma volume in USDC")
    p.add_argument("--universe-selector-reject-random-walk", action="store_true",
                   help="Hard reject cheap random-walk price markets in the black_swan selector")
    return p


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    asyncio.run(
        run_swan_live(
            strategy_name=args.strategy,
            db_path=args.db_path,
            run_id=args.run_id,
            execution_mode=args.execution_mode,
            max_runtime_seconds=args.max_runtime_seconds,
            max_live_orders_total=args.max_live_orders_total,
            redeem_interval_seconds=args.redeem_interval_seconds,
            stake_per_level=args.stake_per_level,
            max_hours_to_close=args.max_hours_to_close,
            min_hours_to_close=args.min_hours_to_close,
            min_total_duration_hours=args.min_total_duration_hours,
            apply_universe_selector=args.apply_universe_selector,
            universe_selector_max_markets=args.universe_selector_max_markets,
            universe_selector_max_tokens=args.universe_selector_max_tokens,
            universe_selector_max_markets_per_category=args.universe_selector_max_markets_per_category,
            universe_selector_min_volume_usdc=args.universe_selector_min_volume_usdc,
            universe_selector_reject_random_walk=args.universe_selector_reject_random_walk,
        )
    )
