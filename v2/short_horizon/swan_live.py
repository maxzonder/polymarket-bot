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
from dataclasses import replace as dataclass_replace
from pathlib import Path

# Ensure repo root and v2/short_horizon/ are on path so all imports resolve.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_V2_SH_DIR = Path(__file__).resolve().parent  # v2/short_horizon/ → exposes short_horizon pkg
for _p in (_REPO_ROOT, _V2_SH_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from short_horizon.core.clock import SystemClock
from short_horizon.core.events import TimerEvent
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
from short_horizon.venue_polymarket import PolymarketUserStream
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
_DEFAULT_STAKE_PER_LEVEL = 5.0     # floor above 1.0 USDC venue minimum

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
            tokens: set[str] = set()
            next_token_to_market_id: dict[str, str] = {}
            next_token_to_side_index: dict[str, int] = {}
            for m in markets:
                if m.token_yes_id:
                    tokens.add(m.token_yes_id)
                    next_token_to_market_id[m.token_yes_id] = m.market_id
                    next_token_to_side_index[m.token_yes_id] = 0
                if m.token_no_id:
                    tokens.add(m.token_no_id)
                    next_token_to_market_id[m.token_no_id] = m.market_id
                    next_token_to_side_index[m.token_no_id] = 1

            token_to_market_id.clear()
            token_to_market_id.update(next_token_to_market_id)
            token_to_side_index.clear()
            token_to_side_index.update(next_token_to_side_index)

            to_add = tokens - subscribed
            to_remove = subscribed - tokens
            if to_add:
                await ws.subscribe(list(to_add))
            if to_remove:
                await ws.unsubscribe(list(to_remove))
            subscribed = tokens
            logger.info(
                "ws_universe_updated",
                markets=len(markets),
                tokens=len(tokens),
                added=len(to_add),
                removed=len(to_remove),
            )
        except Exception:
            logger.exception("ws_universe_builder_error")

        try:
            await asyncio.wait_for(shutdown.wait(), timeout=_WS_UNIVERSE_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


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
            for event in [*updates, *trade_updates]:
                source.inject(event)

        for update in updates:
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
                    ),
                    name=f"ws_trigger_{token_id[:12]}",
                )
            elif not is_below and was_below:
                below_threshold.discard(token_id)


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
        logger.info(
            "ws_price_trigger_no_candidates",
            token_id=token_id,
            ask=ask,
            market_id=market_id,
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
        phase_stake_multipliers=mode_config.phase_stake_multipliers,
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
    # Per-level stake: venue minimum is 1.0 USDC notional; BIG_SWAN_MODE.stake_usdc
    # (0.05) is below that threshold so we use a configurable floor.
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
                duration_stake_multipliers=mode_config.duration_stake_multipliers,
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
            duration_stake_multipliers=mode_config.duration_stake_multipliers,
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

    try:
        summary = await _run_event_stream()
    finally:
        tasks = [cleanup_task, ws_universe_task, ws_monitor_task]
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
                   help="USDC notional per entry level (default: 5.0)")
    p.add_argument("--max-hours-to-close", type=float, default=None,
                   dest="max_hours_to_close",
                   help="Override max hours to close from mode config")
    p.add_argument("--min-hours-to-close", type=float, default=None,
                   dest="min_hours_to_close",
                   help="Override min hours to close from mode config")
    p.add_argument("--min-total-duration-hours", type=float, default=None,
                   dest="min_total_duration_hours",
                   help="Override min total market lifespan (hours) from mode config")
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
        )
    )
