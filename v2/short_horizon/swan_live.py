"""
Swan V2 Live Runner
===================

Replaces bot/main_loop.py for production trading, using the V2 execution path
(pUSD + V2 CTF Exchange spender) instead of the deprecated V1 CLOB client.

Old bot/main_loop.py remains as a backtest harness; this file owns all live
order submission going forward.

Usage:
    # dry-run (default)
    python -m v2.short_horizon.swan_live

    # live
    python -m v2.short_horizon.swan_live --execution-mode live

    # live, with 1-hour resolved redeem sweep
    python -m v2.short_horizon.swan_live --execution-mode live --redeem-interval 3600
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from pathlib import Path

# Ensure repo root is on path so strategy/ and api/ imports resolve.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

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
from short_horizon.telemetry import configure_logging, get_logger
from short_horizon.venue_polymarket import PolymarketUserStream
from short_horizon.venue_polymarket.execution_client import (
    PRIVATE_KEY_ENV_VAR,
    PolymarketExecutionClient,
)
from short_horizon.venue_polymarket.markets import DurationWindow, UniverseFilter

# Import old swan screener (reused as-is for market discovery).
from config import BIG_SWAN_MODE, BotConfig, load_config
from execution.order_manager import POSITIONS_DB
from strategy.market_scorer import MarketScorer
from strategy.screener import EntryCandidate, Screener

_DB_DIR = Path(os.environ.get("POLYBOT_DATA_DIR", Path.home() / ".polybot" / "swan_v2"))
_DEFAULT_DB = _DB_DIR / "swan_v2_live.sqlite3"

_SCREENER_INTERVAL_SECONDS = 300   # 5 min
_CLEANUP_INTERVAL_SECONDS = 1800   # 30 min


def _entry_candidate_to_swan(
    candidate: EntryCandidate,
    *,
    stake_usdc_per_level: float,
) -> SwanCandidate:
    mi = candidate.market_info
    return SwanCandidate(
        market_id=mi.market_id,
        condition_id=mi.condition_id,
        token_id=candidate.token_id,
        question=mi.question or "",
        asset_slug=getattr(mi, "asset_slug", None) or "",
        entry_levels=tuple(sorted(set(candidate.suggested_entry_levels))),
        notional_usdc_per_level=stake_usdc_per_level,
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
) -> None:
    """Runs the swan screener periodically and injects a timer event each time."""
    while not shutdown.is_set():
        try:
            raw_candidates = await asyncio.to_thread(screener.scan)
            swan_candidates = [
                _entry_candidate_to_swan(c, stake_usdc_per_level=stake_usdc_per_level)
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


async def run_swan_live(
    *,
    db_path: Path | str | None = None,
    run_id: str | None = None,
    execution_mode: ExecutionMode | str = ExecutionMode.DRY_RUN,
    max_runtime_seconds: float | None = None,
    max_live_orders_total: int | None = None,
    redeem_interval_seconds: float | None = None,
) -> RunnerSummary:
    resolved_mode = ExecutionMode(str(execution_mode))
    db_path = Path(db_path or _DEFAULT_DB)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    run_id = run_id or f"swan_v2_{uuid.uuid4().hex[:12]}"

    configure_logging()
    logger = get_logger("swan_live", run_id=run_id)
    logger.info("swan_live_starting", execution_mode=resolved_mode.value, db_path=str(db_path))

    clock = SystemClock()

    # ── Strategy ──────────────────────────────────────────────────────────────
    swan_config = SwanConfig(
        strategy_id="swan_v1",
        max_open_resting_bids=BIG_SWAN_MODE.max_open_positions,
        max_resting_markets=BIG_SWAN_MODE.max_resting_markets,
        max_resting_per_cluster=BIG_SWAN_MODE.max_resting_per_cluster,
        stale_order_ttl_seconds=float(BIG_SWAN_MODE.max_hours_to_close * 3600),
    )
    strategy = SwanStrategyV1(config=swan_config, clock=clock)

    # ── Storage ───────────────────────────────────────────────────────────────
    run_context = RunContext(run_id=run_id, strategy_id="swan_v1", mode=resolved_mode.value, config_hash="swan_v1")
    store = SQLiteRuntimeStore(db_path, run=run_context)
    runtime = StrategyRuntime(strategy=strategy, intent_store=store, clock=clock)

    # ── Execution client ──────────────────────────────────────────────────────
    execution_client: PolymarketExecutionClient | None = None
    if resolved_mode is ExecutionMode.LIVE:
        execution_client = PolymarketExecutionClient()
        execution_client.startup()
        reconcile_runtime_orders(runtime=runtime, execution_client=execution_client, execution_mode=resolved_mode)

    # ── Market event source (wide universe: all categories, 15m–7d) ───────────
    universe_filter = UniverseFilter(allowed_assets=())  # empty = all assets
    duration_window = DurationWindow(
        min_seconds=900,                # 15 min minimum remaining
        max_seconds=7 * 24 * 3600,     # 7 day maximum remaining
        require_recurrence=False,
        duration_metric="time_remaining",
        max_seconds_to_end=7 * 24 * 3600,
    )

    if resolved_mode is ExecutionMode.LIVE:
        if execution_client is None:
            raise RuntimeError("execution_client required for live mode")
        credentials = execution_client.api_credentials()
        user_stream = PolymarketUserStream(auth=credentials)
        source = LiveEventSource(user_stream=user_stream)
    else:
        source = LiveEventSource()

    # Override market discovery with swan's wide universe.
    from short_horizon.venue_polymarket import MarketRefreshLoop, FeeMetadataRefreshLoop, SharedMarketDiscovery
    shared_discovery = SharedMarketDiscovery(
        universe_filter=universe_filter,
        duration_window=duration_window,
    )
    source.market_refresh = MarketRefreshLoop(discovery_fn=shared_discovery, max_rows=50_000)
    source.fee_refresh = FeeMetadataRefreshLoop(
        discovery_fn=shared_discovery,
        max_rows=50_000,
        fee_info_fetcher=None,
    )

    # ── Old screener (reused for candidate scoring) ───────────────────────────
    bot_config = load_config()
    market_scorer = MarketScorer(min_score=BIG_SWAN_MODE.min_market_score)
    screener = Screener(
        config=bot_config,
        db_path=str(_DB_DIR / "swan_screener_log.sqlite3"),
        market_scorer=market_scorer,
    )
    # Per-level stake: the screener resolves this from market_score_tiers; use
    # a safe default here and let the screener's suggested_entry_levels guide levels.
    _stake_per_level = BIG_SWAN_MODE.stake_usdc

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

    try:
        summary = await _run_event_stream()
    finally:
        screener_task.cancel()
        cleanup_task.cancel()
        for t in (screener_task, cleanup_task):
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    logger.info("swan_live_finished", run_id=run_id, events=summary.event_count)
    return summary


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Swan V2 live runner")
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
    return p


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    asyncio.run(
        run_swan_live(
            db_path=args.db_path,
            run_id=args.run_id,
            execution_mode=args.execution_mode,
            max_runtime_seconds=args.max_runtime_seconds,
            max_live_orders_total=args.max_live_orders_total,
            redeem_interval_seconds=args.redeem_interval_seconds,
        )
    )
