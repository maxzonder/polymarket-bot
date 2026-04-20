from __future__ import annotations

import argparse
import asyncio
import os
import uuid
from pathlib import Path

from .config import ShortHorizonConfig
from .core.runtime import StrategyRuntime
from .execution import ExecutionMode
from .market_data import LiveEventSource, MarketDataSource
from .probe import assert_min_book_updates_per_minute, cross_validate_probe_against_collector, summarize_probe_db
from .runner import RunnerSummary, drive_runtime_event_stream, drive_runtime_events
from .storage import RunContext, SQLiteRuntimeStore
from .strategies import ShortHorizon15mTouchStrategy
from .telemetry import configure_logging, get_logger
from .venue_polymarket import PolymarketUserStream
from .venue_polymarket.execution_client import PRIVATE_KEY_ENV_VAR, PolymarketExecutionClient


def build_live_runtime(*, db_path: str | Path, run_id: str | None = None, config: ShortHorizonConfig | None = None, config_hash: str = "dev") -> StrategyRuntime:
    config = config or ShortHorizonConfig()
    run_context = RunContext(
        run_id=run_id or generate_run_id(),
        strategy_id=config.strategy_id,
        mode="live",
        config_hash=config_hash,
    )
    store = SQLiteRuntimeStore(db_path, run=run_context)
    strategy = ShortHorizon15mTouchStrategy(config=config)
    return StrategyRuntime(strategy=strategy, intent_store=store)


def run_stub_live(
    *,
    stub_event_log_path: str | Path,
    db_path: str | Path,
    run_id: str | None = None,
    config: ShortHorizonConfig | None = None,
    config_hash: str = "dev",
    execution_mode: ExecutionMode | str = ExecutionMode.SYNTHETIC,
) -> RunnerSummary:
    resolved_mode = ExecutionMode(str(execution_mode))
    if resolved_mode is ExecutionMode.LIVE:
        raise ValueError("stub mode does not support execution_mode=live")
    runtime = build_live_runtime(db_path=db_path, run_id=run_id, config=config, config_hash=config_hash)
    try:
        source = MarketDataSource.from_jsonl(stub_event_log_path)
        return drive_runtime_events(
            events=source.load(),
            runtime=runtime,
            logger_name="short_horizon.live_runner",
            completed_event_name="live_stub_run_completed",
            execution_mode=resolved_mode,
        )
    finally:
        store = runtime.store
        close = getattr(store, "close", None)
        if callable(close):
            close()


async def run_live(
    *,
    db_path: str | Path,
    run_id: str | None = None,
    config: ShortHorizonConfig | None = None,
    config_hash: str = "dev",
    source: LiveEventSource | None = None,
    max_events: int | None = None,
    max_runtime_seconds: float | None = None,
    execution_mode: ExecutionMode | str = ExecutionMode.SYNTHETIC,
    execution_client: PolymarketExecutionClient | None = None,
) -> RunnerSummary:
    resolved_mode = ExecutionMode(str(execution_mode))
    runtime = build_live_runtime(db_path=db_path, run_id=run_id, config=config, config_hash=config_hash)
    client = execution_client
    if resolved_mode is ExecutionMode.LIVE:
        client = client or PolymarketExecutionClient()
        client.startup()
    source = source or build_live_source(execution_mode=resolved_mode, execution_client=client)
    try:
        await source.start()
        return await drive_runtime_event_stream(
            events=source.events,
            runtime=runtime,
            logger_name="short_horizon.live_runner",
            completed_event_name="live_run_completed",
            max_events=max_events,
            max_runtime_seconds=max_runtime_seconds,
            execution_mode=resolved_mode,
            execution_client=client,
        )
    finally:
        await source.stop()
        store = runtime.store
        close = getattr(store, "close", None)
        if callable(close):
            close()


def build_live_source(
    *,
    execution_mode: ExecutionMode | str = ExecutionMode.SYNTHETIC,
    execution_client: PolymarketExecutionClient | None = None,
) -> LiveEventSource:
    resolved_mode = ExecutionMode(str(execution_mode))
    if resolved_mode is not ExecutionMode.LIVE:
        return LiveEventSource()
    if execution_client is None:
        raise ValueError("execution_client is required for live execution mode")
    credentials = getattr(execution_client, "api_credentials", None)
    if not callable(credentials):
        raise TypeError("live execution mode requires execution_client.api_credentials() for authenticated user stream")
    return LiveEventSource(user_stream=PolymarketUserStream(auth=credentials()))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the short-horizon live shell in stub or real live mode")
    parser.add_argument("db_path", help="SQLite DB path for live-shell outputs")
    parser.add_argument("--mode", choices=("stub", "live"), default="stub", help="Input mode for the live runner")
    parser.add_argument(
        "--execution-mode",
        choices=tuple(mode.value for mode in ExecutionMode),
        default=ExecutionMode.SYNTHETIC.value,
        help="Execution mode: synthetic preserves Phase 2 behavior, dry_run translates orders without sending, live sends real orders",
    )
    parser.add_argument("--stub-event-log-path", default=None, help="Path to a JSONL file of normalized stub events for --mode stub")
    parser.add_argument("--run-id", default=None, help="Optional explicit run_id; defaults to a fresh live_<suffix>")
    parser.add_argument("--config-hash", default="dev", help="Config hash label stored in runs table")
    parser.add_argument("--max-events", type=int, default=None, help="Optional cap on processed live events, useful for smoke tests")
    parser.add_argument("--max-runtime-seconds", type=float, default=None, help="Optional wall-clock cap for live probes")
    parser.add_argument("--collector-csv", default=None, help="Optional collector CSV path for post-run cross-validation")
    parser.add_argument(
        "--min-book-updates-per-minute",
        type=float,
        default=1.0,
        help="Fail the live probe if observed BookUpdate throughput falls below this rate over the probe window; set 0 to disable",
    )
    return parser


def generate_run_id() -> str:
    return f"live_{uuid.uuid4().hex[:12]}"


def validate_cli_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> ExecutionMode:
    execution_mode = ExecutionMode(str(args.execution_mode))
    if args.mode == "stub" and execution_mode is ExecutionMode.LIVE:
        parser.error("--execution-mode live requires --mode live")
    if args.mode == "live" and execution_mode is ExecutionMode.LIVE and not os.getenv(PRIVATE_KEY_ENV_VAR):
        parser.error(f"{PRIVATE_KEY_ENV_VAR} is required when --execution-mode live")
    return execution_mode


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    execution_mode = validate_cli_args(parser, args)
    configure_logging()
    logger = get_logger("short_horizon.live_runner", run_id=args.run_id)
    logger.info(
        "live_runner_starting",
        input_mode=args.mode,
        execution_mode=execution_mode.value,
        db_path=str(args.db_path),
        max_events=args.max_events,
        max_runtime_seconds=args.max_runtime_seconds,
    )
    if args.mode == "stub":
        if not args.stub_event_log_path:
            parser.error("--stub-event-log-path is required when --mode stub")
        summary = run_stub_live(
            stub_event_log_path=args.stub_event_log_path,
            db_path=args.db_path,
            run_id=args.run_id,
            config_hash=args.config_hash,
            execution_mode=execution_mode,
        )
    else:
        summary = asyncio.run(
            run_live(
                db_path=args.db_path,
                run_id=args.run_id,
                config_hash=args.config_hash,
                max_events=args.max_events,
                max_runtime_seconds=args.max_runtime_seconds,
                execution_mode=execution_mode,
            )
        )
    logger = get_logger("short_horizon.live_runner", run_id=summary.run_id)
    probe_summary = summarize_probe_db(args.db_path, run_id=summary.run_id)
    logger.info(
        "live_runner_completed",
        run_id=summary.run_id,
        input_events=summary.event_count,
        order_intents=summary.order_intents,
        synthetic_order_events=summary.synthetic_order_events,
        execution_mode=execution_mode.value,
        db_path=str(summary.db_path),
    )
    logger.info(
        "live_probe_summary",
        run_id=probe_summary.run_id,
        total_events=probe_summary.total_events,
        market_state_updates=probe_summary.market_state_updates,
        book_updates=probe_summary.book_updates,
        trade_ticks=probe_summary.trade_ticks,
        order_events=probe_summary.order_events,
        distinct_markets=probe_summary.distinct_markets,
        distinct_tokens=probe_summary.distinct_tokens,
        first_event_time=probe_summary.first_event_time,
        last_event_time=probe_summary.last_event_time,
        window_minutes=probe_summary.window_minutes,
        book_updates_per_minute=probe_summary.book_updates_per_minute,
    )
    if args.min_book_updates_per_minute and args.min_book_updates_per_minute > 0:
        assert_min_book_updates_per_minute(
            probe_summary,
            min_rate=args.min_book_updates_per_minute,
        )
    if args.collector_csv:
        validation = cross_validate_probe_against_collector(
            args.db_path,
            run_id=summary.run_id,
            collector_csv_path=args.collector_csv,
        )
        logger.info(
            "live_probe_cross_validation",
            run_id=validation.run_id,
            collector_rows=validation.collector_rows,
            probe_markets=validation.probe_markets,
            collector_markets=validation.collector_markets,
            overlapping_markets=validation.overlapping_markets,
            probe_tokens=validation.probe_tokens,
            collector_tokens=validation.collector_tokens,
            overlapping_tokens=validation.overlapping_tokens,
        )


if __name__ == "__main__":
    main()
