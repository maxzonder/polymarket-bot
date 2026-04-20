from __future__ import annotations

import argparse
import asyncio
import uuid
from pathlib import Path

from .config import ShortHorizonConfig
from .core.runtime import StrategyRuntime
from .market_data import LiveEventSource, MarketDataSource
from .runner import RunnerSummary, drive_runtime_event_stream, drive_runtime_events
from .storage import RunContext, SQLiteRuntimeStore
from .strategies import ShortHorizon15mTouchStrategy
from .telemetry import configure_logging, get_logger


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


def run_stub_live(*, stub_event_log_path: str | Path, db_path: str | Path, run_id: str | None = None, config: ShortHorizonConfig | None = None, config_hash: str = "dev") -> RunnerSummary:
    runtime = build_live_runtime(db_path=db_path, run_id=run_id, config=config, config_hash=config_hash)
    try:
        source = MarketDataSource.from_jsonl(stub_event_log_path)
        return drive_runtime_events(
            events=source.load(),
            runtime=runtime,
            logger_name="short_horizon.live_runner",
            completed_event_name="live_stub_run_completed",
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
) -> RunnerSummary:
    runtime = build_live_runtime(db_path=db_path, run_id=run_id, config=config, config_hash=config_hash)
    source = source or LiveEventSource()
    try:
        await source.start()
        return await drive_runtime_event_stream(
            events=source.events,
            runtime=runtime,
            logger_name="short_horizon.live_runner",
            completed_event_name="live_run_completed",
            max_events=max_events,
        )
    finally:
        await source.stop()
        store = runtime.store
        close = getattr(store, "close", None)
        if callable(close):
            close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the short-horizon live shell in stub or real live mode")
    parser.add_argument("db_path", help="SQLite DB path for live-shell outputs")
    parser.add_argument("--mode", choices=("stub", "live"), default="stub", help="Input mode for the live runner")
    parser.add_argument("--stub-event-log-path", default=None, help="Path to a JSONL file of normalized stub events for --mode stub")
    parser.add_argument("--run-id", default=None, help="Optional explicit run_id; defaults to a fresh live_<suffix>")
    parser.add_argument("--config-hash", default="dev", help="Config hash label stored in runs table")
    parser.add_argument("--max-events", type=int, default=None, help="Optional cap on processed live events, useful for smoke tests")
    return parser


def generate_run_id() -> str:
    return f"live_{uuid.uuid4().hex[:12]}"


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging()
    if args.mode == "stub":
        if not args.stub_event_log_path:
            parser.error("--stub-event-log-path is required when --mode stub")
        summary = run_stub_live(
            stub_event_log_path=args.stub_event_log_path,
            db_path=args.db_path,
            run_id=args.run_id,
            config_hash=args.config_hash,
        )
    else:
        summary = asyncio.run(
            run_live(
                db_path=args.db_path,
                run_id=args.run_id,
                config_hash=args.config_hash,
                max_events=args.max_events,
            )
        )
    logger = get_logger("short_horizon.live_runner", run_id=summary.run_id)
    logger.info(
        "live_runner_completed",
        run_id=summary.run_id,
        input_events=summary.event_count,
        order_intents=summary.order_intents,
        synthetic_order_events=summary.synthetic_order_events,
        db_path=str(summary.db_path),
    )


if __name__ == "__main__":
    main()
