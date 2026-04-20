from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass
from pathlib import Path

from .config import ShortHorizonConfig
from .core.events import BookUpdate, MarketStateUpdate, OrderAccepted, OrderCanceled, OrderFilled, OrderRejected, TimerEvent, TradeTick
from .core.models import OrderIntent, SkipDecision
from .core.runtime import StrategyRuntime
from .execution import ExecutionEngine
from .replay import ReplayEventSource
from .storage import RunContext, SQLiteRuntimeStore
from .strategies import ShortHorizon15mTouchStrategy
from .strategy_api import CancelOrder, Noop, PlaceOrder, StrategyIntent
from .telemetry import configure_logging, get_logger


@dataclass(frozen=True)
class ReplaySummary:
    run_id: str
    event_count: int
    order_intents: int
    synthetic_order_events: int
    db_path: Path


def build_replay_runtime(*, db_path: str | Path, run_id: str | None = None, config: ShortHorizonConfig | None = None, config_hash: str = "dev") -> StrategyRuntime:
    config = config or ShortHorizonConfig()
    run_context = RunContext(
        run_id=run_id or generate_run_id(),
        strategy_id=config.strategy_id,
        mode="replay",
        config_hash=config_hash,
    )
    store = SQLiteRuntimeStore(db_path, run=run_context)
    strategy = ShortHorizon15mTouchStrategy(config=config)
    return StrategyRuntime(strategy=strategy, intent_store=store)


def replay_file(*, event_log_path: str | Path, db_path: str | Path, run_id: str | None = None, config: ShortHorizonConfig | None = None, config_hash: str = "dev") -> ReplaySummary:
    runtime = build_replay_runtime(db_path=db_path, run_id=run_id, config=config, config_hash=config_hash)
    try:
        events = ReplayEventSource(event_log_path).load()
        return replay_events(events=events, runtime=runtime)
    finally:
        store = runtime.store
        close = getattr(store, "close", None)
        if callable(close):
            close()


def replay_events(*, events: list, runtime: StrategyRuntime) -> ReplaySummary:
    logger = get_logger("short_horizon.replay_runner", run_id=runtime.store.current_run_id)
    execution = ExecutionEngine(store=runtime.store)
    event_count = 0
    order_intents = 0
    synthetic_order_events = 0

    for event in events:
        event_count += 1
        if isinstance(event, MarketStateUpdate):
            runtime.on_market_state(event)
            continue

        if isinstance(event, BookUpdate):
            outputs = runtime.on_book_update(event)
            for output in outputs:
                if isinstance(output, OrderIntent):
                    order_intents += 1
                    synthetic_order_events += len(execution.submit(output, event_time_ms=event.event_time_ms))
            continue

        runtime.store.append_event(event)

        if isinstance(event, TradeTick):
            synthetic_order_events += _apply_strategy_intents(
                runtime.strategy.on_market_event(event),
                execution=execution,
                fallback_event_time_ms=event.event_time_ms,
            )
            continue

        if isinstance(event, TimerEvent):
            synthetic_order_events += _apply_strategy_intents(
                runtime.strategy.on_timer(event),
                execution=execution,
                fallback_event_time_ms=event.event_time_ms,
            )
            continue

        if isinstance(event, (OrderAccepted, OrderRejected, OrderFilled, OrderCanceled)):
            synthetic_order_events += _apply_strategy_intents(
                runtime.strategy.on_order_event(event),
                execution=execution,
                fallback_event_time_ms=event.event_time_ms,
            )
            continue

        raise TypeError(f"Unsupported replay event: {type(event)!r}")

    logger.info(
        "replay_run_completed",
        run_id=runtime.store.current_run_id,
        input_events=event_count,
        order_intents=order_intents,
        synthetic_order_events=synthetic_order_events,
    )
    store_path = Path(getattr(runtime.store, "path", Path("<memory>")))
    return ReplaySummary(
        run_id=runtime.store.current_run_id,
        event_count=event_count,
        order_intents=order_intents,
        synthetic_order_events=synthetic_order_events,
        db_path=store_path,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay normalized short-horizon events into a fresh SQLite run")
    parser.add_argument("event_log_path", help="Path to a JSONL file of normalized events")
    parser.add_argument("db_path", help="SQLite DB path for replay outputs")
    parser.add_argument("--run-id", default=None, help="Optional explicit run_id; defaults to a fresh replay_<ts>_<suffix>")
    parser.add_argument("--config-hash", default="dev", help="Config hash label stored in runs table")
    return parser


def generate_run_id() -> str:
    return f"replay_{uuid.uuid4().hex[:12]}"


def _apply_strategy_intents(intents: list[StrategyIntent], *, execution: ExecutionEngine, fallback_event_time_ms: int) -> int:
    synthetic_events = 0
    for intent in intents:
        if isinstance(intent, PlaceOrder):
            synthetic_events += len(execution.handle_intent(intent, event_time_ms=fallback_event_time_ms))
        elif isinstance(intent, CancelOrder):
            synthetic_events += len(execution.handle_intent(intent, event_time_ms=fallback_event_time_ms))
        elif isinstance(intent, Noop):
            continue
        else:
            raise TypeError(f"Unsupported strategy intent: {type(intent)!r}")
    return synthetic_events


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging()
    summary = replay_file(
        event_log_path=args.event_log_path,
        db_path=args.db_path,
        run_id=args.run_id,
        config_hash=args.config_hash,
    )
    logger = get_logger("short_horizon.replay_runner", run_id=summary.run_id)
    logger.info(
        "replay_runner_completed",
        run_id=summary.run_id,
        input_events=summary.event_count,
        order_intents=summary.order_intents,
        synthetic_order_events=summary.synthetic_order_events,
        db_path=str(summary.db_path),
    )


if __name__ == "__main__":
    main()
