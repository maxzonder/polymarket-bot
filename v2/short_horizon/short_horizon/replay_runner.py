from __future__ import annotations

import argparse
import uuid
from pathlib import Path

from .config import ShortHorizonConfig
from .core.runtime import StrategyRuntime
from .replay import ReplayEventSource
from .runner import RunnerSummary, drive_runtime_events
from .storage import RunContext, SQLiteRuntimeStore
from .strategies import ShortHorizon15mTouchStrategy
from .telemetry import configure_logging, get_logger


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


def replay_file(*, event_log_path: str | Path, db_path: str | Path, run_id: str | None = None, config: ShortHorizonConfig | None = None, config_hash: str = "dev") -> RunnerSummary:
    runtime = build_replay_runtime(db_path=db_path, run_id=run_id, config=config, config_hash=config_hash)
    try:
        events = ReplayEventSource(event_log_path).load()
        return drive_runtime_events(
            events=events,
            runtime=runtime,
            logger_name="short_horizon.replay_runner",
            completed_event_name="replay_run_completed",
        )
    finally:
        store = runtime.store
        close = getattr(store, "close", None)
        if callable(close):
            close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay normalized short-horizon events into a fresh SQLite run")
    parser.add_argument("event_log_path", help="Path to a JSONL file of normalized events")
    parser.add_argument("db_path", help="SQLite DB path for replay outputs")
    parser.add_argument("--run-id", default=None, help="Optional explicit run_id; defaults to a fresh replay_<ts>_<suffix>")
    parser.add_argument("--config-hash", default="dev", help="Config hash label stored in runs table")
    return parser


def generate_run_id() -> str:
    return f"replay_{uuid.uuid4().hex[:12]}"


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
