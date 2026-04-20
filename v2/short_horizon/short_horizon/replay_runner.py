from __future__ import annotations

from .config import ShortHorizonConfig
from .core.runtime import StrategyRuntime
from .replay import ReplayEventSource
from .storage import InMemoryIntentStore
from .strategies import ShortHorizon15mTouchStrategy
from .telemetry import get_logger


def build_replay_runtime() -> StrategyRuntime:
    config = ShortHorizonConfig()
    strategy = ShortHorizon15mTouchStrategy(config=config)
    return StrategyRuntime(strategy=strategy, intent_store=InMemoryIntentStore())


def main() -> None:
    logger = get_logger("short_horizon.replay_runner")
    _ = ReplayEventSource()
    runtime = build_replay_runtime()
    logger.info("replay_runner skeleton booted with strategy=%s", runtime.strategy.__class__.__name__)


if __name__ == "__main__":
    main()
