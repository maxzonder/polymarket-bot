from __future__ import annotations

from .config import ShortHorizonConfig
from .core.runtime import StrategyRuntime
from .market_data import MarketDataSource
from .storage import InMemoryIntentStore
from .strategies import ShortHorizon15mTouchStrategy
from .telemetry import configure_logging, get_logger
from .venue_polymarket import PolymarketVenueAdapter


def build_live_runtime() -> StrategyRuntime:
    config = ShortHorizonConfig()
    strategy = ShortHorizon15mTouchStrategy(config=config)
    return StrategyRuntime(strategy=strategy, intent_store=InMemoryIntentStore())


def main() -> None:
    configure_logging()
    logger = get_logger("short_horizon.live_runner")
    _ = MarketDataSource()
    _ = PolymarketVenueAdapter()
    runtime = build_live_runtime()
    logger.info(
        "live_runner_booted",
        run_id=runtime.store.current_run_id,
        strategy=runtime.strategy.__class__.__name__,
    )


if __name__ == "__main__":
    main()
