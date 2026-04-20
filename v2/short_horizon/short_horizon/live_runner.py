from __future__ import annotations

from .config import ShortHorizonConfig
from .core.runtime import StrategyRuntime
from .market_data import MarketDataSource
from .storage import InMemoryIntentStore
from .strategies import ShortHorizon15mTouchStrategy
from .telemetry import get_logger
from .venue_polymarket import PolymarketVenueAdapter


def build_live_runtime() -> StrategyRuntime:
    config = ShortHorizonConfig()
    strategy = ShortHorizon15mTouchStrategy(config=config)
    return StrategyRuntime(strategy=strategy, intent_store=InMemoryIntentStore())


def main() -> None:
    logger = get_logger("short_horizon.live_runner")
    _ = MarketDataSource()
    _ = PolymarketVenueAdapter()
    runtime = build_live_runtime()
    logger.info("live_runner skeleton booted with strategy=%s", runtime.strategy.__class__.__name__)


if __name__ == "__main__":
    main()
