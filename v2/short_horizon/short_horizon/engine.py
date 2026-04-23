from __future__ import annotations

from .config import ShortHorizonConfig
from .core.clock import SystemClock
from .core.runtime import StrategyRuntime
from .storage import RuntimeStore
from .strategies import ShortHorizon15mTouchStrategy


class ShortHorizonEngine(StrategyRuntime):
    """Compatibility wrapper around the new P1-1 runtime + strategy split."""

    def __init__(self, *, config: ShortHorizonConfig, intent_store: RuntimeStore):
        clock = SystemClock()
        super().__init__(
            strategy=ShortHorizon15mTouchStrategy(config=config, clock=clock),
            intent_store=intent_store,
            clock=clock,
        )
        self.config = config
