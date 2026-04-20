from __future__ import annotations

from .config import ShortHorizonConfig
from .core.runtime import StrategyRuntime
from .storage import RuntimeStore
from .strategies import ShortHorizon15mTouchStrategy


class ShortHorizonEngine(StrategyRuntime):
    """Compatibility wrapper around the new P1-1 runtime + strategy split."""

    def __init__(self, *, config: ShortHorizonConfig, intent_store: RuntimeStore):
        super().__init__(
            strategy=ShortHorizon15mTouchStrategy(config=config),
            intent_store=intent_store,
        )
        self.config = config
