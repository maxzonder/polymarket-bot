from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.core.events import LiquidityRole, OrderFilled, OrderSide
from short_horizon.strategies.swan_strategy_v1 import SwanConfig, SwanStrategyV1
from short_horizon.strategy_api import PlaceOrder


class SwanStrategyExitTest(unittest.TestCase):
    def _fill(self, *, side: OrderSide = OrderSide.BUY, size: float = 12.5) -> OrderFilled:
        return OrderFilled(
            event_time_ms=250_000,
            ingest_time_ms=250_010,
            order_id="entry-1" if side is OrderSide.BUY else "exit-1",
            market_id="m1",
            token_id="tok_yes",
            side=side,
            fill_price=0.05 if side is OrderSide.BUY else 0.80,
            fill_size=size,
            cumulative_filled_size=size,
            remaining_size=0.0,
            source="test.fill",
            liquidity_role=LiquidityRole.MAKER,
        )

    def test_buy_fill_emits_configured_sell_exit(self) -> None:
        strategy = SwanStrategyV1(config=SwanConfig(sell_exit_enabled=True, sell_exit_price=0.80, sell_exit_post_only=True))

        intents = strategy.on_order_event(self._fill())

        self.assertEqual(len(intents), 1)
        self.assertIsInstance(intents[0], PlaceOrder)
        intent = intents[0].intent
        self.assertEqual(intent.side, OrderSide.SELL)
        self.assertEqual(intent.reason, "swan_exit_sell")
        self.assertAlmostEqual(intent.entry_price, 0.80)
        self.assertAlmostEqual(intent.size_shares, 12.5)
        self.assertTrue(intent.post_only)

    def test_sell_fill_reduces_position_without_creating_another_exit(self) -> None:
        strategy = SwanStrategyV1(config=SwanConfig(sell_exit_enabled=True, sell_exit_price=0.80))
        strategy.on_order_event(self._fill(side=OrderSide.BUY, size=12.5))

        intents = strategy.on_order_event(self._fill(side=OrderSide.SELL, size=5.0))

        self.assertEqual(intents, [])
        self.assertAlmostEqual(strategy._positions["tok_yes"], 7.5)


if __name__ == "__main__":
    unittest.main()
