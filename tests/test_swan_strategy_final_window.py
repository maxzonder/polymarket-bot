"""
Tests for SwanStrategyV1 final-window cancellation (issue #180 P0.2).

Verifies that resting bids are cancelled when their market ages into the
final-window danger zone (default 1h), even after placement.
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_V2 = _REPO / "v2" / "short_horizon"
for p in (_REPO, _V2):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from short_horizon.core.events import MarketStateUpdate
from short_horizon.strategies.swan_strategy_v1 import (
    SwanCandidate,
    SwanConfig,
    SwanStrategyV1,
    _RestingBid,
)
from short_horizon.strategies.black_swan_strategy_v1 import (
    BlackSwanConfig,
    BlackSwanStrategyV1,
)
from short_horizon.strategy_api import CancelOrder, PlaceOrder


class _FixedClock:
    def __init__(self, now_ms: int) -> None:
        self._now_ms = now_ms

    def now_ms(self) -> int:
        return self._now_ms

    def advance_ms(self, dt_ms: int) -> None:
        self._now_ms += dt_ms


def _make_market_state(market_id: str, end_time_ms: int, *, duration_seconds: int | None = None) -> MarketStateUpdate:
    return MarketStateUpdate(
        event_time_ms=0,
        ingest_time_ms=0,
        market_id=market_id,
        condition_id=f"cond_{market_id}",
        status="active",
        is_active=True,
        end_time_ms=end_time_ms,
        duration_seconds=duration_seconds,
    )


def _seed_resting_bid(strategy: SwanStrategyV1, market_id: str, token_id: str) -> str:
    bid = _RestingBid(
        market_id=market_id,
        token_id=token_id,
        level=0.01,
        order_id="oid_1",
        notional_usdc=0.10,
        placed_at_ms=0,
    )
    strategy._resting_bids[bid.order_id] = bid
    strategy._bids_by_market.setdefault(market_id, set()).add(bid.order_id)
    return bid.order_id


def test_market_state_cancels_when_in_final_window():
    clock = _FixedClock(now_ms=1_000_000_000_000)
    strategy = SwanStrategyV1(
        config=SwanConfig(cancel_when_remaining_seconds_lt=3600.0),
        clock=clock,
    )
    _seed_resting_bid(strategy, market_id="m1", token_id="t1")

    # Market closes in 30 minutes — inside 1h danger zone
    end_ms = clock.now_ms() + 30 * 60 * 1000
    intents = strategy.on_market_state(_make_market_state("m1", end_ms))
    assert len(intents) == 1
    assert isinstance(intents[0], CancelOrder)
    assert intents[0].reason == "market_entered_final_window"


def test_market_state_keeps_bids_outside_final_window():
    clock = _FixedClock(now_ms=1_000_000_000_000)
    strategy = SwanStrategyV1(
        config=SwanConfig(cancel_when_remaining_seconds_lt=3600.0),
        clock=clock,
    )
    _seed_resting_bid(strategy, market_id="m1", token_id="t1")

    # Market closes in 4 hours — well outside the 1h danger zone
    end_ms = clock.now_ms() + 4 * 3600 * 1000
    intents = strategy.on_market_state(_make_market_state("m1", end_ms))
    assert intents == []


def test_disabled_when_threshold_zero():
    clock = _FixedClock(now_ms=1_000_000_000_000)
    strategy = SwanStrategyV1(
        config=SwanConfig(cancel_when_remaining_seconds_lt=0.0),
        clock=clock,
    )
    _seed_resting_bid(strategy, market_id="m1", token_id="t1")
    # Even though market is in final 5 min, disabled threshold means no cancel.
    end_ms = clock.now_ms() + 5 * 60 * 1000
    intents = strategy.on_market_state(_make_market_state("m1", end_ms))
    assert intents == []


def test_inactive_market_takes_priority():
    clock = _FixedClock(now_ms=1_000_000_000_000)
    strategy = SwanStrategyV1(config=SwanConfig(), clock=clock)
    _seed_resting_bid(strategy, market_id="m1", token_id="t1")

    end_ms = clock.now_ms() + 30 * 60 * 1000
    event = MarketStateUpdate(
        event_time_ms=0,
        ingest_time_ms=0,
        market_id="m1",
        condition_id="c",
        status="closed",
        is_active=False,
        end_time_ms=end_ms,
    )
    intents = strategy.on_market_state(event)
    assert len(intents) == 1
    assert intents[0].reason == "market_no_longer_active"


def test_black_swan_default_ttl_is_ten_hours():
    """Regression: BlackSwanConfig must own its TTL — issue #180 found
    swan_live.py was overriding it with mode_config.max_hours_to_close*3600
    (168h for BLACK_SWAN_MODE). Issue #203 moved the long-market cap from 6h
    to 10h to reduce cancel/reopen churn while keeping duration-relative TTL."""
    cfg = BlackSwanConfig()
    assert cfg.stale_order_ttl_seconds == 36000.0  # 10h


def test_black_swan_has_no_separate_manual_per_market_money_cap():
    cfg = BlackSwanConfig()
    assert cfg.max_effective_notional_per_market_usdc == 0.0


def test_black_swan_strategy_inherits_final_window_cancel():
    clock = _FixedClock(now_ms=1_000_000_000_000)
    strategy = BlackSwanStrategyV1(config=BlackSwanConfig(), clock=clock)
    _seed_resting_bid(strategy, market_id="m1", token_id="t1")
    end_ms = clock.now_ms() + 20 * 60 * 1000  # 20 min remaining → in final hour
    intents = strategy.on_market_state(_make_market_state("m1", end_ms))
    assert len(intents) == 1
    assert intents[0].reason == "market_entered_final_window"


def test_black_swan_relative_final_window_keeps_hour_market_with_twenty_minutes_left():
    clock = _FixedClock(now_ms=1_000_000_000_000)
    strategy = BlackSwanStrategyV1(config=BlackSwanConfig(), clock=clock)
    _seed_resting_bid(strategy, market_id="m1", token_id="t1")
    end_ms = clock.now_ms() + 20 * 60 * 1000

    intents = strategy.on_market_state(_make_market_state("m1", end_ms, duration_seconds=3600))

    assert intents == []


def test_black_swan_relative_final_window_cancels_hour_market_in_final_five_minutes():
    clock = _FixedClock(now_ms=1_000_000_000_000)
    strategy = BlackSwanStrategyV1(config=BlackSwanConfig(), clock=clock)
    _seed_resting_bid(strategy, market_id="m1", token_id="t1")
    end_ms = clock.now_ms() + 5 * 60 * 1000

    intents = strategy.on_market_state(_make_market_state("m1", end_ms, duration_seconds=3600))

    assert len(intents) == 1
    assert intents[0].reason == "market_entered_final_window"


def test_black_swan_relative_stale_ttl_caps_short_market_by_duration():
    clock = _FixedClock(now_ms=1_000_000_000_000)
    strategy = BlackSwanStrategyV1(config=BlackSwanConfig(), clock=clock)
    _seed_resting_bid(strategy, market_id="m1", token_id="t1")
    strategy.on_market_state(_make_market_state("m1", clock.now_ms() + 60 * 60 * 1000, duration_seconds=3600))

    clock.advance_ms(31 * 60 * 1000)
    intents = strategy._cleanup_stale_orders(clock.now_ms())

    assert len(intents) == 1
    assert intents[0].reason == "stale_order_ttl_expired"
