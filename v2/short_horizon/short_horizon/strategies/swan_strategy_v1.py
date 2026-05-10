from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from typing import Iterable

from ..core.clock import Clock, SystemClock
from ..core.events import (
    BookUpdate,
    MarketStateUpdate,
    OrderAccepted,
    OrderCanceled,
    OrderFilled,
    OrderRejected,
    TimerEvent,
    TradeTick,
)
from ..core.models import OrderIntent, SkipDecision, TouchSignal
from ..strategy_api import CancelOrder, Noop, PlaceOrder, StrategyIntent

TIMER_SCREENER_REFRESH = "swan_screener_refresh"
TIMER_STALE_CLEANUP = "swan_stale_cleanup"


@dataclass(frozen=True)
class SwanCandidate:
    """A market candidate produced by the swan screener."""
    market_id: str
    condition_id: str | None
    token_id: str
    question: str
    asset_slug: str | None
    entry_levels: tuple[float, ...]
    notional_usdc_per_level: float
    candidate_id: str = ""


@dataclass
class SwanConfig:
    strategy_id: str = "swan_v1"
    max_open_resting_bids: int = 500
    max_resting_markets: int = 5000
    max_resting_per_cluster: int = 1  # max bids per neg-risk group (unused for now)
    max_total_stake_usdc: float = 500.0
    stale_order_ttl_seconds: float = 3600.0
    # Cancel resting bids when a market enters its final N seconds to close.
    # Issue #180 Phase B3: final_hour cohort has 54.7% hit-rate (worst), avoid
    # filling there. Screener already filters at placement; this catches
    # markets that aged into the danger zone after placement.
    cancel_when_remaining_seconds_lt: float = 3600.0


@dataclass
class _RestingBid:
    market_id: str
    token_id: str
    level: float
    order_id: str
    notional_usdc: float
    placed_at_ms: int


class SwanStrategyV1:
    """
    Swan black-swan resting-bid strategy adapted for the v2 execution path.

    Market discovery (screener) runs in an external async task and updates
    candidates via update_candidates().  On each TIMER_SCREENER_REFRESH
    timer event the strategy diffes new vs old candidates and emits
    PlaceOrder / CancelOrder intents accordingly.
    """

    def __init__(self, *, config: SwanConfig | None = None, clock: Clock | None = None):
        self.config = config or SwanConfig()
        self.clock = clock or SystemClock()
        # Protected by _lock; written by screener thread, read on event loop.
        self._pending_candidates: list[SwanCandidate] | None = None
        self._lock = threading.Lock()
        # Event-loop-local state (no lock needed):
        self._active_candidates: dict[str, SwanCandidate] = {}
        self._resting_bids: dict[str, _RestingBid] = {}          # order_id → bid
        self._bids_by_market: dict[str, set[str]] = {}            # market_id → order_ids
        self._pending_place: dict[str, _RestingBid] = {}          # intent_id → bid (pre-accept)
        self._market_meta: dict[str, MarketStateUpdate] = {}
        self._positions: dict[str, float] = {}                    # token_id → filled qty

    # ─── External API (called from screener thread) ──────────────────────────

    def update_candidates(self, candidates: list[SwanCandidate]) -> None:
        """Thread-safe: store screener results for next timer event to consume."""
        with self._lock:
            self._pending_candidates = list(candidates)

    # ─── TouchStrategy protocol ───────────────────────────────────────────────

    def on_market_state(self, event: MarketStateUpdate) -> list[StrategyIntent]:
        self._market_meta[event.market_id] = event
        # Cancel resting bids for markets that went inactive.
        if not event.is_active and event.market_id in self._bids_by_market:
            return self._cancel_market_bids(event.market_id, reason="market_no_longer_active")
        # Cancel bids on markets that aged into the final-window danger zone.
        if (event.market_id in self._bids_by_market
                and event.end_time_ms is not None
                and self.config.cancel_when_remaining_seconds_lt > 0):
            remaining_s = (event.end_time_ms - self.clock.now_ms()) / 1000.0
            if 0 < remaining_s < self.config.cancel_when_remaining_seconds_lt:
                return self._cancel_market_bids(
                    event.market_id, reason="market_entered_final_window"
                )
        return []

    def detect_touches(self, event: BookUpdate) -> list[TouchSignal]:
        # Swan places resting bids via on_timer, not via ASC-touch detection.
        return []

    def decide_on_touch(self, *, event: BookUpdate, touch: TouchSignal) -> OrderIntent | SkipDecision:
        return SkipDecision(
            reason="swan_no_asc_touch",
            market_id=event.market_id,
            token_id=event.token_id,
            level=touch.level,
            event_time_ms=event.event_time_ms,
        )

    def on_timer(self, timer: TimerEvent) -> list[StrategyIntent]:
        if timer.timer_kind == TIMER_SCREENER_REFRESH:
            return self._process_screener_refresh()
        if timer.timer_kind == TIMER_STALE_CLEANUP:
            return self._cleanup_stale_orders(timer.event_time_ms)
        return []

    def on_order_event(self, event) -> list[StrategyIntent]:
        if isinstance(event, OrderAccepted):
            self._on_accepted(event)
        elif isinstance(event, OrderCanceled):
            self._on_canceled(event)
        elif isinstance(event, OrderFilled):
            self._on_filled(event)
        elif isinstance(event, OrderRejected):
            self._on_rejected(event)
        return []

    def on_market_event(self, event) -> list[StrategyIntent]:
        return []

    # ─── Internal ─────────────────────────────────────────────────────────────

    def _process_screener_refresh(self) -> list[StrategyIntent]:
        with self._lock:
            if self._pending_candidates is None:
                return []
            new_candidates = {c.market_id: c for c in self._pending_candidates}
            self._pending_candidates = None

        intents: list[StrategyIntent] = []

        # Cancel bids for markets that dropped off the candidate list.
        for market_id in set(self._active_candidates) - set(new_candidates):
            intents.extend(self._cancel_market_bids(market_id, reason="dropped_from_screener"))

        # Place bids for new/updated candidates (subject to risk limits).
        for candidate in new_candidates.values():
            intents.extend(self._maybe_place_bids(candidate))

        self._active_candidates = new_candidates
        return intents

    def _maybe_place_bids(self, candidate: SwanCandidate) -> list[StrategyIntent]:
        intents: list[StrategyIntent] = []
        market_bids = self._bids_by_market.get(candidate.market_id, set())

        for level in candidate.entry_levels:
            # Skip if we already have a resting bid at this exact level.
            already_bid = any(
                self._resting_bids[oid].level == level
                for oid in market_bids
                if oid in self._resting_bids
            )
            if already_bid:
                continue

            # Risk caps.
            if len(self._resting_bids) + len(self._pending_place) >= self.config.max_open_resting_bids:
                break
            if (candidate.market_id not in self._bids_by_market
                    and len(self._bids_by_market) >= self.config.max_resting_markets):
                break

            intent = self._build_order_intent(candidate, level)
            intents.append(PlaceOrder(intent=intent))
            self._pending_place[intent.intent_id] = _RestingBid(
                market_id=candidate.market_id,
                token_id=candidate.token_id,
                level=level,
                order_id=intent.intent_id,
                notional_usdc=candidate.notional_usdc_per_level,
                placed_at_ms=self.clock.now_ms(),
            )

        return intents

    def _build_order_intent(self, candidate: SwanCandidate, level: float) -> OrderIntent:
        now_ms = self.clock.now_ms()
        meta = self._market_meta.get(candidate.market_id)
        if meta and meta.end_time_ms and meta.start_time_ms:
            duration_ms = max(meta.end_time_ms - meta.start_time_ms, 1)
            remaining_ms = max(meta.end_time_ms - now_ms, 0)
            lifecycle = 1.0 - remaining_ms / duration_ms
        else:
            lifecycle = 0.5

        return OrderIntent(
            intent_id=str(uuid.uuid4()),
            strategy_id=self.config.strategy_id,
            market_id=candidate.market_id,
            token_id=candidate.token_id,
            condition_id=candidate.condition_id,
            question=candidate.question,
            asset_slug=candidate.asset_slug or "",
            level=level,
            entry_price=level,
            notional_usdc=candidate.notional_usdc_per_level,
            lifecycle_fraction=lifecycle,
            event_time_ms=now_ms,
            reason="swan_resting_bid",
        )

    def _cancel_market_bids(self, market_id: str, reason: str = "") -> list[StrategyIntent]:
        intents: list[StrategyIntent] = []
        for oid in list(self._bids_by_market.get(market_id, set())):
            bid = self._resting_bids.get(oid)
            if bid:
                intents.append(CancelOrder(market_id=market_id, token_id=bid.token_id, reason=reason))
        return intents

    def _cleanup_stale_orders(self, now_ms: int) -> list[StrategyIntent]:
        intents: list[StrategyIntent] = []
        ttl_ms = int(self.config.stale_order_ttl_seconds * 1000)
        for oid, bid in list(self._resting_bids.items()):
            if now_ms - bid.placed_at_ms > ttl_ms:
                intents.append(CancelOrder(
                    market_id=bid.market_id,
                    token_id=bid.token_id,
                    reason="stale_order_ttl_expired",
                ))
        return intents

    def _on_accepted(self, event: OrderAccepted) -> None:
        bid = self._pending_place.pop(event.order_id, None)
        if bid is None:
            return
        self._resting_bids[event.order_id] = bid
        self._bids_by_market.setdefault(bid.market_id, set()).add(event.order_id)

    def _on_canceled(self, event: OrderCanceled) -> None:
        bid = self._resting_bids.pop(event.order_id, None)
        if bid:
            self._bids_by_market.get(bid.market_id, set()).discard(event.order_id)
        self._pending_place.pop(event.order_id, None)

    def _on_filled(self, event: OrderFilled) -> None:
        if event.remaining_size <= 0:
            bid = self._resting_bids.pop(event.order_id, None)
            if bid:
                self._bids_by_market.get(bid.market_id, set()).discard(event.order_id)
        self._positions[event.token_id] = self._positions.get(event.token_id, 0.0) + event.fill_size

    def _on_rejected(self, event: OrderRejected) -> None:
        # OrderRejected has client_order_id (=intent_id), not order_id
        key = event.client_order_id
        if not key:
            return
        self._pending_place.pop(key, None)
        bid = self._resting_bids.pop(key, None)
        if bid:
            self._bids_by_market.get(bid.market_id, set()).discard(key)
