from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from typing import Iterable

from ..core.clock import Clock, SystemClock
from ..core.events import (
    BookUpdate,
    MarketStateUpdate,
    OrderSide,
    OrderAccepted,
    OrderCanceled,
    OrderFilled,
    OrderRejected,
    TimerEvent,
    TradeTick,
)
from ..core.models import OrderIntent, SkipDecision, TouchSignal
from ..core.order_state import OrderState
from ..execution.order_translator import (
    DEFAULT_VENUE_MIN_ORDER_NOTIONAL_USDC,
    VenueConstraints,
    estimate_effective_buy_notional,
)
from ..strategy_api import CancelOrder, Noop, PlaceOrder, StrategyIntent

TIMER_SCREENER_REFRESH = "swan_screener_refresh"
TIMER_STALE_CLEANUP = "swan_stale_cleanup"


def _phase_stake_multiplier(
    lifecycle_fraction: float,
    table: tuple[tuple[float, float], ...],
) -> float:
    """First-match multiplier from a (max_lifecycle, multiplier) table.

    Empty table → 1.0 (no scaling). Sentinel: pass float('inf') as the last
    max_lifecycle to bound the table.
    """
    if not table:
        return 1.0
    for max_lc, mult in table:
        if lifecycle_fraction <= max_lc:
            return mult
    return 1.0


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
    # Optional relative stale TTL. If set and duration is known, effective TTL
    # is min(stale_order_ttl_seconds, duration * fraction). This prevents a
    # fixed multi-hour TTL from dominating short-duration markets.
    stale_order_ttl_fraction_of_duration: float = 0.0
    # Cancel resting bids when a market enters its final N seconds to close.
    # This is the absolute fallback/cap. If cancel_when_remaining_fraction_lt
    # is set and market duration is known, the effective threshold is
    # min(absolute_seconds, duration * fraction). Screener already filters at
    # placement; this catches markets that aged into the danger zone.
    cancel_when_remaining_seconds_lt: float = 3600.0
    cancel_when_remaining_fraction_lt: float = 0.0

    # Lifecycle (phase) stake multipliers (issue #180 P1.3). Tuple of
    # (max_lifecycle_fraction, multiplier); first match wins. Empty = no scaling.
    phase_stake_multipliers: tuple[tuple[float, float], ...] = ()

    # Optional take-profit exit. Resolution-only remains the default; paper/live
    # lifecycle tests can enable this to verify SELL paths end-to-end.
    sell_exit_enabled: bool = False
    sell_exit_price: float = 0.99
    sell_exit_post_only: bool = True

    # Venue-minimum awareness for cheap resting ladders.  Disabled by default
    # for legacy swan behaviour; black_swan enables this so a nominal
    # $1/market budget cannot silently become five venue-minimum orders.
    use_effective_notional_for_caps: bool = False
    max_effective_notional_per_market_usdc: float = 0.0
    skip_below_venue_min_effective_notional: bool = False
    venue_tick_size_fallback: float = 0.01
    venue_min_order_notional_usdc: float = DEFAULT_VENUE_MIN_ORDER_NOTIONAL_USDC
    venue_min_order_shares_fallback: float = 0.0


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
        # Cash/risk accounting for resting-bid ladders.  A natural live trader
        # reserves cash for open bids and consumes cash when a bid fills; a
        # filled entry level should not be re-opened on every screener refresh
        # just because the token is still cheap.  Without these guards paper can
        # churn one-dollar bids into tens of thousands of synthetic fills.
        self._filled_buy_notional_usdc: float = 0.0
        self._filled_buy_notional_by_market: dict[str, float] = {}
        self._completed_entry_levels: set[tuple[str, str, float]] = set()

    # ─── External API (called from screener thread) ──────────────────────────

    def update_candidates(self, candidates: list[SwanCandidate]) -> None:
        """Thread-safe: store screener results for next timer event to consume."""
        with self._lock:
            self._pending_candidates = list(candidates)

    def get_active_market_ids(self) -> frozenset[str]:
        """Thread-safe: return the set of market_ids with live resting bids."""
        return frozenset(self._bids_by_market)

    def hydrate_open_orders(self, rows: Iterable[dict]) -> None:
        """Restore open resting-bid state after startup reconciliation.

        The v2 live runner reconciles persisted orders before the first screener
        refresh.  Without hydration, restarted swan runs know nothing about
        still-open resting bids, so markets that no longer pass the screener do
        not get canceled on the next candidate diff.
        """
        active_states = {
            OrderState.ACCEPTED.value,
            OrderState.PARTIALLY_FILLED.value,
            OrderState.UNKNOWN.value,
        }
        self._resting_bids.clear()
        self._bids_by_market.clear()
        self._pending_place.clear()
        self._active_candidates.clear()
        self._filled_buy_notional_usdc = 0.0
        self._filled_buy_notional_by_market.clear()
        self._completed_entry_levels.clear()

        for row in rows:
            market_id = str(row.get("market_id") or "")
            token_id = str(row.get("token_id") or "")
            order_id = str(row.get("order_id") or row.get("venue_order_id") or row.get("client_order_id") or "")
            if not market_id or not token_id:
                continue
            level = float(row.get("price") or 0.0)
            size = float(row.get("size") or 0.0)
            notional = level * size if level > 0 and size > 0 else 0.0
            cumulative_filled_size = float(row.get("cumulative_filled_size") or 0.0)
            side = str(row.get("side") or "BUY").upper()
            if cumulative_filled_size > 0 and level > 0:
                if side == "BUY":
                    filled_notional = level * cumulative_filled_size
                    self._filled_buy_notional_usdc += filled_notional
                    self._filled_buy_notional_by_market[market_id] = self._filled_buy_notional_by_market.get(market_id, 0.0) + filled_notional
                    if str(row.get("state") or "") == OrderState.FILLED.value:
                        self._completed_entry_levels.add(
                            self._entry_level_key(market_id, token_id, level)
                        )
                elif side == "SELL":
                    self._filled_buy_notional_usdc = max(
                        self._filled_buy_notional_usdc - level * cumulative_filled_size,
                        0.0,
                    )
            if str(row.get("state") or "") not in active_states:
                continue
            if not order_id:
                continue
            remaining_size = float(row.get("remaining_size") or 0.0)
            if remaining_size > 0 and level > 0:
                notional = level * remaining_size
            bid = _RestingBid(
                market_id=market_id,
                token_id=token_id,
                level=level,
                order_id=order_id,
                notional_usdc=notional,
                placed_at_ms=self.clock.now_ms(),
            )
            self._resting_bids[order_id] = bid
            self._bids_by_market.setdefault(market_id, set()).add(order_id)
            self._active_candidates.setdefault(
                market_id,
                SwanCandidate(
                    market_id=market_id,
                    condition_id=str(row.get("condition_id") or "") or None,
                    token_id=token_id,
                    question=str(row.get("question") or ""),
                    asset_slug=str(row.get("asset_slug") or "") or None,
                    entry_levels=(level,) if level > 0 else (),
                    notional_usdc_per_level=notional,
                ),
            )

    # ─── TouchStrategy protocol ───────────────────────────────────────────────

    def on_market_state(self, event: MarketStateUpdate) -> list[StrategyIntent]:
        self._market_meta[event.market_id] = event
        # Cancel resting bids for markets that went inactive.
        if not event.is_active and event.market_id in self._bids_by_market:
            return self._cancel_market_bids(event.market_id, reason="market_no_longer_active")
        # Cancel bids on markets that aged into the final-window danger zone.
        if event.market_id in self._bids_by_market and event.end_time_ms is not None:
            threshold_s = self._final_window_threshold_seconds(event)
            remaining_s = (event.end_time_ms - self.clock.now_ms()) / 1000.0
            if threshold_s > 0 and 0 < remaining_s < threshold_s:
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
            return self._on_filled(event)
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
            level_key = self._entry_level_key(candidate.market_id, candidate.token_id, level)
            if level_key in self._completed_entry_levels:
                continue

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
            effective_notional = self._effective_notional_usdc(intent)
            if self._should_skip_below_venue_min(effective_notional):
                continue
            committed_notional = effective_notional if self.config.use_effective_notional_for_caps else intent.notional_usdc
            if self._would_exceed_market_effective_cap(candidate.market_id, effective_notional):
                continue
            if self._would_exceed_total_stake_cap(committed_notional):
                break
            intents.append(PlaceOrder(intent=intent))
            self._pending_place[intent.intent_id] = _RestingBid(
                market_id=candidate.market_id,
                token_id=candidate.token_id,
                level=level,
                order_id=intent.intent_id,
                notional_usdc=committed_notional,
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

        notional = candidate.notional_usdc_per_level * _phase_stake_multiplier(
            lifecycle, self.config.phase_stake_multipliers
        )

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
            notional_usdc=notional,
            lifecycle_fraction=lifecycle,
            event_time_ms=now_ms,
            reason="swan_resting_bid",
            side=OrderSide.BUY,
            post_only=True,
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
        for oid, bid in list(self._resting_bids.items()):
            ttl_ms = int(self._stale_order_ttl_seconds(bid) * 1000)
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

    def _on_filled(self, event: OrderFilled) -> list[StrategyIntent]:
        intents: list[StrategyIntent] = []
        if event.remaining_size <= 0:
            bid = self._resting_bids.pop(event.order_id, None)
            if bid:
                self._bids_by_market.get(bid.market_id, set()).discard(event.order_id)
        else:
            bid = self._resting_bids.get(event.order_id)
            if bid:
                bid.notional_usdc = max(float(event.remaining_size), 0.0) * float(bid.level)
        if event.side is OrderSide.SELL:
            self._positions[event.token_id] = max(self._positions.get(event.token_id, 0.0) - event.fill_size, 0.0)
            sold_notional = float(event.fill_price) * float(event.fill_size)
            self._filled_buy_notional_usdc = max(
                self._filled_buy_notional_usdc - sold_notional,
                0.0,
            )
            self._filled_buy_notional_by_market[event.market_id] = max(
                self._filled_buy_notional_by_market.get(event.market_id, 0.0) - sold_notional,
                0.0,
            )
            return intents

        self._positions[event.token_id] = self._positions.get(event.token_id, 0.0) + event.fill_size
        filled_notional = float(event.fill_price) * float(event.fill_size)
        self._filled_buy_notional_usdc += filled_notional
        self._filled_buy_notional_by_market[event.market_id] = self._filled_buy_notional_by_market.get(event.market_id, 0.0) + filled_notional
        if event.remaining_size <= 0:
            filled_level = bid.level if bid else float(event.fill_price)
            self._completed_entry_levels.add(
                self._entry_level_key(event.market_id, event.token_id, filled_level)
            )
        if self.config.sell_exit_enabled and event.fill_size > 0:
            exit_price = float(self.config.sell_exit_price)
            intents.append(PlaceOrder(intent=OrderIntent(
                intent_id=str(uuid.uuid4()),
                strategy_id=self.config.strategy_id,
                market_id=event.market_id,
                token_id=event.token_id,
                condition_id=None,
                question=None,
                asset_slug="",
                level=exit_price,
                entry_price=exit_price,
                notional_usdc=exit_price * float(event.fill_size),
                lifecycle_fraction=1.0,
                event_time_ms=event.event_time_ms,
                reason="swan_exit_sell",
                side=OrderSide.SELL,
                size_shares=float(event.fill_size),
                post_only=bool(self.config.sell_exit_post_only),
            )))
        return intents

    def _on_rejected(self, event: OrderRejected) -> None:
        # OrderRejected has client_order_id (=intent_id), not order_id
        key = event.client_order_id
        if not key:
            return
        self._pending_place.pop(key, None)
        bid = self._resting_bids.pop(key, None)
        if bid:
            self._bids_by_market.get(bid.market_id, set()).discard(key)

    def _would_exceed_total_stake_cap(self, next_notional_usdc: float) -> bool:
        cap = float(getattr(self.config, "max_total_stake_usdc", 0.0) or 0.0)
        if cap <= 0:
            return False
        committed = self._filled_buy_notional_usdc
        committed += sum(bid.notional_usdc for bid in self._resting_bids.values())
        committed += sum(bid.notional_usdc for bid in self._pending_place.values())
        return committed + float(next_notional_usdc) > cap + 1e-9

    def _would_exceed_market_effective_cap(self, market_id: str, next_effective_notional_usdc: float) -> bool:
        cap = float(getattr(self.config, "max_effective_notional_per_market_usdc", 0.0) or 0.0)
        if cap <= 0:
            return False
        committed = self._filled_buy_notional_by_market.get(market_id, 0.0)
        committed += sum(
            bid.notional_usdc for bid in self._resting_bids.values()
            if bid.market_id == market_id
        )
        committed += sum(
            bid.notional_usdc for bid in self._pending_place.values()
            if bid.market_id == market_id
        )
        return committed + float(next_effective_notional_usdc) > cap + 1e-9

    def _should_skip_below_venue_min(self, effective_notional_usdc: float) -> bool:
        if not self.config.skip_below_venue_min_effective_notional:
            return False
        minimum = float(self.config.venue_min_order_notional_usdc or 0.0)
        return minimum > 0 and float(effective_notional_usdc) + 1e-12 < minimum

    def _effective_notional_usdc(self, intent: OrderIntent) -> float:
        if not self.config.use_effective_notional_for_caps and not self.config.skip_below_venue_min_effective_notional:
            return float(intent.notional_usdc)
        if OrderSide(str(intent.side)) is not OrderSide.BUY:
            return float(intent.notional_usdc)
        meta = self._market_meta.get(intent.market_id)
        min_order_shares = None
        tick_size = float(self.config.venue_tick_size_fallback or 0.01)
        if meta is not None:
            if meta.tick_size is not None:
                tick_size = float(meta.tick_size)
            if meta.min_order_size is not None:
                min_order_shares = float(meta.min_order_size)
        if min_order_shares is None:
            fallback = float(self.config.venue_min_order_shares_fallback or 0.0)
            min_order_shares = fallback if fallback > 0 else None
        return estimate_effective_buy_notional(
            notional_usdc=float(intent.notional_usdc),
            entry_price=float(intent.entry_price),
            venue_constraints=VenueConstraints(
                tick_size=tick_size,
                min_order_size=float(self.config.venue_min_order_notional_usdc or 0.0),
                min_order_shares=min_order_shares,
            ),
        )

    def _final_window_threshold_seconds(self, event: MarketStateUpdate) -> float:
        absolute = float(self.config.cancel_when_remaining_seconds_lt or 0.0)
        fraction = float(getattr(self.config, "cancel_when_remaining_fraction_lt", 0.0) or 0.0)
        duration_s = self._market_duration_seconds(event.market_id, event)
        if fraction > 0.0 and duration_s and duration_s > 0:
            relative = float(duration_s) * fraction
            return min(absolute, relative) if absolute > 0.0 else relative
        return absolute

    def _stale_order_ttl_seconds(self, bid: _RestingBid) -> float:
        absolute = float(self.config.stale_order_ttl_seconds or 0.0)
        fraction = float(getattr(self.config, "stale_order_ttl_fraction_of_duration", 0.0) or 0.0)
        duration_s = self._market_duration_seconds(bid.market_id)
        if fraction > 0.0 and duration_s and duration_s > 0:
            relative = float(duration_s) * fraction
            return min(absolute, relative) if absolute > 0.0 else relative
        return absolute

    def _market_duration_seconds(self, market_id: str, event: MarketStateUpdate | None = None) -> float | None:
        meta = event or self._market_meta.get(market_id)
        if meta is None:
            return None
        if meta.duration_seconds is not None and meta.duration_seconds > 0:
            return float(meta.duration_seconds)
        if meta.start_time_ms is not None and meta.end_time_ms is not None and meta.end_time_ms > meta.start_time_ms:
            return float(meta.end_time_ms - meta.start_time_ms) / 1000.0
        return None

    @staticmethod
    def _entry_level_key(market_id: str, token_id: str, level: float) -> tuple[str, str, float]:
        return (str(market_id), str(token_id), round(float(level), 8))
