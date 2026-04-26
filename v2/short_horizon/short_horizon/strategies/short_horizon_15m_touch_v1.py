from __future__ import annotations

import math
from typing import Iterable

from ..config import ShortHorizonConfig
from ..core.clock import Clock, SystemClock
from ..core.order_state import OrderState
from ..core.events import BookUpdate, MarketStateUpdate, OrderAccepted, OrderCanceled, OrderFilled, OrderRejected, SpotPriceUpdate, TimerEvent, TradeTick, effective_fee_rate_bps
from ..core.lifecycle import compute_lifecycle_fraction
from ..core.models import MarketState, OrderIntent, SkipDecision, TouchSignal
from ..risk import gate_touch
from ..strategy_api import CancelOrder, Noop, PlaceOrder, StrategyIntent


class FirstTouchTracker:
    def __init__(self, levels: tuple[float, ...]):
        self.levels = tuple(sorted(levels))
        self._last_best_ask: dict[tuple[str, float], float] = {}
        self._fired: set[tuple[str, str, float]] = set()

    def observe_best_ask(
        self,
        *,
        market_id: str,
        token_id: str,
        best_ask: float | None,
        event_time_ms: int,
    ) -> list[TouchSignal]:
        if best_ask is None:
            return []

        touches: list[TouchSignal] = []
        best_ask = float(best_ask)
        for level in self.levels:
            last_key = (token_id, level)
            fired_key = (market_id, token_id, level)
            prev = self._last_best_ask.get(last_key)
            self._last_best_ask[last_key] = best_ask
            if prev is None:
                continue
            if fired_key in self._fired:
                continue
            if prev < level <= best_ask:
                self._fired.add(fired_key)
                touches.append(
                    TouchSignal(
                        market_id=market_id,
                        token_id=token_id,
                        level=level,
                        previous_best_ask=prev,
                        current_best_ask=best_ask,
                        event_time_ms=event_time_ms,
                    )
                )
        return touches


class ShortHorizon15mTouchStrategy:
    def __init__(self, *, config: ShortHorizonConfig, clock: Clock | None = None):
        self.config = config
        self.clock = clock or SystemClock()
        self.touch_tracker = FirstTouchTracker(config.triggers.price_levels)
        self.market_state_by_token: dict[str, MarketState] = {}
        self.active_order_ids_by_market_token: dict[tuple[str, str], str] = {}
        self.active_order_states_by_market_token: dict[tuple[str, str], OrderState] = {}
        self.active_inventory_by_market: dict[str, set[str]] = {}
        self.latest_spot_by_asset: dict[str, SpotPriceUpdate] = {}
        self.spot_history_by_asset: dict[str, list[SpotPriceUpdate]] = {}

    def on_market_event(self, event: BookUpdate | MarketStateUpdate | TradeTick) -> list[StrategyIntent]:
        if isinstance(event, MarketStateUpdate):
            return self.on_market_state(event)
        if isinstance(event, TradeTick):
            return []

        outputs: list[StrategyIntent] = []
        for touch in self.detect_touches(event):
            if self._has_active_order(market_id=event.market_id, token_id=event.token_id):
                outputs.append(
                    Noop(
                        reason="open_order_exists",
                        market_id=event.market_id,
                        token_id=event.token_id,
                        details={"existing_order_id": self.active_order_ids_by_market_token[(event.market_id, event.token_id)]},
                    )
                )
                continue
            market_inventory = self.active_inventory_by_market.get(event.market_id, set())
            if market_inventory:
                outputs.append(
                    Noop(
                        reason="opposite_side_position_on_market",
                        market_id=event.market_id,
                        token_id=event.token_id,
                        details={"inventory_token_ids": sorted(market_inventory)},
                    )
                )
                continue
            decision = self.decide_on_touch(event=event, touch=touch)
            if isinstance(decision, OrderIntent):
                outputs.append(PlaceOrder(decision))
            else:
                outputs.append(
                    Noop(
                        reason=decision.reason,
                        market_id=decision.market_id,
                        token_id=decision.token_id,
                        details=decision.details,
                    )
                )
        return outputs

    def on_order_event(self, event: OrderAccepted | OrderRejected | OrderFilled | OrderCanceled) -> list[StrategyIntent]:
        key = (event.market_id, event.token_id)
        if isinstance(event, OrderAccepted):
            self.active_order_ids_by_market_token[key] = event.order_id
            self.active_order_states_by_market_token[key] = OrderState.ACCEPTED
            return []
        if isinstance(event, OrderFilled):
            self.active_inventory_by_market.setdefault(event.market_id, set()).add(event.token_id)
            if event.remaining_size > 1e-12:
                self.active_order_ids_by_market_token[key] = event.order_id
                self.active_order_states_by_market_token[key] = OrderState.PARTIALLY_FILLED
            else:
                self.active_order_ids_by_market_token.pop(key, None)
                self.active_order_states_by_market_token.pop(key, None)
            return []
        self.active_order_ids_by_market_token.pop(key, None)
        self.active_order_states_by_market_token.pop(key, None)
        return []

    def on_timer(self, timer: TimerEvent) -> list[StrategyIntent]:
        return []

    def on_spot_price_update(self, event: SpotPriceUpdate) -> None:
        asset = _normalize_asset_slug(event.asset_slug)
        self.latest_spot_by_asset[asset] = event
        history = self.spot_history_by_asset.setdefault(asset, [])
        history.append(event)
        cutoff_ms = int(event.event_time_ms) - 20 * 60 * 1000
        while history and int(history[0].event_time_ms) < cutoff_ms:
            history.pop(0)

    def hydrate_open_orders(self, rows: Iterable[dict]) -> None:
        self.active_order_ids_by_market_token.clear()
        self.active_order_states_by_market_token.clear()
        self.active_inventory_by_market.clear()
        for row in rows:
            state = OrderState(str(row["state"]))
            market_id = str(row["market_id"])
            token_id = str(row["token_id"])
            if _row_has_inventory_exposure(row, state=state):
                self.active_inventory_by_market.setdefault(market_id, set()).add(token_id)
            if state in {OrderState.REJECTED, OrderState.FILLED, OrderState.CANCEL_CONFIRMED, OrderState.EXPIRED, OrderState.REPLACED}:
                continue
            key = (market_id, token_id)
            self.active_order_ids_by_market_token[key] = str(row["order_id"])
            self.active_order_states_by_market_token[key] = state

    def on_market_state(self, event: MarketStateUpdate) -> list[StrategyIntent]:
        self.market_state_by_token[event.token_id] = MarketState(
            market_id=event.market_id,
            token_id=event.token_id,
            condition_id=event.condition_id,
            question=event.question,
            asset_slug=event.asset_slug.lower(),
            start_time_ms=event.start_time_ms,
            end_time_ms=event.end_time_ms,
            is_active=event.is_active,
            metadata_is_fresh=event.metadata_is_fresh,
            fee_rate_bps=event.fee_rate_bps,
            fee_fetched_at_ms=event.fee_fetched_at_ms,
            fee_metadata_age_ms=event.fee_metadata_age_ms,
            token_yes_id=event.token_yes_id,
            token_no_id=event.token_no_id,
            tick_size=event.tick_size,
            fee_info=event.fee_info,
        )
        if not event.is_active or event.event_time_ms >= event.end_time_ms:
            self.active_inventory_by_market.pop(event.market_id, None)
        if self.config.execution.hold_to_resolution:
            return []
        key = (event.market_id, event.token_id)
        order_id = self.active_order_ids_by_market_token.get(key)
        order_state = self.active_order_states_by_market_token.get(key)
        if not order_id or order_state not in {OrderState.ACCEPTED, OrderState.PARTIALLY_FILLED}:
            return []
        if event.is_active and event.event_time_ms < event.end_time_ms:
            return []
        self.active_order_states_by_market_token[key] = OrderState.CANCEL_REQUESTED
        return [
            CancelOrder(
                market_id=event.market_id,
                token_id=event.token_id,
                reason="market_inactive_or_window_closed",
            )
        ]

    def detect_touches(self, event: BookUpdate) -> list[TouchSignal]:
        return self.touch_tracker.observe_best_ask(
            market_id=event.market_id,
            token_id=event.token_id,
            best_ask=event.best_ask,
            event_time_ms=event.event_time_ms,
        )

    def decide_on_touch(self, *, event: BookUpdate, touch: TouchSignal) -> OrderIntent | SkipDecision:
        state = self.market_state_by_token.get(event.token_id)
        skip = gate_touch(config=self.config, event=event, state=state, level=touch.level)
        if skip is not None:
            return skip

        assert state is not None
        lifecycle_fraction = compute_lifecycle_fraction(
            start_time_ms=state.start_time_ms,
            end_time_ms=state.end_time_ms,
            event_time_ms=event.event_time_ms,
        )
        spot_gate = self._evaluate_spot_dislocation_gate(event=event, state=state, lifecycle_fraction=float(lifecycle_fraction))
        if isinstance(spot_gate, SkipDecision):
            return spot_gate
        entry_price = float(event.best_ask)
        if spot_gate is not None:
            entry_price = spot_gate["entry_price"]
        return OrderIntent(
            intent_id=f"{state.market_id}:{state.token_id}:{touch.level:.2f}:{event.event_time_ms}",
            strategy_id=self.config.strategy_id,
            market_id=state.market_id,
            token_id=state.token_id,
            condition_id=state.condition_id,
            question=state.question,
            asset_slug=state.asset_slug,
            level=touch.level,
            entry_price=entry_price,
            notional_usdc=self.config.execution.target_trade_size_usdc,
            lifecycle_fraction=float(lifecycle_fraction),
            event_time_ms=event.event_time_ms,
        )

    def _evaluate_spot_dislocation_gate(self, *, event: BookUpdate, state: MarketState, lifecycle_fraction: float) -> dict[str, float] | SkipDecision | None:
        cfg = getattr(self.config, "spot_dislocation", None)
        if cfg is None or not bool(getattr(cfg, "enabled", False)):
            return None

        asset = _normalize_asset_slug(state.asset_slug)
        allowed_assets = {_normalize_asset_slug(item) for item in getattr(cfg, "asset_allowlist", ())}
        if asset not in allowed_assets:
            return _skip("spot_dislocation_asset_not_allowed", event=event, level=event.best_ask or 0.0, details=asset)

        direction = _token_direction(state=state, token_id=event.token_id)
        allowed_directions = {str(item).upper() for item in getattr(cfg, "direction_allowlist", ())}
        if direction not in allowed_directions:
            return _skip("spot_dislocation_direction_not_allowed", event=event, level=event.best_ask or 0.0, details=direction)

        if lifecycle_fraction < float(getattr(cfg, "min_lifecycle_fraction", 0.0)):
            return _skip("spot_dislocation_lifecycle_below_min", event=event, level=event.best_ask or 0.0, details=f"{lifecycle_fraction:.4f}")

        latest_spot = self.latest_spot_by_asset.get(asset)
        if latest_spot is None:
            return _skip("spot_dislocation_missing_spot", event=event, level=event.best_ask or 0.0, details=asset)
        spot_age_ms = abs(int(event.event_time_ms) - int(latest_spot.event_time_ms))
        if spot_age_ms > int(getattr(cfg, "max_spot_staleness_ms", 5_000)):
            return _skip("spot_dislocation_stale_spot", event=event, level=event.best_ask or 0.0, details=str(spot_age_ms))

        start_spot = self._nearest_spot(asset=asset, target_ms=int(state.start_time_ms), max_latency_ms=int(getattr(cfg, "max_start_spot_latency_ms", 5_000)))
        if start_spot is None:
            return _skip("spot_dislocation_missing_start_spot", event=event, level=event.best_ask or 0.0, details=asset)

        sigma = self._realized_vol(asset=asset, end_ms=int(event.event_time_ms), lookback_ms=300_000, min_points=int(getattr(cfg, "min_vol_lookback_points", 5)))
        seconds_to_resolution = max(0.0, (int(state.end_time_ms) - int(event.event_time_ms)) / 1000.0)
        spot_prob = _spot_implied_prob(
            spot=float(latest_spot.spot_price),
            strike=float(start_spot.spot_price),
            sigma_per_sqrt_second=sigma,
            seconds_to_resolution=seconds_to_resolution,
            direction=direction,
        )
        if spot_prob is None:
            return _skip("spot_dislocation_probability_unavailable", event=event, level=event.best_ask or 0.0, details="spot_prob_none")

        entry = _fit_10_entry_price(event=event, target_usdc=float(self.config.execution.target_trade_size_usdc), tick_size=state.tick_size or self.config.triggers.tick_size)
        if entry is None:
            return _skip("spot_dislocation_fit_10_not_allowed", event=event, level=event.best_ask or 0.0, details="insufficient_depth_plus_one_tick")

        spot_gap = spot_prob - entry
        if spot_gap < float(getattr(cfg, "min_spot_gap", 0.0)):
            return _skip("spot_dislocation_gap_below_min", event=event, level=event.best_ask or 0.0, details=f"{spot_gap:.6f}")
        effective_fee_bps = effective_fee_rate_bps(fee_info=state.fee_info, fee_rate_bps=state.fee_rate_bps)
        fee_gap = _break_even_fee_gap(entry, effective_fee_bps or 0.0)
        required_gap = fee_gap + float(getattr(cfg, "edge_buffer_bps", 0.0)) / 10000.0
        if spot_gap < required_gap:
            return _skip("spot_dislocation_edge_below_required_gap", event=event, level=event.best_ask or 0.0, details=f"{spot_gap:.6f}<{required_gap:.6f}")
        return {"entry_price": entry, "spot_implied_prob": spot_prob, "spot_gap": spot_gap}

    def _nearest_spot(self, *, asset: str, target_ms: int, max_latency_ms: int) -> SpotPriceUpdate | None:
        best: SpotPriceUpdate | None = None
        best_delta: int | None = None
        for item in self.spot_history_by_asset.get(asset, []):
            delta = abs(int(item.event_time_ms) - target_ms)
            if delta <= max_latency_ms and (best_delta is None or delta < best_delta):
                best = item
                best_delta = delta
        return best

    def _realized_vol(self, *, asset: str, end_ms: int, lookback_ms: int, min_points: int) -> float | None:
        points = [item for item in self.spot_history_by_asset.get(asset, []) if end_ms - lookback_ms <= int(item.event_time_ms) <= end_ms and float(item.spot_price) > 0]
        if len(points) < min_points:
            return None
        points.sort(key=lambda item: int(item.event_time_ms))
        returns = [math.log(float(points[idx].spot_price) / float(points[idx - 1].spot_price)) for idx in range(1, len(points))]
        if not returns:
            return None
        mean = sum(returns) / len(returns)
        variance = sum((item - mean) ** 2 for item in returns) / len(returns)
        return math.sqrt(variance)

    def _has_active_order(self, *, market_id: str, token_id: str) -> bool:
        return (market_id, token_id) in self.active_order_ids_by_market_token


def _row_has_inventory_exposure(row: dict, *, state: OrderState) -> bool:
    if state is OrderState.FILLED:
        return True
    cumulative_filled_size = row.get("cumulative_filled_size")
    if cumulative_filled_size is None:
        return False
    try:
        return float(cumulative_filled_size) > 1e-12
    except (TypeError, ValueError):
        return False


def _skip(reason: str, *, event: BookUpdate, level: float, details: str = "") -> SkipDecision:
    return SkipDecision(reason=reason, market_id=event.market_id, token_id=event.token_id, level=float(level), event_time_ms=event.event_time_ms, details=details)


def _normalize_asset_slug(value: str | None) -> str:
    text = str(value or "").strip().lower()
    return {
        "bitcoin": "btc",
        "btc": "btc",
        "ethereum": "eth",
        "eth": "eth",
        "solana": "sol",
        "sol": "sol",
        "xrp": "xrp",
        "ripple": "xrp",
    }.get(text, text)


def _token_direction(*, state: MarketState, token_id: str) -> str:
    if state.token_no_id is not None and str(token_id) == str(state.token_no_id):
        return "DOWN/NO"
    if state.token_yes_id is not None and str(token_id) == str(state.token_yes_id):
        return "UP/YES"
    return "UNKNOWN"


def _fit_10_entry_price(*, event: BookUpdate, target_usdc: float, tick_size: float) -> float | None:
    if event.best_ask is None or event.best_ask <= 0:
        return None
    best_ask = float(event.best_ask)
    tick = float(tick_size or 0.01)
    needed_shares = target_usdc / best_ask
    same_tick_size = sum(level.size for level in event.ask_levels if float(level.price) <= best_ask + 1e-12)
    if same_tick_size >= needed_shares:
        return best_ask
    plus_one = min(0.99, best_ask + tick)
    plus_one_size = sum(level.size for level in event.ask_levels if float(level.price) <= plus_one + 1e-12)
    if plus_one_size >= needed_shares:
        return plus_one
    return None


def _spot_implied_prob(*, spot: float, strike: float, sigma_per_sqrt_second: float | None, seconds_to_resolution: float, direction: str) -> float | None:
    if spot <= 0 or strike <= 0 or seconds_to_resolution <= 0:
        return None
    sigma = sigma_per_sqrt_second or 0.0
    if sigma <= 0:
        up_prob = 1.0 if spot > strike else 0.0 if spot < strike else 0.5
    else:
        z = (strike - spot) / (spot * sigma * math.sqrt(seconds_to_resolution))
        up_prob = 1.0 - (0.5 * (1.0 + math.erf(z / math.sqrt(2.0))))
    if direction.upper().startswith("DOWN"):
        return 1.0 - up_prob
    if direction.upper().startswith("UP"):
        return up_prob
    return None


def _break_even_fee_gap(market_price: float, fee_rate_bps: float) -> float:
    denominator = 10000.0 - float(fee_rate_bps or 0.0)
    if denominator <= 0:
        return float("inf")
    return market_price * float(fee_rate_bps or 0.0) / denominator
