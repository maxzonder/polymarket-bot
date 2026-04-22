from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from uuid import NAMESPACE_URL, uuid5

from ..core.models import OrderIntent
from ..venue_polymarket.execution_client import VenueOrderRequest
from ..venue_polymarket.markets import MarketMetadata


class VenueTranslationError(RuntimeError):
    pass


@dataclass(frozen=True)
class VenueConstraints:
    tick_size: float
    min_order_size: float
    min_order_shares: float | None = None
    size_decimals: int = 6
    min_notional_buffer: float = 0.01


@dataclass(frozen=True)
class TranslationPolicy:
    buy_price_rounding: str = "down"
    sell_price_rounding: str = "up"


def translate_place_order(
    intent: OrderIntent,
    market_meta: MarketMetadata,
    venue_constraints: VenueConstraints,
    *,
    client_order_id_seed: str | None = None,
    policy: TranslationPolicy | None = None,
) -> VenueOrderRequest:
    policy = policy or TranslationPolicy()
    _validate_token_membership(intent=intent, market_meta=market_meta)
    if intent.entry_price <= 0:
        raise VenueTranslationError(f"Intent {intent.intent_id} has non-positive entry_price={intent.entry_price}")

    rounded_price = _round_price(
        price=intent.entry_price,
        tick_size=venue_constraints.tick_size,
        direction=policy.buy_price_rounding,
    )
    target_notional = _target_buy_notional(intent=intent, venue_constraints=venue_constraints, rounded_price=rounded_price)
    raw_size = target_notional / rounded_price
    rounded_size = _round_size(raw_size, decimals=venue_constraints.size_decimals, direction="up")
    effective_notional = rounded_price * rounded_size
    minimum_notional = venue_constraints.min_order_size
    if effective_notional + 1e-12 < minimum_notional:
        raise VenueTranslationError(
            f"Intent {intent.intent_id} rounds to effective notional {effective_notional:.6f} below venue minimum {minimum_notional}"
        )
    minimum_shares = venue_constraints.min_order_shares
    if minimum_shares is not None and rounded_size + 1e-12 < minimum_shares:
        raise VenueTranslationError(
            f"Intent {intent.intent_id} rounds to size {rounded_size:.6f} below venue minimum shares {minimum_shares}"
        )

    client_order_id = _client_order_id(intent=intent, seed=client_order_id_seed)
    return VenueOrderRequest(
        token_id=intent.token_id,
        side="BUY",
        price=rounded_price,
        size=rounded_size,
        client_order_id=client_order_id,
        time_in_force="GTC",
        post_only=False,
    )


def _validate_token_membership(*, intent: OrderIntent, market_meta: MarketMetadata) -> None:
    allowed_tokens = {market_meta.token_yes_id, market_meta.token_no_id}
    if intent.token_id not in allowed_tokens:
        raise VenueTranslationError(
            f"Intent {intent.intent_id} token_id {intent.token_id} does not belong to market {market_meta.market_id}"
        )


def _round_price(*, price: float, tick_size: float, direction: str) -> float:
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    price_decimal = Decimal(str(price))
    tick_decimal = Decimal(str(tick_size))
    units = price_decimal / tick_decimal
    if direction == "down":
        rounded_units = units.quantize(Decimal("1"), rounding=ROUND_DOWN)
    elif direction == "up":
        rounded_units = units.quantize(Decimal("1"), rounding=ROUND_CEILING)
    else:
        raise ValueError(f"Unsupported price rounding direction: {direction}")
    return float(rounded_units * tick_decimal)


def _round_size(size: float, *, decimals: int, direction: str = "down") -> float:
    quantum = Decimal("1").scaleb(-int(decimals))
    rounding = ROUND_DOWN if direction == "down" else ROUND_CEILING
    return float(Decimal(str(size)).quantize(quantum, rounding=rounding))


def _target_buy_notional(*, intent: OrderIntent, venue_constraints: VenueConstraints, rounded_price: float) -> float:
    minimum_notional = max(float(venue_constraints.min_order_size), 0.0)
    minimum_shares = venue_constraints.min_order_shares
    minimum_shares_notional = 0.0
    if minimum_shares is not None and rounded_price > 0:
        minimum_shares_notional = max(float(minimum_shares), 0.0) * float(rounded_price)
    buffered_minimum = 0.0
    if intent.notional_usdc >= minimum_notional - 1e-12:
        buffered_minimum = minimum_notional + max(float(venue_constraints.min_notional_buffer), 0.0)
    return max(float(intent.notional_usdc), buffered_minimum, minimum_shares_notional)


def _client_order_id(*, intent: OrderIntent, seed: str | None) -> str:
    material = f"{seed or 'short-horizon'}:{intent.intent_id}:{intent.market_id}:{intent.token_id}"
    return str(uuid5(NAMESPACE_URL, material))


__all__ = [
    "TranslationPolicy",
    "VenueConstraints",
    "VenueTranslationError",
    "translate_place_order",
]
