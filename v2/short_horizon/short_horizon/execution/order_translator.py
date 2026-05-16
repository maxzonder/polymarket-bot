from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from uuid import NAMESPACE_URL, uuid5

from ..core.events import OrderSide
from ..core.models import OrderIntent
from ..venue_polymarket.execution_client import VenueOrderRequest
from ..venue_polymarket.markets import MarketMetadata

DEFAULT_VENUE_MIN_ORDER_NOTIONAL_USDC = 1.0
DEFAULT_POLYMARKET_MIN_ORDER_SHARES_FALLBACK = 5.0
DEFAULT_VENUE_MIN_NOTIONAL_BUFFER_USDC = 0.01


class VenueTranslationError(RuntimeError):
    pass


@dataclass(frozen=True)
class VenueConstraints:
    tick_size: float
    min_order_size: float
    min_order_shares: float | None = None
    size_decimals: int = 6
    min_notional_buffer: float = DEFAULT_VENUE_MIN_NOTIONAL_BUFFER_USDC


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

    side = OrderSide(str(intent.side))
    price_rounding = policy.buy_price_rounding if side is OrderSide.BUY else policy.sell_price_rounding
    rounded_price = _round_price(
        price=intent.entry_price,
        tick_size=venue_constraints.tick_size,
        direction=price_rounding,
    )
    if rounded_price <= 0:
        # Desired price is below the minimum tick — snap up to tick_size so the order
        # participates in the queue at the cheapest valid price rather than being lost.
        rounded_price = venue_constraints.tick_size
    target_notional = _target_order_notional_from_values(
        side=side,
        notional_usdc=intent.notional_usdc,
        venue_constraints=venue_constraints,
        rounded_price=rounded_price,
    )
    raw_size = float(intent.size_shares) if intent.size_shares is not None else target_notional / rounded_price
    size_rounding = "up" if side is OrderSide.BUY and intent.size_shares is None else "down"
    rounded_size = _round_size(raw_size, decimals=venue_constraints.size_decimals, direction=size_rounding)
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
        side=side.value,
        price=rounded_price,
        size=rounded_size,
        client_order_id=client_order_id,
        time_in_force=intent.time_in_force,
        post_only=bool(intent.post_only),
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


def _target_buy_notional_from_values(
    *,
    notional_usdc: float,
    venue_constraints: VenueConstraints,
    rounded_price: float,
) -> float:
    minimum_notional = max(float(venue_constraints.min_order_size), 0.0)
    minimum_shares = venue_constraints.min_order_shares
    minimum_shares_notional = 0.0
    if minimum_shares is not None and rounded_price > 0:
        minimum_shares_notional = max(float(minimum_shares), 0.0) * float(rounded_price)
    buffered_minimum = 0.0
    if float(notional_usdc) >= minimum_notional - 1e-12:
        buffered_minimum = minimum_notional + max(float(venue_constraints.min_notional_buffer), 0.0)
    return max(float(notional_usdc), buffered_minimum, minimum_shares_notional)


def _target_order_notional_from_values(
    *,
    side: OrderSide,
    notional_usdc: float,
    venue_constraints: VenueConstraints,
    rounded_price: float,
) -> float:
    if side is OrderSide.BUY:
        return _target_buy_notional_from_values(
            notional_usdc=notional_usdc,
            venue_constraints=venue_constraints,
            rounded_price=rounded_price,
        )
    return float(notional_usdc)


def estimate_effective_buy_notional(
    *,
    notional_usdc: float,
    entry_price: float,
    venue_constraints: VenueConstraints,
    policy: TranslationPolicy | None = None,
) -> float:
    """Simulate the BUY translation sizing to get expected submitted notional.

    Pre-submit risk guards use this so their projections match the notional
    the translator will actually push to venue after min-notional / min-shares
    upscaling, rather than the raw intent target.
    """
    policy = policy or TranslationPolicy()
    if entry_price <= 0:
        return float(notional_usdc)
    try:
        rounded_price = _round_price(
            price=entry_price,
            tick_size=venue_constraints.tick_size,
            direction=policy.buy_price_rounding,
        )
    except ValueError:
        return float(notional_usdc)
    if rounded_price <= 0:
        rounded_price = float(venue_constraints.tick_size)
    target_notional = _target_buy_notional_from_values(
        notional_usdc=notional_usdc,
        venue_constraints=venue_constraints,
        rounded_price=rounded_price,
    )
    raw_size = target_notional / rounded_price
    rounded_size = _round_size(raw_size, decimals=venue_constraints.size_decimals, direction="up")
    return float(rounded_price * rounded_size)


def _client_order_id(*, intent: OrderIntent, seed: str | None) -> str:
    material = f"{seed or 'short-horizon'}:{intent.intent_id}:{intent.market_id}:{intent.token_id}"
    return str(uuid5(NAMESPACE_URL, material))


__all__ = [
    "DEFAULT_POLYMARKET_MIN_ORDER_SHARES_FALLBACK",
    "DEFAULT_VENUE_MIN_NOTIONAL_BUFFER_USDC",
    "DEFAULT_VENUE_MIN_ORDER_NOTIONAL_USDC",
    "TranslationPolicy",
    "VenueConstraints",
    "VenueTranslationError",
    "estimate_effective_buy_notional",
    "translate_place_order",
]
