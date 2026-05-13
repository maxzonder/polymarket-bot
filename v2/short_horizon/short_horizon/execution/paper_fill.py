from __future__ import annotations

from dataclasses import dataclass
import re

from ..core.events import BookLevel, BookUpdate, TradeTick
from ..core.order_state import OrderState
from . import ExecutionEngine, ExecutionTransitionError, SyntheticFillRequest


_ACTIVE_FILLABLE_STATES = {OrderState.ACCEPTED.value, OrderState.PARTIALLY_FILLED.value}


@dataclass(frozen=True)
class PaperFillModelConfig:
    """Conservative first-pass paper fill settings.

    `crossing_taker` intentionally models executable/crossing dry-run orders, not
    maker queue priority. Maker queue simulation can be layered on top once we
    persist post-only/queue metadata for orders.
    """

    model: str = "crossing_taker"
    fill_without_visible_depth: bool = True
    post_only_book_cross_fills: bool = True
    source_prefix: str = "replay"


class PaperFillSimulator:
    """Generate synthetic fills for dry-run orders from market data.

    The first implementation covers the obvious accounting gap: if a dry-run BUY
    limit is accepted and the current book ask is at/below the limit, the order is
    executable and should receive a synthetic fill. SELL support is implemented
    symmetrically for exit work, even though current OrderIntent translation is
    still BUY-only.
    """

    def __init__(self, config: PaperFillModelConfig | None = None):
        self.config = config or PaperFillModelConfig()
        self._applied_keys: set[tuple[str, int, str, str]] = set()

    def on_book_update(self, event: BookUpdate, *, execution: ExecutionEngine):
        if self.config.model == "none":
            return []
        fills = []
        matching_orders = self._matching_orders(event.market_id, event.token_id, execution=execution)
        if self.config.post_only_book_cross_fills:
            for request in self._post_only_book_fill_requests(matching_orders, event):
                fill = self._apply_fill_request(request, execution=execution)
                if fill is not None:
                    fills.append(fill)
        for order in matching_orders:
            if _is_post_only(order):
                continue
            request = self._fill_request_from_book(order, event)
            if request is None:
                continue
            fill = self._apply_fill_request(request, execution=execution)
            if fill is not None:
                fills.append(fill)
        return fills

    def on_trade_tick(self, event: TradeTick, *, execution: ExecutionEngine):
        if self.config.model == "none":
            return []
        fills = []
        remaining_trade_size = max(float(event.size), 0.0)
        if remaining_trade_size <= 1e-12:
            return []
        for order in self._matching_orders(event.market_id, event.token_id, execution=execution):
            if remaining_trade_size <= 1e-12:
                break
            request = self._fill_request_from_trade(order, event, max_fill_size=remaining_trade_size)
            if request is None:
                continue
            fill = self._apply_fill_request(request, execution=execution)
            if fill is None:
                continue
            remaining_trade_size = max(remaining_trade_size - float(fill.fill_size), 0.0)
            fills.append(fill)
        return fills

    def _apply_fill_request(self, request: SyntheticFillRequest, *, execution: ExecutionEngine):
        key = (request.order_id, request.event_time_ms, request.source, request.fill_id or "")
        if key in self._applied_keys:
            return None
        try:
            fill = execution.apply_fill(request)
        except ExecutionTransitionError:
            return None
        if fill is None:
            return None
        self._applied_keys.add(key)
        return fill

    @staticmethod
    def _matching_orders(market_id: str, token_id: str, *, execution: ExecutionEngine) -> list[dict]:
        orders = []
        for row in execution.store.load_non_terminal_orders():
            if str(row.get("market_id")) != str(market_id):
                continue
            if str(row.get("token_id")) != str(token_id):
                continue
            if str(row.get("state")) not in _ACTIVE_FILLABLE_STATES:
                continue
            if float(row.get("remaining_size") or 0.0) <= 1e-12:
                continue
            if row.get("price") is None:
                continue
            orders.append(row)
        return orders

    def _fill_request_from_book(self, order: dict, event: BookUpdate) -> SyntheticFillRequest | None:
        side = str(order.get("side") or "").upper()
        limit_price = float(order.get("price") or 0.0)
        remaining_size = float(order.get("remaining_size") or order.get("size") or 0.0)
        if side == "BUY":
            if event.best_ask is None or float(event.best_ask) > limit_price + 1e-12:
                return None
            fill_size, fill_price = self._consume_book_levels(
                levels=event.ask_levels,
                limit_price=limit_price,
                remaining_size=remaining_size,
                side=side,
                fallback_price=float(event.best_ask),
            )
        elif side == "SELL":
            if event.best_bid is None or float(event.best_bid) < limit_price - 1e-12:
                return None
            fill_size, fill_price = self._consume_book_levels(
                levels=event.bid_levels,
                limit_price=limit_price,
                remaining_size=remaining_size,
                side=side,
                fallback_price=float(event.best_bid),
            )
        else:
            return None
        if fill_size <= 1e-12:
            return None
        return SyntheticFillRequest(
            order_id=str(order["order_id"]),
            event_time_ms=int(event.event_time_ms),
            fill_size=fill_size,
            fill_price=fill_price,
            source=self.config.source_prefix,
            liquidity_role="taker",
            fill_id=_book_fill_id(order=order, event=event, fill_size=fill_size, fill_price=fill_price, source=self.config.source_prefix),
        )

    def _post_only_book_fill_requests(self, orders: list[dict], event: BookUpdate) -> list[SyntheticFillRequest]:
        """Model shadow maker fills from crossed book snapshots.

        Real post-only orders rest on the bid/ask.  In dry-run they are absent
        from the venue book, so waiting only for trade prints at the exact limit
        price undercounts fills: the public book can later show contra liquidity
        crossing our shadow limit.  For those snapshots, consume visible contra
        depth once, price fills at the resting limit, and allocate priority by
        best price then order creation time.
        """
        post_only_orders = [order for order in orders if _is_post_only(order)]
        if not post_only_orders:
            return []

        requests: list[SyntheticFillRequest] = []
        buy_levels = [[float(level.price), max(float(level.size), 0.0)] for level in event.ask_levels]
        sell_levels = [[float(level.price), max(float(level.size), 0.0)] for level in event.bid_levels]

        buy_orders = [order for order in post_only_orders if str(order.get("side") or "").upper() == "BUY"]
        buy_orders.sort(key=lambda row: (-float(row.get("price") or 0.0), _order_created_sort_key(row)))
        for order in buy_orders:
            limit_price = float(order.get("price") or 0.0)
            if event.best_ask is None or float(event.best_ask) > limit_price + 1e-12:
                continue
            fill_size = _consume_visible_cross_depth(
                levels=buy_levels,
                limit_price=limit_price,
                remaining_size=float(order.get("remaining_size") or order.get("size") or 0.0),
                side="BUY",
            )
            if fill_size <= 1e-12:
                continue
            requests.append(_book_synthetic_request(order=order, event=event, fill_size=fill_size, fill_price=limit_price, source=self.config.source_prefix))

        sell_orders = [order for order in post_only_orders if str(order.get("side") or "").upper() == "SELL"]
        sell_orders.sort(key=lambda row: (float(row.get("price") or 0.0), _order_created_sort_key(row)))
        for order in sell_orders:
            limit_price = float(order.get("price") or 0.0)
            if event.best_bid is None or float(event.best_bid) < limit_price - 1e-12:
                continue
            fill_size = _consume_visible_cross_depth(
                levels=sell_levels,
                limit_price=limit_price,
                remaining_size=float(order.get("remaining_size") or order.get("size") or 0.0),
                side="SELL",
            )
            if fill_size <= 1e-12:
                continue
            requests.append(_book_synthetic_request(order=order, event=event, fill_size=fill_size, fill_price=limit_price, source=self.config.source_prefix))

        return requests

    def _fill_request_from_trade(self, order: dict, event: TradeTick, *, max_fill_size: float | None = None) -> SyntheticFillRequest | None:
        side = str(order.get("side") or "").upper()
        limit_price = float(order.get("price") or 0.0)
        remaining_size = float(order.get("remaining_size") or order.get("size") or 0.0)
        trade_price = float(event.price)
        post_only = _is_post_only(order)
        aggressor_side = getattr(event.aggressor_side, "value", event.aggressor_side)
        if side not in {"BUY", "SELL"}:
            return None
        if post_only:
            # Maker/post-only fills require explicit contra-side taker flow and
            # the trade print must be at our resting price.  A lower SELL print
            # should not fill higher BUY bids: on a price-time priority book
            # those higher bids would have printed at their own prices first.
            normalized_aggressor = str(aggressor_side or "").lower()
            if side == "BUY" and normalized_aggressor != "sell":
                return None
            if side == "SELL" and normalized_aggressor != "buy":
                return None
            if abs(trade_price - limit_price) > 1e-12:
                return None
        else:
            if side == "BUY" and trade_price > limit_price + 1e-12:
                return None
            if side == "SELL" and trade_price < limit_price - 1e-12:
                return None
        fill_size = min(remaining_size, max(float(event.size), 0.0))
        if max_fill_size is not None:
            fill_size = min(fill_size, max(float(max_fill_size), 0.0))
        if fill_size <= 1e-12:
            return None
        return SyntheticFillRequest(
            order_id=str(order["order_id"]),
            event_time_ms=int(event.event_time_ms),
            fill_size=fill_size,
            fill_price=trade_price,
            source=self.config.source_prefix,
            liquidity_role="maker" if post_only else "taker",
            fill_id=_trade_fill_id(order=order, event=event, fill_size=fill_size, source=self.config.source_prefix),
        )

    def _consume_book_levels(
        self,
        *,
        levels: tuple[BookLevel, ...],
        limit_price: float,
        remaining_size: float,
        side: str,
        fallback_price: float,
    ) -> tuple[float, float]:
        eligible: list[BookLevel] = []
        for level in levels:
            price = float(level.price)
            if side == "BUY" and price <= limit_price + 1e-12:
                eligible.append(level)
            elif side == "SELL" and price >= limit_price - 1e-12:
                eligible.append(level)
        if not eligible:
            if not self.config.fill_without_visible_depth:
                return 0.0, fallback_price
            return remaining_size, fallback_price

        fill_size = 0.0
        notional = 0.0
        for level in eligible:
            if fill_size >= remaining_size - 1e-12:
                break
            take = min(remaining_size - fill_size, max(float(level.size), 0.0))
            if take <= 0:
                continue
            fill_size += take
            notional += take * float(level.price)
        if fill_size <= 1e-12:
            return 0.0, fallback_price
        return fill_size, notional / fill_size


def _is_post_only(order: dict) -> bool:
    value = order.get("post_only")
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _order_created_sort_key(order: dict) -> str:
    return str(order.get("intent_created_at") or order.get("created_at") or order.get("order_id") or "")


def _consume_visible_cross_depth(
    *,
    levels: list[list[float]],
    limit_price: float,
    remaining_size: float,
    side: str,
) -> float:
    if remaining_size <= 1e-12:
        return 0.0
    if side == "BUY":
        levels.sort(key=lambda level: level[0])
        eligible = lambda price: price <= limit_price + 1e-12
    elif side == "SELL":
        levels.sort(key=lambda level: level[0], reverse=True)
        eligible = lambda price: price >= limit_price - 1e-12
    else:
        return 0.0

    fill_size = 0.0
    for level in levels:
        price, available = level
        if not eligible(float(price)):
            continue
        take = min(remaining_size - fill_size, max(float(available), 0.0))
        if take <= 0:
            continue
        level[1] = max(float(available) - take, 0.0)
        fill_size += take
        if fill_size >= remaining_size - 1e-12:
            break
    return fill_size


def _book_synthetic_request(*, order: dict, event: BookUpdate, fill_size: float, fill_price: float, source: str) -> SyntheticFillRequest:
    return SyntheticFillRequest(
        order_id=str(order["order_id"]),
        event_time_ms=int(event.event_time_ms),
        fill_size=fill_size,
        fill_price=fill_price,
        source=source,
        liquidity_role="maker",
        fill_id=_book_fill_id(order=order, event=event, fill_size=fill_size, fill_price=fill_price, source=source),
    )


def _trade_fill_id(*, order: dict, event: TradeTick, fill_size: float, source: str) -> str:
    identity = _trade_identity(event)
    return ":".join(
        [
            str(order["order_id"]),
            "trade_fill",
            _fill_id_component(source),
            str(int(event.event_time_ms)),
            identity,
            str(int(round(float(event.price) * 1_000_000))),
            str(int(round(float(fill_size) * 1_000_000))),
        ]
    )


def _trade_identity(event: TradeTick) -> str:
    if event.trade_id:
        return f"trade_{_fill_id_component(event.trade_id)}"
    if event.venue_seq is not None:
        return f"seq_{int(event.venue_seq)}"
    aggressor = getattr(event.aggressor_side, "value", event.aggressor_side)
    return ":".join(
        [
            "fallback",
            str(int(event.ingest_time_ms)),
            _fill_id_component(aggressor),
            str(int(round(float(event.size) * 1_000_000))),
        ]
    )


def _book_fill_id(*, order: dict, event: BookUpdate, fill_size: float, fill_price: float, source: str) -> str:
    if event.book_seq is not None:
        identity = f"seq_{int(event.book_seq)}"
    else:
        identity = ":".join(
            [
                "fallback",
                str(int(event.ingest_time_ms)),
                str(int(round(float(event.best_bid or 0.0) * 1_000_000))),
                str(int(round(float(event.best_ask or 0.0) * 1_000_000))),
            ]
        )
    return ":".join(
        [
            str(order["order_id"]),
            "book_fill",
            _fill_id_component(source),
            str(int(event.event_time_ms)),
            identity,
            str(int(round(float(fill_price) * 1_000_000))),
            str(int(round(float(fill_size) * 1_000_000))),
        ]
    )


def _fill_id_component(value: object) -> str:
    text = str(value or "none")
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", text)[:120] or "none"


__all__ = ["PaperFillModelConfig", "PaperFillSimulator"]
