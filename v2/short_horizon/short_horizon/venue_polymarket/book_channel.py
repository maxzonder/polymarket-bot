from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from ..core.events import BookLevel, BookUpdate


@dataclass
class BookState:
    token_id: str
    market_id: str | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    bid_levels: list[tuple[float, float]] = field(default_factory=list)
    ask_levels: list[tuple[float, float]] = field(default_factory=list)
    last_event_ts_ms: int | None = None
    last_book_hash: str | None = None


class BookNormalizer:
    """Normalize Polymarket CLOB public book frames into canonical BookUpdate events.

    Out-of-order policy: if a frame for a token has `timestamp` older than the last
    applied timestamp for that token, the frame is dropped. This keeps replay output
    deterministic and avoids regressing book state on reconnect races.
    """

    def __init__(self, *, source: str = "polymarket_clob_ws"):
        self.source = source
        self.books: dict[str, BookState] = {}

    def normalize_frame(self, frame: str | dict[str, Any] | list[Any], *, ingest_time_ms: int | None = None) -> list[BookUpdate]:
        payload: Any = frame
        if isinstance(frame, str):
            payload = json.loads(frame)
        if isinstance(payload, list):
            updates: list[BookUpdate] = []
            for item in payload:
                if isinstance(item, dict):
                    updates.extend(self.normalize_event(item, ingest_time_ms=ingest_time_ms))
            return updates
        if not isinstance(payload, dict):
            return []
        return self.normalize_event(payload, ingest_time_ms=ingest_time_ms)

    def normalize_event(self, event: dict[str, Any], *, ingest_time_ms: int | None = None) -> list[BookUpdate]:
        event_type = str(event.get("event_type") or "")
        resolved_ingest_time_ms = int(ingest_time_ms if ingest_time_ms is not None else time.time() * 1000)
        if event_type == "book":
            update = self._apply_book(event, ingest_time_ms=resolved_ingest_time_ms)
            return [update] if update is not None else []
        if event_type == "price_change":
            return self._apply_price_change(event, ingest_time_ms=resolved_ingest_time_ms)
        if event_type == "best_bid_ask":
            update = self._apply_best_bid_ask(event, ingest_time_ms=resolved_ingest_time_ms)
            return [update] if update is not None else []
        return []

    def _ensure_book(self, token_id: str, market_id: str | None = None) -> BookState:
        book = self.books.get(token_id)
        if book is None:
            book = BookState(token_id=token_id, market_id=market_id)
            self.books[token_id] = book
        if market_id:
            book.market_id = market_id
        return book

    def _apply_book(self, event: dict[str, Any], *, ingest_time_ms: int) -> BookUpdate | None:
        token_id = str(event.get("asset_id") or "")
        if not token_id:
            return None
        event_time_ms = _parse_int(event.get("timestamp"))
        book = self._ensure_book(token_id, str(event.get("market") or "") or None)
        if _is_out_of_order(book, event_time_ms):
            return None
        book.bid_levels = parse_levels(event.get("bids"), reverse=True)
        book.ask_levels = parse_levels(event.get("asks"), reverse=False)
        book.best_bid = book.bid_levels[0][0] if book.bid_levels else None
        book.best_ask = book.ask_levels[0][0] if book.ask_levels else None
        book.last_event_ts_ms = event_time_ms
        book.last_book_hash = str(event.get("hash") or "") or None
        return self._to_update(book, event_time_ms=event_time_ms, ingest_time_ms=ingest_time_ms, is_snapshot=True)

    def _apply_price_change(self, event: dict[str, Any], *, ingest_time_ms: int) -> list[BookUpdate]:
        changes = event.get("price_changes") or []
        event_time_ms = _parse_int(event.get("timestamp"))
        market_id = str(event.get("market") or "") or None
        touched: list[BookState] = []
        touched_ids: set[str] = set()
        for change in changes:
            if not isinstance(change, dict):
                continue
            token_id = str(change.get("asset_id") or "")
            if not token_id:
                continue
            book = self._ensure_book(token_id, market_id)
            if _is_out_of_order(book, event_time_ms):
                continue
            side = str(change.get("side") or "").upper()
            price = _parse_float(change.get("price"))
            size = _parse_float(change.get("size"))
            if price is not None and size is not None:
                if side == "BUY":
                    book.bid_levels = upsert_level(book.bid_levels, price, size, reverse=True)
                elif side == "SELL":
                    book.ask_levels = upsert_level(book.ask_levels, price, size, reverse=False)
            best_bid = _parse_float(change.get("best_bid"))
            best_ask = _parse_float(change.get("best_ask"))
            if best_bid is not None:
                book.best_bid = best_bid
            elif book.bid_levels:
                book.best_bid = book.bid_levels[0][0]
            else:
                book.best_bid = None
            if best_ask is not None:
                book.best_ask = best_ask
            elif book.ask_levels:
                book.best_ask = book.ask_levels[0][0]
            else:
                book.best_ask = None
            book.last_event_ts_ms = event_time_ms
            if token_id not in touched_ids:
                touched.append(book)
                touched_ids.add(token_id)
        return [
            self._to_update(book, event_time_ms=event_time_ms, ingest_time_ms=ingest_time_ms, is_snapshot=False)
            for book in touched
        ]

    def _apply_best_bid_ask(self, event: dict[str, Any], *, ingest_time_ms: int) -> BookUpdate | None:
        token_id = str(event.get("asset_id") or "")
        if not token_id:
            return None
        event_time_ms = _parse_int(event.get("timestamp"))
        book = self._ensure_book(token_id, str(event.get("market") or "") or None)
        if _is_out_of_order(book, event_time_ms):
            return None
        book.best_bid = _parse_float(event.get("best_bid"))
        book.best_ask = _parse_float(event.get("best_ask"))
        book.last_event_ts_ms = event_time_ms
        return self._to_update(book, event_time_ms=event_time_ms, ingest_time_ms=ingest_time_ms, is_snapshot=False)

    def _to_update(self, book: BookState, *, event_time_ms: int | None, ingest_time_ms: int, is_snapshot: bool) -> BookUpdate:
        spread = None
        mid_price = None
        if book.best_bid is not None and book.best_ask is not None:
            spread = book.best_ask - book.best_bid
            mid_price = (book.best_ask + book.best_bid) / 2.0
        return BookUpdate(
            event_time_ms=int(event_time_ms if event_time_ms is not None else ingest_time_ms),
            ingest_time_ms=int(ingest_time_ms),
            market_id=str(book.market_id or ""),
            token_id=book.token_id,
            best_bid=book.best_bid,
            best_ask=book.best_ask,
            spread=spread,
            mid_price=mid_price,
            bid_levels=tuple(BookLevel(price=price, size=size) for price, size in book.bid_levels),
            ask_levels=tuple(BookLevel(price=price, size=size) for price, size in book.ask_levels),
            is_snapshot=is_snapshot,
            source=self.source,
        )


def _is_out_of_order(book: BookState, event_time_ms: int | None) -> bool:
    return event_time_ms is not None and book.last_event_ts_ms is not None and event_time_ms < book.last_event_ts_ms


def parse_levels(raw_levels: Any, *, reverse: bool) -> list[tuple[float, float]]:
    levels: list[tuple[float, float]] = []
    for item in raw_levels or []:
        if not isinstance(item, dict):
            continue
        price = _parse_float(item.get("price"))
        size = _parse_float(item.get("size"))
        if price is None or size is None:
            continue
        if size <= 0:
            continue
        levels.append((price, size))
    levels.sort(key=lambda level: level[0], reverse=reverse)
    return levels


def upsert_level(levels: list[tuple[float, float]], price: float, size: float, *, reverse: bool) -> list[tuple[float, float]]:
    out = [(level_price, level_size) for level_price, level_size in levels if abs(level_price - price) >= 1e-9]
    if size > 0:
        out.append((price, size))
    out.sort(key=lambda level: level[0], reverse=reverse)
    return out


def _parse_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


__all__ = ["BookNormalizer", "BookState", "parse_levels", "upsert_level"]
