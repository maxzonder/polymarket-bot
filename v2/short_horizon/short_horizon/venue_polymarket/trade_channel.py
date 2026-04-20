from __future__ import annotations

import json
import time
from typing import Any

from ..core.events import AggressorSide, MarketStateUpdate, TradeTick


class TradeNormalizer:
    """Normalize Polymarket market-channel trade frames into canonical TradeTick events."""

    def __init__(self, *, source: str = "polymarket_clob_ws"):
        self.source = source
        self.condition_to_market_id: dict[str, str] = {}
        self.token_to_market_id: dict[str, str] = {}

    def register_market(self, event: MarketStateUpdate) -> None:
        market_id = str(event.market_id or "").strip()
        if not market_id:
            return
        condition_id = str(event.condition_id or "").strip()
        if condition_id:
            self.condition_to_market_id[condition_id] = market_id
        for token_id in (event.token_yes_id, event.token_no_id, event.token_id):
            token_text = str(token_id or "").strip()
            if token_text:
                self.token_to_market_id[token_text] = market_id

    def normalize_frame(self, frame: str | dict[str, Any] | list[Any], *, ingest_time_ms: int | None = None) -> list[TradeTick]:
        payload: Any = frame
        if isinstance(frame, str):
            payload = json.loads(frame)
        if isinstance(payload, list):
            updates: list[TradeTick] = []
            for item in payload:
                if isinstance(item, dict):
                    updates.extend(self.normalize_event(item, ingest_time_ms=ingest_time_ms))
            return updates
        if not isinstance(payload, dict):
            return []
        return self.normalize_event(payload, ingest_time_ms=ingest_time_ms)

    def normalize_event(self, event: dict[str, Any], *, ingest_time_ms: int | None = None) -> list[TradeTick]:
        if str(event.get("event_type") or "") != "last_trade_price":
            return []
        token_id = str(event.get("asset_id") or "")
        raw_market_id = str(event.get("market") or "").strip()
        market_id = self._resolve_market_id(raw_market_id, token_id=token_id) or ""
        price = _parse_float(event.get("price"))
        size = _parse_float(event.get("size"))
        event_time_ms = _parse_int(event.get("timestamp"))
        if not token_id or not market_id or price is None or size is None or event_time_ms is None:
            return []
        resolved_ingest_time_ms = int(ingest_time_ms if ingest_time_ms is not None else time.time() * 1000)
        side = _parse_aggressor_side(event.get("side"))
        trade_id = _parse_optional_str(event.get("trade_id") or event.get("id"))
        venue_seq = _parse_int(event.get("seq") or event.get("sequence") or event.get("venue_seq"))
        return [
            TradeTick(
                event_time_ms=event_time_ms,
                ingest_time_ms=resolved_ingest_time_ms,
                market_id=market_id,
                token_id=token_id,
                price=price,
                size=size,
                source=self.source,
                trade_id=trade_id,
                aggressor_side=side,
                venue_seq=venue_seq,
            )
        ]

    def _resolve_market_id(self, raw_market_id: str | None, *, token_id: str | None = None) -> str | None:
        if raw_market_id:
            resolved = self.condition_to_market_id.get(raw_market_id)
            if resolved:
                return resolved
            if raw_market_id in self.token_to_market_id.values():
                return raw_market_id
        if token_id:
            resolved = self.token_to_market_id.get(token_id)
            if resolved:
                return resolved
        return raw_market_id


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


def _parse_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_aggressor_side(value: Any) -> AggressorSide | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text == "buy":
        return AggressorSide.BUY
    if text == "sell":
        return AggressorSide.SELL
    return None


__all__ = ["TradeNormalizer"]
