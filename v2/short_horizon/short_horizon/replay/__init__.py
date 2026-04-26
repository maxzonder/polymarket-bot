from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .capture import ReplayCaptureWriter
from .venue_client import CapturedResponseExecutionClient, ReplayFidelityError
from ..core.events import (
    AggressorSide,
    BookLevel,
    BookUpdate,
    EventType,
    LiquidityRole,
    MarketResolvedWithInventory,
    MarketStateUpdate,
    MarketStatus,
    NormalizedEvent,
    OrderAccepted,
    OrderCanceled,
    OrderFilled,
    OrderIntentEvent,
    OrderRejected,
    OrderSide,
    SkipDecisionEvent,
    SpotPriceUpdate,
    TimerEvent,
    TradeTick,
)


class ReplayEventSource:
    """JSONL replay source for normalized short-horizon events."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def load(self) -> list[NormalizedEvent]:
        events: list[NormalizedEvent] = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line_no, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if not isinstance(payload, dict):
                    raise ValueError(f"Replay line {line_no} in {self.path} is not a JSON object")
                events.append(parse_event_record(payload))
        return events


def parse_event_record(payload: dict[str, Any]) -> NormalizedEvent:
    event_type = _event_type_value(payload)

    if event_type == EventType.BOOK_UPDATE:
        return BookUpdate(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            market_id=str(payload["market_id"]),
            token_id=str(payload["token_id"]),
            best_bid=_parse_optional_float(payload.get("best_bid")),
            best_ask=_parse_optional_float(payload.get("best_ask")),
            spread=_parse_optional_float(payload.get("spread")),
            mid_price=_parse_optional_float(payload.get("mid_price")),
            bid_levels=_parse_book_levels(payload.get("bid_levels")),
            ask_levels=_parse_book_levels(payload.get("ask_levels")),
            book_seq=_parse_optional_int(payload.get("book_seq")),
            is_snapshot=bool(payload.get("is_snapshot", False)),
            source=str(payload.get("source", "replay.book_update")),
            run_id=_parse_optional_str(payload.get("run_id")),
        )

    if event_type == EventType.TRADE_TICK:
        aggressor_side = payload.get("aggressor_side")
        return TradeTick(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            market_id=str(payload["market_id"]),
            token_id=str(payload["token_id"]),
            price=float(payload["price"]),
            size=float(payload["size"]),
            source=str(payload.get("source", "replay.trade_tick")),
            trade_id=_parse_optional_str(payload.get("trade_id")),
            aggressor_side=AggressorSide(str(aggressor_side)) if aggressor_side is not None else None,
            venue_seq=_parse_optional_int(payload.get("venue_seq")),
            run_id=_parse_optional_str(payload.get("run_id")),
        )

    if event_type == EventType.MARKET_STATE_UPDATE:
        status = payload.get("status")
        is_active = payload.get("is_active")
        return MarketStateUpdate(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            market_id=str(payload["market_id"]),
            condition_id=_parse_optional_str(payload.get("condition_id")),
            question=_parse_optional_str(payload.get("question")),
            status=MarketStatus(str(status)) if status is not None else MarketStatus.ACTIVE,
            start_time_ms=_parse_optional_timestamp_ms(payload.get("start_time_ms", payload.get("start_time"))),
            end_time_ms=_parse_optional_timestamp_ms(payload.get("end_time_ms", payload.get("end_time"))),
            duration_seconds=_parse_optional_int(payload.get("duration_seconds")),
            token_yes_id=_parse_optional_str(payload.get("token_yes_id")),
            token_no_id=_parse_optional_str(payload.get("token_no_id")),
            fee_rate_bps=_parse_optional_float(payload.get("fee_rate_bps")),
            fee_fetched_at_ms=_parse_optional_timestamp_ms(payload.get("fee_fetched_at_ms", payload.get("fee_fetched_at"))),
            fees_enabled=_parse_optional_bool(payload.get("fees_enabled")),
            is_ascending_market=_parse_optional_bool(payload.get("is_ascending_market")),
            market_source_revision=_parse_optional_str(payload.get("market_source_revision")),
            source=str(payload.get("source", "replay.market_state")),
            run_id=_parse_optional_str(payload.get("run_id")),
            token_id=_parse_optional_str(payload.get("token_id")),
            asset_slug=_parse_optional_str(payload.get("asset_slug")),
            is_active=bool(is_active) if is_active is not None else None,
            metadata_is_fresh=bool(payload.get("metadata_is_fresh", True)),
            fee_metadata_age_ms=_parse_optional_int(payload.get("fee_metadata_age_ms")),
        )

    if event_type == EventType.TIMER_EVENT:
        return TimerEvent(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            timer_kind=str(payload["timer_kind"]),
            source=str(payload.get("source", "replay.timer")),
            market_id=_parse_optional_str(payload.get("market_id")),
            token_id=_parse_optional_str(payload.get("token_id")),
            deadline_ms=_parse_optional_int(payload.get("deadline_ms")),
            payload=payload.get("payload") if isinstance(payload.get("payload"), dict) else None,
            run_id=_parse_optional_str(payload.get("run_id")),
        )

    if event_type == EventType.SPOT_PRICE_UPDATE:
        return SpotPriceUpdate(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            source=str(payload.get("source", "replay.spot_price")),
            asset_slug=str(payload["asset_slug"]),
            spot_price=float(payload["spot_price"]),
            bid=_parse_optional_float(payload.get("bid")),
            ask=_parse_optional_float(payload.get("ask")),
            staleness_ms=_parse_optional_int(payload.get("staleness_ms")),
            currency=str(payload.get("currency", "USD")),
            venue=_parse_optional_str(payload.get("venue")),
            run_id=_parse_optional_str(payload.get("run_id")),
        )

    if event_type == EventType.ORDER_INTENT:
        return OrderIntentEvent(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            order_id=str(payload["order_id"]),
            strategy_id=str(payload.get("strategy_id", "unknown_strategy")),
            market_id=str(payload["market_id"]),
            token_id=str(payload["token_id"]),
            level=float(payload["level"]),
            entry_price=float(payload["entry_price"]),
            notional_usdc=float(payload["notional_usdc"]),
            lifecycle_fraction=float(payload.get("lifecycle_fraction", 0.0)),
            reason=_parse_optional_str(payload.get("reason")) or "ascending_first_touch",
            source=str(payload.get("source", "runtime.order_intent")),
            run_id=_parse_optional_str(payload.get("run_id")),
        )

    if event_type == EventType.SKIP_DECISION:
        return SkipDecisionEvent(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            reason=str(payload["reason"]),
            market_id=str(payload["market_id"]),
            token_id=str(payload["token_id"]),
            level=float(payload["level"]),
            details=_parse_optional_str(payload.get("details")) or "",
            source=str(payload.get("source", "runtime.skip_decision")),
            run_id=_parse_optional_str(payload.get("run_id")),
        )

    if event_type == EventType.ORDER_ACCEPTED:
        return OrderAccepted(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            order_id=str(payload["order_id"]),
            market_id=str(payload["market_id"]),
            token_id=str(payload["token_id"]),
            side=OrderSide(str(payload["side"])),
            price=float(payload["price"]),
            size=float(payload["size"]),
            source=str(payload.get("source", "replay.order_accepted")),
            client_order_id=_parse_optional_str(payload.get("client_order_id")),
            time_in_force=_parse_optional_str(payload.get("time_in_force")),
            post_only=_parse_optional_bool(payload.get("post_only")),
            venue_status=str(payload.get("venue_status", "live")),
            run_id=_parse_optional_str(payload.get("run_id")),
        )

    if event_type == EventType.ORDER_REJECTED:
        return OrderRejected(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            market_id=str(payload["market_id"]),
            token_id=str(payload["token_id"]),
            side=OrderSide(str(payload["side"])),
            source=str(payload.get("source", "replay.order_rejected")),
            client_order_id=_parse_optional_str(payload.get("client_order_id")),
            price=_parse_optional_float(payload.get("price")),
            size=_parse_optional_float(payload.get("size")),
            reject_reason_code=_parse_optional_str(payload.get("reject_reason_code")),
            reject_reason_text=_parse_optional_str(payload.get("reject_reason_text")),
            is_retryable=_parse_optional_bool(payload.get("is_retryable")),
            run_id=_parse_optional_str(payload.get("run_id")),
        )

    if event_type == EventType.ORDER_FILLED:
        liquidity_role = payload.get("liquidity_role")
        return OrderFilled(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            order_id=str(payload["order_id"]),
            market_id=str(payload["market_id"]),
            token_id=str(payload["token_id"]),
            side=OrderSide(str(payload["side"])),
            fill_price=float(payload["fill_price"]),
            fill_size=float(payload["fill_size"]),
            cumulative_filled_size=float(payload["cumulative_filled_size"]),
            remaining_size=float(payload["remaining_size"]),
            source=str(payload.get("source", "replay.order_filled")),
            client_order_id=_parse_optional_str(payload.get("client_order_id")),
            fee_paid_usdc=_parse_optional_float(payload.get("fee_paid_usdc")),
            liquidity_role=LiquidityRole(str(liquidity_role)) if liquidity_role is not None else None,
            venue_fill_id=_parse_optional_str(payload.get("venue_fill_id")),
            run_id=_parse_optional_str(payload.get("run_id")),
        )

    if event_type == EventType.ORDER_CANCELED:
        return OrderCanceled(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            order_id=str(payload["order_id"]),
            market_id=str(payload["market_id"]),
            token_id=str(payload["token_id"]),
            source=str(payload.get("source", "replay.order_canceled")),
            client_order_id=_parse_optional_str(payload.get("client_order_id")),
            cancel_reason=_parse_optional_str(payload.get("cancel_reason")),
            cumulative_filled_size=_parse_optional_float(payload.get("cumulative_filled_size")),
            remaining_size=_parse_optional_float(payload.get("remaining_size")),
            run_id=_parse_optional_str(payload.get("run_id")),
        )

    if event_type == EventType.MARKET_RESOLVED_WITH_INVENTORY:
        return MarketResolvedWithInventory(
            event_time_ms=_parse_timestamp_ms(payload.get("event_time_ms", payload.get("event_time"))),
            ingest_time_ms=_parse_timestamp_ms(payload.get("ingest_time_ms", payload.get("ingest_time"))),
            market_id=str(payload["market_id"]),
            token_id=str(payload["token_id"]),
            side=OrderSide(str(payload["side"])),
            size=float(payload["size"]),
            outcome_price=float(payload["outcome_price"]),
            average_entry_price=float(payload["average_entry_price"]),
            estimated_pnl_usdc=float(payload["estimated_pnl_usdc"]),
            source=str(payload.get("source", "runtime.market_resolved_holding")),
            run_id=_parse_optional_str(payload.get("run_id")),
        )

    raise ValueError(f"Unsupported replay event_type: {event_type!r}")


def _event_type_value(payload: dict[str, Any]) -> EventType:
    raw = payload.get("event_type")
    if raw is None:
        raise ValueError("Replay event is missing event_type")
    return EventType(str(raw))


def _parse_book_levels(value: Any) -> tuple[BookLevel, ...]:
    if not value:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"Book levels must be a list, got {type(value)!r}")
    levels: list[BookLevel] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError(f"Book level must be an object, got {type(item)!r}")
        levels.append(BookLevel(price=float(item["price"]), size=float(item["size"])))
    return tuple(levels)


def _parse_timestamp_ms(value: Any) -> int:
    if value is None:
        raise ValueError("timestamp value is required")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("timestamp string is empty")
        if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
            return int(text)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(round(datetime.fromisoformat(text).timestamp() * 1000))
    raise ValueError(f"Unsupported timestamp type: {type(value)!r}")


def _parse_optional_timestamp_ms(value: Any) -> int | None:
    if value is None:
        return None
    return _parse_timestamp_ms(value)


def _parse_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _parse_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _parse_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _parse_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return bool(value)


__all__ = [
    "CapturedResponseExecutionClient",
    "ReplayCaptureWriter",
    "ReplayEventSource",
    "ReplayFidelityError",
    "parse_event_record",
]
