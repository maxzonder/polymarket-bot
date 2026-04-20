from __future__ import annotations

from typing import NewType


EventTime = NewType("EventTime", int)
IngestTime = NewType("IngestTime", int)

RunId = NewType("RunId", str)
MarketId = NewType("MarketId", str)
TokenId = NewType("TokenId", str)
OrderId = NewType("OrderId", str)
ClientOrderId = NewType("ClientOrderId", str)
ConditionId = NewType("ConditionId", str)
TradeId = NewType("TradeId", str)
VenueFillId = NewType("VenueFillId", str)
StrategyId = NewType("StrategyId", str)


def as_event_time(value: int) -> EventTime:
    return EventTime(int(value))


def as_ingest_time(value: int) -> IngestTime:
    return IngestTime(int(value))
