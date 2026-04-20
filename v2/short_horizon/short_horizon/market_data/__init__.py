from __future__ import annotations

from pathlib import Path

from ..core.events import NormalizedEvent
from ..replay import ReplayEventSource


class MarketDataSource:
    """Local stub source boundary for normalized live-market events."""

    def __init__(self, events: list[NormalizedEvent] | None = None):
        self._events = list(events or [])

    @classmethod
    def from_jsonl(cls, path: str | Path) -> "MarketDataSource":
        return cls(ReplayEventSource(path).load())

    def load(self) -> list[NormalizedEvent]:
        return list(self._events)

    def start(self) -> None:
        return None


__all__ = ["MarketDataSource"]
