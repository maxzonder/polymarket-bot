from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int:
        ...


@dataclass
class SystemClock:
    def now_ms(self) -> int:
        return int(time.time_ns() // 1_000_000)


@dataclass
class ReplayClock:
    current_time_ms: int = 0

    def now_ms(self) -> int:
        return int(self.current_time_ms)

    def advance_to(self, event_time_ms: int) -> int:
        self.current_time_ms = max(int(self.current_time_ms), int(event_time_ms))
        return self.current_time_ms


def advance_clock(clock: Clock, event_time_ms: int) -> int:
    advance_to = getattr(clock, "advance_to", None)
    if callable(advance_to):
        return int(advance_to(event_time_ms))
    return int(event_time_ms)


__all__ = ["Clock", "ReplayClock", "SystemClock", "advance_clock"]
