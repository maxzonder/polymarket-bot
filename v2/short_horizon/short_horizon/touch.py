from __future__ import annotations

from .models import TouchSignal


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
