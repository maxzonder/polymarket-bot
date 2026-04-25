from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

from .core.clock import advance_clock
from .core.events import BookUpdate, MarketStateUpdate, OrderAccepted, OrderCanceled, OrderFilled, OrderRejected, TimerEvent, TradeTick
from .core.models import OrderIntent
from .core.runtime import StrategyRuntime
from .execution import ExecutionEngine, ExecutionMode, ExecutionVenueClient, LiveSubmitGuard
from .strategy_api import CancelOrder, Noop, PlaceOrder, StrategyIntent
from .telemetry import get_logger


@dataclass(frozen=True)
class RunnerSummary:
    run_id: str
    event_count: int
    order_intents: int
    synthetic_order_events: int
    db_path: Path


def drive_runtime_events(
    *,
    events: list,
    runtime: StrategyRuntime,
    logger_name: str,
    completed_event_name: str,
    execution_mode: ExecutionMode | str = ExecutionMode.SYNTHETIC,
    execution_client: ExecutionVenueClient | None = None,
    live_submit_guard: LiveSubmitGuard | None = None,
) -> RunnerSummary:
    logger = get_logger(logger_name, run_id=runtime.store.current_run_id)
    resolved_mode = ExecutionMode(str(execution_mode))
    execution = ExecutionEngine(
        store=runtime.store,
        mode=resolved_mode,
        client=execution_client,
        live_submit_guard=live_submit_guard,
        clock=runtime.clock,
        venue_min_order_shares_fallback=getattr(runtime, "venue_min_order_shares_fallback", 0.0),
    )
    event_count = 0
    order_intents = 0
    synthetic_order_events = 0

    for event in events:
        event_count += 1
        intent_count, synthetic_count = _handle_runtime_event(event=event, runtime=runtime, execution=execution)
        order_intents += intent_count
        synthetic_order_events += synthetic_count

    logger.info(
        completed_event_name,
        run_id=runtime.store.current_run_id,
        input_events=event_count,
        order_intents=order_intents,
        synthetic_order_events=synthetic_order_events,
        execution_mode=resolved_mode.value,
    )
    return RunnerSummary(
        run_id=runtime.store.current_run_id,
        event_count=event_count,
        order_intents=order_intents,
        synthetic_order_events=synthetic_order_events,
        db_path=Path(getattr(runtime.store, "path", Path("<memory>"))),
    )


async def drive_runtime_event_stream(
    *,
    events: AsyncIterator,
    runtime: StrategyRuntime,
    logger_name: str,
    completed_event_name: str,
    max_events: int | None = None,
    max_runtime_seconds: float | None = None,
    execution_mode: ExecutionMode | str = ExecutionMode.SYNTHETIC,
    execution_client: ExecutionVenueClient | None = None,
    live_submit_guard: LiveSubmitGuard | None = None,
    after_event_callback=None,
) -> RunnerSummary:
    logger = get_logger(logger_name, run_id=runtime.store.current_run_id)
    resolved_mode = ExecutionMode(str(execution_mode))
    execution = ExecutionEngine(
        store=runtime.store,
        mode=resolved_mode,
        client=execution_client,
        live_submit_guard=live_submit_guard,
        clock=runtime.clock,
        venue_min_order_shares_fallback=getattr(runtime, "venue_min_order_shares_fallback", 0.0),
    )
    event_count = 0
    order_intents = 0
    synthetic_order_events = 0
    deadline = None if max_runtime_seconds is None else asyncio.get_running_loop().time() + float(max_runtime_seconds)

    while True:
        try:
            if deadline is None:
                event = await anext(events)
            else:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                event = await asyncio.wait_for(anext(events), timeout=remaining)
        except StopAsyncIteration:
            break
        except asyncio.TimeoutError:
            break

        event_count += 1
        intent_count, synthetic_count = _handle_runtime_event(event=event, runtime=runtime, execution=execution)
        order_intents += intent_count
        synthetic_order_events += synthetic_count
        if after_event_callback is not None:
            after_event_callback(event)
        if max_events is not None and event_count >= max_events:
            break

    logger.info(
        completed_event_name,
        run_id=runtime.store.current_run_id,
        input_events=event_count,
        order_intents=order_intents,
        synthetic_order_events=synthetic_order_events,
        execution_mode=resolved_mode.value,
    )
    return RunnerSummary(
        run_id=runtime.store.current_run_id,
        event_count=event_count,
        order_intents=order_intents,
        synthetic_order_events=synthetic_order_events,
        db_path=Path(getattr(runtime.store, "path", Path("<memory>"))),
    )


def _handle_runtime_event(*, event: object, runtime: StrategyRuntime, execution: ExecutionEngine) -> tuple[int, int]:
    event_time_ms = getattr(event, "event_time_ms", None)
    if event_time_ms is not None:
        advance_clock(runtime.clock, int(event_time_ms))

    if isinstance(event, MarketStateUpdate):
        outputs = runtime.on_market_state(event)
        return 0, apply_strategy_intents(
            outputs,
            runtime=runtime,
            execution=execution,
            fallback_event_time_ms=event.event_time_ms,
        )

    if isinstance(event, BookUpdate):
        outputs = runtime.on_book_update(event)
        synthetic_order_events = 0
        order_intents = 0
        for output in outputs:
            if isinstance(output, OrderIntent):
                order_intents += 1
                emitted_events = execution.submit(output, event_time_ms=event.event_time_ms)
                synthetic_order_events += len(emitted_events)
                for emitted_event in emitted_events:
                    synthetic_order_events += apply_strategy_intents(
                        runtime.strategy.on_order_event(emitted_event),
                        runtime=runtime,
                        execution=execution,
                        fallback_event_time_ms=emitted_event.event_time_ms,
                    )
        return order_intents, synthetic_order_events

    if isinstance(event, TradeTick):
        runtime.store.append_event(event)
        return 0, apply_strategy_intents(
            runtime.strategy.on_market_event(event),
            runtime=runtime,
            execution=execution,
            fallback_event_time_ms=event.event_time_ms,
        )

    if isinstance(event, TimerEvent):
        runtime.store.append_event(event)
        return 0, apply_strategy_intents(
            runtime.strategy.on_timer(event),
            runtime=runtime,
            execution=execution,
            fallback_event_time_ms=event.event_time_ms,
        )

    if isinstance(event, (OrderAccepted, OrderRejected, OrderFilled, OrderCanceled)):
        canonical_event = execution.reconcile_order_event(event)
        if canonical_event is None:
            return 0, 0
        runtime.store.append_event_log(canonical_event, order_id=getattr(canonical_event, "order_id", None))
        return 0, apply_strategy_intents(
            runtime.strategy.on_order_event(canonical_event),
            runtime=runtime,
            execution=execution,
            fallback_event_time_ms=canonical_event.event_time_ms,
        )

    raise TypeError(f"Unsupported runner event: {type(event)!r}")


def apply_strategy_intents(intents: list[StrategyIntent], *, runtime: StrategyRuntime, execution: ExecutionEngine, fallback_event_time_ms: int) -> int:
    synthetic_events = 0
    for intent in intents:
        emitted_events = []
        if isinstance(intent, PlaceOrder):
            emitted_events = execution.handle_intent(intent, event_time_ms=fallback_event_time_ms)
        elif isinstance(intent, CancelOrder):
            emitted_events = execution.handle_intent(intent, event_time_ms=fallback_event_time_ms)
        elif isinstance(intent, Noop):
            continue
        else:
            raise TypeError(f"Unsupported strategy intent: {type(intent)!r}")
        synthetic_events += len(emitted_events)
        for emitted_event in emitted_events:
            synthetic_events += apply_strategy_intents(
                runtime.strategy.on_order_event(emitted_event),
                runtime=runtime,
                execution=execution,
                fallback_event_time_ms=emitted_event.event_time_ms,
            )
    return synthetic_events


__all__ = ["RunnerSummary", "apply_strategy_intents", "drive_runtime_events", "drive_runtime_event_stream"]
