from __future__ import annotations

from ..config import ShortHorizonConfig
from ..core.events import BookUpdate
from ..core.lifecycle import compute_lifecycle_fraction, is_in_bucket
from ..core.models import MarketState, SkipDecision


def gate_touch(
    *,
    config: ShortHorizonConfig,
    event: BookUpdate,
    state: MarketState | None,
    level: float,
) -> SkipDecision | None:
    if state is None:
        return SkipDecision(
            reason="missing_market_state",
            market_id=event.market_id,
            token_id=event.token_id,
            level=level,
            event_time_ms=event.event_time_ms,
        )
    if not state.is_active:
        return SkipDecision(
            reason="inactive_market",
            market_id=state.market_id,
            token_id=state.token_id,
            level=level,
            event_time_ms=event.event_time_ms,
        )
    if state.asset_slug not in set(config.universe.allowed_assets):
        return SkipDecision(
            reason="asset_not_allowed",
            market_id=state.market_id,
            token_id=state.token_id,
            level=level,
            event_time_ms=event.event_time_ms,
            details=state.asset_slug,
        )
    if not state.metadata_is_fresh:
        return SkipDecision(
            reason="stale_market_metadata",
            market_id=state.market_id,
            token_id=state.token_id,
            level=level,
            event_time_ms=event.event_time_ms,
        )
    if config.fees.reject_if_fee_metadata_stale:
        if state.fee_metadata_age_ms is None:
            return SkipDecision(
                reason="missing_fee_metadata",
                market_id=state.market_id,
                token_id=state.token_id,
                level=level,
                event_time_ms=event.event_time_ms,
            )
        if state.fee_metadata_age_ms > config.fees.fee_metadata_ttl_seconds * 1000:
            return SkipDecision(
                reason="stale_fee_metadata",
                market_id=state.market_id,
                token_id=state.token_id,
                level=level,
                event_time_ms=event.event_time_ms,
            )
    if event.best_ask is None:
        return SkipDecision(
            reason="missing_best_ask",
            market_id=state.market_id,
            token_id=state.token_id,
            level=level,
            event_time_ms=event.event_time_ms,
        )
    if event.ingest_time_ms - event.event_time_ms > config.execution.stale_market_data_threshold_ms:
        return SkipDecision(
            reason="stale_market_data",
            market_id=state.market_id,
            token_id=state.token_id,
            level=level,
            event_time_ms=event.event_time_ms,
        )

    lifecycle_fraction = compute_lifecycle_fraction(
        start_time_ms=state.start_time_ms,
        end_time_ms=state.end_time_ms,
        event_time_ms=event.event_time_ms,
    )
    if not is_in_bucket(
        fraction=lifecycle_fraction,
        bucket_start=config.lifecycle.bucket_start_fraction,
        bucket_end=config.lifecycle.bucket_end_fraction,
    ):
        return SkipDecision(
            reason="wrong_lifecycle_bucket",
            market_id=state.market_id,
            token_id=state.token_id,
            level=level,
            event_time_ms=event.event_time_ms,
            details=f"fraction={lifecycle_fraction}",
        )

    max_entry_price = level + config.triggers.max_entry_drift_ticks * config.triggers.tick_size
    if float(event.best_ask) > max_entry_price + 1e-9:
        return SkipDecision(
            reason="bad_entry_ask_state",
            market_id=state.market_id,
            token_id=state.token_id,
            level=level,
            event_time_ms=event.event_time_ms,
            details=f"best_ask={event.best_ask}",
        )
    return None
