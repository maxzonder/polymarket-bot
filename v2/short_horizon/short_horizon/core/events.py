from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketStateUpdate:
    event_time_ms: int
    ingest_time_ms: int
    market_id: str
    token_id: str
    condition_id: str
    question: str
    asset_slug: str
    start_time_ms: int
    end_time_ms: int
    is_active: bool
    metadata_is_fresh: bool = True
    fee_rate_bps: float | None = None
    fee_metadata_age_ms: int | None = None
    source: str = "market_state"


@dataclass(frozen=True)
class BookUpdate:
    event_time_ms: int
    ingest_time_ms: int
    market_id: str
    token_id: str
    best_bid: float | None
    best_ask: float | None
    source: str = "book_update"
