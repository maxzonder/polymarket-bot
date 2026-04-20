"""Polymarket venue adapter boundary."""

from .book_channel import BookNormalizer
from .execution_client import (
    DEFAULT_CHAIN_ID,
    DEFAULT_CLOB_HOST,
    ExecutionClientConfigError,
    ExecutionClientNotStartedError,
    PolymarketExecutionClient,
    VenueCancelResult,
    VenueOrderRequest,
    VenueOrderState,
    VenuePlaceResult,
)
from .fee_metadata import FeeMetadataRefreshLoop
from .markets import (
    DiscoveryStats,
    DurationWindow,
    MarketMetadata,
    UniverseFilter,
    discover_short_horizon_markets,
    discover_short_horizon_markets_sync,
    parse_market_discovery_rows,
)
from .market_refresh import MarketRefreshLoop
from .trade_channel import TradeNormalizer
from .websocket import PolymarketWebsocket


class PolymarketVenueAdapter:
    """Placeholder adapter shell for Phase 2 venue wiring."""

    def connect(self) -> None:
        return None


__all__ = [
    "BookNormalizer",
    "DEFAULT_CHAIN_ID",
    "DEFAULT_CLOB_HOST",
    "DiscoveryStats",
    "DurationWindow",
    "ExecutionClientConfigError",
    "ExecutionClientNotStartedError",
    "FeeMetadataRefreshLoop",
    "MarketMetadata",
    "MarketRefreshLoop",
    "PolymarketExecutionClient",
    "PolymarketVenueAdapter",
    "PolymarketWebsocket",
    "TradeNormalizer",
    "UniverseFilter",
    "VenueCancelResult",
    "VenueOrderRequest",
    "VenueOrderState",
    "VenuePlaceResult",
    "discover_short_horizon_markets",
    "discover_short_horizon_markets_sync",
    "parse_market_discovery_rows",
]
