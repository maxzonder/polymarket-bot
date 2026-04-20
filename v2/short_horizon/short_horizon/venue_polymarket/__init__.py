"""Polymarket venue adapter boundary."""

from .book_channel import BookNormalizer
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
    "DiscoveryStats",
    "DurationWindow",
    "FeeMetadataRefreshLoop",
    "MarketMetadata",
    "MarketRefreshLoop",
    "PolymarketVenueAdapter",
    "PolymarketWebsocket",
    "TradeNormalizer",
    "UniverseFilter",
    "discover_short_horizon_markets",
    "discover_short_horizon_markets_sync",
    "parse_market_discovery_rows",
]
