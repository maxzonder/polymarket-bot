"""Polymarket venue adapter boundary."""

from .markets import (
    DiscoveryStats,
    DurationWindow,
    MarketMetadata,
    UniverseFilter,
    discover_short_horizon_markets,
    discover_short_horizon_markets_sync,
    parse_market_discovery_rows,
)


class PolymarketVenueAdapter:
    """Placeholder adapter shell for Phase 2 venue wiring."""

    def connect(self) -> None:
        return None


__all__ = [
    "DiscoveryStats",
    "DurationWindow",
    "MarketMetadata",
    "PolymarketVenueAdapter",
    "UniverseFilter",
    "discover_short_horizon_markets",
    "discover_short_horizon_markets_sync",
    "parse_market_discovery_rows",
]
