"""Polymarket venue adapter boundary."""

from .book_channel import BookNormalizer
from .execution_client import (
    AllowanceApprovalResult,
    COLLATERAL_OFFRAMP_ADDRESS,
    COLLATERAL_ONRAMP_ADDRESS,
    DEFAULT_BRIDGE_HOST,
    DEFAULT_CHAIN_ID,
    DEFAULT_CLOB_HOST,
    DEFAULT_DATA_API_HOST,
    DEFAULT_POLYGON_RPC_URL,
    ExecutionClientConfigError,
    ExecutionClientNotStartedError,
    MAX_UINT256,
    POLYGON_NATIVE_USDC_TOKEN,
    PolymarketExecutionClient,
    POLYMARKET_CTF_TOKEN,
    POLYMARKET_PUSD_TOKEN,
    POLYMARKET_SPENDER_ADDRESSES,
    POLYMARKET_USDC_TOKEN,
    POLYMARKET_V2_SPENDER_ADDRESSES,
    PolygonUsdcBridgeResult,
    PolygonUsdcWrapResult,
    RedeemResult,
    ResolvedPosition,
    VenueApiCredentials,
    VenueCancelResult,
    VenueOrderRequest,
    VenueOrderState,
    VenuePlaceResult,
    ZERO_BYTES32,
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
from .shared_discovery import SharedMarketDiscovery
from .trade_channel import TradeNormalizer
from .universe_selector import SubscriptionPlan, UniverseDecision, UniverseSelectorConfig, build_subscription_plan
from .user_stream import PolymarketUserStream, UserStreamNormalizer
from .websocket import PolymarketWebsocket


class PolymarketVenueAdapter:
    """Placeholder adapter shell for Phase 2 venue wiring."""

    def connect(self) -> None:
        return None


__all__ = [
    "AllowanceApprovalResult",
    "BookNormalizer",
    "COLLATERAL_OFFRAMP_ADDRESS",
    "COLLATERAL_ONRAMP_ADDRESS",
    "DEFAULT_BRIDGE_HOST",
    "DEFAULT_CHAIN_ID",
    "DEFAULT_CLOB_HOST",
    "DEFAULT_DATA_API_HOST",
    "DEFAULT_POLYGON_RPC_URL",
    "DiscoveryStats",
    "DurationWindow",
    "ExecutionClientConfigError",
    "ExecutionClientNotStartedError",
    "FeeMetadataRefreshLoop",
    "MAX_UINT256",
    "MarketMetadata",
    "MarketRefreshLoop",
    "POLYGON_NATIVE_USDC_TOKEN",
    "PolymarketExecutionClient",
    "POLYMARKET_CTF_TOKEN",
    "POLYMARKET_PUSD_TOKEN",
    "POLYMARKET_SPENDER_ADDRESSES",
    "POLYMARKET_USDC_TOKEN",
    "POLYMARKET_V2_SPENDER_ADDRESSES",
    "PolygonUsdcBridgeResult",
    "PolygonUsdcWrapResult",
    "RedeemResult",
    "PolymarketUserStream",
    "PolymarketVenueAdapter",
    "PolymarketWebsocket",
    "SharedMarketDiscovery",
    "SubscriptionPlan",
    "TradeNormalizer",
    "UniverseDecision",
    "UniverseSelectorConfig",
    "UserStreamNormalizer",
    "UniverseFilter",
    "ResolvedPosition",
    "VenueApiCredentials",
    "VenueCancelResult",
    "VenueOrderRequest",
    "VenueOrderState",
    "VenuePlaceResult",
    "ZERO_BYTES32",
    "build_subscription_plan",
    "discover_short_horizon_markets",
    "discover_short_horizon_markets_sync",
    "parse_market_discovery_rows",
]
