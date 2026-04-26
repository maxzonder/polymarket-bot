from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class UniverseConfig:
    allowed_assets: tuple[str, ...] = ("bitcoin", "ethereum")


@dataclass(frozen=True)
class LifecycleConfig:
    bucket_start_fraction: float = 0.20
    bucket_end_fraction: float = 0.40


@dataclass(frozen=True)
class TriggerConfig:
    price_levels: tuple[float, ...] = (0.55, 0.65, 0.70)
    max_entry_drift_ticks: int = 1
    tick_size: float = 0.01


@dataclass(frozen=True)
class ExecutionConfig:
    target_trade_size_usdc: float = 1.0
    stale_market_data_threshold_ms: int = 2000
    hold_to_resolution: bool = True


@dataclass(frozen=True)
class FeesConfig:
    fee_metadata_ttl_seconds: int = 60
    reject_if_fee_metadata_stale: bool = True


@dataclass(frozen=True)
class MarketDiscoveryConfig:
    refresh_interval_seconds: int = 30
    max_rows: int = 20_000


@dataclass(frozen=True)
class RiskConfig:
    global_safe_mode: bool = False
    max_notional_per_strategy_usdc: float = 200.0
    max_daily_loss_usdc: float = 10.0
    max_consecutive_rejects: int = 5
    max_open_orders_total: int = 10
    max_open_orders_per_market: int = 1
    max_orders_per_market_per_run: int = 1
    max_tokens_with_exposure_per_market: int = 1
    micro_live_concurrent_open_notional_cap_usdc: float = 20.0
    micro_live_cumulative_stake_cap_usdc: float = 20.0
    max_trade_notional_usdc: float | None = None


@dataclass(frozen=True)
class SpotDislocationConfig:
    enabled: bool = False
    asset_allowlist: tuple[str, ...] = ("btc", "eth", "sol", "xrp")
    direction_allowlist: tuple[str, ...] = ("DOWN/NO",)
    min_lifecycle_fraction: float = 0.60
    min_spot_gap: float = 0.06
    edge_buffer_bps: float = 200.0
    max_spot_staleness_ms: int = 5_000
    max_start_spot_latency_ms: int = 5_000
    min_vol_lookback_points: int = 5
    fit_10_allowlist: tuple[str, ...] = ("+0_tick", "+1_tick")


@dataclass(frozen=True)
class ShortHorizonConfig:
    strategy_id: str = "short_horizon_15m_touch_v1"
    implementation_language: str = "python"
    universe: UniverseConfig = field(default_factory=UniverseConfig)
    lifecycle: LifecycleConfig = field(default_factory=LifecycleConfig)
    triggers: TriggerConfig = field(default_factory=TriggerConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    fees: FeesConfig = field(default_factory=FeesConfig)
    market_discovery: MarketDiscoveryConfig = field(default_factory=MarketDiscoveryConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    spot_dislocation: SpotDislocationConfig = field(default_factory=SpotDislocationConfig)
