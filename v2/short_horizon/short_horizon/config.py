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
    target_trade_size_usdc: float = 10.0
    stale_market_data_threshold_ms: int = 2000
    hold_to_resolution: bool = True


@dataclass(frozen=True)
class FeesConfig:
    fee_metadata_ttl_seconds: int = 60
    reject_if_fee_metadata_stale: bool = True


@dataclass(frozen=True)
class MarketDiscoveryConfig:
    refresh_interval_seconds: int = 30


@dataclass(frozen=True)
class RiskConfig:
    max_open_orders_total: int = 10
    micro_live_total_stake_cap_usdc: float = 100.0


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
