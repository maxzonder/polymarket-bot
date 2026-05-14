from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Iterable, Literal

from .markets import MarketMetadata

UniverseStage = Literal["selected", "rejected"]
UniverseRejectReason = Literal[
    "inactive",
    "missing_market_id",
    "missing_token_ids",
    "missing_end_time",
    "cap_markets",
    "cap_tokens",
]


@dataclass(frozen=True)
class UniverseSelectorConfig:
    """Pre-subscription selector controls for black-swan WS universe narrowing.

    This is intentionally not a money/risk config.  It only decides which
    already-discovered markets/tokens are worth subscribing to before the later
    WS trigger → screener → pattern → order pipeline runs.

    A value of 0 for caps means unlimited.
    """

    max_markets: int = 0
    max_tokens: int = 0
    include_yes_token: bool = True
    include_no_token: bool = True


@dataclass(frozen=True)
class UniverseDecision:
    market_id: str
    condition_id: str | None
    question: str
    stage: UniverseStage
    selected_token_ids: tuple[str, ...] = ()
    subscription_score: float = 0.0
    reject_reason: UniverseRejectReason | None = None
    asset_slug: str | None = None
    duration_bucket: str | None = None


@dataclass(frozen=True)
class SubscriptionPlan:
    decisions: tuple[UniverseDecision, ...]
    selected_market_ids: tuple[str, ...]
    selected_token_ids: tuple[str, ...]
    token_to_market_id: dict[str, str]
    token_to_side_index: dict[str, int]
    rejection_counts: dict[str, int]

    @property
    def selected_markets_count(self) -> int:
        return len(self.selected_market_ids)

    @property
    def selected_tokens_count(self) -> int:
        return len(self.selected_token_ids)


def build_subscription_plan(
    markets: Iterable[MarketMetadata],
    *,
    config: UniverseSelectorConfig | None = None,
) -> SubscriptionPlan:
    """Build a deterministic token subscription plan from market metadata.

    This first slice is deliberately conservative: only technical validity plus
    global caps.  Strategy policy, scoring, hysteresis, and pattern priors will
    layer on top of the same decision model in later UN steps.
    """

    cfg = config or UniverseSelectorConfig()
    decisions: list[UniverseDecision] = []
    selected_market_ids: list[str] = []
    selected_token_ids: list[str] = []
    token_to_market_id: dict[str, str] = {}
    token_to_side_index: dict[str, int] = {}
    rejection_counts: Counter[str] = Counter()

    for market in markets:
        reason = _technical_reject_reason(market, cfg)
        token_ids = _selected_market_tokens(market, cfg) if reason is None else ()

        if reason is None and cfg.max_markets > 0 and len(selected_market_ids) >= cfg.max_markets:
            reason = "cap_markets"
        if reason is None and cfg.max_tokens > 0 and len(selected_token_ids) + len(token_ids) > cfg.max_tokens:
            reason = "cap_tokens"

        if reason is not None:
            rejection_counts[reason] += 1
            decisions.append(_decision(market, stage="rejected", reject_reason=reason))
            continue

        selected_market_ids.append(str(market.market_id))
        selected_token_ids.extend(token_ids)
        if market.token_yes_id and cfg.include_yes_token:
            token_to_market_id[str(market.token_yes_id)] = str(market.market_id)
            token_to_side_index[str(market.token_yes_id)] = 0
        if market.token_no_id and cfg.include_no_token:
            token_to_market_id[str(market.token_no_id)] = str(market.market_id)
            token_to_side_index[str(market.token_no_id)] = 1
        decisions.append(_decision(market, stage="selected", selected_token_ids=token_ids))

    return SubscriptionPlan(
        decisions=tuple(decisions),
        selected_market_ids=tuple(selected_market_ids),
        selected_token_ids=tuple(selected_token_ids),
        token_to_market_id=token_to_market_id,
        token_to_side_index=token_to_side_index,
        rejection_counts=dict(rejection_counts),
    )


def _technical_reject_reason(
    market: MarketMetadata,
    cfg: UniverseSelectorConfig,
) -> UniverseRejectReason | None:
    if not str(market.market_id or "").strip():
        return "missing_market_id"
    if not bool(market.is_active):
        return "inactive"
    if market.end_time_ms is None:
        return "missing_end_time"
    if not _selected_market_tokens(market, cfg):
        return "missing_token_ids"
    return None


def _selected_market_tokens(market: MarketMetadata, cfg: UniverseSelectorConfig) -> tuple[str, ...]:
    tokens: list[str] = []
    if cfg.include_yes_token and str(market.token_yes_id or "").strip():
        tokens.append(str(market.token_yes_id))
    if cfg.include_no_token and str(market.token_no_id or "").strip():
        tokens.append(str(market.token_no_id))
    return tuple(tokens)


def _decision(
    market: MarketMetadata,
    *,
    stage: UniverseStage,
    selected_token_ids: tuple[str, ...] = (),
    reject_reason: UniverseRejectReason | None = None,
) -> UniverseDecision:
    return UniverseDecision(
        market_id=str(market.market_id or ""),
        condition_id=str(market.condition_id) if market.condition_id is not None else None,
        question=str(market.question or ""),
        stage=stage,
        selected_token_ids=selected_token_ids,
        reject_reason=reject_reason,
        asset_slug=market.asset_slug,
        duration_bucket=_duration_bucket(market.duration_seconds),
    )


def _duration_bucket(duration_seconds: int | None) -> str | None:
    if duration_seconds is None:
        return None
    if duration_seconds <= 15 * 60:
        return "15m"
    if duration_seconds <= 60 * 60:
        return "1h"
    if duration_seconds <= 6 * 60 * 60:
        return "6h"
    if duration_seconds <= 7 * 24 * 60 * 60:
        return "1-7d"
    return "long"


__all__ = [
    "SubscriptionPlan",
    "UniverseDecision",
    "UniverseSelectorConfig",
    "build_subscription_plan",
]
