from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable, Literal

from .markets import MarketMetadata

UniverseStage = Literal["selected", "rejected"]
CatalystKind = Literal["catalyst", "random_walk", "ambiguous", "none"]
UniverseRejectReason = Literal[
    "inactive",
    "missing_market_id",
    "missing_token_ids",
    "missing_end_time",
    "missing_total_duration",
    "volume_below_min",
    "volume_above_max",
    "total_duration_below_min",
    "total_duration_above_max",
    "fees_enabled",
    "fee_rate_above_max",
    "category_blocked",
    "category_not_allowed",
    "random_walk_rejected",
    "cap_markets",
    "cap_category",
    "cap_tokens",
]

_CATALYST_KEYWORDS = (
    "election", "vote", "voter", "poll", "approval rating", "primary", "referendum",
    "court", "lawsuit", "trial", "indict", "verdict", "supreme court",
    "regulation", "regulator", "sec", "cftc", "bill", "executive order", "ban", "tariff",
    "war", "ceasefire", "attack", "sanction", "missile", "invasion", "hostage",
    "hurricane", "tornado", "earthquake", "wildfire", "temperature", "snow", "rainfall", "flood",
    "approval", "approved", "merger", "bankruptcy", "resign", "launch", "earnings",
)
_RANDOM_WALK_KEYWORDS = (
    "up or down", "higher or lower", "above or below", "hit $", "hit ", "reach $",
    "price of", "close above", "close below", "ath", "all-time high",
)
_AMBIGUOUS_RESOLUTION_KEYWORDS = (
    "mentioned", "say ", "tweet", "post on", "according to", "recognized by",
)


@dataclass(frozen=True)
class CatalystClassification:
    kind: CatalystKind
    reason: str = ""


@dataclass(frozen=True)
class _Candidate:
    index: int
    market: MarketMetadata
    token_ids: tuple[str, ...]
    subscription_score: float
    catalyst: CatalystClassification
    retained_market: bool
    reject_reason: UniverseRejectReason | None = None


@dataclass(frozen=True)
class UniverseSelectorConfig:
    """Pre-subscription selector controls for black-swan WS universe narrowing.

    This is intentionally not a money/risk config. It only decides which
    already-discovered markets/tokens are worth subscribing to before the later
    WS trigger → screener → pattern → order pipeline runs.

    A value of 0 for caps means unlimited/disabled.
    """

    max_markets: int = 0
    max_tokens: int = 0
    include_yes_token: bool = True
    include_no_token: bool = True
    min_volume_usdc: float = 0.0
    max_volume_usdc: float = 0.0
    min_total_duration_seconds: int = 0
    max_total_duration_seconds: int = 0
    require_total_duration: bool = False
    reject_fees_enabled: bool = False
    max_fee_rate_bps: float = 0.0
    allowed_categories: tuple[str, ...] = ()
    blocked_categories: tuple[str, ...] = ()
    category_multipliers: dict[str, float] = field(default_factory=dict)
    max_markets_per_category: int = 0
    reject_random_walk: bool = False
    retained_market_ids: tuple[str, ...] = ()
    retained_score_bonus: float = 0.0
    prefer_retained_on_score_tie: bool = True
    base_subscription_score: float = 1.0
    catalyst_multiplier: float = 1.25
    random_walk_multiplier: float = 0.40
    ambiguous_multiplier: float = 0.70


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
    category: str | None = None
    slug: str | None = None
    duration_bucket: str | None = None
    volume_usdc: float = 0.0
    liquidity_usdc: float = 0.0
    neg_risk: bool = False
    neg_risk_group_id: str | None = None
    catalyst_kind: CatalystKind = "none"
    catalyst_reason: str = ""
    retained_market: bool = False


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


def black_swan_universe_config(**overrides) -> UniverseSelectorConfig:
    """Conservative starting policy for black_swan pre-subscription selection.

    The defaults are soft: high-priority categories get boosts, known noisy
    categories get penalties, and random-walk markets are penalized but not
    rejected unless the caller enables `reject_random_walk`.
    """

    values = dict(
        category_multipliers={
            "weather": 1.35,
            "geopolitics": 1.30,
            "politics": 1.25,
            "legal": 1.20,
            "regulation": 1.20,
            "crypto": 0.85,
            "sports": 0.75,
            "tech": 0.70,
            "entertainment": 0.65,
        },
        random_walk_multiplier=0.35,
        ambiguous_multiplier=0.65,
        catalyst_multiplier=1.25,
    )
    values.update(overrides)
    return UniverseSelectorConfig(**values)


def build_subscription_plan(
    markets: Iterable[MarketMetadata],
    *,
    config: UniverseSelectorConfig | None = None,
) -> SubscriptionPlan:
    """Build a deterministic token subscription plan from market metadata.

    Strategic policy and catalyst classification are still pre-subscription only:
    they affect selection/score, but they do not replace the later WS trigger →
    screener → MarketPatternTracker → order-intent path.
    """

    cfg = config or UniverseSelectorConfig()
    selected_market_ids: list[str] = []
    selected_token_ids: list[str] = []
    token_to_market_id: dict[str, str] = {}
    token_to_side_index: dict[str, int] = {}
    retained_market_ids = {str(market_id) for market_id in cfg.retained_market_ids}
    evaluated: list[_Candidate] = []
    selected_indices: set[int] = set()
    final_reject_reasons: dict[int, UniverseRejectReason] = {}
    rejection_counts: Counter[str] = Counter()

    for index, market in enumerate(markets):
        catalyst = classify_catalyst(market)
        retained_market = str(market.market_id or "") in retained_market_ids
        score = _subscription_score(market, cfg, catalyst, retained_market=retained_market)
        reason = _technical_reject_reason(market, cfg)
        if reason is None:
            reason = _policy_reject_reason(market, cfg, catalyst)
        token_ids = _selected_market_tokens(market, cfg) if reason is None else ()
        evaluated.append(_Candidate(
            index=index,
            market=market,
            token_ids=token_ids,
            subscription_score=score,
            catalyst=catalyst,
            retained_market=retained_market,
            reject_reason=reason,
        ))

    category_counts: Counter[str] = Counter()
    ranked_candidates = sorted(
        (candidate for candidate in evaluated if candidate.reject_reason is None),
        key=lambda candidate: _candidate_sort_key(candidate, cfg),
    )

    for candidate in ranked_candidates:
        reason: UniverseRejectReason | None = None
        market = candidate.market
        token_ids = candidate.token_ids
        category = _norm(getattr(market, "category", None))

        if reason is None and cfg.max_markets > 0 and len(selected_market_ids) >= cfg.max_markets:
            reason = "cap_markets"
        if reason is None and cfg.max_tokens > 0 and len(selected_token_ids) + len(token_ids) > cfg.max_tokens:
            reason = "cap_tokens"
        if (
            reason is None
            and cfg.max_markets_per_category > 0
            and category
            and category_counts[category] >= cfg.max_markets_per_category
        ):
            reason = "cap_category"

        if reason is not None:
            final_reject_reasons[candidate.index] = reason
            continue

        selected_indices.add(candidate.index)
        selected_market_ids.append(str(market.market_id))
        selected_token_ids.extend(token_ids)
        if category:
            category_counts[category] += 1
        if market.token_yes_id and cfg.include_yes_token:
            token_to_market_id[str(market.token_yes_id)] = str(market.market_id)
            token_to_side_index[str(market.token_yes_id)] = 0
        if market.token_no_id and cfg.include_no_token:
            token_to_market_id[str(market.token_no_id)] = str(market.market_id)
            token_to_side_index[str(market.token_no_id)] = 1

    decisions: list[UniverseDecision] = []
    for candidate in evaluated:
        reason = candidate.reject_reason or final_reject_reasons.get(candidate.index)
        if reason is not None:
            rejection_counts[reason] += 1
            decisions.append(_decision(
                candidate.market,
                stage="rejected",
                reject_reason=reason,
                subscription_score=candidate.subscription_score,
                catalyst=candidate.catalyst,
                retained_market=candidate.retained_market,
            ))
            continue

        decisions.append(_decision(
            candidate.market,
            stage="selected",
            selected_token_ids=candidate.token_ids if candidate.index in selected_indices else (),
            subscription_score=candidate.subscription_score,
            catalyst=candidate.catalyst,
            retained_market=candidate.retained_market,
        ))

    return SubscriptionPlan(
        decisions=tuple(decisions),
        selected_market_ids=tuple(selected_market_ids),
        selected_token_ids=tuple(selected_token_ids),
        token_to_market_id=token_to_market_id,
        token_to_side_index=token_to_side_index,
        rejection_counts=dict(rejection_counts),
    )


def classify_catalyst(market: MarketMetadata) -> CatalystClassification:
    """Cheap rule-based catalyst/random-walk classifier for subscription policy.

    This deliberately avoids LLM/API calls and does not inspect CLOB/orderbook
    state.  Live pattern acceptance remains a later token-side gate.
    """

    text = " ".join(
        str(part or "").lower()
        for part in (market.question, getattr(market, "slug", None), getattr(market, "series_slug", None))
    )
    catalyst = _first_match(text, _CATALYST_KEYWORDS)
    random_walk = _first_match(text, _RANDOM_WALK_KEYWORDS)
    ambiguous = _first_match(text, _AMBIGUOUS_RESOLUTION_KEYWORDS)
    if catalyst:
        return CatalystClassification("catalyst", catalyst)
    if random_walk:
        return CatalystClassification("random_walk", random_walk)
    if ambiguous:
        return CatalystClassification("ambiguous", ambiguous)
    return CatalystClassification("none", "")


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

    volume = float(getattr(market, "volume_usdc", 0.0) or 0.0)
    if cfg.min_volume_usdc > 0 and volume < cfg.min_volume_usdc:
        return "volume_below_min"
    if cfg.max_volume_usdc > 0 and volume > cfg.max_volume_usdc:
        return "volume_above_max"

    total_duration = getattr(market, "total_duration_seconds", None)
    if cfg.require_total_duration and total_duration is None:
        return "missing_total_duration"
    if total_duration is not None:
        if cfg.min_total_duration_seconds > 0 and total_duration < cfg.min_total_duration_seconds:
            return "total_duration_below_min"
        if cfg.max_total_duration_seconds > 0 and total_duration > cfg.max_total_duration_seconds:
            return "total_duration_above_max"

    if cfg.reject_fees_enabled and bool(getattr(market, "fees_enabled", False)):
        return "fees_enabled"
    fee_rate_bps = getattr(market, "fee_rate_bps", None)
    if cfg.max_fee_rate_bps > 0 and fee_rate_bps is not None and float(fee_rate_bps) > cfg.max_fee_rate_bps:
        return "fee_rate_above_max"

    return None


def _policy_reject_reason(
    market: MarketMetadata,
    cfg: UniverseSelectorConfig,
    catalyst: CatalystClassification,
) -> UniverseRejectReason | None:
    category = _norm(getattr(market, "category", None))
    allowed = {_norm(c) for c in cfg.allowed_categories if _norm(c)}
    blocked = {_norm(c) for c in cfg.blocked_categories if _norm(c)}
    if category and category in blocked:
        return "category_blocked"
    if allowed and category not in allowed:
        return "category_not_allowed"
    if cfg.reject_random_walk and catalyst.kind == "random_walk":
        return "random_walk_rejected"
    return None


def _subscription_score(
    market: MarketMetadata,
    cfg: UniverseSelectorConfig,
    catalyst: CatalystClassification,
    *,
    retained_market: bool = False,
) -> float:
    score = float(cfg.base_subscription_score)
    category = _norm(getattr(market, "category", None))
    if category:
        score *= float(cfg.category_multipliers.get(category, 1.0))
    if catalyst.kind == "catalyst":
        score *= float(cfg.catalyst_multiplier)
    elif catalyst.kind == "random_walk":
        score *= float(cfg.random_walk_multiplier)
    elif catalyst.kind == "ambiguous":
        score *= float(cfg.ambiguous_multiplier)
    if retained_market and cfg.retained_score_bonus > 0:
        score += float(cfg.retained_score_bonus)
    return round(score, 6)


def _candidate_sort_key(candidate: _Candidate, cfg: UniverseSelectorConfig) -> tuple[float, int, int, str, int]:
    retained_rank = 0 if cfg.prefer_retained_on_score_tie and candidate.retained_market else 1
    end_time_rank = candidate.market.end_time_ms if candidate.market.end_time_ms is not None else 2**63 - 1
    market_id = str(candidate.market.market_id or "")
    return (-candidate.subscription_score, retained_rank, int(end_time_rank), market_id, candidate.index)


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
    subscription_score: float = 0.0,
    catalyst: CatalystClassification | None = None,
    retained_market: bool = False,
) -> UniverseDecision:
    catalyst = catalyst or CatalystClassification("none", "")
    return UniverseDecision(
        market_id=str(market.market_id or ""),
        condition_id=str(market.condition_id) if market.condition_id is not None else None,
        question=str(market.question or ""),
        stage=stage,
        selected_token_ids=selected_token_ids,
        subscription_score=subscription_score,
        reject_reason=reject_reason,
        asset_slug=market.asset_slug,
        category=getattr(market, "category", None),
        slug=getattr(market, "slug", None),
        duration_bucket=_duration_bucket(getattr(market, "total_duration_seconds", None) or market.duration_seconds),
        volume_usdc=float(getattr(market, "volume_usdc", 0.0) or 0.0),
        liquidity_usdc=float(getattr(market, "liquidity_usdc", 0.0) or 0.0),
        neg_risk=bool(getattr(market, "neg_risk", False)),
        neg_risk_group_id=getattr(market, "neg_risk_group_id", None),
        catalyst_kind=catalyst.kind,
        catalyst_reason=catalyst.reason,
        retained_market=retained_market,
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


def _first_match(text: str, needles: tuple[str, ...]) -> str:
    return next((needle for needle in needles if needle in text), "")


def _norm(value: object) -> str:
    return str(value or "").strip().lower()


__all__ = [
    "CatalystClassification",
    "SubscriptionPlan",
    "UniverseDecision",
    "UniverseSelectorConfig",
    "black_swan_universe_config",
    "build_subscription_plan",
    "classify_catalyst",
]
