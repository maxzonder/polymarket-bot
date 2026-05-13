"""
Bot Configuration — three trading modes + runtime settings.

Modes:
  fast_tp_mode   — scanner-triggered entry, full exit at 5-10x
  balanced_mode  — resting bids + scanner, partial TP, 20% moonbag
  big_swan_mode  — ONLY resting pre-positioned bids, 60% moonbag, optimize tail_ev
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env", override=False)
except ImportError:
    # python-dotenv not installed — parse .env manually
    _env_file = Path(__file__).parent / ".env"
    if _env_file.exists():
        for _line in _env_file.read_text().splitlines():
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())


@dataclass(frozen=True)
class TPLevel:
    """Single step in the take-profit ladder."""
    progress: float   # progress from entry price to $1.00 (0.0–1.0)
    fraction: float   # fraction of position to sell at this level (0.0–1.0)


@dataclass(frozen=True)
class ModeConfig:
    """Complete strategy configuration for one trading mode."""

    name: str

    # ── Entry ─────────────────────────────────────────────────────────────────
    # price levels for resting limit bids (ascending)
    entry_price_levels: tuple[float, ...]
    # max current price to consider a market for coverage
    entry_price_max: float
    # place resting bids before market dips to floor
    use_resting_bids: bool
    # also enter when scanner sees price already in zone
    scanner_entry: bool

    # ── Exit ──────────────────────────────────────────────────────────────────
    # partial TP ladder (fractions must sum to <= 1 - moonbag_fraction)
    tp_levels: tuple[TPLevel, ...]
    # fraction held to resolution (moonbag)
    moonbag_fraction: float

    # ── Position sizing ───────────────────────────────────────────────────────
    stake_usdc: float          # default stake per trade (USDC tokens bought at entry)
    max_open_positions: int
    max_resting_markets: int        # max distinct markets with live resting bids
    max_resting_per_cluster: int    # max markets per neg-risk group

    # ── Time window ───────────────────────────────────────────────────────────
    min_hours_to_close: float   # reject markets closing sooner than this
    max_hours_to_close: float   # reject markets closing later than this

    # ── Price-tier stake schedule ──────────────────────────────────────────────
    # Tuple of (max_entry_price, stake_usdc) sorted ascending by price.
    # For a given fill price, the first tier where fill_price <= max_entry_price
    # determines the stake. Falls back to stake_usdc if no tier matches.
    # Empty tuple = disabled (use stake_usdc for all levels).
    stake_tiers: tuple[tuple[float, float], ...] = ()

    # ── v1.1 Market-score gates (issue #44 Stage 1) ────────────────────────────
    # Minimum market_score to consider a market at all. 0.0 = disabled (pass all).
    min_market_score: float = 0.0

    # Quality-weighted stake schedule keyed on market_score (not entry price).
    # Tuple of (min_market_score_threshold, stake_usdc) sorted descending by threshold.
    # Example: ((0.60, 0.50), (0.40, 0.25), (0.25, 0.10))
    # Falls back to stake_usdc if no tier matches.
    # Empty tuple = disabled (use price-based stake_tiers instead).
    market_score_tiers: tuple[tuple[float, float], ...] = ()

    # ── v1.1 Per-market exposure cap (issue #44 Stage 1) ──────────────────────
    # Maximum total USDC across all fills on the same (market_id, token_id).
    # 0.0 = disabled (no cap). Works with ExposureManager.
    max_exposure_per_market: float = 0.0

    # ── Time-gate null fallback ───────────────────────────────────────────────
    # If Gamma returns None for hours_to_close, treat as this many hours rather
    # than silently bypassing the time-gate. Prevents markets with unknown deadlines
    # from escaping the filter entirely. Default 48h = safe middle ground.
    hours_to_close_null_default: float = 48.0

    # If True, reject markets where Gamma returns no end_date (hours_to_close is None)
    # instead of falling back to hours_to_close_null_default. Use for strategies where
    # unknown deadline = unknown risk (e.g. long-horizon election markets bypassing gates).
    hours_to_close_null_reject: bool = False

    # ── Screener scoring weights ───────────────────────────────────────────────
    # Tuple of (component_name, weight) pairs. Weights should sum to 1.0.
    # Components: "market_score", "liq", "duration", "category"
    scoring_weights: tuple[tuple[str, float], ...] = ()

    # Duration scoring direction:
    #   True  = prefer long  (flat score=1.0 until max_hours_to_close, then decay)
    #   False = prefer short (linear decay from 0 to max_hours_to_close)
    prefer_long_duration: bool = False

    # ── Cohort size gate ──────────────────────────────────────────────────────
    # Skip an entire neg-risk cohort if it has more than this many outcomes.
    # Prevents 1-of-N guessing games (e.g. 1-of-25 approval rating buckets).
    # 0 = disabled.
    max_cohort_size: int = 0

    # ── Total-duration gate ───────────────────────────────────────────────────
    # Minimum listing_duration_hours (market creation → close) to include a market.
    # 0.0 = disabled. Used to exclude genuinely short-lived markets from the universe.
    # Note: min_hours_to_close already handles remaining-time filtering; this gates
    # on total market lifespan at load time.
    min_total_duration_hours: float = 0.0

    # ── Question-text exclusion keywords ─────────────────────────────────────
    # Reject a market if its question contains ANY of these substrings (case-insensitive).
    # Used to filter weak sports subtypes (tennis, F1, CS2, Valorant) that have
    # materially lower WR (<60%) in black_swan analysis.
    exclude_question_keywords: tuple[str, ...] = ()

    # ── Slug prefix filters (issue #180 Phase A) ─────────────────────────────
    # Asset-slug-based filtering. More reliable than question keywords because
    # slugs follow consistent prefixes per league/type. Both lists default to
    # empty (no slug filtering). When either is non-empty:
    #   exclude_slug_prefixes — reject if slug starts with any of these
    #   include_slug_prefixes — for sports markets only, REQUIRE slug to start
    #     with one of these (allowlist). Non-sports categories are not gated
    #     by include list. Keeps filter scope-bounded.
    exclude_slug_prefixes: tuple[str, ...] = ()
    include_slug_prefixes_for_sports: tuple[str, ...] = ()

    # ── Duration-aware stake multipliers (issue #180 P1.2) ───────────────────
    # Multiplier applied to per-level stake based on hours_to_close at screen time.
    # Tuple of (max_hours, multiplier). First match wins. Last entry is the
    # default cap (use float("inf") as max_hours). Empty tuple = no scaling.
    # BLACK_SWAN_MODE rationale (issue #180 Phase A):
    #   ≤1h: avg X 30x, thin volume — small stake
    #   1-6h: avg X 50-66x — moderate
    #   6h-7d: avg X 95x, best cohort — full stake
    duration_stake_multipliers: tuple[tuple[float, float], ...] = ()

    # ── Cluster (category × duration) bonus table (issue #180 P2.2) ──────────
    # Final-stage multiplier applied to total_score based on the market's
    # (category, duration_bucket) cluster. Values centered at 1.0; >1 boosts,
    # <1 dampens. Empty dict = no cluster scaling. Duration bucket is computed
    # from hours_to_close (≤0.5h="15m", ≤2h="1h", ≤6h="6h", ≤168h="1-7d", else "long").
    cluster_score_multipliers: dict = field(default_factory=dict)

    # ── Lifecycle (phase) stake multipliers (issue #180 P1.3) ────────────────
    # Multiplier applied based on lifecycle_fraction = (now - start) / duration
    # at order placement time. Tuple of (max_lifecycle_fraction, multiplier);
    # first match wins. Empty tuple = no scaling.
    # BLACK_SWAN_MODE rationale (issue #180 Phase B3):
    #   <0.10 (opening_10pct): low EV, dampen
    #   0.10-0.75 (middle): 75.6% WR, 99x avg X — boost
    #   0.75-0.95 (late): 75.1% WR, 100x avg X — boost
    #   >0.95: approaching final_hour, attenuate
    phase_stake_multipliers: tuple[tuple[float, float], ...] = ()


FAST_TP_MODE = ModeConfig(
    name="fast_tp_mode",
    entry_price_levels=(0.005, 0.01, 0.02, 0.03, 0.05),
    entry_price_max=0.05,
    use_resting_bids=False,
    scanner_entry=True,
    tp_levels=(
        TPLevel(progress=0.50, fraction=0.70),
        TPLevel(progress=0.80, fraction=0.30),
    ),
    moonbag_fraction=0.0,
    stake_usdc=0.05,
    max_open_positions=30,
    max_resting_markets=0,
    max_resting_per_cluster=0,
    min_hours_to_close=1.0,
    max_hours_to_close=48.0,
    # duration matters most: scanner needs markets closing soon for quick exit
    scoring_weights=(
        ("market_score", 0.40),
        ("duration",     0.35),
        ("liq",          0.15),
        ("category",     0.10),
    ),
    prefer_long_duration=False,
)

BALANCED_MODE = ModeConfig(
    name="balanced_mode",
    entry_price_levels=(0.002, 0.005, 0.01, 0.02, 0.05),
    entry_price_max=0.10,
    use_resting_bids=True,
    scanner_entry=True,
    tp_levels=(
        TPLevel(progress=0.25, fraction=0.35),
        TPLevel(progress=0.50, fraction=0.25),
        TPLevel(progress=0.75, fraction=0.20),
    ),
    moonbag_fraction=0.20,
    stake_usdc=0.05,
    max_open_positions=20,
    max_resting_markets=1000,
    max_resting_per_cluster=3,
    min_hours_to_close=1.0,
    max_hours_to_close=120.0,
    scoring_weights=(
        ("market_score", 0.50),
        ("duration",     0.25),
        ("liq",          0.15),
        ("category",     0.10),
    ),
    prefer_long_duration=False,
)

# Budget and levels are defined together so market_score_tiers stakes auto-scale with budget.
# Current recommendation: conservative bankroll-aware big_swan sizing.
_BIG_SWAN_BUDGET = 0.5
_BIG_SWAN_LEVELS = (0.01, 0.10, 0.15)
_bsm_s = _BIG_SWAN_BUDGET / len(_BIG_SWAN_LEVELS)  # stake per level at full-budget allocation

BIG_SWAN_MODE = ModeConfig(
    name="big_swan_mode",
    # wide range — we pre-position early, resting bids at floor levels
    entry_price_levels=_BIG_SWAN_LEVELS,
    entry_price_max=0.20,       # must match SWAN_BUY_PRICE_THRESHOLD — no scorer data above this
    use_resting_bids=True,
    scanner_entry=False,        # ONLY resting bids; no chasing dips
    # First-step binary-native ladder.
    # Tier-aware progress presets are intentionally deferred.
    tp_levels=(
        TPLevel(progress=0.10, fraction=0.10),
        TPLevel(progress=0.50, fraction=0.20),
    ),
    moonbag_fraction=0.70,
    stake_usdc=0.05,            # fallback if no tier matches
    max_open_positions=500,
    max_resting_markets=5000,
    max_resting_per_cluster=1,
    max_cohort_size=5,           # skip 1-of-N guessing games (>5 outcomes in a cohort)
    min_hours_to_close=0.25,     # 15 min — allow short-lived markets
    max_hours_to_close=168.0,    # 7 days — big events need time to materialise
    hours_to_close_null_default=48.0,
    hours_to_close_null_reject=True,   # reject markets with unknown end_date (#184)
    # v1.1: market_score gate — reject bottom-half markets
    min_market_score=0.25,
    # Conservative bankroll-aware sizing: full / half / quarter of the $0.50 market budget.
    market_score_tiers=(
        (0.60, _bsm_s),
        (0.40, _bsm_s * 0.50),
        (0.25, _bsm_s * 0.25),
    ),
    max_exposure_per_market=_BIG_SWAN_BUDGET,
    # Duration omitted: all markets passing the 168h hard gate get duration_score=1.0
    # (prefer_long_duration=True + horizon=max_hours_to_close = constant signal).
    # Redistributed to market_score for cleaner ranking.
    scoring_weights=(
        ("market_score", 0.70),
        ("liq",          0.20),
        ("category",     0.10),
    ),
    prefer_long_duration=True,   # flat score=1.0 up to 168h, decay after
)

SMALL_SWAN_MODE = ModeConfig(
    name="small_swan_mode",
    # moderate dip — resting bids at 5c/10c/15c/20c zones
    entry_price_levels=(0.05, 0.10, 0.15, 0.20),
    entry_price_max=0.50,       # screen markets with price up to 50c
    use_resting_bids=True,
    scanner_entry=False,        # pre-position only
    tp_levels=(
        TPLevel(progress=0.20, fraction=0.30),   # quick capital recoup
        TPLevel(progress=0.50, fraction=0.30),   # partial profit
    ),
    moonbag_fraction=0.40,      # 40% held to resolution
    stake_usdc=0.10,            # fallback if no tier matches
    max_open_positions=100,
    max_resting_markets=1000,
    max_resting_per_cluster=1,
    min_hours_to_close=1.0,
    max_hours_to_close=48.0,     # faster turnover — dip/recovery cycle is short
    scoring_weights=(
        ("market_score", 0.50),
        ("duration",     0.25),
        ("liq",          0.15),
        ("category",     0.10),
    ),
    prefer_long_duration=False,
    # deeper floor = larger bet
    stake_tiers=(
        (0.05,  0.20),
        (0.10,  0.10),
        (0.15,  0.05),
        (0.20,  0.05),
    ),
)

_BSV1_BUDGET = 1.0   # USDC per market (spread across 5 price levels)
_BSV1_LEVELS = (0.005, 0.01, 0.02, 0.03, 0.05)

BLACK_SWAN_MODE = ModeConfig(
    name="black_swan_mode",
    # Strict low-zone: 0.5c → 5c only. No bids above 5c (no scorer data above 0.20,
    # and black_swan=1 requires buy_min_price <= 0.05 by definition).
    entry_price_levels=_BSV1_LEVELS,
    entry_price_max=0.05,
    use_resting_bids=True,
    scanner_entry=False,       # pre-deployed resting only; median window = 18 min
    tp_levels=(
        TPLevel(progress=0.30, fraction=0.10),  # sell 10% at ~30c to recoup
        TPLevel(progress=0.70, fraction=0.10),  # sell 10% at ~70c
    ),
    moonbag_fraction=0.80,     # hold 80% to resolution — high-X tail is the edge
    stake_usdc=0.20,           # fallback
    max_open_positions=2000,
    max_resting_markets=20000,
    max_resting_per_cluster=1,
    max_cohort_size=5,
    # 0.4h (24 min): captures 45-60m duration markets in their first 21-36 min.
    # Excludes 15m markets (≤15min remaining always < 24min) — those use a
    # separate strategy. Issue #180 follow-up: 45-60m cohort = 63.6% WR / 49x
    # avg X / $245 vol, currently profitable; was being dropped by the prior
    # 1.0h gate.
    min_hours_to_close=0.4,
    max_hours_to_close=168.0,
    # 50 min: include one-hour markets while still excluding true 15m/ultra-short
    # markets by planned start/end duration when Gamma provides both timestamps.
    min_total_duration_hours=50.0 / 60.0,
    hours_to_close_null_default=48.0,
    hours_to_close_null_reject=True,   # reject markets with unknown end_date (#184)
    # #196 audit: keep MarketScorer as a real hard gate, not just a ranking log.
    # Conservative starting threshold; revisit with score-bucket replay once more
    # paper outcomes accumulate.
    min_market_score=0.25,
    # Lower prices = higher X: allocate more stake there
    stake_tiers=(
        (0.005, _BSV1_BUDGET * 0.40),  # 0.5c tier: 40¢
        (0.01,  _BSV1_BUDGET * 0.25),  # 1c tier:   25¢
        (0.02,  _BSV1_BUDGET * 0.15),  # 2c tier:   15¢
        (0.03,  _BSV1_BUDGET * 0.12),  # 3c tier:   12¢
        (0.05,  _BSV1_BUDGET * 0.08),  # 5c tier:    8¢
    ),
    scoring_weights=(
        ("market_score", 0.60),
        ("liq",          0.25),
        ("category",     0.15),
    ),
    prefer_long_duration=True,  # flat score=1.0 up to 168h
    # Exclude weak sports subtypes (WR < 60% in phase A3 analysis):
    #   Tennis ~54%, F1 ~46%, CS2 ~60%, Valorant ~58%
    exclude_question_keywords=(
        "tennis", " atp ", "wta ", "wimbledon", "roland garros",
        "formula 1", "formula one", "grand prix", "f1 race",
        "counter-strike", " cs2 ", "blast premier", "esl pro league",
        "valorant", " vct ",
    ),
    # Slug prefix filters from issue #180 Phase A.
    # Block weak subtypes; require strong subtypes for sports markets only.
    exclude_slug_prefixes=(
        "atp-", "wta-", "f1-", "grand-prix-",
        "cfb-", "cbb-", "cs2-", "es2-", "val-",
    ),
    include_slug_prefixes_for_sports=(
        # soccer leagues
        "epl-", "lal-", "ucl-", "uel-", "bun-", "fl1-", "sea-", "elc-",
        "spl-", "ere-", "bl2-", "tur-", "por-", "arg-", "mex-",
        # major US leagues
        "nba-", "nhl-", "mlb-", "nfl-", "mls-",
        # esports (strong)
        "lol-", "league-of-legends-",
    ),
    duration_stake_multipliers=(
        # ≤15m: no analytics yet — very short lifecycle, high variance.
        # Gate currently at min_hours_to_close=0.4h so this is prep for
        # when 15m markets are enabled.
        (0.25,          0.40),
        # ≤1h: 45-60m bucket (63.6% WR / 49x avg X / $245 vol).
        (1.0,           0.60),
        (6.0,           0.70),  # 1–6h: moderate (52-72% WR depending on phase)
        (float("inf"),  1.00),  # >6h (workhorse 6h–7d cohort, best avg X)
    ),
    phase_stake_multipliers=(
        (0.10, 0.50),  # opening_10pct: thin, dampen
        (0.75, 1.30),  # middle: best cohort
        (0.95, 1.30),  # late: best cohort
        (1.00, 0.60),  # final ~5% of lifecycle approaching final_hour
    ),
    # Cluster bonuses from #180 Phase A "Main category × duration clusters".
    # 1.0 = neutral; entries omitted from the dict default to 1.0 in scorer.
    cluster_score_multipliers={
        ("crypto",   "15m"):  0.70,  # avg 31x, $232 vol — worst cluster
        ("crypto",   "1h"):   0.95,  # avg 50x — slightly below baseline
        ("crypto",   "1-7d"): 1.20,  # avg 91x — better tail
        ("sports",   "15m"):  0.95,  # avg 57x but noisy
        ("sports",   "6h"):   1.05,  # avg 66x
        ("sports",   "1-7d"): 1.00,  # avg 62x
        ("weather",  "15m"):  1.00,  # no analytics yet — neutral placeholder
        ("weather",  "1-7d"): 1.30,  # avg 116x — best cluster
        ("politics", "1-7d"): 1.20,  # avg ~120x small sample
    },
)

MODES: dict[str, ModeConfig] = {
    "fast_tp_mode": FAST_TP_MODE,
    "balanced_mode": BALANCED_MODE,
    "big_swan_mode": BIG_SWAN_MODE,
    "small_swan_mode": SMALL_SWAN_MODE,
    "black_swan_mode": BLACK_SWAN_MODE,
}

# ── Swan analyzer global threshold ────────────────────────────────────────────
# FIXED constant — reflects the buy_min_price ceiling used when swans_v2 was
# collected. History is the source of truth: the bot must not bid above this
# level, because there is no scorer data for higher prices.
#
# Do NOT derive this from entry_price_levels — that would make the check
# circular (threshold always ≥ any level, warning never fires).
#
# To raise the ceiling: re-run swan_analyzer with --buy-price-threshold <new>,
# then update this constant.
SWAN_BUY_PRICE_THRESHOLD: float = 0.20

# ── Swan analysis thresholds ──────────────────────────────────────────────────
# Single source of truth — used by swan_analyzer, feature_mart, rejected_outcomes.
SWAN_ENTRY_MAX: float = SWAN_BUY_PRICE_THRESHOLD  # buy_min_price ceiling for a valid "swan event" — must equal collection threshold
SWAN_MIN_BUY_VOLUME: float = 1.0     # min USDC traded at the floor (liquidity check)
SWAN_MIN_SELL_VOLUME: float = 30.0   # min USDC traded at the exit (winners exempt)
SWAN_MIN_REAL_X: float = 5.0         # min price multiplier to count as a real swan


def check_swan_buy_price_threshold(mode_config: "ModeConfig") -> list[str]:
    """
    Returns warning strings if the active mode has entry levels above the
    collected data ceiling (SWAN_BUY_PRICE_THRESHOLD).
    Empty list = OK.

    History is the source of truth: the bot must not bid at prices where
    swans_v2 has no data, because the scorer will silently fall back to
    defaults and sizing/scoring will be blind.
    """
    uncovered = [p for p in mode_config.entry_price_levels if p > SWAN_BUY_PRICE_THRESHOLD]
    if uncovered:
        return [
            f"ALERT: mode '{mode_config.name}' has entry levels {uncovered} "
            f"above SWAN_BUY_PRICE_THRESHOLD={SWAN_BUY_PRICE_THRESHOLD} — "
            f"swans_v2 has no data for these prices. "
            f"Re-run swan_analyzer with --buy-price-threshold {max(uncovered)} "
            f"then raise SWAN_BUY_PRICE_THRESHOLD."
        ]
    return []


@dataclass
class BotConfig:
    """Runtime configuration loaded from environment / .env."""

    mode: str = "big_swan_mode"
    dry_run: bool = True
    paper_initial_balance_usdc: float = field(
        default_factory=lambda: float(os.environ.get("PAPER_INITIAL_BALANCE_USDC", "100.0"))
    )

    # ── CLOB credentials (from env) ───────────────────────────────────────────
    private_key: str = field(default_factory=lambda: os.environ.get("POLY_PRIVATE_KEY", ""))

    # ── Polling intervals (seconds) ───────────────────────────────────────────
    screener_interval: int = 300     # 5 minutes
    monitor_interval: int = 90       # 1.5 minutes
    resting_cleanup_interval: int = 3600  # 1 hour

    # ── Screener hard limits ──────────────────────────────────────────────────
    min_volume_usdc: float = 50.0
    max_volume_usdc: float = float("inf")  # no upper cap: hit rate rises with volume (300k-1M: 19.8%, >1M: 19.2%)
    dead_market_hours: float = 48.0    # reject markets with no trades in this many hours

    # ── Category EV weights (recalibrated from black_swan=1 data, issue #196) ──
    # Source: corrected strict black_swan universe excluding true 15m markets
    # (duration_hours IS NOT NULL AND abs(duration_hours - 0.25) <= 1e-6).
    # hit_rate = winners / (winners + strict_losers) per category.
    # avg_x from #196 analysis (2025-08-01 → 2026-05-08).
    #
    # category      winners  losers  hit_rate  avg_x   weight
    # weather          470      85     84.7%   112.4x   1.5
    # geopolitics       42       8     84.0%    85.3x   1.5  (small sample)
    # politics         154      48     76.2%   121.0x   1.4
    # entertainment     44      16     73.3%   136.1x   1.2  (small sample, high X)
    # crypto          1439     584     71.1%    63.6x   1.1
    # health             3       1     75.0%    52.8x   1.0  (tiny sample)
    # sports          2449    1356     64.4%    60.8x   0.8  (largest source, noisy)
    # tech              48      35     57.8%    85.3x   0.6
    # esports          n/a     n/a       n/a      n/a   0.8  (no direct #196 data)
    category_weights: dict = field(default_factory=lambda: {
        "weather":      1.5,
        "geopolitics":  1.5,
        "politics":     1.4,
        "entertainment":1.2,
        "crypto":       1.1,
        "health":       1.0,
        "sports":       0.8,
        "tech":         0.6,
        "esports":      0.8,
    })

    @property
    def mode_config(self) -> ModeConfig:
        return MODES[self.mode]

    def validate(self) -> None:
        if self.mode not in MODES:
            raise ValueError(f"Unknown mode: {self.mode!r}. Choose from {list(MODES)}")
        mc = self.mode_config
        tp_total = sum(tp.fraction for tp in mc.tp_levels)
        if tp_total + mc.moonbag_fraction > 1.0 + 1e-9:
            raise ValueError(
                f"Mode {self.mode}: tp fractions ({tp_total:.2f}) + moonbag "
                f"({mc.moonbag_fraction:.2f}) > 1.0"
            )
        if not mc.scoring_weights:
            raise ValueError(f"Mode {self.mode}: scoring_weights must be defined")
        # History is source of truth: warn if active mode bids above data ceiling.
        for msg in check_swan_buy_price_threshold(mc):
            import warnings as _w
            _w.warn(msg, stacklevel=3)


def load_config() -> BotConfig:
    cfg = BotConfig(
        mode=os.environ.get("BOT_MODE", "big_swan_mode"),
        dry_run=os.environ.get("DRY_RUN", "true").lower() not in ("false", "0", "no"),
    )
    cfg.validate()
    return cfg
