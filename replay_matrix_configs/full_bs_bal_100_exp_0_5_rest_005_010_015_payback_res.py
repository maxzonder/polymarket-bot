# Auto-generated replay matrix config.
# balance=100 exposure=0.5 entry_set=005_010_015 ladder=payback_res

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

    # ── Screener scoring weights ───────────────────────────────────────────────
    # Tuple of (component_name, weight) pairs. Weights should sum to 1.0.
    # Components: "market_score", "liq", "duration", "category"
    # Empty tuple = legacy ef_score/res_score formula (backward-compat fallback).
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
_BIG_SWAN_LEVELS = (0.05, 0.10, 0.15)
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
        TPLevel(progress=0.50, fraction=0.18),
    ),
    moonbag_fraction=0.82,
    stake_usdc=0.05,            # fallback if no tier matches
    max_open_positions=500,
    max_resting_markets=5000,
    max_resting_per_cluster=1,
    max_cohort_size=5,           # skip 1-of-N guessing games (>5 outcomes in a cohort)
    min_hours_to_close=0.25,     # 15 min — allow short-lived markets
    max_hours_to_close=168.0,    # 7 days — big events need time to materialise
    hours_to_close_null_default=48.0,
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

MODES: dict[str, ModeConfig] = {
    "fast_tp_mode": FAST_TP_MODE,
    "balanced_mode": BALANCED_MODE,
    "big_swan_mode": BIG_SWAN_MODE,
    "small_swan_mode": SMALL_SWAN_MODE,
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
    max_volume_usdc: float = 300_000.0  # raised from 50k: geopolitics/politics markets often 100k–1M
    dead_market_hours: float = 48.0    # reject markets with no trades in this many hours

    # ── Scorer DB window ─────────────────────────────────────────────────────
    # Only use swans_v2 rows with entry_min_price in this range for scoring
    scorer_entry_price_max: float = 0.02
    # Minimum sample count to compute a reliable score
    scorer_min_samples: int = 5

    # ── Category EV weights (derived from feature_mart_v1_1 Dec–Feb 2026) ──────
    # Formula: clip(tail_ev / crypto_tail_ev, 0.5, 1.5)
    # tail_ev = swan_rate * win_rate * avg_x per category
    # crypto used as base (1.0): swan_rate=1.16%, win_rate=14.5%, avg_x=37.8 → tail_ev=0.064
    #
    # category     swan%   win%  avg_x  tail_ev  weight
    # geopolitics  14.98%  6.1%   31.7   0.288    1.5  (was 1.1, raw 4.5x)
    # politics      2.89% 10.7%   30.9   0.096    1.5  (was 1.2, raw 1.5x)
    # crypto        1.16% 14.5%   37.8   0.064    1.0  (base)
    # weather       0.92% 17.9%   38.2   0.063    1.0  (was 0.8)
    # sports        0.43% 23.8%   51.4   0.053    0.8  (was 1.3, raw 0.83x)
    # tech          1.58%  9.9%   23.9   0.037    0.6  (was 0.9, raw 0.58x)
    # entertainment 1.77%  4.3%   15.5   0.012    0.5  (was 0.7, raw 0.18x)
    # esports       no data in Dec–Feb window     1.0  (was 1.5, unknown)
    category_weights: dict = field(default_factory=lambda: {
        "geopolitics":  1.5,
        "politics":     1.5,
        "crypto":       1.0,
        "weather":      1.0,
        "health":       1.0,
        "esports":      1.0,
        "sports":       0.8,
        "tech":         0.6,
        "entertainment":0.5,
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
