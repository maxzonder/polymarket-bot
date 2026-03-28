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
    x: float       # multiplier vs entry (5.0 = 5x)
    fraction: float  # fraction of position to sell at this level (0.0–1.0)


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

    # ── Scoring gates ─────────────────────────────────────────────────────────
    # min historical P(market hits entry zone) to bother placing resting bid
    min_entry_fill_score: float
    # min resolution score to accept a trade candidate
    min_resolution_score: float
    # min historical real_x (excluding small bounces)
    min_real_x_historical: float

    # ── Position sizing ───────────────────────────────────────────────────────
    stake_usdc: float          # default stake per trade (USDC tokens bought at entry)
    max_open_positions: int
    max_resting_markets: int        # max distinct markets with live resting bids
    max_resting_per_cluster: int    # max markets per cluster (market_id // 1000)
    max_capital_deployed_pct: float  # max % of balance in open positions

    # ── Time window ───────────────────────────────────────────────────────────
    min_hours_to_close: float   # reject markets closing sooner than this
    max_hours_to_close: float   # reject markets closing later than this

    # ── Optimisation target ───────────────────────────────────────────────────
    # "tail_ev" | "ev_total" | "roi_pct"
    optimize_metric: str

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


FAST_TP_MODE = ModeConfig(
    name="fast_tp_mode",
    entry_price_levels=(0.005, 0.01, 0.02, 0.03, 0.05),
    entry_price_max=0.05,
    use_resting_bids=False,
    scanner_entry=True,
    tp_levels=(
        TPLevel(x=5.0, fraction=0.70),
        TPLevel(x=10.0, fraction=0.30),
    ),
    moonbag_fraction=0.0,
    min_entry_fill_score=0.0,   # scanner-triggered — no fill score needed
    min_resolution_score=0.0,
    min_real_x_historical=5.0,
    stake_usdc=0.05,
    max_open_positions=30,
    max_resting_markets=0,
    max_resting_per_cluster=0,
    max_capital_deployed_pct=0.40,
    min_hours_to_close=1.0,
    max_hours_to_close=48.0,
    optimize_metric="ev_total",
)

BALANCED_MODE = ModeConfig(
    name="balanced_mode",
    entry_price_levels=(0.002, 0.005, 0.01, 0.02, 0.05),
    entry_price_max=0.10,
    use_resting_bids=True,
    scanner_entry=True,
    tp_levels=(
        TPLevel(x=5.0, fraction=0.35),
        TPLevel(x=10.0, fraction=0.25),
        TPLevel(x=20.0, fraction=0.20),
    ),
    moonbag_fraction=0.20,
    min_entry_fill_score=0.05,
    min_resolution_score=0.10,
    min_real_x_historical=5.0,
    stake_usdc=0.05,
    max_open_positions=20,
    max_resting_markets=1000,
    max_resting_per_cluster=3,
    max_capital_deployed_pct=0.35,
    min_hours_to_close=1.0,
    max_hours_to_close=120.0,
    optimize_metric="ev_total",
)

BIG_SWAN_MODE = ModeConfig(
    name="big_swan_mode",
    # wide range — we pre-position early, resting bids at floor levels
    entry_price_levels=(0.001, 0.005, 0.01),
    entry_price_max=0.30,       # screen markets with price up to 30c
    use_resting_bids=True,
    scanner_entry=False,        # ONLY resting bids; no chasing dips
    tp_levels=(
        TPLevel(x=5.0, fraction=0.20),   # recoup capital, keep running
        TPLevel(x=20.0, fraction=0.20),  # partial profit lock
    ),
    moonbag_fraction=0.60,      # 60% held to resolution
    min_entry_fill_score=0.02,  # low bar — wide coverage
    min_resolution_score=0.15,
    min_real_x_historical=10.0,
    stake_usdc=0.05,            # fallback if no tier matches
    max_open_positions=500,
    max_resting_markets=5000,
    max_resting_per_cluster=10,
    max_capital_deployed_pct=0.99,
    min_hours_to_close=1.0,
    max_hours_to_close=720.0,   # raised from 120h: Stage 0 shows >6mo = 10.6% swan_rate
    hours_to_close_null_default=48.0,  # safe fallback for markets without deadline info
    optimize_metric="tail_ev",
    # Price-tier stakes: deeper floor = bigger bet (higher upside)
    # 0.001 → $0.50 (1000x potential), 0.005 → $0.25, 0.010 → $0.10
    stake_tiers=(
        (0.001, 0.50),
        (0.005, 0.25),
        (0.010, 0.10),
    ),
    # v1.1: market_score gate — reject bottom-half markets
    min_market_score=0.25,
    # v1.1: quality-weighted stake override (higher score → bigger stake per level)
    # Applied as a multiplier on top of stake_tiers: top10 gets max, pass gets min.
    # Set to non-empty to activate (replaces stake_tiers when market_score_tiers active).
    market_score_tiers=(
        (0.60, 0.50),   # top10:  $0.50 per entry level
        (0.40, 0.25),   # top25:  $0.25 per entry level
        (0.25, 0.10),   # pass:   $0.10 per entry level
    ),
    # v1.1: per-market cap — never deploy more than $2.00 on a single token
    max_exposure_per_market=2.0,
)

DIP_MODE = ModeConfig(
    name="dip_mode",
    # moderate dip — resting bids at 5c/10c/15c/20c zones
    entry_price_levels=(0.05, 0.10, 0.15, 0.20),
    entry_price_max=0.50,       # screen markets with price up to 50c
    use_resting_bids=True,
    scanner_entry=False,        # pre-position only
    tp_levels=(
        TPLevel(x=2.0, fraction=0.30),   # quick capital recoup
        TPLevel(x=5.0, fraction=0.30),   # partial profit
    ),
    moonbag_fraction=0.40,      # 40% held to resolution
    min_entry_fill_score=0.05,
    min_resolution_score=0.10,
    min_real_x_historical=2.0,  # lower bar vs big_swan (max upside 5–20x from these levels)
    stake_usdc=0.10,            # fallback if no tier matches
    max_open_positions=100,
    max_resting_markets=1000,
    max_resting_per_cluster=1,
    max_capital_deployed_pct=0.50,
    min_hours_to_close=1.0,
    max_hours_to_close=120.0,
    optimize_metric="ev_total",
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
    "dip_mode": DIP_MODE,
}

# ── Swan analyzer global threshold ────────────────────────────────────────────
# Must cover ALL entry_price_levels across ALL modes.
# swan_analyzer.py uses this as --entry-threshold default.
# If a mode has entry levels above this value, swans_v2 will be missing data
# for those levels and scorer/feature_mart will have blind spots.
SWAN_ENTRY_THRESHOLD: float = max(
    max(m.entry_price_levels) for m in MODES.values()
)


def check_swan_threshold(threshold: float) -> list[str]:
    """
    Returns a list of warning strings if threshold doesn't cover all mode entry levels.
    Empty list = OK.
    """
    warnings = []
    for mode in MODES.values():
        uncovered = [p for p in mode.entry_price_levels if p > threshold]
        if uncovered:
            warnings.append(
                f"WARNING: swan_analyzer threshold ${threshold} < "
                f"{mode.name} entry levels {uncovered} — "
                f"swans_v2 will miss these price zones"
            )
    return warnings


@dataclass
class BotConfig:
    """Runtime configuration loaded from environment / .env."""

    mode: str = "big_swan_mode"
    dry_run: bool = True

    # ── CLOB credentials (from env) ───────────────────────────────────────────
    private_key: str = field(default_factory=lambda: os.environ.get("POLY_PRIVATE_KEY", ""))
    api_key: str = field(default_factory=lambda: os.environ.get("POLY_API_KEY", ""))
    api_secret: str = field(default_factory=lambda: os.environ.get("POLY_API_SECRET", ""))
    api_passphrase: str = field(default_factory=lambda: os.environ.get("POLY_PASSPHRASE", ""))

    # ── Polling intervals (seconds) ───────────────────────────────────────────
    screener_interval: int = 300     # 5 minutes
    monitor_interval: int = 90       # 1.5 minutes
    resting_cleanup_interval: int = 3600  # 1 hour

    # ── Resting order TTL (DEPRECATED — commit 7855cac) ─────────────────────
    # No longer used as cancellation trigger. Resting bids are GTC and stay
    # live until the market closes or disappears from Gamma.
    # Field kept for backward-compat with DB schema (expires_at column).
    resting_order_ttl: int = 0       # deprecated, was 86400

    # ── Screener hard limits ──────────────────────────────────────────────────
    min_volume_usdc: float = 50.0
    max_volume_usdc: float = 300_000.0  # raised from 50k: geopolitics/politics markets often 100k–1M

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


def load_config() -> BotConfig:
    cfg = BotConfig(
        mode=os.environ.get("BOT_MODE", "big_swan_mode"),
        dry_run=os.environ.get("DRY_RUN", "true").lower() not in ("false", "0", "no"),
    )
    cfg.validate()
    return cfg
