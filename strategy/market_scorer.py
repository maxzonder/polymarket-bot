"""
Market-Level Scorer (v1.1) — per-market scoring for big_swan_mode.

Replaces the category-level EntryFillScorer + ResolutionScorer with a
single market_score computed from market-level features.

Formula (initial weights from Stage 0 cohort analysis, issue #45):
    market_score = 0.35 · liquidity_score
                 + 0.25 · niche_score
                 + 0.15 · time_score
                 + 0.15 · analogy_score
                 + 0.10 · context_score

Weights are initial hypotheses from Dec–Feb data; calibrate after Stage 2
paper trading.

Sub-scores:
    liquidity_score — log(volume+1) / log(max_observed_volume+1)
                      Strongest predictor: 7.58x lift in top decile.
    niche_score     — 1 / (1 + log1p(comment_count))
                      Proxy for attention/neglect at screener time.
                      Lower comment_count → more niche → better floor pricing.
    time_score      — min(duration_hours / 720, 1.0)
                      Confirmed: >6mo markets have 10.6% swan_rate vs 0.1%.
    analogy_score   — historical good_swan_rate for (category, vol_bucket)
                      normalised to [0, 1] using max observed rate.
                      Built from feature_mart_v1_1 at init time.
    context_score   — neg_risk flag (0.6 if neg_risk else 0.2)
                      neg_risk=1 markets have 4.5x higher swan_rate.

Tier classification:
    top10   total >= TOP10_THRESHOLD
    top25   total >= TOP25_THRESHOLD
    pass    total >= min_score
    reject  total < min_score
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from typing import Optional

from api.gamma_client import MarketInfo
from utils.paths import DB_PATH

# ── Weights (from Stage 0 cohort analysis, issue #45) ───────────────────────
W_LIQUIDITY = 0.35
W_NICHE     = 0.25
W_TIME      = 0.15
W_ANALOGY   = 0.15
W_CONTEXT   = 0.10

# ── Tier thresholds (tune after Stage 2 paper trading) ──────────────────────
TOP10_THRESHOLD  = 0.60   # top ~10% of screener-eligible markets
TOP25_THRESHOLD  = 0.40   # top ~25%
PASS_THRESHOLD   = 0.25   # minimum to be considered at all

# ── Analogy lookup ────────────────────────────────────────────────────────────
# Vol buckets must match feature_mart_v1_1 cohort analysis bucket definitions
def _vol_bucket(volume: float) -> str:
    if volume < 10_000:
        return "<10k"
    if volume < 100_000:
        return "10k-100k"
    if volume < 1_000_000:
        return "100k-1M"
    return ">1M"


@dataclass
class MarketScore:
    """Result of market_score computation for a single market."""
    market_id: str
    liquidity_score: float
    niche_score: float
    time_score: float
    analogy_score: float
    context_score: float
    total: float
    tier: str      # "top10" | "top25" | "pass" | "reject"
    rationale: str


def _make_rationale(ms: "MarketScore") -> str:
    return (
        f"total={ms.total:.3f}({ms.tier}) "
        f"liq={ms.liquidity_score:.3f} "
        f"niche={ms.niche_score:.3f} "
        f"time={ms.time_score:.3f} "
        f"analogy={ms.analogy_score:.3f} "
        f"ctx={ms.context_score:.3f}"
    )


class MarketScorer:
    """
    Computes market_score for a MarketInfo object.

    Must be initialised once (loads analogy table from feature_mart_v1_1).
    Refreshable with .refresh().
    """

    def __init__(self, db_path=None, min_score: float = PASS_THRESHOLD):
        self._db_path = str(db_path or DB_PATH)
        self.min_score = min_score
        self._analogy: dict[tuple[str, str], float] = {}
        self._max_analogy: float = 1.0
        self._max_log_volume: float = math.log1p(1_000_000)  # fallback if no DB
        self._load_analogy_table()

    def _load_analogy_table(self) -> None:
        """
        Build analogy lookup: (category, vol_bucket) → good_swan_rate.
        Normalises to [0, 1] using the max observed rate.
        Falls back silently if feature_mart_v1_1 doesn't exist yet.
        """
        try:
            conn = sqlite3.connect(self._db_path)
            conn.row_factory = sqlite3.Row

            # Max observed log_volume for normalisation
            r = conn.execute("SELECT MAX(log_volume) FROM feature_mart_v1_1").fetchone()
            if r and r[0]:
                self._max_log_volume = float(r[0])

            # good_swan_rate = COUNT(label_20x=1) / COUNT(*)  per (category, vol_bucket)
            rows = conn.execute("""
                SELECT
                    COALESCE(category, 'null') AS cat,
                    CASE
                        WHEN volume_usdc < 10000     THEN '<10k'
                        WHEN volume_usdc < 100000    THEN '10k-100k'
                        WHEN volume_usdc < 1000000   THEN '100k-1M'
                        ELSE                              '>1M'
                    END AS vol_bucket,
                    COUNT(*) AS total,
                    SUM(label_20x) AS good
                FROM feature_mart_v1_1
                GROUP BY cat, vol_bucket
                HAVING total >= 20
            """).fetchall()
            conn.close()

            raw: dict[tuple[str, str], float] = {}
            for r in rows:
                cat, vbk, total, good = r["cat"], r["vol_bucket"], r["total"], r["good"]
                rate = (good or 0) / max(total, 1)
                raw[(cat, vbk)] = rate

            max_rate = max(raw.values()) if raw else 1.0
            self._max_analogy = max_rate
            # Normalise to [0, 1]
            self._analogy = {k: v / max_rate for k, v in raw.items()}

        except Exception:
            # feature_mart_v1_1 not yet built — use fallback (zero analogy scores)
            self._analogy = {}

    def refresh(self) -> None:
        self._load_analogy_table()

    def score(self, market: MarketInfo) -> MarketScore:
        """Compute market_score for a MarketInfo object."""
        return self._score_raw(
            market_id=market.market_id,
            volume=market.volume_usdc,
            comment_count=market.comment_count,
            hours_to_close=market.hours_to_close,
            category=market.category,
            neg_risk=market.neg_risk,
        )

    def score_from_db(
        self,
        market_id: str,
        volume: float,
        comment_count: int,
        hours_to_close: Optional[float],
        category: Optional[str],
        neg_risk: bool = False,
    ) -> MarketScore:
        """Score a market given raw DB fields (for screener and replay contexts)."""
        return self._score_raw(
            market_id=market_id,
            volume=volume,
            comment_count=comment_count,
            hours_to_close=hours_to_close,
            category=category,
            neg_risk=neg_risk,
        )

    def _score_raw(
        self,
        market_id: str,
        volume: float,
        comment_count: int,
        hours_to_close: Optional[float],
        category: Optional[str],
        neg_risk: bool = False,
    ) -> MarketScore:

        # ── liquidity_score: log(volume+1) / log(max_observed+1) ─────────────
        log_vol = math.log1p(max(volume, 0))
        liquidity_score = min(log_vol / max(self._max_log_volume, 1.0), 1.0)

        # ── niche_score: 1 / (1 + log1p(comment_count)) ─────────────────────
        # comment_count=0 → niche_score=1.0 (maximally neglected)
        # comment_count=100 → niche_score ≈ 0.22
        niche_score = 1.0 / (1.0 + math.log1p(max(comment_count, 0)))

        # ── time_score: duration proxy via hours_to_close ────────────────────
        # We use hours_to_close as a forward-looking duration proxy at screener time.
        # Prefer markets with 168h–8760h remaining (1 week to 1 year).
        # Markets < 24h are penalised (not enough time for event to materialise).
        hours = hours_to_close or 0.0
        if hours < 24:
            time_score = 0.1
        elif hours < 168:
            # 1–7 days: linear ramp
            time_score = 0.1 + 0.5 * (hours - 24) / 144
        else:
            # 7 days to 1 year: saturates at 1.0 by 720h (1 month)
            time_score = min(0.6 + 0.4 * (hours - 168) / (720 - 168), 1.0)

        # ── analogy_score: historical good_swan_rate ──────────────────────────
        cat = (category or "null").lower()
        vbk = _vol_bucket(volume)
        # Try exact match, then category-only, then global fallback
        # Explicit None checks: a stored 0.0 (proven zero swan_rate) must not
        # fall through to the floor. Python treats 0.0 as falsy in `or` chains.
        _analogy_exact = self._analogy.get((cat, vbk))
        _analogy_null  = self._analogy.get(("null", vbk))
        if _analogy_exact is not None:
            analogy_score = _analogy_exact
        elif _analogy_null is not None:
            analogy_score = _analogy_null
        else:
            analogy_score = 0.05  # global floor: rare but possible

        # ── context_score: neg_risk flag ──────────────────────────────────────
        # neg_risk=1 shows 4.5x higher swan_rate (2.7% vs 0.6%) from cohort analysis.
        # We give a meaningful boost rather than a symbolic one.
        context_score = 0.6 if neg_risk else 0.2

        # ── Composite ─────────────────────────────────────────────────────────
        total = (
            W_LIQUIDITY * liquidity_score
            + W_NICHE    * niche_score
            + W_TIME     * time_score
            + W_ANALOGY  * analogy_score
            + W_CONTEXT  * context_score
        )
        total = round(min(total, 1.0), 4)

        if total >= TOP10_THRESHOLD:
            tier = "top10"
        elif total >= TOP25_THRESHOLD:
            tier = "top25"
        elif total >= self.min_score:
            tier = "pass"
        else:
            tier = "reject"

        ms = MarketScore(
            market_id=market_id,
            liquidity_score=round(liquidity_score, 4),
            niche_score=round(niche_score, 4),
            time_score=round(time_score, 4),
            analogy_score=round(analogy_score, 4),
            context_score=round(context_score, 4),
            total=total,
            tier=tier,
            rationale="",
        )
        ms.rationale = _make_rationale(ms)
        return ms

    def stake_for_score(
        self,
        market_score: float,
        stake_tiers: tuple[tuple[float, float], ...],
        fallback_stake: float,
    ) -> float:
        """
        Map market_score to quality-weighted stake.

        stake_tiers: sorted by threshold descending, e.g.
            ((0.60, 0.50), (0.40, 0.25), (0.25, 0.10))
        Returns the stake for the first tier whose threshold the score meets.
        Falls back to fallback_stake if no tier matches.
        """
        for threshold, stake in sorted(stake_tiers, key=lambda t: t[0], reverse=True):
            if market_score >= threshold:
                return stake
        return fallback_stake
