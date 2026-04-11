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
                      For neg_risk YES tokens, may use a dedicated YES-only prior
                      from feature_mart_v1_1.
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
        self._analogy_yes_negrisk: dict[tuple[str, str], float] = {}
        self._loser_rates: dict[tuple[str, str], float] = {}
        self._feedback_penalties: dict[tuple[str, str], float] = {}
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

            feature_cols = {
                r[1] for r in conn.execute("PRAGMA table_info(feature_mart_v1_1)").fetchall()
            }

            # good_swan_rate and loser_rate per (category, vol_bucket)
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
                    SUM(label_20x) AS good,
                    SUM(CASE WHEN was_swan = 1 AND (swan_is_winner = 0 OR swan_is_winner IS NULL)
                        THEN 1 ELSE 0 END) AS loser_swans,
                    SUM(was_swan) AS total_swans
                FROM feature_mart_v1_1
                GROUP BY cat, vol_bucket
                HAVING total >= 20
            """).fetchall()

            neg_risk_yes_rows = []
            if "best_swan_is_yes_token" in feature_cols:
                neg_risk_yes_rows = conn.execute("""
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
                    WHERE neg_risk = 1
                      AND best_swan_is_yes_token = 1
                    GROUP BY cat, vol_bucket
                    HAVING total >= 20
                """).fetchall()

            raw: dict[tuple[str, str], float] = {}
            raw_yes_negrisk: dict[tuple[str, str], float] = {}
            raw_loser: dict[tuple[str, str], float] = {}
            for r in rows:
                cat, vbk = r["cat"], r["vol_bucket"]
                total, good = r["total"], r["good"]
                loser_swans, total_swans = r["loser_swans"], r["total_swans"]
                rate = (good or 0) / max(total, 1)
                raw[(cat, vbk)] = rate
                raw_loser[(cat, vbk)] = (loser_swans or 0) / max(total_swans or 1, 1)

            for r in neg_risk_yes_rows:
                cat, vbk = r["cat"], r["vol_bucket"]
                total, good = r["total"], r["good"]
                raw_yes_negrisk[(cat, vbk)] = (good or 0) / max(total, 1)

            max_rate = max([*raw.values(), *raw_yes_negrisk.values()], default=1.0)
            self._max_analogy = max_rate
            # Normalise to [0, 1]
            self._analogy = {k: v / max_rate for k, v in raw.items()}
            self._analogy_yes_negrisk = {k: v / max_rate for k, v in raw_yes_negrisk.items()}
            self._loser_rates = raw_loser
            self._ready = len(self._analogy) > 0

            # Load feedback penalties (from analyze_empty_candidates.py)
            try:
                fp_rows = conn.execute(
                    "SELECT category, vol_bucket, penalty FROM feedback_penalties"
                ).fetchall()
                self._feedback_penalties = {
                    (r["category"], r["vol_bucket"]): float(r["penalty"])
                    for r in fp_rows
                }
            except Exception:
                self._feedback_penalties = {}

            conn.close()

        except Exception:
            # feature_mart_v1_1 not yet built — use fallback (zero analogy scores)
            self._analogy = {}
            self._analogy_yes_negrisk = {}
            self._ready = False

    @property
    def is_ready(self) -> bool:
        """True if analogy table was loaded with sufficient data."""
        return getattr(self, "_ready", False)

    def refresh(self) -> None:
        self._load_analogy_table()

    def score(
        self,
        market: MarketInfo,
        hours_to_close: Optional[float] = None,
        is_no_token: Optional[bool] = None,
    ) -> MarketScore:
        """Compute market_score for a MarketInfo object.

        hours_to_close: pass resolved value (null-default applied) so time_score
        is consistent with screener gate and composite scoring.
        Falls back to market.hours_to_close if not provided.
        """
        return self._score_raw(
            market_id=market.market_id,
            volume=market.volume_usdc,
            comment_count=market.comment_count,
            hours_to_close=hours_to_close if hours_to_close is not None else market.hours_to_close,
            category=market.category,
            neg_risk=market.neg_risk,
            is_no_token=is_no_token,
        )

    def score_from_db(
        self,
        market_id: str,
        volume: float,
        comment_count: int,
        hours_to_close: Optional[float],
        category: Optional[str],
        neg_risk: bool = False,
        is_no_token: Optional[bool] = None,
    ) -> MarketScore:
        """Score a market given raw DB fields (for screener and replay contexts)."""
        return self._score_raw(
            market_id=market_id,
            volume=volume,
            comment_count=comment_count,
            hours_to_close=hours_to_close,
            category=category,
            neg_risk=neg_risk,
            is_no_token=is_no_token,
        )

    def _score_raw(
        self,
        market_id: str,
        volume: float,
        comment_count: int,
        hours_to_close: Optional[float],
        category: Optional[str],
        neg_risk: bool = False,
        is_no_token: Optional[bool] = None,
    ) -> MarketScore:

        # ── liquidity_score: log(volume+1) / log(max_observed+1) ─────────────
        log_vol = math.log1p(max(volume, 0))
        liquidity_score = min(log_vol / max(self._max_log_volume, 1.0), 1.0)

        # ── niche_score: 1 / (1 + log1p(comment_count)) ─────────────────────
        # comment_count=0 → niche_score=1.0 (maximally neglected)
        # comment_count=100 → niche_score ≈ 0.22
        niche_score = 1.0 / (1.0 + math.log1p(max(comment_count, 0)))

        # ── time_score: duration proxy via hours_to_close ────────────────────
        # Issue #53 shows <1d and 1-3d markets have ~16% win rate (equal to 3-7d).
        # We must NOT penalize short markets down to 0.1. Base score 0.5 for anything short,
        # scaling up gently to 1.0 at 720h.
        hours = hours_to_close or 0.0
        time_score = min(0.5 + 0.5 * (hours / 720.0), 1.0)

        # ── analogy_score: historical good_swan_rate ──────────────────────────
        cat = (category or "null").lower()
        vbk = _vol_bucket(volume)
        # Try exact match, then category-only, then global fallback
        # Explicit None checks: a stored 0.0 (proven zero swan_rate) must not
        # fall through to the floor. Python treats 0.0 as falsy in `or` chains.
        _analogy_exact = self._analogy.get((cat, vbk))
        _analogy_null  = self._analogy.get(("null", vbk))
        _analogy_yes_negrisk_exact = None
        _analogy_yes_negrisk_null = None
        if neg_risk and is_no_token is False:
            _analogy_yes_negrisk_exact = self._analogy_yes_negrisk.get((cat, vbk))
            _analogy_yes_negrisk_null = self._analogy_yes_negrisk.get(("null", vbk))

        if _analogy_yes_negrisk_exact is not None:
            analogy_score = _analogy_yes_negrisk_exact
        elif _analogy_yes_negrisk_null is not None:
            analogy_score = _analogy_yes_negrisk_null
        elif _analogy_exact is not None:
            analogy_score = _analogy_exact
        elif _analogy_null is not None:
            analogy_score = _analogy_null
        else:
            analogy_score = 0.05  # global floor: rare but possible

        # ── context_score: neg_risk flag ──────────────────────────────────────
        # neg_risk=1 shows 4.5x higher swan_rate (2.7% vs 0.6%) from cohort analysis.
        # We give a meaningful boost rather than a symbolic one.
        context_score = 0.6 if neg_risk else 0.2

        # ── Feedback penalty: from observed fill/win rates in ml_outcomes ───────
        # Populated by scripts/analyze_empty_candidates.py after 2+ weeks paper trading.
        # Falls back to 1.0 when the table is empty or the cohort has no data yet.
        feedback_penalty = self._feedback_penalties.get((cat, vbk), 1.0)

        # ── Loser penalty: penalise cohorts where swans almost always lose ──────
        # loser_rate > 0.70 means 70%+ of swans in this cohort never paid off.
        # Scaled penalty: loser_rate=0.85 → ×0.7, loser_rate=1.0 → ×0.4 (floor 0.3).
        loser_rate = self._loser_rates.get((cat, vbk), 0.5)
        if loser_rate <= 0.70:
            loser_penalty = 1.0
        else:
            loser_penalty = max(0.3, 1.0 - (loser_rate - 0.70) * 2.0)

        # ── Composite ─────────────────────────────────────────────────────────
        total = (
            W_LIQUIDITY * liquidity_score
            + W_NICHE    * niche_score
            + W_TIME     * time_score
            + W_ANALOGY  * analogy_score
            + W_CONTEXT  * context_score
        ) * loser_penalty * feedback_penalty
        total = round(min(total, 1.0), 4)

        if loser_penalty < 1.0 or feedback_penalty < 1.0:
            import logging as _logging
            _logging.getLogger("market_scorer").debug(
                f"Penalty applied [{market_id[:16]}] cat={cat} vol={vbk} "
                f"loser_penalty={loser_penalty:.2f} feedback_penalty={feedback_penalty:.2f} "
                f"→ total={total:.3f}"
            )

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
