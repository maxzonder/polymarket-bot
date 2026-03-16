"""
Two-score system for big_swan_mode.

EntryFillScorer  — P(market dips to our entry zone)
ResolutionScorer — P(is_winner), E[real_x], P(real_x>=Nx), tail_ev

Both draw from existing swans_v2 + markets tables in the SQLite DB.
They are NEVER combined into one score — they serve different decisions:
  EntryFillScore  → whether to bother placing a resting bid at all
  ResolutionScore → whether a filled position is worth holding vs early exit

Key insight: a market can have high fill score (it dips often) but low resolution
score (it almost never wins). That's a bounce-only market — use fast_tp_mode there.
A market with low fill score but high resolution score is worth pre-positioning
at extreme bid levels and holding to resolution.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional

from utils.paths import DB_PATH


@dataclass
class EntryFillScore:
    """Probability that a market will dip to our entry zone."""
    category: Optional[str]
    sample_count: int                  # total swans in this cohort
    p_fill: float                      # P(market hit entry zone at least once)
    avg_entry_volume_usdc: float       # avg tradeable volume at floor
    avg_floor_duration_seconds: float  # how long price stays in zone
    score: float                       # composite 0–1


@dataclass
class ResolutionScore:
    """Quality of a position AFTER entry — tail potential."""
    category: Optional[str]
    sample_count: int
    p_winner: float                    # P(is_winner=1 | entry zone hit)
    avg_real_x: float
    p_20x: float                       # P(real_x >= 20)
    p_50x: float                       # P(real_x >= 50)
    p_100x: float                      # P(real_x >= 100)
    avg_resolution_x: float            # avg 1/entry_min_price for winners
    tail_ev: float                     # E[real_x | real_x >= 20] * P(real_x >= 20)
    score: float                       # composite 0–1


class EntryFillScorer:
    """
    Uses swans_v2 to estimate P(dip to entry zone) for a category.

    Logic:
    - Count markets in category that had AT LEAST ONE swans_v2 event (floor hit)
    - Divide by total markets in category (proxy for fill rate)
    - Weight by avg entry volume (tradeable liquidity at floor)
    """

    def __init__(
        self,
        db_path=None,
        entry_price_max: float = 0.02,
        min_samples: int = 5,
    ):
        self.db_path = str(db_path or DB_PATH)
        self.entry_price_max = entry_price_max
        self.min_samples = min_samples
        self._cache: dict[Optional[str], EntryFillScore] = {}

    def _load_scores(self) -> dict[Optional[str], EntryFillScore]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        # swans_v2 sample counts per category
        swan_rows = conn.execute("""
            SELECT
                m.category,
                COUNT(DISTINCT s.market_id) AS markets_with_swan,
                COUNT(*) AS swan_count,
                AVG(s.entry_volume_usdc) AS avg_entry_vol,
                AVG(CASE WHEN s.entry_ts_last IS NOT NULL AND s.entry_ts_first IS NOT NULL
                         THEN s.entry_ts_last - s.entry_ts_first ELSE 0 END) AS avg_floor_dur
            FROM swans_v2 s
            JOIN markets m ON s.market_id = m.id
            WHERE s.entry_min_price <= ?
            GROUP BY m.category
        """, (self.entry_price_max,)).fetchall()

        # total market counts per category
        mkt_rows = conn.execute("""
            SELECT category, COUNT(*) AS total_markets
            FROM markets
            GROUP BY category
        """).fetchall()
        conn.close()

        total_by_cat: dict[Optional[str], int] = {r["category"]: r["total_markets"] for r in mkt_rows}

        scores: dict[Optional[str], EntryFillScore] = {}
        for row in swan_rows:
            cat = row["category"]
            n_with_swan = row["markets_with_swan"]
            n_total = total_by_cat.get(cat, max(n_with_swan, 1))
            p_fill = n_with_swan / max(n_total, 1)
            avg_vol = float(row["avg_entry_vol"] or 0)
            avg_dur = float(row["avg_floor_dur"] or 0)
            sample_count = int(row["swan_count"])

            # Composite: weight fill rate by volume signal
            # Normalise volume: $5 avg = 0.5, $50+ = 1.0
            vol_signal = min(avg_vol / 50.0, 1.0)
            composite = p_fill * (0.7 + 0.3 * vol_signal)

            scores[cat] = EntryFillScore(
                category=cat,
                sample_count=sample_count,
                p_fill=p_fill,
                avg_entry_volume_usdc=avg_vol,
                avg_floor_duration_seconds=avg_dur,
                score=composite,
            )

        return scores

    def get(self, category: Optional[str]) -> EntryFillScore:
        """Return fill score for category. Uses cache; refresh with .refresh()."""
        if not self._cache:
            self._cache = self._load_scores()
        score = self._cache.get(category)
        if score is None or score.sample_count < self.min_samples:
            # fallback: global average
            all_scores = list(self._cache.values())
            if all_scores:
                avg_score = sum(s.score for s in all_scores) / len(all_scores)
                return EntryFillScore(
                    category=category,
                    sample_count=0,
                    p_fill=avg_score,
                    avg_entry_volume_usdc=0.0,
                    avg_floor_duration_seconds=0.0,
                    score=avg_score * 0.7,  # penalty for unknown category
                )
            return EntryFillScore(category=category, sample_count=0, p_fill=0.05,
                                  avg_entry_volume_usdc=0.0, avg_floor_duration_seconds=0.0,
                                  score=0.05)
        return score

    def refresh(self) -> None:
        self._cache = self._load_scores()


class ResolutionScorer:
    """
    Uses swans_v2 to score how good a position is AFTER entry.

    Groups by (category, entry_price_bucket) and computes:
    - P(is_winner)
    - P(real_x >= 20x), P(real_x >= 50x), P(real_x >= 100x)
    - tail_ev = E[real_x | real_x >= 20] * P(real_x >= 20)
    - resolution_x distribution for winners
    """

    def __init__(
        self,
        db_path=None,
        min_samples: int = 5,
    ):
        self.db_path = str(db_path or DB_PATH)
        self.min_samples = min_samples
        self._cache: dict[Optional[str], ResolutionScore] = {}

    def _load_scores(self) -> dict[Optional[str], ResolutionScore]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT
                m.category,
                COUNT(*) AS total,
                SUM(s.is_winner) AS winners,
                AVG(s.real_x) AS avg_real_x,
                AVG(s.resolution_x) AS avg_res_x,
                SUM(CASE WHEN s.real_x >= 20 THEN 1 ELSE 0 END) AS cnt_20x,
                SUM(CASE WHEN s.real_x >= 50 THEN 1 ELSE 0 END) AS cnt_50x,
                SUM(CASE WHEN s.real_x >= 100 THEN 1 ELSE 0 END) AS cnt_100x,
                AVG(CASE WHEN s.real_x >= 20 THEN s.real_x ELSE NULL END) AS avg_tail_x
            FROM swans_v2 s
            JOIN markets m ON s.market_id = m.id
            GROUP BY m.category
        """).fetchall()
        conn.close()

        scores: dict[Optional[str], ResolutionScore] = {}
        for row in rows:
            cat = row["category"]
            total = max(int(row["total"] or 1), 1)
            winners = int(row["winners"] or 0)
            p_winner = winners / total
            avg_real_x = float(row["avg_real_x"] or 1.0)
            avg_res_x = float(row["avg_res_x"] or 1.0)
            p_20x = int(row["cnt_20x"] or 0) / total
            p_50x = int(row["cnt_50x"] or 0) / total
            p_100x = int(row["cnt_100x"] or 0) / total
            avg_tail = float(row["avg_tail_x"] or avg_real_x)
            tail_ev = avg_tail * p_20x  # E[real_x | real_x>=20] * P(real_x>=20)

            # Composite: tail_ev dominates, weighted by p_winner as tiebreaker
            # Normalise: tail_ev of 5 → score 0.5; 20 → 0.8; 50+ → 1.0
            score = min(tail_ev / 50.0, 1.0) * 0.7 + p_winner * 0.3

            scores[cat] = ResolutionScore(
                category=cat,
                sample_count=total,
                p_winner=p_winner,
                avg_real_x=avg_real_x,
                p_20x=p_20x,
                p_50x=p_50x,
                p_100x=p_100x,
                avg_resolution_x=avg_res_x,
                tail_ev=tail_ev,
                score=score,
            )

        return scores

    def get(self, category: Optional[str]) -> ResolutionScore:
        if not self._cache:
            self._cache = self._load_scores()
        score = self._cache.get(category)
        if score is None or score.sample_count < self.min_samples:
            all_scores = list(self._cache.values())
            if all_scores:
                avg_score = sum(s.score for s in all_scores) / len(all_scores)
            else:
                avg_score = 0.1
            return ResolutionScore(
                category=category, sample_count=0, p_winner=0.1, avg_real_x=3.0,
                p_20x=0.02, p_50x=0.01, p_100x=0.005, avg_resolution_x=5.0,
                tail_ev=avg_score * 10, score=avg_score * 0.6,
            )
        return score

    def refresh(self) -> None:
        self._cache = self._load_scores()
