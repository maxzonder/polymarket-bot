"""
Market Screener — finds entry candidates from open Polymarket markets.

Polling model: REST call to Gamma API every 5 minutes.
No WebSocket — positions live hours, 5-min latency is irrelevant.

Two entry paths:
  1. Scanner entry (fast_tp / balanced): market already in low-price zone
  2. Resting bid pre-positioning (balanced / big_swan): place bids ahead of dip

Output: list[EntryCandidate] for OrderManager to act on.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from api.gamma_client import MarketInfo, fetch_open_markets
from config import BotConfig
from strategy.scorer import EntryFillScorer, ResolutionScorer


@dataclass
class EntryCandidate:
    """A market candidate approved for entry consideration."""
    market_info: MarketInfo
    token_id: str               # specific token to trade (YES or NO side)
    outcome_name: str           # "Yes" / "No"
    current_price: float        # best_ask or last_trade_price
    entry_fill_score: float     # P(market hits our resting bid zone)
    resolution_score: float     # tail EV score
    total_score: float          # weighted composite for ranking
    suggested_entry_levels: list[float]  # price levels for resting bids
    rationale: str = ""         # human-readable explanation


class Screener:
    """
    Polls Gamma API and produces EntryCandidate list.

    Hard filters (reject if any fails):
    - market is active, not archived
    - volume in [min_volume, max_volume]
    - hours_to_close in [min_hours, max_hours]
    - current price <= mode_config.entry_price_max
    - not fees_enabled if category penalised (configurable)

    Soft scoring (produces ranking):
    - entry_fill_score from EntryFillScorer (historical dip frequency)
    - resolution_score from ResolutionScorer (tail EV per category)
    - category_weight from config
    - duration weight (shorter markets score higher for near-term opportunities)
    - liquidity_inefficiency weight (lower volume = higher score)
    """

    def __init__(
        self,
        config: BotConfig,
        entry_fill_scorer: EntryFillScorer,
        resolution_scorer: ResolutionScorer,
    ):
        self.config = config
        self.mc = config.mode_config
        self.ef_scorer = entry_fill_scorer
        self.res_scorer = resolution_scorer

    def scan(self) -> list[EntryCandidate]:
        """
        Fetch open markets, apply filters, score, return candidates sorted by total_score desc.
        """
        try:
            markets = fetch_open_markets(
                price_max=self.mc.entry_price_max,
                volume_min=self.config.min_volume_usdc,
                volume_max=self.config.max_volume_usdc,
            )
        except RuntimeError:
            return []  # network error — skip this cycle

        candidates: list[EntryCandidate] = []

        for market in markets:
            cands = self._evaluate_market(market)
            candidates.extend(cands)

        candidates.sort(key=lambda c: c.total_score, reverse=True)
        return candidates

    def _evaluate_market(self, m: MarketInfo) -> list[EntryCandidate]:
        """
        Evaluate a single market. Returns 0, 1, or 2 candidates (one per token side).
        """
        # Hard filter: hours to close
        if m.hours_to_close is not None:
            if m.hours_to_close < self.config.min_hours_to_close:
                return []
            if m.hours_to_close > self.config.max_hours_to_close:
                return []

        # Hard filter: must have token IDs
        if not m.token_ids:
            return []

        ef = self.ef_scorer.get(m.category)
        res = self.res_scorer.get(m.category)

        # Hard filter: scoring gates from mode config
        if ef.score < self.mc.min_entry_fill_score:
            return []
        if res.score < self.mc.min_resolution_score:
            return []

        candidates: list[EntryCandidate] = []

        # Each token (YES, NO) is evaluated separately
        for i, token_id in enumerate(m.token_ids):
            outcome = m.outcome_names[i] if i < len(m.outcome_names) else f"outcome_{i}"

            # Current price for this specific token from Gamma (rough; OrderManager will check book)
            # best_ask is market-level in Gamma — we use it as proxy for YES token
            # For a real system: fetch per-token book. Here we use market-level price.
            if i == 0:
                price = m.best_ask if m.best_ask is not None else m.last_trade_price
            else:
                # NO token price ≈ 1 - YES price
                yes_price = m.best_ask if m.best_ask is not None else m.last_trade_price
                price = (1.0 - yes_price) if yes_price is not None else None

            if price is None:
                continue
            if price <= 0 or price > self.mc.entry_price_max:
                continue
            if price >= 0.99:
                continue  # already resolved

            total_score = self._compute_total_score(m, ef.score, res.score)

            # Determine resting bid levels for this token
            entry_levels = [lvl for lvl in self.mc.entry_price_levels if lvl < price]
            if not entry_levels and self.mc.scanner_entry:
                # Price already in zone — scanner entry at current price
                entry_levels = [price]
            if not entry_levels:
                continue

            rationale = (
                f"category={m.category} fill_score={ef.score:.3f} res_score={res.score:.3f} "
                f"p_winner={res.p_winner:.2f} p_20x={res.p_20x:.3f} tail_ev={res.tail_ev:.1f} "
                f"vol=${m.volume_usdc:.0f} hrs_to_close={m.hours_to_close:.1f}"
            )

            candidates.append(EntryCandidate(
                market_info=m,
                token_id=token_id,
                outcome_name=outcome,
                current_price=price,
                entry_fill_score=ef.score,
                resolution_score=res.score,
                total_score=total_score,
                suggested_entry_levels=entry_levels,
                rationale=rationale,
            ))

        return candidates

    def _compute_total_score(
        self,
        m: MarketInfo,
        ef_score: float,
        res_score: float,
    ) -> float:
        """
        Weighted composite score for ranking candidates.

        Components (all normalised 0–1):
        - fill_score × weight
        - resolution_score × weight
        - duration_score (prefer shorter-window markets with urgency)
        - liquidity_inefficiency_score (prefer lower volume)
        - category_weight from config
        """
        mode = self.config.mode_config.optimize_metric

        # Duration score: closer to close = more urgent (for fast_tp/balanced)
        # For big_swan: longer duration = more time for event to materialise
        hours = m.hours_to_close or 24.0
        if mode == "tail_ev":
            # big_swan: prefer markets with 12–168h remaining (1 week sweet spot)
            duration_score = 1.0 - abs(hours - 72) / max(hours, 72)
            duration_score = max(0.0, min(1.0, duration_score))
        else:
            # fast_tp / balanced: prefer markets closing within 48h
            duration_score = max(0.0, 1.0 - hours / 48.0)

        # Liquidity inefficiency: lower volume = worse price efficiency = better edge
        # $500–$5000 volume sweet spot, penalise above $50k
        vol = m.volume_usdc
        if vol < 500:
            liq_score = 0.3   # too thin — fill risk
        elif vol <= 5_000:
            liq_score = 1.0   # sweet spot
        elif vol <= 20_000:
            liq_score = 0.7
        else:
            liq_score = max(0.2, 1.0 - vol / 100_000)

        cat_weight = self.config.category_weights.get(m.category or "", 1.0)

        if mode == "tail_ev":
            # big_swan: resolution score dominates
            score = (
                0.40 * res_score
                + 0.25 * ef_score
                + 0.15 * liq_score
                + 0.10 * duration_score
                + 0.10 * min(cat_weight / 1.5, 1.0)
            )
        else:
            # fast_tp / balanced: fill score more important (need the dip to happen)
            score = (
                0.30 * ef_score
                + 0.30 * res_score
                + 0.20 * duration_score
                + 0.10 * liq_score
                + 0.10 * min(cat_weight / 1.5, 1.0)
            )

        return round(score, 4)
