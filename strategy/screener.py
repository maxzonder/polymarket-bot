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

import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from api.gamma_client import MarketInfo, fetch_open_markets
from config import BotConfig
from strategy.scorer import EntryFillScorer, ResolutionScorer
from utils.logger import setup_logger
from utils.paths import DATA_DIR

logger = setup_logger("screener")

_SCREENER_LOG_INSERT = """
    INSERT INTO screener_log
        (scanned_at, market_id, token_id, question, category,
         current_price, hours_to_close, volume_usdc, ef_score, res_score, outcome)
    VALUES (?,?,?,?,?, ?,?,?,?,?,?)
"""


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
        db_path: Optional[Path] = None,
    ):
        self.config = config
        self.mc = config.mode_config
        self.ef_scorer = entry_fill_scorer
        self.res_scorer = resolution_scorer
        self._db_path = str(db_path or DATA_DIR / "positions.db")

    def scan(self) -> list[EntryCandidate]:
        """
        Fetch open markets, apply filters, score, return candidates sorted by total_score desc.
        All rejection reasons are recorded in screener_log for funnel analysis.
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
        log_entries: list[tuple] = []

        for market in markets:
            cands = self._evaluate_market(market, log_entries)
            candidates.extend(cands)

        candidates.sort(key=lambda c: c.total_score, reverse=True)

        if log_entries:
            self._flush_screener_log(log_entries)

        return candidates

    def _flush_screener_log(self, entries: list[tuple]) -> None:
        """Batch-write screener_log rows. One connection per scan cycle."""
        try:
            conn = sqlite3.connect(self._db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executemany(_SCREENER_LOG_INSERT, entries)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"screener_log flush failed: {e}")

    def _evaluate_market(
        self,
        m: MarketInfo,
        log_entries: list[tuple],
    ) -> list[EntryCandidate]:
        """
        Evaluate a single market. Returns 0, 1, or 2 candidates (one per token side).
        Appends one row to log_entries per rejection/pass for screener_log.
        """
        now = int(time.time())
        q = (m.question or "")[:120]

        def _log(outcome: str, token_id=None, price=None, ef=None, res=None) -> None:
            log_entries.append((
                now,
                m.market_id,
                token_id,
                q,
                m.category,
                price,
                m.hours_to_close,
                m.volume_usdc,
                ef,
                res,
                outcome,
            ))

        # Hard filter: hours to close
        if m.hours_to_close is not None:
            if m.hours_to_close < self.config.min_hours_to_close:
                _log("rejected_hours_to_close_min")
                return []
            if m.hours_to_close > self.config.max_hours_to_close:
                _log("rejected_hours_to_close_max")
                return []

        # Hard filter: must have token IDs
        if not m.token_ids:
            _log("rejected_missing_token_ids")
            return []

        ef_score_obj  = self.ef_scorer.get(m.category)
        res_score_obj = self.res_scorer.get(m.category)
        ef_s  = ef_score_obj.score
        res_s = res_score_obj.score

        # Hard filter: scoring gates from mode config
        if ef_s < self.mc.min_entry_fill_score:
            _log("rejected_entry_fill_score", ef=ef_s, res=res_s)
            return []
        if res_s < self.mc.min_resolution_score:
            _log("rejected_resolution_score", ef=ef_s, res=res_s)
            return []

        candidates: list[EntryCandidate] = []

        # Each token (YES, NO) is evaluated separately
        for i, token_id in enumerate(m.token_ids):
            outcome_name = m.outcome_names[i] if i < len(m.outcome_names) else f"outcome_{i}"

            # Current price for this specific token from Gamma (rough; OrderManager checks book)
            if i == 0:
                price = m.best_ask if m.best_ask is not None else m.last_trade_price
            else:
                # NO token price ≈ 1 - YES price
                yes_price = m.best_ask if m.best_ask is not None else m.last_trade_price
                price = (1.0 - yes_price) if yes_price is not None else None

            if price is None:
                _log("rejected_price_none", token_id=token_id)
                continue
            if price <= 0:
                _log("rejected_price_le_zero", token_id=token_id, price=price)
                continue
            if price > self.mc.entry_price_max:
                _log("rejected_price_above_entry_max", token_id=token_id, price=price,
                     ef=ef_s, res=res_s)
                continue
            if price >= 0.99:
                _log("rejected_price_ge_0_99", token_id=token_id, price=price)
                continue

            total_score = self._compute_total_score(m, ef_s, res_s)

            # Determine resting bid levels for this token
            entry_levels = [lvl for lvl in self.mc.entry_price_levels if lvl < price]
            if not entry_levels and self.mc.scanner_entry:
                entry_levels = [price]
            if not entry_levels:
                continue

            _log("passed_to_order_manager", token_id=token_id, price=price,
                 ef=ef_s, res=res_s)

            rationale = (
                f"category={m.category} fill_score={ef_s:.3f} res_score={res_s:.3f} "
                f"p_winner={res_score_obj.p_winner:.2f} p_20x={res_score_obj.p_20x:.3f} "
                f"tail_ev={res_score_obj.tail_ev:.1f} "
                f"vol=${m.volume_usdc:.0f} hrs_to_close={m.hours_to_close:.1f}"
            )

            candidates.append(EntryCandidate(
                market_info=m,
                token_id=token_id,
                outcome_name=outcome_name,
                current_price=price,
                entry_fill_score=ef_s,
                resolution_score=res_s,
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
