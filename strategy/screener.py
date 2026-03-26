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
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from api.gamma_client import MarketInfo, fetch_open_markets
from api.clob_client import get_orderbook
from config import BotConfig
from strategy.scorer import EntryFillScorer, ResolutionScorer
from strategy.market_scorer import MarketScore, MarketScorer
from utils.logger import setup_logger
from utils.paths import DATA_DIR

logger = setup_logger("screener")

_SCREENER_LOG_INSERT = """
    INSERT INTO screener_log
        (scanned_at, market_id, token_id, question, category,
         current_price, hours_to_close, volume_usdc, ef_score, res_score, outcome, candidate_id,
         market_score)
    VALUES (?,?,?,?,?, ?,?,?,?,?,?,?, ?)
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
    candidate_id: str = ""      # UUID linking screener_log → scan_log → resting_orders
    rationale: str = ""         # human-readable explanation
    market_score: Optional[MarketScore] = None  # v1.1: market-level score breakdown


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
    v1.1: market_scorer (MarketScorer) replaces category-level EntryFillScorer +
          ResolutionScorer for ranking. Legacy scorers still supported as fallback.
    - market_score from MarketScorer (per-market, not per-category)
    - entry_fill_score from EntryFillScorer (backward-compat)
    - resolution_score from ResolutionScorer (backward-compat)
    - category_weight from config
    - duration weight
    - liquidity_inefficiency weight

    v1.1 CLOB pricing:
    - YES token: use Gamma best_ask (unchanged)
    - NO token: query real CLOB best_ask for candidates that pass market_score gate.
      Synthetic (1-YES) used as fast pre-filter; CLOB used for final price.
    """

    def __init__(
        self,
        config: BotConfig,
        entry_fill_scorer: EntryFillScorer,
        resolution_scorer: ResolutionScorer,
        db_path: Optional[Path] = None,
        market_scorer: Optional[MarketScorer] = None,
    ):
        self.config = config
        self.mc = config.mode_config
        self.ef_scorer = entry_fill_scorer
        self.res_scorer = resolution_scorer
        self.market_scorer = market_scorer
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

        def _log(outcome: str, token_id=None, price=None, ef=None, res=None,
                 candidate_id=None, ms=None) -> None:
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
                candidate_id,
                ms,
            ))

        # Hard filter: hours to close.
        # If hours_to_close is None (Gamma didn't return it), apply a safe default
        # rather than silently bypassing the time-gate. This prevents markets with
        # unknown deadlines from flooding the candidate pool.
        mc = self.config.mode_config
        hours = m.hours_to_close
        if hours is None:
            hours = mc.hours_to_close_null_default
            _log("hours_to_close_null_default_applied")
        if hours < mc.min_hours_to_close:
            _log("rejected_hours_to_close_min")
            return []
        if hours > mc.max_hours_to_close:
            _log("rejected_hours_to_close_max")
            return []

        # Hard filter: must have token IDs
        if not m.token_ids:
            _log("rejected_missing_token_ids")
            return []

        # v1.1: market-level score gate — runs BEFORE ef_score.
        # Rationale: ef_score is category-level and can't differentiate within a category.
        # market_scorer uses per-market signals (volume, duration, analogy, neg_risk).
        # Running it first means ef_score never fires on markets already rejected by
        # market_scorer, making the funnel easier to read and market_scorer effective.
        # Issue #47 identified that running market_scorer AFTER ef_score made it a
        # dead gate (ef_score pre-filtered all markets that would fail market_scorer).
        ms_obj: Optional[MarketScore] = None
        if self.market_scorer is not None:
            ms_obj = self.market_scorer.score(m)
            if ms_obj.tier == "reject":
                _log("rejected_market_score", ms=ms_obj.total)
                return []

        ef_score_obj  = self.ef_scorer.get(m.category)
        res_score_obj = self.res_scorer.get(m.category)
        ef_s  = ef_score_obj.score
        res_s = res_score_obj.score

        # Hard filter: scoring gates from mode config
        # v1.1 FIX: If market_scorer is active, bypass category-level ef/res hard gates.
        # market_scorer acts as the unified quality gate. Category-wide gates kill 
        # entire high-opportunity sectors (like crypto) unfairly.
        if self.market_scorer is None:
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

            # Current price for this specific token.
            # YES (i=0): use Gamma best_ask / last_trade_price.
            # NO  (i>0): use synthetic (1-YES) as fast pre-filter.
            #            If candidate passes price gate, refine with real CLOB ask.
            if i == 0:
                price = m.best_ask if m.best_ask is not None else m.last_trade_price
            else:
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

            # v1.1: for NO token that passed the price gate, refine with real CLOB ask.
            # This fixes Issue #44 Problem 2 for the final price used in ordering.
            # We already know from validation (issue #45) that synthetic is accurate
            # to within 0.003 on average, but we use real price for final candidates.
            if i > 0:
                try:
                    ob = get_orderbook(token_id)
                    if ob.best_ask is not None and ob.best_ask > 0:
                        price = ob.best_ask
                except Exception:
                    pass  # fallback to synthetic — acceptable per issue #45 verdict

            total_score = self._compute_total_score(m, ef_s, res_s, ms_obj)

            # Determine resting bid levels for this token
            entry_levels = [lvl for lvl in self.mc.entry_price_levels if lvl < price]
            if not entry_levels and self.mc.scanner_entry:
                entry_levels = [price]
            if not entry_levels:
                _log("rejected_no_entry_levels", token_id=token_id, price=price,
                     ef=ef_s, res=res_s)
                continue

            ms_score = ms_obj.total if ms_obj else None
            candidate_id = str(uuid.uuid4())
            _log("passed_to_order_manager", token_id=token_id, price=price,
                 ef=ef_s, res=res_s, candidate_id=candidate_id, ms=ms_score)

            ms_info = f" {ms_obj.rationale}" if ms_obj else ""
            rationale = (
                f"category={m.category} fill_score={ef_s:.3f} res_score={res_s:.3f} "
                f"p_winner={res_score_obj.p_winner:.2f} p_20x={res_score_obj.p_20x:.3f} "
                f"tail_ev={res_score_obj.tail_ev:.1f} "
                f"vol=${m.volume_usdc:.0f} hrs_to_close={f'{m.hours_to_close:.1f}' if m.hours_to_close is not None else 'N/A'}"
                f"{ms_info}"
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
                candidate_id=candidate_id,
                rationale=rationale,
                market_score=ms_obj,
            ))

        return candidates

    def _compute_total_score(
        self,
        m: MarketInfo,
        ef_score: float,
        res_score: float,
        market_score: Optional[MarketScore] = None,
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
            # big_swan: issue #53 shows short is fine. Flat 1.0 up to 168h, then decay.
            duration_score = 1.0 if hours <= 168.0 else max(0.5, 168.0 / hours)
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
            if market_score is not None:
                # v1.1: market_score replaces category-level signals for big_swan_mode
                score = (
                    0.60 * market_score.total
                    + 0.20 * liq_score
                    + 0.10 * duration_score
                    + 0.10 * min(cat_weight / 1.5, 1.0)
                )
            else:
                # legacy: resolution score dominates
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
