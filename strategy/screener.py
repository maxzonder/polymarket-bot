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
from api.data_api import get_last_trade_ts, get_recent_trades
from config import BotConfig
from strategy.scorer import EntryFillScorer, ResolutionScorer
from strategy.market_scorer import MarketScore, MarketScorer
from strategy.entry_levels import suggested_entry_levels
from utils.logger import setup_logger
from utils.paths import DATA_DIR
from utils.telegram import send_message as tg_alert

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
        skip_logging: bool = False,
    ):
        self.config = config
        self.mc = config.mode_config
        self.ef_scorer = entry_fill_scorer
        self.res_scorer = resolution_scorer
        self.market_scorer = market_scorer
        self._db_path = str(db_path or DATA_DIR / "positions.db")
        self._skip_logging = skip_logging

    def scan(self, allowed_market_ids: Optional[set[str]] = None) -> list[EntryCandidate]:
        """
        Fetch open markets, apply filters, score, return candidates sorted by total_score desc.
        All rejection reasons are recorded in screener_log for funnel analysis.
        """
        # Guard: market_score is the primary ranking signal. If scorer has no data,
        # all scores will be 0 and the bot would trade blind. Skip the cycle instead.
        if self.market_scorer is not None and not self.market_scorer.is_ready:
            logger.error("MarketScorer not ready (feature_mart_v1_1 empty or unavailable) — skipping scan cycle")
            tg_alert("⚠️ <b>MarketScorer not ready</b>\nfeature_mart_v1_1 is empty or failed to load. Screener skipping this cycle — no new positions will be opened.")
            return []

        try:
            markets = fetch_open_markets(
                price_max=self.mc.entry_price_max,
                volume_min=self.config.min_volume_usdc,
                volume_max=self.config.max_volume_usdc,
            )
        except RuntimeError:
            return []  # network error — skip this cycle

        if allowed_market_ids is not None:
            markets = [m for m in markets if m.market_id in allowed_market_ids]

        candidates: list[EntryCandidate] = []
        log_entries: list[tuple] = []

        # Split into binary markets and neg-risk groups for group-aware evaluation.
        neg_risk_groups: dict[str, list[MarketInfo]] = {}
        binary_markets: list[MarketInfo] = []
        for m in markets:
            if m.neg_risk_group_id:
                neg_risk_groups.setdefault(m.neg_risk_group_id, []).append(m)
            else:
                binary_markets.append(m)

        logger.info(
            f"Scan start: {len(markets)} markets fetched — "
            f"{len(binary_markets)} binary, {len(neg_risk_groups)} neg-risk groups "
            f"({sum(len(v) for v in neg_risk_groups.values())} outcomes)"
        )

        for m in binary_markets:
            candidates.extend(self._evaluate_market(m, log_entries))

        for group_id, group_markets in neg_risk_groups.items():
            candidates.extend(self._evaluate_neg_risk_group(group_id, group_markets, log_entries))

        candidates.sort(key=lambda c: c.total_score, reverse=True)

        passed = [c for c in candidates if c.total_score > 0]
        logger.info(
            f"Scan done: {len(passed)} candidates → order manager "
            f"(binary={sum(1 for c in passed if not c.market_info.neg_risk_group_id)}, "
            f"neg-risk={sum(1 for c in passed if c.market_info.neg_risk_group_id)})"
        )

        if log_entries:
            self._flush_screener_log(log_entries)

        return candidates

    def _flush_screener_log(self, entries: list[tuple]) -> None:
        """Batch-write screener_log rows. One connection per scan cycle."""
        if self._skip_logging:
            return
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
        group_boost: float = 1.0,
    ) -> list[EntryCandidate]:
        """
        Evaluate a single market. Returns 0, 1, or 2 candidates (one per token side).
        Appends one row to log_entries per rejection/pass for screener_log.
        """
        now = int(time.time())
        q = (m.question or "")[:120]

        # Resolve hours_to_close once: gate, scoring, and logging all use the same value.
        mc = self.config.mode_config
        hours: float = m.hours_to_close if m.hours_to_close is not None else mc.hours_to_close_null_default

        def _log(outcome: str, token_id=None, price=None, ef=None, res=None,
                 candidate_id=None, ms=None) -> None:
            log_entries.append((
                now,
                m.market_id,
                token_id,
                q,
                m.category,
                price,
                hours,
                m.volume_usdc,
                ef,
                res,
                outcome,
                candidate_id,
                ms,
            ))

        if m.hours_to_close is None:
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
            ms_obj = self.market_scorer.score(m, hours_to_close=hours)
            if ms_obj.tier == "reject":
                _log("rejected_market_score", ms=ms_obj.total)
                return []

        # Dead market filter: reject if no trades in the last dead_market_hours.
        # Checked after market_score gate to avoid hitting Data API for bad markets.
        last_trade = get_last_trade_ts(m.market_id)
        if last_trade is not None:
            hours_since_trade = (time.time() - last_trade) / 3600.0
            if hours_since_trade > self.config.dead_market_hours:
                _log("rejected_dead_market", ms=ms_obj.total if ms_obj else None)
                return []

        ef_score_obj  = self.ef_scorer.get(m.category)
        res_score_obj = self.res_scorer.get(m.category)
        ef_s  = ef_score_obj.score
        res_s = res_score_obj.score

        # ef_score / res_score gates removed: all current modes use scoring_weights
        # with market_score as primary signal. market_scorer.score() already acts
        # as the unified gate above. Category-level ef/res gates are legacy dead code.

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
                # Re-check price gate after CLOB refinement: real NO ask may exceed entry_price_max
                if price > self.mc.entry_price_max:
                    _log("rejected_no_reprice_above_max", token_id=token_id, price=price,
                         ef=ef_s, res=res_s)
                    continue

            total_score = self._compute_total_score(m, ef_s, res_s, ms_obj, hours=hours) * group_boost

            # Entry tiers are MAX acceptable prices, not strictly-below-current bids.
            # If the market is already cheaper than a tier, that tier remains valid and
            # should be executed at the better available price rather than rejected.
            entry_levels = suggested_entry_levels(
                current_price=price,
                configured_levels=self.mc.entry_price_levels,
                scanner_entry=self.mc.scanner_entry,
            )
            if not entry_levels:
                _log("rejected_no_entry_levels", token_id=token_id, price=price,
                     ef=ef_s, res=res_s)
                continue

            ms_score = ms_obj.total if ms_obj else None
            candidate_id = str(uuid.uuid4())
            _log("passed_to_order_manager", token_id=token_id, price=price,
                 ef=ef_s, res=res_s, candidate_id=candidate_id, ms=ms_score)

            boost_info = f" boost={group_boost:.2f}" if group_boost != 1.0 else ""
            neg_risk_info = f" [neg-risk grp={m.neg_risk_group_id[:12]}]" if m.neg_risk_group_id else ""
            ms_str = f"{ms_score:.3f}" if ms_score is not None else "N/A"
            logger.info(
                f"Candidate: {(m.question or '')[:60]!r} | "
                f"{outcome_name} @ ${price:.4f} | "
                f"score={total_score:.3f}{boost_info} ms={ms_str} | "
                f"cat={m.category} vol=${m.volume_usdc:.0f} hrs={hours:.1f}"
                f"{neg_risk_info}"
            )

            ms_info = f" {ms_obj.rationale}" if ms_obj else ""
            rationale = (
                f"category={m.category} fill_score={ef_s:.3f} res_score={res_s:.3f} "
                f"p_winner={res_score_obj.p_winner:.2f} p_20x={res_score_obj.p_20x:.3f} "
                f"tail_ev={res_score_obj.tail_ev:.1f} "
                f"vol=${m.volume_usdc:.0f} hrs_to_close={hours:.1f}"
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

    def _evaluate_neg_risk_group(
        self,
        group_id: str,
        markets: list[MarketInfo],
        log_entries: list[tuple],
    ) -> list[EntryCandidate]:
        """
        Group-aware evaluation of a neg-risk market group.

        Determines the favorite (highest YES price), computes a group_boost based
        on market confidence level, then evaluates only the underdog tokens
        (price <= entry_price_max) with that boost applied to total_score.

        Also checks if the favorite dropped 20%+ in 24h — structural catalyst
        that further boosts underdogs.
        """
        # Sort descending by YES price → favorite first
        sorted_markets = sorted(
            markets,
            key=lambda m: m.best_ask or m.last_trade_price or 0,
            reverse=True,
        )

        if not sorted_markets:
            return []

        fav_market = sorted_markets[0]
        favorite_price = fav_market.best_ask or fav_market.last_trade_price or 0.0

        # Base boost: high confidence in favorite → underdogs are structurally cheap
        if favorite_price >= 0.80:
            group_boost = 1.5
        elif favorite_price >= 0.60:
            group_boost = 1.2
        elif favorite_price >= 0.40:
            group_boost = 1.0
        else:
            group_boost = 0.7   # no clear favorite — underdogs less interesting

        # Step 5: Check if favorite dropped 20%+ in the last 24h (structural catalyst)
        try:
            recent = get_recent_trades(fav_market.market_id, limit=50)
            if recent:
                cutoff = time.time() - 86400
                price_24h_ago = None
                for t in recent:
                    if float(t.get("timestamp", 0)) <= cutoff:
                        price_24h_ago = float(t["price"])
                        break
                if price_24h_ago and favorite_price < price_24h_ago * 0.80:
                    group_boost *= 1.5
                    logger.info(
                        f"Neg-risk group {group_id}: favorite dropped "
                        f"${price_24h_ago:.2f} → ${favorite_price:.2f} — boosting underdogs"
                    )
        except Exception:
            pass  # trades unavailable — continue without catalyst boost

        # Only evaluate underdog tokens (price in our entry zone)
        underdog_markets = [
            m for m in sorted_markets
            if (m.best_ask or m.last_trade_price or 1.0) <= self.mc.entry_price_max
        ]

        # Cap per group: take top N by volume (most liquid underdogs first)
        max_underdogs = self.mc.max_resting_per_cluster if self.mc.max_resting_per_cluster > 0 else 10
        underdog_markets = sorted(underdog_markets, key=lambda m: m.volume_usdc, reverse=True)
        underdog_markets = underdog_markets[:max_underdogs]

        fav_q = (fav_market.question or "")[:50]
        logger.info(
            f"Neg-risk group {group_id[:16]}: {len(sorted_markets)} outcomes | "
            f"fav={fav_q!r} @ ${favorite_price:.3f} | "
            f"boost={group_boost:.2f} | "
            f"{len(underdog_markets)}/{len(sorted_markets)} underdogs to evaluate"
        )

        candidates = []
        for m in underdog_markets:
            cands = self._evaluate_market(m, log_entries, group_boost=group_boost)
            candidates.extend(cands)

        return candidates

    def _compute_total_score(
        self,
        m: MarketInfo,
        ef_score: float,
        res_score: float,
        market_score: Optional[MarketScore] = None,
        hours: Optional[float] = None,
    ) -> float:
        """
        Weighted composite score for ranking candidates.

        Weights come from mc.scoring_weights (config-driven per mode).
        Components (all normalised 0–1): market_score, liq, duration, category.
        Legacy ef_score/res_score path kept as fallback for modes without scoring_weights.
        hours: resolved hours_to_close (null-default already applied by caller).
        """
        if hours is None:
            hours = m.hours_to_close if m.hours_to_close is not None else 24.0

        # Duration score — direction controlled by mc.prefer_long_duration
        if self.mc.prefer_long_duration:
            # Flat 1.0 until max_hours_to_close, then decay (big_swan: events need time)
            horizon = self.mc.max_hours_to_close
            duration_score = 1.0 if hours <= horizon else max(0.5, horizon / hours)
        else:
            # Linear decay: 1.0 at 0h, 0.0 at max_hours_to_close (prefer closing soon)
            duration_score = max(0.0, 1.0 - hours / self.mc.max_hours_to_close)

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

        cat_norm = min(self.config.category_weights.get(m.category or "", 1.0) / 1.5, 1.0)

        if self.mc.scoring_weights:
            ms_val = market_score.total if market_score is not None else 0.0
            components = {
                "market_score": ms_val,
                "liq":          liq_score,
                "duration":     duration_score,
                "category":     cat_norm,
            }
            score = sum(w * components.get(name, 0.0) for name, w in self.mc.scoring_weights)
        else:
            # Legacy fallback for modes without scoring_weights
            if market_score is not None:
                score = (
                    0.60 * market_score.total
                    + 0.20 * liq_score
                    + 0.10 * duration_score
                    + 0.10 * cat_norm
                )
            else:
                score = (
                    0.40 * res_score
                    + 0.25 * ef_score
                    + 0.15 * liq_score
                    + 0.10 * duration_score
                    + 0.10 * cat_norm
                )

        return round(score, 4)
