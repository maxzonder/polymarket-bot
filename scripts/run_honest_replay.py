"""
Full-universe honest replay — no look-ahead bias.

Unlike run_dry_run_replay.py (which replays only known black swans from token_swans),
this script simulates the bot running from day 1 over ALL downloaded markets:

  1. Load every market in the date range from polymarket_dataset.db.
  2. Apply screener static filters (volume, category, duration).
  3. At the "discovery time" for each market (= simulation start or first trade,
     whichever is later), check price-dependent filters (current_price, hours_to_close).
  4. Score the market with MarketScorer (offline, reads feature_mart_v1_1 from SQLite).
  5. Place resting bids at configured levels for markets that pass.
  6. Replay trades chronologically: fill bids, fire TP orders, resolve.

Most resting bids will never fill — the bot scatters bids across hundreds of markets
and only earns on the fraction that actually dip. That's the honest cost of the strategy.

DOCUMENTED LIMITATIONS (read before interpreting results):
  1. current_price = last_trade_price, not CLOB best_ask.
     → Fills may be slightly optimistic; real ask is usually a few ticks higher.
  2. volume uses final total from DB, not volume-at-discovery.
     → Young markets look as liquid as they'll ever be; filter is slightly generous.
  3. NO-token price: 1 - YES synthetic (no CLOB orderbook for NO side).
  4. Spread gate not implemented (no historical orderbook snapshots).
  5. Universe: only markets downloaded in polymarket_dataset.db (~235k of ~300k on Polymarket).
  6. Trade files are full-history snapshots (not daily diffs). We use the latest
     available file per market, which may have been collected after the sim window ends.
     This is fine for fill/TP logic — timestamps are in the trade records.
  7. Spread gate not implemented (no historical orderbook snapshots).
  8. Neg-risk group simulation uses end-of-day volume as underdog sort key
     (volume-at-discovery not available in DB).
  9. Trade tape uses filterAmount=10 (≥$10 USDC trades only). Our resting bids at
     0.001–0.005 price levels are $0.01–$5 USDC — these never appear in the tape.
     → Fill rate at levels 0.001–0.005 is a LOWER BOUND of real performance,
       not an accurate estimate. Actual fills on those levels may be significantly
       higher because micro-trades from other participants at those prices are invisible.

Usage:
    python3 scripts/run_honest_replay.py --start 2026-01-01 --end 2026-01-31
    python3 scripts/run_honest_replay.py --start 2026-01-01 --end 2026-01-31 --mode balanced_mode
    python3 scripts/run_honest_replay.py --start 2026-02-01 --end 2026-02-28 --pessimistic
    python3 scripts/run_honest_replay.py --start 2026-01-01 --end 2026-01-31 --summary
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import execution.order_manager as om_module

from api.clob_client import ClobClient, Orderbook, OrderbookLevel
from api.gamma_client import MarketInfo
from config import BotConfig, ModeConfig
from strategy.market_scorer import MarketScore, MarketScorer
from strategy.risk_manager import RiskManager
from strategy.screener import EntryCandidate
from utils.logger import setup_logger
from utils.paths import DATABASE_DIR, DB_PATH, DATA_DIR

logger = setup_logger("honest_replay")


# ── Trade file index ───────────────────────────────────────────────────────────

def _build_trade_file_index() -> dict[tuple[str, str], str]:
    """
    Build a {(market_id, token_id): path} index once at startup.
    Avoids per-token filesystem scans (O(dates) → O(1) lookup).
    Latest date wins for each key (snapshot is fullest).
    """
    index: dict[tuple[str, str], str] = {}
    try:
        dates = sorted(
            [d for d in os.listdir(DATABASE_DIR) if d[:4].isdigit()],
            reverse=True,
        )
        for d in dates:
            trades_root = os.path.join(DATABASE_DIR, d)
            if not os.path.isdir(trades_root):
                continue
            for entry in os.scandir(trades_root):
                if not (entry.is_dir() and entry.name.endswith("_trades")):
                    continue
                market_id = entry.name[:-7]
                for tf in os.scandir(entry.path):
                    if tf.name.endswith(".json") and not tf.name.startswith("."):
                        key = (market_id, tf.name[:-5])
                        if key not in index:
                            index[key] = tf.path
    except Exception as e:
        logger.warning(f"_build_trade_file_index error: {e}")
    return index


# ── Trade file helpers ─────────────────────────────────────────────────────────

def _load_trades_sorted(fpath: str) -> list[dict]:
    """Load and sort trades ascending by timestamp. Returns [] on error."""
    try:
        with open(fpath, encoding="utf-8") as f:
            trades = json.load(f)
        trades.sort(key=lambda t: t["timestamp"])
        return trades
    except Exception as e:
        logger.debug(f"Trade file read error {fpath}: {e}")
        return []


def _price_at(trades: list[dict], ts: int) -> Optional[float]:
    """Return the last trade price at or before timestamp ts. None if no trades yet."""
    p = None
    for t in trades:
        if t["timestamp"] <= ts:
            p = float(t["price"])
        else:
            break
    return p


# ── Neg-risk group boost pre-computation ──────────────────────────────────────

def _compute_neg_risk_boosts(
    rows: list[dict],
    trade_index: dict[tuple[str, str], str],
    start_ts: int,
    mc: ModeConfig,
) -> dict[str, Optional[float]]:
    """
    Pre-compute group_boost for every token in a neg-risk group.

    Returns {token_id: boost} where:
    - boost=float  → underdog, process with this stake multiplier
    - boost=None   → skip (favorite or over max_resting_per_cluster cap)
    Tokens not in this dict are binary markets — process normally (boost=1.0).

    Logic mirrors screener._evaluate_neg_risk_group:
    - Sort group tokens by price at group_discovery_ts (favorite = highest)
    - group_boost based on favorite_price: ≥0.80→1.5, ≥0.60→1.2, ≥0.40→1.0, else→0.7
    - Favorite-drop catalyst: if favorite dropped 20%+ in 24h → extra ×1.5
    - Only underdogs (price ≤ entry_price_max) pass; capped at max_resting_per_cluster
    """
    # Group rows by neg_risk_market_id
    groups: dict[str, list[dict]] = {}
    for row in rows:
        grp = row.get("neg_risk_market_id")
        if grp:
            groups.setdefault(grp, []).append(row)

    if not groups:
        return {}

    result: dict[str, Optional[float]] = {}

    for group_id, group_rows in groups.items():
        # Load trades for each token in the group
        token_trades: dict[str, list[dict]] = {}
        for row in group_rows:
            fpath = trade_index.get((row["market_id"], row["token_id"]))
            if fpath:
                trades = _load_trades_sorted(fpath)
                if trades:
                    token_trades[row["token_id"]] = trades

        if not token_trades:
            for row in group_rows:
                result[row["token_id"]] = None
            continue

        # Group discovery = first time screener could see any token
        first_trade_tss = [t[0]["timestamp"] for t in token_trades.values()]
        group_discovery_ts = max(start_ts, min(first_trade_tss))

        # Price per token at group_discovery_ts
        prices: dict[str, float] = {}
        for tid, trades in token_trades.items():
            p = _price_at(trades, group_discovery_ts)
            if p is not None:
                prices[tid] = p

        if not prices:
            for row in group_rows:
                result[row["token_id"]] = None
            continue

        # Favorite = highest price token at discovery
        fav_token_id = max(prices, key=lambda t: prices[t])
        favorite_price = prices[fav_token_id]

        # Base boost by favorite confidence
        if favorite_price >= 0.80:
            group_boost = 1.5
        elif favorite_price >= 0.60:
            group_boost = 1.2
        elif favorite_price >= 0.40:
            group_boost = 1.0
        else:
            group_boost = 0.7  # no clear favorite — underdogs less interesting

        # Favorite-drop catalyst: dropped 20%+ in 24h before discovery?
        fav_trades = token_trades.get(fav_token_id, [])
        price_24h_ago = _price_at(fav_trades, group_discovery_ts - 86400)
        if price_24h_ago and favorite_price < price_24h_ago * 0.80:
            group_boost *= 1.5
            logger.info(
                f"Neg-risk group {group_id[:16]}: favorite dropped "
                f"${price_24h_ago:.3f} → ${favorite_price:.3f} — underdogs ×1.5"
            )

        # Underdogs: price at discovery ≤ entry_price_max
        underdog_rows = [
            r for r in group_rows
            if prices.get(r["token_id"], 1.0) <= mc.entry_price_max
        ]

        # Cap: top N by volume (most liquid underdogs first)
        max_underdogs = mc.max_resting_per_cluster if mc.max_resting_per_cluster > 0 else 10
        underdog_rows = sorted(
            underdog_rows,
            key=lambda r: float(r.get("volume") or 0),
            reverse=True,
        )[:max_underdogs]
        allowed_tids = {r["token_id"] for r in underdog_rows}

        logger.debug(
            f"Neg-risk group {group_id[:16]}: {len(group_rows)} tokens | "
            f"fav @ ${favorite_price:.3f} boost={group_boost:.2f} | "
            f"{len(underdog_rows)}/{len(group_rows)} underdogs accepted"
        )

        for row in group_rows:
            tid = row["token_id"]
            result[tid] = group_boost if tid in allowed_tids else None

    return result


# ── Market loading ─────────────────────────────────────────────────────────────

def load_all_markets(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
    config: BotConfig,
) -> list[dict]:
    """
    Load all markets closing within the simulation window, with their tokens.
    Applies static screener filters:
      - volume between config min/max
      - end_date within [start_ts, end_ts + 30d buffer for long markets]
      - excludes markets that closed before start_ts (no point screening)
    """
    mc = config.mode_config
    # Buffer: also include markets closing up to max_hours_to_close after start
    close_buffer = int(mc.max_hours_to_close * 3600)

    rows = conn.execute(
        """
        SELECT
            m.id           AS market_id,
            m.question,
            m.category,
            m.volume,
            m.end_date,
            m.start_date,
            m.duration_hours,
            m.neg_risk,
            m.neg_risk_market_id,
            COALESCE(m.comment_count, 0) AS comment_count,
            t.token_id,
            t.outcome_name,
            t.is_winner
        FROM markets m
        JOIN tokens t ON m.id = t.market_id
        WHERE m.end_date  >= :start
          AND m.end_date  <= :end_buf
          AND m.volume    >= :vol_min
          AND m.volume    <= :vol_max
        ORDER BY m.end_date ASC
        """,
        {
            "start":   start_ts,
            "end_buf": end_ts + close_buffer,
            "vol_min": config.min_volume_usdc,
            "vol_max": config.max_volume_usdc,
        },
    ).fetchall()

    return [dict(r) for r in rows]


# ── Replay adapters: reuse OrderManager entry path ───────────────────────────

def _synthetic_replay_orderbook(token_id: str, current_price: float) -> Orderbook:
    """
    Synthetic orderbook for replay-time entry placement.

    Historical orderbook snapshots do not exist in our dataset, so honest replay
    must synthesize the minimum market state needed to run the shared
    OrderManager path without live API calls.

    We intentionally keep:
    - best_ask ~= discovery price proxy
    - tight enough spread to avoid triggering live spread gate
    - non-zero top bid depth so depth gate does not block offline replay
    """
    best_ask = float(current_price)
    best_bid = max(0.0001, round(best_ask * 0.95, 6))
    return Orderbook(
        token_id=token_id,
        bids=[OrderbookLevel(price=best_bid, size=50.0)],
        asks=[OrderbookLevel(price=best_ask, size=50.0)],
        best_bid=best_bid,
        best_ask=best_ask,
    )


def _build_replay_candidate(
    row: dict,
    current_price: float,
    hours_at_discovery: float,
    entry_levels: list[float],
    market_score: Optional[MarketScore],
    group_boost: float,
) -> EntryCandidate:
    token_id = row["token_id"]
    market_id = row["market_id"]
    outcome = row.get("outcome_name") or ""
    category = row.get("category") or ""
    question = row.get("question") or ""
    volume = float(row.get("volume") or 0.0)
    comment_count = int(row.get("comment_count") or 0)
    neg_risk = bool(row.get("neg_risk", False))
    neg_risk_group_id = row.get("neg_risk_market_id")
    if neg_risk_group_id is not None:
        neg_risk_group_id = str(neg_risk_group_id)

    market_info = MarketInfo(
        market_id=str(market_id),
        condition_id=str(market_id),
        question=question,
        category=category,
        token_ids=[str(token_id)],
        outcome_names=[str(outcome or "Yes")],
        best_ask=current_price,
        best_bid=max(0.0001, round(current_price * 0.95, 6)),
        last_trade_price=current_price,
        volume_usdc=volume,
        liquidity_usdc=0.0,
        comment_count=comment_count,
        fees_enabled=False,
        end_date_ts=int(row.get("end_date") or 0) or None,
        hours_to_close=hours_at_discovery,
        neg_risk=neg_risk,
        neg_risk_group_id=neg_risk_group_id,
    )

    base_score = market_score.total if market_score is not None else 0.0
    total_score = base_score * group_boost
    rationale = (
        "offline replay candidate using shared OrderManager path; "
        f"group_boost={group_boost:.2f} market_score={base_score:.3f}"
    )

    return EntryCandidate(
        market_info=market_info,
        token_id=str(token_id),
        outcome_name=str(outcome),
        current_price=current_price,
        entry_fill_score=0.0,
        resolution_score=0.0,
        total_score=total_score,
        suggested_entry_levels=list(entry_levels),
        candidate_id=str(uuid.uuid4()),
        rationale=rationale,
        market_score=market_score,
    )


# ── Per-token simulation ───────────────────────────────────────────────────────

def simulate_token(
    row: dict,
    om,
    clob: ClobClient,
    mc: ModeConfig,
    risk: RiskManager,
    scorer: Optional[MarketScorer],
    positions_db_path: str,
    start_ts: int,
    end_ts: int,
    dead_market_hours: float,
    trade_index: dict[tuple[str, str], str],
    activation_delay: int = 0,
    fill_fraction: float = 1.0,
    group_boost: float = 1.0,
) -> dict:
    """
    Simulate one market/token pair through the bot's full entry → TP → resolution path.

    Key honest-replay properties:
    - Discovery time = max(start_ts, first trade timestamp, market start_date)
    - Current price determined from trades at discovery time (no future knowledge)
    - Bid levels placed only where current_price > bid_level (pre-position below market)
    - Fills checked only for trades after discovery time
    - Most tokens produce status='no_fill' — that is correct and expected
    """
    token_id    = row["token_id"]
    market_id   = row["market_id"]
    end_date    = int(row["end_date"])
    outcome     = row.get("outcome_name") or ""
    is_winner   = bool(row.get("is_winner", False))
    category    = row.get("category") or ""
    volume      = float(row.get("volume") or 0.0)
    comment_count = int(row.get("comment_count") or 0)
    neg_risk    = bool(row.get("neg_risk", False))

    # ── Load trades (index lookup — no filesystem scan) ────────────────────────
    fpath = trade_index.get((market_id, token_id))
    if fpath is None:
        return {"status": "no_trade_file", "token_id": token_id, "market_id": market_id}

    trades = _load_trades_sorted(fpath)
    if not trades:
        return {"status": "no_trades", "token_id": token_id, "market_id": market_id}

    # ── Discovery time: when screener would first see this token ───────────────
    # max(sim_start, first_trade, market_start_date) — all three gates required
    first_trade_ts = trades[0]["timestamp"]
    market_start_ts = int(row.get("start_date") or 0)
    discovery_ts = max(start_ts, first_trade_ts, market_start_ts)

    # Skip if market already closed before simulation window
    if end_date <= start_ts:
        return {"status": "rejected_closed_before_start", "token_id": token_id, "market_id": market_id}

    # ── Dead market filter: was the market dormant at discovery? ──────────────
    trades_before = [t for t in trades if t["timestamp"] <= discovery_ts]
    if trades_before:
        last_trade_ts = int(trades_before[-1]["timestamp"])
        hours_since_last = (discovery_ts - last_trade_ts) / 3600.0
        if hours_since_last > dead_market_hours:
            return {"status": "rejected_dead_market", "token_id": token_id, "market_id": market_id}

    # ── Hours-to-close check at DISCOVERY time (not at start_ts) ─────────────
    # Critical: a market appearing on Jan 15 with end_date Feb 14 has 30 days
    # at discovery — valid for max_hours_to_close=720h. Using start_ts (Dec 1)
    # would compute 75 days and wrongly reject it.
    hours_at_discovery = (end_date - discovery_ts) / 3600.0
    if hours_at_discovery < mc.min_hours_to_close:
        return {"status": "rejected_hours_min", "token_id": token_id, "market_id": market_id}
    if hours_at_discovery > mc.max_hours_to_close:
        return {"status": "rejected_hours_max", "token_id": token_id, "market_id": market_id}

    # ── Current price at discovery time ───────────────────────────────────────
    current_price = _price_at(trades, discovery_ts)
    if current_price is None:
        return {"status": "rejected_no_price_at_discovery", "token_id": token_id, "market_id": market_id}

    # ── Price screener gates ───────────────────────────────────────────────────
    if current_price <= 0:
        return {"status": "rejected_price_le_zero", "token_id": token_id, "market_id": market_id}
    if current_price > mc.entry_price_max:
        return {"status": "rejected_price_above_max", "token_id": token_id, "market_id": market_id}
    if current_price >= 0.99:
        return {"status": "rejected_price_ge_099", "token_id": token_id, "market_id": market_id}

    # ── Determine entry levels: only levels BELOW current price ───────────────
    # This is honest: we pre-position below market, not at current price.
    # (scanner_entry = True would also enter at current price — we respect that here)
    entry_levels = [lvl for lvl in mc.entry_price_levels if lvl < current_price]
    if not entry_levels and mc.scanner_entry:
        entry_levels = [current_price]
    if not entry_levels:
        return {"status": "rejected_no_entry_levels", "token_id": token_id, "market_id": market_id,
                "current_price": current_price}

    # ── MarketScorer gate (offline, reads feature_mart_v1_1 from SQLite) ──────
    market_score: Optional[MarketScore] = None
    if scorer is not None:
        market_score = scorer.score_from_db(
            market_id=market_id,
            volume=volume,
            comment_count=comment_count,
            hours_to_close=hours_at_discovery,
            category=category,
            neg_risk=neg_risk,
        )
        if market_score.tier == "reject":
            return {"status": "rejected_market_score", "token_id": token_id, "market_id": market_id,
                    "market_score": market_score.total}

    candidate = _build_replay_candidate(
        row=row,
        current_price=current_price,
        hours_at_discovery=hours_at_discovery,
        entry_levels=entry_levels,
        market_score=market_score,
        group_boost=group_boost,
    )

    synthetic_book = _synthetic_replay_orderbook(token_id, current_price)
    with patch("execution.order_manager.get_orderbook", return_value=synthetic_book):
        if mc.scanner_entry and current_price <= mc.entry_price_max:
            placement_results = om.process_scanner_entry(candidate)
            used_scanner_entry = True
        else:
            placement_results = om.process_candidate(candidate)
            used_scanner_entry = False

    pending_buys: dict[str, dict] = {
        result.order_id: {
            "price": float(result.price),
            "remaining_qty": float(result.size),
            "filled_qty": 0.0,
        }
        for result in placement_results
        if result.status in ("live", "matched")
    }

    if not pending_buys:
        return {"status": "no_orders_placed", "token_id": token_id, "market_id": market_id}

    filled_entries = 0
    filled_tps = 0

    def _dispatch_fill(order_id: str, order: dict, fill_qty: float) -> None:
        """Dispatch a single incremental fill immediately so TP ladder is live."""
        nonlocal filled_entries
        if fill_qty <= 1e-9:
            return
        clob.paper_simulate_fill(order_id, order["price"], max(order["filled_qty"], fill_qty))
        try:
            om.on_entry_filled(
                order_id=order_id,
                token_id=token_id,
                market_id=market_id,
                fill_price=order["price"],
                fill_quantity=fill_qty,
                outcome_name=outcome,
            )
        except Exception as e:
            logger.warning(f"on_entry_filled error {token_id[:16]}: {e}")
        filled_entries += 1

    # Scanner-entry orders are marketable at discovery by construction
    # (synthetic best_ask == current_price), so replay them as immediate fills.
    if used_scanner_entry:
        for order_id, order in list(pending_buys.items()):
            _dispatch_fill(order_id, order, order["remaining_qty"])
            del pending_buys[order_id]

    # ── Replay trades after discovery time ────────────────────────────────────
    fill_gate = discovery_ts + activation_delay
    sim_end = min(end_ts, end_date)  # stop at either sim end or market close

    for trade in trades:
        trade_ts = int(trade["timestamp"])
        if trade_ts < discovery_ts:
            continue  # before we discovered the market
        if trade_ts > sim_end:
            break     # simulation window ended

        trade_price  = float(trade["price"])
        trade_budget = float(trade["size"])
        trade_side   = trade.get("side", "")

        # ── BUY bid fills (SELL-side taker hits our resting bids) ─────────────
        if trade_side in ("SELL", "") and trade_ts >= fill_gate and pending_buys:
            effective_budget = trade_budget * fill_fraction
            for order_id, order in sorted(
                pending_buys.items(), key=lambda kv: kv[1]["price"], reverse=True
            ):
                if effective_budget <= 0:
                    break
                if trade_price <= order["price"]:
                    fill_qty = min(order["remaining_qty"], effective_budget)
                    effective_budget -= fill_qty
                    trade_budget     -= fill_qty
                    order["remaining_qty"] -= fill_qty
                    order["filled_qty"]    += fill_qty
                    _dispatch_fill(order_id, order, fill_qty)
                    if order["remaining_qty"] <= 1e-9:
                        del pending_buys[order_id]

        # ── TP fills (BUY-side taker hits our SELL TP orders) ─────────────────
        if trade_side == "BUY":
            open_sells = [
                o for o in clob.get_open_orders(token_id)
                if o.get("side") == "SELL"
            ]
            for tp_order in open_sells:
                if trade_budget <= 0:
                    break
                if trade_price >= float(tp_order["price"]):
                    remaining_tp = float(tp_order["size"]) - float(tp_order.get("filled_size") or 0.0)
                    fill_qty = min(remaining_tp, trade_budget)
                    if fill_qty <= 1e-9:
                        continue
                    trade_budget -= fill_qty
                    cumulative_filled = float(tp_order.get("filled_size") or 0.0) + fill_qty
                    clob.paper_simulate_fill(tp_order["order_id"], float(tp_order["price"]), cumulative_filled)
                    try:
                        om.on_tp_filled(tp_order["order_id"], float(tp_order["price"]), fill_qty)
                    except Exception as e:
                        logger.warning(f"on_tp_filled error {token_id[:16]}: {e}")
                    filled_tps += 1

    # ── Cancel unfilled resting orders at end of trade stream ────────────────
    # Partial fills were already dispatched incrementally; only cancel orders
    # that never got any fill at all.
    conn = sqlite3.connect(positions_db_path)
    for order_id, order in list(pending_buys.items()):
        if order.get("filled_qty", 0.0) > 1e-9:
            continue
        if not clob.cancel_order(order_id):
            logger.warning(f"terminal cancel failed for paper order {order_id[:8]}")
        conn.execute(
            "UPDATE resting_orders SET status='cancelled' WHERE order_id=?",
            (order_id,),
        )
    conn.commit()
    conn.close()

    # ── Resolve position ───────────────────────────────────────────────────────
    try:
        om.on_market_resolved(token_id, is_winner)
    except Exception as e:
        logger.warning(f"on_market_resolved error {token_id[:16]}: {e}")

    return {
        "status": "ok",
        "token_id": token_id,
        "market_id": market_id,
        "category": category,
        "current_price": current_price,
        "entry_levels": entry_levels,
        "trades_count": len(trades),
        "filled_entries": filled_entries,
        "filled_tps": filled_tps,
        "is_winner": is_winner,
        "market_score": market_score.total if market_score else None,
        "group_boost": group_boost,
    }


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(
    results: list[dict],
    positions_db_path: str,
    mode: str,
    start: date,
    end: date,
    initial_balance: float,
) -> None:
    from collections import Counter

    status_counts = Counter(r["status"] for r in results)
    ok = [r for r in results if r["status"] == "ok"]

    conn = sqlite3.connect(positions_db_path)
    conn.row_factory = sqlite3.Row
    positions = conn.execute("SELECT * FROM positions").fetchall()
    paper_balance = conn.execute("SELECT cash_balance FROM paper_balance WHERE id=1").fetchone()
    conn.close()

    total_stake   = sum(float(p["entry_size_usdc"]) for p in positions)
    total_pnl     = sum(float(p["realized_pnl"] or 0.0) for p in positions)
    open_positions = [p for p in positions if p["status"] == "open"]
    open_pos      = len(open_positions)
    resolved      = [p for p in positions if p["status"] == "resolved"]
    winners       = sum(1 for p in resolved if float(p["realized_pnl"] or 0) > 0)
    roi_pct       = (total_pnl / total_stake * 100) if total_stake > 0 else 0.0

    cash_balance = float(paper_balance["cash_balance"]) if paper_balance else 0.0
    open_position_cost_basis = sum(float(p["entry_size_usdc"] or 0.0) for p in open_positions)
    final_equity = cash_balance + open_position_cost_basis
    bankroll_return_pct = ((final_equity - initial_balance) / initial_balance * 100) if initial_balance > 0 else 0.0

    total_screened = len(results)
    total_bids     = sum(r.get("filled_entries", 0) for r in ok)
    total_tps      = sum(r.get("filled_tps", 0) for r in ok)
    fill_rate      = (total_bids / len(ok) * 100) if ok else 0.0

    sep = "=" * 66
    print(f"\n{sep}")
    print(f"  HONEST FULL-UNIVERSE REPLAY   mode={mode}  {start} → {end}")
    print(sep)
    print(f"  Universe (market/token pairs) : {total_screened}")
    print(f"    Passed screener             : {len(ok)}  ({100*len(ok)/total_screened:.1f}%)")
    print(f"    Rejected hours_min          : {status_counts.get('rejected_hours_min', 0)}")
    print(f"    Rejected hours_max          : {status_counts.get('rejected_hours_max', 0)}")
    print(f"    Rejected price above max    : {status_counts.get('rejected_price_above_max', 0)}")
    print(f"    Rejected no entry levels    : {status_counts.get('rejected_no_entry_levels', 0)}")
    print(f"    Rejected dead market        : {status_counts.get('rejected_dead_market', 0)}")
    print(f"    Rejected market score       : {status_counts.get('rejected_market_score', 0)}")
    print(f"    Skipped neg-risk (fav/cap)  : {status_counts.get('skipped_neg_risk', 0)}")
    print(f"    No trade file               : {status_counts.get('no_trade_file', 0)}")
    print(f"    Other rejections            : {sum(v for k,v in status_counts.items() if k not in ('ok','rejected_hours_min','rejected_hours_max','rejected_price_above_max','rejected_no_entry_levels','rejected_dead_market','rejected_market_score','skipped_neg_risk','no_trade_file'))}")
    print()
    print(f"  Resting bids placed (markets) : {len(ok)}")
    print(f"  Entry fills                   : {total_bids}  (fill_rate={fill_rate:.1f}%)")
    print(f"  TP fills                      : {total_tps}")
    print()
    print(f"  Positions opened              : {len(positions)}")
    print(f"    Resolved                    : {len(resolved)}  (winners={winners}  losers={len(resolved)-winners})")
    print(f"    Still open                  : {open_pos}")
    print(f"  Total stake deployed  (USDC)  : ${total_stake:.4f}")
    print(f"  Total realized PnL    (USDC)  : ${total_pnl:.4f}")
    print(f"  ROI on stake                  : {roi_pct:.1f}%")
    print(f"  Initial balance       (USDC)  : ${initial_balance:.4f}")
    print(f"  Final equity          (USDC)  : ${final_equity:.4f}")
    if open_pos:
        print(f"    includes open position cost : ${open_position_cost_basis:.4f}")
    print(f"  Bankroll return               : {bankroll_return_pct:.1f}%")
    print()

    # Category breakdown
    cat_ok = Counter(r.get("category","") for r in ok)
    print(f"  Candidates by category:")
    for cat, cnt in cat_ok.most_common(10):
        print(f"    {cat:<20} {cnt}")

    print()
    print(f"  ⚠  Limitations (see module docstring):")
    print(f"     1. Price = last_trade_price, not CLOB best_ask (optimistic fills)")
    print(f"     2. Volume filter uses final total, not volume-at-discovery")
    print(f"     3. Spread gate not implemented (no historical orderbook)")
    print(f"     4. Neg-risk group boost not simulated")
    print(sep)


# ── Main ──────────────────────────────────────────────────────────────────────



def _history_run_dir(history_root: Path, prefix: str = "run") -> Path:
    """Create a timestamped child directory under history_root for this replay run."""
    history_root.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = history_root / f"{prefix}_{ts}"
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def _reset_replay_output(output_dir: Path) -> tuple[Path, Path]:
    """Ensure replay DB artifacts are fresh even if output_dir already exists."""
    output_dir.mkdir(parents=True, exist_ok=True)
    positions_db = output_dir / "positions.db"
    paper_db = output_dir / "paper_trades.db"
    for path in (positions_db, paper_db):
        if path.exists():
            path.unlink()
    return positions_db, paper_db


def run_honest_replay(
    start: date,
    end: date,
    mode: str,
    output_dir: Path,
    limit: Optional[int] = None,
    activation_delay: int = 0,
    fill_fraction: float = 1.0,
    summary_only: bool = False,
) -> None:
    positions_db, paper_db = _reset_replay_output(output_dir)

    om_module.POSITIONS_DB = positions_db

    # Suppress verbose loggers and Telegram during replay.
    import logging
    logging.getLogger("order_manager").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.CRITICAL)
    # Patch send_message in every module that has already imported it by name.
    import utils.telegram as _tg
    _noop = lambda *a, **kw: None
    _tg.send_message = _noop
    om_module.send_message = _noop  # order_manager imports send_message at module level

    from execution.order_manager import OrderManager

    config = BotConfig(mode=mode, dry_run=True)
    mc     = config.mode_config
    clob   = ClobClient(private_key="replay_dummy", dry_run=True, paper_db_path=paper_db)
    risk   = RiskManager(mc, balance_usdc=config.paper_initial_balance_usdc)
    om     = OrderManager(config, clob, risk)

    # MarketScorer: reads feature_mart_v1_1 from SQLite — no live API needed.
    scorer: Optional[MarketScorer] = None
    if mc.min_market_score > 0 or mc.market_score_tiers:
        try:
            scorer = MarketScorer(db_path=DB_PATH, min_score=mc.min_market_score)
            logger.info(
                f"MarketScorer loaded: min_score={mc.min_market_score} "
                f"analogy_cohorts={len(scorer._analogy)}"
            )
        except Exception as e:
            logger.warning(f"MarketScorer init failed, scoring disabled: {e}")

    # Trade file index: one-time filesystem scan instead of per-token
    logger.info("Building trade file index...")
    t_idx_start = time.monotonic()
    trade_index = _build_trade_file_index()
    logger.info(
        f"Trade file index: {len(trade_index)} tokens "
        f"({time.monotonic() - t_idx_start:.1f}s)"
    )

    start_ts = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
    end_ts   = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())

    db_conn = sqlite3.connect(DB_PATH)
    db_conn.row_factory = sqlite3.Row
    all_rows = load_all_markets(db_conn, start_ts, end_ts, config)
    db_conn.close()

    if limit:
        all_rows = all_rows[:limit]

    logger.info(
        f"Honest replay: {start} → {end} | mode={mode} | "
        f"market/token pairs loaded: {len(all_rows)}"
        + (f" (limit={limit})" if limit else "")
    )
    logger.info(
        f"Fill model: activation_delay={activation_delay}s "
        f"fill_fraction={fill_fraction:.2f} "
        f"({'pessimistic' if fill_fraction < 1.0 or activation_delay > 0 else 'oracle'})"
    )
    logger.info(f"Output: {output_dir}")

    # Neg-risk group boosts: pre-compute once, O(1) lookup per token
    logger.info("Pre-computing neg-risk group boosts...")
    neg_risk_boosts = _compute_neg_risk_boosts(all_rows, trade_index, start_ts, mc)
    nr_underdog = sum(1 for v in neg_risk_boosts.values() if v is not None)
    nr_skip = sum(1 for v in neg_risk_boosts.values() if v is None)
    if neg_risk_boosts:
        logger.info(f"Neg-risk: {nr_underdog} underdog tokens, {nr_skip} skipped (favorites/cap)")

    results: list[dict] = []
    t0 = time.monotonic()

    for i, row in enumerate(all_rows, 1):
        token_id = row["token_id"]

        # Skip neg-risk favorites and over-cap tokens
        if token_id in neg_risk_boosts and neg_risk_boosts[token_id] is None:
            results.append({
                "status": "skipped_neg_risk",
                "token_id": token_id,
                "market_id": row["market_id"],
            })
            continue

        group_boost = neg_risk_boosts.get(token_id, 1.0)

        result = simulate_token(
            row, om, clob, mc, risk, scorer,
            str(positions_db),
            start_ts, end_ts,
            dead_market_hours=config.dead_market_hours,
            trade_index=trade_index,
            activation_delay=activation_delay,
            fill_fraction=fill_fraction,
            group_boost=group_boost,
        )
        results.append(result)

        if not summary_only and result["status"] == "ok" and result.get("filled_entries", 0) > 0:
            ms_info = f" mscore={result['market_score']:.3f}" if result.get("market_score") else ""
            boost_info = f" boost={result['group_boost']:.2f}" if result.get("group_boost", 1.0) != 1.0 else ""
            logger.info(
                f"[{i}/{len(all_rows)}] FILL {row['token_id'][:16]} "
                f"cat={row.get('category','')} "
                f"price={result['current_price']:.4f} "
                f"entries={result['filled_entries']} tps={result['filled_tps']} "
                f"winner={result['is_winner']}{ms_info}{boost_info}"
            )
        elif not summary_only and i % 500 == 0:
            passed = sum(1 for r in results if r["status"] == "ok")
            filled = sum(r.get("filled_entries", 0) for r in results if r["status"] == "ok")
            logger.info(f"  Progress {i}/{len(all_rows)} | passed_screener={passed} | fills={filled}")

    final_passed = sum(1 for r in results if r["status"] == "ok")
    final_filled = sum(r.get("filled_entries", 0) for r in results if r["status"] == "ok")
    logger.info(
        f"Final totals | passed_screener={final_passed} | fills={final_filled}"
    )

    elapsed = time.monotonic() - t0
    logger.info(f"Replay finished in {elapsed:.1f}s  ({len(all_rows)/elapsed:.0f} tokens/s)")

    print_summary(results, str(positions_db), mode, start, end, config.paper_initial_balance_usdc)
    print(f"  Positions DB : {positions_db}")
    print(f"  Paper trades : {paper_db}\n")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Honest full-universe replay — no look-ahead bias"
    )
    ap.add_argument("--start",  metavar="YYYY-MM-DD", required=True)
    ap.add_argument("--end",    metavar="YYYY-MM-DD", required=True)
    ap.add_argument("--mode",   default="big_swan_mode",
                    choices=["big_swan_mode", "balanced_mode", "fast_tp_mode", "small_swan_mode"])
    ap.add_argument("--limit",  type=int, default=None,
                    help="Max market/token pairs to process (default: all)")
    ap.add_argument("--out",    default=None,
                    help="Output dir (default: data/replay_runs/honest_<timestamp>)")
    ap.add_argument("--history-root", default=None,
                    help="Parent dir for timestamped history runs; creates <history-root>/run_<timestamp>")
    ap.add_argument("--pessimistic", action="store_true",
                    help="activation_delay=300s, fill_fraction=0.3")
    ap.add_argument("--activation-delay", type=int, default=0)
    ap.add_argument("--fill-fraction",    type=float, default=1.0)
    ap.add_argument("--summary", action="store_true",
                    help="Only log fills (suppress per-token progress noise)")
    args = ap.parse_args()

    if args.out and args.history_root:
        ap.error("use either --out or --history-root, not both")

    if args.history_root:
        output_dir = _history_run_dir(Path(args.history_root))
    elif args.out:
        output_dir = Path(args.out)
    else:
        ts = time.strftime("%Y%m%d_%H%M%S")
        output_dir = DATA_DIR / "replay_runs" / f"honest_{ts}"

    activation_delay = args.activation_delay
    fill_fraction    = args.fill_fraction
    if args.pessimistic:
        activation_delay = max(activation_delay, 300)
        fill_fraction    = min(fill_fraction, 0.3)

    run_honest_replay(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        mode=args.mode,
        output_dir=output_dir,
        limit=args.limit,
        activation_delay=activation_delay,
        fill_fraction=fill_fraction,
        summary_only=args.summary,
    )


if __name__ == "__main__":
    main()
