"""
Historical dry-run replay baseline.

Replays token_swans events in chronological order through the same entry/TP/resolution
path as live big_swan_mode — paper orders only, no live API calls.

Signal source : token_swans (entry data only — no exit leakage)
Fill model    : conservative — BUY fills at limit price on first trade <= bid_price
                AFTER entry_ts_first; TP fills at limit price on first trade >= tp_price;
                per-trade liquidity is consumed sequentially across all pending orders.
State machine : OrderManager.on_entry_filled / on_tp_filled / on_market_resolved
Paper DB      : isolated per-run directory under data/replay_runs/<timestamp>/

Correctness properties (v4):
  1. Temporal gate    — orders only become active at/after entry_ts_first
  2. Liquidity drain  — each trade's size budget is shared across all fills from that print
  3. Partial fills    — orders stay pending until fully filled; qty reduces each fill
  4. Stable dedup     — ROW_NUMBER() picks the exact row with max possible_x per (token_id, date)
  5. Single dispatch  — on_entry_filled called exactly once per order; filled_entries ==
                        Positions opened always holds (no duplicate positions per order)
  6. Trade direction  — SELL-side trades fill BUY bids; BUY-side trades fill SELL TPs.
                        Uses the 'side' field present in all trade records.
  7. BUY price priority — pending orders iterated highest-price-first so higher bids
                        have first claim on available liquidity (correct CLOB semantics).

Known limitation: wall-clock time.time() is used for DB record timestamps, not replay-time.
This does not affect fill logic or PnL — only order created_at / expires_at fields.

Usage:
    python3 scripts/run_dry_run_replay.py --start 2026-02-01 --end 2026-02-07
    python3 scripts/run_dry_run_replay.py --start 2026-02-01 --end 2026-02-07 --limit 30
    python3 scripts/run_dry_run_replay.py --start 2026-02-01 --end 2026-02-07 --mode fast_tp_mode
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Patch POSITIONS_DB BEFORE importing OrderManager — the constructor binds the path at init time.
import execution.order_manager as om_module

from api.clob_client import ClobClient
from config import BotConfig
from strategy.risk_manager import RiskManager
from utils.logger import setup_logger
from utils.paths import DATABASE_DIR, DB_PATH, DATA_DIR

logger = setup_logger("replay")

# ── Research-baseline universe gates ─────────────────────────────────────────
# These are data-quality filters for the historical cohort, NOT live-bot params.
# ORACLE_CATCHABLE_MAX is derived from config at runtime (see load_candidates).
_RESEARCH_ENTRY_VOLUME_MIN    = 50.0
_RESEARCH_ENTRY_TRADE_COUNT   = 3
_RESEARCH_DURATION_HOURS_MIN  = 24.0
_RESEARCH_VOLUME_1WK_MIN      = 1000.0


# ── Data loading ──────────────────────────────────────────────────────────────

def load_candidates(
    conn: sqlite3.Connection,
    start: date,
    end: date,
    limit: Optional[int],
    oracle_catchable_max: float = 0.01,
) -> list[dict]:
    """
    Load deduplicated token_swans candidates within the oracle universe.

    oracle_catchable_max = max(entry_price_levels) from live config.
    Only swans where entry_min_price <= oracle_catchable_max are loaded —
    these are events where at least the highest resting bid would have filled.

    Uses ROW_NUMBER() to pick the single exact row with the highest possible_x
    per (token_id, date) — every field comes from the same consistent row.
    Ordered by entry_ts_first ascending (chronological replay order).
    """
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                ts.*,
                ROW_NUMBER() OVER (
                    PARTITION BY ts.token_id, ts.date
                    ORDER BY ts.possible_x DESC, ts.rowid ASC
                ) AS rn
            FROM token_swans ts
            WHERE ts.entry_min_price   <= :emax
              AND ts.entry_volume_usdc >= :evol
              AND ts.entry_trade_count >= :etc
              AND ts.date >= :start
              AND ts.date <= :end
        )
        SELECT
            r.token_id,
            r.market_id,
            r.date,
            r.entry_min_price,
            r.entry_volume_usdc,
            r.entry_trade_count,
            r.possible_x,
            r.entry_ts_first,
            r.entry_ts_last,
            m.closed_time,
            m.duration_hours,
            m.volume_1wk,
            t.is_winner,
            t.outcome_name
        FROM ranked r
        JOIN markets m ON r.market_id = m.id
        JOIN tokens  t ON r.token_id  = t.token_id
        WHERE r.rn = 1
          AND m.duration_hours     >= :dh
          AND (m.volume_1wk IS NULL OR m.volume_1wk >= :vwk)
        ORDER BY r.entry_ts_first ASC
        """,
        {
            "emax": oracle_catchable_max,
            "evol": _RESEARCH_ENTRY_VOLUME_MIN,
            "etc":  _RESEARCH_ENTRY_TRADE_COUNT,
            "dh":   _RESEARCH_DURATION_HOURS_MIN,
            "vwk":  _RESEARCH_VOLUME_1WK_MIN,
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
    ).fetchall()

    if limit:
        rows = rows[:limit]
    return [dict(r) for r in rows]


def load_trades(market_id: str, token_id: str, collection_date: str) -> list[dict]:
    """
    Load raw trades for a token, sorted ascending by timestamp.
    Returns [] if the file is missing or unreadable.
    """
    fpath = os.path.join(
        DATABASE_DIR,
        collection_date,
        f"{market_id}_trades",
        f"{token_id}.json",
    )
    if not os.path.exists(fpath):
        return []
    try:
        with open(fpath, encoding="utf-8") as f:
            trades = json.load(f)
        # Files are stored descending — reverse to get chronological order.
        trades.sort(key=lambda t: t["timestamp"])
        return trades
    except Exception as e:
        logger.warning(f"Trade file read error ({market_id}/{token_id[:16]}): {e}")
        return []


# ── Positions DB helper ───────────────────────────────────────────────────────

def _insert_resting_order(
    db_path: str,
    order_id: str,
    token_id: str,
    market_id: str,
    price: float,
    size: float,
    expires_at: int,
    mode: str,
    label: str = "",
) -> None:
    """Insert a paper BUY order into resting_orders so on_entry_filled can mark it matched.

    label is stored in resting_orders.label and used for event-level PnL attribution:
    format 'replay_{collection_date}_{price}' lets callers extract the event date
    by joining positions → resting_orders on entry_order_id.
    """
    now = int(time.time())
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO resting_orders "
        "(order_id, token_id, market_id, side, price, size, status, created_at, expires_at, mode, label) "
        "VALUES (?, ?, ?, 'BUY', ?, ?, 'live', ?, ?, ?, ?)",
        (order_id, token_id, market_id, price, size, now, expires_at, mode, label),
    )
    conn.commit()
    conn.close()


# ── Per-candidate replay ──────────────────────────────────────────────────────

def replay_candidate(
    candidate: dict,
    om,
    clob: ClobClient,
    mc,
    positions_db_path: str,
) -> dict:
    """
    Drive one swan candidate through the full entry → TP → resolution path.

    Correctness properties enforced here:
    1. Temporal gate    : BUY fills only checked for trades at/after entry_ts_first
    2. Liquidity drain  : each trade's token budget shared sequentially across all fills
    3. Partial fills    : order stays pending with reduced qty until fully consumed
    4. Single dispatch  : on_entry_filled called exactly once per order (at full fill or
                          trade-file end); filled_entries == Positions opened always holds

    Returns a result dict summarising what happened.
    """
    token_id = candidate["token_id"]
    market_id = candidate["market_id"]
    collection_date = candidate["date"]
    entry_min_price = float(candidate["entry_min_price"])
    entry_ts_first = int(candidate["entry_ts_first"])
    is_winner = bool(candidate["is_winner"])
    outcome_name = candidate.get("outcome_name") or ""

    # Load trade history
    trades = load_trades(market_id, token_id, collection_date)
    if not trades:
        return {"status": "no_trades", "token_id": token_id, "market_id": market_id}

    # Entry levels: ladder levels that price actually passed through on the way down.
    # A bid at `lvl` fills when price crosses lvl from above → need lvl >= entry_min_price.
    # (entry_min_price is the floor; price crossed all ladder levels above that floor.)
    entry_levels = [lvl for lvl in mc.entry_price_levels if lvl >= entry_min_price]
    if not entry_levels:
        return {"status": "no_levels", "token_id": token_id, "market_id": market_id,
                "entry_min_price": entry_min_price}

    # Place paper resting BUY orders.
    # pending_buys: order_id → {price, remaining_qty, filled_qty}
    # filled_qty accumulates across partial fills; on_entry_filled is called
    # exactly ONCE per order — when fully consumed or after the last trade.
    # Orders are not checked for fills until entry_ts_first (temporal gate).
    now_ts = int(time.time())
    expires_at = now_ts + 86400 * 60  # long TTL — replay doesn't expire orders
    pending_buys: dict[str, dict] = {}

    def _tier_stake(level: float) -> float:
        """Return stake for this entry level from config tier schedule."""
        if mc.stake_tiers:
            for tier_price, tier_stake in mc.stake_tiers:
                if level <= tier_price:
                    return tier_stake
        return mc.stake_usdc

    for level in entry_levels:
        stake = _tier_stake(level)
        token_qty = stake / level
        # Label format: 'replay_{collection_date}_{level}'
        # The collection_date prefix is used by cohort_report for event-level PnL attribution
        # (same token_id can appear on multiple dates; label disambiguates them).
        event_label = f"replay_{collection_date}_{level}"
        result = clob.place_limit_order(
            token_id=token_id,
            side="BUY",
            price=level,
            size=token_qty,
            label=event_label,
        )
        if result.status == "live":
            _insert_resting_order(
                positions_db_path,
                result.order_id,
                token_id,
                market_id,
                level,
                token_qty,
                expires_at,
                mc.name,
                label=event_label,
            )
            pending_buys[result.order_id] = {
                "price": level,
                "remaining_qty": token_qty,
                "filled_qty": 0.0,
            }

    if not pending_buys:
        return {"status": "no_orders", "token_id": token_id, "market_id": market_id}

    filled_entries = 0
    filled_tps = 0

    def _dispatch_entry(order_id: str, order: dict) -> None:
        """Call on_entry_filled exactly once for a completed/flushed order."""
        nonlocal filled_entries
        total_fill = order["filled_qty"]
        if total_fill <= 1e-9:
            return
        clob.paper_simulate_fill(order_id, order["price"], total_fill)
        try:
            om.on_entry_filled(
                order_id=order_id,
                token_id=token_id,
                market_id=market_id,
                fill_price=order["price"],
                fill_quantity=total_fill,
                outcome_name=outcome_name,
            )
        except Exception as e:
            logger.warning(f"on_entry_filled error {token_id[:16]}: {e}")
        filled_entries += 1

    # Process trades in ascending timestamp order
    for trade in trades:
        trade_ts = int(trade["timestamp"])
        trade_price = float(trade["price"])
        trade_budget = float(trade["size"])  # tokens available from this print
        # Trade direction: 'SELL' taker = someone selling into resting BUY bids;
        # 'BUY' taker = someone buying against resting SELL asks (our TP orders).
        # '' fallback preserves behaviour for any legacy files without a side field.
        trade_side = trade.get("side", "")

        # ── Check resting BUY fills (only at/after signal time) ───────────────
        # Temporal gate: orders inactive before the floor event.
        # Liquidity drain: trade_budget is shared sequentially across all fills.
        # Partial fills: on_entry_filled deferred until order fully consumed.
        # Side filter: only SELL-side trades (taker selling) can fill our BUY bids.
        #   Iterating in descending price order restores correct BUY price priority:
        #   higher-priced bids have first claim on available liquidity.
        if trade_side in ("SELL", "") and trade_ts >= entry_ts_first and pending_buys:
            for order_id, order in sorted(
                pending_buys.items(), key=lambda kv: kv[1]["price"], reverse=True
            ):
                if trade_budget <= 0:
                    break
                bid_price = order["price"]
                if trade_price <= bid_price:
                    fill_qty = min(order["remaining_qty"], trade_budget)
                    trade_budget -= fill_qty
                    order["remaining_qty"] -= fill_qty
                    order["filled_qty"] += fill_qty

                    if order["remaining_qty"] <= 1e-9:
                        # Order fully filled — dispatch position + TP orders now
                        _dispatch_entry(order_id, order)
                        del pending_buys[order_id]

        # ── Check TP SELL fills ────────────────────────────────────────────────
        # TP orders are placed by on_entry_filled; they appear in paper_orders
        # as live SELL orders. Fill when trade price crosses the limit.
        # Side filter: only BUY-side trades (taker buying) can fill our SELL TPs.
        if trade_side not in ("SELL",):
            open_sells = [
                o for o in clob.get_open_orders(token_id)
                if o.get("side") == "SELL"
            ]
            if open_sells and trade_budget > 0:
                for tp_order in open_sells:
                    if trade_budget <= 0:
                        break
                    tp_price = float(tp_order["price"])
                    if trade_price >= tp_price:
                        fill_qty = min(float(tp_order["size"]), trade_budget)
                        trade_budget -= fill_qty
                        clob.paper_simulate_fill(tp_order["order_id"], tp_price, fill_qty)
                        try:
                            om.on_tp_filled(tp_order["order_id"], tp_price, fill_qty)
                        except Exception as e:
                            logger.warning(f"on_tp_filled error {token_id[:16]}: {e}")
                        filled_tps += 1

    # Flush any orders that were partially filled but not fully consumed by end of trades.
    # This ensures every touched order produces exactly one position.
    # Orders with zero fills are marked 'cancelled' so they don't pollute live-order counts.
    conn = sqlite3.connect(positions_db_path)
    for order_id, order in list(pending_buys.items()):
        if order["filled_qty"] > 1e-9:
            _dispatch_entry(order_id, order)
        else:
            conn.execute(
                "UPDATE resting_orders SET status='cancelled' WHERE order_id=?",
                (order_id,),
            )
    conn.commit()
    conn.close()

    # ── Market resolution ──────────────────────────────────────────────────────
    try:
        om.on_market_resolved(token_id, is_winner)
    except Exception as e:
        logger.warning(f"on_market_resolved error {token_id[:16]}: {e}")

    return {
        "status": "ok",
        "token_id": token_id,
        "market_id": market_id,
        "entry_min_price": entry_min_price,
        "entry_levels": entry_levels,
        "trades_count": len(trades),
        "filled_entries": filled_entries,
        "filled_tps": filled_tps,
        "is_winner": is_winner,
        "possible_x": candidate.get("possible_x"),
    }


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(results: list[dict], positions_db_path: str, mode: str) -> None:
    ok = [r for r in results if r["status"] == "ok"]
    skipped = len(results) - len(ok)
    total_entries = sum(r.get("filled_entries", 0) for r in ok)
    total_tps = sum(r.get("filled_tps", 0) for r in ok)

    conn = sqlite3.connect(positions_db_path)
    conn.row_factory = sqlite3.Row
    positions = conn.execute("SELECT * FROM positions").fetchall()
    conn.close()

    total_stake = sum(float(p["entry_size_usdc"]) for p in positions)
    total_pnl = sum(float(p["realized_pnl"] or 0.0) for p in positions)
    open_pos = sum(1 for p in positions if p["status"] == "open")
    resolved = [p for p in positions if p["status"] == "resolved"]
    winners = sum(1 for p in resolved if float(p["realized_pnl"] or 0) > 0)
    losers = len(resolved) - winners

    roi_pct = (total_pnl / total_stake * 100) if total_stake > 0 else 0.0

    winner_entries = sum(r.get("filled_entries", 0) for r in ok if r.get("is_winner"))
    winner_candidates = sum(1 for r in ok if r.get("is_winner"))

    sep = "=" * 62
    print(f"\n{sep}")
    print(f"  DRY-RUN REPLAY SUMMARY   mode={mode}")
    print(sep)
    print(f"  Candidates replayed  : {len(results)}")
    print(f"    OK (had trades)    : {len(ok)}")
    print(f"    Skipped            : {skipped}  (no trades file / no entry levels)")
    print(f"  Entry fills          : {total_entries}")
    print(f"  TP fills             : {total_tps}")
    print(f"")
    print(f"  Positions opened     : {len(positions)}")
    print(f"    Resolved           : {len(resolved)}  (winners={winners}  losers={losers})")
    print(f"    Still open         : {open_pos}")
    print(f"  Total stake USDC     : ${total_stake:.4f}")
    print(f"  Total realized PnL   : ${total_pnl:.4f}")
    print(f"  ROI                  : {roi_pct:.1f}%")
    print(f"")
    print(f"  Signal quality check:")
    print(f"    Winners in signal  : {winner_candidates}/{len(ok)}"
          f"  ({100*winner_candidates/len(ok):.0f}%)" if ok else "    Winners in signal  : 0/0")
    print(f"    Entry fills winners: {winner_entries}/{total_entries}"
          f"  ({100*winner_entries/total_entries:.0f}%)" if total_entries else
          f"    Entry fills winners: 0/0")
    print(sep)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_replay(
    start: date,
    end: date,
    limit: Optional[int],
    mode: str,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    positions_db = output_dir / "positions.db"
    paper_db = output_dir / "paper_trades.db"

    # Patch module-level POSITIONS_DB BEFORE OrderManager is instantiated.
    om_module.POSITIONS_DB = positions_db

    from execution.order_manager import OrderManager

    config = BotConfig(mode=mode, dry_run=True)
    mc = config.mode_config
    clob = ClobClient(private_key="replay_dummy", dry_run=True, paper_db_path=paper_db)
    risk = RiskManager(mc, balance_usdc=100.0)
    om = OrderManager(config, clob, risk)

    # Oracle catchable threshold: highest resting ladder level.
    # Swans where entry_min_price > this level have no fillable bids.
    oracle_catchable_max = max(mc.entry_price_levels)
    logger.info(
        f"Oracle universe: entry_min_price <= {oracle_catchable_max:.4f} "
        f"(= max ladder level from config)"
    )

    # Load signal universe
    db_conn = sqlite3.connect(DB_PATH)
    db_conn.row_factory = sqlite3.Row
    candidates = load_candidates(db_conn, start, end, limit, oracle_catchable_max)
    db_conn.close()

    logger.info(
        f"Replay starting: {start} → {end} | mode={mode} | candidates={len(candidates)}"
        + (f" (limit={limit})" if limit else "")
    )
    logger.info(f"Output dir: {output_dir}")

    results: list[dict] = []
    t0 = time.monotonic()

    for i, candidate in enumerate(candidates, 1):
        result = replay_candidate(candidate, om, clob, mc, str(positions_db))
        results.append(result)

        status = result["status"]
        if status == "ok":
            logger.info(
                f"[{i}/{len(candidates)}] {candidate['token_id'][:16]} "
                f"price={candidate['entry_min_price']:.4f} "
                f"entries={result['filled_entries']} tps={result['filled_tps']} "
                f"winner={result['is_winner']} possible_x={result.get('possible_x', '?'):.1f}"
            )
        else:
            logger.debug(
                f"[{i}/{len(candidates)}] {candidate['token_id'][:16]} status={status}"
            )

    elapsed = time.monotonic() - t0
    logger.info(f"Replay finished in {elapsed:.1f}s")

    print_summary(results, str(positions_db), mode)
    print(f"\n  Positions DB : {positions_db}")
    print(f"  Paper trades : {paper_db}\n")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Historical dry-run replay — drives token_swans events through live bot path"
    )
    ap.add_argument("--start", metavar="YYYY-MM-DD", required=True)
    ap.add_argument("--end",   metavar="YYYY-MM-DD", required=True)
    ap.add_argument("--limit", type=int, default=None,
                    help="Max candidates to replay (default: all)")
    ap.add_argument("--mode",  default="big_swan_mode",
                    choices=["big_swan_mode", "balanced_mode", "fast_tp_mode"])
    ap.add_argument("--out",   default=None,
                    help="Output directory (default: data/replay_runs/<timestamp>)")
    args = ap.parse_args()

    if args.out:
        output_dir = Path(args.out)
    else:
        ts = time.strftime("%Y%m%d_%H%M%S")
        output_dir = DATA_DIR / "replay_runs" / ts

    run_replay(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        limit=args.limit,
        mode=args.mode,
        output_dir=output_dir,
    )


if __name__ == "__main__":
    main()
