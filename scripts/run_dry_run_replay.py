"""
Historical dry-run replay baseline.

Replays token_swans events in chronological order through the same entry/TP/resolution
path as live big_swan_mode — paper orders only, no live API calls.

Signal source : token_swans (entry data only — no exit leakage)
Fill model    : conservative — BUY fills on first trade at price <= bid_price;
                TP fills on first trade at price >= tp_price;
                fill_quantity = min(order_tokens, trade.size)
State machine : OrderManager.on_entry_filled / on_tp_filled / on_market_resolved
Paper DB      : isolated per-run directory under data/replay_runs/<timestamp>/

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

# ── Baseline universe gates (same as feature_mart) ────────────────────────────
ENTRY_PRICE_MAX = 0.05
ENTRY_VOLUME_MIN = 50.0
ENTRY_TRADE_COUNT_MIN = 3
DURATION_HOURS_MIN = 24.0
VOLUME_1WK_MIN = 1000.0


# ── Data loading ──────────────────────────────────────────────────────────────

def load_candidates(
    conn: sqlite3.Connection,
    start: date,
    end: date,
    limit: Optional[int],
) -> list[dict]:
    """
    Load deduplicated token_swans candidates within the baseline universe.
    Deduplicates (token_id, date) by taking the row with max possible_x.
    Ordered by entry_ts_first ascending (chronological replay order).
    """
    rows = conn.execute(
        """
        SELECT
            ts.token_id,
            ts.market_id,
            ts.date,
            ts.entry_min_price,
            ts.entry_volume_usdc,
            ts.entry_trade_count,
            MAX(ts.possible_x)      AS possible_x,
            MIN(ts.entry_ts_first)  AS entry_ts_first,
            MAX(ts.entry_ts_last)   AS entry_ts_last,
            m.closed_time,
            m.duration_hours,
            m.volume_1wk,
            t.is_winner,
            t.outcome_name
        FROM token_swans ts
        JOIN markets m ON ts.market_id = m.id
        JOIN tokens  t ON ts.token_id  = t.token_id
        WHERE ts.entry_min_price    <= :emax
          AND ts.entry_volume_usdc  >= :evol
          AND ts.entry_trade_count  >= :etc
          AND m.duration_hours      >= :dh
          AND (m.volume_1wk IS NULL OR m.volume_1wk >= :vwk)
          AND ts.date >= :start
          AND ts.date <= :end
        GROUP BY ts.token_id, ts.date
        ORDER BY MIN(ts.entry_ts_first) ASC
        """,
        {
            "emax": ENTRY_PRICE_MAX,
            "evol": ENTRY_VOLUME_MIN,
            "etc": ENTRY_TRADE_COUNT_MIN,
            "dh": DURATION_HOURS_MIN,
            "vwk": VOLUME_1WK_MIN,
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
) -> None:
    """Insert a paper BUY order into resting_orders so on_entry_filled can mark it matched."""
    now = int(time.time())
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO resting_orders "
        "(order_id, token_id, market_id, side, price, size, status, created_at, expires_at, mode) "
        "VALUES (?, ?, ?, 'BUY', ?, ?, 'live', ?, ?, ?)",
        (order_id, token_id, market_id, price, size, now, expires_at, mode),
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
    Returns a result dict summarising what happened.
    """
    token_id = candidate["token_id"]
    market_id = candidate["market_id"]
    collection_date = candidate["date"]
    entry_min_price = float(candidate["entry_min_price"])
    is_winner = bool(candidate["is_winner"])
    outcome_name = candidate.get("outcome_name") or ""

    # Load trade history
    trades = load_trades(market_id, token_id, collection_date)
    if not trades:
        return {"status": "no_trades", "token_id": token_id, "market_id": market_id}

    # Entry levels: all mode levels at or below the detected floor price
    entry_levels = [lvl for lvl in mc.entry_price_levels if lvl <= entry_min_price]
    if not entry_levels:
        return {"status": "no_levels", "token_id": token_id, "market_id": market_id,
                "entry_min_price": entry_min_price}

    # Place paper resting BUY orders
    now_ts = int(time.time())
    expires_at = now_ts + 86400 * 60  # long TTL — replay doesn't expire orders
    pending_buys: dict[str, dict] = {}  # order_id → {price, token_qty}

    for level in entry_levels:
        token_qty = mc.stake_usdc / level
        result = clob.place_limit_order(
            token_id=token_id,
            side="BUY",
            price=level,
            size=token_qty,
            label=f"replay_{level}",
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
            )
            pending_buys[result.order_id] = {"price": level, "token_qty": token_qty}

    if not pending_buys:
        return {"status": "no_orders", "token_id": token_id, "market_id": market_id}

    filled_entries = 0
    filled_tps = 0

    # Process trades in ascending timestamp order
    for trade in trades:
        trade_price = float(trade["price"])
        trade_size = float(trade["size"])  # tokens

        # ── Check resting BUY fills ────────────────────────────────────────────
        # Fill when a trade occurs at or below our bid price.
        # Use the limit bid price as fill price (limit order semantics).
        for order_id, order in list(pending_buys.items()):
            bid_price = order["price"]
            if trade_price <= bid_price:
                fill_qty = min(order["token_qty"], trade_size)
                clob.paper_simulate_fill(order_id, bid_price, fill_qty)
                try:
                    om.on_entry_filled(
                        order_id=order_id,
                        token_id=token_id,
                        market_id=market_id,
                        fill_price=bid_price,
                        fill_quantity=fill_qty,
                        outcome_name=outcome_name,
                    )
                except Exception as e:
                    logger.warning(f"on_entry_filled error {token_id[:16]}: {e}")
                del pending_buys[order_id]
                filled_entries += 1

        # ── Check TP SELL fills ────────────────────────────────────────────────
        # Query live SELL orders from paper DB; fill when trade crosses the limit price.
        open_sells = [
            o for o in clob.get_open_orders(token_id)
            if o.get("side") == "SELL"
        ]
        for tp_order in open_sells:
            tp_price = float(tp_order["price"])
            if trade_price >= tp_price:
                fill_qty = min(float(tp_order["size"]), trade_size)
                clob.paper_simulate_fill(tp_order["order_id"], tp_price, fill_qty)
                try:
                    om.on_tp_filled(tp_order["order_id"], tp_price, fill_qty)
                except Exception as e:
                    logger.warning(f"on_tp_filled error {token_id[:16]}: {e}")
                filled_tps += 1

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

    # Per-cohort breakdown from results
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
    print(f"    Entry fills from winners: {winner_entries}/{total_entries}"
          f"  ({100*winner_entries/total_entries:.0f}%)" if total_entries else
          f"    Entry fills from winners: 0/0")
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

    # Load signal universe
    db_conn = sqlite3.connect(DB_PATH)
    db_conn.row_factory = sqlite3.Row
    candidates = load_candidates(db_conn, start, end, limit)
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
