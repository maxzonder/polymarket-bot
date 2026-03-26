"""
Full-universe honest replay — no look-ahead bias.

Unlike run_dry_run_replay.py (which replays only known black swans from token_swans),
this script simulates the bot running from day 1 over ALL downloaded markets:

  1. Load every market in the date range from polymarket_dataset.db.
  2. Apply screener static filters (volume, category, duration).
  3. At the "discovery time" for each market (= simulation start or first trade,
     whichever is later), check price-dependent filters (current_price, hours_to_close).
  4. Place resting bids at configured levels for markets that pass.
  5. Replay trades chronologically: fill bids, fire TP orders, resolve.

Most resting bids will never fill — the bot scatters bids across hundreds of markets
and only earns on the fraction that actually dip. That's the honest cost of the strategy.

DOCUMENTED LIMITATIONS (read before interpreting results):
  1. current_price = last_trade_price, not CLOB best_ask.
     → Fills may be slightly optimistic; real ask is usually a few ticks higher.
  2. volume uses final total from DB, not volume-at-discovery.
     → Young markets look as liquid as they'll ever be; filter is slightly generous.
  3. NO-token price: 1 - YES synthetic (no CLOB orderbook for NO side).
  4. MarketScorer disabled (ChromaDB not feasible offline) → min_market_score ignored.
     All markets passing static + price filters are considered.
  5. Universe: only markets downloaded in polymarket_dataset.db (~235k of ~300k on Polymarket).
  6. Trade files are full-history snapshots (not daily diffs). We use the latest
     available file per market, which may have been collected after the sim window ends.
     This is fine for fill/TP logic — timestamps are in the trade records.

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
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import execution.order_manager as om_module

from api.clob_client import ClobClient
from config import BotConfig, ModeConfig
from strategy.risk_manager import RiskManager
from utils.logger import setup_logger
from utils.paths import DATABASE_DIR, DB_PATH, DATA_DIR

logger = setup_logger("honest_replay")


# ── Trade file helpers ─────────────────────────────────────────────────────────

def _find_latest_trade_file(market_id: str, token_id: str) -> Optional[str]:
    """
    Return path to the most-recently-collected trade file for this token.
    Trade files are complete history snapshots — the latest date gives the
    fullest picture (more trades = more TP fill opportunities).
    Returns None if no file found.
    """
    dates = sorted(
        [d for d in os.listdir(DATABASE_DIR) if d[:4].isdigit()],
        reverse=True,  # newest first
    )
    for d in dates:
        fpath = os.path.join(DATABASE_DIR, d, f"{market_id}_trades", f"{token_id}.json")
        if os.path.exists(fpath):
            return fpath
    return None


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


# ── DB helpers (same pattern as run_dry_run_replay.py) ────────────────────────

def _insert_resting_order(
    db_path: str,
    order_id: str,
    token_id: str,
    market_id: str,
    price: float,
    size: float,
    mode: str,
    label: str = "",
) -> None:
    now = int(time.time())
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO resting_orders "
        "(order_id, token_id, market_id, side, price, size, status, created_at, expires_at, mode, label) "
        "VALUES (?, ?, ?, 'BUY', ?, ?, 'live', ?, 0, ?, ?)",
        (order_id, token_id, market_id, price, size, now, mode, label),
    )
    conn.commit()
    conn.close()


# ── Per-token simulation ───────────────────────────────────────────────────────

def simulate_token(
    row: dict,
    om,
    clob: ClobClient,
    mc: ModeConfig,
    positions_db_path: str,
    start_ts: int,
    end_ts: int,
    activation_delay: int = 0,
    fill_fraction: float = 1.0,
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

    # ── Static screener: hours_to_close check ─────────────────────────────────
    # Use end_date vs start_ts as proxy for duration seen by bot.
    # Real bot also checks per scan tick; we use discovery-time estimate.
    hours_at_start = (end_date - start_ts) / 3600.0
    if hours_at_start < mc.min_hours_to_close:
        return {"status": "rejected_hours_min", "token_id": token_id, "market_id": market_id}
    if hours_at_start > mc.max_hours_to_close:
        return {"status": "rejected_hours_max", "token_id": token_id, "market_id": market_id}

    # ── Load trades ────────────────────────────────────────────────────────────
    fpath = _find_latest_trade_file(market_id, token_id)
    if fpath is None:
        return {"status": "no_trade_file", "token_id": token_id, "market_id": market_id}

    trades = _load_trades_sorted(fpath)
    if not trades:
        return {"status": "no_trades", "token_id": token_id, "market_id": market_id}

    # ── Discovery time: when screener would first see this token ───────────────
    first_trade_ts = trades[0]["timestamp"]
    discovery_ts = max(start_ts, first_trade_ts)

    # Skip if market already closed before simulation window
    if end_date <= start_ts:
        return {"status": "rejected_closed_before_start", "token_id": token_id, "market_id": market_id}

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

    # ── Place resting BUY orders ───────────────────────────────────────────────
    def _tier_stake(level: float) -> float:
        if mc.stake_tiers:
            for tier_price, tier_stake in mc.stake_tiers:
                if level <= tier_price:
                    return tier_stake
        return mc.stake_usdc

    now_ts = int(time.time())
    pending_buys: dict[str, dict] = {}

    for level in entry_levels:
        stake = _tier_stake(level)
        token_qty = stake / level
        label = f"honest_{market_id}_{level}"
        result = clob.place_limit_order(
            token_id=token_id,
            side="BUY",
            price=level,
            size=token_qty,
            label=label,
        )
        if result.status == "live":
            _insert_resting_order(
                positions_db_path, result.order_id, token_id, market_id,
                level, token_qty, mc.name, label=label,
            )
            pending_buys[result.order_id] = {
                "price": level,
                "remaining_qty": token_qty,
                "filled_qty": 0.0,
            }

    if not pending_buys:
        return {"status": "no_orders_placed", "token_id": token_id, "market_id": market_id}

    filled_entries = 0
    filled_tps = 0

    def _dispatch_entry(order_id: str, order: dict) -> None:
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
                outcome_name=outcome,
            )
        except Exception as e:
            logger.warning(f"on_entry_filled error {token_id[:16]}: {e}")
        filled_entries += 1

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
                    if order["remaining_qty"] <= 1e-9:
                        _dispatch_entry(order_id, order)
                        del pending_buys[order_id]

        # ── TP fills (BUY-side taker hits our SELL TP orders) ─────────────────
        if trade_side not in ("SELL",):
            open_sells = [
                o for o in clob.get_open_orders(token_id)
                if o.get("side") == "SELL"
            ]
            for tp_order in open_sells:
                if trade_budget <= 0:
                    break
                if trade_price >= float(tp_order["price"]):
                    fill_qty = min(float(tp_order["size"]), trade_budget)
                    trade_budget -= fill_qty
                    clob.paper_simulate_fill(tp_order["order_id"], float(tp_order["price"]), fill_qty)
                    try:
                        om.on_tp_filled(tp_order["order_id"], float(tp_order["price"]), fill_qty)
                    except Exception as e:
                        logger.warning(f"on_tp_filled error {token_id[:16]}: {e}")
                    filled_tps += 1

    # ── Flush partial fills at end of trade stream ────────────────────────────
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
    }


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(
    results: list[dict],
    positions_db_path: str,
    mode: str,
    start: date,
    end: date,
) -> None:
    from collections import Counter

    status_counts = Counter(r["status"] for r in results)
    ok = [r for r in results if r["status"] == "ok"]

    conn = sqlite3.connect(positions_db_path)
    conn.row_factory = sqlite3.Row
    positions = conn.execute("SELECT * FROM positions").fetchall()
    conn.close()

    total_stake   = sum(float(p["entry_size_usdc"]) for p in positions)
    total_pnl     = sum(float(p["realized_pnl"] or 0.0) for p in positions)
    open_pos      = sum(1 for p in positions if p["status"] == "open")
    resolved      = [p for p in positions if p["status"] == "resolved"]
    winners       = sum(1 for p in resolved if float(p["realized_pnl"] or 0) > 0)
    roi_pct       = (total_pnl / total_stake * 100) if total_stake > 0 else 0.0

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
    print(f"    No trade file               : {status_counts.get('no_trade_file', 0)}")
    print(f"    Other rejections            : {sum(v for k,v in status_counts.items() if k not in ('ok','rejected_hours_min','rejected_hours_max','rejected_price_above_max','rejected_no_entry_levels','no_trade_file'))}")
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
    print(f"  ROI                           : {roi_pct:.1f}%")
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
    print(f"     3. MarketScorer disabled (min_market_score ignored)")
    print(sep)


# ── Main ──────────────────────────────────────────────────────────────────────

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
    output_dir.mkdir(parents=True, exist_ok=True)
    positions_db = output_dir / "positions.db"
    paper_db     = output_dir / "paper_trades.db"

    om_module.POSITIONS_DB = positions_db

    from execution.order_manager import OrderManager

    config = BotConfig(mode=mode, dry_run=True)
    mc     = config.mode_config
    clob   = ClobClient(private_key="replay_dummy", dry_run=True, paper_db_path=paper_db)
    risk   = RiskManager(mc, balance_usdc=10_000.0)  # high cap — don't let balance block fills
    om     = OrderManager(config, clob, risk)

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

    results: list[dict] = []
    t0 = time.monotonic()

    for i, row in enumerate(all_rows, 1):
        result = simulate_token(
            row, om, clob, mc, str(positions_db),
            start_ts, end_ts,
            activation_delay=activation_delay,
            fill_fraction=fill_fraction,
        )
        results.append(result)

        if not summary_only and result["status"] == "ok" and result.get("filled_entries", 0) > 0:
            logger.info(
                f"[{i}/{len(all_rows)}] FILL {row['token_id'][:16]} "
                f"cat={row.get('category','')} "
                f"price={result['current_price']:.4f} "
                f"entries={result['filled_entries']} tps={result['filled_tps']} "
                f"winner={result['is_winner']}"
            )
        elif not summary_only and i % 500 == 0:
            passed = sum(1 for r in results if r["status"] == "ok")
            filled = sum(r.get("filled_entries", 0) for r in results if r["status"] == "ok")
            logger.info(f"  Progress {i}/{len(all_rows)} | passed_screener={passed} | fills={filled}")

    elapsed = time.monotonic() - t0
    logger.info(f"Replay finished in {elapsed:.1f}s  ({len(all_rows)/elapsed:.0f} tokens/s)")

    print_summary(results, str(positions_db), mode, start, end)
    print(f"  Positions DB : {positions_db}")
    print(f"  Paper trades : {paper_db}\n")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Honest full-universe replay — no look-ahead bias"
    )
    ap.add_argument("--start",  metavar="YYYY-MM-DD", required=True)
    ap.add_argument("--end",    metavar="YYYY-MM-DD", required=True)
    ap.add_argument("--mode",   default="big_swan_mode",
                    choices=["big_swan_mode", "balanced_mode", "fast_tp_mode", "dip_mode"])
    ap.add_argument("--limit",  type=int, default=None,
                    help="Max market/token pairs to process (default: all)")
    ap.add_argument("--out",    default=None,
                    help="Output dir (default: data/replay_runs/honest_<timestamp>)")
    ap.add_argument("--pessimistic", action="store_true",
                    help="activation_delay=300s, fill_fraction=0.3")
    ap.add_argument("--activation-delay", type=int, default=0)
    ap.add_argument("--fill-fraction",    type=float, default=1.0)
    ap.add_argument("--summary", action="store_true",
                    help="Only log fills (suppress per-token progress noise)")
    args = ap.parse_args()

    if args.out:
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
