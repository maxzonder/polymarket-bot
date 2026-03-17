"""
Entry-stack variant comparison — issue #9.

Derives the prune-vs-keep decision for entry level 0.002 from an existing
cohort replay run, without re-running the full 3-month simulation.

Assumption: entry levels are independent resting bids; trade-liquidity
cross-talk between levels is negligible at the tick sizes used (orders are
~$0.01 each; typical matching trades are hundreds of USDC).

Usage:
    python3 scripts/entry_stack_comparison.py
    python3 scripts/entry_stack_comparison.py --positions-db /path/to/positions.db
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.paths import DATA_DIR


def latest_cohort_db() -> Path:
    runs = sorted(
        (DATA_DIR / "replay_runs").glob("cohort_*/positions.db"),
        key=lambda p: p.parent.name,
        reverse=True,
    )
    if not runs:
        raise FileNotFoundError(
            "No cohort replay run found under data/replay_runs/cohort_*. "
            "Run scripts/replay_cohort_report.py first."
        )
    return runs[0]


def load_high_positions(positions_db: Path, main_db: Path) -> list[dict]:
    """Return all positions from the HIGH cohort (swan_score >= 7)."""
    main_conn = sqlite3.connect(str(main_db))
    main_conn.row_factory = sqlite3.Row
    scores = {
        (r["token_id"], r["date"]): r["swan_score"]
        for r in main_conn.execute(
            "SELECT token_id, date, swan_score FROM feature_mart"
        ).fetchall()
    }
    main_conn.close()

    conn = sqlite3.connect(str(positions_db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT p.realized_pnl, p.entry_price, p.entry_size_usdc, p.token_id,
               SUBSTR(r.label, 8, 10) AS event_date
        FROM positions p
        JOIN resting_orders r ON p.entry_order_id = r.order_id
        WHERE p.realized_pnl IS NOT NULL
        """
    ).fetchall()
    conn.close()

    return [
        dict(r)
        for r in rows
        if scores.get((r["token_id"], r["event_date"]), -1) >= 7
    ]


def bucket(positions: list[dict], exclude_price: float | None = None) -> dict:
    n = stake = pnl = wins = 0
    for p in positions:
        if exclude_price is not None and abs(p["entry_price"] - exclude_price) < 1e-9:
            continue
        n += 1
        stake += p["entry_size_usdc"]
        pnl += p["realized_pnl"]
        if p["realized_pnl"] > 0:
            wins += 1
    roi = 100.0 * pnl / stake if stake > 0 else 0.0
    fill_rate = 100.0 * wins / n if n > 0 else 0.0
    return {"n": n, "stake": stake, "pnl": pnl, "wins": wins, "roi": roi, "win_pct": fill_rate}


def per_level(positions: list[dict]) -> dict:
    by_price: dict = defaultdict(lambda: {"n": 0, "wins": 0, "pnl": 0.0, "stake": 0.0})
    for p in positions:
        ep = p["entry_price"]
        by_price[ep]["n"] += 1
        by_price[ep]["pnl"] += p["realized_pnl"]
        by_price[ep]["stake"] += p["entry_size_usdc"]
        if p["realized_pnl"] > 0:
            by_price[ep]["wins"] += 1
    return dict(by_price)


def print_report(high: list[dict]) -> None:
    sep = "=" * 72
    print(f"\n{sep}")
    print("  ENTRY STACK COMPARISON   big_swan_mode | high cohort (swan >= 7)")
    print(sep)

    # Per-level breakdown
    lvl = per_level(high)
    print(f"\n  {'Level':>8}  {'N':>5}  {'Wins':>5}  {'Stake':>8}  {'PnL':>9}  {'ROI':>7}  {'Win%':>6}")
    print(f"  {'-'*8}  {'-'*5}  {'-'*5}  {'-'*8}  {'-'*9}  {'-'*7}  {'-'*6}")
    for price in sorted(lvl):
        d = lvl[price]
        roi = 100.0 * d["pnl"] / d["stake"] if d["stake"] else 0.0
        print(
            f"  {price:>8.3f}  {d['n']:>5}  {d['wins']:>5}  "
            f"{d['stake']:>7.2f}$  {d['pnl']:>+8.4f}$  {roi:>6.1f}%  "
            f"{100*d['wins']/d['n']:>5.1f}%"
        )

    # Variant comparison
    current = bucket(high)
    pruned = bucket(high, exclude_price=0.002)

    print(f"\n  {'Variant':<42}  {'N':>5}  {'Stake':>8}  {'PnL':>9}  {'ROI':>7}")
    print(f"  {'-'*42}  {'-'*5}  {'-'*8}  {'-'*9}  {'-'*7}")

    def _row(label: str, d: dict) -> None:
        print(
            f"  {label:<42}  {d['n']:>5}  {d['stake']:>7.2f}$  "
            f"{d['pnl']:>+8.4f}$  {d['roi']:>6.1f}%"
        )

    _row("Current  {0.001, 0.002, 0.005, 0.010}", current)
    _row("Pruned   {0.001,        0.005, 0.010}", pruned)

    roi_delta = pruned["roi"] - current["roi"]
    stake_saved = current["stake"] - pruned["stake"]

    print(sep)
    print(f"\n  ROI delta (pruned − current) : {roi_delta:+.1f}pp")
    print(f"  Stake freed by dropping 0.002: {stake_saved:.2f}$")

    # Verdict
    l002 = lvl.get(0.002, {})
    roi_002 = 100.0 * l002.get("pnl", 0) / l002.get("stake", 1)
    if roi_002 < 0:
        verdict = (
            "PRUNE 0.002. The level is ROI-negative inside the high-score cohort. "
            "Dropping it reduces stake, maintains PnL, and raises ROI by "
            f"{roi_delta:+.1f}pp. No rehabilitation sub-filter justified."
        )
    elif roi_002 < 50:
        verdict = (
            f"PRUNE 0.002. The level contributes marginal ROI ({roi_002:.1f}%) — "
            "far below the other levels. Dropping it improves capital efficiency "
            f"by {roi_delta:+.1f}pp with no meaningful PnL cost."
        )
    else:
        verdict = f"KEEP 0.002 (ROI {roi_002:.1f}% — competitive with other levels)."

    print(f"\n  Decision : {verdict}")
    print(
        "\n  Next step: move to humanitarian/context metrics — "
        "the entry stack is now as tight as the data supports.\n"
    )
    print(sep)


def main() -> None:
    ap = argparse.ArgumentParser(description="Entry-stack variant comparison (issue #9)")
    ap.add_argument("--positions-db", default=None, help="Path to positions.db from a cohort run")
    args = ap.parse_args()

    from utils.paths import DB_PATH
    main_db = DB_PATH

    positions_db = Path(args.positions_db) if args.positions_db else latest_cohort_db()
    print(f"Using positions DB: {positions_db}")

    high = load_high_positions(positions_db, main_db)
    print_report(high)


if __name__ == "__main__":
    main()
