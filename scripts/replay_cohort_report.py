"""
Cohort economic readout — issue #8.

Runs the dry-run replay over the full Dec 2025 + Jan 2026 + Feb 2026 baseline
universe and breaks results down by swan_score cohort (high/mid/low) to test
whether score-based filtering creates meaningful economic separation.

Metrics reported per cohort:
  - candidates        : unique (token_id, date) signal events
  - fill_rate         : % of candidates that got at least one entry fill
  - winner_rate       : % of candidates whose token resolved as winner
  - tp_hit_rate       : TP fills / entry fills
  - total stake       : USDC deployed
  - realized PnL      : USDC returned (moonbag + TP)
  - ROI               : PnL / stake %

Usage:
    python3 scripts/replay_cohort_report.py
    python3 scripts/replay_cohort_report.py --months 2026-02   # one month only
    python3 scripts/replay_cohort_report.py --limit 300        # quick smoke-test
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import execution.order_manager as om_module

from api.clob_client import ClobClient
from config import BotConfig
from scripts.run_dry_run_replay import (
    ENTRY_PRICE_MAX, ENTRY_VOLUME_MIN, ENTRY_TRADE_COUNT_MIN,
    DURATION_HOURS_MIN, VOLUME_1WK_MIN,
    load_trades, _insert_resting_order, replay_candidate,
)
from strategy.risk_manager import RiskManager
from utils.logger import setup_logger
from utils.paths import DATA_DIR, DB_PATH

logger = setup_logger("cohort_report")

MONTHS = ("2025-12", "2026-01", "2026-02")


def load_scored_candidates(
    conn: sqlite3.Connection,
    months: tuple[str, ...],
    limit: Optional[int],
) -> list[dict]:
    """
    Load baseline candidates with swan_score from feature_mart.
    Uses the same ROW_NUMBER dedup as run_dry_run_replay.
    Candidates without a score (NULL) are included with score=None.
    """
    month_placeholders = ",".join(f"'{m}'" for m in months)
    rows = conn.execute(
        f"""
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
              AND substr(ts.date, 1, 7) IN ({month_placeholders})
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
            t.outcome_name,
            fm.swan_score,
            fm.price_score,
            fm.neglect_score,
            fm.freshness_score
        FROM ranked r
        JOIN markets m ON r.market_id = m.id
        JOIN tokens  t ON r.token_id  = t.token_id
        LEFT JOIN feature_mart fm ON r.token_id = fm.token_id AND r.date = fm.date
        WHERE r.rn = 1
          AND m.duration_hours     >= :dh
          AND (m.volume_1wk IS NULL OR m.volume_1wk >= :vwk)
        ORDER BY r.entry_ts_first ASC
        """,
        {
            "emax": ENTRY_PRICE_MAX,
            "evol": ENTRY_VOLUME_MIN,
            "etc": ENTRY_TRADE_COUNT_MIN,
            "dh": DURATION_HOURS_MIN,
            "vwk": VOLUME_1WK_MIN,
        },
    ).fetchall()

    if limit:
        rows = rows[:limit]
    return [dict(r) for r in rows]


def cohort_label(swan_score: Optional[int]) -> str:
    if swan_score is None:
        return "unscored"
    if swan_score >= 7:
        return "high  (7-9)"
    if swan_score >= 4:
        return "mid   (4-6)"
    return "low   (0-3)"


def run(months: tuple[str, ...], limit: Optional[int], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    positions_db = output_dir / "positions.db"
    paper_db = output_dir / "paper_trades.db"

    om_module.POSITIONS_DB = positions_db

    from execution.order_manager import OrderManager

    config = BotConfig(mode="big_swan_mode", dry_run=True)
    mc = config.mode_config
    clob = ClobClient(private_key="replay_dummy", dry_run=True, paper_db_path=paper_db)
    risk = RiskManager(mc, balance_usdc=100.0)
    om = OrderManager(config, clob, risk)

    db_conn = sqlite3.connect(DB_PATH)
    db_conn.row_factory = sqlite3.Row
    candidates = load_scored_candidates(db_conn, months, limit)
    db_conn.close()

    logger.info(
        f"Cohort replay: {len(candidates)} candidates | months={months}"
        + (f" [limit={limit}]" if limit else "")
    )

    results: list[dict] = []
    t0 = time.monotonic()

    for i, candidate in enumerate(candidates, 1):
        r = replay_candidate(candidate, om, clob, mc, str(positions_db))
        r["swan_score"] = candidate.get("swan_score")
        r["token_id"] = candidate["token_id"]
        results.append(r)
        if i % 200 == 0:
            logger.info(f"  {i}/{len(candidates)} processed ({time.monotonic()-t0:.0f}s)")

    elapsed = time.monotonic() - t0
    logger.info(f"Replay done in {elapsed:.0f}s")

    # Pull per-token PnL from positions.db
    pnl_conn = sqlite3.connect(str(positions_db))
    pnl_conn.row_factory = sqlite3.Row
    pnl_rows = pnl_conn.execute(
        "SELECT token_id, SUM(realized_pnl) AS pnl, SUM(entry_size_usdc) AS stake "
        "FROM positions GROUP BY token_id"
    ).fetchall()
    pnl_conn.close()
    pnl_by_token = {r["token_id"]: (float(r["pnl"] or 0), float(r["stake"] or 0))
                    for r in pnl_rows}

    # Aggregate by cohort
    cohorts: dict[str, dict] = {}
    for r in results:
        label = cohort_label(r.get("swan_score"))
        c = cohorts.setdefault(label, {
            "n": 0, "filled": 0, "winners": 0,
            "total_entries": 0, "total_tps": 0,
            "stake": 0.0, "pnl": 0.0,
        })
        c["n"] += 1
        c["winners"] += 1 if r.get("is_winner") else 0
        if r.get("filled_entries", 0) > 0:
            c["filled"] += 1
        c["total_entries"] += r.get("filled_entries", 0)
        c["total_tps"] += r.get("filled_tps", 0)
        token_pnl, token_stake = pnl_by_token.get(r.get("token_id", ""), (0.0, 0.0))
        c["pnl"] += token_pnl
        c["stake"] += token_stake

    print_report(cohorts, elapsed, len(candidates), months, positions_db)


def print_report(
    cohorts: dict,
    elapsed: float,
    total: int,
    months: tuple,
    positions_db: Path,
) -> None:
    order = ["high  (7-9)", "mid   (4-6)", "low   (0-3)", "unscored"]

    sep = "=" * 72
    print(f"\n{sep}")
    print(f"  COHORT ECONOMIC READOUT   big_swan_mode | {' + '.join(months)}")
    print(sep)
    print(f"  {'Cohort':<14} {'N':>5} {'FillRate':>9} {'WinRate':>8} "
          f"{'TP/Entry':>9} {'Stake':>8} {'PnL':>9} {'ROI':>7}")
    print(f"  {'-'*14} {'-'*5} {'-'*9} {'-'*8} {'-'*9} {'-'*8} {'-'*9} {'-'*7}")

    for label in order:
        c = cohorts.get(label)
        if not c or c["n"] == 0:
            continue
        fill_rate = 100.0 * c["filled"] / c["n"]
        win_rate = 100.0 * c["winners"] / c["n"]
        tp_per_entry = c["total_tps"] / c["total_entries"] if c["total_entries"] else 0.0
        roi = 100.0 * c["pnl"] / c["stake"] if c["stake"] > 0 else 0.0
        print(
            f"  {label:<14} {c['n']:>5} {fill_rate:>8.1f}% {win_rate:>7.1f}% "
            f"{tp_per_entry:>8.2f}x {c['stake']:>7.2f}$ {c['pnl']:>8.4f}$ {roi:>6.1f}%"
        )

    # Totals
    all_n = sum(c["n"] for c in cohorts.values())
    all_filled = sum(c["filled"] for c in cohorts.values())
    all_win = sum(c["winners"] for c in cohorts.values())
    all_entries = sum(c["total_entries"] for c in cohorts.values())
    all_tps = sum(c["total_tps"] for c in cohorts.values())
    all_stake = sum(c["stake"] for c in cohorts.values())
    all_pnl = sum(c["pnl"] for c in cohorts.values())
    all_roi = 100.0 * all_pnl / all_stake if all_stake > 0 else 0.0
    all_tp_per = all_tps / all_entries if all_entries else 0.0

    print(f"  {'-'*14} {'-'*5} {'-'*9} {'-'*8} {'-'*9} {'-'*8} {'-'*9} {'-'*7}")
    print(
        f"  {'ALL':<14} {all_n:>5} {100*all_filled/all_n:>8.1f}% "
        f"{100*all_win/all_n:>7.1f}% {all_tp_per:>8.2f}x "
        f"{all_stake:>7.2f}$ {all_pnl:>8.4f}$ {all_roi:>6.1f}%"
    )
    print(sep)
    print(f"\n  Replay time  : {elapsed:.0f}s  |  Positions DB: {positions_db}")

    # Narrative verdict
    high = cohorts.get("high  (7-9)", {})
    low = cohorts.get("low   (0-3)", {})
    if high and low and high["stake"] > 0 and low["stake"] > 0:
        high_roi = 100.0 * high["pnl"] / high["stake"]
        low_roi = 100.0 * low["pnl"] / low["stake"]
        delta = high_roi - low_roi
        print(f"\n  High vs Low ROI delta : {delta:+.1f}pp")
        if delta > 20:
            verdict = "Score separates cohorts — high-score universe shows meaningful edge over low."
        elif delta > 0:
            verdict = "Score separates cohorts weakly — directionally right but margin is thin."
        else:
            verdict = "Score does NOT rescue economics — high and low cohorts are economically flat or inverted."
        print(f"  Verdict               : {verdict}")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="Cohort economic readout (issue #8)")
    ap.add_argument("--months", default=None,
                    help="Comma-separated months e.g. 2026-01,2026-02 (default: all three)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Max candidates (for quick smoke-test)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    months = tuple(args.months.split(",")) if args.months else MONTHS

    if args.out:
        output_dir = Path(args.out)
    else:
        ts = time.strftime("%Y%m%d_%H%M%S")
        output_dir = DATA_DIR / "replay_runs" / f"cohort_{ts}"

    run(months=months, limit=args.limit, output_dir=output_dir)


if __name__ == "__main__":
    main()
