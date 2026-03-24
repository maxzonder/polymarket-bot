"""
Neg-risk daily report.

Usage:
    python -m v2.reports.negrisk_daily [--hours N] [--min-size S]

--min-size : minimum sum of best_ask_size across legs to count as liquid (default 5 USDC)
             groups below this threshold are flagged as stale/illiquid
"""
from __future__ import annotations

import argparse
import time

from ..db import conn_negrisk
from ..utils.paths import NEGRISK_DB

# Sports fee at p=0.33 per leg × 3 legs ≈ 0.66%; round up
SPORTS_FEE_EST = 0.008   # ~0.8% conservative basket fee estimate


def report(hours: int = 24, min_size: float = 5.0) -> None:
    if not NEGRISK_DB.exists():
        print("obs_negrisk.db not found — run observer first")
        return

    conn = conn_negrisk()
    since = int(time.time()) - hours * 3600

    # ── Overview ──────────────────────────────────────────────────────────────
    row = conn.execute(
        """SELECT COUNT(*) n_snaps, COUNT(DISTINCT group_id) n_groups
           FROM nr_snapshots WHERE ts >= ?""",
        (since,),
    ).fetchone()
    print(f"\n=== Neg-Risk Observer — last {hours}h (liquidity floor: ${min_size:.0f}) ===")
    print(f"Groups observed : {row['n_groups']}")
    print(f"Snapshots taken : {row['n_snaps']}")

    # ── Episode summary ───────────────────────────────────────────────────────
    dis_row = conn.execute(
        "SELECT COUNT(*) n_open, MIN(min_sum_ask) deepest FROM nr_dislocations WHERE end_ts IS NULL"
    ).fetchone()
    closed_row = conn.execute(
        """SELECT COUNT(*) n_closed, AVG(max_gap) avg_gap, MAX(max_gap) max_gap,
                  AVG((end_ts - start_ts)/60.0) avg_dur_min, MAX((end_ts - start_ts)/60.0) max_dur_min
           FROM nr_dislocations WHERE start_ts >= ? AND end_ts IS NOT NULL""",
        (since,),
    ).fetchone()

    print(f"\nDislocation episodes:")
    print(f"  Open now        : {dis_row['n_open']}")
    if dis_row["deepest"] is not None:
        print(f"  Deepest sum_ask : {dis_row['deepest']:.4f}  (gap={1 - dis_row['deepest']:.4f})")
    print(f"  Closed (window) : {closed_row['n_closed']}")
    if closed_row["avg_dur_min"] is not None:
        print(f"  Avg duration    : {closed_row['avg_dur_min']:.0f}m  (max {closed_row['max_dur_min']:.0f}m)")
        print(f"  Avg/best gap    : {closed_row['avg_gap']:.4f} / {closed_row['max_gap']:.4f}")

    # ── Open episodes — liquid vs stale ───────────────────────────────────────
    open_eps = conn.execute(
        """SELECT g.event_slug, g.n_markets, d.start_ts, d.min_sum_ask, d.max_gap,
                  d.n_snapshots, d.id as ep_id
           FROM nr_dislocations d
           JOIN nr_groups g ON g.id = d.group_id
           WHERE d.end_ts IS NULL
           ORDER BY d.max_gap DESC""",
    ).fetchall()

    if open_eps:
        liquid = []
        stale = []
        for ep in open_eps:
            # Get latest snapshot's leg liquidity
            liq = conn.execute(
                """SELECT COALESCE(SUM(l.best_ask_size), 0) total_size, COUNT(*) n_legs
                   FROM nr_legs l
                   JOIN nr_snapshots s ON s.id = l.snapshot_id
                   WHERE s.group_id = (
                       SELECT group_id FROM nr_dislocations WHERE id=?
                   )
                   AND s.id = (
                       SELECT id FROM nr_snapshots
                       WHERE group_id=(SELECT group_id FROM nr_dislocations WHERE id=?)
                       ORDER BY ts DESC LIMIT 1
                   )""",
                (ep["ep_id"], ep["ep_id"]),
            ).fetchone()
            total_size = liq["total_size"] if liq else 0
            if total_size >= min_size:
                liquid.append((ep, total_size))
            else:
                stale.append((ep, total_size))

        age_col = lambda ep: (time.time() - ep["start_ts"]) / 60

        if liquid:
            print(f"\nLIQUID open dislocations ({len(liquid)}) — potentially actionable:")
            print(f"  {'Slug':<42} {'Legs':>4} {'Gap':>6} {'Net':>6} {'Size$':>7} {'Age':>6} {'Snaps':>5}")
            print(f"  {'-'*80}")
            for ep, sz in liquid:
                net = ep["max_gap"] - SPORTS_FEE_EST
                print(
                    f"  {ep['event_slug'][:41]:<42} "
                    f"{ep['n_markets']:>4} "
                    f"{ep['max_gap']:>5.3f} "
                    f"{net:>+5.3f} "
                    f"${sz:>6.1f} "
                    f"{age_col(ep):>5.0f}m "
                    f"{ep['n_snapshots']:>5}"
                )

        if stale:
            print(f"\nSTALE open dislocations ({len(stale)}) — illiquid, ignore:")
            for ep, sz in stale:
                print(
                    f"  {ep['event_slug'][:42]:<42} "
                    f"gap={ep['max_gap']:.3f} "
                    f"size=${sz:.1f} "
                    f"age={age_col(ep):.0f}m"
                )

    # ── Persistence analysis ──────────────────────────────────────────────────
    print(f"\nPersistence analysis (closed episodes, last {hours}h):")
    buckets = conn.execute(
        """SELECT
             CASE
               WHEN (end_ts - start_ts) < 300   THEN '< 5m'
               WHEN (end_ts - start_ts) < 900   THEN '5-15m'
               WHEN (end_ts - start_ts) < 1800  THEN '15-30m'
               WHEN (end_ts - start_ts) < 3600  THEN '30-60m'
               ELSE '> 60m'
             END dur_bucket,
             COUNT(*) n,
             AVG(max_gap) avg_gap
           FROM nr_dislocations
           WHERE start_ts >= ? AND end_ts IS NOT NULL
           GROUP BY dur_bucket
           ORDER BY MIN(end_ts - start_ts)""",
        (since,),
    ).fetchall()
    if buckets:
        for b in buckets:
            print(f"  {b['dur_bucket']:>8}  count={b['n']}  avg_gap={b['avg_gap']:.4f}")
    else:
        print("  (no closed episodes yet)")

    # ── Top groups by dislocation rate ────────────────────────────────────────
    print(f"\nTop groups by dislocation rate (last {hours}h):")
    rows = conn.execute(
        """SELECT g.event_slug, g.n_markets,
                  COUNT(*) n_snaps,
                  SUM(s.is_dislocated) n_dis,
                  MIN(s.sum_best_ask) min_ask,
                  MAX(s.sum_best_ask) max_ask
           FROM nr_snapshots s
           JOIN nr_groups g ON g.id = s.group_id
           WHERE s.ts >= ? AND s.is_dislocated = 1
           GROUP BY g.id
           HAVING n_dis >= 2
           ORDER BY n_dis DESC, min_ask ASC
           LIMIT 20""",
        (since,),
    ).fetchall()

    if not rows:
        print("  (no groups with ≥2 dislocated snapshots)")
    else:
        print(f"  {'Slug':<38} {'L':>3} {'Dis':>4} {'MinAsk':>7} {'MaxAsk':>7}")
        print(f"  {'-'*65}")
        for r in rows:
            print(
                f"  {(r['event_slug'] or '')[:37]:<38} "
                f"{r['n_markets']:>3} "
                f"{r['n_dis']:>4} "
                f"{r['min_ask']:>7.4f} "
                f"{r['max_ask']:>7.4f}"
            )

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Neg-risk daily report")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--min-size", type=float, default=5.0,
                        help="Min sum(best_ask_size) across legs to count as liquid (USDC)")
    args = parser.parse_args()
    report(args.hours, args.min_size)


if __name__ == "__main__":
    main()
