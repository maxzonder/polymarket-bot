"""
Neg-risk daily report — prints a summary of the last 24h of observations.

Usage:
    python -m v2.reports.negrisk_daily [--hours N]
"""
from __future__ import annotations

import argparse
import sys
import time

from ..db import conn_negrisk
from ..utils.paths import NEGRISK_DB


def report(hours: int = 24) -> None:
    if not NEGRISK_DB.exists():
        print("obs_negrisk.db not found — run observer first")
        return

    conn = conn_negrisk()
    since = int(time.time()) - hours * 3600

    # Overall snapshot stats
    row = conn.execute(
        """SELECT COUNT(*) n_snaps, COUNT(DISTINCT group_id) n_groups
           FROM nr_snapshots WHERE ts >= ?""",
        (since,),
    ).fetchone()
    print(f"\n=== Neg-Risk Observer — last {hours}h ===")
    print(f"Groups observed : {row['n_groups']}")
    print(f"Snapshots taken : {row['n_snaps']}")

    # Dislocation summary
    dis_row = conn.execute(
        """SELECT COUNT(*) n_open, MIN(min_sum_ask) deepest
           FROM nr_dislocations WHERE end_ts IS NULL""",
    ).fetchone()
    closed_row = conn.execute(
        """SELECT COUNT(*) n_closed, AVG(max_gap) avg_gap, MAX(max_gap) max_gap
           FROM nr_dislocations WHERE start_ts >= ? AND end_ts IS NOT NULL""",
        (since,),
    ).fetchone()

    print(f"\nOpen dislocations   : {dis_row['n_open']}")
    if dis_row["deepest"] is not None:
        print(f"  Deepest sum_ask   : {dis_row['deepest']:.4f}  (gap={1 - dis_row['deepest']:.4f})")
    print(f"Closed (last {hours}h)  : {closed_row['n_closed']}")
    if closed_row["avg_gap"] is not None:
        print(f"  Avg max gap       : {closed_row['avg_gap']:.4f}")
        print(f"  Best max gap      : {closed_row['max_gap']:.4f}")

    # Top groups by dislocation frequency
    print(f"\nTop groups by dislocation snapshots (last {hours}h):")
    rows = conn.execute(
        """SELECT g.event_slug, g.event_title, g.n_markets,
                  COUNT(*) n_snaps,
                  SUM(s.is_dislocated) n_dis,
                  MIN(s.sum_best_ask) min_ask
           FROM nr_snapshots s
           JOIN nr_groups g ON g.id = s.group_id
           WHERE s.ts >= ?
           GROUP BY g.id
           ORDER BY n_dis DESC, n_snaps DESC
           LIMIT 15""",
        (since,),
    ).fetchall()

    if not rows:
        print("  (no data)")
    else:
        header = f"{'Slug':<35} {'Legs':>4} {'Snaps':>5} {'Dis%':>5} {'MinAsk':>7}"
        print("  " + header)
        print("  " + "-" * len(header))
        for r in rows:
            dis_pct = 100 * r["n_dis"] / r["n_snaps"] if r["n_snaps"] else 0
            min_ask = f"{r['min_ask']:.4f}" if r["min_ask"] is not None else "  n/a"
            slug = (r["event_slug"] or "")[:34]
            print(f"  {slug:<35} {r['n_markets']:>4} {r['n_snaps']:>5} {dis_pct:>4.0f}% {min_ask:>7}")

    # Current open dislocations detail
    open_eps = conn.execute(
        """SELECT g.event_slug, g.n_markets, d.start_ts, d.min_sum_ask, d.max_gap, d.n_snapshots
           FROM nr_dislocations d
           JOIN nr_groups g ON g.id = d.group_id
           WHERE d.end_ts IS NULL
           ORDER BY d.max_gap DESC""",
    ).fetchall()

    if open_eps:
        print(f"\nCurrently open dislocation episodes ({len(open_eps)}):")
        for ep in open_eps:
            age_min = (time.time() - ep["start_ts"]) / 60
            print(
                f"  {ep['event_slug'][:40]:<40} "
                f"legs={ep['n_markets']} "
                f"sum_ask={ep['min_sum_ask']:.4f} "
                f"gap={ep['max_gap']:.4f} "
                f"age={age_min:.0f}m "
                f"snaps={ep['n_snapshots']}"
            )

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Neg-risk daily report")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    args = parser.parse_args()
    report(args.hours)


if __name__ == "__main__":
    main()
