"""
Crypto observer daily report — prints gap analysis for last N hours.

Usage:
    python -m v2.reports.crypto_daily [--hours N]
"""
from __future__ import annotations

import argparse
import time

from ..db import conn_crypto
from ..utils.paths import CRYPTO_DB


def report(hours: int = 24) -> None:
    if not CRYPTO_DB.exists():
        print("obs_crypto.db not found — run observer first")
        return

    conn = conn_crypto()
    since = int(time.time()) - hours * 3600

    row = conn.execute(
        """SELECT COUNT(*) n_snaps, COUNT(DISTINCT market_id) n_markets
           FROM cr_snapshots WHERE ts >= ? AND in_garbage_time=0""",
        (since,),
    ).fetchone()

    parse_row = conn.execute(
        "SELECT COUNT(*) total, SUM(parse_ok) parsed FROM cr_markets"
    ).fetchone()

    print(f"\n=== Crypto Observer — last {hours}h ===")
    print(f"Markets in DB   : {parse_row['total']} ({parse_row['parsed']} parsed)")
    print(f"Markets observed: {row['n_markets']}")
    print(f"Snapshots taken : {row['n_snaps']} (excl. garbage time)")

    # Biggest gaps (model vs polymarket)
    print(f"\nTop 15 gaps |model - poly| (last {hours}h, excl. garbage time):")
    rows = conn.execute(
        """SELECT m.question, m.asset, m.threshold, m.direction,
                  s.polymarket_price, s.model_price, s.gap, s.tte_hours,
                  s.spot_price, s.vol_estimate
           FROM cr_snapshots s
           JOIN cr_markets m ON m.market_id = s.market_id
           WHERE s.ts >= ? AND s.in_garbage_time = 0
             AND s.gap IS NOT NULL
           ORDER BY ABS(s.gap) DESC
           LIMIT 15""",
        (since,),
    ).fetchall()

    if not rows:
        print("  (no data)")
    else:
        for r in rows:
            q = (r["question"] or "")[:55]
            print(
                f"  [{r['asset'] or '?':>3}] {q:<55} "
                f"poly={r['polymarket_price']:.3f} "
                f"model={r['model_price']:.3f} "
                f"gap={r['gap']:+.3f} "
                f"tte={r['tte_hours']:.0f}h"
            )

    # Asset breakdown
    print(f"\nGap distribution by asset (last {hours}h, non-garbage):")
    asset_rows = conn.execute(
        """SELECT m.asset,
                  COUNT(*) n,
                  AVG(ABS(s.gap)) avg_abs_gap,
                  MAX(ABS(s.gap)) max_abs_gap,
                  AVG(CASE WHEN s.gap > 0.05 THEN 1.0 ELSE 0.0 END) pct_model_above,
                  AVG(CASE WHEN s.gap < -0.05 THEN 1.0 ELSE 0.0 END) pct_poly_above
           FROM cr_snapshots s
           JOIN cr_markets m ON m.market_id = s.market_id
           WHERE s.ts >= ? AND s.in_garbage_time = 0 AND s.gap IS NOT NULL
           GROUP BY m.asset""",
        (since,),
    ).fetchall()

    for r in asset_rows:
        print(
            f"  {r['asset'] or 'unknown':>3}: n={r['n']:>4} "
            f"avg_abs_gap={r['avg_abs_gap']:.3f} "
            f"max={r['max_abs_gap']:.3f} "
            f"model_above={100*r['pct_model_above']:.0f}% "
            f"poly_above={100*r['pct_poly_above']:.0f}%"
        )

    # Resolution accuracy (if any resolved)
    res_row = conn.execute(
        """SELECT COUNT(*) total,
                  SUM(was_directionally_correct) correct
           FROM cr_resolved"""
    ).fetchone()
    if res_row["total"]:
        acc = 100 * res_row["correct"] / res_row["total"] if res_row["total"] else 0
        print(f"\nResolution accuracy: {acc:.0f}% ({res_row['correct']}/{res_row['total']} directionally correct)")

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Crypto daily report")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    args = parser.parse_args()
    report(args.hours)


if __name__ == "__main__":
    main()
