"""
Crypto observer daily report.

Usage:
    python -m v2.reports.crypto_daily [--hours N] [--min-tte H] [--max-poly P]

--min-tte  : minimum hours to expiry to include (default 4h, filters near-expiry noise)
--max-poly : maximum polymarket price to include (default 0.90, filters near-resolved noise)
"""
from __future__ import annotations

import argparse
import time

from ..db import conn_crypto
from ..utils.paths import CRYPTO_DB


def report(hours: int = 24, min_tte: float = 4.0, max_poly: float = 0.90) -> None:
    if not CRYPTO_DB.exists():
        print("obs_crypto.db not found — run observer first")
        return

    conn = conn_crypto()
    since = int(time.time()) - hours * 3600

    parse_row = conn.execute(
        "SELECT COUNT(*) total, SUM(parse_ok) parsed FROM cr_markets"
    ).fetchone()
    snap_row = conn.execute(
        """SELECT COUNT(*) n_snaps, COUNT(DISTINCT market_id) n_markets
           FROM cr_snapshots WHERE ts >= ? AND in_garbage_time=0""",
        (since,),
    ).fetchone()

    print(f"\n=== Crypto Observer — last {hours}h (tte≥{min_tte}h, poly≤{max_poly}) ===")
    print(f"Markets in DB     : {parse_row['total']} ({parse_row['parsed']} parsed, "
          f"{100*parse_row['parsed']/max(parse_row['total'],1):.1f}%)")
    print(f"Markets observed  : {snap_row['n_markets']}")
    print(f"Snapshots (no GT) : {snap_row['n_snaps']}")

    # ── Top gaps per MARKET (deduplicated — max |gap| per market) ─────────────
    print(f"\nTop gaps per market (max |gap|, tte≥{min_tte}h, poly≤{max_poly}):")
    rows = conn.execute(
        """SELECT m.question, m.asset, m.threshold, m.direction,
                  AVG(s.polymarket_price) avg_poly,
                  AVG(s.model_price) avg_model,
                  MAX(ABS(s.gap)) max_abs_gap,
                  AVG(s.gap) avg_gap,
                  AVG(s.tte_hours) avg_tte,
                  COUNT(*) n_snaps
           FROM cr_snapshots s
           JOIN cr_markets m ON m.market_id = s.market_id
           WHERE s.ts >= ?
             AND s.in_garbage_time = 0
             AND s.gap IS NOT NULL
             AND s.tte_hours >= ?
             AND s.polymarket_price <= ?
           GROUP BY m.market_id
           ORDER BY max_abs_gap DESC
           LIMIT 20""",
        (since, min_tte, max_poly),
    ).fetchall()

    if not rows:
        print("  (no data in this range)")
    else:
        print(f"  {'Question':<52} {'Asset':>5} {'AvgPoly':>8} {'AvgModel':>9} {'AvgGap':>7} {'AvgTTE':>7} {'N':>4}")
        print(f"  {'-'*100}")
        for r in rows:
            q = (r["question"] or "")[:51]
            direction_arrow = "↑" if r["direction"] == "above" else "↓"
            thresh = f"${r['threshold']:,.0f}" if r["threshold"] else "?"
            label = f"{r['asset'] or '?'}{direction_arrow}{thresh}"
            print(
                f"  {q:<52} "
                f"{label:>8} "
                f"{r['avg_poly']:>8.3f} "
                f"{r['avg_model']:>9.3f} "
                f"{r['avg_gap']:>+7.3f} "
                f"{r['avg_tte']:>6.0f}h "
                f"{r['n_snaps']:>4}"
            )

    # ── Gap distribution by asset (filtered) ──────────────────────────────────
    print(f"\nGap distribution by asset (filtered, last {hours}h):")
    asset_rows = conn.execute(
        """SELECT m.asset,
                  COUNT(DISTINCT m.market_id) n_markets,
                  COUNT(*) n_snaps,
                  AVG(s.gap) avg_gap,
                  AVG(ABS(s.gap)) avg_abs_gap,
                  AVG(CASE WHEN s.gap < -0.05 THEN 1.0 ELSE 0.0 END) pct_poly_above,
                  AVG(CASE WHEN s.gap > 0.05  THEN 1.0 ELSE 0.0 END) pct_model_above,
                  AVG(CASE WHEN ABS(s.gap) < 0.05 THEN 1.0 ELSE 0.0 END) pct_aligned
           FROM cr_snapshots s
           JOIN cr_markets m ON m.market_id = s.market_id
           WHERE s.ts >= ? AND s.in_garbage_time = 0 AND s.gap IS NOT NULL
             AND s.tte_hours >= ? AND s.polymarket_price <= ?
           GROUP BY m.asset""",
        (since, min_tte, max_poly),
    ).fetchall()

    for r in asset_rows:
        print(
            f"  {r['asset'] or '?':>3}: "
            f"markets={r['n_markets']:>3}  snaps={r['n_snaps']:>5}  "
            f"avg_gap={r['avg_gap']:>+6.3f}  avg|gap|={r['avg_abs_gap']:.3f}  "
            f"poly↑={100*r['pct_poly_above']:>4.0f}%  model↑={100*r['pct_model_above']:>4.0f}%  "
            f"aligned={100*r['pct_aligned']:>4.0f}%"
        )

    # ── Resolution accuracy — full + segmented ────────────────────────────────
    res_total = conn.execute(
        """SELECT COUNT(*) total, SUM(was_directionally_correct) correct
           FROM cr_resolved""",
    ).fetchone()

    if res_total["total"]:
        acc = 100 * res_total["correct"] / res_total["total"]
        print(f"\nResolution accuracy (all): {acc:.0f}%  ({res_total['correct']}/{res_total['total']})")

        # Segmented by |gap| at last snapshot
        print("\nAccuracy by |gap| at resolution:")
        seg_rows = conn.execute(
            """SELECT
                 CASE
                   WHEN ABS(last_gap) < 0.10 THEN '|gap| < 0.10'
                   WHEN ABS(last_gap) < 0.20 THEN '0.10–0.20'
                   WHEN ABS(last_gap) < 0.40 THEN '0.20–0.40'
                   ELSE '> 0.40'
                 END gap_bucket,
                 COUNT(*) n,
                 SUM(was_directionally_correct) correct,
                 AVG(last_polymarket_price) avg_poly,
                 AVG(ABS(last_gap)) avg_abs_gap
               FROM cr_resolved
               WHERE was_directionally_correct IS NOT NULL
               GROUP BY gap_bucket
               ORDER BY MIN(ABS(last_gap))""",
        ).fetchall()
        print(f"  {'|gap| range':<14} {'N':>5} {'Acc':>6} {'AvgPoly':>8} {'Avg|gap|':>9}")
        print(f"  {'-'*46}")
        for s in seg_rows:
            acc_s = 100 * s["correct"] / s["n"] if s["n"] else 0
            print(
                f"  {s['gap_bucket']:<14} "
                f"{s['n']:>5} "
                f"{acc_s:>5.0f}% "
                f"{s['avg_poly']:>8.3f} "
                f"{s['avg_abs_gap']:>9.3f}"
            )

        # Segmented by tte at resolution
        print("\nAccuracy by tte at last snapshot before resolution:")
        tte_rows = conn.execute(
            """SELECT
                 CASE
                   WHEN r.last_gap IS NULL THEN 'no snapshot'
                   WHEN (SELECT tte_hours FROM cr_snapshots
                         WHERE market_id=r.market_id AND ts <= m.resolved_ts
                         ORDER BY ts DESC LIMIT 1) < 24  THEN 'tte < 24h'
                   WHEN (SELECT tte_hours FROM cr_snapshots
                         WHERE market_id=r.market_id AND ts <= m.resolved_ts
                         ORDER BY ts DESC LIMIT 1) < 168 THEN '1d–7d'
                   ELSE '> 7d'
                 END tte_bucket,
                 COUNT(*) n,
                 SUM(r.was_directionally_correct) correct
               FROM cr_resolved r
               JOIN cr_markets m ON m.market_id = r.market_id
               WHERE r.was_directionally_correct IS NOT NULL
               GROUP BY tte_bucket""",
        ).fetchall()
        print(f"  {'tte range':<14} {'N':>5} {'Acc':>6}")
        print(f"  {'-'*28}")
        for t in tte_rows:
            acc_t = 100 * t["correct"] / t["n"] if t["n"] else 0
            print(f"  {t['tte_bucket']:<14} {t['n']:>5} {acc_t:>5.0f}%")

        # Segmented by asset
        print("\nAccuracy by asset:")
        asset_acc = conn.execute(
            """SELECT m.asset, COUNT(*) n, SUM(r.was_directionally_correct) correct
               FROM cr_resolved r
               JOIN cr_markets m ON m.market_id = r.market_id
               WHERE r.was_directionally_correct IS NOT NULL
               GROUP BY m.asset""",
        ).fetchall()
        for a in asset_acc:
            acc_a = 100 * a["correct"] / a["n"] if a["n"] else 0
            print(f"  {a['asset'] or '?':>3}: {acc_a:.0f}%  (n={a['n']})")

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Crypto daily report")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--min-tte", type=float, default=4.0,
                        help="Min hours to expiry (filters near-expiry noise)")
    parser.add_argument("--max-poly", type=float, default=0.90,
                        help="Max polymarket price (filters near-resolved noise)")
    args = parser.parse_args()
    report(args.hours, args.min_tte, args.max_poly)


if __name__ == "__main__":
    main()
