"""
Recalibrate Scorers — reads ml_outcomes + ml_rejected_outcomes, outputs recommended config.

Analyzes per-category precision/recall across both accepted and rejected markets:
  - Accepted markets (ml_outcomes): fill_rate, winner_rate, realized_roi
  - Rejected markets (ml_rejected_outcomes): miss_rate (missed swan opportunities)

Outputs recommended_config.json — a diff against current config thresholds:
  - min_entry_fill_score per category (lower if missing too many swans)
  - min_resolution_score per category (raise if low winner rate)
  - category_weights update (boost categories with high tail_ev, penalize low ones)

Usage:
    python scripts/recalibrate_scorers.py
    python scripts/recalibrate_scorers.py --summary
    python scripts/recalibrate_scorers.py --output /path/to/recommended_config.json
    python scripts/recalibrate_scorers.py \\
        --positions-db /path/to/positions.db \\
        --dataset-db   /path/to/polymarket_dataset.db

Recommendation logic:
  - Category miss_rate > 5%  → lower min_entry_fill_score for that category
  - Category winner_rate < 5% with n>=10 fills → raise min_resolution_score
  - Category avg_tail_ev high (>5x) → boost category_weight
  - Minimum 20 labeled rows needed for a recommendation to fire
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, asdict
from typing import Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from utils.paths import DATA_DIR, DB_PATH

logger = setup_logger("recalibrate_scorers")

POSITIONS_DB = DATA_DIR / "positions.db"
OUTPUT_PATH  = DATA_DIR / "recommended_config.json"

# Minimum rows needed before we trust the stats enough to recommend changes
MIN_REJECTED_ROWS = 20
MIN_FILLED_ROWS   = 10

# Thresholds that trigger recommendations
MISS_RATE_HIGH    = 0.05   # > 5% missed opportunities → relax fill gate
WINNER_RATE_LOW   = 0.05   # < 5% winners despite fills → tighten res gate
TAIL_EV_HIGH      = 5.0    # avg_x > 5 for winners → boost category weight
TAIL_EV_LOW       = 2.0    # avg_x < 2 for winners → penalize category weight


@dataclass
class CategoryStats:
    category: str
    # Rejected side
    total_rejected: int = 0
    had_swan: int = 0
    missed_opportunity: int = 0
    miss_rate: float = 0.0
    avg_ef_score: float = 0.0
    avg_res_score: float = 0.0
    # Accepted / filled side
    total_accepted: int = 0
    got_fill: int = 0
    fill_rate: float = 0.0
    winners: int = 0
    winner_rate: float = 0.0
    avg_realized_roi: Optional[float] = None
    avg_tail_ev: float = 0.0


def load_rejected_stats(dataset_db_path: str) -> dict[str, CategoryStats]:
    """Load per-category stats from ml_rejected_outcomes."""
    conn = sqlite3.connect(dataset_db_path)
    conn.row_factory = sqlite3.Row

    try:
        rows = conn.execute("""
            SELECT
                COALESCE(category, 'unknown') AS category,
                COUNT(*)                      AS total,
                SUM(had_swan_event)           AS had_swan,
                SUM(was_missed_opportunity)   AS missed,
                AVG(ef_score)                 AS avg_ef,
                AVG(res_score)                AS avg_res
            FROM ml_rejected_outcomes
            GROUP BY category
        """).fetchall()
    except sqlite3.OperationalError:
        logger.warning("ml_rejected_outcomes not found — skipping rejected stats")
        conn.close()
        return {}

    conn.close()

    stats: dict[str, CategoryStats] = {}
    for r in rows:
        cat = r["category"]
        total = int(r["total"] or 0)
        missed = int(r["missed"] or 0)
        s = CategoryStats(category=cat)
        s.total_rejected = total
        s.had_swan = int(r["had_swan"] or 0)
        s.missed_opportunity = missed
        s.miss_rate = missed / total if total > 0 else 0.0
        s.avg_ef_score = float(r["avg_ef"] or 0)
        s.avg_res_score = float(r["avg_res"] or 0)
        stats[cat] = s
    return stats


def load_accepted_stats(ops_db_path: str) -> dict[str, CategoryStats]:
    """Load per-category stats from ml_outcomes."""
    conn = sqlite3.connect(ops_db_path)
    conn.row_factory = sqlite3.Row

    try:
        rows = conn.execute("""
            SELECT
                COALESCE(category, 'unknown') AS category,
                COUNT(*)                      AS total,
                SUM(got_fill)                 AS fills,
                SUM(CASE WHEN is_winner=1 THEN 1 ELSE 0 END) AS winners,
                AVG(CASE WHEN got_fill=1 AND realized_roi IS NOT NULL
                         THEN realized_roi END)  AS avg_roi,
                AVG(CASE WHEN got_fill=1 AND realized_roi IS NOT NULL
                         THEN realized_roi END)  AS avg_tail_ev
            FROM ml_outcomes
            GROUP BY category
        """).fetchall()
    except sqlite3.OperationalError:
        logger.warning("ml_outcomes not found in positions.db — skipping accepted stats")
        conn.close()
        return {}

    conn.close()

    stats: dict[str, CategoryStats] = {}
    for r in rows:
        cat = r["category"]
        total = int(r["total"] or 0)
        fills = int(r["fills"] or 0)
        s = CategoryStats(category=cat)
        s.total_accepted = total
        s.got_fill = fills
        s.fill_rate = fills / total if total > 0 else 0.0
        s.winners = int(r["winners"] or 0)
        s.winner_rate = s.winners / fills if fills > 0 else 0.0
        s.avg_realized_roi = float(r["avg_roi"]) if r["avg_roi"] is not None else None
        s.avg_tail_ev = float(r["avg_tail_ev"] or 0)
        stats[cat] = s
    return stats


def merge_stats(
    rejected: dict[str, CategoryStats],
    accepted: dict[str, CategoryStats],
) -> dict[str, CategoryStats]:
    all_cats = set(rejected) | set(accepted)
    merged: dict[str, CategoryStats] = {}
    for cat in all_cats:
        s = rejected.get(cat) or CategoryStats(category=cat)
        a = accepted.get(cat)
        if a:
            s.total_accepted  = a.total_accepted
            s.got_fill        = a.got_fill
            s.fill_rate       = a.fill_rate
            s.winners         = a.winners
            s.winner_rate     = a.winner_rate
            s.avg_realized_roi = a.avg_realized_roi
            s.avg_tail_ev     = a.avg_tail_ev
        merged[cat] = s
    return merged


def build_recommendations(
    stats: dict[str, CategoryStats],
    current_min_ef: float = 0.02,
    current_min_res: float = 0.15,
    current_weights: Optional[dict] = None,
) -> dict:
    """
    Produce a recommendations dict with per-category threshold adjustments.
    Only fires when sufficient data exists.
    """
    current_weights = current_weights or {}
    recommendations: dict = {
        "generated_at": int(time.time()),
        "data_quality": {},
        "threshold_changes": [],
        "category_weight_changes": [],
        "recommended_config": {
            "min_entry_fill_score": current_min_ef,
            "min_resolution_score": current_min_res,
            "category_weights": dict(current_weights),
        },
        "notes": [],
    }

    for cat, s in sorted(stats.items()):
        quality: dict = {
            "rejected": s.total_rejected,
            "accepted": s.total_accepted,
            "fills": s.got_fill,
            "miss_rate": round(s.miss_rate, 4),
            "winner_rate": round(s.winner_rate, 4),
        }
        recommendations["data_quality"][cat] = quality

        # ── Rejected-side: relax fill gate if missing too many swans ──────────
        if s.total_rejected >= MIN_REJECTED_ROWS and s.miss_rate > MISS_RATE_HIGH:
            new_ef = max(current_min_ef * 0.7, 0.005)  # lower by 30%, floor 0.005
            recommendations["threshold_changes"].append({
                "category": cat,
                "param": "min_entry_fill_score",
                "direction": "lower",
                "reason": f"miss_rate={s.miss_rate:.1%} > {MISS_RATE_HIGH:.0%} threshold "
                           f"(n={s.total_rejected} rejected, {s.missed_opportunity} missed)",
                "current": current_min_ef,
                "suggested": round(new_ef, 4),
            })

        # ── Accepted-side: tighten res gate if fills aren't winning ───────────
        if s.got_fill >= MIN_FILLED_ROWS and s.winner_rate < WINNER_RATE_LOW:
            new_res = min(current_min_res * 1.3, 0.5)  # raise by 30%, cap 0.5
            recommendations["threshold_changes"].append({
                "category": cat,
                "param": "min_resolution_score",
                "direction": "raise",
                "reason": f"winner_rate={s.winner_rate:.1%} < {WINNER_RATE_LOW:.0%} threshold "
                           f"(fills={s.got_fill}, winners={s.winners})",
                "current": current_min_res,
                "suggested": round(new_res, 4),
            })

        # ── Category weight adjustments based on tail_ev ─────────────────────
        if s.got_fill >= MIN_FILLED_ROWS:
            current_w = current_weights.get(cat, 1.0)
            if s.avg_tail_ev > TAIL_EV_HIGH:
                new_w = min(current_w * 1.2, 2.0)
                recommendations["category_weight_changes"].append({
                    "category": cat,
                    "reason": f"avg_tail_ev={s.avg_tail_ev:.2f} > {TAIL_EV_HIGH} — boost",
                    "current": current_w,
                    "suggested": round(new_w, 3),
                })
                recommendations["recommended_config"]["category_weights"][cat] = round(new_w, 3)
            elif s.avg_tail_ev < TAIL_EV_LOW and s.avg_tail_ev > 0:
                new_w = max(current_w * 0.85, 0.5)
                recommendations["category_weight_changes"].append({
                    "category": cat,
                    "reason": f"avg_tail_ev={s.avg_tail_ev:.2f} < {TAIL_EV_LOW} — penalize",
                    "current": current_w,
                    "suggested": round(new_w, 3),
                })
                recommendations["recommended_config"]["category_weights"][cat] = round(new_w, 3)

    if not recommendations["threshold_changes"] and not recommendations["category_weight_changes"]:
        recommendations["notes"].append(
            "No changes recommended — either thresholds are well-calibrated "
            "or insufficient data (need ≥20 rejected / ≥10 filled rows per category)."
        )

    return recommendations


def print_summary(stats: dict[str, CategoryStats], recs: dict) -> None:
    print(f"\n{'─'*64}")
    print("  Scorer Recalibration Report")
    print(f"{'─'*64}")
    print(f"  {'Category':<16} {'Rej':>6} {'Miss%':>7} {'Fills':>6} {'Win%':>7} {'ROI':>8}")
    print(f"  {'─'*60}")
    for cat, s in sorted(stats.items(), key=lambda x: -x[1].total_rejected):
        roi_str = f"{s.avg_realized_roi:.2f}" if s.avg_realized_roi is not None else "  N/A"
        print(
            f"  {cat:<16} {s.total_rejected:>6} {s.miss_rate:>6.1%} "
            f"{s.got_fill:>6} {s.winner_rate:>6.1%} {roi_str:>8}"
        )
    print()

    changes = recs.get("threshold_changes", [])
    if changes:
        print(f"  Threshold changes ({len(changes)}):")
        for c in changes:
            arrow = "↓" if c["direction"] == "lower" else "↑"
            print(f"    [{c['category']}] {c['param']} {arrow} "
                  f"{c['current']} → {c['suggested']}")
            print(f"      {c['reason']}")
        print()

    wchanges = recs.get("category_weight_changes", [])
    if wchanges:
        print(f"  Category weight changes ({len(wchanges)}):")
        for c in wchanges:
            print(f"    [{c['category']}] {c['current']} → {c['suggested']}  ({c['reason']})")
        print()

    for note in recs.get("notes", []):
        print(f"  Note: {note}")
    print(f"{'─'*64}\n")


def recalibrate(
    ops_db_path: str,
    dataset_db_path: str,
    output_path: str,
) -> dict:
    rejected = load_rejected_stats(dataset_db_path)
    accepted = load_accepted_stats(ops_db_path)
    stats = merge_stats(rejected, accepted)

    # Pull current config defaults
    try:
        from config import BIG_SWAN_MODE, BotConfig
        cfg = BotConfig()
        current_min_ef  = BIG_SWAN_MODE.min_entry_fill_score
        current_min_res = BIG_SWAN_MODE.min_resolution_score
        current_weights = dict(cfg.category_weights)
    except Exception as e:
        logger.warning(f"Could not load config defaults: {e}; using hardcoded fallbacks")
        current_min_ef  = 0.02
        current_min_res = 0.15
        current_weights = {}

    recs = build_recommendations(stats, current_min_ef, current_min_res, current_weights)

    with open(output_path, "w") as f:
        json.dump(recs, f, indent=2)
    logger.info(f"Recommended config written to {output_path}")

    return {"stats": stats, "recommendations": recs}


def main() -> None:
    ap = argparse.ArgumentParser(description="Recalibrate scorer thresholds from ML data")
    ap.add_argument("--positions-db", default=str(POSITIONS_DB))
    ap.add_argument("--dataset-db",   default=str(DB_PATH))
    ap.add_argument("--output",       default=str(OUTPUT_PATH))
    ap.add_argument("--summary",      action="store_true")
    args = ap.parse_args()

    result = recalibrate(args.positions_db, args.dataset_db, args.output)
    if args.summary or True:  # always print summary
        print_summary(result["stats"], result["recommendations"])


if __name__ == "__main__":
    main()
