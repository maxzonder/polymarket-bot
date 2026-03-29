"""
Daily ML Pipeline v1.1 — orchestrates all data collection and labeling steps.

Run this once per day (e.g. via cron at 04:00 UTC) to:
  1. Run swan_analyzer on any newly closed markets
  2. Rebuild feature_mart_v1_1 (market-level features for MarketScorer, via analyzer/)  ← NEW v1.1
  3. Rebuild feature_mart legacy (token-level, for EntryFillScorer/ResolutionScorer denominators)
  4. Materialize ml_outcomes (accepted candidates → fill/resolution labels)
  5. Materialize ml_rejected_outcomes (rejected markets → missed opportunity labels)
  6. Recalibrate scorer thresholds → recommended_config.json
  7. Print unified summary report

The trading bot runs independently and is NOT touched by this pipeline.

Usage:
    python scripts/daily_pipeline.py
    python scripts/daily_pipeline.py --summary
    python scripts/daily_pipeline.py --step analyzer            # single step
    python scripts/daily_pipeline.py --step feature_mart_v1_1
    python scripts/daily_pipeline.py --step feature_mart
    python scripts/daily_pipeline.py --step ml_outcomes
    python scripts/daily_pipeline.py --step rejected_outcomes
    python scripts/daily_pipeline.py --step recalibrate
    python scripts/daily_pipeline.py \\
        --positions-db /path/to/positions.db \\
        --dataset-db   /path/to/polymarket_dataset.db

Exit codes:
    0 — all steps succeeded
    1 — one or more steps failed (check logs)
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from utils.paths import DATA_DIR, DB_PATH

logger = setup_logger("daily_pipeline")

POSITIONS_DB  = DATA_DIR / "positions.db"
OUTPUT_CONFIG = DATA_DIR / "recommended_config.json"

SCRIPTS_DIR = Path(__file__).parent
ANALYZER_DIR = SCRIPTS_DIR.parent / "analyzer"


# ─── Step runner ─────────────────────────────────────────────────────────────

def _run(label: str, cmd: list[str]) -> bool:
    """Run a subprocess step. Returns True on success."""
    logger.info(f"[{label}] starting: {' '.join(cmd)}")
    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
        )
        elapsed = time.time() - t0
        if result.returncode == 0:
            logger.info(f"[{label}] OK ({elapsed:.1f}s)")
            if result.stdout.strip():
                for line in result.stdout.strip().splitlines():
                    logger.info(f"  {line}")
            return True
        else:
            logger.error(f"[{label}] FAILED (exit {result.returncode}, {elapsed:.1f}s)")
            if result.stderr.strip():
                for line in result.stderr.strip().splitlines()[-20:]:
                    logger.error(f"  {line}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"[{label}] TIMEOUT after 600s")
        return False
    except Exception as e:
        logger.error(f"[{label}] ERROR: {e}")
        return False


# ─── Individual steps ─────────────────────────────────────────────────────────

def step_analyzer(dataset_db: str) -> bool:
    """Run swan_analyzer incrementally (new closed markets only)."""
    return _run("analyzer", [
        sys.executable,
        str(ANALYZER_DIR / "swan_analyzer.py"),
        # incremental: no --recompute flag; analyzer skips already-processed markets
    ])


def step_feature_mart_v1_1(dataset_db: str) -> bool:
    """Rebuild feature_mart_v1_1 (market-level features for MarketScorer — v1.1)."""
    return _run("feature_mart_v1_1", [
        sys.executable,
        str(ANALYZER_DIR / "market_level_features_v1_1.py"),
        "--recompute",
    ])


def step_feature_mart(dataset_db: str) -> bool:
    """Rebuild feature_mart legacy (token-level, for EntryFillScorer/ResolutionScorer)."""
    return _run("feature_mart", [
        sys.executable,
        str(SCRIPTS_DIR / "build_feature_mart.py"),
        "--recompute",
    ])


def step_ml_outcomes(positions_db: str) -> bool:
    """Materialize ml_outcomes from accepted candidates."""
    return _run("ml_outcomes", [
        sys.executable,
        str(SCRIPTS_DIR / "build_ml_outcomes.py"),
        "--db", positions_db,
        "--summary",
    ])


def step_rejected_outcomes(positions_db: str, dataset_db: str) -> bool:
    """Materialize ml_rejected_outcomes (post-hoc labels for rejected markets)."""
    return _run("rejected_outcomes", [
        sys.executable,
        str(SCRIPTS_DIR / "build_rejected_outcomes.py"),
        "--positions-db", positions_db,
        "--dataset-db",   dataset_db,
        "--summary",
    ])


def step_recalibrate(positions_db: str, dataset_db: str) -> bool:
    """Recalibrate scorer thresholds → recommended_config.json."""
    return _run("recalibrate", [
        sys.executable,
        str(SCRIPTS_DIR / "recalibrate_scorers.py"),
        "--positions-db", positions_db,
        "--dataset-db",   dataset_db,
        "--output",       str(OUTPUT_CONFIG),
        "--summary",
    ])


# ─── Summary ──────────────────────────────────────────────────────────────────

def print_pipeline_summary(positions_db: str, dataset_db: str) -> None:
    print(f"\n{'═'*60}")
    print("  Daily Pipeline Summary")
    print(f"{'═'*60}")

    # ml_outcomes
    try:
        conn = sqlite3.connect(positions_db)
        row = conn.execute("""
            SELECT COUNT(*) AS n, SUM(got_fill) AS fills,
                   SUM(CASE WHEN is_winner=1 THEN 1 ELSE 0 END) AS winners
            FROM ml_outcomes
        """).fetchone()
        conn.close()
        print(f"  ml_outcomes:          {row[0]} rows  | fills={row[1]}  winners={row[2]}")
    except Exception as e:
        print(f"  ml_outcomes:          (error: {e})")

    # ml_rejected_outcomes
    try:
        conn = sqlite3.connect(dataset_db)
        row = conn.execute("""
            SELECT COUNT(*) AS n, SUM(had_swan_event) AS swans,
                   SUM(was_missed_opportunity) AS missed
            FROM ml_rejected_outcomes
        """).fetchone()
        conn.close()
        print(f"  ml_rejected_outcomes: {row[0]} rows  | had_swan={row[1]}  missed={row[2]}")
    except Exception as e:
        print(f"  ml_rejected_outcomes: (error: {e})")

    # recommended_config.json
    if OUTPUT_CONFIG.exists():
        import json
        try:
            with open(OUTPUT_CONFIG) as f:
                recs = json.load(f)
            n_thresh = len(recs.get("threshold_changes", []))
            n_weight = len(recs.get("category_weight_changes", []))
            ts = recs.get("generated_at", 0)
            print(f"  recommended_config:   {n_thresh} threshold changes, "
                  f"{n_weight} weight changes  (generated at {ts})")
        except Exception as e:
            print(f"  recommended_config:   (parse error: {e})")
    else:
        print(f"  recommended_config:   not yet generated")

    print(f"{'═'*60}\n")


# ─── Orchestrator ─────────────────────────────────────────────────────────────

STEPS = {
    "analyzer":            step_analyzer,
    "feature_mart_v1_1":   step_feature_mart_v1_1,
    "feature_mart":        step_feature_mart,
    "ml_outcomes":         step_ml_outcomes,
    "rejected_outcomes":   step_rejected_outcomes,
    "recalibrate":         step_recalibrate,
}


def run_pipeline(
    positions_db: str,
    dataset_db: str,
    only_step: Optional[str] = None,
    print_summary: bool = True,
) -> int:
    """
    Run all pipeline steps (or just one).
    Returns 0 on success, 1 if any step failed.
    """
    t0 = time.time()
    results: dict[str, bool] = {}

    if only_step:
        if only_step not in STEPS:
            logger.error(f"Unknown step: {only_step!r}. Choose from: {list(STEPS)}")
            return 1
        fn = STEPS[only_step]
        if only_step in ("analyzer", "feature_mart_v1_1", "feature_mart"):
            ok = fn(dataset_db)
        elif only_step == "ml_outcomes":
            ok = fn(positions_db)
        else:
            ok = fn(positions_db, dataset_db)
        results[only_step] = ok
    else:
        # Full pipeline — order matters
        results["analyzer"]          = step_analyzer(dataset_db)
        results["feature_mart_v1_1"] = step_feature_mart_v1_1(dataset_db)
        results["feature_mart"]      = step_feature_mart(dataset_db)
        results["ml_outcomes"]       = step_ml_outcomes(positions_db)
        results["rejected_outcomes"] = step_rejected_outcomes(positions_db, dataset_db)
        results["recalibrate"]       = step_recalibrate(positions_db, dataset_db)

    elapsed = time.time() - t0
    failed  = [k for k, v in results.items() if not v]
    passed  = [k for k, v in results.items() if v]

    logger.info(
        f"Pipeline done in {elapsed:.1f}s — "
        f"{len(passed)}/{len(results)} steps passed"
        + (f" | FAILED: {failed}" if failed else "")
    )

    if print_summary:
        print_pipeline_summary(positions_db, dataset_db)

    return 0 if not failed else 1


def main() -> None:
    ap = argparse.ArgumentParser(description="Daily ML pipeline orchestrator")
    ap.add_argument("--positions-db", default=str(POSITIONS_DB))
    ap.add_argument("--dataset-db",   default=str(DB_PATH))
    ap.add_argument("--step",         default=None,
                    choices=list(STEPS),
                    help="Run only this step (default: all)")
    ap.add_argument("--summary",      action="store_true",
                    help="Print pipeline summary (always printed by default)")
    args = ap.parse_args()

    rc = run_pipeline(
        positions_db=args.positions_db,
        dataset_db=args.dataset_db,
        only_step=args.step,
        print_summary=True,
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
