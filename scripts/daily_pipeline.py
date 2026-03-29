"""
Daily ML Pipeline v1.1 — orchestrates all data collection and labeling steps.

Run this once per day (e.g. via cron at 04:00 UTC) to:
  1. Ingest new market data (download + parse) from last collected date up to yesterday
  2. Run swan_analyzer on any newly closed markets
  3. Rebuild feature_mart_v1_1 (market-level features for MarketScorer, via analyzer/)
  4. Materialize ml_outcomes (accepted candidates → fill/resolution labels)
  5. Materialize ml_rejected_outcomes (rejected markets → missed opportunity labels)
  6. Recalibrate scorer thresholds → recommended_config.json
  7. Print unified summary report

The trading bot runs independently and is NOT touched by this pipeline.

Usage:
    python scripts/daily_pipeline.py
    python scripts/daily_pipeline.py --summary
    python scripts/daily_pipeline.py --step ingest               # single step
    python scripts/daily_pipeline.py --step analyzer
    python scripts/daily_pipeline.py --step feature_mart_v1_1
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
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from utils.paths import DATA_DIR, DATABASE_DIR, DB_PATH

logger = setup_logger("daily_pipeline")

POSITIONS_DB  = DATA_DIR / "positions.db"
OUTPUT_CONFIG = DATA_DIR / "recommended_config.json"

SCRIPTS_DIR       = Path(__file__).parent
ANALYZER_DIR      = SCRIPTS_DIR.parent / "analyzer"
DATA_COLLECTOR    = SCRIPTS_DIR.parent / "data_collector" / "data_collector_and_parsing.py"


# ─── Step runner ─────────────────────────────────────────────────────────────

def _run(label: str, cmd: list[str], timeout: Optional[int] = 600) -> bool:
    """Run a subprocess step. Returns True on success."""
    logger.info(f"[{label}] starting: {' '.join(cmd)}")
    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
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

def _find_last_collected_date() -> Optional[date]:
    """Return the most recent date directory in DATABASE_DIR, or None if empty."""
    if not DATABASE_DIR.exists():
        return None
    candidates = []
    for entry in DATABASE_DIR.iterdir():
        if entry.is_dir():
            try:
                candidates.append(date.fromisoformat(entry.name))
            except ValueError:
                pass
    return max(candidates) if candidates else None


def _compute_ingest_range() -> Optional[tuple[date, date]]:
    """
    Return (start, end) of dates to ingest, or None if already up to date / no data.
    start = last date dir + 1 day, end = yesterday.
    """
    yesterday = date.today() - timedelta(days=1)
    last_date = _find_last_collected_date()

    if last_date is None:
        return None

    start = last_date + timedelta(days=1)
    if start > yesterday:
        return None

    return start, yesterday


def step_ingest(dataset_db: str, ingest_range: Optional[tuple[date, date]] = None) -> bool:
    """Download and parse market data from last collected date up to yesterday."""
    if ingest_range is None:
        ingest_range = _compute_ingest_range()

    if ingest_range is None:
        if _find_last_collected_date() is None:
            logger.warning("[ingest] No existing date directories found in database/ — skipping auto-ingest. "
                           "Run data_collector_and_parsing.py manually with --start/--end first.")
        else:
            logger.info(f"[ingest] Already up to date (yesterday={date.today() - timedelta(days=1)}) — nothing to fetch")
        return True

    start, end = ingest_range
    logger.info(f"[ingest] Catching up: {start} → {end}")
    return _run("ingest", [
        sys.executable,
        str(DATA_COLLECTOR),
        "--start", start.isoformat(),
        "--end",   end.isoformat(),
    ], timeout=None)


def step_analyzer(dataset_db: str, date_from: Optional[str] = None, date_to: Optional[str] = None) -> bool:
    """Run swan_analyzer for the given date range (or all dates if unspecified)."""
    cmd = [sys.executable, str(ANALYZER_DIR / "swan_analyzer.py")]
    if date_from and date_to:
        cmd += ["--date-from", date_from, "--date-to", date_to]
    return _run("analyzer", cmd)


def step_feature_mart_v1_1(dataset_db: str) -> bool:
    """Rebuild feature_mart_v1_1 (market-level features for MarketScorer — v1.1)."""
    return _run("feature_mart_v1_1", [
        sys.executable,
        str(ANALYZER_DIR / "market_level_features_v1_1.py"),
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
    "ingest":              step_ingest,
    "analyzer":            step_analyzer,
    "feature_mart_v1_1":   step_feature_mart_v1_1,
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
        if only_step in ("ingest", "analyzer", "feature_mart_v1_1"):
            ok = fn(dataset_db)
        elif only_step == "ml_outcomes":
            ok = fn(positions_db)
        else:
            ok = fn(positions_db, dataset_db)
        results[only_step] = ok
    else:
        # Full pipeline — order matters
        # Compute ingest range once so analyzer uses the exact same window
        ingest_range = _compute_ingest_range()
        results["ingest"] = step_ingest(dataset_db, ingest_range)

        if ingest_range is None:
            # Nothing new — skip data-dependent steps to avoid redundant work
            logger.info("No new data ingested — skipping analyzer, feature_mart_v1_1")
        else:
            date_from = ingest_range[0].isoformat()
            date_to   = ingest_range[1].isoformat()
            results["analyzer"]          = step_analyzer(dataset_db, date_from, date_to)
            results["feature_mart_v1_1"] = step_feature_mart_v1_1(dataset_db)

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
