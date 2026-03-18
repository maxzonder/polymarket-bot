"""
Validate whether semantic content signal adds predictive power — issue #12.

Fixes vs v1:
  1. Temporal integrity: neighbours restricted to markets closed before the
     candidate's entry_ts_first (no future leakage even within Dec-Feb window).
  2. avg_x column added to output (feature_mart.real_x).
  3. Permutation test for Q1 vs Q4 win-rate difference.
  4. Monotonicity check retained; verdict only closes hypothesis after
     leakage-free validation + significance gate.

Usage:
    python3 scripts/validate_content_signal.py
    python3 scripts/validate_content_signal.py --top-k 5 --permutations 2000
"""
from __future__ import annotations

import argparse
import random
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.paths import DB_PATH, DATA_DIR
from utils.logger import setup_logger

logger = setup_logger("validate_content_signal")

CHROMA_PATH = DATA_DIR / "chroma_db"
COLLECTION_NAME = "markets"
MONTHS = ("2025-12", "2026-01", "2026-02")

ENTRY_PRICE_MAX = 0.05
ENTRY_VOLUME_MIN = 50
ENTRY_TRADE_COUNT_MIN = 3
DURATION_HOURS_MIN = 24
VOLUME_1WK_MIN = 1000


def load_high_candidates(conn: sqlite3.Connection) -> list[dict]:
    month_placeholders = ",".join(f"'{m}'" for m in MONTHS)
    rows = conn.execute(
        f"""
        WITH ranked AS (
            SELECT ts.*,
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
            r.token_id, r.market_id, r.date,
            r.entry_ts_first,
            t.is_winner,
            fm.swan_score,
            fm.real_x,
            m.question, m.event_title, m.event_description, m.tags
        FROM ranked r
        JOIN markets m  ON r.market_id = m.id
        JOIN tokens  t  ON r.token_id  = t.token_id
        JOIN feature_mart fm ON r.token_id = fm.token_id AND r.date = fm.date
        WHERE r.rn = 1
          AND m.duration_hours >= :dh
          AND (m.volume_1wk IS NULL OR m.volume_1wk >= :vwk)
          AND fm.swan_score >= 7
        ORDER BY r.date ASC
        """,
        {
            "emax": ENTRY_PRICE_MAX,
            "evol": ENTRY_VOLUME_MIN,
            "etc":  ENTRY_TRADE_COUNT_MIN,
            "dh":   DURATION_HOURS_MIN,
            "vwk":  VOLUME_1WK_MIN,
        },
    ).fetchall()
    return [dict(r) for r in rows]


def load_market_closed_times(conn: sqlite3.Connection) -> dict[str, int]:
    """Return {market_id: closed_time_unix} for all markets."""
    rows = conn.execute(
        "SELECT id, closed_time FROM markets WHERE closed_time IS NOT NULL"
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def build_doc(row: dict) -> str:
    question    = row.get("question") or ""
    event_title = row.get("event_title") or ""
    event_desc  = (row.get("event_description") or "")[:200]
    tags        = row.get("tags") or ""
    parts = []
    if question:
        parts.append(question)
    if event_title and event_title != question:
        parts.append(event_title)
    if event_desc:
        parts.append(event_desc)
    if tags:
        parts.append(f"Tags: {tags}")
    return ". ".join(parts) if parts else question


def run_query(
    col,
    candidates: list[dict],
    closed_times: dict[str, int],
    top_k: int,
) -> list[dict]:
    """
    For each candidate, fetch top (top_k * 4) neighbours from ChromaDB, then
    filter to only markets whose closed_time < candidate.entry_ts_first
    (past-only, no leakage). Keep the closest top_k that survive the filter.

    Also record neighbour win-rate among those that appear in our candidate
    baseline (market_winner lookup).
    """
    market_winner: dict[str, bool] = {}
    for c in candidates:
        market_winner[c["market_id"]] = (
            market_winner.get(c["market_id"], False) or bool(c["is_winner"])
        )

    docs       = [build_doc(c) for c in candidates]
    market_ids = [c["market_id"] for c in candidates]

    results: list[dict] = []
    FETCH = top_k * 6   # over-fetch to have budget after temporal filter
    BATCH = 50

    for i in range(0, len(candidates), BATCH):
        batch_docs    = docs[i : i + BATCH]
        batch_markets = market_ids[i : i + BATCH]
        batch_cands   = candidates[i : i + BATCH]

        qr = col.query(
            query_texts=batch_docs,
            n_results=min(FETCH, col.count()),
            include=["metadatas", "distances"],
        )

        for j, cand in enumerate(batch_cands):
            entry_ts = cand["entry_ts_first"] or 0

            pairs = []
            for meta, dist in zip(qr["metadatas"][j], qr["distances"][j]):
                mid = meta["market_id"]
                if mid == batch_markets[j]:
                    continue   # exclude self
                ct = closed_times.get(mid, 0)
                if ct >= entry_ts:
                    continue   # future market — skip
                pairs.append((meta, dist))
                if len(pairs) == top_k:
                    break

            if not pairs:
                continue

            mean_dist      = sum(d for _, d in pairs) / len(pairs)
            n_in_baseline  = 0
            n_neighbours_w = 0
            for meta, _ in pairs:
                mid = meta["market_id"]
                if mid in market_winner:
                    n_in_baseline += 1
                    if market_winner[mid]:
                        n_neighbours_w += 1

            results.append({
                "token_id":    cand["token_id"],
                "market_id":   cand["market_id"],
                "date":        cand["date"],
                "is_winner":   cand["is_winner"],
                "real_x":      cand.get("real_x"),
                "swan_score":  cand["swan_score"],
                "mean_neighbour_dist":      mean_dist,
                "n_neighbours_in_baseline": n_in_baseline,
                "neighbour_winners":        n_neighbours_w,
                "neighbour_win_rate":       n_neighbours_w / n_in_baseline
                                            if n_in_baseline else None,
            })

    return results


def quartile_label(val: float, q1: float, q2: float, q3: float) -> str:
    if val <= q1:
        return "Q1 (closest)"
    if val <= q2:
        return "Q2"
    if val <= q3:
        return "Q3"
    return "Q4 (furthest)"


def permutation_test(
    results: list[dict],
    q1_boundary: float,
    q4_boundary: float,
    n_permutations: int,
) -> tuple[float, float]:
    """
    One-sided permutation test: is WinRate(Q1) - WinRate(Q4) significantly
    different from zero?
    Returns (observed_delta, p_value).
    """
    q1_items = [r for r in results if r["mean_neighbour_dist"] <= q1_boundary]
    q4_items = [r for r in results if r["mean_neighbour_dist"] > q4_boundary]

    if not q1_items or not q4_items:
        return 0.0, 1.0

    def win_rate(items: list[dict]) -> float:
        return sum(1 for r in items if r["is_winner"]) / len(items)

    observed = win_rate(q1_items) - win_rate(q4_items)
    combined = q1_items + q4_items
    n1 = len(q1_items)

    random.seed(42)
    count_ge = 0
    for _ in range(n_permutations):
        random.shuffle(combined)
        delta = win_rate(combined[:n1]) - win_rate(combined[n1:])
        if abs(delta) >= abs(observed):
            count_ge += 1

    p_value = count_ge / n_permutations
    return observed, p_value


def print_report(
    results: list[dict],
    top_k: int,
    elapsed: float,
    n_permutations: int,
) -> None:
    dists = sorted(r["mean_neighbour_dist"] for r in results)
    n = len(dists)
    q1 = dists[n // 4]
    q2 = dists[n // 2]
    q3 = dists[3 * n // 4]
    q4_boundary = q3   # Q4 = everything above q3

    cohorts: dict[str, dict] = {}
    for r in results:
        label = quartile_label(r["mean_neighbour_dist"], q1, q2, q3)
        c = cohorts.setdefault(
            label,
            {"n": 0, "winners": 0, "real_x_sum": 0.0, "real_x_n": 0,
             "nwr_sum": 0.0, "nwr_n": 0},
        )
        c["n"] += 1
        c["winners"] += 1 if r["is_winner"] else 0
        if r.get("real_x") is not None:
            c["real_x_sum"] += r["real_x"]
            c["real_x_n"] += 1
        if r["neighbour_win_rate"] is not None:
            c["nwr_sum"] += r["neighbour_win_rate"]
            c["nwr_n"] += 1

    sep = "=" * 78
    print(f"\n{sep}")
    print(f"  CONTENT SIGNAL VALIDATION (leakage-free)   top_k={top_k}")
    print(f"  High cohort (swan >= 7) | {' + '.join(MONTHS)}")
    print(sep)
    print(
        f"  {'Bucket':<18} {'N':>5} {'WinRate':>8} {'avg_x':>7} {'Nbr WinRate':>12}"
    )
    print(f"  {'-'*18} {'-'*5} {'-'*8} {'-'*7} {'-'*12}")

    wr_values = []
    order = ["Q1 (closest)", "Q2", "Q3", "Q4 (furthest)"]
    for label in order:
        c = cohorts.get(label)
        if not c or c["n"] == 0:
            continue
        wr  = 100.0 * c["winners"] / c["n"]
        avg_x = c["real_x_sum"] / c["real_x_n"] if c["real_x_n"] else float("nan")
        nwr = 100.0 * c["nwr_sum"] / c["nwr_n"] if c["nwr_n"] else float("nan")
        wr_values.append(wr)
        print(
            f"  {label:<18} {c['n']:>5} {wr:>7.1f}%  {avg_x:>6.1f}x  {nwr:>10.1f}%"
        )

    # Totals
    all_n   = sum(c["n"] for c in cohorts.values())
    all_w   = sum(c["winners"] for c in cohorts.values())
    all_rx  = sum(c["real_x_sum"] for c in cohorts.values())
    all_rxn = sum(c["real_x_n"] for c in cohorts.values())
    print(f"  {'-'*18} {'-'*5} {'-'*8} {'-'*7} {'-'*12}")
    print(
        f"  {'ALL':<18} {all_n:>5} {100*all_w/all_n:>7.1f}%  "
        f"{all_rx/all_rxn:>6.1f}x"
    )
    print(sep)

    # Permutation test Q1 vs Q4
    obs_delta, p_val = permutation_test(results, q1, q4_boundary, n_permutations)
    print(f"\n  Permutation test (Q1 vs Q4 win-rate delta, {n_permutations} shuffles):")
    print(f"    observed delta = {obs_delta*100:+.1f}pp   p = {p_val:.3f}")

    # Monotonicity check
    diffs = [wr_values[i+1] - wr_values[i] for i in range(len(wr_values) - 1)]
    monotonic = all(d >= 0 for d in diffs) or all(d <= 0 for d in diffs)
    spread = max(wr_values) - min(wr_values) if wr_values else 0

    # Verdict
    print()
    significant = p_val < 0.05
    pattern_str = " → ".join(f"{v:.1f}%" for v in wr_values)

    if not significant:
        verdict = (
            f"No significant content signal (p={p_val:.3f} > 0.05). "
            "Semantic similarity does not predict winner rate beyond swan_score. "
            "Content hypothesis closed."
        )
    elif not monotonic:
        verdict = (
            f"p={p_val:.3f} but pattern is NON-MONOTONIC ({pattern_str}). "
            f"{spread:.1f}pp spread with no consistent direction — noise. "
            "Content hypothesis closed."
        )
    else:
        verdict = (
            f"Significant monotonic separation (p={p_val:.3f}, {spread:.1f}pp). "
            "Content signal warrants further investigation."
        )

    print(f"  Verdict : {verdict}")
    print(f"\n  Computed in {elapsed:.0f}s | candidates={all_n}")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate content signal (issue #12 v2)")
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--permutations", type=int, default=1000)
    args = ap.parse_args()

    import chromadb

    if not CHROMA_PATH.exists():
        logger.error(f"ChromaDB not found at {CHROMA_PATH}. Run build_chroma.py first.")
        sys.exit(1)

    db_conn = sqlite3.connect(DB_PATH)
    db_conn.row_factory = sqlite3.Row

    logger.info("Loading candidates and market metadata …")
    candidates   = load_high_candidates(db_conn)
    closed_times = load_market_closed_times(db_conn)
    db_conn.close()
    logger.info(f"  {len(candidates)} high-score candidates | {len(closed_times):,} closed-time records")

    client = chromadb.PersistentClient(path=str(CHROMA_PATH))
    col    = client.get_collection(COLLECTION_NAME)
    logger.info(f"ChromaDB: {col.count():,} documents")

    t0 = time.monotonic()
    logger.info(f"Querying top-{args.top_k} past-only neighbours …")
    results = run_query(col, candidates, closed_times, top_k=args.top_k)
    elapsed = time.monotonic() - t0
    logger.info(f"  {len(results)} candidates with valid past neighbours")

    print_report(results, top_k=args.top_k, elapsed=elapsed,
                 n_permutations=args.permutations)


if __name__ == "__main__":
    main()
