"""
Validate whether semantic content signal adds predictive power — issue #12.

For each high-score (swan_score >= 7) candidate in the Dec+Jan+Feb baseline:
  - Query ChromaDB for the top-10 most semantically similar markets
  - Measure whether those neighbours had more/less profitable swans
  - Compare win rates across similarity-score quartiles

Output: one table — does content neighbourhood predict winner rate beyond
what swan_score alone already captures?

Usage:
    python3 scripts/validate_content_signal.py
    python3 scripts/validate_content_signal.py --top-k 5
"""
from __future__ import annotations

import argparse
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

# Baseline gates (same as replay)
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
            t.is_winner,
            fm.swan_score,
            m.question, m.event_title, m.event_description, m.tags
        FROM ranked r
        JOIN markets m  ON r.market_id = m.id
        JOIN tokens  t  ON r.token_id  = t.token_id
        JOIN feature_mart fm ON r.token_id = fm.token_id AND r.date = fm.date
        LEFT JOIN markets m2 ON r.market_id = m2.id
        WHERE r.rn = 1
          AND m.duration_hours     >= :dh
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


def neighbour_win_rate(
    col,
    candidates: list[dict],
    top_k: int,
) -> list[dict]:
    """
    For each candidate, query ChromaDB for top_k nearest neighbours
    (excluding itself). Returns a list of dicts with:
        token_id, market_id, date, is_winner, swan_score,
        mean_neighbour_distance, neighbour_win_rate
    """
    # Build a lookup: market_id → is_winner aggregated over all tokens
    # (a market is "swan-positive" if any token resolved as winner in our baseline)
    market_winner: dict[str, bool] = {}
    for c in candidates:
        market_winner[c["market_id"]] = market_winner.get(c["market_id"], False) or bool(c["is_winner"])

    docs   = [build_doc(c) for c in candidates]
    market_ids = [c["market_id"] for c in candidates]

    results = []
    BATCH = 100

    for i in range(0, len(candidates), BATCH):
        batch_docs = docs[i : i + BATCH]
        batch_markets = market_ids[i : i + BATCH]
        batch_cands = candidates[i : i + BATCH]

        qr = col.query(
            query_texts=batch_docs,
            n_results=top_k + 1,   # +1 because the market itself may appear
            include=["metadatas", "distances"],
        )

        for j, cand in enumerate(batch_cands):
            neighbour_meta = qr["metadatas"][j]
            neighbour_dist = qr["distances"][j]

            # Drop self
            pairs = [
                (m, d)
                for m, d in zip(neighbour_meta, neighbour_dist)
                if m["market_id"] != batch_markets[j]
            ][:top_k]

            if not pairs:
                continue

            # Among neighbours that also appear in baseline high-score universe,
            # count how many resolved as winners
            n_in_baseline = 0
            n_winners     = 0
            for meta, _ in pairs:
                mid = meta["market_id"]
                if mid in market_winner:
                    n_in_baseline += 1
                    if market_winner[mid]:
                        n_winners += 1

            mean_dist = sum(d for _, d in pairs) / len(pairs)

            results.append({
                "token_id":    cand["token_id"],
                "market_id":   cand["market_id"],
                "date":        cand["date"],
                "is_winner":   cand["is_winner"],
                "swan_score":  cand["swan_score"],
                "mean_neighbour_dist": mean_dist,
                "n_neighbours_in_baseline": n_in_baseline,
                "neighbour_winners": n_winners,
                "neighbour_win_rate": n_winners / n_in_baseline if n_in_baseline else None,
            })

    return results


def quartile_label(val: float, q1: float, q2: float, q3: float) -> str:
    """Distance quartile: Q1 = closest (most similar), Q4 = furthest."""
    if val <= q1:
        return "Q1 (closest)"
    if val <= q2:
        return "Q2"
    if val <= q3:
        return "Q3"
    return "Q4 (furthest)"


def print_report(results: list[dict], top_k: int, elapsed: float) -> None:
    dists = sorted(r["mean_neighbour_dist"] for r in results)
    n = len(dists)
    q1 = dists[n // 4]
    q2 = dists[n // 2]
    q3 = dists[3 * n // 4]

    cohorts: dict[str, dict] = {}
    for r in results:
        label = quartile_label(r["mean_neighbour_dist"], q1, q2, q3)
        c = cohorts.setdefault(label, {"n": 0, "winners": 0, "nwr_sum": 0.0, "nwr_n": 0})
        c["n"] += 1
        c["winners"] += 1 if r["is_winner"] else 0
        if r["neighbour_win_rate"] is not None:
            c["nwr_sum"] += r["neighbour_win_rate"]
            c["nwr_n"] += 1

    sep = "=" * 72
    print(f"\n{sep}")
    print(f"  CONTENT SIGNAL VALIDATION   top_k={top_k} | high cohort (swan >= 7)")
    print(f"  Strict window: {' + '.join(MONTHS)}")
    print(sep)
    print(
        f"  {'Similarity bucket':<18} {'N':>5} {'WinRate':>8} "
        f"{'Nbr WinRate':>12}  (mean neighbour win rate)"
    )
    print(f"  {'-'*18} {'-'*5} {'-'*8} {'-'*12}")

    for label in ["Q1 (closest)", "Q2", "Q3", "Q4 (furthest)"]:
        c = cohorts.get(label)
        if not c or c["n"] == 0:
            continue
        win_rate = 100.0 * c["winners"] / c["n"]
        nwr = 100.0 * c["nwr_sum"] / c["nwr_n"] if c["nwr_n"] > 0 else float("nan")
        print(
            f"  {label:<18} {c['n']:>5} {win_rate:>7.1f}%  {nwr:>10.1f}%"
        )

    # Overall
    all_n = sum(c["n"] for c in cohorts.values())
    all_w = sum(c["winners"] for c in cohorts.values())
    all_nwr_sum = sum(c["nwr_sum"] for c in cohorts.values())
    all_nwr_n   = sum(c["nwr_n"]  for c in cohorts.values())
    overall_wr  = 100.0 * all_w / all_n if all_n else 0
    overall_nwr = 100.0 * all_nwr_sum / all_nwr_n if all_nwr_n else float("nan")

    print(f"  {'-'*18} {'-'*5} {'-'*8} {'-'*12}")
    print(f"  {'ALL':<18} {all_n:>5} {overall_wr:>7.1f}%  {overall_nwr:>10.1f}%")
    print(sep)

    # Verdict
    wr_values = []
    for label in ["Q1 (closest)", "Q2", "Q3", "Q4 (furthest)"]:
        c = cohorts.get(label)
        if c and c["n"] > 0:
            wr_values.append(100.0 * c["winners"] / c["n"])

    print()
    if len(wr_values) >= 2:
        spread = max(wr_values) - min(wr_values)
        if spread > 5:
            verdict = (
                f"Content signal SHOWS separation ({spread:.1f}pp spread across "
                "similarity quartiles). Worth investigating further."
            )
        else:
            verdict = (
                f"Content signal does NOT separate cohorts ({spread:.1f}pp spread). "
                "Semantic similarity adds no predictive power beyond swan_score. "
                "Close the content hypothesis."
            )
        print(f"  Verdict : {verdict}")

    print(f"\n  Computed in {elapsed:.0f}s  |  candidates={all_n}")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate content signal (issue #12)")
    ap.add_argument("--top-k", type=int, default=10,
                    help="Neighbours per query (default 10)")
    args = ap.parse_args()

    import chromadb

    if not CHROMA_PATH.exists():
        logger.error(
            f"ChromaDB not found at {CHROMA_PATH}. "
            "Run scripts/build_chroma.py first."
        )
        sys.exit(1)

    db_conn = sqlite3.connect(DB_PATH)
    db_conn.row_factory = sqlite3.Row
    logger.info("Loading high-score candidates …")
    candidates = load_high_candidates(db_conn)
    db_conn.close()
    logger.info(f"  {len(candidates)} high-score candidates loaded")

    client = chromadb.PersistentClient(path=str(CHROMA_PATH))
    col = client.get_collection(COLLECTION_NAME)
    logger.info(f"ChromaDB collection '{COLLECTION_NAME}': {col.count()} documents")

    t0 = time.monotonic()
    logger.info(f"Querying top-{args.top_k} neighbours for each candidate …")
    results = neighbour_win_rate(col, candidates, top_k=args.top_k)
    elapsed = time.monotonic() - t0

    print_report(results, top_k=args.top_k, elapsed=elapsed)


if __name__ == "__main__":
    main()
