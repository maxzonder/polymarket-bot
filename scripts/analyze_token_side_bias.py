#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from typing import Iterable

if __package__:
    from utils.paths import DB_PATH
else:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from utils.paths import DB_PATH


def _token_side(token_order: int | None) -> str:
    if token_order == 0:
        return "yes"
    if token_order == 1:
        return "no"
    if token_order is None:
        return "unknown"
    return f"order_{token_order}"


def load_side_rows(conn: sqlite3.Connection, min_samples: int) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        WITH token_base AS (
            SELECT
                t.token_id,
                t.market_id,
                t.token_order,
                COALESCE(m.category, 'null') AS category,
                CASE
                    WHEN m.volume < 10000 THEN '<10k'
                    WHEN m.volume < 100000 THEN '10k-100k'
                    WHEN m.volume < 1000000 THEN '100k-1M'
                    ELSE '>1M'
                END AS vol_bucket,
                CASE WHEN s.token_id IS NOT NULL THEN 1 ELSE 0 END AS was_swan,
                CASE WHEN s.max_traded_x >= 20 THEN 1 ELSE 0 END AS label_20x
            FROM tokens t
            JOIN markets m ON m.id = t.market_id
            LEFT JOIN swans_v2 s ON s.token_id = t.token_id
            WHERE t.token_order IS NOT NULL
        )
        SELECT
            token_order,
            category,
            vol_bucket,
            COUNT(*) AS total_tokens,
            SUM(was_swan) AS swan_count,
            SUM(label_20x) AS label_20x_count,
            1.0 * SUM(was_swan) / COUNT(*) AS swan_rate,
            1.0 * SUM(label_20x) / COUNT(*) AS label_20x_rate,
            1.0 * SUM(label_20x) / NULLIF(SUM(was_swan), 0) AS label_20x_given_swan_rate
        FROM token_base
        GROUP BY token_order, category, vol_bucket
        HAVING COUNT(*) >= ?
        ORDER BY category, vol_bucket, token_order
        """,
        (min_samples,),
    ).fetchall()


def print_overall(rows: Iterable[sqlite3.Row]) -> None:
    agg: dict[int, dict[str, float]] = defaultdict(lambda: {"total": 0.0, "swans": 0.0, "good": 0.0})
    for row in rows:
        bucket = agg[int(row["token_order"])]
        bucket["total"] += float(row["total_tokens"])
        bucket["swans"] += float(row["swan_count"])
        bucket["good"] += float(row["label_20x_count"])

    print("overall by side")
    for token_order in sorted(agg):
        side = _token_side(token_order)
        total = agg[token_order]["total"]
        swans = agg[token_order]["swans"]
        good = agg[token_order]["good"]
        swan_rate = swans / total if total else 0.0
        good_rate = good / total if total else 0.0
        good_given_swan = good / swans if swans else 0.0
        print(
            f"  {side:<4} total={int(total):>6} swan_rate={swan_rate:>7.2%} "
            f"label_20x_rate={good_rate:>7.2%} label_20x|swan={good_given_swan:>7.2%}"
        )
    print()


def print_pair_diffs(rows: Iterable[sqlite3.Row], top: int) -> None:
    keyed: dict[tuple[str, str], dict[int, sqlite3.Row]] = defaultdict(dict)
    for row in rows:
        keyed[(row["category"], row["vol_bucket"])][int(row["token_order"])] = row

    pairs: list[dict] = []
    for (category, vol_bucket), by_order in keyed.items():
        if 0 not in by_order or 1 not in by_order:
            continue
        yes = by_order[0]
        no = by_order[1]
        pairs.append(
            {
                "category": category,
                "vol_bucket": vol_bucket,
                "yes_total": int(yes["total_tokens"]),
                "no_total": int(no["total_tokens"]),
                "yes_swan_rate": float(yes["swan_rate"] or 0.0),
                "no_swan_rate": float(no["swan_rate"] or 0.0),
                "yes_label_rate": float(yes["label_20x_rate"] or 0.0),
                "no_label_rate": float(no["label_20x_rate"] or 0.0),
                "swan_gap": float(no["swan_rate"] or 0.0) - float(yes["swan_rate"] or 0.0),
                "label_gap": float(no["label_20x_rate"] or 0.0) - float(yes["label_20x_rate"] or 0.0),
            }
        )

    by_swan_gap = sorted(pairs, key=lambda row: abs(row["swan_gap"]), reverse=True)
    by_label_gap = sorted(pairs, key=lambda row: abs(row["label_gap"]), reverse=True)

    print(f"top {min(top, len(by_swan_gap))} cohort gaps by swan_rate (no - yes)")
    for row in by_swan_gap[:top]:
        print(
            f"  {row['category']:<14} {row['vol_bucket']:<10} "
            f"yes={row['yes_swan_rate']:.2%} no={row['no_swan_rate']:.2%} "
            f"gap={row['swan_gap']:+.2%} totals={row['yes_total']}/{row['no_total']}"
        )
    print()

    print(f"top {min(top, len(by_label_gap))} cohort gaps by label_20x_rate (no - yes)")
    for row in by_label_gap[:top]:
        print(
            f"  {row['category']:<14} {row['vol_bucket']:<10} "
            f"yes={row['yes_label_rate']:.2%} no={row['no_label_rate']:.2%} "
            f"gap={row['label_gap']:+.2%} totals={row['yes_total']}/{row['no_total']}"
        )


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze YES/NO side bias from tokens + swans_v2")
    ap.add_argument("--db", default=str(DB_PATH), help="Path to polymarket_dataset.db")
    ap.add_argument("--min-samples", type=int, default=20, help="Minimum tokens per (token_order, category, vol_bucket)")
    ap.add_argument("--top", type=int, default=20, help="How many largest cohort gaps to print")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        rows = load_side_rows(conn, min_samples=max(1, args.min_samples))
    except sqlite3.OperationalError as exc:
        print(f"Dataset is not ready for side-bias analysis: {exc}. Pass --db with a populated polymarket_dataset.db.")
        return
    finally:
        conn.close()

    if not rows:
        print("No side-aware cohorts found. Check that tokens.token_order is populated and swans_v2 exists.")
        return

    print_overall(rows)
    print_pair_diffs(rows, top=max(1, args.top))


if __name__ == "__main__":
    main()
