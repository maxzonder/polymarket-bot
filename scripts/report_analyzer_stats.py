#!/usr/bin/env python3
"""
Overview report for analyzed Polymarket data.

Builds a compact post-analyzer summary from `token_swans` and related tables:
- covered data period
- total days / markets / tokens / swan events
- per-day breakdown
- x-multiple buckets
- liquidity and duration overview
- top categories / top markets by swan count

Usage:
    python scripts/report_analyzer_stats.py
    python scripts/report_analyzer_stats.py --db polymarket_dataset.db
    python scripts/report_analyzer_stats.py --date-from 2026-02-14 --date-to 2026-02-28
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.paths import DB_PATH


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build overview stats from token_swans")
    ap.add_argument("--db", default=str(DB_PATH), help="Path to SQLite DB")
    ap.add_argument("--date-from", help="Inclusive lower date bound, YYYY-MM-DD")
    ap.add_argument("--date-to", help="Inclusive upper date bound, YYYY-MM-DD")
    ap.add_argument("--top", type=int, default=10, help="How many top rows to print in leaderboard sections")
    return ap.parse_args()


def money(v: float | None) -> str:
    return "n/a" if v is None else f"${v:,.2f}"


def num(v: float | None, digits: int = 2) -> str:
    return "n/a" if v is None else f"{v:,.{digits}f}"


def build_where(args: argparse.Namespace) -> tuple[str, list[str]]:
    clauses: list[str] = []
    params: list[str] = []
    if args.date_from:
        clauses.append("s.date >= ?")
        params.append(args.date_from)
    if args.date_to:
        clauses.append("s.date <= ?")
        params.append(args.date_to)
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    return where, params


def print_section(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(str(Path(args.db)))
    conn.row_factory = sqlite3.Row

    where_sql, params = build_where(args)

    overall = conn.execute(
        f"""
        SELECT
            MIN(s.date) AS date_from,
            MAX(s.date) AS date_to,
            COUNT(DISTINCT s.date) AS days,
            COUNT(*) AS swans,
            COUNT(DISTINCT s.market_id) AS markets,
            COUNT(DISTINCT s.token_id) AS tokens,
            ROUND(AVG(s.possible_x), 2) AS avg_x,
            ROUND(MIN(s.possible_x), 2) AS min_x,
            ROUND(MAX(s.possible_x), 2) AS max_x,
            ROUND(AVG(s.entry_volume_usdc), 2) AS avg_entry_liq,
            ROUND(MIN(s.entry_volume_usdc), 2) AS min_entry_liq,
            ROUND(MAX(s.entry_volume_usdc), 2) AS max_entry_liq,
            ROUND(AVG(s.exit_volume_usdc), 2) AS avg_exit_liq,
            ROUND(MIN(s.exit_volume_usdc), 2) AS min_exit_liq,
            ROUND(MAX(s.exit_volume_usdc), 2) AS max_exit_liq,
            ROUND(AVG(s.duration_entry_to_target_minutes), 2) AS avg_minutes_to_target,
            ROUND(AVG(s.duration_entry_to_peak_minutes), 2) AS avg_minutes_to_peak,
            ROUND(AVG(s.target_exit_x), 2) AS avg_target_exit_x,
            ROUND(AVG(s.min_recovery), 2) AS avg_min_recovery,
            ROUND(AVG(s.min_duration_minutes_required), 2) AS avg_min_duration_required
        FROM token_swans s
        {where_sql}
        """,
        params,
    ).fetchone()

    if not overall["swans"]:
        print("No token_swans found for the selected date range.")
        return

    print("ANALYZER OVERVIEW")
    print("=================")
    print(f"period: {overall['date_from']} -> {overall['date_to']}")
    print(f"days: {overall['days']}")
    print(f"swans: {overall['swans']}")
    print(f"markets with swans: {overall['markets']}")
    print(f"tokens with swans: {overall['tokens']}")
    print(f"avg_x: {num(overall['avg_x'])} | min_x: {num(overall['min_x'])} | max_x: {num(overall['max_x'])}")
    print(
        f"entry_liq avg/min/max: {money(overall['avg_entry_liq'])} / {money(overall['min_entry_liq'])} / {money(overall['max_entry_liq'])}"
    )
    print(
        f"exit_liq avg/min/max: {money(overall['avg_exit_liq'])} / {money(overall['min_exit_liq'])} / {money(overall['max_exit_liq'])}"
    )
    print(
        f"avg minutes to target/peak: {num(overall['avg_minutes_to_target'])} / {num(overall['avg_minutes_to_peak'])}"
    )
    print(
        f"avg analyzer params: target_exit_x={num(overall['avg_target_exit_x'])}, min_recovery={num(overall['avg_min_recovery'])}, min_duration_required={num(overall['avg_min_duration_required'])}m"
    )

    print_section("Per-day summary")
    rows = conn.execute(
        f"""
        SELECT
            s.date,
            COUNT(*) AS swans,
            COUNT(DISTINCT s.market_id) AS markets,
            COUNT(DISTINCT s.token_id) AS tokens,
            ROUND(AVG(s.possible_x), 2) AS avg_x,
            ROUND(MAX(s.possible_x), 2) AS max_x,
            ROUND(AVG(s.entry_volume_usdc), 2) AS avg_entry_liq,
            ROUND(AVG(s.exit_volume_usdc), 2) AS avg_exit_liq,
            ROUND(AVG(s.duration_entry_to_target_minutes), 2) AS avg_minutes_to_target
        FROM token_swans s
        {where_sql}
        GROUP BY s.date
        ORDER BY s.date
        """,
        params,
    ).fetchall()
    for r in rows:
        print(
            f"{r['date']} | swans={r['swans']} | markets={r['markets']} | tokens={r['tokens']} | "
            f"avg_x={num(r['avg_x'])} | max_x={num(r['max_x'])} | "
            f"entry_avg={money(r['avg_entry_liq'])} | exit_avg={money(r['avg_exit_liq'])} | "
            f"minutes_to_target_avg={num(r['avg_minutes_to_target'])}"
        )

    print_section("X buckets")
    bucket_rows = conn.execute(
        f"""
        SELECT bucket, COUNT(*) AS swans
        FROM (
            SELECT CASE
                WHEN s.possible_x < 10 THEN '5x-9.99x'
                WHEN s.possible_x < 20 THEN '10x-19.99x'
                WHEN s.possible_x < 30 THEN '20x-29.99x'
                WHEN s.possible_x < 50 THEN '30x-49.99x'
                WHEN s.possible_x < 100 THEN '50x-99.99x'
                ELSE '100x+'
            END AS bucket
            FROM token_swans s
            {where_sql}
        )
        GROUP BY bucket
        ORDER BY CASE bucket
            WHEN '5x-9.99x' THEN 1
            WHEN '10x-19.99x' THEN 2
            WHEN '20x-29.99x' THEN 3
            WHEN '30x-49.99x' THEN 4
            WHEN '50x-99.99x' THEN 5
            ELSE 6
        END
        """,
        params,
    ).fetchall()
    for r in bucket_rows:
        pct = (r['swans'] / overall['swans']) * 100.0
        print(f"{r['bucket']}: {r['swans']} ({pct:.2f}%)")

    print_section("Entry liquidity buckets")
    liq_rows = conn.execute(
        f"""
        SELECT bucket, COUNT(*) AS swans
        FROM (
            SELECT CASE
                WHEN s.entry_volume_usdc < 25 THEN '$10-$24.99'
                WHEN s.entry_volume_usdc < 50 THEN '$25-$49.99'
                WHEN s.entry_volume_usdc < 100 THEN '$50-$99.99'
                WHEN s.entry_volume_usdc < 500 THEN '$100-$499.99'
                WHEN s.entry_volume_usdc < 1000 THEN '$500-$999.99'
                ELSE '$1000+'
            END AS bucket
            FROM token_swans s
            {where_sql}
        )
        GROUP BY bucket
        ORDER BY CASE bucket
            WHEN '$10-$24.99' THEN 1
            WHEN '$25-$49.99' THEN 2
            WHEN '$50-$99.99' THEN 3
            WHEN '$100-$499.99' THEN 4
            WHEN '$500-$999.99' THEN 5
            ELSE 6
        END
        """,
        params,
    ).fetchall()
    for r in liq_rows:
        pct = (r['swans'] / overall['swans']) * 100.0
        print(f"{r['bucket']}: {r['swans']} ({pct:.2f}%)")

    print_section("Duration to target buckets")
    dur_rows = conn.execute(
        f"""
        SELECT bucket, COUNT(*) AS swans
        FROM (
            SELECT CASE
                WHEN s.duration_entry_to_target_minutes < 15 THEN '5-14.99m'
                WHEN s.duration_entry_to_target_minutes < 60 THEN '15-59.99m'
                WHEN s.duration_entry_to_target_minutes < 360 THEN '1h-5.99h'
                WHEN s.duration_entry_to_target_minutes < 1440 THEN '6h-23.99h'
                WHEN s.duration_entry_to_target_minutes < 4320 THEN '1d-2.99d'
                ELSE '3d+'
            END AS bucket
            FROM token_swans s
            {where_sql}
        )
        GROUP BY bucket
        ORDER BY CASE bucket
            WHEN '5-14.99m' THEN 1
            WHEN '15-59.99m' THEN 2
            WHEN '1h-5.99h' THEN 3
            WHEN '6h-23.99h' THEN 4
            WHEN '1d-2.99d' THEN 5
            ELSE 6
        END
        """,
        params,
    ).fetchall()
    for r in dur_rows:
        pct = (r['swans'] / overall['swans']) * 100.0
        print(f"{r['bucket']}: {r['swans']} ({pct:.2f}%)")

    print_section(f"Top {args.top} categories")
    cat_rows = conn.execute(
        f"""
        SELECT
            COALESCE(m.category, 'unknown') AS category,
            COUNT(*) AS swans,
            COUNT(DISTINCT s.market_id) AS markets,
            ROUND(AVG(s.possible_x), 2) AS avg_x,
            ROUND(MAX(s.possible_x), 2) AS max_x
        FROM token_swans s
        LEFT JOIN markets m ON m.id = s.market_id
        {where_sql}
        GROUP BY COALESCE(m.category, 'unknown')
        ORDER BY swans DESC, markets DESC, avg_x DESC
        LIMIT ?
        """,
        [*params, args.top],
    ).fetchall()
    for r in cat_rows:
        print(
            f"{r['category']}: swans={r['swans']} | markets={r['markets']} | avg_x={num(r['avg_x'])} | max_x={num(r['max_x'])}"
        )

    print_section(f"Top {args.top} markets by swan count")
    market_rows = conn.execute(
        f"""
        SELECT
            s.market_id,
            COALESCE(m.question, '(unknown question)') AS question,
            COALESCE(m.category, 'unknown') AS category,
            COUNT(*) AS swans,
            COUNT(DISTINCT s.token_id) AS tokens,
            ROUND(AVG(s.possible_x), 2) AS avg_x,
            ROUND(MAX(s.possible_x), 2) AS max_x
        FROM token_swans s
        LEFT JOIN markets m ON m.id = s.market_id
        {where_sql}
        GROUP BY s.market_id, COALESCE(m.question, '(unknown question)'), COALESCE(m.category, 'unknown')
        ORDER BY swans DESC, avg_x DESC, s.market_id
        LIMIT ?
        """,
        [*params, args.top],
    ).fetchall()
    for r in market_rows:
        print(
            f"{r['market_id']} | swans={r['swans']} | tokens={r['tokens']} | avg_x={num(r['avg_x'])} | max_x={num(r['max_x'])} | {r['category']} | {r['question']}"
        )

    conn.close()


if __name__ == "__main__":
    main()
