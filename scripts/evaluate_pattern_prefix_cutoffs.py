#!/usr/bin/env python3
"""Evaluate MarketPatternTracker labels at historical prefix cutoffs.

This is an offline, point-in-time check for issue #197.  For each token side,
it cuts the historical tape at lifecycle fractions (e.g. 20%, 40%, 60%, 80%),
classifies only the prefix available by that timestamp, applies the same pattern
policy multiplier as live/paper, and compares the gate decision with the final
resolved outcome.

The goal is not to claim executable PnL (historical tape has trade prints, not
live best ask / queue state).  The goal is to quantify whether early prefix
labels are useful or harmful as a screener gate.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Iterable, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from replay.tape_feed import DEFAULT_TAPE_DB_PATH
from strategy.market_pattern_tracker import _classify_details, _duration_bucket, _policy_mult
from utils.paths import DATA_DIR, DB_PATH, ensure_runtime_dirs

DEFAULT_OUTPUT_MD = DATA_DIR / "pattern_prefix_cutoff_report.md"
DEFAULT_OUTPUT_JSON = DATA_DIR / "pattern_prefix_cutoff_report.json"
DEFAULT_FRACTIONS = (0.20, 0.40, 0.60, 0.80)
DEFAULT_ENTRY_PRICE_MAX = 0.05
DEFAULT_MIN_DURATION_HOURS = 0.25
DEFAULT_MAX_DURATION_HOURS = 24 * 365 * 5


@dataclass(frozen=True)
class TokenMeta:
    token_id: str
    market_id: str
    question: str
    category: str | None
    is_winner: int
    duration_hours: float
    start_ts: int
    closed_ts: int


@dataclass(frozen=True)
class TradePoint:
    timestamp: int
    price: float
    size: float


@dataclass(frozen=True)
class PrefixRow:
    token_id: str
    market_id: str
    question: str
    category: str | None
    fraction: float
    cutoff_ts: int
    hours_to_close: float
    duration_bucket: str
    trade_count: int
    price: float
    pattern_label: str
    pattern_mult: float
    observed_lifecycle_fraction: float | None
    first_floor_fraction: float | None
    floor_duration_fraction: float | None
    is_winner: int
    gross_pnl_per_share: float


def evaluate_pattern_prefix_cutoffs(
    *,
    dataset_db: Path,
    tape_db: Path,
    output_md: Path,
    output_json: Path,
    fractions: Sequence[float],
    entry_price_max: float,
    min_duration_hours: float,
    max_duration_hours: float,
    categories: set[str] | None,
    start_date: str | None,
    end_date: str | None,
    max_tokens: int | None,
) -> dict:
    ensure_runtime_dirs()
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    start_ts_filter = _parse_date_start(start_date)
    end_ts_filter = _parse_date_end(end_date)
    metas = load_token_meta(
        dataset_db,
        min_duration_hours=min_duration_hours,
        max_duration_hours=max_duration_hours,
        categories=categories,
        start_ts=start_ts_filter,
        end_ts=end_ts_filter,
        max_tokens=max_tokens,
    )
    rows, skipped_no_tape = build_prefix_rows(
        tape_db,
        metas,
        fractions=fractions,
        entry_price_max=entry_price_max,
    )
    summary = summarize(rows, metas=metas, skipped_no_tape=skipped_no_tape, fractions=fractions, entry_price_max=entry_price_max)
    output_json.write_text(json.dumps({"summary": summary, "rows": [asdict(row) for row in rows]}, indent=2, sort_keys=True), encoding="utf-8")
    output_md.write_text(render_report(summary, rows), encoding="utf-8")
    return summary


def load_token_meta(
    dataset_db: Path,
    *,
    min_duration_hours: float,
    max_duration_hours: float,
    categories: set[str] | None,
    start_ts: int | None,
    end_ts: int | None,
    max_tokens: int | None,
) -> dict[str, TokenMeta]:
    wanted_categories = {c.lower() for c in categories} if categories else None
    conn = sqlite3.connect(dataset_db)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                CAST(t.token_id AS TEXT) AS token_id,
                CAST(t.market_id AS TEXT) AS market_id,
                CAST(COALESCE(t.is_winner, 0) AS INTEGER) AS is_winner,
                COALESCE(m.question, '') AS question,
                COALESCE(m.category, 'unknown') AS category,
                CAST(COALESCE(m.duration_hours, 0) AS REAL) AS duration_hours,
                CAST(COALESCE(m.closed_time, 0) AS INTEGER) AS closed_time
            FROM tokens t
            JOIN markets m ON m.id = t.market_id
            WHERE m.closed_time IS NOT NULL
              AND CAST(m.closed_time AS INTEGER) > 0
              AND t.is_winner IS NOT NULL
            ORDER BY CAST(m.closed_time AS INTEGER), CAST(t.token_id AS TEXT)
            """
        ).fetchall()
    finally:
        conn.close()

    metas: dict[str, TokenMeta] = {}
    for row in rows:
        duration_hours = float(row["duration_hours"] or 0.0)
        if duration_hours < min_duration_hours or duration_hours > max_duration_hours:
            continue
        category = str(row["category"] or "unknown").lower()
        if wanted_categories and category not in wanted_categories:
            continue
        closed_ts = int(row["closed_time"] or 0)
        market_start_ts = int(round(closed_ts - duration_hours * 3600.0))
        if start_ts is not None and closed_ts < start_ts:
            continue
        if end_ts is not None and market_start_ts > end_ts:
            continue
        token_id = str(row["token_id"])
        metas[token_id] = TokenMeta(
            token_id=token_id,
            market_id=str(row["market_id"]),
            question=str(row["question"] or ""),
            category=category,
            is_winner=int(row["is_winner"] or 0),
            duration_hours=duration_hours,
            start_ts=market_start_ts,
            closed_ts=closed_ts,
        )
        if max_tokens is not None and len(metas) >= max_tokens:
            break
    return metas


def build_prefix_rows(
    tape_db: Path,
    metas: dict[str, TokenMeta],
    *,
    fractions: Sequence[float],
    entry_price_max: float,
) -> tuple[list[PrefixRow], int]:
    conn = sqlite3.connect(tape_db)
    conn.row_factory = sqlite3.Row
    rows: list[PrefixRow] = []
    skipped_no_tape = 0
    try:
        for token_id in metas:
            token_trades = [
                TradePoint(timestamp=int(row["timestamp"]), price=float(row["price"]), size=float(row["size"] or 0.0))
                for row in conn.execute(
                    """
                    SELECT timestamp, price, size
                    FROM tape INDEXED BY idx_tape_token_ts
                    WHERE token_id=?
                    ORDER BY timestamp, source_file_id, seq
                    """,
                    (token_id,),
                )
            ]
            skipped_no_tape += flush_token(token_id, token_trades, metas, fractions, entry_price_max, rows)
    finally:
        conn.close()
    return rows, skipped_no_tape


def flush_token(
    token_id: str | None,
    trades: Sequence[TradePoint],
    metas: dict[str, TokenMeta],
    fractions: Sequence[float],
    entry_price_max: float,
    out: list[PrefixRow],
) -> int:
    if token_id is None:
        return 0
    meta = metas.get(token_id)
    if meta is None:
        return 0
    in_window = [t for t in trades if meta.start_ts <= int(t.timestamp) <= meta.closed_ts]
    if len(in_window) < 1:
        return 1
    in_window.sort(key=lambda t: t.timestamp)
    for fraction in fractions:
        cutoff_ts = int(round(meta.start_ts + (meta.closed_ts - meta.start_ts) * float(fraction)))
        prefix = [t for t in in_window if t.timestamp <= cutoff_ts]
        if not prefix:
            continue
        price = float(prefix[-1].price)
        if price <= 0 or price > entry_price_max or price >= 0.99:
            continue
        details = _classify_details([
            {"timestamp": t.timestamp, "price": t.price, "size": t.size}
            for t in prefix
        ], meta.closed_ts)
        hours_to_close = max(0.0, (meta.closed_ts - cutoff_ts) / 3600.0)
        duration_bucket = _duration_bucket(hours_to_close)
        pattern_mult = float(_policy_mult(details.label, meta.category, duration_bucket))
        gross = (1.0 - price) if meta.is_winner else -price
        out.append(PrefixRow(
            token_id=meta.token_id,
            market_id=meta.market_id,
            question=meta.question,
            category=meta.category,
            fraction=float(fraction),
            cutoff_ts=cutoff_ts,
            hours_to_close=hours_to_close,
            duration_bucket=duration_bucket,
            trade_count=details.trade_count,
            price=price,
            pattern_label=details.label,
            pattern_mult=pattern_mult,
            observed_lifecycle_fraction=details.observed_lifecycle_fraction,
            first_floor_fraction=details.first_floor_fraction,
            floor_duration_fraction=details.floor_duration_fraction,
            is_winner=meta.is_winner,
            gross_pnl_per_share=gross,
        ))
    return 0


def summarize(rows: Sequence[PrefixRow], *, metas: dict[str, TokenMeta], skipped_no_tape: int, fractions: Sequence[float], entry_price_max: float) -> dict:
    by_fraction = []
    for fraction in fractions:
        items = [r for r in rows if abs(r.fraction - float(fraction)) < 1e-9]
        by_fraction.append(summary_row({"fraction": f"{fraction:.2f}"}, items))
    by_label = []
    for label in sorted({r.pattern_label for r in rows}):
        by_label.append(summary_row({"pattern_label": label}, [r for r in rows if r.pattern_label == label]))
    by_fraction_label = []
    for fraction in fractions:
        labels = sorted({r.pattern_label for r in rows if abs(r.fraction - float(fraction)) < 1e-9})
        for label in labels:
            by_fraction_label.append(summary_row(
                {"fraction": f"{fraction:.2f}", "pattern_label": label},
                [r for r in rows if abs(r.fraction - float(fraction)) < 1e-9 and r.pattern_label == label],
            ))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tokens_loaded": len(metas),
        "candidate_prefix_rows": len(rows),
        "skipped_tokens_no_tape": skipped_no_tape,
        "entry_price_max": entry_price_max,
        "fractions": [float(f) for f in fractions],
        "overall": summary_row({}, rows),
        "by_fraction": by_fraction,
        "by_label": by_label,
        "by_fraction_label": by_fraction_label,
    }


def summary_row(prefix: dict, items: Sequence[PrefixRow]) -> dict:
    n = len(items)
    skipped = [r for r in items if r.pattern_mult == 0.0]
    passed = [r for r in items if r.pattern_mult > 0.0]
    winners = [r for r in items if r.is_winner]
    skipped_winners = [r for r in skipped if r.is_winner]
    row = dict(prefix)
    row.update({
        "n": n,
        "winner_rate": rate(len(winners), n),
        "avg_entry_price": avg([r.price for r in items]),
        "avg_gross_pnl_per_share_all": avg([r.gross_pnl_per_share for r in items]),
        "skip_n": len(skipped),
        "skip_rate": rate(len(skipped), n),
        "skipped_winner_n": len(skipped_winners),
        "skipped_winner_rate_of_all": rate(len(skipped_winners), n),
        "passed_n": len(passed),
        "passed_winner_rate": rate(sum(1 for r in passed if r.is_winner), len(passed)),
        "passed_avg_gross_pnl_per_share": avg([r.gross_pnl_per_share for r in passed]),
    })
    return row


def render_report(summary: dict, rows: Sequence[PrefixRow]) -> str:
    lines = [
        "# Pattern prefix cutoff report",
        "",
        "Offline point-in-time evaluation of `MarketPatternTracker` on historical tape.",
        "This uses historical trade prints only; it is not a live fill/queue simulation.",
        "",
        "## Scope",
        f"- generated_at: `{summary['generated_at']}`",
        f"- tokens_loaded: `{summary['tokens_loaded']}`",
        f"- candidate_prefix_rows: `{summary['candidate_prefix_rows']}`",
        f"- skipped_tokens_no_tape: `{summary['skipped_tokens_no_tape']}`",
        f"- entry_price_max: `{summary['entry_price_max']}`",
        f"- fractions: `{summary['fractions']}`",
        "",
        "## Overall",
    ]
    lines.extend(format_summary_bullets(summary["overall"]))
    lines.append("")
    lines.append("## By cutoff fraction")
    for row in summary["by_fraction"]:
        lines.append(f"- fraction `{row['fraction']}`: " + compact_summary(row))
    lines.append("")
    lines.append("## By pattern label")
    for row in sorted(summary["by_label"], key=lambda r: (-r["n"], r.get("pattern_label", ""))):
        lines.append(f"- `{row['pattern_label']}`: " + compact_summary(row))
    lines.append("")
    lines.append("## Worst skipped-winner slices")
    bad = [r for r in summary["by_fraction_label"] if r["skip_n"] > 0]
    bad.sort(key=lambda r: (-r["skipped_winner_n"], -float(r["skipped_winner_rate_of_all"] or 0), -r["n"]))
    for row in bad[:20]:
        lines.append(f"- fraction `{row['fraction']}` label `{row['pattern_label']}`: " + compact_summary(row))
    lines.append("")
    lines.append("## Interpretation notes")
    lines.append("- `skip_n` is where current pattern policy would hard-reject (`pattern_mult == 0`).")
    lines.append("- `skipped_winner_n` is the main false-reject signal: low-price prefixes that later resolved as winners but the pattern gate would have skipped.")
    lines.append("- Gross PnL is per share from historical last trade price at cutoff; it ignores spread, queue, fees, min size, and fill probability.")
    return "\n".join(lines) + "\n"


def format_summary_bullets(row: dict) -> list[str]:
    return [
        f"- n: `{row['n']}`",
        f"- winner_rate: `{fmt_pct(row['winner_rate'])}`",
        f"- avg_entry_price: `{fmt_float(row['avg_entry_price'])}`",
        f"- avg_gross_pnl_per_share_all: `{fmt_float(row['avg_gross_pnl_per_share_all'])}`",
        f"- skip_rate: `{fmt_pct(row['skip_rate'])}` (`{row['skip_n']}` rows)",
        f"- skipped_winner_n: `{row['skipped_winner_n']}`",
        f"- passed_winner_rate: `{fmt_pct(row['passed_winner_rate'])}`",
        f"- passed_avg_gross_pnl_per_share: `{fmt_float(row['passed_avg_gross_pnl_per_share'])}`",
    ]


def compact_summary(row: dict) -> str:
    return (
        f"n `{row['n']}`, win `{fmt_pct(row['winner_rate'])}`, "
        f"skip `{fmt_pct(row['skip_rate'])}`/{row['skip_n']}, "
        f"skipped winners `{row['skipped_winner_n']}`, "
        f"passed win `{fmt_pct(row['passed_winner_rate'])}`, "
        f"passed gross `{fmt_float(row['passed_avg_gross_pnl_per_share'])}`"
    )


def avg(values: Iterable[float]) -> float | None:
    vals = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    return mean(vals) if vals else None


def rate(num: int, den: int) -> float | None:
    return (float(num) / float(den)) if den else None


def fmt_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.1f}%"


def fmt_float(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.4f}"


def parse_fractions(raw: str) -> tuple[float, ...]:
    return tuple(float(x.strip()) for x in raw.split(",") if x.strip())


def parse_categories(raw: str | None) -> set[str] | None:
    if not raw:
        return None
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def _parse_date_start(value: str | None) -> int | None:
    if not value:
        return None
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp())


def _parse_date_end(value: str | None) -> int | None:
    if not value:
        return None
    dt = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    if len(value) <= 10:
        dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.timestamp())


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate live pattern-gate labels at historical prefix cutoffs")
    ap.add_argument("--dataset-db", type=Path, default=DB_PATH)
    ap.add_argument("--tape-db", type=Path, default=DEFAULT_TAPE_DB_PATH)
    ap.add_argument("--output-md", type=Path, default=DEFAULT_OUTPUT_MD)
    ap.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    ap.add_argument("--fractions", default=",".join(str(x) for x in DEFAULT_FRACTIONS))
    ap.add_argument("--entry-price-max", type=float, default=DEFAULT_ENTRY_PRICE_MAX)
    ap.add_argument("--min-duration-hours", type=float, default=DEFAULT_MIN_DURATION_HOURS)
    ap.add_argument("--max-duration-hours", type=float, default=DEFAULT_MAX_DURATION_HOURS)
    ap.add_argument("--categories", default=None, help="Comma-separated category filter, e.g. crypto,weather")
    ap.add_argument("--start-date", default=None, help="YYYY-MM-DD inclusive by market close")
    ap.add_argument("--end-date", default=None, help="YYYY-MM-DD inclusive by market start")
    ap.add_argument("--max-tokens", type=int, default=None, help="Debug cap")
    args = ap.parse_args()
    summary = evaluate_pattern_prefix_cutoffs(
        dataset_db=args.dataset_db.expanduser(),
        tape_db=args.tape_db.expanduser(),
        output_md=args.output_md.expanduser(),
        output_json=args.output_json.expanduser(),
        fractions=parse_fractions(args.fractions),
        entry_price_max=args.entry_price_max,
        min_duration_hours=args.min_duration_hours,
        max_duration_hours=args.max_duration_hours,
        categories=parse_categories(args.categories),
        start_date=args.start_date,
        end_date=args.end_date,
        max_tokens=args.max_tokens,
    )
    print(json.dumps(summary["overall"], indent=2, sort_keys=True))
    print(f"report: {args.output_md}")
    print(f"json: {args.output_json}")


if __name__ == "__main__":
    main()
