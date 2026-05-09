#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import random
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

DEFAULT_TOUCH_DB = Path("v2/short_horizon/data/touch_dataset.sqlite3")
DEFAULT_REPORT_PATH = Path("v2/short_horizon/docs/phase6/t1_reversal_trigger.md")
DEFAULT_FEE_RATE_BPS = 720.0
DEFAULT_BOOTSTRAP_SAMPLES = 1000
DEFAULT_SEED = 1721
DEFAULT_ASSET_ALLOWLIST = {"btc", "eth", "sol", "xrp"}

LIFECYCLE_BUCKETS: tuple[tuple[str, float | None, float | None], ...] = (
    ("<0.40", None, 0.40),
    ("0.40-0.60", 0.40, 0.60),
    ("0.60-0.75", 0.60, 0.75),
    (">=0.75", 0.75, None),
)


@dataclass(frozen=True)
class TouchRow:
    probe_id: str
    market_id: str
    token_id: str
    asset_slug: str
    direction: str
    touch_level: float
    lifecycle_fraction: float | None
    resolves_yes: int
    fee_rate_bps: float | None


@dataclass(frozen=True)
class EvaluatedTouch:
    row: TouchRow
    lifecycle_bucket: str
    continuation_entry_price: float
    reversal_entry_price: float
    continuation_ev_per_usdc: float
    reversal_ev_per_usdc: float


@dataclass(frozen=True)
class SegmentSummary:
    asset_slug: str
    direction: str
    lifecycle_bucket: str
    level: float
    n: int
    continuation_win_rate: float
    reversal_win_rate: float
    continuation_ev_per_usdc: float
    reversal_ev_per_usdc: float
    reversal_ci95_lower: float | None
    reversal_ci95_upper: float | None
    delta_ev_per_usdc: float
    gate: str

    def as_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReversalEvaluationSummary:
    touch_dataset_path: str
    report_path: str
    input_rows: int
    evaluated_rows: int
    skipped_rows: int
    fee_rate_bps: float
    asset_allowlist: list[str]
    bootstrap_samples: int
    segments: list[SegmentSummary]
    best_reversal_segment: SegmentSummary | None
    go_gate: str

    def as_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["segments"] = [segment.as_json() for segment in self.segments]
        payload["best_reversal_segment"] = self.best_reversal_segment.as_json() if self.best_reversal_segment else None
        return payload


def evaluate_reversal_trigger(
    touch_dataset_path: str | Path = DEFAULT_TOUCH_DB,
    *,
    report_path: str | Path = DEFAULT_REPORT_PATH,
    fee_rate_bps: float = DEFAULT_FEE_RATE_BPS,
    bootstrap_samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
    min_segment_n: int = 50,
    asset_allowlist: set[str] | None = DEFAULT_ASSET_ALLOWLIST,
    seed: int = DEFAULT_SEED,
) -> ReversalEvaluationSummary:
    rows = load_touch_rows(touch_dataset_path)
    normalized_asset_allowlist = _normalize_set(asset_allowlist)
    evaluated: list[EvaluatedTouch] = []
    skipped = 0
    for row in rows:
        if normalized_asset_allowlist is not None and row.asset_slug.lower() not in normalized_asset_allowlist:
            skipped += 1
            continue
        item = evaluate_touch(row, fee_rate_bps=fee_rate_bps)
        if item is None:
            skipped += 1
            continue
        evaluated.append(item)

    segments = summarize_segments(
        evaluated,
        bootstrap_samples=bootstrap_samples,
        min_segment_n=min_segment_n,
        seed=seed,
    )
    passing = [segment for segment in segments if segment.gate == "GO"]
    sufficiently_sampled = [segment for segment in segments if segment.n >= min_segment_n]
    best_pool = passing or sufficiently_sampled or segments
    best = max(best_pool, key=lambda s: (s.reversal_ev_per_usdc, s.n), default=None)
    summary = ReversalEvaluationSummary(
        touch_dataset_path=str(touch_dataset_path),
        report_path=str(report_path),
        input_rows=len(rows),
        evaluated_rows=len(evaluated),
        skipped_rows=skipped,
        fee_rate_bps=fee_rate_bps,
        asset_allowlist=sorted(normalized_asset_allowlist) if normalized_asset_allowlist is not None else [],
        bootstrap_samples=bootstrap_samples,
        segments=segments,
        best_reversal_segment=best,
        go_gate="GO" if passing else "NO-GO",
    )
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_report(summary, min_segment_n=min_segment_n), encoding="utf-8")
    return summary


def load_touch_rows(path: str | Path) -> list[TouchRow]:
    path = Path(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='touch_dataset'").fetchone() is None:
            raise ValueError(f"{path} does not contain table touch_dataset")
        cols = _table_columns(conn, "touch_dataset")
        required = {
            "probe_id",
            "market_id",
            "token_id",
            "asset_slug",
            "direction",
            "touch_level",
            "lifecycle_fraction",
            "resolves_yes",
        }
        missing = sorted(required - cols)
        if missing:
            raise ValueError(f"touch_dataset missing required columns: {missing}")
        fee_expr = "fee_rate_bps" if "fee_rate_bps" in cols else "NULL AS fee_rate_bps"
        rows = conn.execute(
            f"""
            SELECT probe_id, market_id, token_id, asset_slug, direction, touch_level,
                   lifecycle_fraction, resolves_yes, {fee_expr}
            FROM touch_dataset
            ORDER BY touch_time_iso, probe_id
            """
        ).fetchall()
        return [_touch_row_from_sql(row) for row in rows]
    finally:
        conn.close()


def evaluate_touch(row: TouchRow, *, fee_rate_bps: float) -> EvaluatedTouch | None:
    if row.touch_level <= 0 or row.touch_level >= 1:
        return None
    bucket = lifecycle_bucket(row.lifecycle_fraction)
    if bucket is None:
        return None
    continuation_entry = row.touch_level
    reversal_entry = 1.0 - row.touch_level
    # Current touch continuation buys the touched token. Reversal buys the opposite token.
    continuation_win = bool(row.resolves_yes)
    reversal_win = not continuation_win
    continuation_ev = realized_ev_per_usdc(continuation_entry, continuation_win, fee_rate_bps)
    reversal_ev = realized_ev_per_usdc(reversal_entry, reversal_win, fee_rate_bps)
    return EvaluatedTouch(
        row=row,
        lifecycle_bucket=bucket,
        continuation_entry_price=continuation_entry,
        reversal_entry_price=reversal_entry,
        continuation_ev_per_usdc=continuation_ev,
        reversal_ev_per_usdc=reversal_ev,
    )


def realized_ev_per_usdc(entry_price: float, wins: bool, fee_rate_bps: float) -> float:
    if entry_price <= 0:
        return 0.0
    fee = fee_rate_bps / 10000.0
    payout = (1.0 / entry_price) if wins else 0.0
    return payout - 1.0 - fee


def summarize_segments(
    evaluated: Sequence[EvaluatedTouch],
    *,
    bootstrap_samples: int,
    min_segment_n: int,
    seed: int,
) -> list[SegmentSummary]:
    groups: dict[tuple[str, str, str, float], list[EvaluatedTouch]] = {}
    for item in evaluated:
        key = (
            item.row.asset_slug.lower(),
            item.row.direction,
            item.lifecycle_bucket,
            round(item.row.touch_level, 4),
        )
        groups.setdefault(key, []).append(item)

    summaries: list[SegmentSummary] = []
    rng = random.Random(seed)
    for (asset, direction, bucket, level), items in groups.items():
        continuation_values = [item.continuation_ev_per_usdc for item in items]
        reversal_values = [item.reversal_ev_per_usdc for item in items]
        reversal_ci = bootstrap_mean_ci(reversal_values, samples=bootstrap_samples, rng=rng) if len(items) >= min_segment_n else (None, None)
        cont_wins = sum(1 for item in items if item.row.resolves_yes)
        rev_wins = len(items) - cont_wins
        cont_ev = mean(continuation_values)
        rev_ev = mean(reversal_values)
        ci_lower, ci_upper = reversal_ci
        gate = "GO" if len(items) >= min_segment_n and ci_lower is not None and ci_lower > 0 and rev_ev > 0 else "NO-GO"
        summaries.append(
            SegmentSummary(
                asset_slug=asset,
                direction=direction,
                lifecycle_bucket=bucket,
                level=level,
                n=len(items),
                continuation_win_rate=cont_wins / len(items),
                reversal_win_rate=rev_wins / len(items),
                continuation_ev_per_usdc=cont_ev,
                reversal_ev_per_usdc=rev_ev,
                reversal_ci95_lower=ci_lower,
                reversal_ci95_upper=ci_upper,
                delta_ev_per_usdc=rev_ev - cont_ev,
                gate=gate,
            )
        )
    return sorted(summaries, key=lambda s: (s.gate != "GO", -s.reversal_ev_per_usdc, -s.n, s.asset_slug, s.direction, s.lifecycle_bucket, s.level))


def lifecycle_bucket(value: float | None) -> str | None:
    if value is None or math.isnan(value):
        return None
    for label, lo, hi in LIFECYCLE_BUCKETS:
        if lo is not None and value < lo:
            continue
        if hi is not None and value >= hi:
            continue
        return label
    return None


def bootstrap_mean_ci(values: Sequence[float], *, samples: int, rng: random.Random) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    if samples <= 0:
        m = mean(values)
        return m, m
    n = len(values)
    means = []
    for _ in range(samples):
        means.append(sum(values[rng.randrange(n)] for _ in range(n)) / n)
    means.sort()
    return percentile(means, 0.025), percentile(means, 0.975)


def render_report(summary: ReversalEvaluationSummary, *, min_segment_n: int) -> str:
    lines = [
        "# T1 reversal trigger",
        "",
        "## Scope",
        "",
        "This report evaluates whether the existing ASC-touch event is better interpreted as exhaustion instead of continuation.",
        "For each touch row, continuation buys the touched token at `touch_level`; reversal buys the opposite token at `1 - touch_level`.",
        f"Both sides use a taker fee of `{summary.fee_rate_bps:.1f} bps`.",
        "",
        "## Inputs",
        "",
        f"- Touch dataset: `{summary.touch_dataset_path}`",
        f"- Input rows: `{summary.input_rows}`",
        f"- Evaluated rows: `{summary.evaluated_rows}`",
        f"- Skipped rows: `{summary.skipped_rows}`",
        f"- Asset allowlist: `{', '.join(summary.asset_allowlist) if summary.asset_allowlist else 'all'}`",
        f"- Bootstrap samples: `{summary.bootstrap_samples}`",
        f"- Segment GO gate: reversal EV > 0, CI95 lower > 0, n >= `{min_segment_n}`",
        "",
        "## Headline",
        "",
        f"- Gate: `{summary.go_gate}`",
    ]
    if summary.best_reversal_segment is not None:
        best = summary.best_reversal_segment
        lines.extend(
            [
                f"- Best sampled reversal segment: `{best.asset_slug} | {best.direction} | {best.lifecycle_bucket} | level={best.level:.2f}`",
                f"- Best reversal EV/USDC: `{best.reversal_ev_per_usdc:.6f}` with n=`{best.n}` and CI95=`{fmt_optional(best.reversal_ci95_lower)}..{fmt_optional(best.reversal_ci95_upper)}`",
            ]
        )
    lines.extend([
        "",
        "## Segment comparison",
        "",
        "| asset | direction | lifecycle | level | n | continuation win | continuation EV/USDC | reversal win | reversal EV/USDC | reversal CI95 | delta | gate |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- |",
    ])
    for segment in summary.segments:
        lines.append(
            "| {asset} | {direction} | {bucket} | {level:.2f} | {n} | {cwin:.2%} | {cev:.6f} | {rwin:.2%} | {rev:.6f} | {ci} | {delta:.6f} | {gate} |".format(
                asset=segment.asset_slug,
                direction=segment.direction,
                bucket=segment.lifecycle_bucket,
                level=segment.level,
                n=segment.n,
                cwin=segment.continuation_win_rate,
                cev=segment.continuation_ev_per_usdc,
                rwin=segment.reversal_win_rate,
                rev=segment.reversal_ev_per_usdc,
                ci=f"{fmt_optional(segment.reversal_ci95_lower)}..{fmt_optional(segment.reversal_ci95_upper)}",
                delta=segment.delta_ev_per_usdc,
                gate=segment.gate,
            )
        )
    lines.extend([
        "",
        "## Interpretation contract",
        "",
        "- If reversal is symmetric-negative across all segments, T1 closes as no-go: the touch trigger is noisy on both sides.",
        "- If any segment has reversal EV > 0, CI95 lower > 0, and n >= 50, lock that segment for fresh holdout evaluation.",
        "",
    ])
    return "\n".join(lines)


def mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    pos = (len(values) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return values[int(pos)]
    return values[lo] * (hi - pos) + values[hi] * (pos - lo)


def fmt_optional(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.6f}"


def _touch_row_from_sql(row: sqlite3.Row) -> TouchRow:
    return TouchRow(
        probe_id=str(row["probe_id"]),
        market_id=str(row["market_id"]),
        token_id=str(row["token_id"]),
        asset_slug=str(row["asset_slug"] or ""),
        direction=str(row["direction"] or ""),
        touch_level=float(row["touch_level"]),
        lifecycle_fraction=_optional_float(row["lifecycle_fraction"]),
        resolves_yes=int(row["resolves_yes"] or 0),
        fee_rate_bps=_optional_float(row["fee_rate_bps"]),
    )


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _normalize_set(values: Iterable[str] | None) -> set[str] | None:
    if values is None:
        return None
    normalized = {value.strip().lower() for value in values if value and value.strip()}
    return normalized or None


def _parse_csv_set(value: str | None) -> set[str] | None:
    if value is None:
        return None
    return {part.strip() for part in value.split(",") if part.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate T1 ASC-touch reversal trigger")
    parser.add_argument("--touch-dataset", default=str(DEFAULT_TOUCH_DB), help="SQLite DB containing touch_dataset")
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH), help="Output markdown report")
    parser.add_argument("--fee-rate-bps", type=float, default=DEFAULT_FEE_RATE_BPS, help="Taker fee rate applied to both sides")
    parser.add_argument("--bootstrap-samples", type=int, default=DEFAULT_BOOTSTRAP_SAMPLES)
    parser.add_argument("--min-segment-n", type=int, default=50)
    parser.add_argument(
        "--asset-allowlist",
        default=",".join(sorted(DEFAULT_ASSET_ALLOWLIST)),
        help="Comma-separated assets to evaluate. Pass an empty string for all assets.",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()
    asset_allowlist = _parse_csv_set(args.asset_allowlist) if args.asset_allowlist != "" else None
    summary = evaluate_reversal_trigger(
        args.touch_dataset,
        report_path=args.report,
        fee_rate_bps=args.fee_rate_bps,
        bootstrap_samples=args.bootstrap_samples,
        min_segment_n=args.min_segment_n,
        asset_allowlist=asset_allowlist,
        seed=args.seed,
    )
    print(
        "T1 reversal trigger: "
        f"gate={summary.go_gate} evaluated={summary.evaluated_rows}/{summary.input_rows} "
        f"segments={len(summary.segments)} report={summary.report_path}"
    )
    if summary.best_reversal_segment is not None:
        best = summary.best_reversal_segment
        print(
            "Best reversal segment: "
            f"{best.asset_slug}|{best.direction}|{best.lifecycle_bucket}|{best.level:.2f} "
            f"n={best.n} ev={best.reversal_ev_per_usdc:.6f} "
            f"ci={fmt_optional(best.reversal_ci95_lower)}..{fmt_optional(best.reversal_ci95_upper)} "
            f"gate={best.gate}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
