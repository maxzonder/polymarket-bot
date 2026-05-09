#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import random
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Sequence

from _order_flow_features import (
    DEFAULT_CAPTURE_GLOB,
    OrderFlowFeature,
    build_order_flow_features,
)

DEFAULT_TOUCH_DB = Path("v2/short_horizon/data/touch_dataset.sqlite3")
DEFAULT_REPORT_PATH = Path("v2/short_horizon/docs/phase6/t2_order_flow_trigger.md")
DEFAULT_OUTPUT_DB = Path("v2/short_horizon/data/order_flow_trigger.sqlite3")
DEFAULT_FEE_RATE_BPS = 720.0
DEFAULT_BOOTSTRAP_SAMPLES = 1000
DEFAULT_NULL_SAMPLES = 500
DEFAULT_SEED = 1722
DEFAULT_ASSET_ALLOWLIST = {"btc", "eth", "sol", "xrp"}

LIFECYCLE_BUCKETS: tuple[tuple[str, float | None, float | None], ...] = (
    ("<0.40", None, 0.40),
    ("0.40-0.60", 0.40, 0.60),
    ("0.60-0.75", 0.60, 0.75),
    (">=0.75", 0.75, None),
)


@dataclass(frozen=True)
class PredicateConfig:
    name: str
    description: str
    evaluator: Callable[[OrderFlowFeature], bool]


@dataclass(frozen=True)
class SegmentSummary:
    predicate: str
    asset_slug: str
    direction: str
    lifecycle_bucket: str
    level: float
    n: int
    win_rate: float
    ev_per_usdc: float
    ci95_lower: float | None
    ci95_upper: float | None
    null_p_value: float | None
    gate: str

    def as_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class EvaluationSummary:
    touch_dataset_path: str
    capture_glob: str
    report_path: str
    output_db_path: str | None
    raw_feature_rows: int
    deduped_feature_rows: int
    predicate_count: int
    segment_count: int
    fee_rate_bps: float
    bootstrap_samples: int
    null_samples: int
    segments: list[SegmentSummary]
    best_segment: SegmentSummary | None
    go_gate: str

    def as_json(self) -> dict[str, object]:
        payload = asdict(self)
        payload["segments"] = [segment.as_json() for segment in self.segments]
        payload["best_segment"] = self.best_segment.as_json() if self.best_segment else None
        return payload


def evaluate_order_flow_trigger(
    touch_dataset_path: str | Path = DEFAULT_TOUCH_DB,
    capture_glob: str = DEFAULT_CAPTURE_GLOB,
    *,
    report_path: str | Path = DEFAULT_REPORT_PATH,
    output_db_path: str | Path | None = DEFAULT_OUTPUT_DB,
    fee_rate_bps: float = DEFAULT_FEE_RATE_BPS,
    bootstrap_samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
    null_samples: int = DEFAULT_NULL_SAMPLES,
    min_segment_n: int = 50,
    max_null_p_value: float = 0.05,
    asset_allowlist: set[str] | None = DEFAULT_ASSET_ALLOWLIST,
    seed: int = DEFAULT_SEED,
) -> EvaluationSummary:
    raw_features = build_order_flow_features(
        touch_dataset_path,
        capture_glob,
        asset_allowlist=asset_allowlist,
    )
    features = dedupe_features(raw_features)
    predicates = default_predicates()
    segments = summarize_segments(
        features,
        predicates,
        fee_rate_bps=fee_rate_bps,
        bootstrap_samples=bootstrap_samples,
        null_samples=null_samples,
        min_segment_n=min_segment_n,
        max_null_p_value=max_null_p_value,
        seed=seed,
    )
    passing = [segment for segment in segments if segment.gate == "GO"]
    sampled = [segment for segment in segments if segment.n >= min_segment_n]
    best_pool = passing or sampled or segments
    best = max(best_pool, key=lambda s: (s.ev_per_usdc, s.n), default=None)
    summary = EvaluationSummary(
        touch_dataset_path=str(touch_dataset_path),
        capture_glob=capture_glob,
        report_path=str(report_path),
        output_db_path=str(output_db_path) if output_db_path is not None else None,
        raw_feature_rows=len(raw_features),
        deduped_feature_rows=len(features),
        predicate_count=len(predicates),
        segment_count=len(segments),
        fee_rate_bps=fee_rate_bps,
        bootstrap_samples=bootstrap_samples,
        null_samples=null_samples,
        segments=segments,
        best_segment=best,
        go_gate="GO" if passing else "NO-GO",
    )
    if output_db_path is not None:
        write_output_db(output_db_path, features, segments, summary)
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_report(summary, min_segment_n=min_segment_n, max_null_p_value=max_null_p_value), encoding="utf-8")
    return summary


def dedupe_features(features: Sequence[OrderFlowFeature]) -> list[OrderFlowFeature]:
    by_probe: dict[str, OrderFlowFeature] = {}
    for feature in features:
        previous = by_probe.get(feature.probe_id)
        if previous is None or coverage_score(feature) > coverage_score(previous):
            by_probe[feature.probe_id] = feature
    return sorted(by_probe.values(), key=lambda f: (f.touch_ms, f.probe_id))


def coverage_score(feature: OrderFlowFeature) -> tuple[int, int, int, int]:
    return (
        1 if feature.has_l2_levels else 0,
        feature.pre_book_n + feature.post_book_n,
        feature.pre_trade_n + feature.post_trade_n,
        -abs(feature.last_pre_book_delta_ms or 10**12),
    )


def default_predicates() -> list[PredicateConfig]:
    return [
        PredicateConfig("buy_share_pre_5s_ge_0_6", "5s known-side buy share >= 0.60", lambda f: _buy_share(f, 5_000, 0.60)),
        PredicateConfig("buy_share_pre_5s_ge_0_7", "5s known-side buy share >= 0.70", lambda f: _buy_share(f, 5_000, 0.70)),
        PredicateConfig("signed_size_pre_5s_gt_0", "5s signed taker size > 0", lambda f: _signed_size(f, 5_000) > 0),
        PredicateConfig("mid_delta_pre_5s_gt_0", "5s BBO mid delta > 0", lambda f: _mid_delta(f, 5_000) > 0),
        PredicateConfig("ask_stays_ge_level_1s", "best ask does not revert below touch level for 1s", lambda f: f.post_windows.get(1_000) is not None and f.post_windows[1_000].ask_stays_ge_level is True),
        PredicateConfig(
            "buy_share_0_6_and_ask_stays_1s",
            "5s buy share >= 0.60 and ask stays >= level for 1s",
            lambda f: _buy_share(f, 5_000, 0.60) and f.post_windows.get(1_000) is not None and f.post_windows[1_000].ask_stays_ge_level is True,
        ),
        PredicateConfig(
            "signed_size_and_mid_delta_gt_0",
            "5s signed taker size > 0 and 5s mid delta > 0",
            lambda f: _signed_size(f, 5_000) > 0 and _mid_delta(f, 5_000) > 0,
        ),
    ]


def summarize_segments(
    features: Sequence[OrderFlowFeature],
    predicates: Sequence[PredicateConfig],
    *,
    fee_rate_bps: float,
    bootstrap_samples: int,
    null_samples: int,
    min_segment_n: int,
    max_null_p_value: float,
    seed: int,
) -> list[SegmentSummary]:
    rng = random.Random(seed)
    base_labels = labels_by_base_stratum(features)
    groups: dict[tuple[str, str, str, str, float], list[OrderFlowFeature]] = {}
    for predicate in predicates:
        for feature in features:
            bucket = lifecycle_bucket(feature.lifecycle_fraction)
            if bucket is None or not predicate.evaluator(feature):
                continue
            key = (predicate.name, feature.asset_slug, feature.direction, bucket, round(feature.touch_level, 4))
            groups.setdefault(key, []).append(feature)

    segments: list[SegmentSummary] = []
    for (predicate, asset, direction, bucket, level), rows in groups.items():
        values = [realized_ev_per_usdc(row, fee_rate_bps=fee_rate_bps) for row in rows]
        ci_lower, ci_upper = bootstrap_mean_ci(values, samples=bootstrap_samples, rng=rng) if len(rows) >= min_segment_n else (None, None)
        null_p = null_p_value(rows, base_labels, observed_ev=mean(values), fee_rate_bps=fee_rate_bps, samples=null_samples, rng=rng) if len(rows) >= min_segment_n else None
        ev = mean(values)
        gate = (
            "GO"
            if len(rows) >= min_segment_n
            and ev > 0
            and ci_lower is not None
            and ci_lower > 0
            and null_p is not None
            and null_p <= max_null_p_value
            else "NO-GO"
        )
        segments.append(
            SegmentSummary(
                predicate=predicate,
                asset_slug=asset,
                direction=direction,
                lifecycle_bucket=bucket,
                level=level,
                n=len(rows),
                win_rate=sum(row.resolves_yes for row in rows) / len(rows),
                ev_per_usdc=ev,
                ci95_lower=ci_lower,
                ci95_upper=ci_upper,
                null_p_value=null_p,
                gate=gate,
            )
        )
    return sorted(segments, key=lambda s: (s.gate != "GO", -s.ev_per_usdc, -s.n, s.predicate, s.asset_slug, s.direction))


def realized_ev_per_usdc(feature: OrderFlowFeature, *, fee_rate_bps: float, resolves_yes: int | None = None) -> float:
    entry_price = feature.best_ask_at_or_after_touch or feature.touch_level
    if entry_price <= 0:
        return 0.0
    wins = feature.resolves_yes if resolves_yes is None else resolves_yes
    payout = (1.0 / entry_price) if wins else 0.0
    return payout - 1.0 - (fee_rate_bps / 10000.0)


def labels_by_base_stratum(features: Sequence[OrderFlowFeature]) -> dict[tuple[str, str, str, float], list[int]]:
    labels: dict[tuple[str, str, str, float], list[int]] = {}
    for feature in features:
        bucket = lifecycle_bucket(feature.lifecycle_fraction)
        if bucket is None:
            continue
        labels.setdefault((feature.asset_slug, feature.direction, bucket, round(feature.touch_level, 4)), []).append(feature.resolves_yes)
    return labels


def null_p_value(
    rows: Sequence[OrderFlowFeature],
    base_labels: dict[tuple[str, str, str, float], list[int]],
    *,
    observed_ev: float,
    fee_rate_bps: float,
    samples: int,
    rng: random.Random,
) -> float | None:
    if samples <= 0:
        return None
    exceed = 0
    for _ in range(samples):
        shuffled_values: list[float] = []
        for row in rows:
            bucket = lifecycle_bucket(row.lifecycle_fraction)
            if bucket is None:
                continue
            labels = base_labels.get((row.asset_slug, row.direction, bucket, round(row.touch_level, 4)))
            if not labels:
                continue
            shuffled_values.append(realized_ev_per_usdc(row, fee_rate_bps=fee_rate_bps, resolves_yes=rng.choice(labels)))
        if shuffled_values and mean(shuffled_values) >= observed_ev:
            exceed += 1
    return (exceed + 1) / (samples + 1)


def write_output_db(path: str | Path, features: Sequence[OrderFlowFeature], segments: Sequence[SegmentSummary], summary: EvaluationSummary) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            DROP TABLE IF EXISTS order_flow_features;
            DROP TABLE IF EXISTS order_flow_segments;
            DROP TABLE IF EXISTS summary_meta;
            CREATE TABLE order_flow_features (
                probe_id TEXT,
                bundle_name TEXT,
                market_id TEXT,
                token_id TEXT,
                asset_slug TEXT,
                direction TEXT,
                touch_level REAL,
                touch_time_iso TEXT,
                lifecycle_fraction REAL,
                resolves_yes INTEGER,
                pre_book_n INTEGER,
                post_book_n INTEGER,
                pre_trade_n INTEGER,
                post_trade_n INTEGER,
                best_ask_at_or_after_touch REAL,
                buy_share_pre_5s REAL,
                signed_size_pre_5s REAL,
                mid_delta_pre_5s REAL,
                ask_stays_ge_level_1s INTEGER
            );
            CREATE TABLE order_flow_segments (
                predicate TEXT,
                asset_slug TEXT,
                direction TEXT,
                lifecycle_bucket TEXT,
                level REAL,
                n INTEGER,
                win_rate REAL,
                ev_per_usdc REAL,
                ci95_lower REAL,
                ci95_upper REAL,
                null_p_value REAL,
                gate TEXT
            );
            CREATE TABLE summary_meta (key TEXT PRIMARY KEY, value TEXT);
            """
        )
        conn.executemany(
            """
            INSERT INTO order_flow_features VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    f.probe_id,
                    f.bundle_name,
                    f.market_id,
                    f.token_id,
                    f.asset_slug,
                    f.direction,
                    f.touch_level,
                    f.touch_time_iso,
                    f.lifecycle_fraction,
                    f.resolves_yes,
                    f.pre_book_n,
                    f.post_book_n,
                    f.pre_trade_n,
                    f.post_trade_n,
                    f.best_ask_at_or_after_touch,
                    _window_buy_share(f, 5_000),
                    _signed_size(f, 5_000),
                    _mid_delta(f, 5_000),
                    _bool_int(f.post_windows.get(1_000).ask_stays_ge_level if f.post_windows.get(1_000) else None),
                )
                for f in features
            ],
        )
        conn.executemany(
            "INSERT INTO order_flow_segments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    s.predicate,
                    s.asset_slug,
                    s.direction,
                    s.lifecycle_bucket,
                    s.level,
                    s.n,
                    s.win_rate,
                    s.ev_per_usdc,
                    s.ci95_lower,
                    s.ci95_upper,
                    s.null_p_value,
                    s.gate,
                )
                for s in segments
            ],
        )
        conn.executemany(
            "INSERT INTO summary_meta VALUES (?, ?)",
            [("summary_json", json.dumps(summary.as_json(), sort_keys=True))],
        )
        conn.commit()
    finally:
        conn.close()


def render_report(summary: EvaluationSummary, *, min_segment_n: int, max_null_p_value: float) -> str:
    lines = [
        "# T2 order-flow trigger",
        "",
        "## Scope",
        "",
        "This report evaluates simple Polymarket-native order-flow predicates around existing touch events.",
        "The first pass uses capture-bundle BBO and TradeTick data only; L2 depth is optional and not required.",
        "",
        "## Inputs",
        "",
        f"- Touch dataset: `{summary.touch_dataset_path}`",
        f"- Capture glob: `{summary.capture_glob}`",
        f"- Raw feature rows: `{summary.raw_feature_rows}`",
        f"- Deduped feature rows: `{summary.deduped_feature_rows}`",
        f"- Predicates: `{summary.predicate_count}`",
        f"- Segments: `{summary.segment_count}`",
        f"- Fee rate: `{summary.fee_rate_bps:.1f} bps`",
        f"- GO gate: n >= `{min_segment_n}`, EV > 0, CI95 lower > 0, null p <= `{max_null_p_value}`",
        "",
        "## Headline",
        "",
        f"- Gate: `{summary.go_gate}`",
    ]
    if summary.best_segment is not None:
        best = summary.best_segment
        lines.extend(
            [
                f"- Best sampled segment: `{best.predicate} | {best.asset_slug} | {best.direction} | {best.lifecycle_bucket} | level={best.level:.2f}`",
                f"- Best EV/USDC: `{best.ev_per_usdc:.6f}` with n=`{best.n}`, CI95=`{fmt_optional(best.ci95_lower)}..{fmt_optional(best.ci95_upper)}`, null p=`{fmt_optional(best.null_p_value)}`",
            ]
        )
    lines.extend([
        "",
        "## Segment comparison",
        "",
        "| predicate | asset | direction | lifecycle | level | n | win | EV/USDC | CI95 | null p | gate |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- |",
    ])
    for segment in summary.segments:
        lines.append(
            "| {predicate} | {asset} | {direction} | {bucket} | {level:.2f} | {n} | {win:.2%} | {ev:.6f} | {ci} | {p} | {gate} |".format(
                predicate=segment.predicate,
                asset=segment.asset_slug,
                direction=segment.direction,
                bucket=segment.lifecycle_bucket,
                level=segment.level,
                n=segment.n,
                win=segment.win_rate,
                ev=segment.ev_per_usdc,
                ci=f"{fmt_optional(segment.ci95_lower)}..{fmt_optional(segment.ci95_upper)}",
                p=fmt_optional(segment.null_p_value),
                gate=segment.gate,
            )
        )
    lines.extend([
        "",
        "## Interpretation contract",
        "",
        "- Promote only segments that clear positive EV, positive CI lower bound, and null-shuffle sanity.",
        "- If coverage is tiny, treat the result as a data collection blocker rather than a strategy signal.",
        "",
    ])
    return "\n".join(lines)


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
        value = mean(values)
        return value, value
    n = len(values)
    means = [sum(values[rng.randrange(n)] for _ in range(n)) / n for _ in range(samples)]
    means.sort()
    return percentile(means, 0.025), percentile(means, 0.975)


def percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    pos = (len(values) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return values[int(pos)]
    return values[lo] * (hi - pos) + values[hi] * (pos - lo)


def mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def fmt_optional(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.6f}"


def _buy_share(feature: OrderFlowFeature, window_ms: int, threshold: float) -> bool:
    value = _window_buy_share(feature, window_ms)
    return value is not None and value >= threshold


def _window_buy_share(feature: OrderFlowFeature, window_ms: int) -> float | None:
    stats = feature.trade_windows.get(window_ms)
    return stats.buy_share if stats is not None else None


def _signed_size(feature: OrderFlowFeature, window_ms: int) -> float:
    stats = feature.trade_windows.get(window_ms)
    return stats.signed_size if stats is not None else 0.0


def _mid_delta(feature: OrderFlowFeature, window_ms: int) -> float:
    stats = feature.book_windows.get(window_ms)
    return stats.mid_delta if stats is not None and stats.mid_delta is not None else 0.0


def _bool_int(value: bool | None) -> int | None:
    if value is None:
        return None
    return 1 if value else 0


def _parse_csv_set(value: str | None) -> set[str] | None:
    if value is None or value.strip() == "":
        return None
    return {part.strip() for part in value.split(",") if part.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate T2 Polymarket-native order-flow trigger candidates")
    parser.add_argument("--touch-dataset", default=str(DEFAULT_TOUCH_DB))
    parser.add_argument("--capture-glob", default=DEFAULT_CAPTURE_GLOB)
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--output-db", default=str(DEFAULT_OUTPUT_DB))
    parser.add_argument("--fee-rate-bps", type=float, default=DEFAULT_FEE_RATE_BPS)
    parser.add_argument("--bootstrap-samples", type=int, default=DEFAULT_BOOTSTRAP_SAMPLES)
    parser.add_argument("--null-samples", type=int, default=DEFAULT_NULL_SAMPLES)
    parser.add_argument("--min-segment-n", type=int, default=50)
    parser.add_argument("--max-null-p-value", type=float, default=0.05)
    parser.add_argument("--asset-allowlist", default=",".join(sorted(DEFAULT_ASSET_ALLOWLIST)))
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()
    summary = evaluate_order_flow_trigger(
        args.touch_dataset,
        args.capture_glob,
        report_path=args.report,
        output_db_path=args.output_db,
        fee_rate_bps=args.fee_rate_bps,
        bootstrap_samples=args.bootstrap_samples,
        null_samples=args.null_samples,
        min_segment_n=args.min_segment_n,
        max_null_p_value=args.max_null_p_value,
        asset_allowlist=_parse_csv_set(args.asset_allowlist),
        seed=args.seed,
    )
    print(
        "T2 order-flow trigger: "
        f"gate={summary.go_gate} features={summary.deduped_feature_rows}/{summary.raw_feature_rows} "
        f"segments={summary.segment_count} report={summary.report_path}"
    )
    if summary.best_segment is not None:
        best = summary.best_segment
        print(
            "Best segment: "
            f"{best.predicate}|{best.asset_slug}|{best.direction}|{best.lifecycle_bucket}|{best.level:.2f} "
            f"n={best.n} ev={best.ev_per_usdc:.6f} "
            f"ci={fmt_optional(best.ci95_lower)}..{fmt_optional(best.ci95_upper)} "
            f"null_p={fmt_optional(best.null_p_value)} gate={best.gate}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
