#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Iterable, Sequence

if __package__:
    from replay.tape_feed import DEFAULT_TAPE_DB_PATH
    from utils.logger import setup_logger
    from utils.paths import DATA_DIR, DB_PATH, ensure_runtime_dirs
else:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from replay.tape_feed import DEFAULT_TAPE_DB_PATH
    from utils.logger import setup_logger
    from utils.paths import DATA_DIR, DB_PATH, ensure_runtime_dirs

logger = setup_logger("pattern_discovery")
DEFAULT_OUTPUT_DB_PATH = DATA_DIR / "pattern_discovery.sqlite3"
DEFAULT_REPORT_PATH = DATA_DIR / "pattern_discovery_report.md"
DEFAULT_GRID = tuple(float(x) / 100.0 for x in range(0, 101, 10))
DEFAULT_DECISION_FRACTION = 0.40
DEFAULT_BOOTSTRAP_SAMPLES = 1000
DEFAULT_VALIDATION_TRAIN_FRACTION = 0.70
CATEGORY_FEE_RATE_V2 = {
    "crypto": 0.072,
    "sports": 0.03,
    "finance": 0.04,
    "politics": 0.04,
    "mentions": 0.04,
    "tech": 0.04,
    "technology": 0.04,
    "economics": 0.05,
    "culture": 0.05,
    "weather": 0.05,
    "other": 0.05,
    "general": 0.05,
    "other / general": 0.05,
    "geopolitics": 0.0,
}


@dataclass(frozen=True)
class TokenMeta:
    token_id: str
    market_id: str
    question: str
    asset_slug: str
    direction: str
    is_winner: int
    token_order: int | None
    category: str
    fees_enabled: int
    duration_hours: float
    start_ts: int
    closed_ts: int


@dataclass(frozen=True)
class TradePoint:
    timestamp: int
    price: float
    size: float
    market_id: str
    token_id: str
    source_file_id: int
    seq: int


@dataclass(frozen=True)
class PatternFeature:
    token_id: str
    market_id: str
    question: str
    asset_slug: str
    direction: str
    split: str
    category: str
    utc_hour: int
    day_of_week: int
    start_ts: int
    closed_ts: int
    decision_ts: int
    decision_fraction: float
    n_trades_prefix: int
    n_trades_full: int
    start_price: float
    decision_price: float
    end_price: float
    prefix_return: float
    prefix_abs_path: float
    prefix_efficiency: float
    prefix_min_price: float
    prefix_max_price: float
    prefix_range: float
    prefix_volatility: float
    early_return: float
    late_return: float
    max_runup: float
    max_drawdown: float
    crossing_count_05: int
    primary_label: str
    labels: str
    is_winner: int
    gross_pnl_per_share: float
    fee_per_share: float
    net_pnl_per_share: float


@dataclass(frozen=True)
class DiscoverySummary:
    dataset_db_path: str
    tape_db_path: str
    output_db_path: str
    report_path: str
    eligible_tokens: int
    processed_tokens: int
    skipped_no_trades: int
    decision_fraction: float
    grid: tuple[float, ...]

    def as_json(self) -> dict:
        return asdict(self)


def discover_market_patterns(
    dataset_db_path: str | Path = DB_PATH,
    tape_db_path: str | Path = DEFAULT_TAPE_DB_PATH,
    *,
    output_db_path: str | Path = DEFAULT_OUTPUT_DB_PATH,
    report_path: str | Path = DEFAULT_REPORT_PATH,
    market_duration_minutes: float = 15.0,
    assets: set[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    time_grid: Sequence[float] = DEFAULT_GRID,
    decision_fraction: float = DEFAULT_DECISION_FRACTION,
    bootstrap_samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
    validation_train_fraction: float = DEFAULT_VALIDATION_TRAIN_FRACTION,
) -> DiscoverySummary:
    ensure_runtime_dirs()
    dataset_db_path = Path(dataset_db_path)
    tape_db_path = Path(tape_db_path)
    output_db_path = Path(output_db_path)
    report_path = Path(report_path)
    output_db_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    grid = _normalize_grid(time_grid)
    metas = load_token_meta(
        dataset_db_path,
        market_duration_minutes=market_duration_minutes,
        assets=assets,
        start_ts=_parse_date_start(start_date),
        end_ts=_parse_date_end(end_date),
    )
    features, skipped_no_trades = build_pattern_features(
        tape_db_path,
        metas,
        grid=grid,
        decision_fraction=decision_fraction,
    )
    features = assign_time_splits(features, train_fraction=validation_train_fraction)
    write_output_db(output_db_path, features, summary_meta={
        "dataset_db_path": str(dataset_db_path),
        "tape_db_path": str(tape_db_path),
        "market_duration_minutes": market_duration_minutes,
        "assets": sorted(assets or []),
        "start_date": start_date,
        "end_date": end_date,
        "time_grid": list(grid),
        "decision_fraction": decision_fraction,
        "validation_train_fraction": validation_train_fraction,
    })
    aggregates = aggregate_features(features, bootstrap_samples=bootstrap_samples)
    insert_aggregates(output_db_path, aggregates)
    report_path.write_text(render_report(features, aggregates, summary_meta={
        "dataset_db_path": str(dataset_db_path),
        "tape_db_path": str(tape_db_path),
        "output_db_path": str(output_db_path),
        "market_duration_minutes": market_duration_minutes,
        "assets": sorted(assets or []),
        "start_date": start_date or "all",
        "end_date": end_date or "all",
        "decision_fraction": decision_fraction,
        "validation_train_fraction": validation_train_fraction,
        "grid": ",".join(f"{x:.2f}" for x in grid),
    }), encoding="utf-8")

    summary = DiscoverySummary(
        dataset_db_path=str(dataset_db_path),
        tape_db_path=str(tape_db_path),
        output_db_path=str(output_db_path),
        report_path=str(report_path),
        eligible_tokens=len(metas),
        processed_tokens=len(features),
        skipped_no_trades=skipped_no_trades,
        decision_fraction=float(decision_fraction),
        grid=tuple(grid),
    )
    return summary


def load_token_meta(
    dataset_db_path: Path,
    *,
    market_duration_minutes: float,
    assets: set[str] | None = None,
    start_ts: int | None = None,
    end_ts: int | None = None,
) -> dict[str, TokenMeta]:
    min_hours = float(market_duration_minutes) / 60.0
    max_hours = float(market_duration_minutes) / 60.0
    wanted_assets = {_normalize_asset(item) for item in (assets or set()) if item}
    conn = sqlite3.connect(dataset_db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                CAST(t.token_id AS TEXT) AS token_id,
                CAST(t.market_id AS TEXT) AS market_id,
                CAST(COALESCE(t.is_winner, 0) AS INTEGER) AS is_winner,
                t.token_order AS token_order,
                COALESCE(m.question, '') AS question,
                COALESCE(m.category, 'unknown') AS category,
                CAST(COALESCE(m.fees_enabled, 1) AS INTEGER) AS fees_enabled,
                CAST(COALESCE(m.duration_hours, 0) AS REAL) AS duration_hours,
                CAST(COALESCE(m.closed_time, 0) AS INTEGER) AS closed_time
            FROM tokens t
            JOIN markets m ON m.id = t.market_id
            WHERE m.closed_time IS NOT NULL
              AND CAST(m.closed_time AS INTEGER) > 0
              AND t.is_winner IS NOT NULL
            """
        ).fetchall()
    finally:
        conn.close()

    metas: dict[str, TokenMeta] = {}
    for row in rows:
        duration_hours = float(row["duration_hours"] or 0.0)
        if duration_hours + 1e-9 < min_hours or duration_hours - 1e-9 > max_hours:
            continue
        closed_ts = int(row["closed_time"] or 0)
        start = int(round(closed_ts - duration_hours * 3600.0))
        if start_ts is not None and closed_ts < start_ts:
            continue
        if end_ts is not None and start > end_ts:
            continue
        question = str(row["question"] or "")
        asset = _asset_from_question(question)
        if wanted_assets and _normalize_asset(asset) not in wanted_assets:
            continue
        token_id = str(row["token_id"])
        metas[token_id] = TokenMeta(
            token_id=token_id,
            market_id=str(row["market_id"]),
            question=question,
            asset_slug=asset,
            direction=_direction_from_token_order(row["token_order"]),
            is_winner=int(row["is_winner"] or 0),
            token_order=int(row["token_order"]) if row["token_order"] is not None else None,
            category=str(row["category"] or "unknown"),
            fees_enabled=int(row["fees_enabled"] if row["fees_enabled"] is not None else 1),
            duration_hours=duration_hours,
            start_ts=start,
            closed_ts=closed_ts,
        )
    return metas


def build_pattern_features(
    tape_db_path: Path,
    metas: dict[str, TokenMeta],
    *,
    grid: Sequence[float],
    decision_fraction: float,
) -> tuple[list[PatternFeature], int]:
    conn = sqlite3.connect(tape_db_path)
    conn.row_factory = sqlite3.Row
    features: list[PatternFeature] = []
    skipped_no_trades = 0
    current_token_id: str | None = None
    token_trades: list[TradePoint] = []

    def flush(token_id: str | None, trades: list[TradePoint]) -> None:
        nonlocal skipped_no_trades
        if token_id is None:
            return
        meta = metas.get(token_id)
        if meta is None:
            return
        if not trades:
            skipped_no_trades += 1
            return
        feature = feature_for_token(meta=meta, trades=trades, grid=grid, decision_fraction=decision_fraction)
        if feature is None:
            skipped_no_trades += 1
            return
        features.append(feature)

    try:
        rows = conn.execute(
            "SELECT source_file_id, seq, timestamp, market_id, token_id, price, size FROM tape ORDER BY token_id, timestamp, source_file_id, seq"
        )
        for row in rows:
            token_id = str(row["token_id"])
            if token_id not in metas:
                continue
            if current_token_id is None:
                current_token_id = token_id
            elif token_id != current_token_id:
                flush(current_token_id, token_trades)
                token_trades = []
                current_token_id = token_id
            token_trades.append(
                TradePoint(
                    timestamp=int(row["timestamp"]),
                    price=float(row["price"]),
                    size=float(row["size"] or 0.0),
                    market_id=str(row["market_id"]),
                    token_id=token_id,
                    source_file_id=int(row["source_file_id"] or 0),
                    seq=int(row["seq"] or 0),
                )
            )
        flush(current_token_id, token_trades)
    finally:
        conn.close()
    return features, skipped_no_trades


def feature_for_token(*, meta: TokenMeta, trades: Sequence[TradePoint], grid: Sequence[float], decision_fraction: float) -> PatternFeature | None:
    in_window = [t for t in trades if meta.start_ts <= int(t.timestamp) <= meta.closed_ts]
    if len(in_window) < 2:
        return None
    in_window.sort(key=lambda t: (t.timestamp, t.source_file_id, t.seq))
    decision_ts = int(round(meta.start_ts + (meta.closed_ts - meta.start_ts) * float(decision_fraction)))
    prefix = [t for t in in_window if int(t.timestamp) <= decision_ts]
    if len(prefix) < 2:
        return None

    sampled = sample_prices(in_window, start_ts=meta.start_ts, closed_ts=meta.closed_ts, grid=grid)
    start_price = sampled[0]
    decision_price = price_at_or_before(in_window, decision_ts)
    if decision_price is None:
        return None
    end_price = sampled[-1]
    prefix_prices = [t.price for t in prefix]
    returns = [prefix_prices[idx] - prefix_prices[idx - 1] for idx in range(1, len(prefix_prices))]
    abs_path = sum(abs(x) for x in returns)
    prefix_return = decision_price - start_price
    efficiency = abs(prefix_return) / abs_path if abs_path > 1e-12 else 0.0
    prefix_min = min(prefix_prices)
    prefix_max = max(prefix_prices)
    prefix_range = prefix_max - prefix_min
    volatility = pstdev(returns) if len(returns) > 1 else 0.0
    split_idx = max(1, len(prefix_prices) // 2)
    early_return = prefix_prices[split_idx - 1] - prefix_prices[0]
    late_return = prefix_prices[-1] - prefix_prices[split_idx - 1]
    max_runup = prefix_max - start_price
    max_drawdown = start_price - prefix_min
    crossing_count = _count_crossings(prefix_prices, step=0.05)
    labels = classify_pattern(
        prefix_return=prefix_return,
        prefix_range=prefix_range,
        efficiency=efficiency,
        early_return=early_return,
        late_return=late_return,
        max_runup=max_runup,
        max_drawdown=max_drawdown,
        crossing_count=crossing_count,
        volatility=volatility,
    )
    primary = labels[0]
    gross = (1.0 - decision_price) if meta.is_winner else -decision_price
    fee = _entry_fee_per_share(decision_price, _fee_rate_for_category(meta.category, fees_enabled=meta.fees_enabled), fees_enabled=meta.fees_enabled)
    return PatternFeature(
        token_id=meta.token_id,
        market_id=meta.market_id,
        question=meta.question,
        asset_slug=meta.asset_slug,
        direction=meta.direction,
        split="all",
        category=meta.category,
        utc_hour=datetime.fromtimestamp(meta.start_ts, tz=timezone.utc).hour,
        day_of_week=datetime.fromtimestamp(meta.start_ts, tz=timezone.utc).weekday(),
        start_ts=meta.start_ts,
        closed_ts=meta.closed_ts,
        decision_ts=decision_ts,
        decision_fraction=float(decision_fraction),
        n_trades_prefix=len(prefix),
        n_trades_full=len(in_window),
        start_price=start_price,
        decision_price=decision_price,
        end_price=end_price,
        prefix_return=prefix_return,
        prefix_abs_path=abs_path,
        prefix_efficiency=efficiency,
        prefix_min_price=prefix_min,
        prefix_max_price=prefix_max,
        prefix_range=prefix_range,
        prefix_volatility=volatility,
        early_return=early_return,
        late_return=late_return,
        max_runup=max_runup,
        max_drawdown=max_drawdown,
        crossing_count_05=crossing_count,
        primary_label=primary,
        labels=",".join(labels),
        is_winner=int(meta.is_winner),
        gross_pnl_per_share=gross,
        fee_per_share=fee,
        net_pnl_per_share=gross - fee,
    )


def sample_prices(trades: Sequence[TradePoint], *, start_ts: int, closed_ts: int, grid: Sequence[float]) -> list[float]:
    values: list[float] = []
    idx = 0
    last_price = trades[0].price
    for frac in grid:
        target = int(round(start_ts + (closed_ts - start_ts) * float(frac)))
        while idx < len(trades) and int(trades[idx].timestamp) <= target:
            last_price = float(trades[idx].price)
            idx += 1
        values.append(last_price)
    return values


def price_at_or_before(trades: Sequence[TradePoint], ts: int) -> float | None:
    price: float | None = None
    for trade in trades:
        if int(trade.timestamp) > ts:
            break
        price = float(trade.price)
    return price


def classify_pattern(
    *,
    prefix_return: float,
    prefix_range: float,
    efficiency: float,
    early_return: float,
    late_return: float,
    max_runup: float,
    max_drawdown: float,
    crossing_count: int,
    volatility: float,
) -> list[str]:
    labels: list[str] = []
    strong = 0.10
    medium = 0.06
    flat = 0.025
    if prefix_return >= strong and efficiency >= 0.65:
        labels.append("monotonic_uptrend")
    elif prefix_return <= -strong and efficiency >= 0.65:
        labels.append("monotonic_downtrend")
    elif abs(early_return) <= flat and late_return >= medium:
        labels.append("flat_then_breakout_up")
    elif abs(early_return) <= flat and late_return <= -medium:
        labels.append("flat_then_breakout_down")
    elif early_return >= medium and late_return <= -medium:
        labels.append("breakout_then_reversal_down")
    elif early_return <= -medium and late_return >= medium:
        labels.append("dip_then_reversal_up")
    elif max_drawdown >= strong and prefix_return >= -flat:
        labels.append("v_shape_or_dip_recovery")
    elif max_runup >= strong and prefix_return <= flat:
        labels.append("inverted_v_or_spike_fade")
    elif prefix_range >= 0.25 and efficiency < 0.35:
        labels.append("high_volatility_chop")
    elif crossing_count >= 4 or (volatility >= 0.04 and efficiency < 0.45):
        labels.append("choppy_mean_reverting")
    elif 0 < prefix_return < strong:
        labels.append("slow_grind_up")
    elif -strong < prefix_return < 0:
        labels.append("slow_grind_down")
    else:
        labels.append("flat_or_unclear")

    if prefix_range >= 0.20:
        labels.append("wide_range")
    if efficiency >= 0.70:
        labels.append("directional")
    if crossing_count >= 4:
        labels.append("many_level_crossings")
    return labels


def assign_time_splits(features: Sequence[PatternFeature], *, train_fraction: float) -> list[PatternFeature]:
    if not features:
        return []
    train_fraction = max(0.0, min(1.0, float(train_fraction)))
    ordered = sorted(features, key=lambda item: (item.start_ts, item.token_id))
    cutoff = int(round(len(ordered) * train_fraction))
    cutoff = min(max(cutoff, 1), len(ordered)) if len(ordered) > 1 else len(ordered)
    train_ids = {item.token_id for item in ordered[:cutoff]}
    return [replace(item, split="train" if item.token_id in train_ids else "test") for item in features]


def aggregate_features(features: Sequence[PatternFeature], *, bootstrap_samples: int) -> list[dict]:
    groupers = {
        "pattern": lambda f: (f.primary_label,),
        "asset_pattern": lambda f: (f.asset_slug, f.primary_label),
        "asset_direction_pattern": lambda f: (f.asset_slug, f.direction, f.primary_label),
        "split_pattern": lambda f: (f.split, f.primary_label),
        "utc_hour_pattern": lambda f: (str(f.utc_hour), f.primary_label),
        "asset_utc_hour": lambda f: (f.asset_slug, str(f.utc_hour)),
    }
    rows: list[dict] = []
    for group_type, key_fn in groupers.items():
        buckets: dict[tuple[str, ...], list[PatternFeature]] = defaultdict(list)
        for feature in features:
            buckets[key_fn(feature)].append(feature)
        for key, items in buckets.items():
            rows.append(_aggregate_row(group_type, key, items, bootstrap_samples=bootstrap_samples))
    rows.sort(key=lambda r: (r["group_type"], -int(r["n"]), str(r["group_key"])))
    return rows


def _aggregate_row(group_type: str, key: tuple[str, ...], items: Sequence[PatternFeature], *, bootstrap_samples: int) -> dict:
    pnls = [float(item.net_pnl_per_share) for item in items]
    wins = sum(int(item.is_winner) for item in items)
    avg_entry = mean(float(item.decision_price) for item in items)
    avg_gross = mean(float(item.gross_pnl_per_share) for item in items)
    avg_fee = mean(float(item.fee_per_share) for item in items)
    avg_net = mean(pnls) if pnls else 0.0
    ci_low, ci_high = bootstrap_mean_ci(pnls, samples=bootstrap_samples)
    return {
        "group_type": group_type,
        "group_key": "|".join(key),
        "n": len(items),
        "wins": wins,
        "win_rate": wins / len(items) if items else 0.0,
        "avg_entry_price": avg_entry,
        "avg_gross_pnl_per_share": avg_gross,
        "avg_fee_per_share": avg_fee,
        "avg_net_pnl_per_share": avg_net,
        "ci95_low": ci_low,
        "ci95_high": ci_high,
        "frequency_per_day": _frequency_per_day(items),
    }


def bootstrap_mean_ci(values: Sequence[float], *, samples: int) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    if len(values) == 1 or samples <= 0:
        value = float(mean(values))
        return value, value
    # Deterministic lightweight bootstrap without importing random state into reports.
    import random

    rng = random.Random(2602)
    means: list[float] = []
    n = len(values)
    for _ in range(int(samples)):
        means.append(sum(values[rng.randrange(n)] for _ in range(n)) / n)
    means.sort()
    lo = means[int(0.025 * (len(means) - 1))]
    hi = means[int(0.975 * (len(means) - 1))]
    return lo, hi


def write_output_db(path: Path, features: Sequence[PatternFeature], *, summary_meta: dict) -> None:
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            CREATE TABLE pattern_run_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE pattern_features(
                token_id TEXT PRIMARY KEY,
                market_id TEXT NOT NULL,
                question TEXT,
                asset_slug TEXT,
                direction TEXT,
                split TEXT,
                category TEXT,
                utc_hour INTEGER,
                day_of_week INTEGER,
                start_ts INTEGER,
                closed_ts INTEGER,
                decision_ts INTEGER,
                decision_fraction REAL,
                n_trades_prefix INTEGER,
                n_trades_full INTEGER,
                start_price REAL,
                decision_price REAL,
                end_price REAL,
                prefix_return REAL,
                prefix_abs_path REAL,
                prefix_efficiency REAL,
                prefix_min_price REAL,
                prefix_max_price REAL,
                prefix_range REAL,
                prefix_volatility REAL,
                early_return REAL,
                late_return REAL,
                max_runup REAL,
                max_drawdown REAL,
                crossing_count_05 INTEGER,
                primary_label TEXT,
                labels TEXT,
                is_winner INTEGER,
                gross_pnl_per_share REAL,
                fee_per_share REAL,
                net_pnl_per_share REAL
            );
            CREATE INDEX idx_pattern_features_label ON pattern_features(primary_label);
            CREATE INDEX idx_pattern_features_asset_label ON pattern_features(asset_slug, primary_label);
            CREATE INDEX idx_pattern_features_hour ON pattern_features(utc_hour);
            """
        )
        for key, value in summary_meta.items():
            conn.execute("INSERT INTO pattern_run_meta(key, value) VALUES (?, ?)", (str(key), json.dumps(value, sort_keys=True)))
        conn.executemany(
            """
            INSERT INTO pattern_features VALUES (
                :token_id, :market_id, :question, :asset_slug, :direction, :split, :category, :utc_hour, :day_of_week,
                :start_ts, :closed_ts, :decision_ts, :decision_fraction, :n_trades_prefix, :n_trades_full,
                :start_price, :decision_price, :end_price, :prefix_return, :prefix_abs_path, :prefix_efficiency,
                :prefix_min_price, :prefix_max_price, :prefix_range, :prefix_volatility, :early_return,
                :late_return, :max_runup, :max_drawdown, :crossing_count_05, :primary_label, :labels,
                :is_winner, :gross_pnl_per_share, :fee_per_share, :net_pnl_per_share
            )
            """,
            [asdict(feature) for feature in features],
        )
        conn.commit()
    finally:
        conn.close()


def insert_aggregates(path: Path, aggregates: Sequence[dict]) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            DROP TABLE IF EXISTS pattern_aggregates;
            CREATE TABLE pattern_aggregates(
                group_type TEXT NOT NULL,
                group_key TEXT NOT NULL,
                n INTEGER NOT NULL,
                wins INTEGER NOT NULL,
                win_rate REAL NOT NULL,
                avg_entry_price REAL NOT NULL,
                avg_gross_pnl_per_share REAL NOT NULL,
                avg_fee_per_share REAL NOT NULL,
                avg_net_pnl_per_share REAL NOT NULL,
                ci95_low REAL,
                ci95_high REAL,
                frequency_per_day REAL,
                PRIMARY KEY(group_type, group_key)
            );
            """
        )
        conn.executemany(
            """
            INSERT INTO pattern_aggregates VALUES (
                :group_type, :group_key, :n, :wins, :win_rate, :avg_entry_price,
                :avg_gross_pnl_per_share, :avg_fee_per_share, :avg_net_pnl_per_share,
                :ci95_low, :ci95_high, :frequency_per_day
            )
            """,
            list(aggregates),
        )
        conn.commit()
    finally:
        conn.close()


def render_report(features: Sequence[PatternFeature], aggregates: Sequence[dict], *, summary_meta: dict) -> str:
    lines: list[str] = []
    lines.append("# Pattern discovery report")
    lines.append("")
    lines.append(f"Generated at: `{datetime.now(timezone.utc).isoformat()}`")
    lines.append("")
    lines.append("## Scope")
    for key, value in summary_meta.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.append(f"- processed token trajectories: `{len(features)}`")
    lines.append("")
    lines.append("## Read me first")
    lines.append("- Labels are heuristic and based only on the trajectory prefix up to the configured decision fraction.")
    lines.append("- PnL is a fee-aware per-share hold-to-resolution estimate at the decision price; it is not a live execution claim.")
    lines.append("- Promising groups need collector/live-like validation before any trading use.")
    lines.append("")
    lines.append("## Pattern leaderboard")
    _append_aggregate_section(lines, aggregates, group_type="pattern", limit=20)
    lines.append("")
    lines.append("## BTC vs ETH / asset-pattern comparison")
    _append_aggregate_section(lines, aggregates, group_type="asset_pattern", limit=30)
    lines.append("")
    lines.append("## Asset + direction + pattern")
    _append_aggregate_section(lines, aggregates, group_type="asset_direction_pattern", limit=30)
    lines.append("")
    lines.append("## Date-split pattern stability")
    _append_aggregate_section(lines, aggregates, group_type="split_pattern", limit=30)
    lines.append("")
    lines.append("## Intraday pattern slices")
    _append_aggregate_section(lines, aggregates, group_type="utc_hour_pattern", limit=30)
    lines.append("")
    lines.append("## Label counts")
    counts = Counter(item.primary_label for item in features)
    for label, count in counts.most_common():
        lines.append(f"- `{label}`: `{count}`")
    lines.append("")
    lines.append("## Next checks")
    lines.append("- Compare top historical patterns with collector/touch_dataset executable filters.")
    lines.append("- Add spot-regime features when a spot cache is available for the same date range.")
    lines.append("- Require date-split stability before promoting any pattern to a candidate rule.")
    lines.append("")
    return "\n".join(lines)


def _append_aggregate_section(lines: list[str], aggregates: Sequence[dict], *, group_type: str, limit: int) -> None:
    rows = [row for row in aggregates if row["group_type"] == group_type]
    rows.sort(key=lambda r: (float(r["avg_net_pnl_per_share"]), int(r["n"])), reverse=True)
    for row in rows[:limit]:
        ci = "n/a" if row["ci95_low"] is None else f"{row['ci95_low']:+.4f}..{row['ci95_high']:+.4f}"
        lines.append(
            f"- `{row['group_key']}`: n=`{row['n']}`, win=`{row['win_rate']:.1%}`, "
            f"entry=`{row['avg_entry_price']:.4f}`, net/share=`{row['avg_net_pnl_per_share']:+.4f}`, "
            f"CI95=`{ci}`, freq/day=`{(row['frequency_per_day'] or 0.0):.2f}`"
        )


def _frequency_per_day(items: Sequence[PatternFeature]) -> float | None:
    if not items:
        return None
    start = min(item.start_ts for item in items)
    end = max(item.closed_ts for item in items)
    days = max(1.0 / 24.0, (end - start) / 86400.0)
    return len(items) / days


def _count_crossings(prices: Sequence[float], *, step: float) -> int:
    count = 0
    for left, right in zip(prices, prices[1:]):
        if abs(right - left) < 1e-12:
            continue
        lo, hi = sorted((left, right))
        first = math.floor(lo / step) + 1
        last = math.floor(hi / step)
        count += max(0, last - first + 1)
    return count


def _fee_rate_for_category(category: str, *, fees_enabled: int) -> float:
    if int(fees_enabled or 0) == 0:
        return 0.0
    value = (category or "").strip().lower()
    if value in {"science & tech", "science and tech"}:
        value = "tech"
    if value in {"other/general", "other-general"}:
        value = "other / general"
    return float(CATEGORY_FEE_RATE_V2.get(value, 0.05))


def _entry_fee_per_share(price: float, fee_rate: float, *, fees_enabled: int) -> float:
    if int(fees_enabled or 0) == 0:
        return 0.0
    p = max(0.0, min(1.0, float(price)))
    return float(fee_rate) * p * (1.0 - p)


def _asset_from_question(question: str) -> str:
    text = (question or "").lower()
    mapping = [
        ("bitcoin", "btc"), ("btc", "btc"),
        ("ethereum", "eth"), ("eth", "eth"),
        ("solana", "sol"), ("sol", "sol"),
        ("xrp", "xrp"), ("ripple", "xrp"),
        ("dogecoin", "doge"), ("doge", "doge"),
        ("binance", "bnb"), ("bnb", "bnb"),
        ("hyperliquid", "hype"), ("hype", "hype"),
    ]
    for needle, slug in mapping:
        if needle in text:
            return slug
    return "unknown"


def _normalize_asset(value: str) -> str:
    text = str(value or "").strip().lower()
    return {"bitcoin": "btc", "ethereum": "eth", "solana": "sol", "ripple": "xrp", "dogecoin": "doge"}.get(text, text)


def _direction_from_token_order(token_order: object) -> str:
    if token_order is None:
        return "unknown"
    try:
        order = int(token_order)
    except (TypeError, ValueError):
        return "unknown"
    if order == 0:
        return "UP/YES"
    if order == 1:
        return "DOWN/NO"
    return "unknown"


def _normalize_grid(values: Sequence[float]) -> tuple[float, ...]:
    cleaned = sorted({round(float(v), 6) for v in values if 0.0 <= float(v) <= 1.0})
    if not cleaned or cleaned[0] != 0.0:
        cleaned.insert(0, 0.0)
    if cleaned[-1] != 1.0:
        cleaned.append(1.0)
    return tuple(cleaned)


def _parse_date_start(value: str | None) -> int | None:
    if not value:
        return None
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp())


def _parse_date_end(value: str | None) -> int | None:
    if not value:
        return None
    if len(value) == 10:
        value = value + "T23:59:59+00:00"
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover interpretable 15m crypto market movement patterns from historical tape.")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to polymarket_dataset.db")
    parser.add_argument("--tape-db", default=str(DEFAULT_TAPE_DB_PATH), help="Path to historical_tape.db")
    parser.add_argument("--output-db", default=str(DEFAULT_OUTPUT_DB_PATH))
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--market-duration-minutes", type=float, default=15.0)
    parser.add_argument("--assets", nargs="*", default=None, help="Asset allowlist, e.g. btc eth sol xrp")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--time-grid", default=",".join(str(int(x * 100)) for x in DEFAULT_GRID), help="Comma-separated lifecycle percent grid, e.g. 0,10,...,100")
    parser.add_argument("--decision-fraction", type=float, default=DEFAULT_DECISION_FRACTION)
    parser.add_argument("--bootstrap-samples", type=int, default=DEFAULT_BOOTSTRAP_SAMPLES)
    parser.add_argument("--validation-train-fraction", type=float, default=DEFAULT_VALIDATION_TRAIN_FRACTION)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    grid = tuple(float(item.strip()) / 100.0 for item in str(args.time_grid).split(",") if item.strip())
    summary = discover_market_patterns(
        args.db,
        args.tape_db,
        output_db_path=args.output_db,
        report_path=args.report_path,
        market_duration_minutes=float(args.market_duration_minutes),
        assets={_normalize_asset(item) for item in args.assets} if args.assets else None,
        start_date=args.start_date,
        end_date=args.end_date,
        time_grid=grid,
        decision_fraction=float(args.decision_fraction),
        bootstrap_samples=int(args.bootstrap_samples),
        validation_train_fraction=float(args.validation_train_fraction),
    )
    print(json.dumps(summary.as_json(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
