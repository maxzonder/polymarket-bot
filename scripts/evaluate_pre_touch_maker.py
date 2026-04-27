#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import glob
import json
import math
import random
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

DEFAULT_TOUCH_DB = Path("/home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3")
DEFAULT_CAPTURE_GLOB = "/home/polybot/.polybot/capture_bundles/*/events_log.jsonl"
DEFAULT_REPORT_PATH = Path("v2/short_horizon/docs/phase6/t3_pre_touch_maker.md")
DEFAULT_OUTPUT_DB = Path("v2/short_horizon/data/pre_touch_maker.sqlite3")
DEFAULT_MIN_ORDER_SHARES = 5.0
DEFAULT_SEED = 1723
ASSET_ALLOWLIST = {"btc", "eth", "sol", "xrp"}
LEVELS = {0.55, 0.65, 0.70}
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
    touch_time_iso: str
    touch_ms: int
    asset_slug: str
    direction: str
    touch_level: float
    lifecycle_fraction: float | None
    resolves_yes: int
    tick_size: float | None


@dataclass(frozen=True)
class BookPoint:
    ts_ms: int
    best_bid: float | None
    best_ask: float | None

    @property
    def spread(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def mid(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0


@dataclass(frozen=True)
class TradePoint:
    ts_ms: int
    price: float
    size: float
    aggressor_side: str | None


@dataclass(frozen=True)
class BundleSlice:
    bundle_name: str
    path: Path
    touches: list[TouchRow]
    books_by_token: dict[str, list[BookPoint]]
    trades_by_token: dict[str, list[TradePoint]]
    has_l2_levels: bool


@dataclass(frozen=True)
class MakerConfig:
    stake_usdc: float
    queue_factor: float
    max_ticks_from_level: int
    trigger_lookback_ms: int
    cancel_after_ms: int
    momentum_lookback_ms: int
    require_mid_momentum: bool
    require_spread_compression: bool
    min_order_shares: float = DEFAULT_MIN_ORDER_SHARES

    @property
    def label(self) -> str:
        return (
            f"stake={self.stake_usdc:g}|q={self.queue_factor:g}|ticks={self.max_ticks_from_level}|"
            f"lookback={self.trigger_lookback_ms // 1000}s|cancel={self.cancel_after_ms // 1000}s|"
            f"mom={int(self.require_mid_momentum)}|spread={int(self.require_spread_compression)}"
        )


@dataclass(frozen=True)
class MakerReplay:
    config_label: str
    bundle_name: str
    probe_id: str
    market_id: str
    token_id: str
    asset_slug: str
    direction: str
    lifecycle_bucket: str
    touch_level: float
    resolves_yes: int
    status: str
    trigger_ms: int | None
    trigger_delta_ms: int | None
    trigger_best_bid: float | None
    trigger_best_ask: float | None
    ask_reached_level: bool
    order_shares: float
    required_sell_volume: float
    observed_sell_volume: float
    cost_usdc: float
    pnl_usdc: float


@dataclass(frozen=True)
class SegmentSummary:
    config_label: str
    asset_slug: str
    direction: str
    lifecycle_bucket: str
    level: float
    fired_n: int
    post_only_safe_n: int
    filled_n: int
    fill_rate: float | None
    gross_cost_usdc: float
    pnl_usdc: float
    ev_per_usdc: float | None
    ci95_lower: float | None
    ci95_upper: float | None
    gate: str

    def as_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EvaluationSummary:
    touch_db: str
    capture_glob: str
    output_db: str | None
    report_path: str
    bundle_count: int
    candidate_appearances: int
    replay_rows: int
    segment_count: int
    best_segment: SegmentSummary | None
    go_gate: str
    any_l2_levels: bool

    def as_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["best_segment"] = self.best_segment.as_json() if self.best_segment else None
        return payload


def evaluate_pre_touch_maker(
    touch_db: str | Path = DEFAULT_TOUCH_DB,
    capture_glob: str = DEFAULT_CAPTURE_GLOB,
    *,
    output_db: str | Path | None = DEFAULT_OUTPUT_DB,
    report_path: str | Path = DEFAULT_REPORT_PATH,
    bootstrap_samples: int = 1000,
    seed: int = DEFAULT_SEED,
) -> EvaluationSummary:
    touches = load_touch_rows(touch_db)
    configs = default_configs()
    slices = load_slices(capture_glob, touches, max_lookback_ms=max(c.trigger_lookback_ms for c in configs))
    replays: list[MakerReplay] = []
    for config in configs:
        for bundle in slices:
            for touch in bundle.touches:
                replays.append(
                    replay_pre_touch(
                        touch,
                        bundle_name=bundle.bundle_name,
                        books=bundle.books_by_token.get(touch.token_id, []),
                        trades=bundle.trades_by_token.get(touch.token_id, []),
                        config=config,
                    )
                )
    segments = summarize_segments(replays, bootstrap_samples=bootstrap_samples, seed=seed)
    passing = [segment for segment in segments if segment.gate == "GO"]
    sampled = [segment for segment in segments if segment.filled_n >= 1]
    best_pool = passing or sampled or segments
    best = max(best_pool, key=lambda s: ((s.ev_per_usdc if s.ev_per_usdc is not None else -999), s.filled_n, s.fired_n), default=None)
    summary = EvaluationSummary(
        touch_db=str(touch_db),
        capture_glob=capture_glob,
        output_db=str(output_db) if output_db is not None else None,
        report_path=str(report_path),
        bundle_count=len(slices),
        candidate_appearances=sum(len(bundle.touches) for bundle in slices),
        replay_rows=len(replays),
        segment_count=len(segments),
        best_segment=best,
        go_gate="GO" if passing else "NO-GO",
        any_l2_levels=any(bundle.has_l2_levels for bundle in slices),
    )
    if output_db is not None:
        write_output_db(output_db, replays, segments, summary)
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_report(summary, segments), encoding="utf-8")
    return summary


def default_configs() -> list[MakerConfig]:
    # Keep the first T3 pass deliberately bounded. The capture bundles are large
    # and BBO-only; broader momentum/spread sweeps can be a follow-up if this
    # simple pre-touch maker geometry shows life.
    return [
        MakerConfig(
            stake_usdc=stake,
            queue_factor=q,
            max_ticks_from_level=2,
            trigger_lookback_ms=lookback,
            cancel_after_ms=cancel,
            momentum_lookback_ms=30_000,
            require_mid_momentum=momentum,
            require_spread_compression=spread,
        )
        for stake in (1.0,)
        for q in (1.0, 2.0, 5.0)
        for lookback in (60_000, 120_000)
        for cancel in (60_000, 120_000)
        for momentum in (False,)
        for spread in (False,)
    ]


def load_touch_rows(path: str | Path) -> list[TouchRow]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='touch_dataset'").fetchone() is None:
            raise ValueError(f"{path} does not contain table touch_dataset")
        rows = conn.execute(
            """
            SELECT probe_id, market_id, token_id, touch_time_iso, asset_slug, direction,
                   touch_level, lifecycle_fraction, resolves_yes, tick_size
            FROM touch_dataset
            WHERE asset_slug IN ('btc', 'eth', 'sol', 'xrp')
              AND touch_level IN (0.55, 0.65, 0.70)
            ORDER BY touch_time_iso, probe_id
            """
        ).fetchall()
        out: list[TouchRow] = []
        for row in rows:
            touch_ms = parse_ts_ms(row["touch_time_iso"])
            if touch_ms is None:
                continue
            out.append(
                TouchRow(
                    probe_id=str(row["probe_id"]),
                    market_id=str(row["market_id"]),
                    token_id=str(row["token_id"]),
                    touch_time_iso=str(row["touch_time_iso"]),
                    touch_ms=touch_ms,
                    asset_slug=str(row["asset_slug"]).lower(),
                    direction=str(row["direction"]),
                    touch_level=float(row["touch_level"]),
                    lifecycle_fraction=_float_or_none(row["lifecycle_fraction"]),
                    resolves_yes=int(row["resolves_yes"]),
                    tick_size=_float_or_none(row["tick_size"]),
                )
            )
        return out
    finally:
        conn.close()


def load_slices(capture_glob: str, touches: Sequence[TouchRow], *, max_lookback_ms: int) -> list[BundleSlice]:
    paths = sorted(glob.glob(capture_glob))
    slices: list[BundleSlice] = []
    for path in paths:
        loaded = load_bundle_slice(path, touches, max_lookback_ms=max_lookback_ms)
        if loaded is not None:
            slices.append(loaded)
    return slices


def load_bundle_slice(path: str | Path, touches: Sequence[TouchRow], *, max_lookback_ms: int) -> BundleSlice | None:
    path = Path(path)
    token_ids = {touch.token_id for touch in touches}
    books_by_token: dict[str, list[BookPoint]] = {}
    trades_by_token: dict[str, list[TradePoint]] = {}
    min_ts: int | None = None
    max_ts: int | None = None
    has_l2 = False
    with path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            if not raw.strip():
                continue
            payload = json.loads(raw)
            ts = parse_ts_ms(payload.get("event_time_ms", payload.get("event_time")))
            if ts is None:
                continue
            min_ts = ts if min_ts is None else min(min_ts, ts)
            max_ts = ts if max_ts is None else max(max_ts, ts)
            token_id = str(payload.get("token_id") or "")
            if token_id not in token_ids:
                continue
            typ = payload.get("event_type")
            if typ == "BookUpdate":
                has_l2 = has_l2 or bool(payload.get("bid_levels") or payload.get("ask_levels"))
                books_by_token.setdefault(token_id, []).append(
                    BookPoint(ts_ms=ts, best_bid=_float_or_none(payload.get("best_bid")), best_ask=_float_or_none(payload.get("best_ask")))
                )
            elif typ == "TradeTick":
                price = _float_or_none(payload.get("price"))
                size = _float_or_none(payload.get("size"))
                if price is None or size is None:
                    continue
                side = payload.get("aggressor_side")
                trades_by_token.setdefault(token_id, []).append(
                    TradePoint(ts_ms=ts, price=price, size=size, aggressor_side=str(side).lower() if side else None)
                )
    if min_ts is None or max_ts is None:
        return None
    for values in books_by_token.values():
        values.sort(key=lambda x: x.ts_ms)
    for values in trades_by_token.values():
        values.sort(key=lambda x: x.ts_ms)
    local = [
        touch for touch in touches
        if min_ts - max_lookback_ms <= touch.touch_ms <= max_ts + 120_000
        and (touch.token_id in books_by_token or touch.token_id in trades_by_token)
    ]
    if not local:
        return None
    return BundleSlice(path.parent.name, path, local, books_by_token, trades_by_token, has_l2)


def replay_pre_touch(
    touch: TouchRow,
    *,
    bundle_name: str,
    books: Sequence[BookPoint],
    trades: Sequence[TradePoint],
    config: MakerConfig,
) -> MakerReplay:
    bucket = lifecycle_bucket(touch.lifecycle_fraction) or "unknown"
    order_shares = order_shares_for_stake(stake_usdc=config.stake_usdc, entry_price=touch.touch_level, min_order_shares=config.min_order_shares)
    required_sell_volume = order_shares * config.queue_factor
    trigger = find_trigger_book(touch, books, config)
    if trigger is None:
        return _replay(touch, config, bundle_name, bucket, "no_fire", None, False, order_shares, required_sell_volume, 0.0, 0.0, 0.0)
    # The trigger rule should already ensure post-only safety. Keep this guard for corrupted/misaligned bundles.
    if trigger.best_ask is not None and trigger.best_ask <= touch.touch_level + 1e-9:
        return _replay(touch, config, bundle_name, bucket, "post_only_would_cross", trigger, False, order_shares, required_sell_volume, 0.0, 0.0, 0.0)
    cancel_ms = trigger.ts_ms + config.cancel_after_ms
    ask_reached = any(
        book.best_ask is not None and book.best_ask <= touch.touch_level + 1e-9
        for book in books_between(books, trigger.ts_ms, cancel_ms)
    )
    observed_sell_volume = 0.0
    if ask_reached:
        for trade in trades_between(trades, trigger.ts_ms, cancel_ms):
            if trade.aggressor_side == "sell" and trade.price <= touch.touch_level + 1e-9:
                observed_sell_volume += trade.size
    if ask_reached and observed_sell_volume >= required_sell_volume:
        cost = order_shares * touch.touch_level
        pnl = (order_shares if touch.resolves_yes else 0.0) - cost
        return _replay(touch, config, bundle_name, bucket, "filled", trigger, ask_reached, order_shares, required_sell_volume, observed_sell_volume, cost, pnl)
    return _replay(touch, config, bundle_name, bucket, "unfilled", trigger, ask_reached, order_shares, required_sell_volume, observed_sell_volume, 0.0, 0.0)


def find_trigger_book(touch: TouchRow, books: Sequence[BookPoint], config: MakerConfig) -> BookPoint | None:
    if not books:
        return None
    start_ms = touch.touch_ms - config.trigger_lookback_ms
    tick = touch.tick_size if touch.tick_size and touch.tick_size > 0 else 0.01
    max_ask = touch.touch_level + config.max_ticks_from_level * tick + 1e-9
    for book in books_between(books, start_ms, touch.touch_ms - 1):
        if book.best_ask is None:
            continue
        if not (touch.touch_level + 1e-9 < book.best_ask <= max_ask):
            continue
        if config.require_mid_momentum and not has_positive_mid_momentum(books, book.ts_ms, config.momentum_lookback_ms):
            continue
        if config.require_spread_compression and not has_spread_compression(books, book.ts_ms, config.momentum_lookback_ms):
            continue
        return book
    return None


def books_between(books: Sequence[BookPoint], start_ms: int, end_ms: int) -> Sequence[BookPoint]:
    if not books or end_ms < start_ms:
        return ()
    times = [book.ts_ms for book in books]
    left = bisect.bisect_left(times, start_ms)
    right = bisect.bisect_right(times, end_ms)
    return books[left:right]


def trades_between(trades: Sequence[TradePoint], start_ms: int, end_ms: int) -> Sequence[TradePoint]:
    if not trades or end_ms < start_ms:
        return ()
    times = [trade.ts_ms for trade in trades]
    left = bisect.bisect_left(times, start_ms)
    right = bisect.bisect_right(times, end_ms)
    return trades[left:right]


def has_positive_mid_momentum(books: Sequence[BookPoint], ts_ms: int, lookback_ms: int) -> bool:
    current = last_book_at_or_before(books, ts_ms)
    previous = last_book_at_or_before(books, ts_ms - lookback_ms)
    return current is not None and previous is not None and current.mid is not None and previous.mid is not None and current.mid > previous.mid


def has_spread_compression(books: Sequence[BookPoint], ts_ms: int, lookback_ms: int) -> bool:
    current = last_book_at_or_before(books, ts_ms)
    if current is None or current.spread is None:
        return False
    spreads = [book.spread for book in books if ts_ms - lookback_ms <= book.ts_ms <= ts_ms and book.spread is not None]
    if len(spreads) < 3:
        return False
    return current.spread < median(spreads)


def last_book_at_or_before(books: Sequence[BookPoint], ts_ms: int) -> BookPoint | None:
    if not books:
        return None
    times = [book.ts_ms for book in books]
    idx = bisect.bisect_right(times, ts_ms) - 1
    return books[idx] if idx >= 0 else None


def summarize_segments(replays: Sequence[MakerReplay], *, bootstrap_samples: int, seed: int) -> list[SegmentSummary]:
    groups: dict[tuple[str, str, str, str, float], list[MakerReplay]] = {}
    for replay in replays:
        if replay.status == "no_fire":
            continue
        key = (replay.config_label, replay.asset_slug, replay.direction, replay.lifecycle_bucket, replay.touch_level)
        groups.setdefault(key, []).append(replay)
    rng = random.Random(seed)
    out: list[SegmentSummary] = []
    for (label, asset, direction, bucket, level), rows in groups.items():
        safe = [row for row in rows if row.status != "post_only_would_cross"]
        filled = [row for row in rows if row.status == "filled"]
        cost = sum(row.cost_usdc for row in filled)
        pnl = sum(row.pnl_usdc for row in filled)
        ev = pnl / cost if cost else None
        ci = bootstrap_ev_ci(filled, samples=bootstrap_samples, rng=rng) if len(filled) >= 2 else (None, None)
        fill_rate = len(filled) / len(rows) if rows else None
        gate = "GO" if len(filled) >= 100 and fill_rate is not None and fill_rate >= 0.30 and ev is not None and ev > 0 and ci[0] is not None and ci[0] > 0 else "NO-GO"
        out.append(
            SegmentSummary(label, asset, direction, bucket, level, len(rows), len(safe), len(filled), fill_rate, cost, pnl, ev, ci[0], ci[1], gate)
        )
    return sorted(out, key=lambda x: (x.gate != "GO", -1 if x.ev_per_usdc is None else -x.ev_per_usdc, -x.filled_n, -x.fired_n))


def bootstrap_ev_ci(rows: Sequence[MakerReplay], *, samples: int, rng: random.Random) -> tuple[float | None, float | None]:
    if not rows:
        return None, None
    values: list[float] = []
    n = len(rows)
    for _ in range(samples):
        cost = pnl = 0.0
        for _i in range(n):
            row = rows[rng.randrange(n)]
            cost += row.cost_usdc
            pnl += row.pnl_usdc
        values.append(pnl / cost if cost else 0.0)
    values.sort()
    return percentile(values, 0.025), percentile(values, 0.975)


def write_output_db(path: str | Path, replays: Sequence[MakerReplay], segments: Sequence[SegmentSummary], summary: EvaluationSummary) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            DROP TABLE IF EXISTS pre_touch_replays;
            DROP TABLE IF EXISTS pre_touch_segments;
            DROP TABLE IF EXISTS summary_meta;
            CREATE TABLE pre_touch_replays (
                config_label TEXT, bundle_name TEXT, probe_id TEXT, market_id TEXT, token_id TEXT,
                asset_slug TEXT, direction TEXT, lifecycle_bucket TEXT, touch_level REAL, resolves_yes INTEGER,
                status TEXT, trigger_ms INTEGER, trigger_delta_ms INTEGER, trigger_best_bid REAL, trigger_best_ask REAL,
                ask_reached_level INTEGER, order_shares REAL, required_sell_volume REAL, observed_sell_volume REAL,
                cost_usdc REAL, pnl_usdc REAL
            );
            CREATE TABLE pre_touch_segments (
                config_label TEXT, asset_slug TEXT, direction TEXT, lifecycle_bucket TEXT, level REAL,
                fired_n INTEGER, post_only_safe_n INTEGER, filled_n INTEGER, fill_rate REAL,
                gross_cost_usdc REAL, pnl_usdc REAL, ev_per_usdc REAL, ci95_lower REAL, ci95_upper REAL, gate TEXT
            );
            CREATE TABLE summary_meta (key TEXT PRIMARY KEY, value TEXT);
            """
        )
        conn.executemany(
            "INSERT INTO pre_touch_replays VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(r.config_label, r.bundle_name, r.probe_id, r.market_id, r.token_id, r.asset_slug, r.direction, r.lifecycle_bucket, r.touch_level, r.resolves_yes, r.status, r.trigger_ms, r.trigger_delta_ms, r.trigger_best_bid, r.trigger_best_ask, 1 if r.ask_reached_level else 0, r.order_shares, r.required_sell_volume, r.observed_sell_volume, r.cost_usdc, r.pnl_usdc) for r in replays],
        )
        conn.executemany(
            "INSERT INTO pre_touch_segments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(s.config_label, s.asset_slug, s.direction, s.lifecycle_bucket, s.level, s.fired_n, s.post_only_safe_n, s.filled_n, s.fill_rate, s.gross_cost_usdc, s.pnl_usdc, s.ev_per_usdc, s.ci95_lower, s.ci95_upper, s.gate) for s in segments],
        )
        conn.execute("INSERT INTO summary_meta VALUES (?, ?)", ("summary_json", json.dumps(summary.as_json(), sort_keys=True)))
        conn.commit()
    finally:
        conn.close()


def render_report(summary: EvaluationSummary, segments: Sequence[SegmentSummary]) -> str:
    lines = [
        "# T3 pre-touch maker trigger",
        "",
        f"Generated at: `{datetime.now(timezone.utc).isoformat()}`",
        f"Touch DB: `{summary.touch_db}`",
        f"Capture glob: `{summary.capture_glob}`",
        "",
        "## Scope",
        "",
        "This offline replay tests a maker-friendly pre-touch redesign: post a resting BUY at the target level while best ask is still above the target but within two ticks.",
        "The replay uses capture-bundle BBO plus TradeTick data. Maker fee is modeled as `0 bps`; full L2 queue position is unavailable unless future captures include depth levels.",
        "",
        "Trigger approximation:",
        "- target levels: `0.55`, `0.65`, `0.70`",
        "- eligible pre-touch book: `touch_level < best_ask <= touch_level + 2*tick_size`",
        "- optional BBO momentum: current mid > mid 30s earlier",
        "- optional spread compression: current spread below median spread over prior 30s",
        "- fill proxy: after trigger, best ask reaches target and cumulative SELL prints at/below target exceed `order_shares * queue_factor` before cancel",
        "",
        "## Coverage",
        "",
        f"- capture bundles: `{summary.bundle_count}`",
        f"- candidate appearances: `{summary.candidate_appearances}`",
        f"- replay rows: `{summary.replay_rows}`",
        f"- segments: `{summary.segment_count}`",
        f"- any L2 levels: `{str(summary.any_l2_levels).lower()}`",
        "",
        "## Headline",
        "",
        f"- Gate: `{summary.go_gate}`",
    ]
    if summary.best_segment:
        b = summary.best_segment
        lines.extend([
            f"- Best segment: `{b.config_label} | {b.asset_slug} | {b.direction} | {b.lifecycle_bucket} | level={b.level:.2f}`",
            f"- Best EV/USDC: `{fmt(b.ev_per_usdc)}`; fired `{b.fired_n}`, filled `{b.filled_n}`, fill rate `{fmt_pct(b.fill_rate)}`, CI95 `{fmt(b.ci95_lower)}..{fmt(b.ci95_upper)}`",
        ])
    lines.extend([
        "",
        "## Segment comparison",
        "",
        "| config | asset | direction | lifecycle | level | fired | safe | filled | fill rate | EV/USDC | CI95 | gate |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ])
    for s in list(segments)[:200]:
        lines.append(
            f"| {s.config_label} | {s.asset_slug} | {s.direction} | {s.lifecycle_bucket} | {s.level:.2f} | {s.fired_n} | {s.post_only_safe_n} | {s.filled_n} | {fmt_pct(s.fill_rate)} | {fmt(s.ev_per_usdc)} | {fmt(s.ci95_lower)}..{fmt(s.ci95_upper)} | {s.gate} |"
        )
    lines.extend([
        "",
        "## Proposed live changes if T3 ever passes offline",
        "",
        "- Add a config-gated pre-touch maker trigger separate from ASC touch.",
        "- Emit explicit pre-touch signal events with trigger book state, target level, cancel window, and queue assumptions.",
        "- Require depth/L2 capture before live maker sizing; BBO-only replay is not enough to size queue position.",
        "- Keep maker path disabled unless offline replay clears fill-rate and positive-CI gates on fresh data.",
        "",
    ])
    return "\n".join(lines)


def _replay(touch: TouchRow, config: MakerConfig, bundle_name: str, bucket: str, status: str, trigger: BookPoint | None, ask_reached: bool, order_shares: float, required_sell_volume: float, observed_sell_volume: float, cost: float, pnl: float) -> MakerReplay:
    return MakerReplay(config.label, bundle_name, touch.probe_id, touch.market_id, touch.token_id, touch.asset_slug, touch.direction, bucket, touch.touch_level, touch.resolves_yes, status, trigger.ts_ms if trigger else None, (trigger.ts_ms - touch.touch_ms) if trigger else None, trigger.best_bid if trigger else None, trigger.best_ask if trigger else None, ask_reached, order_shares, required_sell_volume, observed_sell_volume, cost, pnl)


def order_shares_for_stake(*, stake_usdc: float, entry_price: float, min_order_shares: float) -> float:
    return max(min_order_shares, stake_usdc / entry_price)


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


def parse_ts_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if 0 < number < 10_000_000_000:
            number *= 1000
        return int(number)
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None


def median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    pos = (len(values) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return values[int(pos)]
    return values[lo] * (hi - pos) + values[hi] * (pos - lo)


def fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.6f}"


def fmt_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2%}"


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate T3 pre-touch maker trigger replay")
    parser.add_argument("--touch-db", default=str(DEFAULT_TOUCH_DB))
    parser.add_argument("--capture-glob", default=DEFAULT_CAPTURE_GLOB)
    parser.add_argument("--output-db", default=str(DEFAULT_OUTPUT_DB))
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--bootstrap-samples", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()
    summary = evaluate_pre_touch_maker(
        args.touch_db,
        args.capture_glob,
        output_db=args.output_db,
        report_path=args.report,
        bootstrap_samples=args.bootstrap_samples,
        seed=args.seed,
    )
    print(f"T3 pre-touch maker: gate={summary.go_gate} bundles={summary.bundle_count} candidates={summary.candidate_appearances} segments={summary.segment_count} report={summary.report_path}")
    if summary.best_segment:
        b = summary.best_segment
        print(f"Best segment: {b.config_label}|{b.asset_slug}|{b.direction}|{b.lifecycle_bucket}|{b.level:.2f} fired={b.fired_n} filled={b.filled_n} ev={fmt(b.ev_per_usdc)} ci={fmt(b.ci95_lower)}..{fmt(b.ci95_upper)} gate={b.gate}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
