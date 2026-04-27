#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import json
import math
import sqlite3
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Iterable, Sequence

if __package__:
    from utils.logger import setup_logger
    from utils.paths import DATA_DIR, ensure_runtime_dirs
else:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from utils.logger import setup_logger
    from utils.paths import DATA_DIR, ensure_runtime_dirs

logger = setup_logger("trigger_overlay")
DEFAULT_TRIGGER_DB = DATA_DIR / "trigger_discovery.sqlite3"
DEFAULT_TOUCH_DB = DATA_DIR / "touch_dataset.sqlite3"
DEFAULT_OUTPUT_DB = DATA_DIR / "trigger_overlay.sqlite3"
DEFAULT_REPORT_PATH = DATA_DIR / "trigger_overlay_report.md"
DEFAULT_MATCH_WINDOW_SECONDS = 90.0
DEFAULT_FIT_ALLOWLIST = ("+0_tick", "+1_tick")
DEFAULT_MAX_ASK_SLIPPAGE_TICKS = 1.0
DEFAULT_MIN_COST_USDC = 1.0


@dataclass(frozen=True)
class TriggerRow:
    token_id: str
    market_id: str
    question: str
    asset_slug: str
    direction: str
    split: str
    utc_hour: int
    trading_session: str
    start_ts: int
    closed_ts: int
    trigger_ts: int
    trigger_fraction: float
    trigger_price: float
    trigger_label: str
    trigger_tags: str


@dataclass(frozen=True)
class TouchRow:
    probe_id: str
    token_id: str
    market_id: str
    asset_slug: str
    direction: str
    touch_ts: float
    lifecycle_fraction: float | None
    touch_level: float | None
    best_bid_at_touch: float | None
    best_ask_at_touch: float | None
    tick_size: float | None
    fee_rate_bps: float | None
    fees_enabled: int
    ask_size_at_touch_level: float | None
    fit_10_usdc: str | None
    fit_50_usdc: str | None
    fit_100_usdc: str | None
    survived_ms: float | None
    end_reason: str | None
    resolves_yes: int
    would_be_size_at_min_shares: float
    would_be_cost_after_min_shares: float | None
    would_be_estimated_fee_usdc: float | None


@dataclass(frozen=True)
class OverlayMatch:
    trigger_key: str
    probe_id: str
    token_id: str
    market_id: str
    asset_slug: str
    direction: str
    split: str
    trading_session: str
    utc_hour: int
    trigger_label: str
    trigger_tags: str
    trigger_ts: int
    touch_ts: float
    match_delta_seconds: float
    trigger_fraction: float
    touch_lifecycle_fraction: float | None
    trigger_price: float
    best_ask_at_touch: float
    tick_size: float
    ask_slippage_ticks: float
    fit_10_usdc: str | None
    ask_size_at_touch_level: float | None
    survived_ms: float | None
    is_winner: int
    entry_fee_per_share: float
    net_pnl_per_share: float
    min_order_cost_usdc: float | None
    min_order_net_pnl_usdc: float | None
    executable: int
    reject_reason: str | None


@dataclass(frozen=True)
class OverlaySummary:
    trigger_db_path: str
    touch_db_path: str
    output_db_path: str
    report_path: str
    triggers_loaded: int
    touch_rows_loaded: int
    matched_rows: int
    executable_rows: int
    match_window_seconds: float
    max_ask_slippage_ticks: float
    fit_allowlist: tuple[str, ...]

    def as_json(self) -> dict:
        return asdict(self)


def evaluate_trigger_overlay(
    trigger_db_path: str | Path = DEFAULT_TRIGGER_DB,
    touch_db_path: str | Path = DEFAULT_TOUCH_DB,
    *,
    output_db_path: str | Path = DEFAULT_OUTPUT_DB,
    report_path: str | Path = DEFAULT_REPORT_PATH,
    assets: set[str] | None = None,
    directions: set[str] | None = None,
    trigger_labels: set[str] | None = None,
    match_window_seconds: float = DEFAULT_MATCH_WINDOW_SECONDS,
    max_ask_slippage_ticks: float = DEFAULT_MAX_ASK_SLIPPAGE_TICKS,
    fit_allowlist: Sequence[str] = DEFAULT_FIT_ALLOWLIST,
    min_cost_usdc: float = DEFAULT_MIN_COST_USDC,
    bootstrap_samples: int = 1000,
) -> OverlaySummary:
    ensure_runtime_dirs()
    trigger_db_path = Path(trigger_db_path)
    touch_db_path = Path(touch_db_path)
    output_db_path = Path(output_db_path)
    report_path = Path(report_path)
    output_db_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    triggers = load_triggers(trigger_db_path, assets=assets, directions=directions, trigger_labels=trigger_labels)
    touches = load_touches(touch_db_path, assets=assets, directions=directions)
    index = index_touches(touches)
    matches = match_triggers_to_touches(
        triggers,
        index,
        match_window_seconds=float(match_window_seconds),
        max_ask_slippage_ticks=float(max_ask_slippage_ticks),
        fit_allowlist=tuple(fit_allowlist),
        min_cost_usdc=float(min_cost_usdc),
    )
    aggregates = aggregate_matches(matches, bootstrap_samples=bootstrap_samples)
    write_output_db(output_db_path, matches, aggregates, summary_meta={
        "trigger_db_path": str(trigger_db_path),
        "touch_db_path": str(touch_db_path),
        "assets": sorted(assets or []),
        "directions": sorted(directions or []),
        "trigger_labels": sorted(trigger_labels or []),
        "match_window_seconds": match_window_seconds,
        "max_ask_slippage_ticks": max_ask_slippage_ticks,
        "fit_allowlist": list(fit_allowlist),
        "min_cost_usdc": min_cost_usdc,
    })
    report_path.write_text(render_report(matches, aggregates, summary_meta={
        "trigger_db_path": str(trigger_db_path),
        "touch_db_path": str(touch_db_path),
        "output_db_path": str(output_db_path),
        "assets": sorted(assets or []),
        "directions": sorted(directions or []),
        "trigger_labels": sorted(trigger_labels or []),
        "match_window_seconds": match_window_seconds,
        "max_ask_slippage_ticks": max_ask_slippage_ticks,
        "fit_allowlist": list(fit_allowlist),
        "min_cost_usdc": min_cost_usdc,
    }), encoding="utf-8")
    return OverlaySummary(
        trigger_db_path=str(trigger_db_path),
        touch_db_path=str(touch_db_path),
        output_db_path=str(output_db_path),
        report_path=str(report_path),
        triggers_loaded=len(triggers),
        touch_rows_loaded=len(touches),
        matched_rows=len(matches),
        executable_rows=sum(1 for item in matches if item.executable),
        match_window_seconds=float(match_window_seconds),
        max_ask_slippage_ticks=float(max_ask_slippage_ticks),
        fit_allowlist=tuple(fit_allowlist),
    )


def load_triggers(
    path: Path,
    *,
    assets: set[str] | None = None,
    directions: set[str] | None = None,
    trigger_labels: set[str] | None = None,
) -> list[TriggerRow]:
    clauses: list[str] = []
    params: list[str] = []
    if assets:
        clauses.append(f"asset_slug IN ({','.join('?' for _ in assets)})")
        params.extend(sorted(assets))
    if directions:
        clauses.append(f"direction IN ({','.join('?' for _ in directions)})")
        params.extend(sorted(directions))
    if trigger_labels:
        clauses.append(f"trigger_label IN ({','.join('?' for _ in trigger_labels)})")
        params.extend(sorted(trigger_labels))
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT token_id, market_id, question, asset_slug, direction, split, utc_hour, trading_session,
                   start_ts, closed_ts, trigger_ts, trigger_fraction, trigger_price, trigger_label, trigger_tags
            FROM trigger_features
            {where}
            ORDER BY token_id, trigger_ts, trigger_fraction
            """,
            params,
        ).fetchall()
    finally:
        conn.close()
    return [
        TriggerRow(
            token_id=str(row["token_id"]),
            market_id=str(row["market_id"]),
            question=str(row["question"] or ""),
            asset_slug=str(row["asset_slug"] or ""),
            direction=str(row["direction"] or ""),
            split=str(row["split"] or "all"),
            utc_hour=int(row["utc_hour"] or 0),
            trading_session=str(row["trading_session"] or "unknown"),
            start_ts=int(row["start_ts"] or 0),
            closed_ts=int(row["closed_ts"] or 0),
            trigger_ts=int(row["trigger_ts"] or 0),
            trigger_fraction=float(row["trigger_fraction"] or 0.0),
            trigger_price=float(row["trigger_price"] or 0.0),
            trigger_label=str(row["trigger_label"] or ""),
            trigger_tags=str(row["trigger_tags"] or ""),
        )
        for row in rows
    ]


def load_touches(path: Path, *, assets: set[str] | None = None, directions: set[str] | None = None) -> list[TouchRow]:
    clauses: list[str] = []
    params: list[str] = []
    if assets:
        clauses.append(f"asset_slug IN ({','.join('?' for _ in assets)})")
        params.extend(sorted(assets))
    if directions:
        clauses.append(f"direction IN ({','.join('?' for _ in directions)})")
        params.extend(sorted(directions))
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(f"SELECT * FROM touch_dataset {where} ORDER BY token_id, touch_time_iso", params).fetchall()
    finally:
        conn.close()
    touches: list[TouchRow] = []
    for row in rows:
        touch_ts = _parse_iso_ts(str(row["touch_time_iso"]))
        touches.append(
            TouchRow(
                probe_id=str(row["probe_id"]),
                token_id=str(row["token_id"]),
                market_id=str(row["market_id"]),
                asset_slug=str(row["asset_slug"]),
                direction=str(row["direction"]),
                touch_ts=touch_ts,
                lifecycle_fraction=_optional_float(row["lifecycle_fraction"]),
                touch_level=_optional_float(row["touch_level"]),
                best_bid_at_touch=_optional_float(row["best_bid_at_touch"]),
                best_ask_at_touch=_optional_float(row["best_ask_at_touch"]),
                tick_size=_optional_float(row["tick_size"]),
                fee_rate_bps=_optional_float(row["fee_rate_bps"]),
                fees_enabled=int(row["fees_enabled"] if row["fees_enabled"] is not None else 1),
                ask_size_at_touch_level=_optional_float(row["ask_size_at_touch_level"]),
                fit_10_usdc=str(row["fit_10_usdc"] or ""),
                fit_50_usdc=str(row["fit_50_usdc"] or ""),
                fit_100_usdc=str(row["fit_100_usdc"] or ""),
                survived_ms=_optional_float(row["survived_ms"]),
                end_reason=str(row["end_reason"] or ""),
                resolves_yes=int(row["resolves_yes"] or 0),
                would_be_size_at_min_shares=float(row["would_be_size_at_min_shares"] or 0.0),
                would_be_cost_after_min_shares=_optional_float(row["would_be_cost_after_min_shares"]),
                would_be_estimated_fee_usdc=_optional_float(row["would_be_estimated_fee_usdc"]),
            )
        )
    return touches


def index_touches(touches: Sequence[TouchRow]) -> dict[str, tuple[list[float], list[TouchRow]]]:
    grouped: dict[str, list[TouchRow]] = defaultdict(list)
    for touch in touches:
        grouped[touch.token_id].append(touch)
    out: dict[str, tuple[list[float], list[TouchRow]]] = {}
    for token_id, rows in grouped.items():
        rows.sort(key=lambda item: item.touch_ts)
        out[token_id] = ([item.touch_ts for item in rows], rows)
    return out


def match_triggers_to_touches(
    triggers: Sequence[TriggerRow],
    touch_index: dict[str, tuple[list[float], list[TouchRow]]],
    *,
    match_window_seconds: float,
    max_ask_slippage_ticks: float,
    fit_allowlist: Sequence[str],
    min_cost_usdc: float,
) -> list[OverlayMatch]:
    matches: list[OverlayMatch] = []
    fit_allowed = {str(value) for value in fit_allowlist if str(value)}
    for trigger in triggers:
        indexed = touch_index.get(trigger.token_id)
        if not indexed:
            continue
        times, touches = indexed
        pos = bisect.bisect_left(times, float(trigger.trigger_ts))
        candidates = []
        for idx in (pos - 1, pos, pos + 1):
            if 0 <= idx < len(touches):
                delta = abs(float(touches[idx].touch_ts) - float(trigger.trigger_ts))
                if delta <= match_window_seconds:
                    candidates.append((delta, touches[idx]))
        if not candidates:
            continue
        candidates.sort(key=lambda item: item[0])
        delta, touch = candidates[0]
        matches.append(_overlay_match_for(trigger, touch, match_delta_seconds=delta, max_ask_slippage_ticks=max_ask_slippage_ticks, fit_allowed=fit_allowed, min_cost_usdc=min_cost_usdc))
    return matches


def _overlay_match_for(
    trigger: TriggerRow,
    touch: TouchRow,
    *,
    match_delta_seconds: float,
    max_ask_slippage_ticks: float,
    fit_allowed: set[str],
    min_cost_usdc: float,
) -> OverlayMatch:
    ask = touch.best_ask_at_touch
    tick = touch.tick_size or 0.01
    reject_reason: str | None = None
    executable = 1
    ask_slippage_ticks = float("nan")
    fee_per_share = 0.0
    net_pnl_per_share = 0.0
    min_order_net_pnl_usdc: float | None = None
    if ask is None or ask <= 0.0 or ask >= 1.0:
        reject_reason = "missing_or_invalid_ask"
        executable = 0
        ask = float("nan")
    else:
        ask_slippage_ticks = (float(ask) - float(trigger.trigger_price)) / max(float(tick), 1e-9)
        if ask_slippage_ticks - max_ask_slippage_ticks > 1e-9:
            reject_reason = "ask_slippage_too_high"
            executable = 0
        elif fit_allowed and str(touch.fit_10_usdc or "") not in fit_allowed:
            reject_reason = "fit_10_not_allowed"
            executable = 0
        elif touch.would_be_cost_after_min_shares is None or touch.would_be_cost_after_min_shares < min_cost_usdc:
            reject_reason = "min_order_cost_too_low"
            executable = 0
        fee_per_share = _entry_fee_per_share(float(ask), touch.fee_rate_bps, fees_enabled=touch.fees_enabled)
        gross = (1.0 - float(ask)) if touch.resolves_yes else -float(ask)
        net_pnl_per_share = gross - fee_per_share
        if touch.would_be_size_at_min_shares:
            min_order_net_pnl_usdc = net_pnl_per_share * float(touch.would_be_size_at_min_shares)
    return OverlayMatch(
        trigger_key=f"{trigger.token_id}:{trigger.trigger_fraction:.6f}",
        probe_id=touch.probe_id,
        token_id=trigger.token_id,
        market_id=trigger.market_id,
        asset_slug=trigger.asset_slug,
        direction=trigger.direction,
        split=trigger.split,
        trading_session=trigger.trading_session,
        utc_hour=trigger.utc_hour,
        trigger_label=trigger.trigger_label,
        trigger_tags=trigger.trigger_tags,
        trigger_ts=trigger.trigger_ts,
        touch_ts=touch.touch_ts,
        match_delta_seconds=float(match_delta_seconds),
        trigger_fraction=trigger.trigger_fraction,
        touch_lifecycle_fraction=touch.lifecycle_fraction,
        trigger_price=trigger.trigger_price,
        best_ask_at_touch=float(ask),
        tick_size=float(tick),
        ask_slippage_ticks=float(ask_slippage_ticks),
        fit_10_usdc=touch.fit_10_usdc,
        ask_size_at_touch_level=touch.ask_size_at_touch_level,
        survived_ms=touch.survived_ms,
        is_winner=int(touch.resolves_yes),
        entry_fee_per_share=fee_per_share,
        net_pnl_per_share=net_pnl_per_share,
        min_order_cost_usdc=touch.would_be_cost_after_min_shares,
        min_order_net_pnl_usdc=min_order_net_pnl_usdc,
        executable=executable,
        reject_reason=reject_reason,
    )


def aggregate_matches(matches: Sequence[OverlayMatch], *, bootstrap_samples: int) -> list[dict]:
    groupers = {
        "all": lambda m: ("all",),
        "executable_all": lambda m: ("executable",) if m.executable else None,
        "trigger": lambda m: (m.trigger_label,),
        "executable_trigger": lambda m: (m.trigger_label,) if m.executable else None,
        "asset_direction_trigger": lambda m: (m.asset_slug, m.direction, m.trigger_label),
        "executable_asset_direction_trigger": lambda m: (m.asset_slug, m.direction, m.trigger_label) if m.executable else None,
        "trading_session": lambda m: (m.trading_session,),
        "executable_trading_session": lambda m: (m.trading_session,) if m.executable else None,
        "trading_session_trigger": lambda m: (m.trading_session, m.trigger_label),
        "executable_trading_session_trigger": lambda m: (m.trading_session, m.trigger_label) if m.executable else None,
        "reject_reason": lambda m: (m.reject_reason or "executable",),
    }
    rows: list[dict] = []
    for group_type, key_fn in groupers.items():
        buckets: dict[tuple[str, ...], list[OverlayMatch]] = defaultdict(list)
        for match in matches:
            key = key_fn(match)
            if key is not None:
                buckets[key].append(match)
        for key, items in buckets.items():
            rows.append(_aggregate_row(group_type, key, items, bootstrap_samples=bootstrap_samples))
    rows.sort(key=lambda row: (row["group_type"], -int(row["n"]), row["group_key"]))
    return rows


def _aggregate_row(group_type: str, key: tuple[str, ...], items: Sequence[OverlayMatch], *, bootstrap_samples: int) -> dict:
    pnls = [float(item.net_pnl_per_share) for item in items]
    min_order_pnls = [float(item.min_order_net_pnl_usdc) for item in items if item.min_order_net_pnl_usdc is not None]
    executable = sum(int(item.executable) for item in items)
    wins = sum(int(item.is_winner) for item in items)
    ci_low, ci_high = bootstrap_mean_ci(pnls, samples=bootstrap_samples)
    return {
        "group_type": group_type,
        "group_key": "|".join(key),
        "n": len(items),
        "executable_n": executable,
        "executable_rate": executable / len(items) if items else 0.0,
        "wins": wins,
        "win_rate": wins / len(items) if items else 0.0,
        "avg_entry_price": mean(float(item.best_ask_at_touch) for item in items),
        "avg_ask_slippage_ticks": mean(float(item.ask_slippage_ticks) for item in items),
        "avg_net_pnl_per_share": mean(pnls) if pnls else 0.0,
        "ci95_low": ci_low,
        "ci95_high": ci_high,
        "total_min_order_net_pnl_usdc": sum(min_order_pnls),
        "avg_min_order_net_pnl_usdc": mean(min_order_pnls) if min_order_pnls else None,
    }


def write_output_db(path: Path, matches: Sequence[OverlayMatch], aggregates: Sequence[dict], *, summary_meta: dict) -> None:
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            CREATE TABLE overlay_run_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE overlay_matches(
                trigger_key TEXT NOT NULL,
                probe_id TEXT NOT NULL,
                token_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                asset_slug TEXT,
                direction TEXT,
                split TEXT,
                trading_session TEXT,
                utc_hour INTEGER,
                trigger_label TEXT,
                trigger_tags TEXT,
                trigger_ts INTEGER,
                touch_ts REAL,
                match_delta_seconds REAL,
                trigger_fraction REAL,
                touch_lifecycle_fraction REAL,
                trigger_price REAL,
                best_ask_at_touch REAL,
                tick_size REAL,
                ask_slippage_ticks REAL,
                fit_10_usdc TEXT,
                ask_size_at_touch_level REAL,
                survived_ms REAL,
                is_winner INTEGER,
                entry_fee_per_share REAL,
                net_pnl_per_share REAL,
                min_order_cost_usdc REAL,
                min_order_net_pnl_usdc REAL,
                executable INTEGER,
                reject_reason TEXT,
                PRIMARY KEY(trigger_key, probe_id)
            );
            CREATE INDEX idx_overlay_matches_executable ON overlay_matches(executable);
            CREATE INDEX idx_overlay_matches_trigger ON overlay_matches(trigger_label);
            CREATE INDEX idx_overlay_matches_asset_trigger ON overlay_matches(asset_slug, direction, trigger_label);
            CREATE TABLE overlay_aggregates(
                group_type TEXT NOT NULL,
                group_key TEXT NOT NULL,
                n INTEGER NOT NULL,
                executable_n INTEGER NOT NULL,
                executable_rate REAL NOT NULL,
                wins INTEGER NOT NULL,
                win_rate REAL NOT NULL,
                avg_entry_price REAL NOT NULL,
                avg_ask_slippage_ticks REAL NOT NULL,
                avg_net_pnl_per_share REAL NOT NULL,
                ci95_low REAL,
                ci95_high REAL,
                total_min_order_net_pnl_usdc REAL,
                avg_min_order_net_pnl_usdc REAL,
                PRIMARY KEY(group_type, group_key)
            );
            """
        )
        for key, value in summary_meta.items():
            conn.execute("INSERT INTO overlay_run_meta(key, value) VALUES (?, ?)", (str(key), json.dumps(value, sort_keys=True)))
        conn.executemany(
            """
            INSERT INTO overlay_matches VALUES (
                :trigger_key, :probe_id, :token_id, :market_id, :asset_slug, :direction, :split,
                :trading_session, :utc_hour, :trigger_label, :trigger_tags, :trigger_ts, :touch_ts,
                :match_delta_seconds, :trigger_fraction, :touch_lifecycle_fraction, :trigger_price,
                :best_ask_at_touch, :tick_size, :ask_slippage_ticks, :fit_10_usdc,
                :ask_size_at_touch_level, :survived_ms, :is_winner, :entry_fee_per_share,
                :net_pnl_per_share, :min_order_cost_usdc, :min_order_net_pnl_usdc,
                :executable, :reject_reason
            )
            """,
            [asdict(match) for match in matches],
        )
        conn.executemany(
            """
            INSERT INTO overlay_aggregates VALUES (
                :group_type, :group_key, :n, :executable_n, :executable_rate, :wins, :win_rate,
                :avg_entry_price, :avg_ask_slippage_ticks, :avg_net_pnl_per_share,
                :ci95_low, :ci95_high, :total_min_order_net_pnl_usdc, :avg_min_order_net_pnl_usdc
            )
            """,
            list(aggregates),
        )
        conn.commit()
    finally:
        conn.close()


def render_report(matches: Sequence[OverlayMatch], aggregates: Sequence[dict], *, summary_meta: dict) -> str:
    lines: list[str] = []
    lines.append("# Trigger executable overlay report")
    lines.append("")
    lines.append(f"Generated at: `{datetime.now(timezone.utc).isoformat()}`")
    lines.append("")
    lines.append("## Scope")
    for key, value in summary_meta.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.append(f"- matched trigger/touch rows: `{len(matches)}`")
    lines.append(f"- executable rows: `{sum(1 for item in matches if item.executable)}`")
    lines.append("")
    lines.append("## Read me first")
    lines.append("- This overlays historical trigger candidates onto collector/touch rows; it is closer to executable than tape price, but still not a live fill guarantee.")
    lines.append("- Entry price is `best_ask_at_touch`; reject reasons cover ask slippage, fit_10 depth labels, and min-order cost.")
    lines.append("- Treat positive groups as dry-run candidates only after fresh split stability.")
    lines.append("")
    lines.append("## Executable headline")
    _append_aggregate_section(lines, aggregates, group_type="executable_all", limit=5, min_n=1)
    lines.append("")
    lines.append("## Reject reasons")
    _append_aggregate_section(lines, aggregates, group_type="reject_reason", limit=10, min_n=1, sort_by_n=True)
    lines.append("")
    lines.append("## Executable trigger candidates")
    _append_aggregate_section(lines, aggregates, group_type="executable_asset_direction_trigger", limit=30, min_n=3)
    lines.append("")
    lines.append("## Executable session view")
    _append_aggregate_section(lines, aggregates, group_type="executable_trading_session", limit=10, min_n=3)
    lines.append("")
    lines.append("## Executable session + trigger view")
    _append_aggregate_section(lines, aggregates, group_type="executable_trading_session_trigger", limit=30, min_n=3)
    lines.append("")
    lines.append("## All matched trigger candidates")
    _append_aggregate_section(lines, aggregates, group_type="asset_direction_trigger", limit=30, min_n=3)
    lines.append("")
    lines.append("## Next checks")
    lines.append("- If executable n is small, keep collecting rather than trading.")
    lines.append("- Re-run on fresh collector windows before dry-run/live wiring.")
    lines.append("- Promote only trigger + asset + direction + session combinations that remain positive after ask/depth/min-size filters.")
    return "\n".join(lines)


def _append_aggregate_section(lines: list[str], aggregates: Sequence[dict], *, group_type: str, limit: int, min_n: int, sort_by_n: bool = False) -> None:
    rows = [row for row in aggregates if row["group_type"] == group_type and int(row["n"]) >= int(min_n)]
    if sort_by_n:
        rows.sort(key=lambda row: int(row["n"]), reverse=True)
    else:
        rows.sort(key=lambda row: (float(row["avg_net_pnl_per_share"]), int(row["n"])), reverse=True)
    if not rows:
        lines.append(f"- no groups with n >= {min_n}")
        return
    for row in rows[:limit]:
        ci = "n/a" if row["ci95_low"] is None else f"{row['ci95_low']:+.4f}..{row['ci95_high']:+.4f}"
        avg_min = "n/a" if row["avg_min_order_net_pnl_usdc"] is None else f"{row['avg_min_order_net_pnl_usdc']:+.4f}"
        lines.append(
            f"- `{row['group_key']}`: n=`{row['n']}`, executable=`{row['executable_n']}` ({row['executable_rate']:.1%}), "
            f"win=`{row['win_rate']:.1%}`, ask=`{row['avg_entry_price']:.4f}`, "
            f"slip_ticks=`{row['avg_ask_slippage_ticks']:.2f}`, net/share=`{row['avg_net_pnl_per_share']:+.4f}`, "
            f"CI95=`{ci}`, avg_min_order_pnl=`{avg_min}`"
        )


def bootstrap_mean_ci(values: Sequence[float], *, samples: int) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    if len(values) == 1 or samples <= 0:
        value = float(mean(values))
        return value, value
    if len(values) * int(samples) > 2_000_000:
        avg = float(mean(values))
        sigma = float(pstdev(values))
        half_width = 1.96 * sigma / math.sqrt(len(values))
        return avg - half_width, avg + half_width
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


def _entry_fee_per_share(price: float, fee_rate_bps: float | None, *, fees_enabled: int) -> float:
    if int(fees_enabled or 0) == 0:
        return 0.0
    rate = float(fee_rate_bps or 0.0) / 10000.0
    p = max(0.0, min(1.0, float(price)))
    return rate * p * (1.0 - p)


def _parse_iso_ts(value: str) -> float:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text).timestamp()


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out):
        return None
    return out


def _parse_csv_set(value: str | None) -> set[str] | None:
    if not value:
        return None
    items = {item.strip() for item in value.split(",") if item.strip()}
    return items or None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Overlay trigger-discovery rows with collector/touch executable context.")
    parser.add_argument("--trigger-db", default=str(DEFAULT_TRIGGER_DB))
    parser.add_argument("--touch-db", default=str(DEFAULT_TOUCH_DB))
    parser.add_argument("--output-db", default=str(DEFAULT_OUTPUT_DB))
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--assets", default=None, help="Comma-separated asset allowlist, e.g. btc,eth")
    parser.add_argument("--directions", default=None, help="Comma-separated directions, e.g. UP/YES,DOWN/NO")
    parser.add_argument("--trigger-labels", default=None, help="Comma-separated trigger labels")
    parser.add_argument("--match-window-seconds", type=float, default=DEFAULT_MATCH_WINDOW_SECONDS)
    parser.add_argument("--max-ask-slippage-ticks", type=float, default=DEFAULT_MAX_ASK_SLIPPAGE_TICKS)
    parser.add_argument("--fit-allowlist", default=",".join(DEFAULT_FIT_ALLOWLIST))
    parser.add_argument("--min-cost-usdc", type=float, default=DEFAULT_MIN_COST_USDC)
    parser.add_argument("--bootstrap-samples", type=int, default=1000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = evaluate_trigger_overlay(
        args.trigger_db,
        args.touch_db,
        output_db_path=args.output_db,
        report_path=args.report_path,
        assets=_parse_csv_set(args.assets),
        directions=_parse_csv_set(args.directions),
        trigger_labels=_parse_csv_set(args.trigger_labels),
        match_window_seconds=float(args.match_window_seconds),
        max_ask_slippage_ticks=float(args.max_ask_slippage_ticks),
        fit_allowlist=tuple(sorted(_parse_csv_set(args.fit_allowlist) or set())),
        min_cost_usdc=float(args.min_cost_usdc),
        bootstrap_samples=int(args.bootstrap_samples),
    )
    print(json.dumps(summary.as_json(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
