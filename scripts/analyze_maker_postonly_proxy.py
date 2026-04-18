#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

if __package__:
    from .analyze_price_resolution import build_touch_events_for_token
    from replay.tape_feed import DEFAULT_TAPE_DB_PATH
    from utils.logger import setup_logger
    from utils.paths import DATA_DIR, DB_PATH, ensure_runtime_dirs
else:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from analyze_price_resolution import build_touch_events_for_token  # type: ignore
    from replay.tape_feed import DEFAULT_TAPE_DB_PATH
    from utils.logger import setup_logger
    from utils.paths import DATA_DIR, DB_PATH, ensure_runtime_dirs


logger = setup_logger("maker_postonly_proxy")
DEFAULT_OUTPUT_DB_PATH = DATA_DIR / "maker_postonly_proxy.db"


@dataclass(frozen=True)
class TokenMeta:
    token_id: str
    market_id: str
    is_winner: int
    token_order: int | None
    category: str
    neg_risk: int
    fees_enabled: int
    duration_hours: float
    closed_time: int


@dataclass(frozen=True)
class TradePoint:
    timestamp: int
    price: float
    size: float
    side: str
    market_id: str
    token_id: str
    source_file_id: int
    seq: int


@dataclass(frozen=True)
class MakerCandidate:
    market_id: str
    token_id: str
    trigger_ts: int
    trigger_price: float
    bid_price: float
    order_size_shares: float
    cancel_after_sec: int
    time_to_close_sec: int
    time_to_close_bucket: str
    total_duration_sec: int
    remaining_frac: float
    relative_time_bucket: str
    effective_precursor_lookback_sec: int
    effective_volume_lookback_sec: int
    effective_max_time_to_close_sec: int
    lookback_min_price: float
    lookback_max_price: float
    lookback_volume_usdc: float
    lookback_trade_count: int
    precursor_hit: int
    trend_anchor_far_offset_sec: int
    trend_anchor_mid_offset_sec: int
    trend_anchor_near_offset_sec: int
    trend_anchor_far_price: float
    trend_anchor_mid_price: float
    trend_anchor_near_price: float
    trend_label: str
    trend_strength: float
    category: str
    neg_risk: int
    fees_enabled: int
    is_winner: int


class RollingTradeWindow:
    def __init__(self, window_sec: int) -> None:
        self.window_sec = max(0, int(window_sec))
        self._rows: deque[tuple[int, float, float]] = deque()
        self._notional = 0.0

    def expire(self, current_ts: int) -> None:
        cutoff = int(current_ts) - self.window_sec
        while self._rows and self._rows[0][0] < cutoff:
            _ts, price, size = self._rows.popleft()
            self._notional -= price * size

    def add(self, ts: int, price: float, size: float) -> None:
        self._rows.append((ts, price, size))
        self._notional += price * size

    @property
    def trade_count(self) -> int:
        return len(self._rows)

    @property
    def volume_usdc(self) -> float:
        return max(0.0, float(self._notional))

    @property
    def min_price(self) -> float | None:
        if not self._rows:
            return None
        return min(row[1] for row in self._rows)

    @property
    def max_price(self) -> float | None:
        if not self._rows:
            return None
        return max(row[1] for row in self._rows)

    def latest_price_at_or_before(self, target_ts: int) -> float | None:
        if not self._rows:
            return None
        if target_ts <= self._rows[0][0]:
            return float(self._rows[0][1])
        for ts, price, _size in reversed(self._rows):
            if ts <= target_ts:
                return float(price)
        return float(self._rows[0][1])


def _resolve_effective_window_sec(
    total_duration_sec: int,
    *,
    absolute_sec: int,
    relative_frac: float,
    min_sec: int,
    max_sec: int,
    mode: str,
) -> int:
    candidates: list[float] = []

    if mode in ("absolute", "hybrid") and absolute_sec > 0:
        candidates.append(float(absolute_sec))

    if mode in ("relative", "hybrid") and relative_frac > 0 and total_duration_sec > 0:
        rel_sec = float(total_duration_sec) * float(relative_frac)
        rel_sec = max(float(min_sec), rel_sec)
        if max_sec > 0:
            rel_sec = min(float(max_sec), rel_sec)
        candidates.append(rel_sec)

    if not candidates:
        return max(0, int(absolute_sec))

    if mode == "hybrid":
        return max(1, int(round(min(candidates))))
    return max(1, int(round(candidates[-1])))


def _classify_trend(
    far_price: float,
    mid_price: float,
    near_price: float,
    trigger_price: float,
    *,
    min_step: float = 0.01,
) -> tuple[str, float]:
    points = [float(far_price), float(mid_price), float(near_price), float(trigger_price)]
    diffs = [points[idx + 1] - points[idx] for idx in range(len(points) - 1)]
    trend_strength = float(trigger_price - far_price)

    if max(points) - min(points) < min_step:
        return "flat", trend_strength
    if all(diff >= min_step for diff in diffs):
        return "ascending", trend_strength
    if all(diff <= -min_step for diff in diffs):
        return "descending", trend_strength
    return "mixed", trend_strength


def _time_to_close_bucket(seconds: int) -> str:
    if seconds < 60:
        return "lt_1m"
    if seconds < 5 * 60:
        return "1m_5m"
    if seconds < 15 * 60:
        return "5m_15m"
    if seconds < 60 * 60:
        return "15m_60m"
    if seconds < 6 * 60 * 60:
        return "1h_6h"
    if seconds < 24 * 60 * 60:
        return "6h_24h"
    if seconds < 3 * 24 * 60 * 60:
        return "1d_3d"
    if seconds < 7 * 24 * 60 * 60:
        return "3d_7d"
    return "gt_7d"


def _relative_time_bucket(time_to_close_sec: int, total_duration_sec: int) -> tuple[float, str]:
    if total_duration_sec <= 0:
        return 0.0, "unknown"
    frac = max(0.0, float(time_to_close_sec) / float(total_duration_sec))
    if frac <= 0.05:
        return frac, "0_5pct"
    if frac <= 0.10:
        return frac, "5_10pct"
    if frac <= 0.20:
        return frac, "10_20pct"
    if frac <= 0.30:
        return frac, "20_30pct"
    return frac, "gt_30pct"


def ensure_output_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA temp_store=MEMORY;

        CREATE TABLE IF NOT EXISTS research_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS maker_entry_candidates (
            market_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            trigger_ts INTEGER NOT NULL,
            trigger_price REAL NOT NULL,
            bid_price REAL NOT NULL,
            order_size_shares REAL NOT NULL,
            cancel_after_sec INTEGER NOT NULL,
            time_to_close_sec INTEGER NOT NULL,
            time_to_close_bucket TEXT NOT NULL,
            total_duration_sec INTEGER NOT NULL,
            remaining_frac REAL NOT NULL,
            relative_time_bucket TEXT NOT NULL,
            effective_precursor_lookback_sec INTEGER NOT NULL,
            effective_volume_lookback_sec INTEGER NOT NULL,
            effective_max_time_to_close_sec INTEGER NOT NULL,
            lookback_min_price REAL NOT NULL,
            lookback_max_price REAL NOT NULL,
            lookback_volume_usdc REAL NOT NULL,
            lookback_trade_count INTEGER NOT NULL,
            precursor_hit INTEGER NOT NULL,
            trend_anchor_far_offset_sec INTEGER NOT NULL,
            trend_anchor_mid_offset_sec INTEGER NOT NULL,
            trend_anchor_near_offset_sec INTEGER NOT NULL,
            trend_anchor_far_price REAL NOT NULL,
            trend_anchor_mid_price REAL NOT NULL,
            trend_anchor_near_price REAL NOT NULL,
            trend_label TEXT NOT NULL,
            trend_strength REAL NOT NULL,
            category TEXT NOT NULL,
            neg_risk INTEGER NOT NULL,
            fees_enabled INTEGER NOT NULL,
            is_winner INTEGER NOT NULL,
            PRIMARY KEY (token_id, trigger_ts, bid_price)
        );

        CREATE TABLE IF NOT EXISTS maker_entry_proxy_fills (
            market_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            trigger_ts INTEGER NOT NULL,
            proxy_mode TEXT NOT NULL,
            accepted_proxy INTEGER NOT NULL,
            fill_status TEXT NOT NULL,
            first_fill_ts INTEGER,
            time_to_fill_sec INTEGER,
            cumulative_fillable_size REAL NOT NULL,
            required_fill_size REAL NOT NULL,
            fill_price REAL NOT NULL,
            cancel_after_sec INTEGER NOT NULL,
            bid_price REAL NOT NULL,
            order_size_shares REAL NOT NULL,
            time_to_close_bucket TEXT NOT NULL,
            relative_time_bucket TEXT NOT NULL,
            category TEXT NOT NULL,
            neg_risk INTEGER NOT NULL,
            fees_enabled INTEGER NOT NULL,
            is_winner INTEGER NOT NULL,
            pnl_per_share REAL NOT NULL,
            pnl_per_order REAL NOT NULL,
            PRIMARY KEY (token_id, trigger_ts, bid_price, proxy_mode)
        );

        CREATE TABLE IF NOT EXISTS maker_entry_proxy_summary (
            proxy_mode TEXT NOT NULL,
            bid_price REAL NOT NULL,
            time_to_close_bucket TEXT NOT NULL,
            relative_time_bucket TEXT NOT NULL,
            category TEXT NOT NULL,
            fees_enabled INTEGER NOT NULL,
            n_candidates INTEGER NOT NULL,
            accepted_count INTEGER NOT NULL,
            filled_count INTEGER NOT NULL,
            winner_count_if_filled INTEGER NOT NULL,
            fill_rate REAL NOT NULL,
            accept_rate REAL NOT NULL,
            win_rate_if_filled REAL NOT NULL,
            avg_ev_per_placed_share REAL NOT NULL,
            avg_ev_per_filled_share REAL NOT NULL,
            avg_ev_per_placed_order REAL NOT NULL,
            avg_ev_per_filled_order REAL NOT NULL,
            median_time_to_fill_sec REAL NOT NULL,
            avg_fillable_size REAL NOT NULL,
            PRIMARY KEY (proxy_mode, bid_price, time_to_close_bucket, relative_time_bucket, category, fees_enabled)
        );
        """
    )


def reset_output_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DELETE FROM research_meta;
        DELETE FROM maker_entry_candidates;
        DELETE FROM maker_entry_proxy_fills;
        DELETE FROM maker_entry_proxy_summary;
        """
    )
    conn.commit()


def load_token_meta(dataset_db_path: Path) -> dict[str, TokenMeta]:
    conn = sqlite3.connect(dataset_db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
            t.token_id,
            t.market_id,
            COALESCE(t.is_winner, 0) AS is_winner,
            t.token_order,
            COALESCE(m.category, 'unknown') AS category,
            COALESCE(m.neg_risk, 0) AS neg_risk,
            COALESCE(m.fees_enabled, 0) AS fees_enabled,
            COALESCE(m.duration_hours, 0) AS duration_hours,
            COALESCE(m.closed_time, 0) AS closed_time
        FROM tokens t
        JOIN markets m ON m.id = t.market_id
        WHERE COALESCE(m.closed_time, 0) > 0
        """
    ).fetchall()
    conn.close()
    return {
        str(row["token_id"]): TokenMeta(
            token_id=str(row["token_id"]),
            market_id=str(row["market_id"]),
            is_winner=int(row["is_winner"] or 0),
            token_order=int(row["token_order"]) if row["token_order"] is not None else None,
            category=str(row["category"]),
            neg_risk=int(row["neg_risk"] or 0),
            fees_enabled=int(row["fees_enabled"] or 0),
            duration_hours=float(row["duration_hours"] or 0.0),
            closed_time=int(row["closed_time"] or 0),
        )
        for row in rows
    }


def iter_token_trades(tape_db_path: Path, selected_token_ids: set[str] | None = None) -> Iterable[tuple[str, list[TradePoint]]]:
    conn = sqlite3.connect(tape_db_path)
    conn.row_factory = sqlite3.Row
    current_token_id: str | None = None
    bucket: list[TradePoint] = []
    try:
        if selected_token_ids:
            conn.execute("CREATE TEMP TABLE selected_tokens (id TEXT PRIMARY KEY)")
            conn.executemany(
                "INSERT INTO selected_tokens(id) VALUES (?)",
                ((token_id,) for token_id in sorted(selected_token_ids)),
            )
            query = (
                "SELECT source_file_id, seq, timestamp, market_id, token_id, price, size, side "
                "FROM tape WHERE token_id IN (SELECT id FROM selected_tokens) "
                "ORDER BY token_id ASC, timestamp ASC, source_file_id ASC, seq ASC"
            )
        else:
            query = (
                "SELECT source_file_id, seq, timestamp, market_id, token_id, price, size, side "
                "FROM tape ORDER BY token_id ASC, timestamp ASC, source_file_id ASC, seq ASC"
            )

        for row in conn.execute(query):
            token_id = str(row["token_id"])
            trade = TradePoint(
                timestamp=int(row["timestamp"]),
                market_id=str(row["market_id"]),
                token_id=token_id,
                price=float(row["price"]),
                size=float(row["size"]),
                side=str(row["side"] or "").upper(),
                source_file_id=int(row["source_file_id"]),
                seq=int(row["seq"]),
            )
            if current_token_id is None:
                current_token_id = token_id
            elif token_id != current_token_id:
                yield current_token_id, bucket
                bucket = []
                current_token_id = token_id
            bucket.append(trade)
        if current_token_id is not None and bucket:
            yield current_token_id, bucket
    finally:
        conn.close()


def find_first_candidate(
    *,
    meta: TokenMeta,
    trades: Sequence[TradePoint],
    trigger_price_min: float,
    trigger_price_max: float,
    precursor_max_price: float,
    precursor_lookback_sec: int,
    precursor_lookback_frac: float,
    precursor_lookback_min_sec: int,
    precursor_lookback_max_sec: int,
    lookback_volume_sec: int,
    lookback_volume_frac: float,
    lookback_volume_min_sec: int,
    lookback_volume_max_sec: int,
    min_lookback_volume_usdc: float,
    max_time_to_close_sec: int,
    max_time_to_close_frac: float,
    max_time_to_close_min_sec: int,
    temporal_filter_mode: str,
    bid_price: float,
    order_size_shares: float,
    cancel_after_sec: int,
) -> MakerCandidate | None:
    total_duration_sec = max(0, int(round(meta.duration_hours * 3600)))
    effective_precursor_lookback_sec = _resolve_effective_window_sec(
        total_duration_sec,
        absolute_sec=precursor_lookback_sec,
        relative_frac=precursor_lookback_frac,
        min_sec=precursor_lookback_min_sec,
        max_sec=precursor_lookback_max_sec,
        mode=temporal_filter_mode,
    )
    effective_volume_lookback_sec = _resolve_effective_window_sec(
        total_duration_sec,
        absolute_sec=lookback_volume_sec,
        relative_frac=lookback_volume_frac,
        min_sec=lookback_volume_min_sec,
        max_sec=lookback_volume_max_sec,
        mode=temporal_filter_mode,
    )
    effective_max_time_to_close_sec = _resolve_effective_window_sec(
        total_duration_sec,
        absolute_sec=max_time_to_close_sec,
        relative_frac=max_time_to_close_frac,
        min_sec=max_time_to_close_min_sec,
        max_sec=0,
        mode=temporal_filter_mode,
    )

    precursor_window = RollingTradeWindow(effective_precursor_lookback_sec)
    volume_window = RollingTradeWindow(effective_volume_lookback_sec)

    for trade in trades:
        precursor_window.expire(trade.timestamp)
        volume_window.expire(trade.timestamp)
        time_to_close_sec = meta.closed_time - trade.timestamp
        remaining_frac, relative_time_bucket = _relative_time_bucket(time_to_close_sec, total_duration_sec)
        lookback_min = precursor_window.min_price
        lookback_max = precursor_window.max_price
        lookback_vol = volume_window.volume_usdc
        lookback_n = volume_window.trade_count
        precursor_hit = int(lookback_min is not None and lookback_min <= precursor_max_price)

        far_offset_sec = max(1, effective_precursor_lookback_sec)
        mid_offset_sec = max(1, int(round(effective_precursor_lookback_sec * 0.50)))
        near_offset_sec = max(1, int(round(effective_precursor_lookback_sec * 0.20)))
        far_price = precursor_window.latest_price_at_or_before(trade.timestamp - far_offset_sec)
        mid_price = precursor_window.latest_price_at_or_before(trade.timestamp - mid_offset_sec)
        near_price = precursor_window.latest_price_at_or_before(trade.timestamp - near_offset_sec)
        trend_label = "unknown"
        trend_strength = 0.0
        if far_price is not None and mid_price is not None and near_price is not None:
            trend_label, trend_strength = _classify_trend(
                far_price,
                mid_price,
                near_price,
                trade.price,
            )

        if (
            trigger_price_min <= trade.price <= trigger_price_max
            and 0 < time_to_close_sec <= effective_max_time_to_close_sec
            and precursor_hit == 1
            and lookback_vol >= min_lookback_volume_usdc
            and bid_price >= trade.price
        ):
            return MakerCandidate(
                market_id=meta.market_id,
                token_id=meta.token_id,
                trigger_ts=trade.timestamp,
                trigger_price=trade.price,
                bid_price=bid_price,
                order_size_shares=order_size_shares,
                cancel_after_sec=cancel_after_sec,
                time_to_close_sec=time_to_close_sec,
                time_to_close_bucket=_time_to_close_bucket(time_to_close_sec),
                total_duration_sec=total_duration_sec,
                remaining_frac=remaining_frac,
                relative_time_bucket=relative_time_bucket,
                effective_precursor_lookback_sec=effective_precursor_lookback_sec,
                effective_volume_lookback_sec=effective_volume_lookback_sec,
                effective_max_time_to_close_sec=effective_max_time_to_close_sec,
                lookback_min_price=float(lookback_min or trade.price),
                lookback_max_price=float(lookback_max or trade.price),
                lookback_volume_usdc=float(lookback_vol),
                lookback_trade_count=int(lookback_n),
                precursor_hit=1,
                trend_anchor_far_offset_sec=far_offset_sec,
                trend_anchor_mid_offset_sec=mid_offset_sec,
                trend_anchor_near_offset_sec=near_offset_sec,
                trend_anchor_far_price=float(far_price if far_price is not None else trade.price),
                trend_anchor_mid_price=float(mid_price if mid_price is not None else trade.price),
                trend_anchor_near_price=float(near_price if near_price is not None else trade.price),
                trend_label=trend_label,
                trend_strength=float(trend_strength),
                category=meta.category,
                neg_risk=meta.neg_risk,
                fees_enabled=meta.fees_enabled,
                is_winner=meta.is_winner,
            )

        precursor_window.add(trade.timestamp, trade.price, trade.size)
        volume_window.add(trade.timestamp, trade.price, trade.size)
    return None


def find_first_candidate_touch(
    *,
    meta: TokenMeta,
    trades: Sequence[TradePoint],
    direction_gap: float,
    direction_lookback_sec: int,
    touch_volume_lookback_sec: int,
    touch_direction: str,
    max_time_to_close_sec: int,
    max_time_to_close_frac: float,
    max_time_to_close_min_sec: int,
    temporal_filter_mode: str,
    min_lookback_volume_usdc: float,
    bid_price: float,
    order_size_shares: float,
    cancel_after_sec: int,
) -> MakerCandidate | None:
    total_duration_sec = max(0, int(round(meta.duration_hours * 3600)))
    effective_max_time_to_close_sec = _resolve_effective_window_sec(
        total_duration_sec,
        absolute_sec=max_time_to_close_sec,
        relative_frac=max_time_to_close_frac,
        min_sec=max_time_to_close_min_sec,
        max_sec=0,
        mode=temporal_filter_mode,
    )
    touch_events = build_touch_events_for_token(
        meta=meta,
        trades=trades,
        price_levels=(float(bid_price),),
        direction_gap=float(direction_gap),
        direction_lookback_sec=int(direction_lookback_sec),
        touch_volume_lookback_sec=int(touch_volume_lookback_sec),
    )
    filtered_events = [
        event
        for event in touch_events
        if str(event.touch_direction) == touch_direction
        and event.lookback_volume_usdc >= min_lookback_volume_usdc
        and 0 < event.time_to_close_sec <= effective_max_time_to_close_sec
    ]
    if not filtered_events:
        return None
    event = min(filtered_events, key=lambda row: row.first_touch_ts)
    remaining_frac, relative_time_bucket = _relative_time_bucket(event.time_to_close_sec, total_duration_sec)
    if touch_direction == "ascending":
        trend_strength = float(event.touch_price - event.lookback_min_price)
    else:
        trend_strength = float(event.touch_price - event.lookback_max_price)
    return MakerCandidate(
        market_id=meta.market_id,
        token_id=meta.token_id,
        trigger_ts=int(event.first_touch_ts),
        trigger_price=float(event.touch_price),
        bid_price=bid_price,
        order_size_shares=order_size_shares,
        cancel_after_sec=cancel_after_sec,
        time_to_close_sec=int(event.time_to_close_sec),
        time_to_close_bucket=_time_to_close_bucket(int(event.time_to_close_sec)),
        total_duration_sec=total_duration_sec,
        remaining_frac=remaining_frac,
        relative_time_bucket=relative_time_bucket,
        effective_precursor_lookback_sec=int(direction_lookback_sec),
        effective_volume_lookback_sec=int(touch_volume_lookback_sec),
        effective_max_time_to_close_sec=effective_max_time_to_close_sec,
        lookback_min_price=float(event.lookback_min_price),
        lookback_max_price=float(event.lookback_max_price),
        lookback_volume_usdc=float(event.lookback_volume_usdc),
        lookback_trade_count=int(event.lookback_trade_count),
        precursor_hit=1,
        trend_anchor_far_offset_sec=int(direction_lookback_sec),
        trend_anchor_mid_offset_sec=max(1, int(round(direction_lookback_sec * 0.50))),
        trend_anchor_near_offset_sec=max(1, int(round(direction_lookback_sec * 0.20))),
        trend_anchor_far_price=float(event.lookback_min_price if touch_direction == "ascending" else event.lookback_max_price),
        trend_anchor_mid_price=float(event.prev_price),
        trend_anchor_near_price=float(event.touch_price),
        trend_label=str(event.touch_direction),
        trend_strength=trend_strength,
        category=meta.category,
        neg_risk=meta.neg_risk,
        fees_enabled=meta.fees_enabled,
        is_winner=meta.is_winner,
    )


def build_fill_rows(
    *,
    candidate: MakerCandidate,
    trades: Sequence[TradePoint],
    rest_delay_sec: int,
    conservative_queue_multiplier: float,
) -> list[tuple]:
    window_start = candidate.trigger_ts + max(0, rest_delay_sec)
    window_end = candidate.trigger_ts + candidate.cancel_after_sec

    future_trades = [
        trade for trade in trades
        if window_start < trade.timestamp <= window_end
    ]

    def _can_fill_passive_buy(trade: TradePoint) -> bool:
        return trade.price <= candidate.bid_price + 1e-9 and trade.side in ("SELL", "")

    raw_fillable_size = sum(trade.size for trade in future_trades if _can_fill_passive_buy(trade))

    def _first_fill_ts(required_size: float) -> int | None:
        cum = 0.0
        for trade in future_trades:
            if _can_fill_passive_buy(trade):
                cum += trade.size
                if cum + 1e-9 >= required_size:
                    return trade.timestamp
        return None

    rows: list[tuple] = []
    for proxy_mode, required_multiplier in (
        ("optimistic", 1.0),
        ("conservative", max(1.0, conservative_queue_multiplier)),
    ):
        required_size = candidate.order_size_shares * required_multiplier
        first_fill_ts = _first_fill_ts(required_size)
        filled = first_fill_ts is not None
        fill_status = "filled" if filled else "unfilled"
        accepted_proxy = 1
        pnl_per_share = (1.0 - candidate.bid_price) if (filled and candidate.is_winner) else (-candidate.bid_price if filled else 0.0)
        pnl_per_order = pnl_per_share * candidate.order_size_shares
        rows.append(
            (
                candidate.market_id,
                candidate.token_id,
                candidate.trigger_ts,
                proxy_mode,
                accepted_proxy,
                fill_status,
                first_fill_ts,
                (first_fill_ts - candidate.trigger_ts) if first_fill_ts is not None else None,
                float(raw_fillable_size),
                float(required_size),
                candidate.bid_price,
                candidate.cancel_after_sec,
                candidate.bid_price,
                candidate.order_size_shares,
                candidate.time_to_close_bucket,
                candidate.relative_time_bucket,
                candidate.category,
                candidate.neg_risk,
                candidate.fees_enabled,
                candidate.is_winner,
                float(pnl_per_share),
                float(pnl_per_order),
            )
        )
    return rows


def write_candidates(conn: sqlite3.Connection, rows: Iterable[tuple]) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO maker_entry_candidates(
            market_id, token_id, trigger_ts, trigger_price, bid_price, order_size_shares,
            cancel_after_sec, time_to_close_sec, time_to_close_bucket,
            total_duration_sec, remaining_frac, relative_time_bucket,
            effective_precursor_lookback_sec, effective_volume_lookback_sec, effective_max_time_to_close_sec,
            lookback_min_price, lookback_max_price, lookback_volume_usdc,
            lookback_trade_count, precursor_hit,
            trend_anchor_far_offset_sec, trend_anchor_mid_offset_sec, trend_anchor_near_offset_sec,
            trend_anchor_far_price, trend_anchor_mid_price, trend_anchor_near_price,
            trend_label, trend_strength,
            category, neg_risk, fees_enabled, is_winner
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def write_fills(conn: sqlite3.Connection, rows: Iterable[tuple]) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO maker_entry_proxy_fills(
            market_id, token_id, trigger_ts, proxy_mode, accepted_proxy, fill_status,
            first_fill_ts, time_to_fill_sec, cumulative_fillable_size, required_fill_size,
            fill_price, cancel_after_sec, bid_price, order_size_shares, time_to_close_bucket,
            relative_time_bucket, category, neg_risk, fees_enabled, is_winner, pnl_per_share, pnl_per_order
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def build_summary(conn: sqlite3.Connection) -> int:
    conn.execute("DELETE FROM maker_entry_proxy_summary")
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                proxy_mode,
                bid_price,
                time_to_close_bucket,
                relative_time_bucket,
                category,
                fees_enabled,
                accepted_proxy,
                CASE WHEN fill_status='filled' THEN 1 ELSE 0 END AS is_filled,
                CASE WHEN fill_status='filled' AND is_winner=1 THEN 1 ELSE 0 END AS is_winner_filled,
                pnl_per_share,
                pnl_per_order,
                cumulative_fillable_size,
                time_to_fill_sec,
                ROW_NUMBER() OVER (
                    PARTITION BY proxy_mode, bid_price, time_to_close_bucket, relative_time_bucket, category, fees_enabled
                    ORDER BY time_to_fill_sec
                ) AS rn,
                COUNT(*) OVER (
                    PARTITION BY proxy_mode, bid_price, time_to_close_bucket, relative_time_bucket, category, fees_enabled
                ) AS cnt
            FROM maker_entry_proxy_fills
        ), summary AS (
            SELECT
                proxy_mode,
                bid_price,
                time_to_close_bucket,
                relative_time_bucket,
                category,
                fees_enabled,
                COUNT(*) AS n_candidates,
                SUM(accepted_proxy) AS accepted_count,
                SUM(is_filled) AS filled_count,
                SUM(is_winner_filled) AS winner_count_if_filled,
                AVG(CAST(is_filled AS REAL)) AS fill_rate,
                AVG(CAST(accepted_proxy AS REAL)) AS accept_rate,
                AVG(CASE WHEN is_filled=1 THEN CAST(is_winner_filled AS REAL) END) AS win_rate_if_filled,
                AVG(pnl_per_share) AS avg_ev_per_placed_share,
                AVG(CASE WHEN is_filled=1 THEN pnl_per_share END) AS avg_ev_per_filled_share,
                AVG(pnl_per_order) AS avg_ev_per_placed_order,
                AVG(CASE WHEN is_filled=1 THEN pnl_per_order END) AS avg_ev_per_filled_order,
                AVG(CASE WHEN is_filled=1 THEN cumulative_fillable_size END) AS avg_fillable_size,
                MIN(CASE WHEN rn = CAST(((cnt + 1) / 2) AS INT) THEN time_to_fill_sec END) AS median_time_to_fill_sec
            FROM ranked
            GROUP BY proxy_mode, bid_price, time_to_close_bucket, relative_time_bucket, category, fees_enabled
        )
        SELECT
            proxy_mode,
            bid_price,
            time_to_close_bucket,
            relative_time_bucket,
            category,
            fees_enabled,
            n_candidates,
            accepted_count,
            filled_count,
            winner_count_if_filled,
            COALESCE(fill_rate, 0.0),
            COALESCE(accept_rate, 0.0),
            COALESCE(win_rate_if_filled, 0.0),
            COALESCE(avg_ev_per_placed_share, 0.0),
            COALESCE(avg_ev_per_filled_share, 0.0),
            COALESCE(avg_ev_per_placed_order, 0.0),
            COALESCE(avg_ev_per_filled_order, 0.0),
            COALESCE(median_time_to_fill_sec, 0.0),
            COALESCE(avg_fillable_size, 0.0)
        FROM summary
        """
    ).fetchall()
    conn.executemany(
        """
        INSERT INTO maker_entry_proxy_summary(
            proxy_mode, bid_price, time_to_close_bucket, relative_time_bucket, category, fees_enabled,
            n_candidates, accepted_count, filled_count, winner_count_if_filled,
            fill_rate, accept_rate, win_rate_if_filled,
            avg_ev_per_placed_share, avg_ev_per_filled_share,
            avg_ev_per_placed_order, avg_ev_per_filled_order,
            median_time_to_fill_sec, avg_fillable_size
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def build_maker_postonly_proxy_research(
    *,
    dataset_db_path: Path,
    tape_db_path: Path,
    output_db_path: Path,
    candidate_mode: str,
    trigger_price_min: float,
    trigger_price_max: float,
    precursor_max_price: float,
    direction_gap: float,
    direction_lookback_sec: int,
    touch_volume_lookback_sec: int,
    touch_direction: str,
    precursor_lookback_sec: int,
    precursor_lookback_frac: float,
    precursor_lookback_min_sec: int,
    precursor_lookback_max_sec: int,
    lookback_volume_sec: int,
    lookback_volume_frac: float,
    lookback_volume_min_sec: int,
    lookback_volume_max_sec: int,
    min_lookback_volume_usdc: float,
    max_time_to_close_sec: int,
    max_time_to_close_frac: float,
    max_time_to_close_min_sec: int,
    temporal_filter_mode: str,
    min_duration_hours: float,
    max_duration_hours: float,
    progress_every_tokens: int,
    bid_price: float,
    order_size_shares: float,
    cancel_after_sec: int,
    rest_delay_sec: int,
    conservative_queue_multiplier: float,
) -> dict[str, int]:
    token_meta = load_token_meta(dataset_db_path)
    logger.info("Loaded %s labeled tokens with valid closed_time", len(token_meta))

    output_conn = sqlite3.connect(output_db_path)
    ensure_output_schema(output_conn)
    reset_output_tables(output_conn)
    output_conn.executemany(
        "INSERT INTO research_meta(key, value) VALUES (?, ?)",
        [
            ("dataset_db_path", str(dataset_db_path)),
            ("tape_db_path", str(tape_db_path)),
            ("candidate_mode", candidate_mode),
            ("trigger_price_min", f"{trigger_price_min:.4f}"),
            ("trigger_price_max", f"{trigger_price_max:.4f}"),
            ("precursor_max_price", f"{precursor_max_price:.4f}"),
            ("direction_gap", f"{direction_gap:.4f}"),
            ("direction_lookback_sec", str(direction_lookback_sec)),
            ("touch_volume_lookback_sec", str(touch_volume_lookback_sec)),
            ("touch_direction", touch_direction),
            ("precursor_lookback_sec", str(precursor_lookback_sec)),
            ("precursor_lookback_frac", f"{precursor_lookback_frac:.4f}"),
            ("precursor_lookback_min_sec", str(precursor_lookback_min_sec)),
            ("precursor_lookback_max_sec", str(precursor_lookback_max_sec)),
            ("lookback_volume_sec", str(lookback_volume_sec)),
            ("lookback_volume_frac", f"{lookback_volume_frac:.6f}"),
            ("lookback_volume_min_sec", str(lookback_volume_min_sec)),
            ("lookback_volume_max_sec", str(lookback_volume_max_sec)),
            ("min_lookback_volume_usdc", f"{min_lookback_volume_usdc:.4f}"),
            ("max_time_to_close_sec", str(max_time_to_close_sec)),
            ("max_time_to_close_frac", f"{max_time_to_close_frac:.4f}"),
            ("max_time_to_close_min_sec", str(max_time_to_close_min_sec)),
            ("temporal_filter_mode", temporal_filter_mode),
            ("min_duration_hours", f"{min_duration_hours:.6f}"),
            ("max_duration_hours", f"{max_duration_hours:.6f}"),
            ("progress_every_tokens", str(progress_every_tokens)),
            ("bid_price", f"{bid_price:.4f}"),
            ("order_size_shares", f"{order_size_shares:.4f}"),
            ("cancel_after_sec", str(cancel_after_sec)),
            ("rest_delay_sec", str(rest_delay_sec)),
            ("conservative_queue_multiplier", f"{conservative_queue_multiplier:.4f}"),
            ("fill_side_policy", "passive_buy_fills_on_sell_or_unknown_side"),
            ("trend_anchor_fractions", "1.00,0.50,0.20"),
        ],
    )
    output_conn.commit()

    stats = {
        "tokens_seen": 0,
        "eligible_tokens": 0,
        "duration_filtered_out": 0,
        "selected_tokens": 0,
        "candidates": 0,
        "fill_rows": 0,
        "summary_rows": 0,
    }

    candidate_rows: list[tuple] = []
    fill_rows: list[tuple] = []

    selected_token_ids = {
        token_id
        for token_id, meta in token_meta.items()
        if not (min_duration_hours > 0 and meta.duration_hours + 1e-9 < min_duration_hours)
        and not (max_duration_hours > 0 and meta.duration_hours - 1e-9 > max_duration_hours)
    }
    stats["selected_tokens"] = len(selected_token_ids)
    stats["duration_filtered_out"] = max(0, len(token_meta) - len(selected_token_ids))
    logger.info(
        "Selected %s tokens after duration filter (filtered_out=%s)",
        stats["selected_tokens"],
        stats["duration_filtered_out"],
    )

    for token_id, trades in iter_token_trades(tape_db_path, selected_token_ids=selected_token_ids):
        stats["tokens_seen"] += 1
        meta = token_meta.get(token_id)
        if meta is None:
            continue
        stats["eligible_tokens"] += 1
        if candidate_mode == "touch":
            candidate = find_first_candidate_touch(
                meta=meta,
                trades=trades,
                direction_gap=direction_gap,
                direction_lookback_sec=direction_lookback_sec,
                touch_volume_lookback_sec=touch_volume_lookback_sec,
                touch_direction=touch_direction,
                max_time_to_close_sec=max_time_to_close_sec,
                max_time_to_close_frac=max_time_to_close_frac,
                max_time_to_close_min_sec=max_time_to_close_min_sec,
                temporal_filter_mode=temporal_filter_mode,
                min_lookback_volume_usdc=min_lookback_volume_usdc,
                bid_price=bid_price,
                order_size_shares=order_size_shares,
                cancel_after_sec=cancel_after_sec,
            )
        else:
            candidate = find_first_candidate(
                meta=meta,
                trades=trades,
                trigger_price_min=trigger_price_min,
                trigger_price_max=trigger_price_max,
                precursor_max_price=precursor_max_price,
                precursor_lookback_sec=precursor_lookback_sec,
                precursor_lookback_frac=precursor_lookback_frac,
                precursor_lookback_min_sec=precursor_lookback_min_sec,
                precursor_lookback_max_sec=precursor_lookback_max_sec,
                lookback_volume_sec=lookback_volume_sec,
                lookback_volume_frac=lookback_volume_frac,
                lookback_volume_min_sec=lookback_volume_min_sec,
                lookback_volume_max_sec=lookback_volume_max_sec,
                min_lookback_volume_usdc=min_lookback_volume_usdc,
                max_time_to_close_sec=max_time_to_close_sec,
                max_time_to_close_frac=max_time_to_close_frac,
                max_time_to_close_min_sec=max_time_to_close_min_sec,
                temporal_filter_mode=temporal_filter_mode,
                bid_price=bid_price,
                order_size_shares=order_size_shares,
                cancel_after_sec=cancel_after_sec,
            )
        if candidate is None:
            if progress_every_tokens > 0 and stats["tokens_seen"] % progress_every_tokens == 0:
                logger.info(
                    "progress tokens_seen=%s eligible_tokens=%s duration_filtered_out=%s candidates=%s fill_rows=%s",
                    stats["tokens_seen"],
                    stats["eligible_tokens"],
                    stats["duration_filtered_out"],
                    stats["candidates"],
                    stats["fill_rows"],
                )
            continue
        stats["candidates"] += 1
        candidate_rows.append(
            (
                candidate.market_id,
                candidate.token_id,
                candidate.trigger_ts,
                candidate.trigger_price,
                candidate.bid_price,
                candidate.order_size_shares,
                candidate.cancel_after_sec,
                candidate.time_to_close_sec,
                candidate.time_to_close_bucket,
                candidate.total_duration_sec,
                candidate.remaining_frac,
                candidate.relative_time_bucket,
                candidate.effective_precursor_lookback_sec,
                candidate.effective_volume_lookback_sec,
                candidate.effective_max_time_to_close_sec,
                candidate.lookback_min_price,
                candidate.lookback_max_price,
                candidate.lookback_volume_usdc,
                candidate.lookback_trade_count,
                candidate.precursor_hit,
                candidate.trend_anchor_far_offset_sec,
                candidate.trend_anchor_mid_offset_sec,
                candidate.trend_anchor_near_offset_sec,
                candidate.trend_anchor_far_price,
                candidate.trend_anchor_mid_price,
                candidate.trend_anchor_near_price,
                candidate.trend_label,
                candidate.trend_strength,
                candidate.category,
                candidate.neg_risk,
                candidate.fees_enabled,
                candidate.is_winner,
            )
        )
        new_fill_rows = build_fill_rows(
            candidate=candidate,
            trades=trades,
            rest_delay_sec=rest_delay_sec,
            conservative_queue_multiplier=conservative_queue_multiplier,
        )
        stats["fill_rows"] += len(new_fill_rows)
        fill_rows.extend(new_fill_rows)

        if progress_every_tokens > 0 and stats["tokens_seen"] % progress_every_tokens == 0:
            logger.info(
                "progress tokens_seen=%s eligible_tokens=%s duration_filtered_out=%s candidates=%s fill_rows=%s",
                stats["tokens_seen"],
                stats["eligible_tokens"],
                stats["duration_filtered_out"],
                stats["candidates"],
                stats["fill_rows"],
            )

    logger.info(
        "Finished token scan tokens_seen=%s eligible_tokens=%s duration_filtered_out=%s candidates=%s fill_rows=%s",
        stats["tokens_seen"],
        stats["eligible_tokens"],
        stats["duration_filtered_out"],
        stats["candidates"],
        stats["fill_rows"],
    )

    logger.info("Writing %s candidate rows", len(candidate_rows))
    write_candidates(output_conn, candidate_rows)
    logger.info("Writing %s fill rows", len(fill_rows))
    write_fills(output_conn, fill_rows)
    stats["summary_rows"] = build_summary(output_conn)
    logger.info("Built %s summary rows", stats["summary_rows"])
    output_conn.close()
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Proxy backtest for post-only maker entry on historical tape")
    parser.add_argument("--db", dest="dataset_db", type=Path, default=DB_PATH)
    parser.add_argument("--tape-db", dest="tape_db", type=Path, default=DEFAULT_TAPE_DB_PATH)
    parser.add_argument("--output-db", dest="output_db", type=Path, default=DEFAULT_OUTPUT_DB_PATH)
    parser.add_argument("--candidate-mode", choices=("touch", "legacy"), default="touch")
    parser.add_argument("--trigger-price-min", type=float, default=0.70)
    parser.add_argument("--trigger-price-max", type=float, default=0.75)
    parser.add_argument("--precursor-max-price", type=float, default=0.70)
    parser.add_argument("--direction-gap", type=float, default=0.10)
    parser.add_argument("--direction-lookback-sec", type=int, default=5 * 60)
    parser.add_argument("--touch-volume-lookback-sec", type=int, default=60)
    parser.add_argument("--touch-direction", choices=("ascending", "descending"), default="ascending")
    parser.add_argument("--temporal-filter-mode", choices=("absolute", "relative", "hybrid"), default="hybrid")
    parser.add_argument("--precursor-lookback-sec", type=int, default=15 * 60)
    parser.add_argument("--precursor-lookback-frac", type=float, default=0.25)
    parser.add_argument("--precursor-lookback-min-sec", type=int, default=5 * 60)
    parser.add_argument("--precursor-lookback-max-sec", type=int, default=15 * 60)
    parser.add_argument("--lookback-volume-sec", type=int, default=60)
    parser.add_argument("--lookback-volume-frac", type=float, default=(1.0 / 60.0))
    parser.add_argument("--lookback-volume-min-sec", type=int, default=30)
    parser.add_argument("--lookback-volume-max-sec", type=int, default=5 * 60)
    parser.add_argument("--min-lookback-volume-usdc", type=float, default=30.0)
    parser.add_argument("--max-time-to-close-sec", type=int, default=30 * 60)
    parser.add_argument("--max-time-to-close-frac", type=float, default=0.25)
    parser.add_argument("--max-time-to-close-min-sec", type=int, default=5 * 60)
    parser.add_argument("--min-duration-hours", type=float, default=0.0)
    parser.add_argument("--max-duration-hours", type=float, default=0.0)
    parser.add_argument("--progress-every-tokens", type=int, default=5000)
    parser.add_argument("--bid-price", type=float, default=0.80)
    parser.add_argument("--order-size-shares", type=float, default=100.0)
    parser.add_argument("--cancel-after-sec", type=int, default=10 * 60)
    parser.add_argument("--rest-delay-sec", type=int, default=5)
    parser.add_argument("--conservative-queue-multiplier", type=float, default=2.0)
    return parser.parse_args()


def main() -> None:
    ensure_runtime_dirs()
    args = parse_args()
    if args.output_db.exists():
        args.output_db.unlink()
    stats = build_maker_postonly_proxy_research(
        dataset_db_path=args.dataset_db,
        tape_db_path=args.tape_db,
        output_db_path=args.output_db,
        candidate_mode=args.candidate_mode,
        trigger_price_min=args.trigger_price_min,
        trigger_price_max=args.trigger_price_max,
        precursor_max_price=args.precursor_max_price,
        direction_gap=args.direction_gap,
        direction_lookback_sec=args.direction_lookback_sec,
        touch_volume_lookback_sec=args.touch_volume_lookback_sec,
        touch_direction=args.touch_direction,
        precursor_lookback_sec=args.precursor_lookback_sec,
        precursor_lookback_frac=args.precursor_lookback_frac,
        precursor_lookback_min_sec=args.precursor_lookback_min_sec,
        precursor_lookback_max_sec=args.precursor_lookback_max_sec,
        lookback_volume_sec=args.lookback_volume_sec,
        lookback_volume_frac=args.lookback_volume_frac,
        lookback_volume_min_sec=args.lookback_volume_min_sec,
        lookback_volume_max_sec=args.lookback_volume_max_sec,
        min_lookback_volume_usdc=args.min_lookback_volume_usdc,
        max_time_to_close_sec=args.max_time_to_close_sec,
        max_time_to_close_frac=args.max_time_to_close_frac,
        max_time_to_close_min_sec=args.max_time_to_close_min_sec,
        temporal_filter_mode=args.temporal_filter_mode,
        min_duration_hours=args.min_duration_hours,
        max_duration_hours=args.max_duration_hours,
        progress_every_tokens=args.progress_every_tokens,
        bid_price=args.bid_price,
        order_size_shares=args.order_size_shares,
        cancel_after_sec=args.cancel_after_sec,
        rest_delay_sec=args.rest_delay_sec,
        conservative_queue_multiplier=args.conservative_queue_multiplier,
    )
    logger.info("Research DB written to %s", args.output_db)
    for key in sorted(stats):
        logger.info("%s=%s", key, stats[key])


if __name__ == "__main__":
    main()
