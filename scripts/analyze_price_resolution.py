#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import os
import sqlite3
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
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


logger = setup_logger("price_resolution_research")
DEFAULT_OUTPUT_DB_PATH = DATA_DIR / "price_resolution_research.db"
DEFAULT_REACTION_WINDOWS = (60, 300)
DEFAULT_PRICE_STEP = 0.05
DEFAULT_DIRECTION_GAP = 0.10
DEFAULT_DIRECTION_LOOKBACK_SEC = 300
DEFAULT_TOUCH_VOLUME_LOOKBACK_SEC = 60


@dataclass(frozen=True)
class TokenMeta:
    token_id: str
    market_id: str
    is_winner: int
    token_order: int | None
    category: str
    neg_risk: int
    closed_time: int


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
class TouchEvent:
    market_id: str
    token_id: str
    price_level: float
    touch_direction: str
    first_touch_ts: int
    touch_price: float
    touch_size: float
    prev_price: float
    lookback_min_price: float
    lookback_max_price: float
    lookback_volume_usdc: float
    lookback_trade_count: int
    max_price_after_touch: float
    min_price_after_touch: float
    close_price_after_touch: float
    closed_time: int
    time_to_close_sec: int
    minutes_to_resolution: float
    token_order: int | None
    is_winner: int
    category: str
    neg_risk: int


class RollingTradeWindow:
    def __init__(self, window_sec: int) -> None:
        self.window_sec = max(0, int(window_sec))
        self._rows: deque[tuple[int, float, float]] = deque()
        self._minq: deque[tuple[int, float]] = deque()
        self._maxq: deque[tuple[int, float]] = deque()
        self._notional_sum = 0.0

    def expire(self, current_ts: int) -> None:
        if self.window_sec <= 0:
            self.clear()
            return
        cutoff = int(current_ts) - self.window_sec
        while self._rows and self._rows[0][0] < cutoff:
            ts, price, size = self._rows.popleft()
            self._notional_sum -= price * size
            if self._minq and self._minq[0][0] == ts and self._minq[0][1] == price:
                self._minq.popleft()
            if self._maxq and self._maxq[0][0] == ts and self._maxq[0][1] == price:
                self._maxq.popleft()

    def add(self, ts: int, price: float, size: float) -> None:
        self._rows.append((ts, price, size))
        self._notional_sum += price * size

        while self._minq and self._minq[-1][1] >= price:
            self._minq.pop()
        self._minq.append((ts, price))

        while self._maxq and self._maxq[-1][1] <= price:
            self._maxq.pop()
        self._maxq.append((ts, price))

    def clear(self) -> None:
        self._rows.clear()
        self._minq.clear()
        self._maxq.clear()
        self._notional_sum = 0.0

    @property
    def min_price(self) -> float | None:
        return self._minq[0][1] if self._minq else None

    @property
    def max_price(self) -> float | None:
        return self._maxq[0][1] if self._maxq else None

    @property
    def volume_usdc(self) -> float:
        return max(0.0, float(self._notional_sum))

    @property
    def trade_count(self) -> int:
        return len(self._rows)


def _token_side(token_order: int | None) -> str:
    if token_order == 0:
        return "yes"
    if token_order == 1:
        return "no"
    if token_order is None:
        return "unknown"
    return f"order_{token_order}"


def _market_type(neg_risk: int) -> str:
    return "neg_risk" if int(neg_risk or 0) else "binary"


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


def _percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    idx = max(0.0, min(1.0, q)) * (len(values) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return float(values[lo])
    frac = idx - lo
    return float(values[lo]) * (1.0 - frac) + float(values[hi]) * frac


def _generate_price_levels(step: float) -> tuple[float, ...]:
    if step <= 0 or step >= 1:
        raise ValueError("price step must be between 0 and 1")
    levels: list[float] = []
    current = step
    while current < (1.0 - 1e-9):
        rounded = round(current + 1e-9, 4)
        if rounded >= 1.0:
            break
        levels.append(rounded)
        current += step
    return tuple(levels)


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

        CREATE TABLE IF NOT EXISTS token_price_first_touch (
            market_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            price_level REAL NOT NULL,
            touch_direction TEXT NOT NULL,
            first_touch_ts INTEGER NOT NULL,
            touch_price REAL NOT NULL,
            touch_size REAL NOT NULL,
            prev_price REAL NOT NULL,
            lookback_min_price REAL NOT NULL,
            lookback_max_price REAL NOT NULL,
            lookback_volume_usdc REAL NOT NULL,
            lookback_trade_count INTEGER NOT NULL,
            PRIMARY KEY (token_id, price_level, touch_direction)
        );

        CREATE INDEX IF NOT EXISTS idx_first_touch_level_dir
        ON token_price_first_touch (price_level, touch_direction, first_touch_ts);

        CREATE TABLE IF NOT EXISTS token_price_resolution_events (
            market_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            price_level REAL NOT NULL,
            touch_direction TEXT NOT NULL,
            first_touch_ts INTEGER NOT NULL,
            touch_price REAL NOT NULL,
            touch_size REAL NOT NULL,
            prev_price REAL NOT NULL,
            lookback_min_price REAL NOT NULL,
            lookback_max_price REAL NOT NULL,
            lookback_volume_usdc REAL NOT NULL,
            lookback_trade_count INTEGER NOT NULL,
            max_price_after_touch REAL NOT NULL,
            min_price_after_touch REAL NOT NULL,
            close_price_after_touch REAL NOT NULL,
            closed_time INTEGER NOT NULL,
            time_to_close_sec INTEGER NOT NULL,
            minutes_to_resolution REAL NOT NULL,
            token_order INTEGER,
            is_winner INTEGER NOT NULL,
            category TEXT NOT NULL,
            neg_risk INTEGER NOT NULL,
            PRIMARY KEY (token_id, price_level, touch_direction)
        );

        CREATE INDEX IF NOT EXISTS idx_resolution_level_dir
        ON token_price_resolution_events (price_level, touch_direction, first_touch_ts);

        CREATE INDEX IF NOT EXISTS idx_resolution_token_dir
        ON token_price_resolution_events (token_id, touch_direction, price_level);

        CREATE TABLE IF NOT EXISTS price_resolution_heatmap (
            reaction_window_sec INTEGER NOT NULL,
            price_level REAL NOT NULL,
            touch_direction TEXT NOT NULL,
            time_to_close_bucket TEXT NOT NULL,
            market_type TEXT NOT NULL,
            token_side TEXT NOT NULL,
            n_tokens INTEGER NOT NULL,
            winner_count INTEGER NOT NULL,
            win_rate REAL NOT NULL,
            avg_touch_price REAL NOT NULL,
            avg_gross_edge REAL NOT NULL,
            avg_touch_volume_usdc REAL NOT NULL,
            median_minutes_to_resolution REAL NOT NULL,
            p10_minutes_to_resolution REAL NOT NULL,
            p90_minutes_to_resolution REAL NOT NULL,
            PRIMARY KEY (
                reaction_window_sec,
                price_level,
                touch_direction,
                time_to_close_bucket,
                market_type,
                token_side
            )
        );

        CREATE TABLE IF NOT EXISTS price_level_transition_matrix (
            reaction_window_sec INTEGER NOT NULL,
            from_price_level REAL NOT NULL,
            to_price_level REAL NOT NULL,
            touch_direction TEXT NOT NULL,
            time_to_close_bucket TEXT NOT NULL,
            market_type TEXT NOT NULL,
            token_side TEXT NOT NULL,
            n_tokens INTEGER NOT NULL,
            reached_count INTEGER NOT NULL,
            reach_rate REAL NOT NULL,
            avg_minutes_to_target REAL NOT NULL,
            median_minutes_to_target REAL NOT NULL,
            p10_minutes_to_target REAL NOT NULL,
            p90_minutes_to_target REAL NOT NULL,
            PRIMARY KEY (
                reaction_window_sec,
                from_price_level,
                to_price_level,
                touch_direction,
                time_to_close_bucket,
                market_type,
                token_side
            )
        );

        CREATE TABLE IF NOT EXISTS price_resolution_touch_diagnostics (
            reaction_window_sec INTEGER NOT NULL,
            price_level REAL NOT NULL,
            touch_direction TEXT NOT NULL,
            time_to_close_bucket TEXT NOT NULL,
            market_type TEXT NOT NULL,
            token_side TEXT NOT NULL,
            outcome_group TEXT NOT NULL,
            samples INTEGER NOT NULL,
            avg_touch_price REAL NOT NULL,
            avg_prev_price REAL NOT NULL,
            avg_lookback_min_price REAL NOT NULL,
            avg_lookback_max_price REAL NOT NULL,
            avg_lookback_volume_usdc REAL NOT NULL,
            avg_lookback_trade_count REAL NOT NULL,
            avg_price_runup REAL NOT NULL,
            avg_price_reversal_room REAL NOT NULL,
            PRIMARY KEY (
                reaction_window_sec,
                price_level,
                touch_direction,
                time_to_close_bucket,
                market_type,
                token_side,
                outcome_group
            )
        );

        CREATE TABLE IF NOT EXISTS price_level_regret_stats (
            reaction_window_sec INTEGER NOT NULL,
            price_level REAL NOT NULL,
            touch_direction TEXT NOT NULL,
            time_to_close_bucket TEXT NOT NULL,
            market_type TEXT NOT NULL,
            token_side TEXT NOT NULL,
            winner_samples INTEGER NOT NULL,
            avg_max_price_after_touch REAL NOT NULL,
            median_max_price_after_touch REAL NOT NULL,
            p10_max_price_after_touch REAL NOT NULL,
            p90_max_price_after_touch REAL NOT NULL,
            avg_extra_upside REAL NOT NULL,
            PRIMARY KEY (
                reaction_window_sec,
                price_level,
                touch_direction,
                time_to_close_bucket,
                market_type,
                token_side
            )
        );
        """
    )
    conn.commit()


def reset_output_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DELETE FROM research_meta;
        DELETE FROM token_price_first_touch;
        DELETE FROM token_price_resolution_events;
        DELETE FROM price_resolution_heatmap;
        DELETE FROM price_level_transition_matrix;
        DELETE FROM price_level_regret_stats;
        DELETE FROM price_resolution_touch_diagnostics;
        """
    )
    conn.commit()


def load_token_meta(dataset_db_path: Path) -> dict[str, TokenMeta]:
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
                COALESCE(m.category, 'unknown') AS category,
                CAST(COALESCE(m.neg_risk, 0) AS INTEGER) AS neg_risk,
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

    meta: dict[str, TokenMeta] = {}
    for row in rows:
        token_id = str(row["token_id"])
        meta[token_id] = TokenMeta(
            token_id=token_id,
            market_id=str(row["market_id"]),
            is_winner=int(row["is_winner"]),
            token_order=int(row["token_order"]) if row["token_order"] is not None else None,
            category=str(row["category"] or "unknown"),
            neg_risk=int(row["neg_risk"] or 0),
            closed_time=int(row["closed_time"] or 0),
        )
    return meta


def _crossed_up_levels(prev_price: float, current_price: float, price_levels: Sequence[float]) -> list[float]:
    return [level for level in price_levels if prev_price < level <= current_price]


def _crossed_down_levels(prev_price: float, current_price: float, price_levels: Sequence[float]) -> list[float]:
    return [level for level in price_levels if current_price <= level < prev_price]


def build_touch_events_for_token(
    *,
    meta: TokenMeta,
    trades: Sequence[TradePoint],
    price_levels: Sequence[float],
    direction_gap: float,
    direction_lookback_sec: int,
    touch_volume_lookback_sec: int,
) -> list[TouchEvent]:
    if len(trades) < 2:
        return []

    direction_window = RollingTradeWindow(direction_lookback_sec)
    touch_window = RollingTradeWindow(touch_volume_lookback_sec)
    recorded: set[tuple[float, str]] = set()
    raw_events: list[tuple[float, str, int, int, float, float, float, float, float, float, int]] = []

    first_trade = trades[0]
    direction_window.add(first_trade.timestamp, first_trade.price, first_trade.size)
    touch_window.add(first_trade.timestamp, first_trade.price, first_trade.size)

    for idx in range(1, len(trades)):
        trade = trades[idx]
        prev_trade = trades[idx - 1]

        direction_window.expire(trade.timestamp)
        touch_window.expire(trade.timestamp)

        lookback_min = direction_window.min_price
        lookback_max = direction_window.max_price
        lookback_volume = touch_window.volume_usdc + trade.price * trade.size
        lookback_trade_count = touch_window.trade_count + 1

        if lookback_min is not None and prev_trade.price < trade.price:
            for level in _crossed_up_levels(prev_trade.price, trade.price, price_levels):
                key = (level, "ascending")
                if key in recorded:
                    continue
                if lookback_min <= max(0.0, level - direction_gap):
                    recorded.add(key)
                    raw_events.append(
                        (
                            level,
                            "ascending",
                            idx,
                            trade.timestamp,
                            trade.price,
                            trade.size,
                            prev_trade.price,
                            float(lookback_min),
                            float(lookback_max if lookback_max is not None else prev_trade.price),
                            float(lookback_volume),
                            int(lookback_trade_count),
                        )
                    )

        if lookback_max is not None and prev_trade.price > trade.price:
            for level in _crossed_down_levels(prev_trade.price, trade.price, price_levels):
                key = (level, "descending")
                if key in recorded:
                    continue
                if lookback_max >= min(1.0, level + direction_gap):
                    recorded.add(key)
                    raw_events.append(
                        (
                            level,
                            "descending",
                            idx,
                            trade.timestamp,
                            trade.price,
                            trade.size,
                            prev_trade.price,
                            float(lookback_min if lookback_min is not None else prev_trade.price),
                            float(lookback_max),
                            float(lookback_volume),
                            int(lookback_trade_count),
                        )
                    )

        direction_window.add(trade.timestamp, trade.price, trade.size)
        touch_window.add(trade.timestamp, trade.price, trade.size)

    if not raw_events:
        return []

    suffix_max: list[float] = [0.0] * len(trades)
    suffix_min: list[float] = [0.0] * len(trades)
    suffix_max[-1] = float(trades[-1].price)
    suffix_min[-1] = float(trades[-1].price)
    for idx in range(len(trades) - 2, -1, -1):
        suffix_max[idx] = max(float(trades[idx].price), suffix_max[idx + 1])
        suffix_min[idx] = min(float(trades[idx].price), suffix_min[idx + 1])

    events: list[TouchEvent] = []
    for event in raw_events:
        (
            level,
            direction,
            trade_index,
            touch_ts,
            touch_price,
            touch_size,
            prev_price,
            lookback_min,
            lookback_max,
            lookback_volume,
            lookback_trade_count,
        ) = event
        time_to_close = int(meta.closed_time) - int(touch_ts)
        events.append(
            TouchEvent(
                market_id=meta.market_id,
                token_id=meta.token_id,
                price_level=float(level),
                touch_direction=direction,
                first_touch_ts=int(touch_ts),
                touch_price=float(touch_price),
                touch_size=float(touch_size),
                prev_price=float(prev_price),
                lookback_min_price=float(lookback_min),
                lookback_max_price=float(lookback_max),
                lookback_volume_usdc=float(lookback_volume),
                lookback_trade_count=int(lookback_trade_count),
                max_price_after_touch=float(suffix_max[trade_index]),
                min_price_after_touch=float(suffix_min[trade_index]),
                close_price_after_touch=float(trades[-1].price),
                closed_time=int(meta.closed_time),
                time_to_close_sec=int(time_to_close),
                minutes_to_resolution=max(0.0, float(time_to_close) / 60.0),
                token_order=meta.token_order,
                is_winner=meta.is_winner,
                category=meta.category,
                neg_risk=meta.neg_risk,
            )
        )
    return events


def _batched(iterable: Iterable[tuple], batch_size: int = 1000) -> Iterable[list[tuple]]:
    chunk: list[tuple] = []
    for row in iterable:
        chunk.append(row)
        if len(chunk) >= batch_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def write_touch_events(conn: sqlite3.Connection, events: Sequence[TouchEvent]) -> None:
    first_touch_rows = [
        (
            event.market_id,
            event.token_id,
            event.price_level,
            event.touch_direction,
            event.first_touch_ts,
            event.touch_price,
            event.touch_size,
            event.prev_price,
            event.lookback_min_price,
            event.lookback_max_price,
            event.lookback_volume_usdc,
            event.lookback_trade_count,
        )
        for event in events
    ]
    resolution_rows = [
        (
            event.market_id,
            event.token_id,
            event.price_level,
            event.touch_direction,
            event.first_touch_ts,
            event.touch_price,
            event.touch_size,
            event.prev_price,
            event.lookback_min_price,
            event.lookback_max_price,
            event.lookback_volume_usdc,
            event.lookback_trade_count,
            event.max_price_after_touch,
            event.min_price_after_touch,
            event.close_price_after_touch,
            event.closed_time,
            event.time_to_close_sec,
            event.minutes_to_resolution,
            event.token_order,
            event.is_winner,
            event.category,
            event.neg_risk,
        )
        for event in events
    ]

    for batch in _batched(first_touch_rows):
        conn.executemany(
            """
            INSERT OR REPLACE INTO token_price_first_touch(
                market_id, token_id, price_level, touch_direction, first_touch_ts,
                touch_price, touch_size, prev_price, lookback_min_price,
                lookback_max_price, lookback_volume_usdc, lookback_trade_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )

    for batch in _batched(resolution_rows):
        conn.executemany(
            """
            INSERT OR REPLACE INTO token_price_resolution_events(
                market_id, token_id, price_level, touch_direction, first_touch_ts,
                touch_price, touch_size, prev_price, lookback_min_price,
                lookback_max_price, lookback_volume_usdc, lookback_trade_count,
                max_price_after_touch, min_price_after_touch, close_price_after_touch,
                closed_time, time_to_close_sec, minutes_to_resolution, token_order,
                is_winner, category, neg_risk
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )


def build_event_tables(
    *,
    dataset_db_path: Path,
    tape_db_path: Path,
    output_db_path: Path,
    price_levels: Sequence[float],
    direction_gap: float,
    direction_lookback_sec: int,
    touch_volume_lookback_sec: int,
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
            ("price_levels", ",".join(f"{level:.4f}" for level in price_levels)),
            ("direction_gap", f"{direction_gap:.4f}"),
            ("direction_lookback_sec", str(direction_lookback_sec)),
            ("touch_volume_lookback_sec", str(touch_volume_lookback_sec)),
        ],
    )
    output_conn.commit()

    tape_conn = sqlite3.connect(tape_db_path)
    tape_conn.row_factory = sqlite3.Row

    stats = {
        "tokens_seen": 0,
        "eligible_tokens": 0,
        "tokens_with_events": 0,
        "events_written": 0,
        "trades_scanned": 0,
    }

    def flush_token(current_token_id: str | None, token_trades: list[TradePoint]) -> None:
        if current_token_id is None or not token_trades:
            return
        stats["tokens_seen"] += 1
        meta = token_meta.get(current_token_id)
        if meta is None:
            return
        stats["eligible_tokens"] += 1
        events = build_touch_events_for_token(
            meta=meta,
            trades=token_trades,
            price_levels=price_levels,
            direction_gap=direction_gap,
            direction_lookback_sec=direction_lookback_sec,
            touch_volume_lookback_sec=touch_volume_lookback_sec,
        )
        if not events:
            return
        stats["tokens_with_events"] += 1
        stats["events_written"] += len(events)
        write_touch_events(output_conn, events)

    current_token_id: str | None = None
    token_trades: list[TradePoint] = []

    query = (
        "SELECT source_file_id, seq, timestamp, market_id, token_id, price, size "
        "FROM tape ORDER BY token_id ASC, timestamp ASC, source_file_id ASC, seq ASC"
    )
    for row in tape_conn.execute(query):
        token_id = str(row["token_id"])
        if current_token_id is None:
            current_token_id = token_id
        elif token_id != current_token_id:
            flush_token(current_token_id, token_trades)
            token_trades = []
            current_token_id = token_id

        token_trades.append(
            TradePoint(
                timestamp=int(row["timestamp"]),
                market_id=str(row["market_id"]),
                token_id=token_id,
                price=float(row["price"]),
                size=float(row["size"]),
                source_file_id=int(row["source_file_id"]),
                seq=int(row["seq"]),
            )
        )
        stats["trades_scanned"] += 1

    flush_token(current_token_id, token_trades)
    output_conn.commit()
    tape_conn.close()
    output_conn.close()
    return stats


def aggregate_heatmap_rows(
    events: Sequence[sqlite3.Row],
    reaction_windows: Sequence[int],
    min_touch_volume_usdc: float,
) -> list[tuple]:
    buckets: dict[tuple, dict[str, list[float] | float | int]] = {}
    for row in events:
        time_to_close = int(row["time_to_close_sec"])
        touch_volume = float(row["lookback_volume_usdc"] or 0.0)
        touch_price = float(row["touch_price"] or 0.0)
        if time_to_close <= 0 or touch_volume < min_touch_volume_usdc:
            continue
        time_bucket = _time_to_close_bucket(time_to_close)
        token_side = _token_side(row["token_order"])
        market_type = _market_type(int(row["neg_risk"] or 0))
        dimensions = [
            ("all", "all"),
            (market_type, "all"),
            ("all", token_side),
            (market_type, token_side),
        ]
        for reaction_window in reaction_windows:
            if time_to_close < reaction_window:
                continue
            for dim_market_type, dim_token_side in dimensions:
                key = (
                    int(reaction_window),
                    float(row["price_level"]),
                    str(row["touch_direction"]),
                    time_bucket,
                    dim_market_type,
                    dim_token_side,
                )
                bucket = buckets.setdefault(
                    key,
                    {
                        "n_tokens": 0,
                        "winner_count": 0,
                        "touch_prices": [],
                        "touch_volumes": [],
                        "minutes": [],
                    },
                )
                bucket["n_tokens"] = int(bucket["n_tokens"]) + 1
                bucket["winner_count"] = int(bucket["winner_count"]) + int(row["is_winner"] or 0)
                cast_touch_prices = bucket["touch_prices"]
                cast_touch_volumes = bucket["touch_volumes"]
                cast_minutes = bucket["minutes"]
                assert isinstance(cast_touch_prices, list)
                assert isinstance(cast_touch_volumes, list)
                assert isinstance(cast_minutes, list)
                cast_touch_prices.append(touch_price)
                cast_touch_volumes.append(touch_volume)
                cast_minutes.append(float(row["minutes_to_resolution"] or 0.0))

    rows: list[tuple] = []
    for key, bucket in sorted(buckets.items()):
        touch_prices = sorted(float(v) for v in bucket["touch_prices"])
        touch_volumes = sorted(float(v) for v in bucket["touch_volumes"])
        minutes = sorted(float(v) for v in bucket["minutes"])
        n_tokens = int(bucket["n_tokens"])
        winner_count = int(bucket["winner_count"])
        avg_touch_price = sum(touch_prices) / n_tokens if n_tokens else 0.0
        avg_touch_volume = sum(touch_volumes) / n_tokens if n_tokens else 0.0
        win_rate = float(winner_count) / n_tokens if n_tokens else 0.0
        rows.append(
            (
                *key,
                n_tokens,
                winner_count,
                win_rate,
                float(avg_touch_price),
                float(win_rate - avg_touch_price),
                float(avg_touch_volume),
                _percentile(minutes, 0.50),
                _percentile(minutes, 0.10),
                _percentile(minutes, 0.90),
            )
        )
    return rows


def aggregate_transition_rows(
    events: Sequence[sqlite3.Row],
    reaction_windows: Sequence[int],
    min_touch_volume_usdc: float,
    price_levels: Sequence[float],
) -> list[tuple]:
    events_by_token_direction: dict[tuple[str, str], list[sqlite3.Row]] = defaultdict(list)
    for row in events:
        if int(row["time_to_close_sec"] or 0) <= 0:
            continue
        events_by_token_direction[(str(row["token_id"]), str(row["touch_direction"]))].append(row)

    buckets: dict[tuple, dict[str, list[float] | int]] = {}
    for (_token_id, direction), rows in events_by_token_direction.items():
        ordered = sorted(rows, key=lambda r: float(r["price_level"]))
        if direction == "descending":
            ordered = sorted(rows, key=lambda r: float(r["price_level"]), reverse=True)
        touched_levels = {float(row["price_level"]): row for row in ordered}
        token_side = _token_side(ordered[0]["token_order"])
        market_type = _market_type(int(ordered[0]["neg_risk"] or 0))
        dimensions = [
            ("all", "all"),
            (market_type, "all"),
            ("all", token_side),
            (market_type, token_side),
        ]

        for reaction_window in reaction_windows:
            for from_row in ordered:
                time_to_close = int(from_row["time_to_close_sec"] or 0)
                if time_to_close < reaction_window:
                    continue
                if float(from_row["lookback_volume_usdc"] or 0.0) < min_touch_volume_usdc:
                    continue
                from_level = float(from_row["price_level"])
                if direction == "ascending":
                    candidate_levels = [level for level in price_levels if level > from_level]
                else:
                    candidate_levels = [level for level in price_levels if level < from_level]
                if not candidate_levels:
                    continue
                time_bucket = _time_to_close_bucket(time_to_close)
                for candidate_level in candidate_levels:
                    target_row = touched_levels.get(float(candidate_level))
                    reached = target_row is not None
                    minutes_to_target = 0.0
                    if reached:
                        minutes_to_target = max(
                            0.0,
                            (int(target_row["first_touch_ts"]) - int(from_row["first_touch_ts"])) / 60.0,
                        )
                    for dim_market_type, dim_token_side in dimensions:
                        key = (
                            int(reaction_window),
                            from_level,
                            float(candidate_level),
                            direction,
                            time_bucket,
                            dim_market_type,
                            dim_token_side,
                        )
                        bucket = buckets.setdefault(key, {"n_tokens": 0, "reached_count": 0, "minutes_to_target": []})
                        bucket["n_tokens"] += 1
                        bucket["reached_count"] += 1 if reached else 0
                        if reached:
                            cast_minutes = bucket["minutes_to_target"]
                            assert isinstance(cast_minutes, list)
                            cast_minutes.append(minutes_to_target)

    rows: list[tuple] = []
    for key, bucket in sorted(buckets.items()):
        n_tokens = int(bucket["n_tokens"])
        reached_count = int(bucket["reached_count"])
        reached_minutes = sorted(float(v) for v in bucket["minutes_to_target"])
        rows.append(
            (
                *key,
                n_tokens,
                reached_count,
                float(reached_count) / n_tokens if n_tokens else 0.0,
                sum(reached_minutes) / reached_count if reached_count else 0.0,
                _percentile(reached_minutes, 0.50),
                _percentile(reached_minutes, 0.10),
                _percentile(reached_minutes, 0.90),
            )
        )
    return rows


def aggregate_regret_rows(
    events: Sequence[sqlite3.Row],
    reaction_windows: Sequence[int],
    min_touch_volume_usdc: float,
) -> list[tuple]:
    buckets: dict[tuple, list[float]] = defaultdict(list)
    for row in events:
        if int(row["is_winner"] or 0) != 1:
            continue
        time_to_close = int(row["time_to_close_sec"] or 0)
        touch_volume = float(row["lookback_volume_usdc"] or 0.0)
        if time_to_close <= 0 or touch_volume < min_touch_volume_usdc:
            continue
        price_level = float(row["price_level"])
        max_price_after_touch = float(row["max_price_after_touch"])
        token_side = _token_side(row["token_order"])
        market_type = _market_type(int(row["neg_risk"] or 0))
        time_bucket = _time_to_close_bucket(time_to_close)
        dimensions = [
            ("all", "all"),
            (market_type, "all"),
            ("all", token_side),
            (market_type, token_side),
        ]
        for reaction_window in reaction_windows:
            if time_to_close < reaction_window:
                continue
            for dim_market_type, dim_token_side in dimensions:
                key = (
                    int(reaction_window),
                    price_level,
                    str(row["touch_direction"]),
                    time_bucket,
                    dim_market_type,
                    dim_token_side,
                )
                buckets[key].append(max_price_after_touch)

    rows: list[tuple] = []
    for key, values in sorted(buckets.items()):
        ordered = sorted(float(v) for v in values)
        price_level = float(key[1])
        rows.append(
            (
                *key,
                len(ordered),
                sum(ordered) / len(ordered),
                _percentile(ordered, 0.50),
                _percentile(ordered, 0.10),
                _percentile(ordered, 0.90),
                sum(v - price_level for v in ordered) / len(ordered),
            )
        )
    return rows


def aggregate_touch_diagnostics_rows(
    events: Sequence[sqlite3.Row],
    reaction_windows: Sequence[int],
    min_touch_volume_usdc: float,
) -> list[tuple]:
    buckets: dict[tuple, dict[str, list[float] | int]] = {}
    for row in events:
        time_to_close = int(row["time_to_close_sec"] or 0)
        touch_volume = float(row["lookback_volume_usdc"] or 0.0)
        if time_to_close <= 0 or touch_volume < min_touch_volume_usdc:
            continue
        price_runup = float(row["touch_price"] or 0.0) - float(row["lookback_min_price"] or 0.0)
        price_reversal_room = float(row["lookback_max_price"] or 0.0) - float(row["touch_price"] or 0.0)
        token_side = _token_side(row["token_order"])
        market_type = _market_type(int(row["neg_risk"] or 0))
        time_bucket = _time_to_close_bucket(time_to_close)
        outcome_group = "winner" if int(row["is_winner"] or 0) == 1 else "loser"
        dimensions = [
            ("all", "all"),
            (market_type, "all"),
            ("all", token_side),
            (market_type, token_side),
        ]
        for reaction_window in reaction_windows:
            if time_to_close < reaction_window:
                continue
            for dim_market_type, dim_token_side in dimensions:
                key = (
                    int(reaction_window),
                    float(row["price_level"]),
                    str(row["touch_direction"]),
                    time_bucket,
                    dim_market_type,
                    dim_token_side,
                    outcome_group,
                )
                bucket = buckets.setdefault(
                    key,
                    {
                        "samples": 0,
                        "touch_prices": [],
                        "prev_prices": [],
                        "lookback_min_prices": [],
                        "lookback_max_prices": [],
                        "lookback_volumes": [],
                        "lookback_trade_counts": [],
                        "price_runups": [],
                        "price_reversal_rooms": [],
                    },
                )
                bucket["samples"] = int(bucket["samples"]) + 1
                for field, value in (
                    ("touch_prices", float(row["touch_price"] or 0.0)),
                    ("prev_prices", float(row["prev_price"] or 0.0)),
                    ("lookback_min_prices", float(row["lookback_min_price"] or 0.0)),
                    ("lookback_max_prices", float(row["lookback_max_price"] or 0.0)),
                    ("lookback_volumes", touch_volume),
                    ("lookback_trade_counts", float(row["lookback_trade_count"] or 0.0)),
                    ("price_runups", price_runup),
                    ("price_reversal_rooms", price_reversal_room),
                ):
                    cast_values = bucket[field]
                    assert isinstance(cast_values, list)
                    cast_values.append(value)

    rows: list[tuple] = []
    for key, bucket in sorted(buckets.items()):
        samples = int(bucket["samples"])

        def _avg(name: str) -> float:
            values = bucket[name]
            assert isinstance(values, list)
            return sum(float(v) for v in values) / samples if samples else 0.0

        rows.append(
            (
                *key,
                samples,
                _avg("touch_prices"),
                _avg("prev_prices"),
                _avg("lookback_min_prices"),
                _avg("lookback_max_prices"),
                _avg("lookback_volumes"),
                _avg("lookback_trade_counts"),
                _avg("price_runups"),
                _avg("price_reversal_rooms"),
            )
        )
    return rows


def build_aggregate_tables(
    *,
    output_db_path: Path,
    reaction_windows: Sequence[int],
    min_touch_volume_usdc: float,
    price_levels: Sequence[float],
) -> dict[str, int]:
    conn = sqlite3.connect(output_db_path)
    conn.row_factory = sqlite3.Row
    try:
        events = conn.execute(
            "SELECT * FROM token_price_resolution_events ORDER BY token_id, touch_direction, price_level"
        ).fetchall()

        heatmap_rows = aggregate_heatmap_rows(events, reaction_windows, min_touch_volume_usdc)
        conn.executemany(
            """
            INSERT INTO price_resolution_heatmap(
                reaction_window_sec, price_level, touch_direction, time_to_close_bucket,
                market_type, token_side, n_tokens, winner_count, win_rate,
                avg_touch_price, avg_gross_edge, avg_touch_volume_usdc,
                median_minutes_to_resolution, p10_minutes_to_resolution,
                p90_minutes_to_resolution
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            heatmap_rows,
        )

        transition_rows = aggregate_transition_rows(events, reaction_windows, min_touch_volume_usdc, price_levels)
        conn.executemany(
            """
            INSERT INTO price_level_transition_matrix(
                reaction_window_sec, from_price_level, to_price_level, touch_direction,
                time_to_close_bucket, market_type, token_side, n_tokens, reached_count,
                reach_rate, avg_minutes_to_target, median_minutes_to_target,
                p10_minutes_to_target, p90_minutes_to_target
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            transition_rows,
        )

        regret_rows = aggregate_regret_rows(events, reaction_windows, min_touch_volume_usdc)
        conn.executemany(
            """
            INSERT INTO price_level_regret_stats(
                reaction_window_sec, price_level, touch_direction, time_to_close_bucket,
                market_type, token_side, winner_samples, avg_max_price_after_touch,
                median_max_price_after_touch, p10_max_price_after_touch,
                p90_max_price_after_touch, avg_extra_upside
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            regret_rows,
        )

        diagnostic_rows = aggregate_touch_diagnostics_rows(events, reaction_windows, min_touch_volume_usdc)
        conn.executemany(
            """
            INSERT INTO price_resolution_touch_diagnostics(
                reaction_window_sec, price_level, touch_direction, time_to_close_bucket,
                market_type, token_side, outcome_group, samples, avg_touch_price,
                avg_prev_price, avg_lookback_min_price, avg_lookback_max_price,
                avg_lookback_volume_usdc, avg_lookback_trade_count,
                avg_price_runup, avg_price_reversal_room
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            diagnostic_rows,
        )

        conn.executemany(
            "INSERT INTO research_meta(key, value) VALUES (?, ?)",
            [
                ("reaction_windows_sec", ",".join(str(v) for v in reaction_windows)),
                ("min_touch_volume_usdc", f"{min_touch_volume_usdc:.4f}"),
                ("heatmap_rows", str(len(heatmap_rows))),
                ("transition_rows", str(len(transition_rows))),
                ("regret_rows", str(len(regret_rows))),
                ("diagnostic_rows", str(len(diagnostic_rows))),
            ],
        )
        conn.commit()
        return {
            "resolution_events": len(events),
            "heatmap_rows": len(heatmap_rows),
            "transition_rows": len(transition_rows),
            "regret_rows": len(regret_rows),
            "diagnostic_rows": len(diagnostic_rows),
        }
    finally:
        conn.close()


def build_price_resolution_research(
    *,
    dataset_db_path: Path,
    tape_db_path: Path,
    output_db_path: Path,
    price_step: float = DEFAULT_PRICE_STEP,
    reaction_windows: Sequence[int] = DEFAULT_REACTION_WINDOWS,
    direction_gap: float = DEFAULT_DIRECTION_GAP,
    direction_lookback_sec: int = DEFAULT_DIRECTION_LOOKBACK_SEC,
    touch_volume_lookback_sec: int = DEFAULT_TOUCH_VOLUME_LOOKBACK_SEC,
    min_touch_volume_usdc: float = 0.0,
) -> dict[str, int]:
    ensure_runtime_dirs()
    dataset_db_path = Path(dataset_db_path)
    tape_db_path = Path(tape_db_path)
    output_db_path = Path(output_db_path)
    if not dataset_db_path.exists():
        raise FileNotFoundError(f"dataset DB not found: {dataset_db_path}")
    if not tape_db_path.exists():
        raise FileNotFoundError(f"tape DB not found: {tape_db_path}")
    output_db_path.parent.mkdir(parents=True, exist_ok=True)
    if output_db_path.exists():
        output_db_path.unlink()

    price_levels = _generate_price_levels(price_step)
    build_stats = build_event_tables(
        dataset_db_path=dataset_db_path,
        tape_db_path=tape_db_path,
        output_db_path=output_db_path,
        price_levels=price_levels,
        direction_gap=float(direction_gap),
        direction_lookback_sec=int(direction_lookback_sec),
        touch_volume_lookback_sec=int(touch_volume_lookback_sec),
    )
    aggregate_stats = build_aggregate_tables(
        output_db_path=output_db_path,
        reaction_windows=tuple(sorted({int(v) for v in reaction_windows if int(v) > 0})),
        min_touch_volume_usdc=float(min_touch_volume_usdc),
        price_levels=price_levels,
    )
    return {**build_stats, **aggregate_stats}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Build historical price-level research tables from raw historical tape. "
            "Creates first-touch events, resolution heatmaps, transition matrices, and regret stats."
        )
    )
    ap.add_argument("--db", default=str(DB_PATH), help="Path to polymarket_dataset.db")
    ap.add_argument("--tape-db", default=str(DEFAULT_TAPE_DB_PATH), help="Path to historical_tape.db")
    ap.add_argument(
        "--output-db",
        default=str(DEFAULT_OUTPUT_DB_PATH),
        help="Path to output sqlite DB with materialized research tables",
    )
    ap.add_argument("--price-step", type=float, default=DEFAULT_PRICE_STEP, help="Price grid step, e.g. 0.05")
    ap.add_argument(
        "--reaction-window",
        action="append",
        type=int,
        default=None,
        help="Reaction window in seconds. Repeatable. Default: 60 and 300",
    )
    ap.add_argument(
        "--direction-gap",
        type=float,
        default=DEFAULT_DIRECTION_GAP,
        help="Required prior price gap for ascending/descending classification",
    )
    ap.add_argument(
        "--direction-lookback-sec",
        type=int,
        default=DEFAULT_DIRECTION_LOOKBACK_SEC,
        help="Lookback window for direction classification",
    )
    ap.add_argument(
        "--touch-volume-lookback-sec",
        type=int,
        default=DEFAULT_TOUCH_VOLUME_LOOKBACK_SEC,
        help="Trailing lookback window used to estimate local touch volume",
    )
    ap.add_argument(
        "--min-touch-volume-usdc",
        type=float,
        default=0.0,
        help="Minimum trailing touch notional required for aggregate tables",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    reaction_windows = tuple(args.reaction_window or DEFAULT_REACTION_WINDOWS)
    stats = build_price_resolution_research(
        dataset_db_path=Path(args.db),
        tape_db_path=Path(args.tape_db),
        output_db_path=Path(args.output_db),
        price_step=float(args.price_step),
        reaction_windows=reaction_windows,
        direction_gap=float(args.direction_gap),
        direction_lookback_sec=int(args.direction_lookback_sec),
        touch_volume_lookback_sec=int(args.touch_volume_lookback_sec),
        min_touch_volume_usdc=float(args.min_touch_volume_usdc),
    )
    logger.info("Research DB written to %s", args.output_db)
    for key in sorted(stats):
        logger.info("%s=%s", key, stats[key])


if __name__ == "__main__":
    main()
