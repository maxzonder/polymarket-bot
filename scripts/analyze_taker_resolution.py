#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

if __package__:
    from .analyze_price_resolution import (
        DEFAULT_DIRECTION_GAP,
        DEFAULT_DIRECTION_LOOKBACK_SEC,
        DEFAULT_PRICE_STEP,
        DEFAULT_TOUCH_VOLUME_LOOKBACK_SEC,
        TradePoint,
        _generate_price_levels,
        _market_type,
        _time_to_close_bucket,
        _token_side,
        build_touch_events_for_token,
    )
    from replay.tape_feed import DEFAULT_TAPE_DB_PATH
    from utils.logger import setup_logger
    from utils.paths import DATA_DIR, DB_PATH, ensure_runtime_dirs
else:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from analyze_price_resolution import (  # type: ignore
        DEFAULT_DIRECTION_GAP,
        DEFAULT_DIRECTION_LOOKBACK_SEC,
        DEFAULT_PRICE_STEP,
        DEFAULT_TOUCH_VOLUME_LOOKBACK_SEC,
        TradePoint,
        _generate_price_levels,
        _market_type,
        _time_to_close_bucket,
        _token_side,
        build_touch_events_for_token,
    )
    from replay.tape_feed import DEFAULT_TAPE_DB_PATH
    from utils.logger import setup_logger
    from utils.paths import DATA_DIR, DB_PATH, ensure_runtime_dirs


logger = setup_logger("taker_resolution_research")
DEFAULT_OUTPUT_DB_PATH = DATA_DIR / "taker_resolution_research.db"
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
    is_winner: int
    token_order: int | None
    category: str
    neg_risk: int
    fees_enabled: int
    duration_hours: float
    closed_time: int


@dataclass(frozen=True)
class TakerEvent:
    market_id: str
    token_id: str
    price_level: float
    touch_direction: str
    first_touch_ts: int
    entry_price: float
    entry_size: float
    entry_fee_rate: float
    entry_fee_per_share: float
    gross_pnl_per_share: float
    net_pnl_per_share: float
    gross_pnl_per_order: float
    net_pnl_per_order: float
    time_to_close_sec: int
    time_to_close_bucket: str
    duration_hours: float
    minutes_to_resolution: float
    token_order: int | None
    is_winner: int
    category: str
    neg_risk: int
    fees_enabled: int


def _normalize_category(category: str) -> str:
    value = (category or "").strip().lower()
    if value in {"science & tech", "science and tech"}:
        return "tech"
    if value in {"other/general", "other-general"}:
        return "other / general"
    return value


def _fee_rate_for_category(category: str, *, fees_enabled: int, default_fee_rate: float) -> float:
    if int(fees_enabled or 0) == 0:
        return 0.0
    return float(CATEGORY_FEE_RATE_V2.get(_normalize_category(category), default_fee_rate))


def _taker_entry_fee_per_share(price: float, fee_rate: float, *, fees_enabled: int) -> float:
    if int(fees_enabled or 0) == 0:
        return 0.0
    p = max(0.0, min(1.0, float(price)))
    return float(fee_rate) * p * (1.0 - p)


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

        CREATE TABLE IF NOT EXISTS taker_resolution_events (
            market_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            price_level REAL NOT NULL,
            touch_direction TEXT NOT NULL,
            first_touch_ts INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            entry_size REAL NOT NULL,
            entry_fee_rate REAL NOT NULL,
            entry_fee_per_share REAL NOT NULL,
            gross_pnl_per_share REAL NOT NULL,
            net_pnl_per_share REAL NOT NULL,
            gross_pnl_per_order REAL NOT NULL,
            net_pnl_per_order REAL NOT NULL,
            time_to_close_sec INTEGER NOT NULL,
            time_to_close_bucket TEXT NOT NULL,
            duration_hours REAL NOT NULL,
            minutes_to_resolution REAL NOT NULL,
            token_order INTEGER,
            is_winner INTEGER NOT NULL,
            category TEXT NOT NULL,
            neg_risk INTEGER NOT NULL,
            fees_enabled INTEGER NOT NULL,
            PRIMARY KEY (token_id, price_level, touch_direction)
        );

        CREATE INDEX IF NOT EXISTS idx_taker_resolution_level_dir
        ON taker_resolution_events (price_level, touch_direction, time_to_close_bucket);

        CREATE TABLE IF NOT EXISTS taker_resolution_summary (
            price_level REAL NOT NULL,
            touch_direction TEXT NOT NULL,
            time_to_close_bucket TEXT NOT NULL,
            market_type TEXT NOT NULL,
            token_side TEXT NOT NULL,
            category TEXT NOT NULL,
            fees_enabled INTEGER NOT NULL,
            n_tokens INTEGER NOT NULL,
            winner_count INTEGER NOT NULL,
            win_rate REAL NOT NULL,
            avg_entry_price REAL NOT NULL,
            avg_entry_fee_per_share REAL NOT NULL,
            avg_gross_edge REAL NOT NULL,
            avg_net_edge REAL NOT NULL,
            avg_gross_pnl_per_share REAL NOT NULL,
            avg_net_pnl_per_share REAL NOT NULL,
            avg_gross_pnl_per_order REAL NOT NULL,
            avg_net_pnl_per_order REAL NOT NULL,
            avg_minutes_to_resolution REAL NOT NULL,
            PRIMARY KEY (
                price_level,
                touch_direction,
                time_to_close_bucket,
                market_type,
                token_side,
                category,
                fees_enabled
            )
        );
        """
    )
    conn.commit()


def reset_output_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DELETE FROM research_meta;
        DELETE FROM taker_resolution_events;
        DELETE FROM taker_resolution_summary;
        """
    )
    conn.commit()


def load_token_meta(
    dataset_db_path: Path,
    *,
    min_duration_hours: float = 0.0,
    max_duration_hours: float = 0.0,
) -> dict[str, TokenMeta]:
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
                CAST(COALESCE(m.fees_enabled, 0) AS INTEGER) AS fees_enabled,
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

    meta: dict[str, TokenMeta] = {}
    for row in rows:
        duration_hours = float(row["duration_hours"] or 0.0)
        if min_duration_hours > 0 and duration_hours + 1e-9 < float(min_duration_hours):
            continue
        if max_duration_hours > 0 and duration_hours - 1e-9 > float(max_duration_hours):
            continue
        token_id = str(row["token_id"])
        meta[token_id] = TokenMeta(
            token_id=token_id,
            market_id=str(row["market_id"]),
            is_winner=int(row["is_winner"]),
            token_order=int(row["token_order"]) if row["token_order"] is not None else None,
            category=str(row["category"] or "unknown"),
            neg_risk=int(row["neg_risk"] or 0),
            fees_enabled=int(row["fees_enabled"] or 0),
            duration_hours=duration_hours,
            closed_time=int(row["closed_time"] or 0),
        )
    return meta


def _batched(iterable: Iterable[tuple], batch_size: int = 1000) -> Iterable[list[tuple]]:
    chunk: list[tuple] = []
    for row in iterable:
        chunk.append(row)
        if len(chunk) >= batch_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def write_events(conn: sqlite3.Connection, events: Sequence[TakerEvent]) -> None:
    rows = [
        (
            event.market_id,
            event.token_id,
            event.price_level,
            event.touch_direction,
            event.first_touch_ts,
            event.entry_price,
            event.entry_size,
            event.entry_fee_rate,
            event.entry_fee_per_share,
            event.gross_pnl_per_share,
            event.net_pnl_per_share,
            event.gross_pnl_per_order,
            event.net_pnl_per_order,
            event.time_to_close_sec,
            event.time_to_close_bucket,
            event.duration_hours,
            event.minutes_to_resolution,
            event.token_order,
            event.is_winner,
            event.category,
            event.neg_risk,
            event.fees_enabled,
        )
        for event in events
    ]
    for batch in _batched(rows):
        conn.executemany(
            """
            INSERT OR REPLACE INTO taker_resolution_events(
                market_id, token_id, price_level, touch_direction, first_touch_ts,
                entry_price, entry_size, entry_fee_rate, entry_fee_per_share,
                gross_pnl_per_share, net_pnl_per_share, gross_pnl_per_order,
                net_pnl_per_order, time_to_close_sec, time_to_close_bucket,
                duration_hours, minutes_to_resolution, token_order, is_winner,
                category, neg_risk, fees_enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )


def build_taker_events_for_token(
    *,
    meta: TokenMeta,
    trades: Sequence[TradePoint],
    price_levels: Sequence[float],
    direction_gap: float,
    direction_lookback_sec: int,
    touch_volume_lookback_sec: int,
    order_size_shares: float,
    default_fee_rate: float,
) -> list[TakerEvent]:
    touch_events = build_touch_events_for_token(
        meta=meta,
        trades=trades,
        price_levels=price_levels,
        direction_gap=direction_gap,
        direction_lookback_sec=direction_lookback_sec,
        touch_volume_lookback_sec=touch_volume_lookback_sec,
    )
    fee_rate = _fee_rate_for_category(meta.category, fees_enabled=meta.fees_enabled, default_fee_rate=default_fee_rate)
    rows: list[TakerEvent] = []
    for event in touch_events:
        entry_price = float(event.touch_price)
        entry_fee_per_share = _taker_entry_fee_per_share(entry_price, fee_rate, fees_enabled=meta.fees_enabled)
        gross_pnl_per_share = (1.0 - entry_price) if meta.is_winner else (-entry_price)
        net_pnl_per_share = gross_pnl_per_share - entry_fee_per_share
        rows.append(
            TakerEvent(
                market_id=meta.market_id,
                token_id=meta.token_id,
                price_level=float(event.price_level),
                touch_direction=str(event.touch_direction),
                first_touch_ts=int(event.first_touch_ts),
                entry_price=entry_price,
                entry_size=float(event.touch_size),
                entry_fee_rate=float(fee_rate),
                entry_fee_per_share=float(entry_fee_per_share),
                gross_pnl_per_share=float(gross_pnl_per_share),
                net_pnl_per_share=float(net_pnl_per_share),
                gross_pnl_per_order=float(gross_pnl_per_share * order_size_shares),
                net_pnl_per_order=float(net_pnl_per_share * order_size_shares),
                time_to_close_sec=int(event.time_to_close_sec),
                time_to_close_bucket=_time_to_close_bucket(int(event.time_to_close_sec)),
                duration_hours=float(meta.duration_hours),
                minutes_to_resolution=float(event.minutes_to_resolution),
                token_order=meta.token_order,
                is_winner=int(meta.is_winner),
                category=str(meta.category),
                neg_risk=int(meta.neg_risk),
                fees_enabled=int(meta.fees_enabled),
            )
        )
    return rows


def build_event_tables(
    *,
    dataset_db_path: Path,
    tape_db_path: Path,
    output_db_path: Path,
    price_levels: Sequence[float],
    direction_gap: float,
    direction_lookback_sec: int,
    touch_volume_lookback_sec: int,
    order_size_shares: float,
    min_duration_hours: float,
    max_duration_hours: float,
    default_fee_rate: float,
) -> dict[str, int]:
    token_meta = load_token_meta(
        dataset_db_path,
        min_duration_hours=min_duration_hours,
        max_duration_hours=max_duration_hours,
    )
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
            ("order_size_shares", f"{order_size_shares:.4f}"),
            ("min_duration_hours", f"{min_duration_hours:.6f}"),
            ("max_duration_hours", f"{max_duration_hours:.6f}"),
            ("default_fee_rate", f"{default_fee_rate:.6f}"),
            ("fee_model", "Fee Structure V2 category map + fees_enabled"),
            ("exit_fee_model", "hold_to_resolution_no_exit_fee"),
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
        events = build_taker_events_for_token(
            meta=meta,
            trades=token_trades,
            price_levels=price_levels,
            direction_gap=direction_gap,
            direction_lookback_sec=direction_lookback_sec,
            touch_volume_lookback_sec=touch_volume_lookback_sec,
            order_size_shares=order_size_shares,
            default_fee_rate=default_fee_rate,
        )
        if not events:
            return
        stats["tokens_with_events"] += 1
        stats["events_written"] += len(events)
        write_events(output_conn, events)

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


def build_summary(output_db_path: Path, *, min_touch_volume_usdc: float) -> dict[str, int]:
    conn = sqlite3.connect(output_db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("DELETE FROM taker_resolution_summary")
        events = conn.execute(
            "SELECT * FROM taker_resolution_events ORDER BY token_id, touch_direction, price_level"
        ).fetchall()
        buckets: dict[tuple, dict[str, float | int]] = {}
        for row in events:
            time_to_close = int(row["time_to_close_sec"] or 0)
            if time_to_close <= 0:
                continue
            key = (
                float(row["price_level"]),
                str(row["touch_direction"]),
                str(row["time_to_close_bucket"]),
                _market_type(int(row["neg_risk"] or 0)),
                _token_side(row["token_order"]),
                str(row["category"]),
                int(row["fees_enabled"] or 0),
            )
            bucket = buckets.setdefault(
                key,
                {
                    "n_tokens": 0,
                    "winner_count": 0,
                    "sum_entry_price": 0.0,
                    "sum_entry_fee_per_share": 0.0,
                    "sum_gross_pnl_per_share": 0.0,
                    "sum_net_pnl_per_share": 0.0,
                    "sum_gross_pnl_per_order": 0.0,
                    "sum_net_pnl_per_order": 0.0,
                    "sum_minutes_to_resolution": 0.0,
                },
            )
            bucket["n_tokens"] = int(bucket["n_tokens"]) + 1
            bucket["winner_count"] = int(bucket["winner_count"]) + int(row["is_winner"] or 0)
            bucket["sum_entry_price"] = float(bucket["sum_entry_price"]) + float(row["entry_price"] or 0.0)
            bucket["sum_entry_fee_per_share"] = float(bucket["sum_entry_fee_per_share"]) + float(row["entry_fee_per_share"] or 0.0)
            bucket["sum_gross_pnl_per_share"] = float(bucket["sum_gross_pnl_per_share"]) + float(row["gross_pnl_per_share"] or 0.0)
            bucket["sum_net_pnl_per_share"] = float(bucket["sum_net_pnl_per_share"]) + float(row["net_pnl_per_share"] or 0.0)
            bucket["sum_gross_pnl_per_order"] = float(bucket["sum_gross_pnl_per_order"]) + float(row["gross_pnl_per_order"] or 0.0)
            bucket["sum_net_pnl_per_order"] = float(bucket["sum_net_pnl_per_order"]) + float(row["net_pnl_per_order"] or 0.0)
            bucket["sum_minutes_to_resolution"] = float(bucket["sum_minutes_to_resolution"]) + float(row["minutes_to_resolution"] or 0.0)

        rows: list[tuple] = []
        for key, bucket in sorted(buckets.items()):
            n_tokens = int(bucket["n_tokens"])
            if n_tokens <= 0:
                continue
            winner_count = int(bucket["winner_count"])
            avg_entry_price = float(bucket["sum_entry_price"]) / n_tokens
            avg_entry_fee_per_share = float(bucket["sum_entry_fee_per_share"]) / n_tokens
            win_rate = float(winner_count) / n_tokens
            rows.append(
                (
                    *key,
                    n_tokens,
                    winner_count,
                    win_rate,
                    avg_entry_price,
                    avg_entry_fee_per_share,
                    win_rate - avg_entry_price,
                    win_rate - avg_entry_price - avg_entry_fee_per_share,
                    float(bucket["sum_gross_pnl_per_share"]) / n_tokens,
                    float(bucket["sum_net_pnl_per_share"]) / n_tokens,
                    float(bucket["sum_gross_pnl_per_order"]) / n_tokens,
                    float(bucket["sum_net_pnl_per_order"]) / n_tokens,
                    float(bucket["sum_minutes_to_resolution"]) / n_tokens,
                )
            )

        conn.executemany(
            """
            INSERT INTO taker_resolution_summary(
                price_level, touch_direction, time_to_close_bucket, market_type,
                token_side, category, fees_enabled, n_tokens, winner_count, win_rate,
                avg_entry_price, avg_entry_fee_per_share, avg_gross_edge, avg_net_edge,
                avg_gross_pnl_per_share, avg_net_pnl_per_share,
                avg_gross_pnl_per_order, avg_net_pnl_per_order, avg_minutes_to_resolution
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.execute(
            "INSERT INTO research_meta(key, value) VALUES (?, ?)",
            ("summary_rows", str(len(rows))),
        )
        conn.commit()
        return {
            "summary_rows": len(rows),
            "resolution_events": len(events),
        }
    finally:
        conn.close()


def build_taker_resolution_research(
    *,
    dataset_db_path: Path,
    tape_db_path: Path,
    output_db_path: Path,
    price_step: float = DEFAULT_PRICE_STEP,
    direction_gap: float = DEFAULT_DIRECTION_GAP,
    direction_lookback_sec: int = DEFAULT_DIRECTION_LOOKBACK_SEC,
    touch_volume_lookback_sec: int = DEFAULT_TOUCH_VOLUME_LOOKBACK_SEC,
    order_size_shares: float = 100.0,
    min_duration_hours: float = 0.0,
    max_duration_hours: float = 0.0,
    default_fee_rate: float = 0.05,
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
    event_stats = build_event_tables(
        dataset_db_path=dataset_db_path,
        tape_db_path=tape_db_path,
        output_db_path=output_db_path,
        price_levels=price_levels,
        direction_gap=float(direction_gap),
        direction_lookback_sec=int(direction_lookback_sec),
        touch_volume_lookback_sec=int(touch_volume_lookback_sec),
        order_size_shares=float(order_size_shares),
        min_duration_hours=float(min_duration_hours),
        max_duration_hours=float(max_duration_hours),
        default_fee_rate=float(default_fee_rate),
    )
    summary_stats = build_summary(output_db_path, min_touch_volume_usdc=float(min_touch_volume_usdc))
    return {**event_stats, **summary_stats}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Build taker hold-to-resolution research tables from historical tape. "
            "Entry happens at first touch price, with Fee Structure V2 entry fee applied on fee-enabled markets."
        )
    )
    ap.add_argument("--db", default=str(DB_PATH), help="Path to polymarket_dataset.db")
    ap.add_argument("--tape-db", default=str(DEFAULT_TAPE_DB_PATH), help="Path to historical_tape.db")
    ap.add_argument("--output-db", default=str(DEFAULT_OUTPUT_DB_PATH), help="Path to output sqlite DB")
    ap.add_argument("--price-step", type=float, default=DEFAULT_PRICE_STEP, help="Price grid step, e.g. 0.05")
    ap.add_argument("--direction-gap", type=float, default=DEFAULT_DIRECTION_GAP)
    ap.add_argument("--direction-lookback-sec", type=int, default=DEFAULT_DIRECTION_LOOKBACK_SEC)
    ap.add_argument("--touch-volume-lookback-sec", type=int, default=DEFAULT_TOUCH_VOLUME_LOOKBACK_SEC)
    ap.add_argument("--order-size-shares", type=float, default=100.0)
    ap.add_argument("--min-duration-hours", type=float, default=0.0)
    ap.add_argument("--max-duration-hours", type=float, default=0.0)
    ap.add_argument(
        "--default-fee-rate",
        type=float,
        default=0.05,
        help="Fallback Fee Structure V2 rate for fee-enabled categories not in the explicit map",
    )
    ap.add_argument("--min-touch-volume-usdc", type=float, default=0.0)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    stats = build_taker_resolution_research(
        dataset_db_path=Path(args.db),
        tape_db_path=Path(args.tape_db),
        output_db_path=Path(args.output_db),
        price_step=float(args.price_step),
        direction_gap=float(args.direction_gap),
        direction_lookback_sec=int(args.direction_lookback_sec),
        touch_volume_lookback_sec=int(args.touch_volume_lookback_sec),
        order_size_shares=float(args.order_size_shares),
        min_duration_hours=float(args.min_duration_hours),
        max_duration_hours=float(args.max_duration_hours),
        default_fee_rate=float(args.default_fee_rate),
        min_touch_volume_usdc=float(args.min_touch_volume_usdc),
    )
    logger.info("Research DB written to %s", args.output_db)
    for key in sorted(stats):
        logger.info("%s=%s", key, stats[key])


if __name__ == "__main__":
    main()
