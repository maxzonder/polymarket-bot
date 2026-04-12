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
    from replay.tape_feed import DEFAULT_TAPE_DB_PATH
    from utils.logger import setup_logger
    from utils.paths import DATA_DIR, DB_PATH, ensure_runtime_dirs
else:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
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
    lookback_min_price: float
    lookback_max_price: float
    lookback_volume_usdc: float
    lookback_trade_count: int
    precursor_hit: int
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
            lookback_min_price REAL NOT NULL,
            lookback_max_price REAL NOT NULL,
            lookback_volume_usdc REAL NOT NULL,
            lookback_trade_count INTEGER NOT NULL,
            precursor_hit INTEGER NOT NULL,
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
            PRIMARY KEY (proxy_mode, bid_price, time_to_close_bucket, category, fees_enabled)
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
            closed_time=int(row["closed_time"] or 0),
        )
        for row in rows
    }


def iter_token_trades(tape_db_path: Path) -> Iterable[tuple[str, list[TradePoint]]]:
    conn = sqlite3.connect(tape_db_path)
    conn.row_factory = sqlite3.Row
    current_token_id: str | None = None
    bucket: list[TradePoint] = []
    query = (
        "SELECT source_file_id, seq, timestamp, market_id, token_id, price, size "
        "FROM tape ORDER BY token_id ASC, timestamp ASC, source_file_id ASC, seq ASC"
    )
    try:
        for row in conn.execute(query):
            token_id = str(row["token_id"])
            trade = TradePoint(
                timestamp=int(row["timestamp"]),
                market_id=str(row["market_id"]),
                token_id=token_id,
                price=float(row["price"]),
                size=float(row["size"]),
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
    lookback_volume_sec: int,
    min_lookback_volume_usdc: float,
    max_time_to_close_sec: int,
    bid_price: float,
    order_size_shares: float,
    cancel_after_sec: int,
) -> MakerCandidate | None:
    window = RollingTradeWindow(max(precursor_lookback_sec, lookback_volume_sec))
    for trade in trades:
        window.expire(trade.timestamp)
        time_to_close_sec = meta.closed_time - trade.timestamp
        lookback_min = window.min_price
        lookback_max = window.max_price
        lookback_vol = window.volume_usdc
        lookback_n = window.trade_count
        precursor_hit = int(lookback_min is not None and lookback_min <= precursor_max_price)

        if (
            trigger_price_min <= trade.price <= trigger_price_max
            and 0 < time_to_close_sec <= max_time_to_close_sec
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
                lookback_min_price=float(lookback_min or trade.price),
                lookback_max_price=float(lookback_max or trade.price),
                lookback_volume_usdc=float(lookback_vol),
                lookback_trade_count=int(lookback_n),
                precursor_hit=1,
                category=meta.category,
                neg_risk=meta.neg_risk,
                fees_enabled=meta.fees_enabled,
                is_winner=meta.is_winner,
            )

        window.add(trade.timestamp, trade.price, trade.size)
    return None


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
        if window_start <= trade.timestamp <= window_end
    ]
    raw_fillable_size = sum(trade.size for trade in future_trades if trade.price <= candidate.bid_price + 1e-9)

    def _first_fill_ts(required_size: float) -> int | None:
        cum = 0.0
        for trade in future_trades:
            if trade.price <= candidate.bid_price + 1e-9:
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
            lookback_min_price, lookback_max_price, lookback_volume_usdc,
            lookback_trade_count, precursor_hit, category, neg_risk, fees_enabled, is_winner
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            category, neg_risk, fees_enabled, is_winner, pnl_per_share, pnl_per_order
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    PARTITION BY proxy_mode, bid_price, time_to_close_bucket, category, fees_enabled
                    ORDER BY time_to_fill_sec
                ) AS rn,
                COUNT(*) OVER (
                    PARTITION BY proxy_mode, bid_price, time_to_close_bucket, category, fees_enabled
                ) AS cnt
            FROM maker_entry_proxy_fills
        ), summary AS (
            SELECT
                proxy_mode,
                bid_price,
                time_to_close_bucket,
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
            GROUP BY proxy_mode, bid_price, time_to_close_bucket, category, fees_enabled
        )
        SELECT
            proxy_mode,
            bid_price,
            time_to_close_bucket,
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
            proxy_mode, bid_price, time_to_close_bucket, category, fees_enabled,
            n_candidates, accepted_count, filled_count, winner_count_if_filled,
            fill_rate, accept_rate, win_rate_if_filled,
            avg_ev_per_placed_share, avg_ev_per_filled_share,
            avg_ev_per_placed_order, avg_ev_per_filled_order,
            median_time_to_fill_sec, avg_fillable_size
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    trigger_price_min: float,
    trigger_price_max: float,
    precursor_max_price: float,
    precursor_lookback_sec: int,
    lookback_volume_sec: int,
    min_lookback_volume_usdc: float,
    max_time_to_close_sec: int,
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
            ("trigger_price_min", f"{trigger_price_min:.4f}"),
            ("trigger_price_max", f"{trigger_price_max:.4f}"),
            ("precursor_max_price", f"{precursor_max_price:.4f}"),
            ("precursor_lookback_sec", str(precursor_lookback_sec)),
            ("lookback_volume_sec", str(lookback_volume_sec)),
            ("min_lookback_volume_usdc", f"{min_lookback_volume_usdc:.4f}"),
            ("max_time_to_close_sec", str(max_time_to_close_sec)),
            ("bid_price", f"{bid_price:.4f}"),
            ("order_size_shares", f"{order_size_shares:.4f}"),
            ("cancel_after_sec", str(cancel_after_sec)),
            ("rest_delay_sec", str(rest_delay_sec)),
            ("conservative_queue_multiplier", f"{conservative_queue_multiplier:.4f}"),
        ],
    )
    output_conn.commit()

    stats = {
        "tokens_seen": 0,
        "eligible_tokens": 0,
        "candidates": 0,
        "fill_rows": 0,
        "summary_rows": 0,
    }

    candidate_rows: list[tuple] = []
    fill_rows: list[tuple] = []

    for token_id, trades in iter_token_trades(tape_db_path):
        stats["tokens_seen"] += 1
        meta = token_meta.get(token_id)
        if meta is None:
            continue
        stats["eligible_tokens"] += 1
        candidate = find_first_candidate(
            meta=meta,
            trades=trades,
            trigger_price_min=trigger_price_min,
            trigger_price_max=trigger_price_max,
            precursor_max_price=precursor_max_price,
            precursor_lookback_sec=precursor_lookback_sec,
            lookback_volume_sec=lookback_volume_sec,
            min_lookback_volume_usdc=min_lookback_volume_usdc,
            max_time_to_close_sec=max_time_to_close_sec,
            bid_price=bid_price,
            order_size_shares=order_size_shares,
            cancel_after_sec=cancel_after_sec,
        )
        if candidate is None:
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
                candidate.lookback_min_price,
                candidate.lookback_max_price,
                candidate.lookback_volume_usdc,
                candidate.lookback_trade_count,
                candidate.precursor_hit,
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

    write_candidates(output_conn, candidate_rows)
    write_fills(output_conn, fill_rows)
    stats["summary_rows"] = build_summary(output_conn)
    output_conn.close()
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Proxy backtest for post-only maker entry on historical tape")
    parser.add_argument("--db", dest="dataset_db", type=Path, default=DB_PATH)
    parser.add_argument("--tape-db", dest="tape_db", type=Path, default=DEFAULT_TAPE_DB_PATH)
    parser.add_argument("--output-db", dest="output_db", type=Path, default=DEFAULT_OUTPUT_DB_PATH)
    parser.add_argument("--trigger-price-min", type=float, default=0.70)
    parser.add_argument("--trigger-price-max", type=float, default=0.75)
    parser.add_argument("--precursor-max-price", type=float, default=0.70)
    parser.add_argument("--precursor-lookback-sec", type=int, default=15 * 60)
    parser.add_argument("--lookback-volume-sec", type=int, default=60)
    parser.add_argument("--min-lookback-volume-usdc", type=float, default=30.0)
    parser.add_argument("--max-time-to-close-sec", type=int, default=30 * 60)
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
        trigger_price_min=args.trigger_price_min,
        trigger_price_max=args.trigger_price_max,
        precursor_max_price=args.precursor_max_price,
        precursor_lookback_sec=args.precursor_lookback_sec,
        lookback_volume_sec=args.lookback_volume_sec,
        min_lookback_volume_usdc=args.min_lookback_volume_usdc,
        max_time_to_close_sec=args.max_time_to_close_sec,
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
