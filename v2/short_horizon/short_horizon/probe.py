from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LiveProbeSummary:
    run_id: str
    total_events: int
    market_state_updates: int
    book_updates: int
    trade_ticks: int
    order_events: int
    distinct_markets: int
    distinct_tokens: int
    first_event_time: str | None
    last_event_time: str | None


@dataclass(frozen=True)
class CollectorCrossValidation:
    run_id: str
    collector_rows: int
    probe_markets: int
    collector_markets: int
    overlapping_markets: int
    probe_tokens: int
    collector_tokens: int
    overlapping_tokens: int


def summarize_probe_db(db_path: str | Path, *, run_id: str) -> LiveProbeSummary:
    conn = sqlite3.connect(Path(db_path))
    try:
        total_events = _scalar(conn, "SELECT COUNT(*) FROM events_log WHERE run_id = ?", (run_id,))
        market_state_updates = _scalar(conn, "SELECT COUNT(*) FROM events_log WHERE run_id = ? AND event_type = 'MarketStateUpdate'", (run_id,))
        book_updates = _scalar(conn, "SELECT COUNT(*) FROM events_log WHERE run_id = ? AND event_type = 'BookUpdate'", (run_id,))
        trade_ticks = _scalar(conn, "SELECT COUNT(*) FROM events_log WHERE run_id = ? AND event_type = 'TradeTick'", (run_id,))
        order_events = _scalar(
            conn,
            "SELECT COUNT(*) FROM events_log WHERE run_id = ? AND event_type IN ('OrderAccepted', 'OrderRejected', 'OrderFilled', 'OrderCanceled')",
            (run_id,),
        )
        distinct_markets = _scalar(conn, "SELECT COUNT(DISTINCT market_id) FROM events_log WHERE run_id = ? AND market_id IS NOT NULL", (run_id,))
        distinct_tokens = _scalar(conn, "SELECT COUNT(DISTINCT token_id) FROM events_log WHERE run_id = ? AND token_id IS NOT NULL", (run_id,))
        first_event_time, last_event_time = conn.execute(
            "SELECT MIN(event_time), MAX(event_time) FROM events_log WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    finally:
        conn.close()
    return LiveProbeSummary(
        run_id=run_id,
        total_events=int(total_events),
        market_state_updates=int(market_state_updates),
        book_updates=int(book_updates),
        trade_ticks=int(trade_ticks),
        order_events=int(order_events),
        distinct_markets=int(distinct_markets),
        distinct_tokens=int(distinct_tokens),
        first_event_time=first_event_time,
        last_event_time=last_event_time,
    )


def cross_validate_probe_against_collector(db_path: str | Path, *, run_id: str, collector_csv_path: str | Path) -> CollectorCrossValidation:
    probe_market_ids, probe_token_ids = _probe_sets(db_path=db_path, run_id=run_id)
    collector_market_ids: set[str] = set()
    collector_token_ids: set[str] = set()
    collector_rows = 0
    with Path(collector_csv_path).open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            collector_rows += 1
            market_id = str(row.get("market_id") or "").strip()
            token_id = str(row.get("token_id") or "").strip()
            if market_id:
                collector_market_ids.add(market_id)
            if token_id:
                collector_token_ids.add(token_id)
    return CollectorCrossValidation(
        run_id=run_id,
        collector_rows=collector_rows,
        probe_markets=len(probe_market_ids),
        collector_markets=len(collector_market_ids),
        overlapping_markets=len(probe_market_ids & collector_market_ids),
        probe_tokens=len(probe_token_ids),
        collector_tokens=len(collector_token_ids),
        overlapping_tokens=len(probe_token_ids & collector_token_ids),
    )


def _probe_sets(*, db_path: str | Path, run_id: str) -> tuple[set[str], set[str]]:
    conn = sqlite3.connect(Path(db_path))
    try:
        market_rows = conn.execute(
            "SELECT DISTINCT market_id FROM events_log WHERE run_id = ? AND market_id IS NOT NULL",
            (run_id,),
        ).fetchall()
        token_rows = conn.execute(
            "SELECT DISTINCT token_id FROM events_log WHERE run_id = ? AND token_id IS NOT NULL",
            (run_id,),
        ).fetchall()
    finally:
        conn.close()
    return (
        {str(row[0]) for row in market_rows if row[0] is not None},
        {str(row[0]) for row in token_rows if row[0] is not None},
    )


def _scalar(conn: sqlite3.Connection, query: str, params: tuple[object, ...]) -> int:
    row = conn.execute(query, params).fetchone()
    return int(row[0] or 0) if row is not None else 0


__all__ = ["CollectorCrossValidation", "LiveProbeSummary", "cross_validate_probe_against_collector", "summarize_probe_db"]
