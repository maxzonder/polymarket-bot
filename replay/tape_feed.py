from __future__ import annotations

import json
import os
from dataclasses import dataclass
from heapq import heappop, heappush
from itertools import count
from pathlib import Path
from typing import Iterable, Iterator, Optional

from utils.paths import DATABASE_DIR


@dataclass(frozen=True)
class TapeTrade:
    timestamp: int
    market_id: str
    token_id: str
    price: float
    size: float
    side: str
    raw: dict


@dataclass(frozen=True)
class TapeBatch:
    batch_start_ts: int
    batch_end_ts: int
    trades: tuple[TapeTrade, ...]


@dataclass(frozen=True)
class TradeFileRef:
    market_id: str
    token_id: str
    path: Path


def discover_trade_files(database_dir: Path = DATABASE_DIR) -> list[TradeFileRef]:
    refs: list[TradeFileRef] = []
    if not database_dir.exists():
        return refs

    for date_dir in sorted(database_dir.iterdir()):
        if not date_dir.is_dir() or not date_dir.name[:4].isdigit():
            continue
        for market_dir in sorted(date_dir.iterdir()):
            if not market_dir.is_dir() or not market_dir.name.endswith("_trades"):
                continue
            market_id = market_dir.name[:-7]
            for trade_file in sorted(market_dir.iterdir()):
                if trade_file.suffix != ".json" or trade_file.name.startswith("."):
                    continue
                refs.append(
                    TradeFileRef(
                        market_id=market_id,
                        token_id=trade_file.stem,
                        path=trade_file,
                    )
                )
    return refs


def iter_trade_file(
    ref: TradeFileRef,
    *,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
) -> Iterator[TapeTrade]:
    try:
        raw_trades = json.loads(ref.path.read_text(encoding="utf-8"))
    except Exception:
        return iter(())

    raw_trades.sort(key=lambda row: int(row.get("timestamp") or 0))

    def _gen() -> Iterator[TapeTrade]:
        for row in raw_trades:
            ts = int(row.get("timestamp") or 0)
            if start_ts is not None and ts < start_ts:
                continue
            if end_ts is not None and ts > end_ts:
                break
            yield TapeTrade(
                timestamp=ts,
                market_id=ref.market_id,
                token_id=ref.token_id,
                price=float(row.get("price") or 0.0),
                size=float(row.get("size") or 0.0),
                side=str(row.get("side") or ""),
                raw=row,
            )

    return _gen()


def iter_global_tape(
    *,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    database_dir: Path = DATABASE_DIR,
    selected_tokens: Optional[set[str]] = None,
    selected_markets: Optional[set[str]] = None,
) -> Iterator[TapeTrade]:
    refs = discover_trade_files(database_dir)
    heap: list[tuple[int, int, TapeTrade, Iterator[TapeTrade]]] = []
    seq = count()

    for ref in refs:
        if selected_tokens is not None and ref.token_id not in selected_tokens:
            continue
        if selected_markets is not None and ref.market_id not in selected_markets:
            continue
        it = iter_trade_file(ref, start_ts=start_ts, end_ts=end_ts)
        try:
            trade = next(it)
        except StopIteration:
            continue
        heappush(heap, (trade.timestamp, next(seq), trade, it))

    while heap:
        _ts, _seq, trade, it = heappop(heap)
        yield trade
        try:
            nxt = next(it)
        except StopIteration:
            continue
        heappush(heap, (nxt.timestamp, next(seq), nxt, it))


def iter_tape_batches(
    *,
    batch_seconds: int,
    start_ts: int,
    end_ts: int,
    database_dir: Path = DATABASE_DIR,
    selected_tokens: Optional[set[str]] = None,
    selected_markets: Optional[set[str]] = None,
) -> Iterator[TapeBatch]:
    if batch_seconds <= 0:
        raise ValueError("batch_seconds must be > 0")
    if end_ts < start_ts:
        raise ValueError("end_ts must be >= start_ts")

    current_batch_start = start_ts
    current_batch_end = min(end_ts, start_ts + batch_seconds - 1)
    current_trades: list[TapeTrade] = []

    for trade in iter_global_tape(
        start_ts=start_ts,
        end_ts=end_ts,
        database_dir=database_dir,
        selected_tokens=selected_tokens,
        selected_markets=selected_markets,
    ):
        while trade.timestamp > current_batch_end:
            yield TapeBatch(
                batch_start_ts=current_batch_start,
                batch_end_ts=current_batch_end,
                trades=tuple(current_trades),
            )
            current_trades = []
            current_batch_start = current_batch_end + 1
            current_batch_end = min(end_ts, current_batch_start + batch_seconds - 1)
        current_trades.append(trade)

    while current_batch_start <= end_ts:
        yield TapeBatch(
            batch_start_ts=current_batch_start,
            batch_end_ts=current_batch_end,
            trades=tuple(current_trades),
        )
        current_trades = []
        current_batch_start = current_batch_end + 1
        current_batch_end = min(end_ts, current_batch_start + batch_seconds - 1)
