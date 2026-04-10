from __future__ import annotations

import sqlite3
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from api.clob_client import Orderbook, OrderbookLevel
from api.gamma_client import MarketInfo

from replay.tape_feed import TapeBatch, TapeTrade


@dataclass
class TokenSnapshot:
    token_id: str
    market_id: str
    outcome_name: str
    last_trade_ts: Optional[int] = None
    last_price: Optional[float] = None
    recent_trades: deque[dict] = field(default_factory=lambda: deque(maxlen=128))
    is_winner: Optional[bool] = None


@dataclass(frozen=True)
class MarketMeta:
    market_id: str
    question: str
    category: Optional[str]
    volume_usdc: float
    comment_count: int
    end_date_ts: Optional[int]
    start_date_ts: Optional[int]
    neg_risk: bool
    neg_risk_group_id: Optional[str]
    token_ids: tuple[str, ...]
    outcome_names: tuple[str, ...]


class OfflineDryRunState:
    """Time-driven snapshot state fed by global historical tape batches."""

    def __init__(
        self,
        markets: dict[str, MarketMeta],
        tokens: dict[str, TokenSnapshot],
        *,
        initial_active_market_ids: Optional[set[str]] = None,
        start_events: tuple[tuple[int, str], ...] = (),
        end_events: tuple[tuple[int, str], ...] = (),
    ):
        self.markets = markets
        self.tokens = tokens
        self.now_ts: Optional[int] = None
        self.dirty_tokens: set[str] = set()
        self.dirty_markets: set[str] = set()
        self._priced_market_ids: set[str] = set()
        self._orderbook_cache: dict[str, Orderbook] = {}
        self._initial_active_market_ids = set(initial_active_market_ids or set())
        self._start_events = start_events
        self._end_events = end_events
        self._active_market_ids: set[str] = set(self._initial_active_market_ids)
        self._start_event_idx = 0
        self._end_event_idx = 0
        self._active_synced_ts: Optional[int] = None

    @classmethod
    def from_rows(cls, rows: list[dict]) -> "OfflineDryRunState":
        grouped: dict[str, list[dict]] = {}
        for row in rows:
            grouped.setdefault(str(row["market_id"]), []).append(row)

        markets: dict[str, MarketMeta] = {}
        tokens: dict[str, TokenSnapshot] = {}
        initial_active_market_ids: set[str] = set()
        start_events: list[tuple[int, str]] = []
        end_events: list[tuple[int, str]] = []

        for market_id, market_rows in grouped.items():
            ordered = sorted(
                market_rows,
                key=lambda row: cls._outcome_sort_key(row.get("outcome_name") or ""),
            )
            token_ids = tuple(str(row["token_id"]) for row in ordered)
            outcome_names = tuple(str(row.get("outcome_name") or "") for row in ordered)
            sample = ordered[0]
            market_meta = MarketMeta(
                market_id=market_id,
                question=str(sample.get("question") or ""),
                category=sample.get("category"),
                volume_usdc=float(sample.get("volume") or 0.0),
                comment_count=int(sample.get("comment_count") or 0),
                end_date_ts=int(sample.get("end_date") or 0) or None,
                start_date_ts=int(sample.get("start_date") or 0) or None,
                neg_risk=bool(sample.get("neg_risk", False)),
                neg_risk_group_id=(
                    str(sample.get("neg_risk_market_id"))
                    if sample.get("neg_risk_market_id") is not None
                    else None
                ),
                token_ids=token_ids,
                outcome_names=outcome_names,
            )
            markets[market_id] = market_meta

            if market_meta.start_date_ts is None:
                initial_active_market_ids.add(market_id)
            else:
                start_events.append((market_meta.start_date_ts, market_id))
            if market_meta.end_date_ts is not None:
                end_events.append((market_meta.end_date_ts, market_id))
            for row in ordered:
                token_id = str(row["token_id"])
                raw_winner = row.get("is_winner")
                tokens[token_id] = TokenSnapshot(
                    token_id=token_id,
                    market_id=market_id,
                    outcome_name=str(row.get("outcome_name") or ""),
                    is_winner=bool(raw_winner) if raw_winner is not None else None,
                )

        return cls(
            markets=markets,
            tokens=tokens,
            initial_active_market_ids=initial_active_market_ids,
            start_events=tuple(sorted(start_events)),
            end_events=tuple(sorted(end_events)),
        )

    @staticmethod
    def _outcome_sort_key(outcome_name: str) -> tuple[int, str]:
        lowered = str(outcome_name or "").strip().lower()
        if lowered == "yes":
            return (0, lowered)
        if lowered == "no":
            return (1, lowered)
        return (2, lowered)

    def apply_batch(self, batch: TapeBatch) -> None:
        self.now_ts = batch.batch_end_ts
        self.dirty_tokens = set()
        self.dirty_markets = set()
        for trade in batch.trades:
            self.apply_trade(trade)
        self._sync_active_markets(self.now_ts)

    def apply_trade(self, trade: TapeTrade) -> None:
        snap = self.tokens.get(trade.token_id)
        if snap is None:
            return
        self.dirty_tokens.add(trade.token_id)
        self.dirty_markets.add(trade.market_id)
        self._priced_market_ids.add(trade.market_id)
        self._orderbook_cache.pop(trade.token_id, None)
        snap.last_trade_ts = trade.timestamp
        snap.last_price = trade.price
        snap.recent_trades.append(
            {
                "timestamp": trade.timestamp,
                "price": trade.price,
                "size": trade.size,
                "side": trade.side,
                **trade.raw,
            }
        )

    def token_price(self, token_id: str) -> Optional[float]:
        snap = self.tokens.get(token_id)
        return snap.last_price if snap is not None else None

    def get_orderbook(self, token_id: str) -> Orderbook:
        cached = self._orderbook_cache.get(token_id)
        if cached is not None:
            return cached

        price = self.token_price(token_id)
        if price is None:
            book = Orderbook(token_id=token_id, bids=[], asks=[], best_bid=None, best_ask=None)
        else:
            best_ask = float(price)
            best_bid = max(0.0001, round(best_ask * 0.95, 6))
            depth = max(50.0, round(10.0 / max(best_ask, 0.0001), 2))
            book = Orderbook(
                token_id=token_id,
                bids=[OrderbookLevel(price=best_bid, size=depth)],
                asks=[OrderbookLevel(price=best_ask, size=depth)],
                best_bid=best_bid,
                best_ask=best_ask,
            )

        self._orderbook_cache[token_id] = book
        return book

    def get_last_trade_ts(self, market_id: str) -> Optional[int]:
        market = self.markets.get(str(market_id))
        if market is None:
            return None
        latest: Optional[int] = None
        for token_id in market.token_ids:
            snap = self.tokens[token_id]
            if snap.last_trade_ts is not None and (latest is None or snap.last_trade_ts > latest):
                latest = snap.last_trade_ts
        return latest

    def get_recent_trades(self, market_id: str, limit: int = 50) -> list[dict]:
        market = self.markets.get(str(market_id))
        if market is None:
            return []
        merged: list[dict] = []
        for token_id in market.token_ids:
            merged.extend(self.tokens[token_id].recent_trades)
        merged.sort(key=lambda row: int(row.get("timestamp") or 0), reverse=True)
        return merged[:limit]

    def fetch_open_markets(
        self,
        price_max: float = 0.30,
        volume_min: float = 50.0,
        volume_max: float = 100_000.0,
        limit_pages: int = 50,
    ) -> list[MarketInfo]:
        del limit_pages
        if self.now_ts is None:
            return []

        self._sync_active_markets(self.now_ts)
        candidate_market_ids = self._active_market_ids & self._priced_market_ids

        markets: list[MarketInfo] = []
        for market_id in candidate_market_ids:
            market = self.markets[market_id]
            if market.volume_usdc < volume_min or market.volume_usdc > volume_max:
                continue

            token_prices = [self.tokens[token_id].last_price for token_id in market.token_ids]
            if all(price is None for price in token_prices):
                continue

            yes_price = self._derive_yes_price(market)
            if yes_price is None:
                continue

            any_in_zone = any(price is not None and price <= price_max for price in token_prices)
            if not any_in_zone and yes_price > price_max and yes_price < (1.0 - price_max):
                continue

            markets.append(
                MarketInfo(
                    market_id=market.market_id,
                    condition_id=market.market_id,
                    question=market.question,
                    category=market.category,
                    token_ids=list(market.token_ids),
                    outcome_names=list(market.outcome_names),
                    best_ask=yes_price,
                    best_bid=max(0.0001, round(yes_price * 0.95, 6)),
                    last_trade_price=yes_price,
                    volume_usdc=market.volume_usdc,
                    liquidity_usdc=0.0,
                    comment_count=market.comment_count,
                    fees_enabled=False,
                    end_date_ts=market.end_date_ts,
                    hours_to_close=(
                        (market.end_date_ts - self.now_ts) / 3600.0
                        if market.end_date_ts is not None
                        else None
                    ),
                    neg_risk=market.neg_risk,
                    neg_risk_group_id=market.neg_risk_group_id,
                )
            )
        return markets

    def _sync_active_markets(self, ts: int) -> None:
        ts = int(ts)
        if self._active_synced_ts is not None and ts < self._active_synced_ts:
            self._active_market_ids = set(self._initial_active_market_ids)
            self._start_event_idx = 0
            self._end_event_idx = 0
            self._active_synced_ts = None

        while self._start_event_idx < len(self._start_events) and self._start_events[self._start_event_idx][0] <= ts:
            _start_ts, market_id = self._start_events[self._start_event_idx]
            self._active_market_ids.add(market_id)
            self._start_event_idx += 1

        while self._end_event_idx < len(self._end_events) and self._end_events[self._end_event_idx][0] <= ts:
            _end_ts, market_id = self._end_events[self._end_event_idx]
            self._active_market_ids.discard(market_id)
            self._end_event_idx += 1

        self._active_synced_ts = ts

    def _derive_yes_price(self, market: MarketMeta) -> Optional[float]:
        for token_id, outcome_name in zip(market.token_ids, market.outcome_names):
            if str(outcome_name).strip().lower() == "yes":
                return self.tokens[token_id].last_price
        for token_id, outcome_name in zip(market.token_ids, market.outcome_names):
            if str(outcome_name).strip().lower() == "no":
                no_price = self.tokens[token_id].last_price
                if no_price is not None:
                    return max(0.0001, min(0.9999, round(1.0 - no_price, 6)))
        for token_id in market.token_ids:
            price = self.tokens[token_id].last_price
            if price is not None:
                return price
        return None


def load_all_markets(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
    config,
) -> list[dict]:
    """
    Load all markets closing within the simulation window, with their tokens.
    Applies static screener filters:
      - volume between config min/max
      - end_date within [start_ts, end_ts + max_hours_to_close buffer]
      - excludes markets that closed before start_ts
    """
    mc = config.mode_config
    close_buffer = int(mc.max_hours_to_close * 3600)

    rows = conn.execute(
        """
        SELECT
            m.id           AS market_id,
            m.question,
            m.category,
            m.volume,
            m.end_date,
            m.start_date,
            m.duration_hours,
            m.neg_risk,
            m.neg_risk_market_id,
            COALESCE(m.comment_count, 0) AS comment_count,
            t.token_id,
            t.outcome_name,
            t.is_winner
        FROM markets m
        JOIN tokens t ON m.id = t.market_id
        WHERE m.end_date  >= :start
          AND m.end_date  <= :end_buf
          AND m.volume    >= :vol_min
          AND m.volume    <= :vol_max
        ORDER BY m.end_date ASC
        """,
        {
            "start":   start_ts,
            "end_buf": end_ts + close_buffer,
            "vol_min": config.min_volume_usdc,
            "vol_max": config.max_volume_usdc,
        },
    ).fetchall()

    return [dict(r) for r in rows]
