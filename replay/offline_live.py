from __future__ import annotations

import sqlite3
import time
from bisect import bisect_right
from collections import OrderedDict
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import execution.order_manager as om_module
import strategy.screener as screener_module

from api.clob_client import ClobClient, Orderbook, OrderbookLevel
from api.gamma_client import MarketInfo
from config import BotConfig
from execution.order_manager import OrderManager
from execution.position_monitor import PositionMonitor
from strategy.market_scorer import MarketScorer
from strategy.risk_manager import RiskManager
from strategy.scorer import EntryFillScorer, ResolutionScorer
from strategy.screener import Screener
from utils.logger import setup_logger
from utils.paths import DB_PATH

from scripts.run_honest_replay import (
    _build_trade_file_index,
    _load_trades_sorted,
    load_all_markets,
)

logger = setup_logger("offline_live_replay")


def _write_runtime_snapshot(
    output_dir: Path,
    *,
    start: date,
    end: date,
    mode: str,
    limit: Optional[int],
    summary_only: bool,
    config: BotConfig,
) -> None:
    import json
    from dataclasses import asdict

    snapshot = {
        "run": {
            "kind": "offline_live",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "mode": mode,
            "limit": limit,
            "summary_only": summary_only,
            "output_dir": str(output_dir),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        },
        "bot_config": asdict(config),
    }
    (output_dir / "config_snapshot.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


@dataclass
class TokenRuntimeInfo:
    token_id: str
    market_id: str
    outcome_name: str
    is_winner: bool
    trade_path: Optional[str]


@dataclass
class MarketRuntimeInfo:
    market_id: str
    question: str
    category: Optional[str]
    volume_usdc: float
    comment_count: int
    end_date_ts: Optional[int]
    start_date_ts: Optional[int]
    neg_risk: bool
    neg_risk_group_id: Optional[str]
    token_ids: list[str]
    outcome_names: list[str]
    primary_token_id: Optional[str]


@dataclass
class TradeSeries:
    trades: list[dict]
    timestamps: list[int]
    prices: list[float]


@dataclass
class OfflineRunStats:
    screener_cycles: int = 0
    monitor_cycles: int = 0
    candidates_seen: int = 0
    orders_placed: int = 0
    markets_resolved: int = 0
    tokens_resolved: int = 0


class HistoricalMarketFeed:
    def __init__(
        self,
        markets: dict[str, MarketRuntimeInfo],
        tokens: dict[str, TokenRuntimeInfo],
        *,
        trade_cache_size: int = 512,
    ):
        self.markets = markets
        self.tokens = tokens
        self.trade_cache_size = max(8, int(trade_cache_size))
        self.now_ts: int = 0
        self._trade_cache: OrderedDict[str, TradeSeries] = OrderedDict()
        self._resolved_markets: set[str] = set()

    @classmethod
    def from_rows(
        cls,
        rows: list[dict],
        trade_index: dict[tuple[str, str], str],
        *,
        trade_cache_size: int = 512,
    ) -> "HistoricalMarketFeed":
        grouped: dict[str, list[dict]] = {}
        for row in rows:
            grouped.setdefault(str(row["market_id"]), []).append(row)

        tokens: dict[str, TokenRuntimeInfo] = {}
        markets: dict[str, MarketRuntimeInfo] = {}

        for market_id, market_rows in grouped.items():
            ordered_rows = sorted(
                market_rows,
                key=lambda row: cls._outcome_sort_key(row.get("outcome_name") or ""),
            )

            token_ids = [str(row["token_id"]) for row in ordered_rows]
            outcome_names = [str(row.get("outcome_name") or "") for row in ordered_rows]

            primary_token_id = None
            for row in ordered_rows:
                outcome = str(row.get("outcome_name") or "").strip().lower()
                if outcome == "yes":
                    primary_token_id = str(row["token_id"])
                    break
            if primary_token_id is None and token_ids:
                primary_token_id = token_ids[0]

            sample = ordered_rows[0]
            markets[market_id] = MarketRuntimeInfo(
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
                primary_token_id=primary_token_id,
            )

            for row in ordered_rows:
                token_id = str(row["token_id"])
                tokens[token_id] = TokenRuntimeInfo(
                    token_id=token_id,
                    market_id=market_id,
                    outcome_name=str(row.get("outcome_name") or ""),
                    is_winner=bool(row.get("is_winner", False)),
                    trade_path=trade_index.get((market_id, token_id)),
                )

        return cls(markets, tokens, trade_cache_size=trade_cache_size)

    @staticmethod
    def _outcome_sort_key(outcome_name: str) -> tuple[int, str]:
        lowered = str(outcome_name or "").strip().lower()
        if lowered == "yes":
            return (0, lowered)
        if lowered == "no":
            return (1, lowered)
        return (2, lowered)

    def set_now(self, ts: int) -> None:
        self.now_ts = int(ts)

    def _load_series(self, token_id: str) -> TradeSeries:
        if token_id in self._trade_cache:
            series = self._trade_cache.pop(token_id)
            self._trade_cache[token_id] = series
            return series

        token = self.tokens[token_id]
        trades = _load_trades_sorted(token.trade_path) if token.trade_path else []
        series = TradeSeries(
            trades=trades,
            timestamps=[int(t.get("timestamp") or 0) for t in trades],
            prices=[float(t.get("price") or 0.0) for t in trades],
        )
        self._trade_cache[token_id] = series
        while len(self._trade_cache) > self.trade_cache_size:
            self._trade_cache.popitem(last=False)
        return series

    def _series_price_at(self, token_id: str, ts: int) -> Optional[float]:
        series = self._load_series(token_id)
        idx = bisect_right(series.timestamps, int(ts)) - 1
        if idx < 0:
            return None
        price = series.prices[idx]
        return price if price > 0 else None

    def _series_last_trade_ts(self, token_id: str, ts: int) -> Optional[int]:
        series = self._load_series(token_id)
        idx = bisect_right(series.timestamps, int(ts)) - 1
        if idx < 0:
            return None
        return series.timestamps[idx]

    def token_price(self, token_id: str) -> Optional[float]:
        return self._series_price_at(token_id, self.now_ts)

    def fetch_open_markets(
        self,
        price_max: float = 0.30,
        volume_min: float = 50.0,
        volume_max: float = 100_000.0,
        limit_pages: int = 50,
    ) -> list[MarketInfo]:
        del limit_pages
        markets: list[MarketInfo] = []

        for market in self.markets.values():
            if market.market_id in self._resolved_markets:
                continue
            if market.start_date_ts and market.start_date_ts > self.now_ts:
                continue
            if market.end_date_ts and market.end_date_ts <= self.now_ts:
                continue
            if market.volume_usdc < volume_min or market.volume_usdc > volume_max:
                continue

            token_prices = {
                token_id: self.token_price(token_id)
                for token_id in market.token_ids
            }

            yes_price = self._derive_yes_price(market, token_prices)
            if yes_price is None:
                continue

            any_token_in_zone = any(
                price is not None and price <= price_max
                for price in token_prices.values()
            )
            if not any_token_in_zone and yes_price > price_max and yes_price < (1.0 - price_max):
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

    def _derive_yes_price(
        self,
        market: MarketRuntimeInfo,
        token_prices: dict[str, Optional[float]],
    ) -> Optional[float]:
        for token_id, outcome_name in zip(market.token_ids, market.outcome_names):
            if str(outcome_name or "").strip().lower() == "yes":
                return token_prices.get(token_id)

        for token_id, outcome_name in zip(market.token_ids, market.outcome_names):
            if str(outcome_name or "").strip().lower() == "no":
                no_price = token_prices.get(token_id)
                if no_price is not None:
                    return max(0.0001, min(0.9999, round(1.0 - no_price, 6)))

        for token_id in market.token_ids:
            price = token_prices.get(token_id)
            if price is not None:
                return price
        return None

    def get_last_trade_ts(self, condition_id: str, timeout: int = 10) -> Optional[int]:
        del timeout
        market = self.markets.get(str(condition_id))
        if market is None:
            return None
        last_ts: Optional[int] = None
        for token_id in market.token_ids:
            token_last = self._series_last_trade_ts(token_id, self.now_ts)
            if token_last is not None and (last_ts is None or token_last > last_ts):
                last_ts = token_last
        return last_ts

    def get_recent_trades(self, condition_id: str, limit: int = 50, timeout: int = 10) -> list[dict]:
        del timeout
        market = self.markets.get(str(condition_id))
        if market is None or market.primary_token_id is None:
            return []
        series = self._load_series(market.primary_token_id)
        idx = bisect_right(series.timestamps, self.now_ts)
        return list(reversed(series.trades[:idx]))[:limit]

    def get_orderbook(self, token_id: str) -> Orderbook:
        price = self.token_price(token_id)
        if price is None:
            return Orderbook(token_id=token_id, bids=[], asks=[], best_bid=None, best_ask=None)

        best_ask = float(price)
        best_bid = max(0.0001, round(best_ask * 0.95, 6))
        depth = max(50.0, round(10.0 / max(best_ask, 0.0001), 2))
        return Orderbook(
            token_id=token_id,
            bids=[OrderbookLevel(price=best_bid, size=depth)],
            asks=[OrderbookLevel(price=best_ask, size=depth)],
            best_bid=best_bid,
            best_ask=best_ask,
        )

    def resolve_due_markets(self, order_manager: OrderManager) -> tuple[int, int]:
        resolved_markets = 0
        resolved_tokens = 0

        for market in self.markets.values():
            if market.market_id in self._resolved_markets:
                continue
            if market.end_date_ts is None or market.end_date_ts > self.now_ts:
                continue

            for token_id in market.token_ids:
                token = self.tokens[token_id]
                order_manager.on_market_resolved(token_id, token.is_winner)
                self._cancel_live_resting(order_manager, market.market_id, token_id)
                resolved_tokens += 1

            self._resolved_markets.add(market.market_id)
            resolved_markets += 1

        return resolved_markets, resolved_tokens

    def _cancel_live_resting(self, order_manager: OrderManager, market_id: str, token_id: str) -> None:
        conn = sqlite3.connect(str(om_module.POSITIONS_DB))
        live_orders = conn.execute(
            "SELECT order_id FROM resting_orders WHERE market_id=? AND token_id=? AND status='live'",
            (market_id, token_id),
        ).fetchall()
        for row in live_orders:
            order_manager.clob.cancel_order(row[0])
            conn.execute(
                "UPDATE resting_orders SET status='cancelled' WHERE order_id=?",
                (row[0],),
            )
        conn.commit()
        conn.close()


class OfflineLiveRunner:
    def __init__(
        self,
        *,
        start: date,
        end: date,
        mode: str,
        output_dir: Path,
        limit: Optional[int] = None,
        summary_only: bool = False,
        trade_cache_size: int = 512,
    ):
        self.start = start
        self.end = end
        self.mode = mode
        self.output_dir = output_dir
        self.limit = limit
        self.summary_only = summary_only
        self.trade_cache_size = trade_cache_size

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.positions_db = self.output_dir / "positions.db"
        self.paper_db = self.output_dir / "paper_trades.db"
        om_module.POSITIONS_DB = self.positions_db

        self.config = BotConfig(mode=mode, dry_run=True)
        self.mc = self.config.mode_config
        _write_runtime_snapshot(
            self.output_dir,
            start=start,
            end=end,
            mode=mode,
            limit=limit,
            summary_only=summary_only,
            config=self.config,
        )

        self.clob = ClobClient(private_key="offline_live_dummy", dry_run=True, paper_db_path=self.paper_db)
        risk = RiskManager(self.mc, balance_usdc=self.config.paper_initial_balance_usdc)
        self.order_manager = OrderManager(self.config, self.clob, risk)

        self.entry_fill_scorer = EntryFillScorer(
            entry_price_max=max(self.mc.entry_price_levels) if self.mc.entry_price_levels else self.config.scorer_entry_price_max,
            min_samples=self.config.scorer_min_samples,
        )
        self.resolution_scorer = ResolutionScorer(min_samples=self.config.scorer_min_samples)
        self.market_scorer: Optional[MarketScorer] = None
        if self.mc.min_market_score > 0 or self.mc.market_score_tiers:
            self.market_scorer = MarketScorer(db_path=DB_PATH, min_score=self.mc.min_market_score)
        self.screener = Screener(
            config=self.config,
            entry_fill_scorer=self.entry_fill_scorer,
            resolution_scorer=self.resolution_scorer,
            db_path=self.positions_db,
            market_scorer=self.market_scorer,
        )
        self.monitor = PositionMonitor(self.config, self.clob, self.order_manager)

        self.stats = OfflineRunStats()
        self.start_ts = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
        self.end_ts = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())

        db_conn = sqlite3.connect(DB_PATH)
        db_conn.row_factory = sqlite3.Row
        rows = load_all_markets(db_conn, self.start_ts, self.end_ts, self.config)
        db_conn.close()
        if limit is not None:
            rows = rows[:limit]

        logger.info("Building historical trade index...")
        trade_index = _build_trade_file_index()
        self.feed = HistoricalMarketFeed.from_rows(
            rows,
            trade_index,
            trade_cache_size=trade_cache_size,
        )
        self.total_markets = len(self.feed.markets)
        self.total_tokens = len(self.feed.tokens)

    def run(self) -> None:
        logger.info(
            f"Offline live replay: {self.start} → {self.end} | mode={self.mode} | "
            f"markets={self.total_markets} tokens={self.total_tokens} | output={self.output_dir}"
        )
        t0 = time.monotonic()

        next_screener = self.start_ts
        next_monitor = self.start_ts

        while True:
            ts = min(next_screener, next_monitor)
            if ts > self.end_ts:
                break
            self.feed.set_now(ts)

            with self._patched_runtime(ts):
                if ts == next_screener:
                    self._run_screener_cycle(ts)
                    next_screener += max(1, self.config.screener_interval)
                if ts == next_monitor:
                    self._run_monitor_cycle(ts)
                    next_monitor += max(1, self.config.monitor_interval)

        self.feed.set_now(self.end_ts)
        with self._patched_runtime(self.end_ts):
            self._run_monitor_cycle(self.end_ts)

        elapsed = time.monotonic() - t0
        logger.info(f"Offline live replay finished in {elapsed:.1f}s")
        self.print_summary(elapsed)

    def _patched_runtime(self, ts: int):
        _noop = lambda *a, **kw: None
        stack = ExitStack()
        stack.enter_context(patch("time.time", return_value=float(ts)))
        stack.enter_context(patch.object(screener_module, "fetch_open_markets", side_effect=self.feed.fetch_open_markets))
        stack.enter_context(patch.object(screener_module, "get_last_trade_ts", side_effect=self.feed.get_last_trade_ts))
        stack.enter_context(patch.object(screener_module, "get_recent_trades", side_effect=self.feed.get_recent_trades))
        stack.enter_context(patch("execution.order_manager.get_orderbook", side_effect=self.feed.get_orderbook))
        stack.enter_context(patch("execution.position_monitor.get_orderbook", side_effect=self.feed.get_orderbook))
        stack.enter_context(patch.object(screener_module, "tg_alert", _noop))
        stack.enter_context(patch.object(om_module, "send_message", _noop))
        return stack

    def _run_screener_cycle(self, ts: int) -> None:
        candidates = self.screener.scan()
        self.stats.screener_cycles += 1
        self.stats.candidates_seen += len(candidates)

        placed = 0
        for candidate in candidates:
            if self.mc.scanner_entry and candidate.current_price <= self.mc.entry_price_max:
                placed += len(self.order_manager.process_scanner_entry(candidate))
            elif self.mc.use_resting_bids:
                placed += len(self.order_manager.process_candidate(candidate))

        self.stats.orders_placed += placed
        if not self.summary_only:
            logger.info(
                f"[tick {ts}] screener candidates={len(candidates)} placed={placed} "
                f"open_positions={self.order_manager.get_open_position_count()} "
                f"resting={self.order_manager.get_live_resting_order_count()} "
                f"cash=${self.order_manager.get_cash_balance():.4f}"
            )

    def _run_monitor_cycle(self, ts: int) -> None:
        self.monitor._check_fills_dry_run()
        self.monitor._update_peak_prices()
        resolved_markets, resolved_tokens = self.feed.resolve_due_markets(self.order_manager)
        self.stats.monitor_cycles += 1
        self.stats.markets_resolved += resolved_markets
        self.stats.tokens_resolved += resolved_tokens

        if not self.summary_only:
            logger.info(
                f"[tick {ts}] monitor resolved_markets={resolved_markets} "
                f"resolved_tokens={resolved_tokens} open_positions={self.order_manager.get_open_position_count()} "
                f"resting={self.order_manager.get_live_resting_order_count()} cash=${self.order_manager.get_cash_balance():.4f}"
            )

    def print_summary(self, elapsed_s: float) -> None:
        conn = sqlite3.connect(self.positions_db)
        conn.row_factory = sqlite3.Row
        positions = conn.execute("SELECT * FROM positions").fetchall()
        resting_live = conn.execute("SELECT COUNT(*) FROM resting_orders WHERE status='live'").fetchone()[0]
        tp_live = conn.execute("SELECT COUNT(*) FROM tp_orders WHERE status='live'").fetchone()[0]
        conn.close()

        total_stake = sum(float(p["entry_size_usdc"] or 0.0) for p in positions)
        total_pnl = sum(float(p["realized_pnl"] or 0.0) for p in positions)
        resolved = [p for p in positions if p["status"] == "resolved"]
        winners = sum(1 for p in resolved if float(p["realized_pnl"] or 0.0) > 0)
        open_positions = sum(1 for p in positions if p["status"] == "open")
        roi_pct = (total_pnl / total_stake * 100.0) if total_stake > 0 else 0.0

        sep = "=" * 66
        print(f"\n{sep}")
        print(f"  OFFLINE LIVE REPLAY   mode={self.mode}  {self.start} → {self.end}")
        print(sep)
        print(f"  Markets loaded                : {self.total_markets}")
        print(f"  Tokens loaded                 : {self.total_tokens}")
        print(f"  Screener cycles               : {self.stats.screener_cycles}")
        print(f"  Monitor cycles                : {self.stats.monitor_cycles}")
        print(f"  Candidates seen               : {self.stats.candidates_seen}")
        print(f"  Orders placed                 : {self.stats.orders_placed}")
        print(f"  Markets resolved              : {self.stats.markets_resolved}")
        print(f"  Tokens resolved               : {self.stats.tokens_resolved}")
        print()
        print(f"  Positions opened              : {len(positions)}")
        print(f"    Resolved                    : {len(resolved)}  (winners={winners} losers={len(resolved)-winners})")
        print(f"    Still open                  : {open_positions}")
        print(f"  Live resting BUYs             : {resting_live}")
        print(f"  Live TP SELLs                 : {tp_live}")
        print(f"  Total stake deployed  (USDC)  : ${total_stake:.4f}")
        print(f"  Total realized PnL    (USDC)  : ${total_pnl:.4f}")
        print(f"  ROI                           : {roi_pct:.1f}%")
        print(f"  Ending cash balance   (USDC)  : ${self.order_manager.get_cash_balance():.4f}")
        print(f"  Runtime seconds               : {elapsed_s:.1f}")
        print(sep)
        print(f"  Positions DB : {self.positions_db}")
        print(f"  Paper trades : {self.paper_db}\n")
