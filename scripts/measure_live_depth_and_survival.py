#!/usr/bin/env python3
"""Live Polymarket short-horizon depth + ask-survival measurement.

Combined replacement for the original split P0-A / P0-B plan.

What it does:
- bootstraps active short-horizon markets from Gamma
- subscribes to Polymarket CLOB market websocket by token asset ids
- watches ascending best_ask touches at configured price levels
- records ask-side depth snapshot at touch
- records ask survival for the touched level over a short post-touch window
- writes per-event rows to CSV for later analysis

Notes:
- this is intentionally measurement-only, not trading code
- this script depends on a websocket client library (`websockets`)
- live trigger semantics are best_ask-touch based, which is more execution-grounded
  than the current historical research path
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

from api.gamma_client import GAMMA_BASE

try:
    import websockets
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: websockets. Install it before running this script, for example: "
        "python3 -m pip install websockets"
    ) from exc


WS_MARKET_ENDPOINT = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DEFAULT_LEVELS = (0.55, 0.65, 0.70)
DEFAULT_NOTIONALS = (10.0, 50.0, 100.0)
DEFAULT_DEPTH_LEVELS = 5
DEFAULT_SURVIVAL_WINDOW_MS = 2000
DEFAULT_SAMPLING_INTERVAL_MS = 25
DEFAULT_REFRESH_INTERVAL_SEC = 30
DEFAULT_DURATION_MIN = 840
DEFAULT_DURATION_MAX = 960
DEFAULT_PING_INTERVAL_SEC = 10


@dataclass
class MarketToken:
    market_id: str
    condition_id: str
    question: str
    token_id: str
    outcome: str
    end_time_iso: str
    start_time_iso: Optional[str]
    duration_seconds: Optional[int]
    fees_enabled: bool
    fee_rate_bps: Optional[float]
    tick_size: Optional[float]


@dataclass
class BookState:
    token_id: str
    market_id: str | None = None
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    bid_levels: list[tuple[float, float]] = field(default_factory=list)
    ask_levels: list[tuple[float, float]] = field(default_factory=list)
    last_event_ts_ms: Optional[int] = None
    last_ingest_monotonic_ms: Optional[int] = None
    last_book_hash: Optional[str] = None


@dataclass
class SurvivalProbe:
    probe_id: str
    token: MarketToken
    level: float
    started_at_ms: int
    initial_best_ask: float
    initial_ask_size_at_level: float
    ask_levels_at_touch: list[tuple[float, float]]
    notional_fit: dict[str, str]
    done: bool = False
    end_reason: Optional[str] = None
    survived_ms: Optional[int] = None


class CsvSink:
    FIELDNAMES = [
        "probe_id",
        "recorded_at",
        "market_id",
        "condition_id",
        "token_id",
        "outcome",
        "question",
        "touch_level",
        "touch_time_iso",
        "duration_seconds",
        "start_time_iso",
        "end_time_iso",
        "fees_enabled",
        "fee_rate_bps",
        "tick_size",
        "best_bid_at_touch",
        "best_ask_at_touch",
        "ask_level_1_price",
        "ask_level_1_size",
        "ask_level_2_price",
        "ask_level_2_size",
        "ask_level_3_price",
        "ask_level_3_size",
        "ask_level_4_price",
        "ask_level_4_size",
        "ask_level_5_price",
        "ask_level_5_size",
        "ask_size_at_touch_level",
        "fit_10_usdc",
        "fit_50_usdc",
        "fit_100_usdc",
        "survived_ms",
        "end_reason",
    ]

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=self.FIELDNAMES)
        if self.path.stat().st_size == 0:
            self._writer.writeheader()
            self._fh.flush()

    def write_probe(self, probe: SurvivalProbe, book: BookState) -> None:
        levels = list(probe.ask_levels_at_touch[:5])
        while len(levels) < 5:
            levels.append((None, None))
        row = {
            "probe_id": probe.probe_id,
            "recorded_at": utc_now_iso(),
            "market_id": probe.token.market_id,
            "condition_id": probe.token.condition_id,
            "token_id": probe.token.token_id,
            "outcome": probe.token.outcome,
            "question": probe.token.question,
            "touch_level": f"{probe.level:.2f}",
            "touch_time_iso": iso_from_ms(probe.started_at_ms),
            "duration_seconds": probe.token.duration_seconds,
            "start_time_iso": probe.token.start_time_iso,
            "end_time_iso": probe.token.end_time_iso,
            "fees_enabled": int(probe.token.fees_enabled),
            "fee_rate_bps": probe.token.fee_rate_bps,
            "tick_size": probe.token.tick_size,
            "best_bid_at_touch": book.best_bid,
            "best_ask_at_touch": probe.initial_best_ask,
            "ask_level_1_price": levels[0][0],
            "ask_level_1_size": levels[0][1],
            "ask_level_2_price": levels[1][0],
            "ask_level_2_size": levels[1][1],
            "ask_level_3_price": levels[2][0],
            "ask_level_3_size": levels[2][1],
            "ask_level_4_price": levels[3][0],
            "ask_level_4_size": levels[3][1],
            "ask_level_5_price": levels[4][0],
            "ask_level_5_size": levels[4][1],
            "ask_size_at_touch_level": probe.initial_ask_size_at_level,
            "fit_10_usdc": probe.notional_fit.get("10"),
            "fit_50_usdc": probe.notional_fit.get("50"),
            "fit_100_usdc": probe.notional_fit.get("100"),
            "survived_ms": probe.survived_ms,
            "end_reason": probe.end_reason,
        }
        self._writer.writerow(row)
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class LiveDepthCollector:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.logger = logging.getLogger("measure_live_depth_and_survival")
        self.books: dict[str, BookState] = {}
        self.tokens_by_id: dict[str, MarketToken] = {}
        self.last_best_ask: dict[tuple[str, float], Optional[float]] = {}
        self.active_probes: dict[str, SurvivalProbe] = {}
        self.completed_probes = 0
        self.csv = CsvSink(Path(args.output_csv))
        self.stop_event = asyncio.Event()

    async def run(self) -> None:
        self._install_signal_handlers()
        bootstrap = self.fetch_active_market_tokens()
        if not bootstrap:
            raise SystemExit("No eligible active tokens found for configured duration window.")
        self.tokens_by_id = {token.token_id: token for token in bootstrap}
        self.logger.info("Bootstrapped %s eligible tokens", len(self.tokens_by_id))

        consumer = asyncio.create_task(self.market_ws_loop())
        refresh = asyncio.create_task(self.refresh_loop())
        survival = asyncio.create_task(self.survival_loop())

        try:
            await self.stop_event.wait()
        finally:
            for task in (consumer, refresh, survival):
                task.cancel()
            await asyncio.gather(consumer, refresh, survival, return_exceptions=True)
            self.csv.close()

    def fetch_active_market_tokens(self) -> list[MarketToken]:
        markets: list[MarketToken] = []
        offset = 0
        seen_market_ids: set[str] = set()
        now_ts = time.time()
        session = requests.Session()

        while True:
            resp = session.get(
                f"{GAMMA_BASE}/markets",
                params={
                    "limit": 100,
                    "offset": offset,
                    "active": "true",
                    "closed": "false",
                    "archived": "false",
                },
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list) or not payload:
                break

            for raw in payload:
                market_id = str(raw.get("id") or "")
                if not market_id or market_id in seen_market_ids:
                    continue
                token_ids = _load_json_list(raw.get("clobTokenIds", "[]"))
                outcomes = _load_json_list(raw.get("outcomes", "[]"))
                if len(token_ids) != 2 or len(outcomes) != 2:
                    continue
                start_iso = raw.get("startDate") or raw.get("startDateIso") or raw.get("gameStartTime")
                end_iso = raw.get("endDate") or raw.get("endDateIso")
                if not end_iso:
                    continue
                start_ts = parse_iso_to_ts(start_iso) if start_iso else None
                end_ts = parse_iso_to_ts(end_iso)
                if end_ts is None:
                    continue
                duration_seconds = int(end_ts - start_ts) if start_ts is not None else None
                if duration_seconds is None:
                    hours_to_close = raw.get("hours_to_close")
                    if hours_to_close is not None:
                        duration_seconds = int(float(hours_to_close) * 3600)
                if duration_seconds is None:
                    continue
                if duration_seconds < self.args.min_duration_seconds or duration_seconds > self.args.max_duration_seconds:
                    continue
                if end_ts <= now_ts:
                    continue
                condition_id = str(raw.get("conditionId") or market_id)
                fees_enabled = bool(raw.get("feesEnabled"))
                tick_size = _parse_float(raw.get("orderPriceMinTickSize"))
                fee_rate_bps = None
                fee_schedule = raw.get("feeSchedule") or raw.get("fee_schedule")
                if isinstance(fee_schedule, dict):
                    rate = _parse_float(fee_schedule.get("rate"))
                    if rate is not None:
                        fee_rate_bps = rate * 10000.0
                elif raw.get("takerBaseFee") is not None:
                    fee_rate_bps = _parse_float(raw.get("takerBaseFee"))
                question = str(raw.get("question") or "")
                for token_id, outcome in zip(token_ids, outcomes):
                    markets.append(
                        MarketToken(
                            market_id=market_id,
                            condition_id=condition_id,
                            question=question,
                            token_id=str(token_id),
                            outcome=str(outcome),
                            end_time_iso=str(end_iso),
                            start_time_iso=str(start_iso) if start_iso else None,
                            duration_seconds=duration_seconds,
                            fees_enabled=fees_enabled,
                            fee_rate_bps=fee_rate_bps,
                            tick_size=tick_size,
                        )
                    )
                seen_market_ids.add(market_id)

            offset += len(payload)
            if len(payload) < 100:
                break
            time.sleep(0.05)

        return markets

    async def refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self.args.refresh_interval_sec)
            try:
                latest = self.fetch_active_market_tokens()
                latest_by_id = {token.token_id: token for token in latest}
                new_token_ids = [tid for tid in latest_by_id if tid not in self.tokens_by_id]
                removed_token_ids = [tid for tid in self.tokens_by_id if tid not in latest_by_id]
                self.tokens_by_id = latest_by_id
                if new_token_ids or removed_token_ids:
                    self.logger.info(
                        "Eligible token refresh: +%s / -%s (now %s)",
                        len(new_token_ids),
                        len(removed_token_ids),
                        len(self.tokens_by_id),
                    )
            except Exception:
                self.logger.exception("Active market refresh failed")

    async def market_ws_loop(self) -> None:
        while True:
            asset_ids = list(self.tokens_by_id.keys())
            if not asset_ids:
                self.logger.warning("No asset ids to subscribe, retrying")
                await asyncio.sleep(5)
                continue
            try:
                async with websockets.connect(WS_MARKET_ENDPOINT, ping_interval=None, max_size=8_000_000) as ws:
                    await ws.send(json.dumps({
                        "assets_ids": asset_ids,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }))
                    self.logger.info("Subscribed to market ws with %s assets", len(asset_ids))
                    heartbeat = asyncio.create_task(self._ping_loop(ws))
                    try:
                        async for message in ws:
                            if isinstance(message, bytes):
                                continue
                            await self.handle_ws_message(message)
                    finally:
                        heartbeat.cancel()
                        await asyncio.gather(heartbeat, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("Market websocket loop crashed, reconnecting")
                await asyncio.sleep(3)

    async def _ping_loop(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(DEFAULT_PING_INTERVAL_SEC)
            await ws.send("PING")

    async def handle_ws_message(self, raw_message: str) -> None:
        if raw_message == "PONG":
            return
        payload = json.loads(raw_message)
        if isinstance(payload, list):
            for item in payload:
                await self.handle_event(item)
        elif isinstance(payload, dict):
            await self.handle_event(payload)

    async def handle_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("event_type")
        if event_type == "book":
            self._apply_book(event)
        elif event_type == "price_change":
            self._apply_price_change(event)
        elif event_type == "best_bid_ask":
            self._apply_best_bid_ask(event)
        elif event_type == "last_trade_price":
            return
        else:
            return

        asset_id = str(event.get("asset_id") or "")
        if not asset_id or asset_id not in self.tokens_by_id:
            return
        await self._detect_touches(asset_id)

    def _ensure_book(self, token_id: str, market_id: Optional[str] = None) -> BookState:
        book = self.books.get(token_id)
        if book is None:
            book = BookState(token_id=token_id, market_id=market_id)
            self.books[token_id] = book
        if market_id:
            book.market_id = market_id
        return book

    def _apply_book(self, event: dict[str, Any]) -> None:
        token_id = str(event.get("asset_id"))
        book = self._ensure_book(token_id, str(event.get("market") or ""))
        book.bid_levels = parse_levels(event.get("bids"), reverse=True)
        book.ask_levels = parse_levels(event.get("asks"), reverse=False)
        book.best_bid = book.bid_levels[0][0] if book.bid_levels else None
        book.best_ask = book.ask_levels[0][0] if book.ask_levels else None
        book.last_event_ts_ms = _parse_int(event.get("timestamp"))
        book.last_ingest_monotonic_ms = monotonic_ms()
        book.last_book_hash = str(event.get("hash") or "") or None

    def _apply_price_change(self, event: dict[str, Any]) -> None:
        changes = event.get("price_changes") or []
        event_ts = _parse_int(event.get("timestamp"))
        market_id = str(event.get("market") or "")
        for change in changes:
            token_id = str(change.get("asset_id") or "")
            if not token_id:
                continue
            book = self._ensure_book(token_id, market_id)
            side = str(change.get("side") or "").upper()
            price = _parse_float(change.get("price"))
            size = _parse_float(change.get("size"))
            if price is not None and size is not None:
                if side == "BUY":
                    book.bid_levels = upsert_level(book.bid_levels, price, size, reverse=True)
                elif side == "SELL":
                    book.ask_levels = upsert_level(book.ask_levels, price, size, reverse=False)
            best_bid = _parse_float(change.get("best_bid"))
            best_ask = _parse_float(change.get("best_ask"))
            if best_bid is not None:
                book.best_bid = best_bid
            elif book.bid_levels:
                book.best_bid = book.bid_levels[0][0]
            if best_ask is not None:
                book.best_ask = best_ask
            elif book.ask_levels:
                book.best_ask = book.ask_levels[0][0]
            book.last_event_ts_ms = event_ts
            book.last_ingest_monotonic_ms = monotonic_ms()

    def _apply_best_bid_ask(self, event: dict[str, Any]) -> None:
        token_id = str(event.get("asset_id"))
        book = self._ensure_book(token_id, str(event.get("market") or ""))
        book.best_bid = _parse_float(event.get("best_bid"))
        book.best_ask = _parse_float(event.get("best_ask"))
        book.last_event_ts_ms = _parse_int(event.get("timestamp"))
        book.last_ingest_monotonic_ms = monotonic_ms()

    async def _detect_touches(self, token_id: str) -> None:
        token = self.tokens_by_id.get(token_id)
        book = self.books.get(token_id)
        if token is None or book is None or book.best_ask is None:
            return
        best_ask = float(book.best_ask)
        for level in self.args.levels:
            key = (token_id, level)
            prev = self.last_best_ask.get(key)
            self.last_best_ask[key] = best_ask
            if prev is None:
                continue
            if prev < level <= best_ask:
                probe_id = f"{token_id}:{level:.2f}:{book.last_event_ts_ms or int(time.time()*1000)}"
                if probe_id in self.active_probes:
                    continue
                ask_levels = list(book.ask_levels[: self.args.depth_levels])
                ask_size = sum(size for price, size in ask_levels if abs(price - level) < 1e-9)
                fit = evaluate_notional_fit(ask_levels, self.args.notionals)
                probe = SurvivalProbe(
                    probe_id=probe_id,
                    token=token,
                    level=level,
                    started_at_ms=book.last_event_ts_ms or int(time.time() * 1000),
                    initial_best_ask=best_ask,
                    initial_ask_size_at_level=ask_size,
                    ask_levels_at_touch=ask_levels,
                    notional_fit=fit,
                )
                self.active_probes[probe_id] = probe
                self.logger.info(
                    "Touch detected token=%s outcome=%s level=%.2f best_ask=%.4f fit10=%s fit50=%s fit100=%s",
                    token.token_id,
                    token.outcome,
                    level,
                    best_ask,
                    fit.get("10"),
                    fit.get("50"),
                    fit.get("100"),
                )

    async def survival_loop(self) -> None:
        while True:
            await asyncio.sleep(self.args.sampling_interval_ms / 1000.0)
            now_ms = int(time.time() * 1000)
            to_finalize: list[tuple[SurvivalProbe, BookState]] = []
            for probe_id, probe in list(self.active_probes.items()):
                if probe.done:
                    continue
                book = self.books.get(probe.token.token_id)
                if book is None or book.best_ask is None:
                    continue
                elapsed = now_ms - probe.started_at_ms
                level_present = any(abs(price - probe.level) < 1e-9 and size > 0 for price, size in book.ask_levels)
                best_ask = float(book.best_ask)
                if not level_present:
                    probe.done = True
                    probe.end_reason = "level_removed"
                    probe.survived_ms = max(0, elapsed)
                elif best_ask > probe.level + 1e-9:
                    probe.done = True
                    probe.end_reason = "best_ask_moved_above_level"
                    probe.survived_ms = max(0, elapsed)
                elif elapsed >= self.args.survival_window_ms:
                    probe.done = True
                    probe.end_reason = "window_complete_level_still_present"
                    probe.survived_ms = elapsed
                if probe.done:
                    to_finalize.append((probe, book))
                    self.active_probes.pop(probe_id, None)
            for probe, book in to_finalize:
                self.csv.write_probe(probe, book)
                self.completed_probes += 1
                self.logger.info(
                    "Probe complete id=%s survived_ms=%s reason=%s completed=%s",
                    probe.probe_id,
                    probe.survived_ms,
                    probe.end_reason,
                    self.completed_probes,
                )
                if self.args.max_events and self.completed_probes >= self.args.max_events:
                    self.logger.info("Reached max_events=%s, stopping", self.args.max_events)
                    self.stop_event.set()
                    return

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop_event.set)
            except NotImplementedError:
                pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def iso_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


def parse_iso_to_ts(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _parse_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _load_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    try:
        return json.loads(value)
    except Exception:
        return []


def parse_levels(raw_levels: Any, *, reverse: bool) -> list[tuple[float, float]]:
    levels: list[tuple[float, float]] = []
    for item in raw_levels or []:
        if not isinstance(item, dict):
            continue
        price = _parse_float(item.get("price"))
        size = _parse_float(item.get("size"))
        if price is None or size is None:
            continue
        if size <= 0:
            continue
        levels.append((price, size))
    levels.sort(key=lambda x: x[0], reverse=reverse)
    return levels


def upsert_level(levels: list[tuple[float, float]], price: float, size: float, *, reverse: bool) -> list[tuple[float, float]]:
    out = [(p, s) for p, s in levels if abs(p - price) >= 1e-9]
    if size > 0:
        out.append((price, size))
    out.sort(key=lambda x: x[0], reverse=reverse)
    return out


def evaluate_notional_fit(ask_levels: list[tuple[float, float]], notionals: tuple[float, ...]) -> dict[str, str]:
    results: dict[str, str] = {}
    for notional in notionals:
        remaining = notional
        worst_ticks = None
        if not ask_levels:
            results[str(int(notional))] = "no_book"
            continue
        best_ask = ask_levels[0][0]
        for price, size in ask_levels:
            level_notional = price * size
            take = min(level_notional, remaining)
            if take > 0:
                ticks = round((price - best_ask) / 0.01)
                worst_ticks = ticks if worst_ticks is None else max(worst_ticks, ticks)
                remaining -= take
            if remaining <= 1e-9:
                break
        if remaining > 1e-9:
            results[str(int(notional))] = "insufficient_depth"
        elif worst_ticks is None or worst_ticks <= 0:
            results[str(int(notional))] = "+0_tick"
        elif worst_ticks == 1:
            results[str(int(notional))] = "+1_tick"
        else:
            results[str(int(notional))] = "+2plus_ticks"
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-csv", default="v2/short_horizon/docs/phase0/live_depth_survival.csv")
    parser.add_argument("--levels", nargs="*", type=float, default=list(DEFAULT_LEVELS))
    parser.add_argument("--notionals", nargs="*", type=float, default=list(DEFAULT_NOTIONALS))
    parser.add_argument("--depth-levels", type=int, default=DEFAULT_DEPTH_LEVELS)
    parser.add_argument("--survival-window-ms", type=int, default=DEFAULT_SURVIVAL_WINDOW_MS)
    parser.add_argument("--sampling-interval-ms", type=int, default=DEFAULT_SAMPLING_INTERVAL_MS)
    parser.add_argument("--refresh-interval-sec", type=int, default=DEFAULT_REFRESH_INTERVAL_SEC)
    parser.add_argument("--min-duration-seconds", type=int, default=DEFAULT_DURATION_MIN)
    parser.add_argument("--max-duration-seconds", type=int, default=DEFAULT_DURATION_MAX)
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--log-level", default="INFO")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.levels = tuple(float(x) for x in args.levels)
    args.notionals = tuple(float(x) for x in args.notionals)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    collector = LiveDepthCollector(args)
    asyncio.run(collector.run())


if __name__ == "__main__":
    main()
