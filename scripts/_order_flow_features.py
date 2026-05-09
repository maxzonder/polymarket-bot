from __future__ import annotations

import glob
import json
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

DEFAULT_CAPTURE_GLOB = "/home/polybot/.polybot/capture_bundles/*/events_log.jsonl"
DEFAULT_PRE_WINDOWS_MS = (1_000, 5_000, 15_000, 30_000)
DEFAULT_POST_WINDOWS_MS = (1_000, 5_000)


@dataclass(frozen=True)
class TouchRow:
    probe_id: str
    market_id: str
    token_id: str
    asset_slug: str
    direction: str
    touch_level: float
    touch_time_iso: str
    touch_ms: int
    lifecycle_fraction: float | None
    resolves_yes: int
    fee_rate_bps: float | None = None
    best_ask_at_touch: float | None = None
    best_bid_at_touch: float | None = None
    tick_size: float | None = None


@dataclass(frozen=True)
class BookPoint:
    ts_ms: int
    market_id: str
    token_id: str
    best_bid: float | None
    best_ask: float | None
    bid_levels: tuple[tuple[float, float], ...] = ()
    ask_levels: tuple[tuple[float, float], ...] = ()

    @property
    def spread(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def mid(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0


@dataclass(frozen=True)
class TradePoint:
    ts_ms: int
    market_id: str
    token_id: str
    price: float
    size: float
    aggressor_side: str | None


@dataclass(frozen=True)
class TradeWindowStats:
    window_ms: int
    buy_trade_count: int
    sell_trade_count: int
    unknown_trade_count: int
    buy_size: float
    sell_size: float
    unknown_size: float
    total_size: float
    signed_size: float
    buy_share: float | None
    last_trade_side: str | None
    last_trade_price: float | None
    last_trade_delta_ms: int | None


@dataclass(frozen=True)
class BookWindowStats:
    window_ms: int
    book_count: int
    bid_delta: float | None
    ask_delta: float | None
    mid_delta: float | None


@dataclass(frozen=True)
class PostBookStats:
    window_ms: int
    book_count: int
    ask_reverts_below_level: bool | None
    ask_stays_ge_level: bool | None


@dataclass(frozen=True)
class BundleEvents:
    bundle_name: str
    path: Path
    books_by_token: dict[str, list[BookPoint]]
    trades_by_token: dict[str, list[TradePoint]]
    has_l2_book_levels: bool
    min_ts_ms: int | None
    max_ts_ms: int | None


@dataclass(frozen=True)
class OrderFlowFeature:
    bundle_name: str
    probe_id: str
    market_id: str
    token_id: str
    asset_slug: str
    direction: str
    touch_level: float
    touch_time_iso: str
    touch_ms: int
    lifecycle_fraction: float | None
    resolves_yes: int
    fee_rate_bps: float | None
    pre_book_n: int
    post_book_n: int
    pre_trade_n: int
    post_trade_n: int
    first_book_delta_ms: int | None
    last_pre_book_delta_ms: int | None
    first_post_book_delta_ms: int | None
    has_l2_levels: bool
    best_bid_pre: float | None
    best_ask_pre: float | None
    best_bid_at_or_after_touch: float | None
    best_ask_at_or_after_touch: float | None
    spread_pre: float | None
    spread_at: float | None
    mid_pre: float | None
    mid_at: float | None
    trade_windows: dict[int, TradeWindowStats] = field(default_factory=dict)
    book_windows: dict[int, BookWindowStats] = field(default_factory=dict)
    post_windows: dict[int, PostBookStats] = field(default_factory=dict)


def parse_ts_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return None
        # Some producers may emit seconds; normalized capture emits milliseconds.
        if 0 < number < 10_000_000_000:
            number *= 1000.0
        return int(number)
    text = str(value).strip()
    if not text:
        return None
    try:
        numeric = float(text)
    except ValueError:
        numeric = None
    if numeric is not None:
        return parse_ts_ms(numeric)
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None


def load_touch_rows(path: str | Path, *, asset_allowlist: Iterable[str] | None = None) -> list[TouchRow]:
    path = Path(path)
    allowed_assets = _normalize_set(asset_allowlist)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        _require_table(conn, "touch_dataset", path)
        cols = _table_columns(conn, "touch_dataset")
        required = {
            "probe_id",
            "market_id",
            "token_id",
            "asset_slug",
            "direction",
            "touch_level",
            "touch_time_iso",
            "lifecycle_fraction",
            "resolves_yes",
        }
        missing = sorted(required - cols)
        if missing:
            raise ValueError(f"touch_dataset missing required columns: {missing}")
        optional = {
            "fee_rate_bps": "NULL AS fee_rate_bps",
            "best_ask_at_touch": "NULL AS best_ask_at_touch",
            "best_bid_at_touch": "NULL AS best_bid_at_touch",
            "tick_size": "NULL AS tick_size",
        }
        select_optional = [name if name in cols else expr for name, expr in optional.items()]
        rows = conn.execute(
            f"""
            SELECT probe_id, market_id, token_id, asset_slug, direction, touch_level,
                   touch_time_iso, lifecycle_fraction, resolves_yes,
                   {', '.join(select_optional)}
            FROM touch_dataset
            ORDER BY touch_time_iso, probe_id
            """
        ).fetchall()
        touches: list[TouchRow] = []
        for row in rows:
            asset = str(row["asset_slug"] or "").lower()
            if allowed_assets is not None and asset not in allowed_assets:
                continue
            touch_ms = parse_ts_ms(row["touch_time_iso"])
            if touch_ms is None:
                continue
            touches.append(
                TouchRow(
                    probe_id=str(row["probe_id"]),
                    market_id=str(row["market_id"]),
                    token_id=str(row["token_id"]),
                    asset_slug=asset,
                    direction=str(row["direction"] or ""),
                    touch_level=float(row["touch_level"]),
                    touch_time_iso=str(row["touch_time_iso"]),
                    touch_ms=touch_ms,
                    lifecycle_fraction=_float_or_none(row["lifecycle_fraction"]),
                    resolves_yes=int(row["resolves_yes"] or 0),
                    fee_rate_bps=_float_or_none(row["fee_rate_bps"]),
                    best_ask_at_touch=_float_or_none(row["best_ask_at_touch"]),
                    best_bid_at_touch=_float_or_none(row["best_bid_at_touch"]),
                    tick_size=_float_or_none(row["tick_size"]),
                )
            )
        return touches
    finally:
        conn.close()


def load_bundle_events(path: str | Path) -> BundleEvents:
    path = Path(path)
    books_by_token: dict[str, list[BookPoint]] = {}
    trades_by_token: dict[str, list[TradePoint]] = {}
    min_ts: int | None = None
    max_ts: int | None = None
    has_l2 = False
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            raw = raw.strip()
            if not raw:
                continue
            payload = json.loads(raw)
            ts_ms = parse_ts_ms(payload.get("event_time_ms", payload.get("event_time")))
            if ts_ms is None:
                continue
            min_ts = ts_ms if min_ts is None else min(min_ts, ts_ms)
            max_ts = ts_ms if max_ts is None else max(max_ts, ts_ms)
            token_id = str(payload.get("token_id") or "")
            if not token_id:
                continue
            market_id = str(payload.get("market_id") or "")
            event_type = payload.get("event_type")
            if event_type == "BookUpdate":
                bid_levels = _parse_levels(payload.get("bid_levels"))
                ask_levels = _parse_levels(payload.get("ask_levels"))
                has_l2 = has_l2 or bool(bid_levels or ask_levels)
                books_by_token.setdefault(token_id, []).append(
                    BookPoint(
                        ts_ms=ts_ms,
                        market_id=market_id,
                        token_id=token_id,
                        best_bid=_float_or_none(payload.get("best_bid")),
                        best_ask=_float_or_none(payload.get("best_ask")),
                        bid_levels=bid_levels,
                        ask_levels=ask_levels,
                    )
                )
            elif event_type == "TradeTick":
                price = _float_or_none(payload.get("price"))
                size = _float_or_none(payload.get("size"))
                if price is None or size is None:
                    continue
                side = payload.get("aggressor_side")
                trades_by_token.setdefault(token_id, []).append(
                    TradePoint(
                        ts_ms=ts_ms,
                        market_id=market_id,
                        token_id=token_id,
                        price=price,
                        size=size,
                        aggressor_side=(str(side).lower() if side is not None and str(side).strip() else None),
                    )
                )
    for books in books_by_token.values():
        books.sort(key=lambda item: item.ts_ms)
    for trades in trades_by_token.values():
        trades.sort(key=lambda item: item.ts_ms)
    return BundleEvents(
        bundle_name=path.parent.name,
        path=path,
        books_by_token=books_by_token,
        trades_by_token=trades_by_token,
        has_l2_book_levels=has_l2,
        min_ts_ms=min_ts,
        max_ts_ms=max_ts,
    )


def build_order_flow_features(
    touch_db: str | Path,
    capture_glob: str = DEFAULT_CAPTURE_GLOB,
    *,
    pre_windows_ms: Sequence[int] = DEFAULT_PRE_WINDOWS_MS,
    post_windows_ms: Sequence[int] = DEFAULT_POST_WINDOWS_MS,
    asset_allowlist: Iterable[str] | None = None,
) -> list[OrderFlowFeature]:
    touches = load_touch_rows(touch_db, asset_allowlist=asset_allowlist)
    if not touches:
        return []
    max_pre_ms = max(pre_windows_ms, default=0)
    max_post_ms = max(post_windows_ms, default=0)
    features: list[OrderFlowFeature] = []
    paths = sorted(glob.glob(capture_glob))
    for event_path in paths:
        bundle = load_bundle_events(event_path)
        if bundle.min_ts_ms is None or bundle.max_ts_ms is None:
            continue
        for touch in touches:
            if touch.token_id not in bundle.books_by_token and touch.token_id not in bundle.trades_by_token:
                continue
            if touch.touch_ms < bundle.min_ts_ms - max_pre_ms or touch.touch_ms > bundle.max_ts_ms + max_post_ms:
                continue
            features.append(
                compute_feature_for_touch(
                    touch,
                    bundle,
                    pre_windows_ms=pre_windows_ms,
                    post_windows_ms=post_windows_ms,
                )
            )
    return features


def compute_feature_for_touch(
    touch: TouchRow,
    bundle: BundleEvents,
    *,
    pre_windows_ms: Sequence[int] = DEFAULT_PRE_WINDOWS_MS,
    post_windows_ms: Sequence[int] = DEFAULT_POST_WINDOWS_MS,
) -> OrderFlowFeature:
    books = bundle.books_by_token.get(touch.token_id, [])
    trades = bundle.trades_by_token.get(touch.token_id, [])
    pre_books = [book for book in books if book.ts_ms <= touch.touch_ms]
    post_books = [book for book in books if book.ts_ms >= touch.touch_ms]
    pre_trades = [trade for trade in trades if trade.ts_ms <= touch.touch_ms]
    post_trades = [trade for trade in trades if trade.ts_ms >= touch.touch_ms]
    first_book = books[0] if books else None
    last_pre_book = pre_books[-1] if pre_books else None
    first_post_book = post_books[0] if post_books else None
    trade_windows = {
        int(window): trade_window_stats(trades, touch.touch_ms, int(window))
        for window in pre_windows_ms
    }
    book_windows = {
        int(window): book_window_stats(books, touch.touch_ms, int(window))
        for window in pre_windows_ms
    }
    post_windows = {
        int(window): post_book_stats(books, touch.touch_ms, int(window), touch_level=touch.touch_level)
        for window in post_windows_ms
    }
    return OrderFlowFeature(
        bundle_name=bundle.bundle_name,
        probe_id=touch.probe_id,
        market_id=touch.market_id,
        token_id=touch.token_id,
        asset_slug=touch.asset_slug,
        direction=touch.direction,
        touch_level=touch.touch_level,
        touch_time_iso=touch.touch_time_iso,
        touch_ms=touch.touch_ms,
        lifecycle_fraction=touch.lifecycle_fraction,
        resolves_yes=touch.resolves_yes,
        fee_rate_bps=touch.fee_rate_bps,
        pre_book_n=len(pre_books),
        post_book_n=len(post_books),
        pre_trade_n=len(pre_trades),
        post_trade_n=len(post_trades),
        first_book_delta_ms=(first_book.ts_ms - touch.touch_ms) if first_book else None,
        last_pre_book_delta_ms=(last_pre_book.ts_ms - touch.touch_ms) if last_pre_book else None,
        first_post_book_delta_ms=(first_post_book.ts_ms - touch.touch_ms) if first_post_book else None,
        has_l2_levels=bundle.has_l2_book_levels,
        best_bid_pre=last_pre_book.best_bid if last_pre_book else None,
        best_ask_pre=last_pre_book.best_ask if last_pre_book else None,
        best_bid_at_or_after_touch=first_post_book.best_bid if first_post_book else None,
        best_ask_at_or_after_touch=first_post_book.best_ask if first_post_book else None,
        spread_pre=last_pre_book.spread if last_pre_book else None,
        spread_at=first_post_book.spread if first_post_book else None,
        mid_pre=last_pre_book.mid if last_pre_book else None,
        mid_at=first_post_book.mid if first_post_book else None,
        trade_windows=trade_windows,
        book_windows=book_windows,
        post_windows=post_windows,
    )


def trade_window_stats(trades: Sequence[TradePoint], touch_ms: int, window_ms: int) -> TradeWindowStats:
    start_ms = touch_ms - window_ms
    selected = [trade for trade in trades if start_ms <= trade.ts_ms <= touch_ms]
    buy_count = sell_count = unknown_count = 0
    buy_size = sell_size = unknown_size = 0.0
    for trade in selected:
        side = (trade.aggressor_side or "").lower()
        if side == "buy":
            buy_count += 1
            buy_size += trade.size
        elif side == "sell":
            sell_count += 1
            sell_size += trade.size
        else:
            unknown_count += 1
            unknown_size += trade.size
    total_size = buy_size + sell_size + unknown_size
    known_total = buy_size + sell_size
    last = selected[-1] if selected else None
    return TradeWindowStats(
        window_ms=window_ms,
        buy_trade_count=buy_count,
        sell_trade_count=sell_count,
        unknown_trade_count=unknown_count,
        buy_size=buy_size,
        sell_size=sell_size,
        unknown_size=unknown_size,
        total_size=total_size,
        signed_size=buy_size - sell_size,
        buy_share=(buy_size / known_total) if known_total > 0 else None,
        last_trade_side=last.aggressor_side if last else None,
        last_trade_price=last.price if last else None,
        last_trade_delta_ms=(last.ts_ms - touch_ms) if last else None,
    )


def book_window_stats(books: Sequence[BookPoint], touch_ms: int, window_ms: int) -> BookWindowStats:
    start_ms = touch_ms - window_ms
    selected = [book for book in books if start_ms <= book.ts_ms <= touch_ms]
    if not selected:
        return BookWindowStats(window_ms, 0, None, None, None)
    first = selected[0]
    last = selected[-1]
    return BookWindowStats(
        window_ms=window_ms,
        book_count=len(selected),
        bid_delta=_delta(last.best_bid, first.best_bid),
        ask_delta=_delta(last.best_ask, first.best_ask),
        mid_delta=_delta(last.mid, first.mid),
    )


def post_book_stats(books: Sequence[BookPoint], touch_ms: int, window_ms: int, *, touch_level: float) -> PostBookStats:
    end_ms = touch_ms + window_ms
    selected = [book for book in books if touch_ms <= book.ts_ms <= end_ms]
    asks = [book.best_ask for book in selected if book.best_ask is not None]
    if not asks:
        return PostBookStats(window_ms, len(selected), None, None)
    return PostBookStats(
        window_ms=window_ms,
        book_count=len(selected),
        ask_reverts_below_level=any(ask < touch_level - 1e-9 for ask in asks),
        ask_stays_ge_level=all(ask >= touch_level - 1e-9 for ask in asks),
    )


def _parse_levels(value: Any) -> tuple[tuple[float, float], ...]:
    if not isinstance(value, list):
        return ()
    levels: list[tuple[float, float]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        price = _float_or_none(item.get("price"))
        size = _float_or_none(item.get("size"))
        if price is not None and size is not None:
            levels.append((price, size))
    return tuple(levels)


def _delta(new: float | None, old: float | None) -> float | None:
    if new is None or old is None:
        return None
    return new - old


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _require_table(conn: sqlite3.Connection, table: str, path: Path) -> None:
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is None:
        raise ValueError(f"{path} does not contain table {table}")


def _normalize_set(values: Iterable[str] | None) -> set[str] | None:
    if values is None:
        return None
    normalized = {str(value).strip().lower() for value in values if str(value).strip()}
    return normalized or None
