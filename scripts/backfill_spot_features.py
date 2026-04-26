#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_TOUCH_DATASET_PATH = Path("v2/short_horizon/data/touch_dataset.sqlite3")
DEFAULT_OUTPUT_PATH = Path("v2/short_horizon/data/spot_features.sqlite3")
DEFAULT_CACHE_PATH = Path("v2/short_horizon/data/spot_ohlc_cache.sqlite3")
BINANCE_BASE = "https://api.binance.com"
COINGECKO_BASE = "https://api.coingecko.com"
CRYPTOCOMPARE_BASE = "https://min-api.cryptocompare.com"
DEFAULT_LOOKBACK_SECONDS = 300
DEFAULT_BATCH_SECONDS = 1000
DEFAULT_SLEEP_SECONDS = 0.05

ASSET_SYMBOLS = {
    "btc": "BTCUSDT",
    "bitcoin": "BTCUSDT",
    "eth": "ETHUSDT",
    "ethereum": "ETHUSDT",
    "bnb": "BNBUSDT",
    "doge": "DOGEUSDT",
    "dogecoin": "DOGEUSDT",
    "xrp": "XRPUSDT",
    "sol": "SOLUSDT",
    "solana": "SOLUSDT",
    # Binance spot/alpha did not expose HYPE during P6-2b validation. Use
    # CryptoCompare minute bars as a coarse fallback so the full touch dataset
    # remains joinable; latency is reported per asset in the summary.
    "hyperliquid": "CRYPTOCOMPARE:HYPE",
    "hype": "CRYPTOCOMPARE:HYPE",
}

SPOT_FEATURE_COLUMNS: list[tuple[str, str]] = [
    ("probe_id", "TEXT PRIMARY KEY"),
    ("asset_slug", "TEXT NOT NULL"),
    ("spot_symbol", "TEXT NOT NULL"),
    ("touch_time_iso", "TEXT NOT NULL"),
    ("nearest_spot_bar_time_iso", "TEXT"),
    ("nearest_spot_latency_ms", "REAL"),
    ("start_time_iso", "TEXT"),
    ("nearest_start_bar_time_iso", "TEXT"),
    ("nearest_start_latency_ms", "REAL"),
    ("end_time_iso", "TEXT"),
    ("direction", "TEXT"),
    ("best_ask_at_touch", "REAL"),
    ("spot_at_touch", "REAL"),
    ("spot_at_window_start", "REAL"),
    ("spot_return_since_window_start", "REAL"),
    ("spot_realized_vol_recent_60s", "REAL"),
    ("spot_realized_vol_recent_300s", "REAL"),
    ("spot_velocity_recent_30s", "REAL"),
    ("seconds_to_resolution", "REAL"),
    ("spot_implied_prob", "REAL"),
    ("spot_implied_prob_minus_market_prob", "REAL"),
    ("source", "TEXT NOT NULL"),
]


@dataclass(frozen=True)
class SpotBar:
    asset_slug: str
    symbol: str
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    source: str = "binance.klines_1s"


@dataclass(frozen=True)
class SpotFeatureSummary:
    touch_dataset_path: str
    output_path: str
    cache_path: str
    input_rows: int
    output_rows: int
    skipped_unsupported_asset: int
    skipped_missing_spot: int
    coverage: float | None
    by_asset: dict[str, dict[str, Any]]

    def as_json(self) -> dict[str, Any]:
        return asdict(self)


def build_spot_features(
    touch_dataset_path: str | Path,
    *,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    cache_path: str | Path = DEFAULT_CACHE_PATH,
    binance_base: str = BINANCE_BASE,
    coingecko_base: str = COINGECKO_BASE,
    cryptocompare_base: str = CRYPTOCOMPARE_BASE,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
    fetch_klines: Callable[[str, int, int], list[SpotBar]] | None = None,
    fetch_coingecko_prices: Callable[[str, int, int], list[SpotBar]] | None = None,
    fetch_cryptocompare_minutes: Callable[[str, int, int], list[SpotBar]] | None = None,
    asset_symbols: dict[str, str] | None = None,
) -> SpotFeatureSummary:
    touch_dataset_path = Path(touch_dataset_path)
    output_path = Path(output_path)
    cache_path = Path(cache_path)
    symbols = {**ASSET_SYMBOLS, **(asset_symbols or {})}
    rows = load_touch_rows(touch_dataset_path)
    cache_conn = sqlite3.connect(cache_path)
    cache_conn.row_factory = sqlite3.Row
    _ensure_cache_schema(cache_conn)
    output_conn = sqlite3.connect(output_path)
    output_conn.row_factory = sqlite3.Row
    _reset_output_schema(output_conn)

    fetcher = fetch_klines or (lambda symbol, start_ms, end_ms: fetch_binance_klines(symbol, start_ms, end_ms, binance_base=binance_base))
    coingecko_fetcher = fetch_coingecko_prices or (
        lambda coin_id, start_ms, end_ms: fetch_coingecko_market_chart_range(coin_id, start_ms, end_ms, coingecko_base=coingecko_base)
    )
    cryptocompare_fetcher = fetch_cryptocompare_minutes or (
        lambda fsym, start_ms, end_ms: fetch_cryptocompare_histominute(fsym, start_ms, end_ms, cryptocompare_base=cryptocompare_base)
    )
    _prefetch_needed_bars(
        rows,
        cache_conn,
        symbols=symbols,
        sleep_seconds=sleep_seconds,
        fetch_klines=fetcher,
        fetch_coingecko_prices=coingecko_fetcher,
        fetch_cryptocompare_minutes=cryptocompare_fetcher,
    )

    input_rows = len(rows)
    output_rows = 0
    skipped_unsupported_asset = 0
    skipped_missing_spot = 0
    by_asset: dict[str, dict[str, Any]] = defaultdict(lambda: {"input": 0, "output": 0, "unsupported": 0, "missing_spot": 0, "median_touch_latency_ms": None})
    latencies_by_asset: dict[str, list[float]] = defaultdict(list)

    for row in rows:
        asset = str(row["asset_slug"] or "").lower()
        by_asset[asset]["input"] += 1
        symbol = symbols.get(asset)
        if not symbol:
            skipped_unsupported_asset += 1
            by_asset[asset]["unsupported"] += 1
            continue
        feature = _feature_for_touch(cache_conn, row, symbol=symbol)
        if feature is None:
            skipped_missing_spot += 1
            by_asset[asset]["missing_spot"] += 1
            continue
        _insert_feature(output_conn, feature)
        output_rows += 1
        by_asset[asset]["output"] += 1
        latency = feature.get("nearest_spot_latency_ms")
        if latency is not None:
            latencies_by_asset[asset].append(float(latency))

    output_conn.commit()
    output_conn.execute("CREATE INDEX IF NOT EXISTS idx_spot_features_asset_time ON spot_features(asset_slug, touch_time_iso)")
    output_conn.commit()
    for asset, latencies in latencies_by_asset.items():
        by_asset[asset]["median_touch_latency_ms"] = _median(latencies)
    output_conn.close()
    cache_conn.close()
    return SpotFeatureSummary(
        touch_dataset_path=str(touch_dataset_path),
        output_path=str(output_path),
        cache_path=str(cache_path),
        input_rows=input_rows,
        output_rows=output_rows,
        skipped_unsupported_asset=skipped_unsupported_asset,
        skipped_missing_spot=skipped_missing_spot,
        coverage=(output_rows / input_rows) if input_rows else None,
        by_asset=dict(sorted(by_asset.items())),
    )


def load_touch_rows(path: str | Path) -> list[sqlite3.Row]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        _require_table(conn, "touch_dataset", path)
        return list(
            conn.execute(
                """
                SELECT probe_id, asset_slug, direction, touch_time_iso, start_time_iso, end_time_iso,
                       best_ask_at_touch
                FROM touch_dataset
                ORDER BY touch_time_iso, probe_id
                """
            )
        )
    finally:
        conn.close()


def fetch_binance_klines(
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    binance_base: str = BINANCE_BASE,
    timeout_seconds: float = 30.0,
) -> list[SpotBar]:
    params = {
        "symbol": symbol,
        "interval": "1s",
        "startTime": str(int(start_ms)),
        "endTime": str(int(end_ms)),
        "limit": str(DEFAULT_BATCH_SECONDS),
    }
    url = f"{binance_base.rstrip('/')}/api/v3/klines?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "polymarket-bot-p6-2b-spot-backfill/1.0"})
    with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - fixed HTTPS default endpoint, configurable for tests.
        payload = json.loads(response.read().decode("utf-8"))
    bars: list[SpotBar] = []
    for item in payload:
        if not isinstance(item, list) or len(item) < 6:
            continue
        bars.append(
            SpotBar(
                asset_slug="",
                symbol=symbol,
                open_time_ms=int(item[0]),
                open=float(item[1]),
                high=float(item[2]),
                low=float(item[3]),
                close=float(item[4]),
                volume=float(item[5]),
            )
        )
    return bars


def fetch_coingecko_market_chart_range(
    coin_id: str,
    start_ms: int,
    end_ms: int,
    *,
    coingecko_base: str = COINGECKO_BASE,
    timeout_seconds: float = 30.0,
) -> list[SpotBar]:
    params = {
        "vs_currency": "usd",
        "from": str(int(start_ms // 1000)),
        "to": str(int(end_ms // 1000)),
    }
    url = f"{coingecko_base.rstrip('/')}/api/v3/coins/{coin_id}/market_chart/range?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "polymarket-bot-p6-2b-spot-backfill/1.0"})
    with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - fixed HTTPS default endpoint, configurable for tests.
        payload = json.loads(response.read().decode("utf-8"))
    bars: list[SpotBar] = []
    for item in payload.get("prices", []):
        if not isinstance(item, list) or len(item) < 2:
            continue
        ts = int(item[0])
        price = float(item[1])
        bars.append(
            SpotBar(
                asset_slug="",
                symbol=f"COINGECKO:{coin_id}",
                open_time_ms=ts,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=None,
                source="coingecko.market_chart_range",
            )
        )
    return bars


def fetch_cryptocompare_histominute(
    fsym: str,
    start_ms: int,
    end_ms: int,
    *,
    cryptocompare_base: str = CRYPTOCOMPARE_BASE,
    timeout_seconds: float = 30.0,
) -> list[SpotBar]:
    # CryptoCompare histominute returns data backward from toTs. Page from the
    # end of the requested range until the start is covered.
    out: dict[int, SpotBar] = {}
    cursor_s = int(end_ms // 1000)
    start_s = int(start_ms // 1000)
    while cursor_s >= start_s:
        params = {
            "fsym": fsym,
            "tsym": "USD",
            "limit": "2000",
            "toTs": str(cursor_s),
        }
        url = f"{cryptocompare_base.rstrip('/')}/data/v2/histominute?{urlencode(params)}"
        request = Request(url, headers={"User-Agent": "polymarket-bot-p6-2b-spot-backfill/1.0"})
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - fixed HTTPS default endpoint, configurable for tests.
            payload = json.loads(response.read().decode("utf-8"))
        rows = payload.get("Data", {}).get("Data", [])
        if not rows:
            break
        min_seen = cursor_s
        for item in rows:
            ts_s = int(item.get("time", 0))
            min_seen = min(min_seen, ts_s)
            ts_ms = ts_s * 1000
            if ts_ms < start_ms or ts_ms > end_ms:
                continue
            close = float(item.get("close") or 0.0)
            if close <= 0:
                continue
            out[ts_ms] = SpotBar(
                asset_slug="",
                symbol=f"CRYPTOCOMPARE:{fsym}",
                open_time_ms=ts_ms,
                open=float(item.get("open") or close),
                high=float(item.get("high") or close),
                low=float(item.get("low") or close),
                close=close,
                volume=_float_or_none(item.get("volumefrom")),
                source="cryptocompare.histominute",
            )
        next_cursor = min_seen - 60
        if next_cursor >= cursor_s:
            break
        cursor_s = next_cursor
    return [out[key] for key in sorted(out)]


def _prefetch_needed_bars(
    rows: list[sqlite3.Row],
    conn: sqlite3.Connection,
    *,
    symbols: dict[str, str],
    sleep_seconds: float,
    fetch_klines: Callable[[str, int, int], list[SpotBar]],
    fetch_coingecko_prices: Callable[[str, int, int], list[SpotBar]],
    fetch_cryptocompare_minutes: Callable[[str, int, int], list[SpotBar]],
) -> None:
    ranges: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
    for row in rows:
        asset = str(row["asset_slug"] or "").lower()
        symbol = symbols.get(asset)
        if not symbol:
            continue
        times = [_parse_iso_ms(row["touch_time_iso"]), _parse_iso_ms(row["start_time_iso"]), _parse_iso_ms(row["end_time_iso"])]
        present = [value for value in times if value is not None]
        if not present:
            continue
        start = min(present) - (DEFAULT_LOOKBACK_SECONDS * 1000)
        end = max(present) + 1000
        ranges[(asset, symbol)].append((start, end))

    for (asset, symbol), items in ranges.items():
        for start_ms, end_ms in _merge_ranges(items):
            cursor = start_ms
            if symbol.startswith("CRYPTOCOMPARE:"):
                if not _coarse_range_is_cached(conn, asset, symbol, start_ms, end_ms, interval_ms=60_000):
                    fsym = symbol.split(":", 1)[1]
                    bars = fetch_cryptocompare_minutes(fsym, start_ms, end_ms)
                    _insert_bars(
                        conn,
                        (
                            SpotBar(
                                asset_slug=asset,
                                symbol=symbol,
                                open_time_ms=bar.open_time_ms,
                                open=bar.open,
                                high=bar.high,
                                low=bar.low,
                                close=bar.close,
                                volume=bar.volume,
                                source=bar.source,
                            )
                            for bar in bars
                        ),
                    )
                    conn.commit()
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
                continue
            if symbol.startswith("COINGECKO:"):
                # CoinGecko returns coarse historical samples for old ranges. Cache the
                # whole merged range in one call and rely on latency reporting downstream.
                if not _range_has_any_cache(conn, asset, symbol, start_ms, end_ms):
                    coin_id = symbol.split(":", 1)[1]
                    bars = fetch_coingecko_prices(coin_id, start_ms, end_ms)
                    _insert_bars(
                        conn,
                        (
                            SpotBar(
                                asset_slug=asset,
                                symbol=symbol,
                                open_time_ms=bar.open_time_ms,
                                open=bar.open,
                                high=bar.high,
                                low=bar.low,
                                close=bar.close,
                                volume=bar.volume,
                                source=bar.source,
                            )
                            for bar in bars
                        ),
                    )
                    conn.commit()
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
                continue
            while cursor <= end_ms:
                batch_end = min(end_ms, cursor + (DEFAULT_BATCH_SECONDS - 1) * 1000)
                if not _range_is_cached(conn, asset, symbol, cursor, batch_end):
                    bars = fetch_klines(symbol, cursor, batch_end)
                    _insert_bars(conn, (SpotBar(asset_slug=asset, symbol=symbol, open_time_ms=bar.open_time_ms, open=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume, source=bar.source) for bar in bars))
                    conn.commit()
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
                cursor = batch_end + 1000


def _feature_for_touch(conn: sqlite3.Connection, row: sqlite3.Row, *, symbol: str) -> dict[str, Any] | None:
    asset = str(row["asset_slug"] or "").lower()
    touch_ms = _parse_iso_ms(row["touch_time_iso"])
    start_ms = _parse_iso_ms(row["start_time_iso"])
    end_ms = _parse_iso_ms(row["end_time_iso"])
    if touch_ms is None or start_ms is None or end_ms is None:
        return None
    max_latency_ms = 120_000 if symbol.startswith("CRYPTOCOMPARE:") else 7_200_000 if symbol.startswith("COINGECKO:") else 5000
    touch_bar = _nearest_bar(conn, asset, symbol, touch_ms, max_latency_ms=max_latency_ms)
    start_bar = _nearest_bar(conn, asset, symbol, start_ms, max_latency_ms=max_latency_ms)
    bar_30s = _nearest_bar(conn, asset, symbol, touch_ms - 30_000, max_latency_ms=max_latency_ms)
    if touch_bar is None or start_bar is None or bar_30s is None:
        return None
    recent_60 = _bars_between(conn, asset, symbol, touch_ms - 60_000, touch_ms)
    recent_300 = _bars_between(conn, asset, symbol, touch_ms - 300_000, touch_ms)
    spot_at_touch = float(touch_bar["close"])
    spot_at_start = float(start_bar["close"])
    vol_60 = _realized_vol(recent_60)
    vol_300 = _realized_vol(recent_300)
    seconds_to_resolution = max(0.0, (end_ms - touch_ms) / 1000.0)
    spot_implied_prob = _spot_implied_prob(
        spot=spot_at_touch,
        strike=spot_at_start,
        sigma_per_sqrt_second=vol_300,
        seconds_to_resolution=seconds_to_resolution,
        direction=str(row["direction"] or ""),
    )
    best_ask = _float_or_none(row["best_ask_at_touch"])
    return {
        "probe_id": row["probe_id"],
        "asset_slug": asset,
        "spot_symbol": symbol,
        "touch_time_iso": row["touch_time_iso"],
        "nearest_spot_bar_time_iso": _iso_from_ms(int(touch_bar["open_time_ms"])),
        "nearest_spot_latency_ms": abs(int(touch_bar["open_time_ms"]) - touch_ms),
        "start_time_iso": row["start_time_iso"],
        "nearest_start_bar_time_iso": _iso_from_ms(int(start_bar["open_time_ms"])),
        "nearest_start_latency_ms": abs(int(start_bar["open_time_ms"]) - start_ms),
        "end_time_iso": row["end_time_iso"],
        "direction": row["direction"],
        "best_ask_at_touch": best_ask,
        "spot_at_touch": spot_at_touch,
        "spot_at_window_start": spot_at_start,
        "spot_return_since_window_start": (spot_at_touch / spot_at_start - 1.0) if spot_at_start else None,
        "spot_realized_vol_recent_60s": vol_60,
        "spot_realized_vol_recent_300s": vol_300,
        "spot_velocity_recent_30s": (spot_at_touch - float(bar_30s["close"])) / 30.0,
        "seconds_to_resolution": seconds_to_resolution,
        "spot_implied_prob": spot_implied_prob,
        "spot_implied_prob_minus_market_prob": (spot_implied_prob - best_ask) if spot_implied_prob is not None and best_ask is not None else None,
        "source": str(touch_bar["source"]),
    }


def _spot_implied_prob(
    *,
    spot: float,
    strike: float,
    sigma_per_sqrt_second: float | None,
    seconds_to_resolution: float,
    direction: str,
) -> float | None:
    if spot <= 0 or strike <= 0 or seconds_to_resolution <= 0:
        return None
    sigma = sigma_per_sqrt_second or 0.0
    if sigma <= 0:
        up_prob = 1.0 if spot > strike else 0.0 if spot < strike else 0.5
    else:
        z = (strike - spot) / (spot * sigma * math.sqrt(seconds_to_resolution))
        up_prob = 1.0 - _normal_cdf(z)
    if direction.upper().startswith("DOWN"):
        return 1.0 - up_prob
    if direction.upper().startswith("UP"):
        return up_prob
    return up_prob


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _realized_vol(rows: list[sqlite3.Row]) -> float | None:
    closes = [float(row["close"]) for row in rows if _float_or_none(row["close"]) is not None and float(row["close"]) > 0]
    if len(closes) < 2:
        return None
    returns = [math.log(closes[idx] / closes[idx - 1]) for idx in range(1, len(closes)) if closes[idx - 1] > 0]
    if not returns:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((item - mean) ** 2 for item in returns) / len(returns)
    return math.sqrt(variance)


def _nearest_bar(conn: sqlite3.Connection, asset: str, symbol: str, target_ms: int, *, max_latency_ms: int) -> sqlite3.Row | None:
    rows = list(
        conn.execute(
            """
            SELECT * FROM spot_ohlc_cache
            WHERE asset_slug = ? AND symbol = ? AND open_time_ms BETWEEN ? AND ?
            ORDER BY ABS(open_time_ms - ?) ASC
            LIMIT 1
            """,
            (asset, symbol, target_ms - max_latency_ms, target_ms + max_latency_ms, target_ms),
        )
    )
    return rows[0] if rows else None


def _bars_between(conn: sqlite3.Connection, asset: str, symbol: str, start_ms: int, end_ms: int) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT * FROM spot_ohlc_cache
            WHERE asset_slug = ? AND symbol = ? AND open_time_ms BETWEEN ? AND ?
            ORDER BY open_time_ms
            """,
            (asset, symbol, start_ms, end_ms),
        )
    )


def _ensure_cache_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spot_ohlc_cache (
            asset_slug TEXT NOT NULL,
            symbol TEXT NOT NULL,
            open_time_ms INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL,
            source TEXT NOT NULL,
            PRIMARY KEY(asset_slug, symbol, open_time_ms)
        )
        """
    )


def _reset_output_schema(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS spot_features")
    columns_sql = ",\n            ".join(f"{name} {definition}" for name, definition in SPOT_FEATURE_COLUMNS)
    conn.execute(f"CREATE TABLE spot_features (\n            {columns_sql}\n        )")


def _insert_bars(conn: sqlite3.Connection, bars: Iterable[SpotBar]) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO spot_ohlc_cache (
            asset_slug, symbol, open_time_ms, open, high, low, close, volume, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(bar.asset_slug, bar.symbol, bar.open_time_ms, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.source) for bar in bars],
    )


def _insert_feature(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    columns = [name for name, _ in SPOT_FEATURE_COLUMNS]
    placeholders = ", ".join("?" for _ in columns)
    conn.execute(
        f"INSERT INTO spot_features ({', '.join(columns)}) VALUES ({placeholders})",
        [row.get(column) for column in columns],
    )


def _range_has_any_cache(conn: sqlite3.Connection, asset: str, symbol: str, start_ms: int, end_ms: int) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM spot_ohlc_cache WHERE asset_slug = ? AND symbol = ? AND open_time_ms BETWEEN ? AND ? LIMIT 1",
            (asset, symbol, start_ms, end_ms),
        ).fetchone()
    )


def _coarse_range_is_cached(conn: sqlite3.Connection, asset: str, symbol: str, start_ms: int, end_ms: int, *, interval_ms: int) -> bool:
    expected = int((end_ms - start_ms) // interval_ms) + 1
    actual = int(
        conn.execute(
            "SELECT COUNT(*) FROM spot_ohlc_cache WHERE asset_slug = ? AND symbol = ? AND open_time_ms BETWEEN ? AND ?",
            (asset, symbol, start_ms, end_ms),
        ).fetchone()[0]
    )
    return actual >= max(1, int(expected * 0.90))


def _range_is_cached(conn: sqlite3.Connection, asset: str, symbol: str, start_ms: int, end_ms: int) -> bool:
    expected = int((end_ms - start_ms) // 1000) + 1
    actual = int(
        conn.execute(
            "SELECT COUNT(*) FROM spot_ohlc_cache WHERE asset_slug = ? AND symbol = ? AND open_time_ms BETWEEN ? AND ?",
            (asset, symbol, start_ms, end_ms),
        ).fetchone()[0]
    )
    return actual >= max(1, int(expected * 0.95))


def _merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not ranges:
        return []
    ranges = sorted(ranges)
    merged = [ranges[0]]
    for start, end in ranges[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end + 1000:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def _require_table(conn: sqlite3.Connection, table: str, path: str | Path) -> None:
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is None:
        raise ValueError(f"{path} does not contain required table {table}")


def _parse_iso_ms(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


def _iso_from_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if str(value).strip() == "":
            return None
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except Exception:
        return None


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill offline spot features for the P6 touch dataset")
    parser.add_argument("--touch-dataset", default=str(DEFAULT_TOUCH_DATASET_PATH), help="SQLite touch_dataset path from P6-2a-3")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="SQLite output path for spot_features")
    parser.add_argument("--cache", default=str(DEFAULT_CACHE_PATH), help="SQLite cache path for 1s OHLC bars")
    parser.add_argument("--binance-base", default=BINANCE_BASE, help="Binance REST base URL")
    parser.add_argument("--coingecko-base", default=COINGECKO_BASE, help="CoinGecko REST base URL for coarse fallback assets")
    parser.add_argument("--cryptocompare-base", default=CRYPTOCOMPARE_BASE, help="CryptoCompare REST base URL for minute fallback assets")
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS, help="Sleep between uncached REST calls")
    parser.add_argument("--asset-symbol", action="append", default=[], help="Override/add mapping as asset=SYMBOL; can be repeated")
    parser.add_argument("--summary-out", default=None, help="Optional JSON summary path")
    args = parser.parse_args()

    overrides: dict[str, str] = {}
    for item in args.asset_symbol:
        if "=" not in item:
            raise ValueError(f"--asset-symbol must be asset=SYMBOL, got {item!r}")
        asset, symbol = item.split("=", 1)
        overrides[asset.strip().lower()] = symbol.strip().upper()

    summary = build_spot_features(
        args.touch_dataset,
        output_path=args.output,
        cache_path=args.cache,
        binance_base=args.binance_base,
        coingecko_base=args.coingecko_base,
        cryptocompare_base=args.cryptocompare_base,
        sleep_seconds=args.sleep_seconds,
        asset_symbols=overrides,
    )
    payload = json.dumps(summary.as_json(), indent=2, sort_keys=True)
    if args.summary_out:
        path = Path(args.summary_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload + "\n")
    print(payload)


if __name__ == "__main__":
    main()
