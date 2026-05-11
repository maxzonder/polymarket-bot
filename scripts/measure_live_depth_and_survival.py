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
import re
import signal
import sqlite3
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api.gamma_client import GAMMA_BASE
from utils.paths import DATA_DIR

try:
    import websockets
except ImportError:  # pragma: no cover
    websockets = None  # type: ignore[assignment]


WS_MARKET_ENDPOINT = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SCHEMA_VERSION = 2
DEFAULT_LEVELS = (0.55, 0.65, 0.70)
LOW_TAIL_LEVELS = (0.005, 0.01, 0.02, 0.03, 0.05, 0.10, 0.15, 0.20)
BLACK_SWAN_5C_LEVELS = (0.005, 0.01, 0.02, 0.03, 0.05)
WIDE_LEVELS = (0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90)
LEVEL_PRESETS = {
    "crypto_15m_touch": DEFAULT_LEVELS,
    "crypto_wide": WIDE_LEVELS,
    "black_swan_low_tail": LOW_TAIL_LEVELS,
    "black_swan_5c": BLACK_SWAN_5C_LEVELS,
    "crypto_wide_low_tail": LOW_TAIL_LEVELS + WIDE_LEVELS,
}
UNIVERSE_MODES = (
    "crypto_15m",
    "crypto_multi_horizon",
    "weather_temperature",
    "sports_fast",
    "black_swan_low_tail",
    "mp_full_depth",
)
DEFAULT_NOTIONALS = (10.0, 50.0, 100.0)
DEFAULT_MIN_SHARES = 5.0
DEFAULT_DEPTH_LEVELS = 5
DEFAULT_SURVIVAL_WINDOW_MS = 2000
DEFAULT_SAMPLING_INTERVAL_MS = 25
DEFAULT_REFRESH_INTERVAL_SEC = 30
DEFAULT_BOOK_SNAPSHOT_INTERVAL_MS = 0
DEFAULT_STALE_BOOK_MS = 5_000
DEFAULT_WIDE_SPREAD = 0.10
DEFAULT_DURATION_MIN = 840
DEFAULT_DURATION_MAX = 960
DEFAULT_PING_INTERVAL_SEC = 10
DEFAULT_MAX_DISCOVERY_ROWS = 20000
DEFAULT_OUTPUT_SUBDIR = Path("short_horizon") / "phase0"
DEFAULT_OUTPUT_BASENAME = "live_depth_survival.csv"
BINANCE_TICKER_BASE = "https://api.binance.com"
DEFAULT_BINANCE_SPOT_SYMBOLS = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "xrp": "XRPUSDT",
    "bnb": "BNBUSDT",
    "doge": "DOGEUSDT",
}


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
    seconds_to_end: Optional[int]
    recurrence: Optional[str]
    series_slug: Optional[str]
    fees_enabled: bool
    fee_rate_bps: Optional[float]
    tick_size: Optional[float]
    category: Optional[str] = None
    event_slug: Optional[str] = None
    asset_slug: Optional[str] = None
    universe_mode: Optional[str] = None
    horizon_bucket: Optional[str] = None
    event_subtype: Optional[str] = None
    parent_event_slug: Optional[str] = None
    rule_parse_status: Optional[str] = None
    payoff_type: Optional[str] = None


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
class BookObservation:
    event_ts_ms: int
    best_bid: Optional[float]
    best_ask: Optional[float]


@dataclass
class SpotObservation:
    asset_slug: str
    event_ts_ms: int
    ingest_ts_ms: int
    source: str
    spot_price: float
    bid: Optional[float] = None
    ask: Optional[float] = None


@dataclass
class PreTouchFeatures:
    best_bid_1s_before: Optional[float] = None
    best_ask_1s_before: Optional[float] = None
    best_bid_delta_1s: Optional[float] = None
    best_ask_delta_1s: Optional[float] = None
    best_bid_5s_before: Optional[float] = None
    best_ask_5s_before: Optional[float] = None
    best_bid_delta_5s: Optional[float] = None
    best_ask_delta_5s: Optional[float] = None
    best_bid_15s_before: Optional[float] = None
    best_ask_15s_before: Optional[float] = None
    best_bid_delta_15s: Optional[float] = None
    best_ask_delta_15s: Optional[float] = None
    best_bid_60s_before: Optional[float] = None
    best_ask_60s_before: Optional[float] = None
    best_bid_delta_60s: Optional[float] = None
    best_ask_delta_60s: Optional[float] = None


@dataclass
class SurvivalProbe:
    probe_id: str
    token: MarketToken
    level: float
    started_at_ms: int
    initial_best_ask: float
    initial_ask_size_at_level: float
    ask_levels_at_touch: list[tuple[float, float]]
    bid_levels_at_touch: list[tuple[float, float]]
    notional_fit: dict[str, str]
    fit_details: dict[str, dict[str, Optional[float] | str]]
    min_share_details: dict[str, Optional[float] | str]
    pre_touch_features: PreTouchFeatures = field(default_factory=PreTouchFeatures)
    spot_features: dict[str, Any] = field(default_factory=dict)
    max_favorable_move: float = 0.0
    max_adverse_move: float = 0.0
    held_at_or_above_level: bool = True
    immediate_reversal_flag: bool = False
    done: bool = False
    end_reason: Optional[str] = None
    survived_ms: Optional[int] = None


class CsvSink:
    FIELDNAMES = [
        "schema_version",
        "run_id",
        "probe_id",
        "recorded_at",
        "market_id",
        "condition_id",
        "token_id",
        "outcome",
        "question",
        "category",
        "event_slug",
        "asset_slug",
        "universe_mode",
        "horizon_bucket",
        "event_subtype",
        "parent_event_slug",
        "rule_parse_status",
        "payoff_type",
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
        "spread_at_touch",
        "mid_price_at_touch",
        "microprice_at_touch",
        "imbalance_top1",
        "imbalance_top3",
        "imbalance_top5",
        "bid_level_1_price",
        "bid_level_1_size",
        "bid_level_2_price",
        "bid_level_2_size",
        "bid_level_3_price",
        "bid_level_3_size",
        "bid_level_4_price",
        "bid_level_4_size",
        "bid_level_5_price",
        "bid_level_5_size",
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
        "fit_5_shares",
        "entry_price_for_5_shares",
        "avg_entry_price_for_5_shares",
        "cost_for_5_shares",
        "worst_tick_for_5_shares",
        "estimated_fee_for_5_shares",
        "required_hit_rate_for_5_shares",
        "fit_10_usdc",
        "avg_entry_price_for_10_usdc",
        "fit_50_usdc",
        "avg_entry_price_for_50_usdc",
        "fit_100_usdc",
        "avg_entry_price_for_100_usdc",
        "book_age_ms_at_touch",
        "event_to_ingest_latency_ms_at_touch",
        "missing_depth_flag_at_touch",
        "crossed_book_flag_at_touch",
        "wide_spread_flag_at_touch",
        "book_stale_flag_at_touch",
        "best_bid_1s_before",
        "best_ask_1s_before",
        "best_bid_delta_1s",
        "best_ask_delta_1s",
        "best_bid_5s_before",
        "best_ask_5s_before",
        "best_bid_delta_5s",
        "best_ask_delta_5s",
        "best_bid_15s_before",
        "best_ask_15s_before",
        "best_bid_delta_15s",
        "best_ask_delta_15s",
        "best_bid_60s_before",
        "best_ask_60s_before",
        "best_bid_delta_60s",
        "best_ask_delta_60s",
        "max_favorable_move",
        "max_adverse_move",
        "held_at_or_above_level",
        "immediate_reversal_flag",
        "spot_price_at_touch",
        "spot_source_at_touch",
        "spot_latency_ms_at_touch",
        "spot_bid_at_touch",
        "spot_ask_at_touch",
        "spot_return_30s",
        "spot_return_1m",
        "spot_return_3m",
        "spot_return_5m",
        "survived_ms",
        "end_reason",
    ]

    def __init__(self, path: Path, *, run_id: str):
        self.path = path
        self.run_id = run_id
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=self.FIELDNAMES, extrasaction="ignore")
        if self.path.stat().st_size == 0:
            self._writer.writeheader()
            self._fh.flush()

    def write_probe(self, probe: SurvivalProbe, book: BookState) -> dict[str, Any]:
        row = build_touch_row(probe, book, run_id=self.run_id)
        self._writer.writerow(row)
        self._fh.flush()
        return row

    def close(self) -> None:
        self._fh.close()


class SqliteSink:
    def __init__(self, path: Path, *, run_id: str, args: argparse.Namespace):
        self.path = path
        self.run_id = run_id
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stale_book_ms = int(getattr(args, "stale_book_ms", DEFAULT_STALE_BOOK_MS))
        self.wide_spread_threshold = float(getattr(args, "wide_spread_threshold", DEFAULT_WIDE_SPREAD))
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()
        self.conn.execute(
            """
            INSERT OR REPLACE INTO collection_runs(
                run_id, schema_version, started_at, level_preset, levels_json, depth_levels, output_csv,
                universe_mode, book_snapshot_interval_ms, stale_book_ms, wide_spread_threshold
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                SCHEMA_VERSION,
                utc_now_iso(),
                getattr(args, "level_preset", None),
                json.dumps(list(args.levels)),
                int(args.depth_levels),
                str(args.output_csv),
                str(getattr(args, "universe_mode", "crypto_15m")),
                int(getattr(args, "book_snapshot_interval_ms", 0) or 0),
                int(getattr(args, "stale_book_ms", DEFAULT_STALE_BOOK_MS)),
                float(getattr(args, "wide_spread_threshold", DEFAULT_WIDE_SPREAD)),
            ),
        )
        self.conn.commit()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS collection_runs (
                run_id TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                level_preset TEXT,
                levels_json TEXT NOT NULL,
                depth_levels INTEGER NOT NULL,
                output_csv TEXT,
                universe_mode TEXT,
                book_snapshot_interval_ms INTEGER,
                stale_book_ms INTEGER,
                wide_spread_threshold REAL
            );
            CREATE TABLE IF NOT EXISTS touch_events (
                probe_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                recorded_at TEXT,
                market_id TEXT,
                condition_id TEXT,
                token_id TEXT,
                outcome TEXT,
                question TEXT,
                category TEXT,
                event_slug TEXT,
                asset_slug TEXT,
                universe_mode TEXT,
                horizon_bucket TEXT,
                event_subtype TEXT,
                parent_event_slug TEXT,
                rule_parse_status TEXT,
                payoff_type TEXT,
                touch_level REAL,
                touch_time_iso TEXT,
                duration_seconds INTEGER,
                start_time_iso TEXT,
                end_time_iso TEXT,
                fees_enabled INTEGER,
                fee_rate_bps REAL,
                tick_size REAL,
                best_bid_at_touch REAL,
                best_ask_at_touch REAL,
                spread_at_touch REAL,
                mid_price_at_touch REAL,
                microprice_at_touch REAL,
                imbalance_top1 REAL,
                imbalance_top3 REAL,
                imbalance_top5 REAL,
                bid_levels_json TEXT,
                ask_levels_json TEXT,
                ask_size_at_touch_level REAL,
                fit_5_shares TEXT,
                entry_price_for_5_shares REAL,
                avg_entry_price_for_5_shares REAL,
                cost_for_5_shares REAL,
                worst_tick_for_5_shares INTEGER,
                estimated_fee_for_5_shares REAL,
                required_hit_rate_for_5_shares REAL,
                fit_10_usdc TEXT,
                avg_entry_price_for_10_usdc REAL,
                fit_50_usdc TEXT,
                avg_entry_price_for_50_usdc REAL,
                fit_100_usdc TEXT,
                avg_entry_price_for_100_usdc REAL,
                book_age_ms_at_touch INTEGER,
                event_to_ingest_latency_ms_at_touch INTEGER,
                missing_depth_flag_at_touch INTEGER,
                crossed_book_flag_at_touch INTEGER,
                wide_spread_flag_at_touch INTEGER,
                book_stale_flag_at_touch INTEGER,
                best_bid_1s_before REAL,
                best_ask_1s_before REAL,
                best_bid_delta_1s REAL,
                best_ask_delta_1s REAL,
                best_bid_5s_before REAL,
                best_ask_5s_before REAL,
                best_bid_delta_5s REAL,
                best_ask_delta_5s REAL,
                best_bid_15s_before REAL,
                best_ask_15s_before REAL,
                best_bid_delta_15s REAL,
                best_ask_delta_15s REAL,
                best_bid_60s_before REAL,
                best_ask_60s_before REAL,
                best_bid_delta_60s REAL,
                best_ask_delta_60s REAL,
                max_favorable_move REAL,
                max_adverse_move REAL,
                held_at_or_above_level INTEGER,
                immediate_reversal_flag INTEGER,
                spot_price_at_touch REAL,
                spot_source_at_touch TEXT,
                spot_latency_ms_at_touch INTEGER,
                spot_bid_at_touch REAL,
                spot_ask_at_touch REAL,
                spot_return_30s REAL,
                spot_return_1m REAL,
                spot_return_3m REAL,
                spot_return_5m REAL,
                survived_ms INTEGER,
                end_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_touch_events_market_time ON touch_events(market_id, touch_time_iso);
            CREATE INDEX IF NOT EXISTS idx_touch_events_token_time ON touch_events(token_id, touch_time_iso);
            CREATE TABLE IF NOT EXISTS book_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                probe_id TEXT,
                market_id TEXT,
                token_id TEXT NOT NULL,
                event_time_iso TEXT,
                event_ts_ms INTEGER,
                ingest_monotonic_ms INTEGER,
                source TEXT NOT NULL,
                update_kind TEXT NOT NULL,
                book_hash TEXT,
                best_bid REAL,
                best_ask REAL,
                bid_levels_json TEXT NOT NULL,
                ask_levels_json TEXT NOT NULL,
                depth_levels INTEGER NOT NULL,
                book_age_ms INTEGER,
                event_to_ingest_latency_ms INTEGER,
                missing_depth_flag INTEGER NOT NULL DEFAULT 0,
                crossed_book_flag INTEGER NOT NULL DEFAULT 0,
                wide_spread_flag INTEGER NOT NULL DEFAULT 0,
                book_stale_flag INTEGER NOT NULL DEFAULT 0,
                zero_size_level_flag INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_book_snapshots_token_time ON book_snapshots(token_id, event_ts_ms);
            CREATE INDEX IF NOT EXISTS idx_book_snapshots_market_time ON book_snapshots(market_id, event_ts_ms);
            CREATE TABLE IF NOT EXISTS spot_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                asset_slug TEXT NOT NULL,
                event_ts_ms INTEGER NOT NULL,
                ingest_ts_ms INTEGER NOT NULL,
                event_time_iso TEXT NOT NULL,
                source TEXT NOT NULL,
                spot_price REAL NOT NULL,
                bid REAL,
                ask REAL,
                latency_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_spot_snapshots_asset_time ON spot_snapshots(asset_slug, event_ts_ms);
            """
        )
        self.conn.commit()

    def write_probe(self, probe: SurvivalProbe, book: BookState, row: Optional[dict[str, Any]] = None) -> None:
        if row is None:
            row = build_touch_row(probe, book, run_id=self.run_id)
        self.write_book_snapshot(book, update_kind="touch", probe=probe)
        self.conn.execute(
            """
            INSERT OR REPLACE INTO touch_events(
                probe_id, run_id, schema_version, recorded_at, market_id, condition_id, token_id, outcome,
                question, category, event_slug, asset_slug, universe_mode, horizon_bucket, event_subtype,
                parent_event_slug, rule_parse_status, payoff_type, touch_level, touch_time_iso, duration_seconds, start_time_iso,
                end_time_iso, fees_enabled, fee_rate_bps, tick_size, best_bid_at_touch, best_ask_at_touch,
                spread_at_touch, mid_price_at_touch, microprice_at_touch, imbalance_top1, imbalance_top3,
                imbalance_top5, bid_levels_json, ask_levels_json, ask_size_at_touch_level, fit_5_shares,
                entry_price_for_5_shares, avg_entry_price_for_5_shares, cost_for_5_shares,
                worst_tick_for_5_shares, estimated_fee_for_5_shares, required_hit_rate_for_5_shares,
                fit_10_usdc, avg_entry_price_for_10_usdc, fit_50_usdc, avg_entry_price_for_50_usdc,
                fit_100_usdc, avg_entry_price_for_100_usdc, book_age_ms_at_touch,
                event_to_ingest_latency_ms_at_touch, missing_depth_flag_at_touch, crossed_book_flag_at_touch,
                wide_spread_flag_at_touch, book_stale_flag_at_touch, best_bid_1s_before, best_ask_1s_before,
                best_bid_delta_1s, best_ask_delta_1s, best_bid_5s_before, best_ask_5s_before,
                best_bid_delta_5s, best_ask_delta_5s, best_bid_15s_before, best_ask_15s_before,
                best_bid_delta_15s, best_ask_delta_15s, best_bid_60s_before, best_ask_60s_before,
                best_bid_delta_60s, best_ask_delta_60s, max_favorable_move, max_adverse_move,
                held_at_or_above_level, immediate_reversal_flag, spot_price_at_touch, spot_source_at_touch,
                spot_latency_ms_at_touch, spot_bid_at_touch, spot_ask_at_touch, spot_return_30s,
                spot_return_1m, spot_return_3m, spot_return_5m, survived_ms, end_reason
            ) VALUES (
                :probe_id, :run_id, :schema_version, :recorded_at, :market_id, :condition_id, :token_id, :outcome,
                :question, :category, :event_slug, :asset_slug, :universe_mode, :horizon_bucket, :event_subtype,
                :parent_event_slug, :rule_parse_status, :payoff_type, :touch_level, :touch_time_iso, :duration_seconds, :start_time_iso,
                :end_time_iso, :fees_enabled, :fee_rate_bps, :tick_size, :best_bid_at_touch, :best_ask_at_touch,
                :spread_at_touch, :mid_price_at_touch, :microprice_at_touch, :imbalance_top1, :imbalance_top3,
                :imbalance_top5, :bid_levels_json, :ask_levels_json, :ask_size_at_touch_level, :fit_5_shares,
                :entry_price_for_5_shares, :avg_entry_price_for_5_shares, :cost_for_5_shares,
                :worst_tick_for_5_shares, :estimated_fee_for_5_shares, :required_hit_rate_for_5_shares,
                :fit_10_usdc, :avg_entry_price_for_10_usdc, :fit_50_usdc, :avg_entry_price_for_50_usdc,
                :fit_100_usdc, :avg_entry_price_for_100_usdc, :book_age_ms_at_touch,
                :event_to_ingest_latency_ms_at_touch, :missing_depth_flag_at_touch, :crossed_book_flag_at_touch,
                :wide_spread_flag_at_touch, :book_stale_flag_at_touch, :best_bid_1s_before, :best_ask_1s_before,
                :best_bid_delta_1s, :best_ask_delta_1s, :best_bid_5s_before, :best_ask_5s_before,
                :best_bid_delta_5s, :best_ask_delta_5s, :best_bid_15s_before, :best_ask_15s_before,
                :best_bid_delta_15s, :best_ask_delta_15s, :best_bid_60s_before, :best_ask_60s_before,
                :best_bid_delta_60s, :best_ask_delta_60s, :max_favorable_move, :max_adverse_move,
                :held_at_or_above_level, :immediate_reversal_flag, :spot_price_at_touch, :spot_source_at_touch,
                :spot_latency_ms_at_touch, :spot_bid_at_touch, :spot_ask_at_touch, :spot_return_30s,
                :spot_return_1m, :spot_return_3m, :spot_return_5m, :survived_ms, :end_reason
            )
            """,
            row,
        )
        self.conn.commit()

    def write_book_snapshot(
        self,
        book: BookState,
        *,
        update_kind: str,
        probe: Optional[SurvivalProbe] = None,
    ) -> None:
        row = build_book_snapshot_row(
            book,
            run_id=self.run_id,
            update_kind=update_kind,
            probe=probe,
            stale_book_ms=self.stale_book_ms,
            wide_spread_threshold=self.wide_spread_threshold,
        )
        self.conn.execute(
            """
            INSERT OR REPLACE INTO book_snapshots(
                snapshot_id, run_id, schema_version, probe_id, market_id, token_id, event_time_iso,
                event_ts_ms, ingest_monotonic_ms, source, update_kind, book_hash, best_bid, best_ask,
                bid_levels_json, ask_levels_json, depth_levels, book_age_ms, event_to_ingest_latency_ms,
                missing_depth_flag, crossed_book_flag, wide_spread_flag, book_stale_flag, zero_size_level_flag
            ) VALUES (
                :snapshot_id, :run_id, :schema_version, :probe_id, :market_id, :token_id, :event_time_iso,
                :event_ts_ms, :ingest_monotonic_ms, :source, :update_kind, :book_hash, :best_bid, :best_ask,
                :bid_levels_json, :ask_levels_json, :depth_levels, :book_age_ms, :event_to_ingest_latency_ms,
                :missing_depth_flag, :crossed_book_flag, :wide_spread_flag, :book_stale_flag, :zero_size_level_flag
            )
            """,
            row,
        )
        self.conn.commit()

    def write_spot_snapshot(self, spot: SpotObservation) -> None:
        snapshot_id = f"{self.run_id}:spot:{spot.asset_slug}:{spot.event_ts_ms}:{spot.source}"
        latency_ms = max(0, spot.ingest_ts_ms - spot.event_ts_ms)
        self.conn.execute(
            """
            INSERT OR REPLACE INTO spot_snapshots(
                snapshot_id, run_id, schema_version, asset_slug, event_ts_ms, ingest_ts_ms, event_time_iso,
                source, spot_price, bid, ask, latency_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                self.run_id,
                SCHEMA_VERSION,
                spot.asset_slug,
                spot.event_ts_ms,
                spot.ingest_ts_ms,
                iso_from_ms(spot.event_ts_ms),
                spot.source,
                spot.spot_price,
                spot.bid,
                spot.ask,
                latency_ms,
            ),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class LiveDepthCollector:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.logger = logging.getLogger("measure_live_depth_and_survival")
        self.books: dict[str, BookState] = {}
        self.tokens_by_id: dict[str, MarketToken] = {}
        self.last_best_ask: dict[tuple[str, float], Optional[float]] = {}
        self.fired_touches: set[tuple[str, str, float]] = set()
        self.active_probes: dict[str, SurvivalProbe] = {}
        self.book_history: dict[str, deque[BookObservation]] = {}
        self.spot_history_by_asset: dict[str, deque[SpotObservation]] = {}
        self.latest_spot_by_asset: dict[str, SpotObservation] = {}
        self.completed_probes = 0
        self.csv = CsvSink(Path(args.output_csv), run_id=args.run_id)
        self.sqlite = SqliteSink(Path(args.output_sqlite), run_id=args.run_id, args=args) if args.output_sqlite else None
        self.stop_event = asyncio.Event()
        self.last_discovery_stats: dict[str, Any] = {}
        self.market_ws: Any = None
        self.periodic_snapshots_written = 0

    async def run(self) -> None:
        self._install_signal_handlers()
        bootstrap = self.fetch_active_market_tokens()
        self.tokens_by_id = {token.token_id: token for token in bootstrap}
        if not bootstrap:
            self.logger.warning("No eligible active tokens on bootstrap attempt; collector will stay alive and retry.")
        else:
            self.logger.info("Bootstrapped %s eligible tokens", len(self.tokens_by_id))

        consumer = asyncio.create_task(self.market_ws_loop())
        refresh = asyncio.create_task(self.refresh_loop())
        survival = asyncio.create_task(self.survival_loop())
        heartbeat = asyncio.create_task(self.heartbeat_loop())
        snapshot = asyncio.create_task(self.book_snapshot_loop())
        spot = asyncio.create_task(self.spot_feed_loop())
        tasks = (consumer, refresh, survival, heartbeat, snapshot, spot)

        try:
            await self.stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.csv.close()
            if self.sqlite is not None:
                self.sqlite.close()

    def fetch_active_market_tokens(self) -> list[MarketToken]:
        markets: list[MarketToken] = []
        offset = 0
        seen_market_ids: set[str] = set()
        now_ts = time.time()
        session = requests.Session()
        stats = {
            "rows_seen": 0,
            "skipped_duplicate": 0,
            "skipped_outcomes": 0,
            "skipped_asset_slug": 0,
            "skipped_text_filter": 0,
            "skipped_payoff_type": 0,
            "skipped_missing_end": 0,
            "skipped_bad_time": 0,
            "skipped_nonpositive_remaining": 0,
            "skipped_duration_window": 0,
            "skipped_recurrence": 0,
            "skipped_volume": 0,
            "eligible_markets": 0,
            "eligible_tokens": 0,
        }
        allowed_asset_slugs = _allowed_asset_slugs(getattr(self.args, "asset_slug", None))

        while True:
            for _attempt in range(8):
                _disc_params: dict[str, Any] = {
                    "limit": 100,
                    "offset": offset,
                    "active": "true",
                    "closed": "false",
                    "archived": "false",
                    "order": self.args.discovery_order,
                    "ascending": str(self.args.discovery_ascending).lower(),
                }
                # When duration_metric=time_remaining, push end_date window into the
                # API query so the 20k-row scan reaches markets regardless of creation age.
                if (
                    getattr(self.args, "duration_metric", None) == "time_remaining"
                    and getattr(self.args, "min_duration_seconds", None) is not None
                    and getattr(self.args, "max_duration_seconds", None) is not None
                ):
                    import datetime as _dt
                    _now = _dt.datetime.now(_dt.timezone.utc)
                    _disc_params["end_date_min"] = (
                        _now + _dt.timedelta(seconds=self.args.min_duration_seconds)
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
                    _disc_params["end_date_max"] = (
                        _now + _dt.timedelta(seconds=self.args.max_duration_seconds)
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
                resp = session.get(
                    f"{GAMMA_BASE}/markets",
                    params=_disc_params,
                    timeout=30,
                )
                if resp.status_code == 429:
                    wait = min(5 * (2 ** _attempt), 60)
                    logging.warning("discovery 429 at offset=%d, retry in %ds", offset, wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            else:
                raise RuntimeError(f"discovery failed after retries at offset={offset}")
            payload = resp.json()
            if not isinstance(payload, list) or not payload:
                break

            for raw in payload:
                stats["rows_seen"] += 1
                market_id = str(raw.get("id") or "")
                if not market_id or market_id in seen_market_ids:
                    stats["skipped_duplicate"] += 1
                    continue
                token_ids = _load_json_list(raw.get("clobTokenIds", "[]"))
                outcomes = _load_json_list(raw.get("outcomes", "[]"))
                if len(token_ids) != 2 or len(outcomes) != 2:
                    stats["skipped_outcomes"] += 1
                    continue

                event0 = None
                events = raw.get("events") or []
                if events and isinstance(events[0], dict):
                    event0 = events[0]

                start_iso = None
                end_iso = None
                recurrence = None
                series_slug = None
                if event0:
                    # For short-horizon recurring series, live discovery must gate on the actual window start,
                    # not on top-level market creation time. Gamma `raw.startDate` often reflects shell creation.
                    start_iso = event0.get("startTime") or event0.get("startDate") or event0.get("eventStartTime")
                    end_iso = event0.get("endDate") or raw.get("endDate") or raw.get("endDateIso")
                    series = event0.get("series") or []
                    if series and isinstance(series[0], dict):
                        recurrence = series[0].get("recurrence")
                        series_slug = series[0].get("slug")
                    if not series_slug:
                        series_slug = event0.get("seriesSlug")
                if not start_iso:
                    start_iso = raw.get("startDate") or raw.get("startDateIso") or raw.get("gameStartTime")
                if not end_iso:
                    end_iso = raw.get("endDate") or raw.get("endDateIso")
                if not end_iso:
                    stats["skipped_missing_end"] += 1
                    continue

                start_ts = parse_iso_to_ts(start_iso) if start_iso else None
                end_ts = parse_iso_to_ts(end_iso)
                if end_ts is None:
                    stats["skipped_bad_time"] += 1
                    continue
                remaining_seconds = int(end_ts - now_ts)
                if remaining_seconds <= 0:
                    stats["skipped_nonpositive_remaining"] += 1
                    continue
                seconds_until_start = int(start_ts - now_ts) if start_ts is not None else None
                duration_seconds = int(end_ts - start_ts) if start_ts is not None else None

                is_target_recurrence = str(recurrence or "").lower() == "15m"
                question = str(raw.get("question") or "")
                is_updown_question = "up or down" in question.lower()
                if self.args.require_recurrence and not (is_target_recurrence or is_updown_question):
                    stats["skipped_recurrence"] += 1
                    continue

                series_slug_lower = str(series_slug or "").lower()
                implied_duration_seconds = None
                if series_slug_lower.endswith("-15m") or "-15m" in series_slug_lower:
                    implied_duration_seconds = 900
                elif series_slug_lower.endswith("-5m") or "-5m" in series_slug_lower:
                    implied_duration_seconds = 300
                elif str(recurrence or "").lower() == "15m":
                    implied_duration_seconds = 900
                elif str(recurrence or "").lower() == "5m":
                    implied_duration_seconds = 300

                if self.args.duration_metric == "time_remaining":
                    target_metric = remaining_seconds
                elif self.args.duration_metric == "lifecycle":
                    target_metric = duration_seconds
                else:
                    target_metric = implied_duration_seconds if implied_duration_seconds is not None else duration_seconds
                if target_metric is None:
                    stats["skipped_bad_time"] += 1
                    continue
                if target_metric < self.args.min_duration_seconds or target_metric > self.args.max_duration_seconds:
                    stats["skipped_duration_window"] += 1
                    continue
                if seconds_until_start is not None and seconds_until_start > self.args.max_seconds_until_start:
                    stats["skipped_duration_window"] += 1
                    continue
                if (
                    self.args.duration_metric == "time_remaining"
                    and remaining_seconds > self.args.max_seconds_to_end
                ):
                    stats["skipped_duration_window"] += 1
                    continue

                market_volume = _parse_float(raw.get("volumeNum") or raw.get("volume")) or 0.0
                min_vol = getattr(self.args, "min_volume_usdc", None)
                max_vol = getattr(self.args, "max_volume_usdc", None)
                if min_vol is not None and market_volume < min_vol:
                    stats["skipped_volume"] += 1
                    continue
                if max_vol is not None and market_volume > max_vol:
                    stats["skipped_volume"] += 1
                    continue

                condition_id = str(raw.get("conditionId") or market_id)
                fees_enabled = bool(raw.get("feesEnabled"))
                tick_size = _parse_float(raw.get("orderPriceMinTickSize"))
                category = _extract_category(raw, event0)
                event_slug = str((event0 or {}).get("slug") or raw.get("slug") or "") or None
                asset_slug = _extract_asset_slug(raw, event0, question)
                event_subtype = _extract_event_subtype(raw, event0, question)
                parent_event_slug = _extract_parent_event_slug(raw, event0)
                rule_parse_status = _rule_parse_status(raw, event0, question)
                payoff_type = _extract_payoff_type(raw, event0, question)
                if allowed_asset_slugs is not None and str(asset_slug or "").lower() not in allowed_asset_slugs:
                    stats["skipped_asset_slug"] += 1
                    continue
                if not _matches_market_text_filters(
                    raw,
                    event0,
                    question,
                    category=category,
                    universe_mode=self.args.universe_mode,
                    include_keywords=getattr(self.args, "market_keyword", None),
                    exclude_keywords=getattr(self.args, "exclude_market_keyword", None),
                ):
                    stats["skipped_text_filter"] += 1
                    continue
                if not _matches_payoff_type_filter(payoff_type, getattr(self.args, "payoff_type", None)):
                    stats["skipped_payoff_type"] += 1
                    continue
                fee_rate_bps = None
                fee_schedule = raw.get("feeSchedule") or raw.get("fee_schedule")
                if isinstance(fee_schedule, dict):
                    rate = _parse_float(fee_schedule.get("rate"))
                    if rate is not None:
                        fee_rate_bps = rate * 10000.0
                elif raw.get("takerBaseFee") is not None:
                    fee_rate_bps = _parse_float(raw.get("takerBaseFee"))
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
                            duration_seconds=implied_duration_seconds if implied_duration_seconds is not None else duration_seconds,
                            seconds_to_end=remaining_seconds,
                            recurrence=str(recurrence) if recurrence is not None else None,
                            series_slug=str(series_slug) if series_slug is not None else None,
                            fees_enabled=fees_enabled,
                            fee_rate_bps=fee_rate_bps,
                            tick_size=tick_size,
                            category=category,
                            event_slug=event_slug,
                            asset_slug=asset_slug,
                            universe_mode=self.args.universe_mode,
                            horizon_bucket=_horizon_bucket(
                                implied_duration_seconds if implied_duration_seconds is not None else duration_seconds
                            ),
                            event_subtype=event_subtype,
                            parent_event_slug=parent_event_slug,
                            rule_parse_status=rule_parse_status,
                            payoff_type=payoff_type,
                        )
                    )
                    stats["eligible_tokens"] += 1
                stats["eligible_markets"] += 1
                seen_market_ids.add(market_id)

            offset += len(payload)
            if len(payload) < 100 or offset >= self.args.max_discovery_rows:
                break
            time.sleep(0.05)

        self.last_discovery_stats = stats
        sample_questions = [token.question for token in markets[: min(3, len(markets))]]
        self.logger.info(
            "Discovery stats: rows=%s eligible_markets=%s eligible_tokens=%s skipped_duration=%s skipped_recurrence=%s skipped_asset=%s skipped_text=%s skipped_payoff=%s skipped_remaining=%s order=%s asc=%s metric=%s sample=%s",
            stats["rows_seen"],
            stats["eligible_markets"],
            stats["eligible_tokens"],
            stats["skipped_duration_window"],
            stats["skipped_recurrence"],
            stats["skipped_asset_slug"],
            stats["skipped_text_filter"],
            stats["skipped_payoff_type"],
            stats["skipped_nonpositive_remaining"],
            self.args.discovery_order,
            self.args.discovery_ascending,
            self.args.duration_metric,
            sample_questions,
        )
        return markets

    async def refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self.args.refresh_interval_sec)
            try:
                latest = self.fetch_active_market_tokens()
                latest_by_id = {token.token_id: token for token in latest}
                new_token_ids = [tid for tid in latest_by_id if tid not in self.tokens_by_id]
                removed_token_ids = [tid for tid in self.tokens_by_id if tid not in latest_by_id]
                token_set_changed = bool(new_token_ids or removed_token_ids)
                self.tokens_by_id = latest_by_id
                self.books = {tid: book for tid, book in self.books.items() if tid in latest_by_id}
                if token_set_changed:
                    self.logger.info(
                        "Eligible token refresh: +%s / -%s (now %s)",
                        len(new_token_ids),
                        len(removed_token_ids),
                        len(self.tokens_by_id),
                    )
                    if self.market_ws is not None:
                        self.logger.info("Universe changed, restarting websocket subscription")
                        await self.market_ws.close()
            except Exception:
                self.logger.exception("Active market refresh failed")

    async def heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.args.heartbeat_interval_sec)
            self.logger.info(
                "Heartbeat: subscribed_tokens=%s books=%s fired_touches=%s active_probes=%s completed_probes=%s periodic_snapshots=%s discovery=%s",
                len(self.tokens_by_id),
                len(self.books),
                len(self.fired_touches),
                len(self.active_probes),
                self.completed_probes,
                self.periodic_snapshots_written,
                self.last_discovery_stats,
            )

    async def book_snapshot_loop(self) -> None:
        interval_ms = int(getattr(self.args, "book_snapshot_interval_ms", 0) or 0)
        if interval_ms <= 0 or self.sqlite is None:
            return
        while True:
            await asyncio.sleep(interval_ms / 1000.0)
            written = 0
            for token_id, book in list(self.books.items()):
                if token_id not in self.tokens_by_id:
                    continue
                if not book.bid_levels and not book.ask_levels and book.best_bid is None and book.best_ask is None:
                    continue
                self.sqlite.write_book_snapshot(book, update_kind="periodic")
                written += 1
            self.periodic_snapshots_written += written
            if written:
                self.logger.debug("Periodic book snapshots written=%s total=%s", written, self.periodic_snapshots_written)

    async def spot_feed_loop(self) -> None:
        if getattr(self.args, "spot_feed", "none") == "none":
            return
        interval = float(getattr(self.args, "spot_poll_interval_sec", 1.0) or 1.0)
        session = requests.Session()
        while True:
            symbols = self._active_spot_symbols()
            if not symbols:
                await asyncio.sleep(interval)
                continue
            started = time.monotonic()
            for asset_slug, symbol in symbols.items():
                try:
                    observation = await asyncio.to_thread(fetch_binance_spot_observation, session, asset_slug, symbol)
                    self.record_spot_observation(observation)
                except Exception:
                    self.logger.debug("Spot fetch failed asset=%s symbol=%s", asset_slug, symbol, exc_info=True)
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.0, interval - elapsed))

    def _active_spot_symbols(self) -> dict[str, str]:
        configured = _allowed_asset_slugs(getattr(self.args, "spot_asset_slug", None))
        if not configured:
            configured = _allowed_asset_slugs(getattr(self.args, "asset_slug", None))
        if not configured:
            configured = {token.asset_slug for token in self.tokens_by_id.values() if token.asset_slug}
        return {asset: DEFAULT_BINANCE_SPOT_SYMBOLS[asset] for asset in sorted(configured) if asset in DEFAULT_BINANCE_SPOT_SYMBOLS}

    async def market_ws_loop(self) -> None:
        if websockets is None:  # pragma: no cover
            raise RuntimeError(
                "Missing dependency: websockets. Install it before running live collection, for example: "
                "python3 -m pip install websockets"
            )
        while True:
            asset_ids = list(self.tokens_by_id.keys())
            if not asset_ids:
                self.logger.warning("No asset ids to subscribe, retrying")
                await asyncio.sleep(5)
                continue
            try:
                async with websockets.connect(WS_MARKET_ENDPOINT, ping_interval=None, max_size=8_000_000) as ws:
                    self.market_ws = ws
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
                        self.market_ws = None
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
        self._record_book_observation(asset_id)
        await self._detect_touches(asset_id)

    def record_spot_observation(self, spot: SpotObservation) -> None:
        history = self.spot_history_by_asset.setdefault(spot.asset_slug, deque())
        history.append(spot)
        cutoff = spot.event_ts_ms - 360_000
        while history and history[0].event_ts_ms < cutoff:
            history.popleft()
        self.latest_spot_by_asset[spot.asset_slug] = spot
        if self.sqlite is not None:
            self.sqlite.write_spot_snapshot(spot)

    def _spot_features_at_touch(self, asset_slug: Optional[str], touch_ts_ms: int) -> dict[str, Any]:
        if not asset_slug:
            return empty_spot_features()
        history = self.spot_history_by_asset.get(asset_slug)
        if not history:
            return empty_spot_features()
        latest = latest_spot_before(history, touch_ts_ms)
        return spot_features_from_history(history, latest, touch_ts_ms)

    def _record_book_observation(self, token_id: str) -> None:
        book = self.books.get(token_id)
        if book is None:
            return
        event_ts_ms = book.last_event_ts_ms or int(time.time() * 1000)
        history = self.book_history.setdefault(token_id, deque())
        history.append(BookObservation(event_ts_ms=event_ts_ms, best_bid=book.best_bid, best_ask=book.best_ask))
        cutoff = event_ts_ms - 65_000
        while history and history[0].event_ts_ms < cutoff:
            history.popleft()

    def _pre_touch_features(self, token_id: str, touch_ts_ms: int, best_bid: Optional[float], best_ask: float) -> PreTouchFeatures:
        history = self.book_history.get(token_id, deque())
        features = PreTouchFeatures()
        for seconds in (1, 5, 15, 60):
            obs = latest_observation_before(history, touch_ts_ms - seconds * 1000)
            if obs is None:
                continue
            setattr(features, f"best_bid_{seconds}s_before", obs.best_bid)
            setattr(features, f"best_ask_{seconds}s_before", obs.best_ask)
            if best_bid is not None and obs.best_bid is not None:
                setattr(features, f"best_bid_delta_{seconds}s", best_bid - obs.best_bid)
            if obs.best_ask is not None:
                setattr(features, f"best_ask_delta_{seconds}s", best_ask - obs.best_ask)
        return features

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
            fired_key = (token.market_id, token_id, level)
            prev = self.last_best_ask.get(key)
            self.last_best_ask[key] = best_ask
            if prev is None:
                continue
            if fired_key in self.fired_touches:
                continue
            if prev < level <= best_ask:
                self.fired_touches.add(fired_key)
                probe_id = f"{token_id}:{level:.2f}:{book.last_event_ts_ms or int(time.time()*1000)}"
                if probe_id in self.active_probes:
                    continue
                ask_levels = list(book.ask_levels[: self.args.depth_levels])
                bid_levels = list(book.bid_levels[: self.args.depth_levels])
                ask_size = sum(size for price, size in ask_levels if abs(price - level) < 1e-9)
                fit_details = evaluate_notional_entry_details(ask_levels, self.args.notionals)
                fit = {key: str(value.get("fit")) for key, value in fit_details.items()}
                min_share_details = evaluate_share_entry_details(
                    ask_levels,
                    self.args.min_shares,
                    fee_rate_bps=token.fee_rate_bps if token.fees_enabled else None,
                )
                probe = SurvivalProbe(
                    probe_id=probe_id,
                    token=token,
                    level=level,
                    started_at_ms=book.last_event_ts_ms or int(time.time() * 1000),
                    initial_best_ask=best_ask,
                    initial_ask_size_at_level=ask_size,
                    ask_levels_at_touch=ask_levels,
                    bid_levels_at_touch=bid_levels,
                    notional_fit=fit,
                    fit_details=fit_details,
                    min_share_details=min_share_details,
                    pre_touch_features=self._pre_touch_features(
                        token_id,
                        book.last_event_ts_ms or int(time.time() * 1000),
                        book.best_bid,
                        best_ask,
                    ),
                    spot_features=self._spot_features_at_touch(
                        token.asset_slug,
                        book.last_event_ts_ms or int(time.time() * 1000),
                    ),
                )
                self.active_probes[probe_id] = probe
                self.logger.info(
                    "Touch detected token=%s outcome=%s level=%.2f best_ask=%.4f fit10=%s fit50=%s fit100=%s first_touch_only=yes",
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
                move = best_ask - probe.initial_best_ask
                probe.max_favorable_move = max(probe.max_favorable_move, move)
                probe.max_adverse_move = min(probe.max_adverse_move, move)
                if best_ask < probe.level - 1e-9:
                    probe.held_at_or_above_level = False
                    if elapsed <= min(1000, self.args.survival_window_ms):
                        probe.immediate_reversal_flag = True
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
                row = self.csv.write_probe(probe, book)
                if self.sqlite is not None:
                    self.sqlite.write_probe(probe, book, row)
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


def padded_levels(levels: list[tuple[float, float]], depth: int = DEFAULT_DEPTH_LEVELS) -> list[tuple[Optional[float], Optional[float]]]:
    out: list[tuple[Optional[float], Optional[float]]] = list(levels[:depth])
    while len(out) < depth:
        out.append((None, None))
    return out


def depth_size(levels: list[tuple[float, float]], depth: int) -> float:
    return sum(size for _, size in levels[:depth])


def compute_book_metrics(
    bid_levels: list[tuple[float, float]],
    ask_levels: list[tuple[float, float]],
    *,
    best_bid: Optional[float] = None,
    best_ask: Optional[float] = None,
) -> dict[str, Optional[float]]:
    bid = best_bid if best_bid is not None else (bid_levels[0][0] if bid_levels else None)
    ask = best_ask if best_ask is not None else (ask_levels[0][0] if ask_levels else None)
    bid_size = bid_levels[0][1] if bid_levels else None
    ask_size = ask_levels[0][1] if ask_levels else None
    spread = ask - bid if bid is not None and ask is not None else None
    mid = (ask + bid) / 2.0 if bid is not None and ask is not None else None
    microprice = None
    if bid is not None and ask is not None and bid_size is not None and ask_size is not None and bid_size + ask_size > 0:
        microprice = (ask * bid_size + bid * ask_size) / (bid_size + ask_size)

    def imbalance(depth: int) -> Optional[float]:
        bid_sum = depth_size(bid_levels, depth)
        ask_sum = depth_size(ask_levels, depth)
        denom = bid_sum + ask_sum
        return bid_sum / denom if denom > 0 else None

    return {
        "spread_at_touch": spread,
        "mid_price_at_touch": mid,
        "microprice_at_touch": microprice,
        "imbalance_top1": imbalance(1),
        "imbalance_top3": imbalance(3),
        "imbalance_top5": imbalance(5),
    }


def fit_label_from_worst_tick(worst_tick: Optional[int]) -> str:
    if worst_tick is None:
        return "no_book"
    if worst_tick <= 0:
        return "+0_tick"
    if worst_tick == 1:
        return "+1_tick"
    return "+2plus_ticks"


def evaluate_share_entry_details(
    ask_levels: list[tuple[float, float]],
    shares: float,
    *,
    fee_rate_bps: Optional[float] = None,
) -> dict[str, Optional[float] | str]:
    if shares <= 0:
        return {"fit": "invalid_size"}
    if not ask_levels:
        return {"fit": "no_book"}
    remaining_shares = shares
    cost = 0.0
    worst_tick: Optional[int] = None
    best_ask = ask_levels[0][0]
    entry_price = best_ask
    for price, size in ask_levels:
        take = min(size, remaining_shares)
        if take > 0:
            ticks = round((price - best_ask) / 0.01)
            worst_tick = ticks if worst_tick is None else max(worst_tick, ticks)
            cost += take * price
            remaining_shares -= take
        if remaining_shares <= 1e-9:
            break
    if remaining_shares > 1e-9:
        return {"fit": "insufficient_depth", "entry_price": entry_price}
    avg_entry = cost / shares
    fee = cost * ((fee_rate_bps or 0.0) / 10000.0)
    required_hit = (cost + fee) / shares
    return {
        "fit": fit_label_from_worst_tick(worst_tick),
        "entry_price": entry_price,
        "avg_entry_price": avg_entry,
        "cost": cost,
        "worst_tick": worst_tick,
        "estimated_fee": fee,
        "required_hit_rate": required_hit,
    }


def evaluate_notional_entry_details(
    ask_levels: list[tuple[float, float]], notionals: tuple[float, ...]
) -> dict[str, dict[str, Optional[float] | str]]:
    results: dict[str, dict[str, Optional[float] | str]] = {}
    for notional in notionals:
        key = str(int(notional)) if float(notional).is_integer() else str(notional)
        if notional <= 0:
            results[key] = {"fit": "invalid_notional"}
            continue
        if not ask_levels:
            results[key] = {"fit": "no_book"}
            continue
        remaining = notional
        cost = 0.0
        shares = 0.0
        worst_tick: Optional[int] = None
        best_ask = ask_levels[0][0]
        for price, size in ask_levels:
            level_notional = price * size
            take_cost = min(level_notional, remaining)
            if take_cost > 0:
                ticks = round((price - best_ask) / 0.01)
                worst_tick = ticks if worst_tick is None else max(worst_tick, ticks)
                cost += take_cost
                shares += take_cost / price
                remaining -= take_cost
            if remaining <= 1e-9:
                break
        if remaining > 1e-9:
            results[key] = {"fit": "insufficient_depth"}
        else:
            results[key] = {
                "fit": fit_label_from_worst_tick(worst_tick),
                "avg_entry_price": cost / shares if shares > 0 else None,
                "worst_tick": worst_tick,
            }
    return results


def empty_spot_features() -> dict[str, Any]:
    return {
        "spot_price_at_touch": None,
        "spot_source_at_touch": None,
        "spot_latency_ms_at_touch": None,
        "spot_bid_at_touch": None,
        "spot_ask_at_touch": None,
        "spot_return_30s": None,
        "spot_return_1m": None,
        "spot_return_3m": None,
        "spot_return_5m": None,
    }


def latest_spot_before(history: deque[SpotObservation], target_ts_ms: int) -> Optional[SpotObservation]:
    latest: Optional[SpotObservation] = None
    for obs in history:
        if obs.event_ts_ms <= target_ts_ms:
            latest = obs
        else:
            break
    return latest


def spot_features_from_history(
    history: deque[SpotObservation], latest: Optional[SpotObservation], touch_ts_ms: int
) -> dict[str, Any]:
    features = empty_spot_features()
    if latest is None:
        return features
    features.update(
        {
            "spot_price_at_touch": latest.spot_price,
            "spot_source_at_touch": latest.source,
            "spot_latency_ms_at_touch": max(0, latest.ingest_ts_ms - latest.event_ts_ms),
            "spot_bid_at_touch": latest.bid,
            "spot_ask_at_touch": latest.ask,
        }
    )
    for label, seconds in (("30s", 30), ("1m", 60), ("3m", 180), ("5m", 300)):
        base = latest_spot_before(history, touch_ts_ms - seconds * 1000)
        if base is not None and base.spot_price > 0:
            features[f"spot_return_{label}"] = (latest.spot_price / base.spot_price) - 1.0
    return features


def fetch_binance_spot_observation(session: requests.Session, asset_slug: str, symbol: str) -> SpotObservation:
    now_ms = int(time.time() * 1000)
    response = session.get(
        f"{BINANCE_TICKER_BASE}/api/v3/ticker/price",
        params={"symbol": symbol},
        timeout=5,
    )
    response.raise_for_status()
    payload = response.json()
    return SpotObservation(
        asset_slug=asset_slug,
        event_ts_ms=now_ms,
        ingest_ts_ms=int(time.time() * 1000),
        source="binance.ticker",
        spot_price=float(payload["price"]),
    )


def latest_observation_before(history: deque[BookObservation], target_ts_ms: int) -> Optional[BookObservation]:
    latest: Optional[BookObservation] = None
    for obs in history:
        if obs.event_ts_ms <= target_ts_ms:
            latest = obs
        else:
            break
    return latest


def build_touch_row(probe: SurvivalProbe, book: BookState, *, run_id: str) -> dict[str, Any]:
    bid_levels = padded_levels(probe.bid_levels_at_touch)
    ask_levels = padded_levels(probe.ask_levels_at_touch)
    metrics = compute_book_metrics(
        [(p, s) for p, s in probe.bid_levels_at_touch if p is not None and s is not None],
        [(p, s) for p, s in probe.ask_levels_at_touch if p is not None and s is not None],
        best_bid=probe.bid_levels_at_touch[0][0] if probe.bid_levels_at_touch else book.best_bid,
        best_ask=probe.initial_best_ask,
    )
    min_share = probe.min_share_details
    quality = build_book_snapshot_row(book, run_id=run_id, update_kind="touch", probe=probe)
    pre = probe.pre_touch_features
    spot = probe.spot_features or empty_spot_features()
    row: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "probe_id": probe.probe_id,
        "recorded_at": utc_now_iso(),
        "market_id": probe.token.market_id,
        "condition_id": probe.token.condition_id,
        "token_id": probe.token.token_id,
        "outcome": probe.token.outcome,
        "question": probe.token.question,
        "category": probe.token.category,
        "event_slug": probe.token.event_slug,
        "asset_slug": probe.token.asset_slug,
        "universe_mode": probe.token.universe_mode,
        "horizon_bucket": probe.token.horizon_bucket,
        "event_subtype": probe.token.event_subtype,
        "parent_event_slug": probe.token.parent_event_slug,
        "rule_parse_status": probe.token.rule_parse_status,
        "payoff_type": probe.token.payoff_type,
        "touch_level": probe.level,
        "touch_time_iso": iso_from_ms(probe.started_at_ms),
        "duration_seconds": probe.token.duration_seconds,
        "start_time_iso": probe.token.start_time_iso,
        "end_time_iso": probe.token.end_time_iso,
        "fees_enabled": int(probe.token.fees_enabled),
        "fee_rate_bps": probe.token.fee_rate_bps,
        "tick_size": probe.token.tick_size,
        "best_bid_at_touch": probe.bid_levels_at_touch[0][0] if probe.bid_levels_at_touch else book.best_bid,
        "best_ask_at_touch": probe.initial_best_ask,
        "bid_levels_json": json.dumps(probe.bid_levels_at_touch),
        "ask_levels_json": json.dumps(probe.ask_levels_at_touch),
        "ask_size_at_touch_level": probe.initial_ask_size_at_level,
        "fit_5_shares": min_share.get("fit"),
        "entry_price_for_5_shares": min_share.get("entry_price"),
        "avg_entry_price_for_5_shares": min_share.get("avg_entry_price"),
        "cost_for_5_shares": min_share.get("cost"),
        "worst_tick_for_5_shares": min_share.get("worst_tick"),
        "estimated_fee_for_5_shares": min_share.get("estimated_fee"),
        "required_hit_rate_for_5_shares": min_share.get("required_hit_rate"),
        "book_age_ms_at_touch": quality.get("book_age_ms"),
        "event_to_ingest_latency_ms_at_touch": quality.get("event_to_ingest_latency_ms"),
        "missing_depth_flag_at_touch": quality.get("missing_depth_flag"),
        "crossed_book_flag_at_touch": quality.get("crossed_book_flag"),
        "wide_spread_flag_at_touch": quality.get("wide_spread_flag"),
        "book_stale_flag_at_touch": quality.get("book_stale_flag"),
        "best_bid_1s_before": pre.best_bid_1s_before,
        "best_ask_1s_before": pre.best_ask_1s_before,
        "best_bid_delta_1s": pre.best_bid_delta_1s,
        "best_ask_delta_1s": pre.best_ask_delta_1s,
        "best_bid_5s_before": pre.best_bid_5s_before,
        "best_ask_5s_before": pre.best_ask_5s_before,
        "best_bid_delta_5s": pre.best_bid_delta_5s,
        "best_ask_delta_5s": pre.best_ask_delta_5s,
        "best_bid_15s_before": pre.best_bid_15s_before,
        "best_ask_15s_before": pre.best_ask_15s_before,
        "best_bid_delta_15s": pre.best_bid_delta_15s,
        "best_ask_delta_15s": pre.best_ask_delta_15s,
        "best_bid_60s_before": pre.best_bid_60s_before,
        "best_ask_60s_before": pre.best_ask_60s_before,
        "best_bid_delta_60s": pre.best_bid_delta_60s,
        "best_ask_delta_60s": pre.best_ask_delta_60s,
        "max_favorable_move": probe.max_favorable_move,
        "max_adverse_move": probe.max_adverse_move,
        "held_at_or_above_level": int(probe.held_at_or_above_level),
        "immediate_reversal_flag": int(probe.immediate_reversal_flag),
        "spot_price_at_touch": spot.get("spot_price_at_touch"),
        "spot_source_at_touch": spot.get("spot_source_at_touch"),
        "spot_latency_ms_at_touch": spot.get("spot_latency_ms_at_touch"),
        "spot_bid_at_touch": spot.get("spot_bid_at_touch"),
        "spot_ask_at_touch": spot.get("spot_ask_at_touch"),
        "spot_return_30s": spot.get("spot_return_30s"),
        "spot_return_1m": spot.get("spot_return_1m"),
        "spot_return_3m": spot.get("spot_return_3m"),
        "spot_return_5m": spot.get("spot_return_5m"),
        "survived_ms": probe.survived_ms,
        "end_reason": probe.end_reason,
    }
    row.update(metrics)
    for idx, (price, size) in enumerate(bid_levels, start=1):
        row[f"bid_level_{idx}_price"] = price
        row[f"bid_level_{idx}_size"] = size
    for idx, (price, size) in enumerate(ask_levels, start=1):
        row[f"ask_level_{idx}_price"] = price
        row[f"ask_level_{idx}_size"] = size
    for notional in (10, 50, 100):
        details = probe.fit_details.get(str(notional), {})
        row[f"fit_{notional}_usdc"] = details.get("fit", probe.notional_fit.get(str(notional)))
        row[f"avg_entry_price_for_{notional}_usdc"] = details.get("avg_entry_price")
    return row


def build_book_snapshot_row(
    book: BookState,
    *,
    run_id: str,
    update_kind: str,
    probe: Optional[SurvivalProbe] = None,
    stale_book_ms: int = DEFAULT_STALE_BOOK_MS,
    wide_spread_threshold: float = DEFAULT_WIDE_SPREAD,
) -> dict[str, Any]:
    event_ts_ms = book.last_event_ts_ms or (probe.started_at_ms if probe is not None else None)
    token_id = probe.token.token_id if probe is not None else book.token_id
    market_id = probe.token.market_id if probe is not None else book.market_id
    probe_id = probe.probe_id if probe is not None else None
    bid_levels = probe.bid_levels_at_touch if probe is not None else book.bid_levels
    ask_levels = probe.ask_levels_at_touch if probe is not None else book.ask_levels
    best_bid = bid_levels[0][0] if bid_levels else book.best_bid
    best_ask = probe.initial_best_ask if probe is not None else book.best_ask
    now_wall_ms = int(time.time() * 1000)
    now_mono_ms = monotonic_ms()
    book_age_ms = None
    if book.last_ingest_monotonic_ms is not None:
        book_age_ms = max(0, now_mono_ms - book.last_ingest_monotonic_ms)
    event_to_ingest_latency_ms = None
    if event_ts_ms is not None:
        event_to_ingest_latency_ms = max(0, now_wall_ms - event_ts_ms)
    spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
    missing_depth = not bid_levels or not ask_levels
    crossed = best_bid is not None and best_ask is not None and best_bid >= best_ask
    wide_spread = spread is not None and spread >= wide_spread_threshold
    stale = book_age_ms is not None and book_age_ms >= stale_book_ms
    zero_size = any(size <= 0 for _, size in bid_levels + ask_levels)
    suffix = probe_id or str(event_ts_ms or now_wall_ms)
    snapshot_id = f"{run_id}:{token_id}:{update_kind}:{suffix}"
    return {
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "schema_version": SCHEMA_VERSION,
        "probe_id": probe_id,
        "market_id": market_id,
        "token_id": token_id,
        "event_time_iso": iso_from_ms(event_ts_ms) if event_ts_ms is not None else None,
        "event_ts_ms": event_ts_ms,
        "ingest_monotonic_ms": book.last_ingest_monotonic_ms,
        "source": "clob_ws",
        "update_kind": update_kind,
        "book_hash": book.last_book_hash,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_levels_json": json.dumps(bid_levels),
        "ask_levels_json": json.dumps(ask_levels),
        "depth_levels": max(len(bid_levels), len(ask_levels)),
        "book_age_ms": book_age_ms,
        "event_to_ingest_latency_ms": event_to_ingest_latency_ms,
        "missing_depth_flag": int(missing_depth),
        "crossed_book_flag": int(crossed),
        "wide_spread_flag": int(wide_spread),
        "book_stale_flag": int(stale),
        "zero_size_level_flag": int(zero_size),
    }


def _horizon_bucket(duration_seconds: Optional[int]) -> Optional[str]:
    if duration_seconds is None:
        return None
    if duration_seconds <= 6 * 60:
        return "5m"
    if duration_seconds <= 18 * 60:
        return "15m"
    if duration_seconds <= 75 * 60:
        return "1h"
    if duration_seconds <= 36 * 3600:
        return "1d"
    if duration_seconds <= 7 * 24 * 3600:
        return "1-7d"
    if duration_seconds <= 30 * 24 * 3600:
        return "8-30d"
    return ">30d"


def _extract_parent_event_slug(raw: dict[str, Any], event0: Optional[dict[str, Any]]) -> Optional[str]:
    for value in (
        raw.get("parentEventSlug"),
        raw.get("parent_event_slug"),
        (event0 or {}).get("parentEventSlug"),
        (event0 or {}).get("parent_event_slug"),
        (event0 or {}).get("slug"),
    ):
        if value:
            return str(value)
    return None


def _extract_event_subtype(raw: dict[str, Any], event0: Optional[dict[str, Any]], question: str) -> Optional[str]:
    explicit = raw.get("eventSubtype") or raw.get("event_subtype") or (event0 or {}).get("eventSubtype")
    if explicit:
        return str(explicit).lower()
    text = " ".join(str(x or "") for x in (raw.get("slug"), (event0 or {}).get("slug"), question)).lower()
    subtype_patterns = (
        ("spread", ("spread", " ats ", "-ats-", "against the spread")),
        ("total", ("total", "over/under", " o/u ", "-ou-")),
        ("winning_margin", ("winning margin", "margin of victory", "winning-margin")),
        ("player_prop", ("player", "points", "rebounds", "assists", "touchdowns")),
        ("temperature_exact", ("temperature", "highest temperature", "exact temperature")),
        ("temperature_range", ("between", "temperature range")),
    )
    for subtype, needles in subtype_patterns:
        if any(needle in text for needle in needles):
            return subtype
    return None


def _extract_payoff_type(raw: dict[str, Any], event0: Optional[dict[str, Any]], question: str) -> Optional[str]:
    explicit = raw.get("payoffType") or raw.get("payoff_type") or (event0 or {}).get("payoffType")
    if explicit:
        return str(explicit).lower()
    text = " ".join(str(x or "") for x in (question, raw.get("slug"), (event0 or {}).get("slug"))).lower()
    if " or higher" in text or " above " in text or " greater than" in text:
        return "above"
    if " or lower" in text or " below " in text or " less than" in text:
        return "below"
    if "between" in text or "range" in text:
        return "range"
    if "temperature" in text and re.search(r"\b\d+(?:\.\d+)?\s*°?\s*[cf]\b", text):
        return "exact"
    if "spread" in text or "against the spread" in text or "-ats-" in text:
        return "spread"
    if "total" in text or "over/under" in text:
        return "total"
    if "up or down" in text:
        return "above_below_pair"
    if "winner" in text or "win the" in text:
        return "winner"
    return "unknown"


def _rule_parse_status(raw: dict[str, Any], event0: Optional[dict[str, Any]], question: str) -> str:
    payoff_type = _extract_payoff_type(raw, event0, question)
    if payoff_type and payoff_type != "unknown":
        return "parsed"
    return "unknown"


def _extract_asset_slug(raw: dict[str, Any], event0: Optional[dict[str, Any]], question: str) -> Optional[str]:
    candidates = [
        raw.get("assetSlug"),
        raw.get("asset_slug"),
        (event0 or {}).get("assetSlug"),
        (event0 or {}).get("asset_slug"),
    ]
    for value in candidates:
        if value:
            return str(value).lower()
    text = " ".join(
        str(part or "")
        for part in (
            question,
            raw.get("slug"),
            (event0 or {}).get("slug"),
            raw.get("question"),
        )
    ).lower()
    for asset in ("bitcoin", "btc"):
        if asset in text:
            return "btc"
    for asset in ("ethereum", "eth"):
        if asset in text:
            return "eth"
    for asset in ("solana", " sol", "sol-"):
        if asset in text:
            return "sol"
    for asset in ("xrp", "ripple"):
        if asset in text:
            return "xrp"
    for asset in ("bnb", "binance coin"):
        if asset in text:
            return "bnb"
    for asset in ("doge", "dogecoin"):
        if asset in text:
            return "doge"
    if "hype" in text:
        return "hype"
    return None


def _allowed_asset_slugs(values: Optional[list[str]]) -> Optional[set[str]]:
    if not values:
        return None
    allowed: set[str] = set()
    for value in values:
        for part in str(value or "").split(","):
            slug = part.strip().lower()
            if slug:
                allowed.add(slug)
    return allowed or None


def _matches_market_text_filters(
    raw: dict[str, Any],
    event0: Optional[dict[str, Any]],
    question: str,
    *,
    category: Optional[str],
    universe_mode: str,
    include_keywords: Optional[list[str]],
    exclude_keywords: Optional[list[str]],
) -> bool:
    text = " ".join(
        str(part or "")
        for part in (
            question,
            raw.get("slug"),
            raw.get("description"),
            category,
            (event0 or {}).get("slug"),
            (event0 or {}).get("title"),
            (event0 or {}).get("description"),
        )
    ).lower()
    include = _keyword_set(include_keywords)
    if not include and universe_mode == "weather_temperature":
        include = {"temperature", "weather", "highest temperature", "low temperature"}
    if include and not any(_keyword_matches(text, keyword) for keyword in include):
        return False
    exclude = _keyword_set(exclude_keywords)
    if exclude and any(_keyword_matches(text, keyword) for keyword in exclude):
        return False
    return True


def _keyword_matches(text: str, keyword: str) -> bool:
    if " " in keyword or "-" in keyword:
        return keyword in text
    return re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", text) is not None


def _matches_payoff_type_filter(payoff_type: Optional[str], values: Optional[list[str]]) -> bool:
    allowed = _keyword_set(values)
    if not allowed:
        return True
    return str(payoff_type or "").lower() in allowed


def _keyword_set(values: Optional[list[str]]) -> set[str]:
    out: set[str] = set()
    for value in values or []:
        for part in str(value or "").split(","):
            keyword = part.strip().lower()
            if keyword:
                out.add(keyword)
    return out


def _extract_category(raw: dict[str, Any], event0: Optional[dict[str, Any]]) -> Optional[str]:
    candidates = [
        raw.get("category"),
        raw.get("categorySlug"),
        raw.get("category_slug"),
        (event0 or {}).get("category"),
        (event0 or {}).get("categorySlug"),
    ]
    for value in candidates:
        if value:
            return str(value)
    tags = raw.get("tags") or (event0 or {}).get("tags") or []
    if isinstance(tags, list) and tags:
        first = tags[0]
        if isinstance(first, dict):
            return str(first.get("slug") or first.get("label") or first.get("name") or "") or None
        return str(first)
    return None


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


def default_output_csv_path() -> Path:
    return DATA_DIR / DEFAULT_OUTPUT_SUBDIR / DEFAULT_OUTPUT_BASENAME


def default_output_sqlite_path() -> Path:
    return default_output_csv_path().with_suffix(".sqlite3")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-csv", default=str(default_output_csv_path()))
    parser.add_argument("--output-sqlite", default=str(default_output_sqlite_path()), help="Structured sidecar output path; pass empty string to disable")
    parser.add_argument("--run-id", default=None, help="Collection run id for CSV/SQLite rows")
    parser.add_argument("--level-preset", choices=sorted(LEVEL_PRESETS), default="crypto_15m_touch")
    parser.add_argument("--universe-mode", choices=UNIVERSE_MODES, default="crypto_15m")
    parser.add_argument("--levels", nargs="*", type=float, default=None, help="Explicit levels override --level-preset")
    parser.add_argument("--notionals", nargs="*", type=float, default=list(DEFAULT_NOTIONALS))
    parser.add_argument("--min-shares", type=float, default=DEFAULT_MIN_SHARES)
    parser.add_argument("--depth-levels", type=int, default=DEFAULT_DEPTH_LEVELS)
    parser.add_argument(
        "--book-snapshot-interval-ms",
        type=int,
        default=DEFAULT_BOOK_SNAPSHOT_INTERVAL_MS,
        help="Persist periodic top-N book snapshots to SQLite sidecar; 0 disables periodic snapshots",
    )
    parser.add_argument("--stale-book-ms", type=int, default=DEFAULT_STALE_BOOK_MS)
    parser.add_argument("--wide-spread-threshold", type=float, default=DEFAULT_WIDE_SPREAD)
    parser.add_argument("--survival-window-ms", type=int, default=DEFAULT_SURVIVAL_WINDOW_MS)
    parser.add_argument("--sampling-interval-ms", type=int, default=DEFAULT_SAMPLING_INTERVAL_MS)
    parser.add_argument("--refresh-interval-sec", type=int, default=DEFAULT_REFRESH_INTERVAL_SEC)
    parser.add_argument("--heartbeat-interval-sec", type=int, default=60)
    parser.add_argument("--min-duration-seconds", type=int, default=DEFAULT_DURATION_MIN)
    parser.add_argument("--max-duration-seconds", type=int, default=DEFAULT_DURATION_MAX)
    parser.add_argument("--max-seconds-until-start", type=int, default=1800)
    parser.add_argument(
        "--max-seconds-to-end",
        type=int,
        default=1800,
        help="Only used when --duration-metric=time_remaining; recurring 15m markets expose long-horizon endDate values",
    )
    parser.add_argument("--duration-metric", choices=("lifecycle", "time_remaining", "implied_series"), default="implied_series")
    parser.add_argument(
        "--asset-slug",
        action="append",
        default=None,
        help="Restrict discovery to asset slugs; repeatable and comma-separated values are accepted, e.g. --asset-slug btc,eth,sol,xrp",
    )
    parser.add_argument(
        "--market-keyword",
        action="append",
        default=None,
        help="Require at least one keyword in question/slug/category text; repeatable and comma-separated values are accepted",
    )
    parser.add_argument(
        "--exclude-market-keyword",
        action="append",
        default=None,
        help="Exclude markets whose question/slug/category text contains any keyword; repeatable and comma-separated values are accepted",
    )
    parser.add_argument(
        "--payoff-type",
        action="append",
        default=None,
        help="Restrict parsed payoff types; repeatable and comma-separated values are accepted, e.g. range,above,below",
    )
    parser.add_argument("--spot-feed", choices=("none", "binance"), default="none", help="Optional live spot feed for crypto spot context")
    parser.add_argument("--spot-poll-interval-sec", type=float, default=1.0, help="Polling interval for --spot-feed=binance")
    parser.add_argument(
        "--spot-asset-slug",
        action="append",
        default=None,
        help="Restrict spot feed assets; repeatable and comma-separated. Defaults to --asset-slug or active token asset slugs.",
    )
    parser.add_argument("--discovery-order", default="createdAt")
    parser.add_argument("--discovery-ascending", action="store_true")
    parser.add_argument("--max-discovery-rows", type=int, default=DEFAULT_MAX_DISCOVERY_ROWS)
    parser.add_argument("--min-volume-usdc", type=float, default=None, help="Skip markets with lifetime volume below this threshold (USDC)")
    parser.add_argument("--max-volume-usdc", type=float, default=None, help="Skip markets with lifetime volume above this threshold (USDC)")
    parser.add_argument("--require-recurrence", action="store_true", default=True)
    parser.add_argument("--no-require-recurrence", dest="require_recurrence", action="store_false")
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--log-level", default="INFO")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.levels is None:
        args.levels = LEVEL_PRESETS[args.level_preset]
    else:
        args.levels = tuple(float(x) for x in args.levels)
    args.notionals = tuple(float(x) for x in args.notionals)
    if args.output_sqlite == "":
        args.output_sqlite = None
    if args.run_id is None:
        args.run_id = f"live_depth_survival_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    collector = LiveDepthCollector(args)
    asyncio.run(collector.run())


if __name__ == "__main__":
    main()
