from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests

GAMMA_BASE = "https://gamma-api.polymarket.com"


@dataclass(frozen=True)
class UniverseFilter:
    allowed_assets: tuple[str, ...] = ("bitcoin", "ethereum")


@dataclass(frozen=True)
class DurationWindow:
    min_seconds: int = 850
    max_seconds: int = 960
    require_recurrence: bool = True
    duration_metric: str = "implied_series"
    max_seconds_until_start: int = 900
    max_seconds_to_end: int = 960


@dataclass(frozen=True)
class MarketMetadata:
    market_id: str
    condition_id: str
    question: str
    token_yes_id: str
    token_no_id: str
    start_time_ms: int | None
    end_time_ms: int
    asset_slug: str | None
    is_active: bool
    duration_seconds: int | None
    fees_enabled: bool
    fee_rate_bps: float | None
    tick_size: float | None
    recurrence: str | None = None
    series_slug: str | None = None
    yes_outcome_name: str | None = None
    no_outcome_name: str | None = None


@dataclass(frozen=True)
class DiscoveryStats:
    rows_seen: int = 0
    skipped_duplicate: int = 0
    skipped_outcomes: int = 0
    skipped_missing_end: int = 0
    skipped_bad_time: int = 0
    skipped_nonpositive_remaining: int = 0
    skipped_duration_window: int = 0
    skipped_recurrence: int = 0
    skipped_asset_filter: int = 0
    eligible_markets: int = 0


def parse_market_discovery_rows(
    rows: list[dict[str, Any]],
    *,
    universe_filter: UniverseFilter | None = None,
    duration_window: DurationWindow | None = None,
    now_ts: float | None = None,
) -> tuple[list[MarketMetadata], DiscoveryStats]:
    universe_filter = universe_filter or UniverseFilter()
    duration_window = duration_window or DurationWindow()
    now_ts = float(time.time() if now_ts is None else now_ts)

    markets: list[MarketMetadata] = []
    stats = {
        "rows_seen": 0,
        "skipped_duplicate": 0,
        "skipped_outcomes": 0,
        "skipped_missing_end": 0,
        "skipped_bad_time": 0,
        "skipped_nonpositive_remaining": 0,
        "skipped_duration_window": 0,
        "skipped_recurrence": 0,
        "skipped_asset_filter": 0,
        "eligible_markets": 0,
    }
    seen_market_ids: set[str] = set()
    seen_recurrence_keys: set[tuple[str, str, str]] = set()

    for raw in rows:
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
        if duration_window.require_recurrence and not (is_target_recurrence or is_updown_question):
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

        target_metric = _duration_metric_value(
            duration_metric=duration_window.duration_metric,
            remaining_seconds=remaining_seconds,
            duration_seconds=duration_seconds,
            implied_duration_seconds=implied_duration_seconds,
        )
        if target_metric is None:
            stats["skipped_bad_time"] += 1
            continue
        if target_metric < duration_window.min_seconds or target_metric > duration_window.max_seconds:
            stats["skipped_duration_window"] += 1
            continue
        if seconds_until_start is not None and seconds_until_start > duration_window.max_seconds_until_start:
            stats["skipped_duration_window"] += 1
            continue
        if duration_window.duration_metric == "time_remaining" and remaining_seconds > duration_window.max_seconds_to_end:
            stats["skipped_duration_window"] += 1
            continue

        recurrence_key = _recurrence_key(series_slug, start_iso, end_iso)
        if recurrence_key is not None:
            if recurrence_key in seen_recurrence_keys:
                stats["skipped_duplicate"] += 1
                continue
            seen_recurrence_keys.add(recurrence_key)

        asset_slug = infer_asset_slug(raw=raw, question=question, series_slug=series_slug)
        if universe_filter.allowed_assets and asset_slug not in set(universe_filter.allowed_assets):
            stats["skipped_asset_filter"] += 1
            continue

        condition_id = str(raw.get("conditionId") or market_id)
        fees_enabled = bool(raw.get("feesEnabled"))
        tick_size = _parse_float(raw.get("orderPriceMinTickSize"))
        fee_rate_bps = _extract_fee_rate_bps(raw)
        token_yes_id, token_no_id, yes_outcome_name, no_outcome_name = _select_yes_no_tokens(token_ids, outcomes)
        markets.append(
            MarketMetadata(
                market_id=market_id,
                condition_id=condition_id,
                question=question,
                token_yes_id=token_yes_id,
                token_no_id=token_no_id,
                start_time_ms=int(start_ts * 1000) if start_ts is not None else None,
                end_time_ms=int(end_ts * 1000),
                asset_slug=asset_slug,
                is_active=bool(raw.get("active", True)),
                duration_seconds=implied_duration_seconds if implied_duration_seconds is not None else duration_seconds,
                fees_enabled=fees_enabled,
                fee_rate_bps=fee_rate_bps,
                tick_size=tick_size,
                recurrence=str(recurrence) if recurrence is not None else None,
                series_slug=str(series_slug) if series_slug is not None else None,
                yes_outcome_name=yes_outcome_name,
                no_outcome_name=no_outcome_name,
            )
        )
        stats["eligible_markets"] += 1
        seen_market_ids.add(market_id)

    return markets, DiscoveryStats(**stats)


def discover_short_horizon_markets_sync(
    universe_filter: UniverseFilter | None = None,
    duration_window: DurationWindow | None = None,
    max_rows: int = 20_000,
    *,
    order: str = "volume",
    ascending: bool = False,
    session: requests.Session | None = None,
    now_ts: float | None = None,
) -> list[MarketMetadata]:
    universe_filter = universe_filter or UniverseFilter()
    duration_window = duration_window or DurationWindow()
    own_session = session is None
    session = session or requests.Session()

    try:
        raw_rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            response = session.get(
                f"{GAMMA_BASE}/markets",
                params={
                    "limit": 100,
                    "offset": offset,
                    "active": "true",
                    "closed": "false",
                    "archived": "false",
                    "order": order,
                    "ascending": str(ascending).lower(),
                },
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list) or not payload:
                break
            raw_rows.extend(item for item in payload if isinstance(item, dict))
            offset += len(payload)
            if len(payload) < 100 or offset >= max_rows:
                break
            time.sleep(0.05)
        markets, _ = parse_market_discovery_rows(
            raw_rows,
            universe_filter=universe_filter,
            duration_window=duration_window,
            now_ts=now_ts,
        )
        return markets
    finally:
        if own_session:
            session.close()


async def discover_short_horizon_markets(
    universe_filter: UniverseFilter | None = None,
    duration_window: DurationWindow | None = None,
    max_rows: int = 20_000,
    *,
    order: str = "volume",
    ascending: bool = False,
    session: requests.Session | None = None,
    now_ts: float | None = None,
) -> list[MarketMetadata]:
    return await asyncio.to_thread(
        discover_short_horizon_markets_sync,
        universe_filter,
        duration_window,
        max_rows,
        order=order,
        ascending=ascending,
        session=session,
        now_ts=now_ts,
    )


def infer_asset_slug(*, raw: dict[str, Any], question: str, series_slug: str | None) -> str | None:
    haystacks = [str(question or "").lower(), str(series_slug or "").lower()]
    tags = raw.get("tags") or []
    for tag in tags:
        if isinstance(tag, dict):
            haystacks.extend(
                [
                    str(tag.get("slug") or "").lower(),
                    str(tag.get("label") or "").lower(),
                    str(tag.get("name") or "").lower(),
                ]
            )
        else:
            haystacks.append(str(tag).lower())
    for needle, asset in (("bitcoin", "bitcoin"), ("ethereum", "ethereum"), ("ether", "ethereum")):
        if any(needle in text for text in haystacks):
            return asset
    return None


def parse_iso_to_ts(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _extract_fee_rate_bps(raw: dict[str, Any]) -> float | None:
    fee_schedule = raw.get("feeSchedule") or raw.get("fee_schedule")
    if isinstance(fee_schedule, dict):
        rate = _parse_float(fee_schedule.get("rate"))
        if rate is not None:
            return rate * 10000.0
    if raw.get("takerBaseFee") is not None:
        return _parse_float(raw.get("takerBaseFee"))
    return None


def _duration_metric_value(*, duration_metric: str, remaining_seconds: int, duration_seconds: int | None, implied_duration_seconds: int | None) -> int | None:
    if duration_metric == "time_remaining":
        return remaining_seconds
    if duration_metric == "lifecycle":
        return duration_seconds
    return implied_duration_seconds if implied_duration_seconds is not None else duration_seconds


def _recurrence_key(series_slug: str | None, start_iso: str | None, end_iso: str | None) -> tuple[str, str, str] | None:
    if not series_slug or not start_iso or not end_iso:
        return None
    return (str(series_slug).lower(), str(start_iso), str(end_iso))


def _select_yes_no_tokens(token_ids: list[Any], outcomes: list[Any]) -> tuple[str, str, str | None, str | None]:
    normalized = [(str(token_id), str(outcome)) for token_id, outcome in zip(token_ids, outcomes)]
    yes_token = next(((token_id, outcome) for token_id, outcome in normalized if outcome.strip().lower() == "yes"), None)
    no_token = next(((token_id, outcome) for token_id, outcome in normalized if outcome.strip().lower() == "no"), None)
    if yes_token and no_token:
        return yes_token[0], no_token[0], yes_token[1], no_token[1]
    first = normalized[0]
    second = normalized[1]
    return first[0], second[0], first[1], second[1]


def _load_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    try:
        return json.loads(value)
    except Exception:
        return []


def _parse_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


__all__ = [
    "DiscoveryStats",
    "DurationWindow",
    "MarketMetadata",
    "UniverseFilter",
    "discover_short_horizon_markets",
    "discover_short_horizon_markets_sync",
    "infer_asset_slug",
    "parse_market_discovery_rows",
]
