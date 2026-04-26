#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

GAMMA_BASE = "https://gamma-api.polymarket.com"
DEFAULT_COLLECTOR_GLOB = "/home/polybot/.polybot/short_horizon/phase0/live_depth_survival_*.csv"
DEFAULT_OUTPUT_PATH = Path("v2/short_horizon/data/market_resolutions.sqlite3")


@dataclass(frozen=True)
class MarketResolution:
    market_id: str
    condition_id: str | None
    token_yes_id: str | None
    token_no_id: str | None
    outcome_resolved_at: str | None
    outcome_token_id: str | None
    outcome_price_yes: float | None
    outcome_price_no: float | None
    question: str | None
    raw_json: str | None
    fetched_at: str


@dataclass(frozen=True)
class BackfillSummary:
    requested: int
    already_present: int
    fetched: int
    resolved: int
    unresolved: int
    output_path: Path


def backfill_market_resolutions(
    market_ids: Iterable[str],
    *,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    gamma_base: str = GAMMA_BASE,
    sleep_seconds: float = 0.05,
    force: bool = False,
    fetch_market: Callable[[str], dict[str, Any] | None] | None = None,
) -> BackfillSummary:
    ids = _dedupe_market_ids(market_ids)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(out)
    _ensure_schema(conn)

    existing = set() if force else _existing_market_ids(conn)
    requested = len(ids)
    already_present = sum(1 for market_id in ids if market_id in existing)
    fetched = 0
    resolved = 0
    unresolved = 0
    fetch_market = fetch_market or (lambda market_id: fetch_gamma_market(market_id, gamma_base=gamma_base))

    for market_id in ids:
        if market_id in existing:
            continue
        raw = fetch_market(market_id)
        resolution = normalize_gamma_market(market_id, raw)
        _upsert_resolution(conn, resolution)
        fetched += 1
        if resolution.outcome_token_id:
            resolved += 1
        else:
            unresolved += 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    conn.commit()
    conn.close()
    return BackfillSummary(
        requested=requested,
        already_present=already_present,
        fetched=fetched,
        resolved=resolved,
        unresolved=unresolved,
        output_path=out,
    )


def fetch_gamma_market(market_id: str, *, gamma_base: str = GAMMA_BASE, timeout_seconds: float = 30.0) -> dict[str, Any] | None:
    # For closed historical 15m markets, Gamma only returned rows reliably when
    # closed/active filters were present during P6-2a validation.
    params = {
        "limit": "1",
        "closed": "true",
        "active": "false",
        "id": str(market_id),
    }
    url = f"{gamma_base.rstrip('/')}/markets?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "polymarket-bot-p6-2a-resolution-backfill/1.0"})
    with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - fixed HTTPS default endpoint, configurable for tests.
        payload = json.loads(response.read().decode("utf-8"))
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[0]
    return None


def normalize_gamma_market(market_id: str, raw: dict[str, Any] | None) -> MarketResolution:
    fetched_at = _utc_now_iso()
    if not raw:
        return MarketResolution(
            market_id=str(market_id),
            condition_id=None,
            token_yes_id=None,
            token_no_id=None,
            outcome_resolved_at=None,
            outcome_token_id=None,
            outcome_price_yes=None,
            outcome_price_no=None,
            question=None,
            raw_json=None,
            fetched_at=fetched_at,
        )

    token_ids = _load_json_list(raw.get("clobTokenIds"))
    outcomes = _load_json_list(raw.get("outcomes"))
    prices = [_parse_float(value) for value in _load_json_list(raw.get("outcomePrices"))]
    token_yes_id, token_no_id = _select_yes_no_tokens(token_ids, outcomes)
    outcome_price_yes = _price_for_token(token_yes_id, token_ids, prices)
    outcome_price_no = _price_for_token(token_no_id, token_ids, prices)
    outcome_token_id = _resolved_token_id(token_ids, prices)
    if str(raw.get("umaResolutionStatus") or "").lower() not in {"resolved", ""} and outcome_token_id is None:
        outcome_token_id = None
    return MarketResolution(
        market_id=str(raw.get("id") or market_id),
        condition_id=_optional_str(raw.get("conditionId")),
        token_yes_id=token_yes_id,
        token_no_id=token_no_id,
        outcome_resolved_at=_optional_str(raw.get("closedTime") or raw.get("umaEndDate") or raw.get("updatedAt") or raw.get("endDate")),
        outcome_token_id=outcome_token_id,
        outcome_price_yes=outcome_price_yes,
        outcome_price_no=outcome_price_no,
        question=_optional_str(raw.get("question")),
        raw_json=json.dumps(raw, sort_keys=True),
        fetched_at=fetched_at,
    )


def load_market_ids_from_csv(path: str | Path) -> list[str]:
    with Path(path).open(newline="") as fh:
        reader = csv.DictReader(fh)
        if "market_id" not in (reader.fieldnames or []):
            raise ValueError(f"CSV {path} does not contain a market_id column")
        return _dedupe_market_ids(row.get("market_id") for row in reader)


def latest_collector_csv(glob_pattern: str = DEFAULT_COLLECTOR_GLOB) -> Path:
    paths = [path for path in Path("/").glob(glob_pattern.lstrip("/")) if path.is_file() and "probe" not in path.name]
    if not paths:
        raise FileNotFoundError(f"no collector CSV found for pattern {glob_pattern}")
    return max(paths, key=lambda path: path.stat().st_mtime)


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_resolutions (
            market_id TEXT PRIMARY KEY,
            condition_id TEXT,
            token_yes_id TEXT,
            token_no_id TEXT,
            outcome_resolved_at TEXT,
            outcome_token_id TEXT,
            outcome_price_yes REAL,
            outcome_price_no REAL,
            question TEXT,
            raw_json TEXT,
            fetched_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_resolutions_outcome_token ON market_resolutions(outcome_token_id)")


def _existing_market_ids(conn: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in conn.execute("SELECT market_id FROM market_resolutions")}


def _upsert_resolution(conn: sqlite3.Connection, item: MarketResolution) -> None:
    conn.execute(
        """
        INSERT INTO market_resolutions (
            market_id, condition_id, token_yes_id, token_no_id, outcome_resolved_at,
            outcome_token_id, outcome_price_yes, outcome_price_no, question, raw_json, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(market_id) DO UPDATE SET
            condition_id = excluded.condition_id,
            token_yes_id = excluded.token_yes_id,
            token_no_id = excluded.token_no_id,
            outcome_resolved_at = excluded.outcome_resolved_at,
            outcome_token_id = excluded.outcome_token_id,
            outcome_price_yes = excluded.outcome_price_yes,
            outcome_price_no = excluded.outcome_price_no,
            question = excluded.question,
            raw_json = excluded.raw_json,
            fetched_at = excluded.fetched_at
        """,
        (
            item.market_id,
            item.condition_id,
            item.token_yes_id,
            item.token_no_id,
            item.outcome_resolved_at,
            item.outcome_token_id,
            item.outcome_price_yes,
            item.outcome_price_no,
            item.question,
            item.raw_json,
            item.fetched_at,
        ),
    )


def _dedupe_market_ids(values: Iterable[str | None]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _select_yes_no_tokens(token_ids: list[Any], outcomes: list[Any]) -> tuple[str | None, str | None]:
    pairs = [(str(token_id), str(outcome)) for token_id, outcome in zip(token_ids, outcomes)]
    if not pairs:
        return None, None
    yes = next((token_id for token_id, outcome in pairs if outcome.strip().lower() in {"yes", "up"}), None)
    no = next((token_id for token_id, outcome in pairs if outcome.strip().lower() in {"no", "down"}), None)
    if yes and no:
        return yes, no
    if len(pairs) >= 2:
        return pairs[0][0], pairs[1][0]
    return pairs[0][0], None


def _resolved_token_id(token_ids: list[Any], prices: list[float | None]) -> str | None:
    candidates = [(str(token_id), price) for token_id, price in zip(token_ids, prices) if price is not None]
    winners = [token_id for token_id, price in candidates if price >= 0.999]
    if len(winners) == 1:
        return winners[0]
    if candidates:
        token_id, price = max(candidates, key=lambda item: item[1] if item[1] is not None else -1.0)
        if price is not None and price >= 0.5:
            return token_id
    return None


def _price_for_token(token_id: str | None, token_ids: list[Any], prices: list[float | None]) -> float | None:
    if token_id is None:
        return None
    for candidate, price in zip(token_ids, prices):
        if str(candidate) == token_id:
            return price
    return None


def _load_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    try:
        decoded = json.loads(value)
    except Exception:
        return []
    return decoded if isinstance(decoded, list) else []


def _parse_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _count_rows(path: Path) -> tuple[int, int]:
    conn = sqlite3.connect(path)
    total = int(conn.execute("SELECT COUNT(*) FROM market_resolutions").fetchone()[0])
    resolved = int(conn.execute("SELECT COUNT(*) FROM market_resolutions WHERE outcome_token_id IS NOT NULL").fetchone()[0])
    conn.close()
    return total, resolved


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Polymarket Gamma market resolutions for short-horizon touch datasets")
    parser.add_argument("--csv-path", default=None, help="Collector CSV path; defaults to latest non-probe live_depth_survival_*.csv")
    parser.add_argument("--market-id", action="append", default=[], help="Specific market_id to fetch; can be repeated")
    parser.add_argument("--market-ids-file", default=None, help="Text file with one market_id per line")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="SQLite output path")
    parser.add_argument("--gamma-base", default=GAMMA_BASE, help="Gamma API base URL")
    parser.add_argument("--sleep-seconds", type=float, default=0.05, help="Sleep between Gamma requests")
    parser.add_argument("--limit", type=int, default=None, help="Limit fetched market ids for smoke tests")
    parser.add_argument("--force", action="store_true", help="Refetch ids already present in the output DB")
    args = parser.parse_args()

    ids: list[str] = []
    if args.market_id:
        ids.extend(args.market_id)
    if args.market_ids_file:
        ids.extend(Path(args.market_ids_file).read_text().splitlines())
    if not ids:
        csv_path = Path(args.csv_path) if args.csv_path else latest_collector_csv()
        ids.extend(load_market_ids_from_csv(csv_path))
    ids = _dedupe_market_ids(ids)
    if args.limit is not None:
        ids = ids[: max(0, int(args.limit))]

    summary = backfill_market_resolutions(
        ids,
        output_path=args.output,
        gamma_base=args.gamma_base,
        sleep_seconds=args.sleep_seconds,
        force=args.force,
    )
    total, resolved = _count_rows(summary.output_path)
    print(
        json.dumps(
            {
                "requested": summary.requested,
                "already_present": summary.already_present,
                "fetched": summary.fetched,
                "resolved": summary.resolved,
                "unresolved": summary.unresolved,
                "output_path": str(summary.output_path),
                "total_rows": total,
                "resolved_rows": resolved,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
