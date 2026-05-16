#!/usr/bin/env python3
"""Build a technical + economic report for a black_swan paper run.

Read-only by design. The script inspects the short_horizon runtime SQLite DB,
the optional screener log DB, and the matching JSON log file.

Typical prod usage:

    cd /home/polybot/claude-polymarket
    POLYMARKET_DATA_DIR=/home/polybot/.polybot \
      ./.venv/bin/python scripts/paper_report.py --paper-db latest --output /tmp/paper_report.md
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


ISO_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"
TECH_LEVELS = {"warning", "error", "critical"}
LIVE_STATES = {"accepted", "partially_filled", "pending_send", "cancel_requested"}
FINAL_STATES = {"filled", "cancel_confirmed", "rejected", "expired", "cancel_rejected", "unknown"}


@dataclass(frozen=True)
class RunContext:
    repo: Path
    paper_db: Path
    run_id: str
    started_at: str | None
    run_started_ts: int | None
    log_path: Path | None
    screener_db: Path | None


@dataclass
class LogSummary:
    parsed_lines: int = 0
    malformed_lines: int = 0
    first_ts: str | None = None
    last_ts: str | None = None
    levels: Counter[str] = None  # type: ignore[assignment]
    events: Counter[str] = None  # type: ignore[assignment]
    warning_events: Counter[str] = None  # type: ignore[assignment]
    error_signatures: Counter[str] = None  # type: ignore[assignment]
    api_error_signatures: Counter[str] = None  # type: ignore[assignment]
    ws_events: Counter[str] = None  # type: ignore[assignment]
    latest_by_event: dict[str, dict[str, Any]] = None  # type: ignore[assignment]
    held_book: dict[str, Any] | None = None
    universe_latest: dict[str, Any] | None = None
    selector_stage: Counter[str] = None  # type: ignore[assignment]
    selector_reject_reason: Counter[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self.levels = Counter()
        self.events = Counter()
        self.warning_events = Counter()
        self.error_signatures = Counter()
        self.api_error_signatures = Counter()
        self.ws_events = Counter()
        self.latest_by_event = {}
        self.selector_stage = Counter()
        self.selector_reject_reason = Counter()


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def parse_iso(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def iso_to_epoch(value: str | None) -> int | None:
    parsed = parse_iso(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return int(parsed.timestamp())


def age_text(value: str | None, *, now: dt.datetime | None = None) -> str:
    parsed = parse_iso(value)
    if parsed is None:
        return "n/a"
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    now = now or utc_now()
    seconds = max(0, int((now - parsed).total_seconds()))
    if seconds < 90:
        return f"{seconds}s"
    minutes = seconds / 60
    if minutes < 90:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    if hours < 48:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


def fmt_num(value: Any, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(num):
        return "n/a"
    if abs(num - round(num)) < 10 ** (-(digits + 1)):
        return f"{int(round(num)):,}".replace(",", " ")
    return f"{num:,.{digits}f}".replace(",", " ").rstrip("0").rstrip(".")


def fmt_money(value: Any, digits: int = 2, *, signed: bool = False) -> str:
    if value is None:
        return "n/a"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "n/a"
    sign = "+" if signed and num >= 0 else ""
    return f"{sign}${num:,.{digits}f}".replace(",", " ")


def fmt_pct(value: Any, digits: int = 1, *, signed: bool = False) -> str:
    if value is None:
        return "n/a"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "n/a"
    sign = "+" if signed and num >= 0 else ""
    return f"{sign}{num:.{digits}f}%"


def one_line(text: Any, limit: int = 180) -> str:
    s = re.sub(r"\s+", " ", str(text or "")).strip()
    return s if len(s) <= limit else s[: limit - 1] + "…"


def connect_ro(path: Path | None) -> sqlite3.Connection | None:
    if path is None or not path.exists():
        return None
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def rows(con: sqlite3.Connection | None, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    if con is None:
        return []
    return list(con.execute(sql, tuple(params)))


def scalar(con: sqlite3.Connection | None, sql: str, params: Iterable[Any] = ()) -> Any:
    if con is None:
        return None
    row = con.execute(sql, tuple(params)).fetchone()
    return row[0] if row else None


def latest_paper_db(data_dir: Path) -> Path | None:
    roots = [data_dir / "swan_v2", Path.home() / ".polybot" / "swan_v2"]
    candidates: list[Path] = []
    for root in roots:
        candidates.extend(Path(p) for p in glob.glob(str(root / "black_swan_v1_paper_*.sqlite3")))
    candidates = [p for p in candidates if p.exists()]
    return max(candidates, key=lambda p: p.stat().st_mtime) if candidates else None


def infer_log_path(run_id: str, data_dir: Path) -> Path | None:
    suffix = run_id.split("_paper_", 1)[-1] if "_paper_" in run_id else run_id
    log_dir = data_dir / "logs"
    candidates = [
        log_dir / f"paper_bs3_{suffix}.log",
        log_dir / f"paper_bs2_{suffix}.log",
        log_dir / f"paper_bs_{suffix}.log",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    globbed = [Path(p) for p in glob.glob(str(log_dir / f"*{suffix}*.log*"))]
    globbed = [p for p in globbed if p.exists()]
    return max(globbed, key=lambda p: p.stat().st_mtime) if globbed else None


def repo_git_sha(repo: Path) -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=repo, text=True).strip()
    except Exception:
        return None


def get_run_context(args: argparse.Namespace) -> RunContext:
    repo = Path(args.repo).resolve()
    data_dir = Path(args.data_dir or os.environ.get("POLYMARKET_DATA_DIR") or os.environ.get("POLYBOT_DATA_DIR") or Path.home() / ".polybot").expanduser()
    paper_db = latest_paper_db(data_dir) if args.paper_db == "latest" else Path(args.paper_db).expanduser()
    if paper_db is None or not paper_db.exists():
        raise SystemExit(f"paper DB not found: {args.paper_db}")
    con = connect_ro(paper_db)
    run = rows(con, "select * from runs order by started_at desc limit 1")
    if con is not None:
        con.close()
    if not run:
        run_id = paper_db.stem
        started_at = None
    else:
        run_id = str(run[0]["run_id"])
        started_at = run[0]["started_at"]
    log_path = None if args.no_log else (Path(args.log).expanduser() if args.log else infer_log_path(run_id, data_dir))
    screener_db = None
    if args.screener_db:
        screener_db = Path(args.screener_db).expanduser()
    else:
        candidate = data_dir / "swan_v2" / "swan_screener_log.sqlite3"
        screener_db = candidate if candidate.exists() else None
    return RunContext(
        repo=repo,
        paper_db=paper_db,
        run_id=run_id,
        started_at=started_at,
        run_started_ts=iso_to_epoch(started_at),
        log_path=log_path if log_path and log_path.exists() else None,
        screener_db=screener_db if screener_db and screener_db.exists() else None,
    )


def classify_api_error(error_text: str) -> str | None:
    text = error_text.lower()
    if "clob.polymarket.com" not in text and "gamma" not in text and "http" not in text:
        return None
    if "404" in text:
        return "HTTP 404"
    if "429" in text or "rate" in text:
        return "rate-limit/429"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    m = re.search(r"\b(5\d\d)\b", text)
    if m:
        return f"HTTP {m.group(1)}"
    m = re.search(r"\b(4\d\d)\b", text)
    if m:
        return f"HTTP {m.group(1)}"
    return "api/http"


def stream_log(path: Path | None, *, max_bytes: int | None = None) -> LogSummary:
    summary = LogSummary()
    if path is None or not path.exists():
        return summary
    start_offset = 0
    if max_bytes and max_bytes > 0:
        size = path.stat().st_size
        start_offset = max(0, size - max_bytes)
    with path.open("rb") as fh:
        if start_offset:
            fh.seek(start_offset)
            fh.readline()  # discard partial line
        for raw in fh:
            try:
                line = raw.decode("utf-8", errors="replace").strip()
            except Exception:
                summary.malformed_lines += 1
                continue
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                summary.malformed_lines += 1
                continue
            if not isinstance(obj, dict):
                continue
            summary.parsed_lines += 1
            event = str(obj.get("event") or obj.get("event_type") or "unknown")
            level = str(obj.get("level") or "info").lower()
            logger = str(obj.get("logger") or "")
            ts = obj.get("timestamp") or obj.get("event_time") or obj.get("time")
            if ts:
                ts = str(ts)
                summary.first_ts = summary.first_ts or ts
                summary.last_ts = ts
            summary.levels[level] += 1
            summary.events[event] += 1
            summary.latest_by_event[event] = obj
            if event.startswith("ws_") or "websocket" in logger:
                summary.ws_events[event] += 1
            if event == "ws_universe_updated":
                summary.universe_latest = obj
            if event == "held_book_freshness_check":
                summary.held_book = obj
            if event == "ws_universe_selector_decision":
                summary.selector_stage[str(obj.get("stage") or "unknown")] += 1
                reject = obj.get("reject_reason")
                if reject:
                    summary.selector_reject_reason[str(reject)] += 1
            if level in TECH_LEVELS:
                summary.warning_events[event] += 1
                err = obj.get("error") or obj.get("exception") or obj.get("message") or event
                signature = f"{event}: {one_line(err, 140)}"
                summary.error_signatures[signature] += 1
                api_kind = classify_api_error(str(err))
                if api_kind:
                    summary.api_error_signatures[f"{api_kind}: {event}"] += 1
            else:
                err = obj.get("error")
                if err:
                    api_kind = classify_api_error(str(err))
                    if api_kind:
                        summary.api_error_signatures[f"{api_kind}: {event}"] += 1
    return summary


def event_counts(con: sqlite3.Connection) -> list[sqlite3.Row]:
    return rows(
        con,
        """
        select event_type, source, count(*) as n
        from events_log
        group by event_type, source
        order by n desc
        limit 40
        """,
    )


def run_metadata(con: sqlite3.Connection) -> dict[str, Any]:
    run = rows(con, "select * from runs order by started_at desc limit 1")
    return dict(run[0]) if run else {}


def order_state_summary(con: sqlite3.Connection) -> list[sqlite3.Row]:
    return rows(
        con,
        """
        with fill_by_order as (
          select order_id, sum(price * size) as fill_cost_basis, sum(size) as fill_shares
          from fills
          group by order_id
        )
        select
          o.state,
          o.side,
          count(*) as n,
          sum(coalesce(o.price,0) * coalesce(o.size,0)) as order_notional,
          sum(coalesce(f.fill_cost_basis,0)) as fill_cost_basis,
          sum(coalesce(f.fill_shares,0)) as fill_shares,
          sum(case
                when o.state in ('accepted','partially_filled','pending_send','cancel_requested')
                then coalesce(o.price,0) * coalesce(o.remaining_size, max(coalesce(o.size,0)-coalesce(o.cumulative_filled_size,0), 0))
                else 0
              end) as open_reserved_notional
        from orders o
        left join fill_by_order f on f.order_id=o.order_id
        group by o.state, o.side
        order by n desc
        """,
    )


def level_summary(con: sqlite3.Connection) -> list[sqlite3.Row]:
    return rows(
        con,
        """
        with fill_by_order as (
          select order_id, sum(price * size) as fill_cost_basis, sum(size) as fill_shares
          from fills
          group by order_id
        )
        select
          o.price,
          count(*) as orders,
          sum(case when o.state='accepted' then 1 else 0 end) as accepted,
          sum(case when o.state='partially_filled' then 1 else 0 end) as partially_filled,
          sum(case when o.state='filled' then 1 else 0 end) as filled,
          sum(case when o.state='cancel_confirmed' then 1 else 0 end) as canceled,
          sum(coalesce(o.price,0)*coalesce(o.size,0)) as order_notional,
          sum(coalesce(f.fill_cost_basis,0)) as fill_cost_basis,
          sum(coalesce(f.fill_shares,0)) as fill_shares,
          sum(case
                when o.state in ('accepted','partially_filled','pending_send','cancel_requested')
                then coalesce(o.price,0) * coalesce(o.remaining_size, max(coalesce(o.size,0)-coalesce(o.cumulative_filled_size,0), 0))
                else 0
              end) as open_reserved_notional
        from orders o
        left join fill_by_order f on f.order_id=o.order_id
        where o.side='BUY'
        group by o.price
        order by o.price
        """,
    )


def fill_summary(con: sqlite3.Connection) -> dict[str, Any]:
    row = rows(
        con,
        """
        select
          count(*) as fills,
          count(distinct order_id) as filled_orders,
          count(distinct market_id) as filled_markets,
          count(distinct token_id) as filled_tokens,
          sum(price * size) as cost_basis,
          sum(size) as shares,
          avg(price) as avg_fill_price,
          sum(coalesce(fee_paid_usdc,0)) as fees,
          min(filled_at) as first_fill_at,
          max(filled_at) as last_fill_at
        from fills
        """,
    )
    return dict(row[0]) if row else {}


def order_summary(con: sqlite3.Connection) -> dict[str, Any]:
    row = rows(
        con,
        """
        select
          count(*) as orders,
          count(distinct market_id) as ordered_markets,
          count(distinct token_id) as ordered_tokens,
          sum(case when state='accepted' then 1 else 0 end) as accepted,
          sum(case when state='partially_filled' then 1 else 0 end) as partially_filled,
          sum(case when state='filled' then 1 else 0 end) as filled,
          sum(case when state='cancel_confirmed' then 1 else 0 end) as canceled,
          sum(case when state='rejected' then 1 else 0 end) as rejected,
          sum(coalesce(price,0) * coalesce(size,0)) as gross_order_notional,
          min(intent_created_at) as first_order_at,
          max(intent_created_at) as last_order_at,
          max(last_state_change_at) as last_order_change_at
        from orders
        """,
    )
    return dict(row[0]) if row else {}


def category_summary(paper: sqlite3.Connection, screener: sqlite3.Connection | None, started_ts: int | None) -> list[dict[str, Any]]:
    if screener is None:
        return []
    # Pull the latest known category per ordered token from screener_log, scoped to the run when possible.
    token_rows = rows(paper, "select distinct market_id, token_id from orders")
    if not token_rows:
        return []
    pairs = [(str(r["market_id"]), str(r["token_id"])) for r in token_rows]
    category_by_pair: dict[tuple[str, str], str] = {}
    # Small loop over ordered tokens is faster and clearer than attaching cross-DB files.
    for market_id, token_id in pairs:
        if started_ts is not None:
            r = screener.execute(
                """
                select category
                from screener_log
                where market_id=? and token_id=? and scanned_at>=? and category is not null
                order by scanned_at desc
                limit 1
                """,
                (market_id, token_id, started_ts),
            ).fetchone()
        else:
            r = None
        if r is None:
            r = screener.execute(
                """
                select category
                from screener_log
                where market_id=? and token_id=? and category is not null
                order by scanned_at desc
                limit 1
                """,
                (market_id, token_id),
            ).fetchone()
        category_by_pair[(market_id, token_id)] = str(r[0]) if r and r[0] else "unknown"

    fills_by_pair = defaultdict(lambda: {"fills": 0, "cost_basis": 0.0, "shares": 0.0})
    for r in rows(paper, "select market_id, token_id, count(*) fills, sum(price*size) cost_basis, sum(size) shares from fills group by market_id, token_id"):
        d = fills_by_pair[(str(r["market_id"]), str(r["token_id"]))]
        d["fills"] += int(r["fills"] or 0)
        d["cost_basis"] += float(r["cost_basis"] or 0.0)
        d["shares"] += float(r["shares"] or 0.0)

    by_cat = defaultdict(lambda: {"orders": 0, "markets": set(), "tokens": set(), "order_notional": 0.0, "open_reserved": 0.0, "filled_orders": 0, "fills": 0, "cost_basis": 0.0, "shares": 0.0})
    for r in rows(paper, "select market_id, token_id, order_id, state, price, size, cumulative_filled_size, remaining_size from orders"):
        key = (str(r["market_id"]), str(r["token_id"]))
        cat = category_by_pair.get(key, "unknown")
        d = by_cat[cat]
        d["orders"] += 1
        d["markets"].add(key[0])
        d["tokens"].add(key[1])
        price = float(r["price"] or 0.0)
        size = float(r["size"] or 0.0)
        filled = float(r["cumulative_filled_size"] or 0.0)
        remaining = r["remaining_size"]
        remaining_size = float(remaining) if remaining is not None else max(size - filled, 0.0)
        d["order_notional"] += price * size
        if str(r["state"]) in LIVE_STATES:
            d["open_reserved"] += price * remaining_size
        if filled > 0:
            d["filled_orders"] += 1
    for pair, f in fills_by_pair.items():
        cat = category_by_pair.get(pair, "unknown")
        d = by_cat[cat]
        d["fills"] += f["fills"]
        d["cost_basis"] += f["cost_basis"]
        d["shares"] += f["shares"]
    out: list[dict[str, Any]] = []
    for cat, d in by_cat.items():
        out.append({
            "category": cat,
            "orders": d["orders"],
            "markets": len(d["markets"]),
            "tokens": len(d["tokens"]),
            "order_notional": d["order_notional"],
            "open_reserved": d["open_reserved"],
            "filled_orders": d["filled_orders"],
            "fills": d["fills"],
            "cost_basis": d["cost_basis"],
            "shares": d["shares"],
        })
    return sorted(out, key=lambda x: (x["cost_basis"], x["open_reserved"], x["orders"]), reverse=True)


def screener_funnel(screener: sqlite3.Connection | None, started_ts: int | None, limit: int = 20) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
    if screener is None:
        return [], []
    params: tuple[Any, ...] = (started_ts,) if started_ts is not None else ()
    where = "where scanned_at >= ?" if started_ts is not None else ""
    by_outcome = rows(
        screener,
        f"""
        select outcome, count(*) as n, count(distinct market_id) as markets, count(distinct token_id) as tokens,
               avg(current_price) as avg_price, avg(market_score) as avg_score
        from screener_log
        {where}
        group by outcome
        order by n desc
        limit {int(limit)}
        """,
        params,
    )
    by_category = rows(
        screener,
        f"""
        select category, outcome, count(*) as n
        from screener_log
        {where}
        group by category, outcome
        order by n desc
        limit {int(limit)}
        """,
        params,
    )
    return by_outcome, by_category


def latest_marks(con: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    tokens = [str(r[0]) for r in con.execute("select distinct token_id from fills")]
    if not tokens:
        return {}
    placeholders = ",".join("?" for _ in tokens)
    sql = f"""
    with latest as (
      select token_id, max(seq) as seq
      from events_log
      where event_type='BookUpdate' and token_id in ({placeholders})
      group by token_id
    )
    select e.token_id, e.event_time, e.payload_json
    from latest l
    join events_log e on e.token_id=l.token_id and e.seq=l.seq
    """
    marks: dict[str, dict[str, Any]] = {}
    for r in con.execute(sql, tokens):
        try:
            payload = json.loads(r["payload_json"])
        except Exception:
            payload = {}
        marks[str(r["token_id"])] = {
            "event_time": r["event_time"],
            "best_bid": payload.get("best_bid"),
            "best_ask": payload.get("best_ask"),
            "mid_price": payload.get("mid_price"),
            "spread": payload.get("spread"),
        }
    return marks


def mtm_summary(con: sqlite3.Connection) -> dict[str, Any]:
    marks = latest_marks(con)
    if not marks:
        return {"tokens_marked": 0}
    by_token = rows(
        con,
        """
        select token_id, market_id, sum(size) as shares, sum(price*size) as cost_basis, avg(price) as avg_entry
        from fills
        group by token_id, market_id
        """,
    )
    totals = {
        "tokens": len(by_token),
        "tokens_marked": 0,
        "shares": 0.0,
        "cost_basis": 0.0,
        "bid_value": 0.0,
        "mid_value": 0.0,
        "ask_value": 0.0,
        "stale_marks": 0,
        "top_positions": [],
    }
    now = utc_now()
    positions: list[dict[str, Any]] = []
    for r in by_token:
        token = str(r["token_id"])
        mark = marks.get(token)
        shares = float(r["shares"] or 0.0)
        cost = float(r["cost_basis"] or 0.0)
        totals["shares"] += shares
        totals["cost_basis"] += cost
        if not mark:
            continue
        bid = mark.get("best_bid")
        mid = mark.get("mid_price")
        ask = mark.get("best_ask")
        if bid is None and ask is not None:
            bid = 0.0
        if mid is None and bid is not None and ask is not None:
            mid = (float(bid) + float(ask)) / 2.0
        totals["tokens_marked"] += 1
        bid_value = shares * float(bid or 0.0)
        mid_value = shares * float(mid or bid or 0.0)
        ask_value = shares * float(ask or mid or bid or 0.0)
        totals["bid_value"] += bid_value
        totals["mid_value"] += mid_value
        totals["ask_value"] += ask_value
        mark_time = parse_iso(mark.get("event_time"))
        if mark_time and mark_time.tzinfo is None:
            mark_time = mark_time.replace(tzinfo=dt.UTC)
        if mark_time and (now - mark_time).total_seconds() > 10 * 60:
            totals["stale_marks"] += 1
        positions.append({
            "market_id": r["market_id"],
            "token_id": token,
            "shares": shares,
            "cost_basis": cost,
            "avg_entry": r["avg_entry"],
            "best_bid": bid,
            "best_ask": ask,
            "mid_price": mid,
            "bid_pnl": bid_value - cost,
            "mid_pnl": mid_value - cost,
            "mark_age": age_text(mark.get("event_time")),
        })
    totals["bid_pnl"] = totals["bid_value"] - totals["cost_basis"]
    totals["mid_pnl"] = totals["mid_value"] - totals["cost_basis"]
    totals["ask_pnl"] = totals["ask_value"] - totals["cost_basis"]
    totals["bid_roi_pct"] = 100.0 * totals["bid_pnl"] / totals["cost_basis"] if totals["cost_basis"] else None
    totals["mid_roi_pct"] = 100.0 * totals["mid_pnl"] / totals["cost_basis"] if totals["cost_basis"] else None
    positions.sort(key=lambda x: abs(float(x["mid_pnl"] or x["bid_pnl"] or 0.0)), reverse=True)
    totals["top_positions"] = positions[:10]
    return totals


def cancel_reasons(con: sqlite3.Connection) -> list[tuple[str, int]]:
    out = Counter()
    for r in rows(con, "select payload_json from events_log where event_type='OrderCanceled'"):
        try:
            payload = json.loads(r["payload_json"])
        except Exception:
            continue
        out[str(payload.get("cancel_reason") or "unknown")] += 1
    return out.most_common()


def top_questions(con: sqlite3.Connection, *, limit: int = 10) -> list[dict[str, Any]]:
    out = []
    for r in rows(
        con,
        """
        select o.market_id, m.question,
               count(*) as orders,
               sum(case when o.cumulative_filled_size > 0 then 1 else 0 end) as filled_orders,
               sum(coalesce(o.price,0)*coalesce(o.size,0)) as order_notional,
               sum(coalesce(o.price,0)*coalesce(o.cumulative_filled_size,0)) as filled_cost_basis
        from orders o
        left join markets m on m.market_id=o.market_id
        group by o.market_id, m.question
        order by filled_cost_basis desc, order_notional desc
        limit ?
        """,
        (limit,),
    ):
        out.append(dict(r))
    return out


def render_counter(counter: Counter[str], limit: int = 10, *, total: int | None = None) -> list[str]:
    lines = []
    total = total if total is not None else sum(counter.values())
    for key, n in counter.most_common(limit):
        pct = 100.0 * n / total if total else 0.0
        lines.append(f"- {key}: {fmt_num(n, 0)} ({fmt_pct(pct)})")
    return lines or ["- n/a"]


def render_report(ctx: RunContext, *, max_log_bytes: int | None = None, skip_mtm: bool = False) -> str:
    paper = connect_ro(ctx.paper_db)
    if paper is None:
        raise SystemExit(f"Cannot open paper DB: {ctx.paper_db}")
    screener = connect_ro(ctx.screener_db)
    run = run_metadata(paper)
    orders = order_summary(paper)
    fills = fill_summary(paper)
    by_state = order_state_summary(paper)
    by_level = level_summary(paper)
    by_event = event_counts(paper)
    cancels = cancel_reasons(paper)
    category = category_summary(paper, screener, ctx.run_started_ts)
    funnel, funnel_category = screener_funnel(screener, ctx.run_started_ts)
    mtm = {"tokens_marked": 0} if skip_mtm else mtm_summary(paper)
    questions = top_questions(paper)
    log_summary = stream_log(ctx.log_path, max_bytes=max_log_bytes)

    now = utc_now()
    repo_sha = repo_git_sha(ctx.repo)
    run_started = run.get("started_at") or ctx.started_at
    last_event = scalar(paper, "select max(event_time) from events_log")
    first_event = scalar(paper, "select min(event_time) from events_log")
    db_size_gb = ctx.paper_db.stat().st_size / (1024 ** 3)
    log_size_gb = ctx.log_path.stat().st_size / (1024 ** 3) if ctx.log_path else None
    open_reserved = sum(float(r["open_reserved_notional"] or 0.0) for r in by_state)
    cost_basis = float(fills.get("cost_basis") or 0.0)
    gross_order_notional = float(orders.get("gross_order_notional") or 0.0)
    order_count = int(orders.get("orders") or 0)
    filled_orders = int(fills.get("filled_orders") or 0)
    fill_rate = 100.0 * filled_orders / order_count if order_count else None
    live_orders = int((orders.get("accepted") or 0) + (orders.get("partially_filled") or 0))
    canceled = int(orders.get("canceled") or 0)
    cancel_rate = 100.0 * canceled / order_count if order_count else None
    total_events = sum(int(r["n"]) for r in by_event)

    lines: list[str] = []
    lines.append(f"# Paper report — {ctx.run_id}")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Generated: {now.replace(microsecond=0).isoformat().replace('+00:00', 'Z')}")
    lines.append(f"- Repo: `{ctx.repo}` @ `{repo_sha or 'unknown'}`")
    lines.append(f"- Paper DB: `{ctx.paper_db}` ({db_size_gb:.2f} GiB)")
    lines.append(f"- Log: `{ctx.log_path}`" + (f" ({log_size_gb:.2f} GiB)" if log_size_gb is not None else ""))
    lines.append(f"- Run: mode=`{run.get('mode', 'n/a')}`, strategy=`{run.get('strategy_id', 'n/a')}`, started={run_started or 'n/a'} ({age_text(run_started)} ago), finished={run.get('finished_at') or 'still running/none'}")
    lines.append(f"- Runtime freshness: first_event={first_event or 'n/a'}, last_event={last_event or 'n/a'} ({age_text(last_event)} ago)")
    if log_summary.parsed_lines:
        lines.append(f"- Log parsed: {fmt_num(log_summary.parsed_lines, 0)} JSON lines; last_log_ts={log_summary.last_ts or 'n/a'} ({age_text(log_summary.last_ts)} ago)")
    if max_log_bytes:
        lines.append(f"- Log scope: last {fmt_num(max_log_bytes / (1024 * 1024), 0)} MiB only")
    lines.append("")

    lines.append("## Technical report")
    lines.append("")
    lines.append("### Health / activity")
    lines.append(f"- DB events: {fmt_num(total_events, 0)}")
    lines.append(f"- Markets in DB: {fmt_num(scalar(paper, 'select count(*) from markets'), 0)}")
    lines.append(f"- Orders: {fmt_num(order_count, 0)} total; {fmt_num(live_orders, 0)} live/partial; {fmt_num(filled_orders, 0)} filled; {fmt_num(canceled, 0)} canceled")
    lines.append(f"- Fills: {fmt_num(fills.get('fills'), 0)} fills across {fmt_num(fills.get('filled_markets'), 0)} markets / {fmt_num(fills.get('filled_tokens'), 0)} tokens")
    lines.append("")

    lines.append("### Event volume by type/source")
    for r in by_event[:12]:
        pct = 100.0 * int(r["n"]) / total_events if total_events else 0.0
        lines.append(f"- {r['event_type']} / {r['source']}: {fmt_num(r['n'], 0)} ({fmt_pct(pct)})")
    lines.append("")

    lines.append("### WS / universe")
    if log_summary.ws_events:
        lines.extend(render_counter(log_summary.ws_events, 12))
    else:
        lines.append("- No WS events found in parsed log scope.")
    if log_summary.universe_latest:
        u = log_summary.universe_latest
        lines.append(f"- Latest universe: tokens={fmt_num(u.get('tokens'), 0)}, markets={fmt_num(u.get('markets'), 0)}, added={fmt_num(u.get('added'), 0)}, removed={fmt_num(u.get('removed'), 0)}, protected_tokens={fmt_num(u.get('protected_tokens'), 0)}, selector_applied={u.get('selector_applied')}")
    if log_summary.selector_stage:
        lines.append("- Selector stages:")
        lines.extend("  " + x for x in render_counter(log_summary.selector_stage, 8))
    if log_summary.selector_reject_reason:
        lines.append("- Top selector rejects:")
        lines.extend("  " + x for x in render_counter(log_summary.selector_reject_reason, 8))
    lines.append("")

    lines.append("### Log warnings / errors / API")
    if log_summary.parsed_lines:
        for level in ["warning", "error", "critical"]:
            n = log_summary.levels.get(level, 0)
            lines.append(f"- {level}: {fmt_num(n, 0)}")
        if log_summary.warning_events:
            lines.append("- Top warning/error events:")
            lines.extend("  " + x for x in render_counter(log_summary.warning_events, 10))
        if log_summary.api_error_signatures:
            lines.append("- API/HTTP signatures:")
            lines.extend("  " + x for x in render_counter(log_summary.api_error_signatures, 10))
        if log_summary.error_signatures:
            lines.append("- Top concrete error samples:")
            for signature, n in log_summary.error_signatures.most_common(8):
                lines.append(f"  - {fmt_num(n, 0)} × {signature}")
    else:
        lines.append("- Log was not available or had no JSON lines.")
    if log_summary.held_book:
        hb = log_summary.held_book
        lines.append(f"- Latest held-book freshness: held_tokens={fmt_num(hb.get('held_tokens'), 0)}, fresh={fmt_num(hb.get('fresh'), 0)}, stale={fmt_num(hb.get('stale'), 0)}, missing={fmt_num(hb.get('missing'), 0)}, attempted={fmt_num(hb.get('attempted'), 0)}, refreshed={fmt_num(hb.get('refreshed'), 0)}, failed={fmt_num(hb.get('failed'), 0)}")
    if cancels:
        lines.append("- Cancel reasons:")
        for reason, n in cancels[:8]:
            lines.append(f"  - {reason}: {fmt_num(n, 0)}")
    lines.append("")

    lines.append("### Screener funnel")
    if funnel:
        total_funnel = sum(int(r["n"]) for r in funnel)
        for r in funnel[:15]:
            pct = 100.0 * int(r["n"]) / total_funnel if total_funnel else 0.0
            lines.append(f"- {r['outcome']}: {fmt_num(r['n'], 0)} rows ({fmt_pct(pct)}), markets={fmt_num(r['markets'], 0)}, tokens={fmt_num(r['tokens'], 0)}, avg_price={fmt_num(r['avg_price'], 4)}, avg_score={fmt_num(r['avg_score'], 3)}")
    else:
        lines.append("- Screener DB missing or no rows in run window.")
    if funnel_category:
        lines.append("- Top category/outcome rows:")
        for r in funnel_category[:12]:
            lines.append(f"  - {r['category'] or 'unknown'} / {r['outcome']}: {fmt_num(r['n'], 0)}")
    lines.append("")

    lines.append("## Economic report")
    lines.append("")
    lines.append("### Trading overview")
    lines.append(f"- Gross order notional placed: {fmt_money(gross_order_notional)}")
    lines.append(f"- Filled cost basis: {fmt_money(cost_basis, 4)}")
    lines.append(f"- Open reserved notional: {fmt_money(open_reserved, 4)}")
    lines.append(f"- Fill rate by orders: {fmt_pct(fill_rate)} ({fmt_num(filled_orders, 0)} / {fmt_num(order_count, 0)})")
    lines.append(f"- Cancel rate by orders: {fmt_pct(cancel_rate)} ({fmt_num(canceled, 0)} / {fmt_num(order_count, 0)})")
    lines.append(f"- Avg fill price: {fmt_num(fills.get('avg_fill_price'), 5)}; shares bought={fmt_num(fills.get('shares'), 2)}; fees={fmt_money(fills.get('fees'), 6)}")
    lines.append(f"- Fill window: first={fills.get('first_fill_at') or 'n/a'}, last={fills.get('last_fill_at') or 'n/a'}")
    lines.append("")

    lines.append("### Mark-to-market PnL")
    if mtm.get("tokens_marked"):
        lines.append(f"- Marked tokens: {fmt_num(mtm.get('tokens_marked'), 0)} / {fmt_num(mtm.get('tokens'), 0)}; stale_marks>{10}m: {fmt_num(mtm.get('stale_marks'), 0)}")
        lines.append(f"- Liquidation @ best_bid: value={fmt_money(mtm.get('bid_value'), 4)}, PnL={fmt_money(mtm.get('bid_pnl'), 4, signed=True)}, ROI={fmt_pct(mtm.get('bid_roi_pct'), signed=True)}")
        lines.append(f"- Indicative @ mid: value={fmt_money(mtm.get('mid_value'), 4)}, PnL={fmt_money(mtm.get('mid_pnl'), 4, signed=True)}, ROI={fmt_pct(mtm.get('mid_roi_pct'), signed=True)}")
        lines.append("- Largest marked positions by abs PnL:")
        for p in mtm.get("top_positions", [])[:8]:
            lines.append(f"  - market={p['market_id']} token={str(p['token_id'])[:10]}… shares={fmt_num(p['shares'], 2)} cost={fmt_money(p['cost_basis'], 4)} bid={fmt_num(p['best_bid'], 4)} ask={fmt_num(p['best_ask'], 4)} mid_pnl={fmt_money(p['mid_pnl'], 4, signed=True)} mark_age={p['mark_age']}")
    elif skip_mtm:
        lines.append("- Skipped by --skip-mtm.")
    else:
        lines.append("- No fill marks available; PnL is unrealized/unknown until marks or resolution.")
    lines.append("")

    lines.append("### Orders by state")
    for r in by_state:
        lines.append(f"- {r['side']} / {r['state']}: orders={fmt_num(r['n'], 0)}, order_notional={fmt_money(r['order_notional'], 2)}, fill_cost_basis={fmt_money(r['fill_cost_basis'], 4)}, open_reserved={fmt_money(r['open_reserved_notional'], 4)}")
    lines.append("")

    lines.append("### Ladder by price")
    for r in by_level:
        lines.append(f"- {fmt_num(r['price'], 4)}: orders={fmt_num(r['orders'], 0)}, accepted={fmt_num(r['accepted'], 0)}, partial={fmt_num(r['partially_filled'], 0)}, filled={fmt_num(r['filled'], 0)}, canceled={fmt_num(r['canceled'], 0)}, fill_cost={fmt_money(r['fill_cost_basis'], 4)}, open_reserved={fmt_money(r['open_reserved_notional'], 4)}")
    lines.append("")

    lines.append("### Categories")
    if category:
        for c in category[:12]:
            lines.append(f"- {c['category']}: orders={fmt_num(c['orders'], 0)}, markets={fmt_num(c['markets'], 0)}, tokens={fmt_num(c['tokens'], 0)}, filled_orders={fmt_num(c['filled_orders'], 0)}, fills={fmt_num(c['fills'], 0)}, cost={fmt_money(c['cost_basis'], 4)}, open_reserved={fmt_money(c['open_reserved'], 4)}")
    else:
        lines.append("- Category join unavailable (screener DB missing or no ordered tokens).")
    lines.append("")

    lines.append("### Top markets by deployed/ordered notional")
    for q in questions:
        lines.append(f"- {q['market_id']}: filled_cost={fmt_money(q['filled_cost_basis'], 4)}, order_notional={fmt_money(q['order_notional'], 2)}, orders={fmt_num(q['orders'], 0)}, filled_orders={fmt_num(q['filled_orders'], 0)} — {one_line(q.get('question'), 140)}")
    lines.append("")

    lines.append("## Notes")
    lines.append("- PnL here is mark-to-market for current paper inventory, not settled realized PnL. For pennies strategy this is noisy until resolution; best_bid/liquidation is more conservative than mid when books are stale or 0/1-wide.")
    lines.append("- `fill_cost_basis` uses actual fill price × shares from `fills`; open reserved notional uses current live/partial remaining order size.")
    lines.append("- Technical log counts are from the parsed JSON log; DB event counts come from `events_log`.")

    if screener is not None:
        screener.close()
    paper.close()
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="Technical + economic paper-run report")
    ap.add_argument("--repo", default=".", help="Repository root")
    ap.add_argument("--data-dir", default=None, help="Polybot data dir; defaults to POLYMARKET_DATA_DIR/POLYBOT_DATA_DIR/~/.polybot")
    ap.add_argument("--paper-db", default="latest", help="Paper SQLite DB path or 'latest'")
    ap.add_argument("--screener-db", default="", help="Screener log SQLite DB path")
    ap.add_argument("--log", default="", help="Matching JSON log path")
    ap.add_argument("--no-log", action="store_true", help="Skip log parsing")
    ap.add_argument("--max-log-mb", type=float, default=0.0, help="Parse only the last N MiB of the log; 0 = full log")
    ap.add_argument("--skip-mtm", action="store_true", help="Skip latest BookUpdate mark-to-market scan")
    ap.add_argument("--output", default="", help="Write Markdown to this path instead of stdout")
    args = ap.parse_args()

    ctx = get_run_context(args)
    max_log_bytes = int(args.max_log_mb * 1024 * 1024) if args.max_log_mb and args.max_log_mb > 0 else None
    report = render_report(ctx, max_log_bytes=max_log_bytes, skip_mtm=args.skip_mtm)
    if args.output:
        out = Path(args.output).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report, encoding="utf-8")
    else:
        print(report, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
