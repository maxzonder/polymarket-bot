"""Analyze a replay run directory and print a human-readable report.

Usage:
    python3 scripts/honest_replay_analyze.py --run-dir /path/to/replay_run
    python3 scripts/honest_replay_analyze.py --run-dir /path/to/replay_run --top 10
    python3 scripts/honest_replay_analyze.py --compare /path/to/run1 /path/to/run2
"""

from __future__ import annotations

import argparse
import io
import json
import re
import sqlite3
import sys
from contextlib import redirect_stdout
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import MODES, ModeConfig, TPLevel
from utils.paths import DB_PATH


QUICK_ROWS = [
    ("universe", "universe", "int"),
    ("passed_screener", "passed_screener", "int"),
    ("entry_fills", "entry_fills", "int"),
    ("tp_fills", "tp_fills", "int"),
    ("fill_rate", "fill_rate_pct", "pct1"),
    ("positions", "positions", "int"),
    ("resolved", "resolved_positions", "int"),
    ("open", "open_positions", "int"),
    ("winners", "winners", "int"),
    ("losers", "losers", "int"),
    ("win_rate", "win_rate_pct", "pct1"),
    ("markets", "distinct_markets", "int"),
    ("tokens", "distinct_tokens", "int"),
    ("stake", "stake", "float4"),
    ("pnl", "pnl", "float4"),
    ("roi_on_stake", "roi_on_stake_pct", "pct1"),
    ("initial_balance", "initial_balance", "float4"),
    ("final_equity", "final_equity", "float4"),
    ("bankroll_return", "bankroll_return_pct", "pct1"),
    ("avg_stake", "avg_stake", "float4"),
    ("avg_entry", "avg_entry_price", "float6"),
    ("avg_peak_x", "avg_peak_x", "float4"),
    ("max_peak_x", "max_peak_x", "float4"),
]

SPARK_CHARS = "▁▂▃▄▅▆▇█"


def _conn(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _one(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> dict:
    row = cur.execute(sql, params).fetchone()
    return dict(row) if row is not None else {}


def _rows(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> list[dict]:
    return [dict(row) for row in cur.execute(sql, params).fetchall()]


def _print_section(title: str) -> None:
    print(f"\n== {title} ==")


def _safe_div(numer: float | int | None, denom: float | int | None) -> Optional[float]:
    if numer is None or denom in (None, 0):
        return None
    return float(numer) / float(denom)


def _fmt_number(value, places: int = 4) -> str:
    if value is None:
        return "n/a"
    text = f"{float(value):,.{places}f}"
    return text.rstrip("0").rstrip(".")


def _fmt_money(value, places: int = 4) -> str:
    if value is None:
        return "n/a"
    return f"${_fmt_number(value, places)}"


def _fmt_pct(value, places: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{places}f}%"


def _fmt_price(value) -> str:
    if value is None:
        return "n/a"
    numeric = float(value)
    text = f"{numeric:.6f}".rstrip("0")
    if text.endswith("."):
        text += "0"
    if "." not in text:
        text += ".00"
    else:
        decimals = len(text.split(".", 1)[1])
        if decimals < 2:
            text += "0" * (2 - decimals)
    return text


def _fmt_ts(ts) -> str:
    if ts in (None, 0):
        return "n/a"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _fmt_hours(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.1f}h"


def _fmt_int(value) -> str:
    if value is None:
        return "n/a"
    return f"{int(value):,}"


def _fmt_json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _fmt_compact(value) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return _fmt_number(value, 6)
    if isinstance(value, (dict, list, tuple)):
        return _fmt_json(value)
    return str(value)


def _fmt_quick(value, kind: str) -> str:
    if value is None:
        return "n/a"
    if kind == "int":
        return _fmt_int(value)
    if kind == "pct1":
        return _fmt_pct(value, 1)
    if kind == "float6":
        return _fmt_number(value, 6)
    if kind == "float4":
        return _fmt_number(value, 4)
    return str(value)


def _labelize(key: str) -> str:
    return key.replace("_", " ")


def _short_text(text: str | None, limit: int = 96) -> str:
    if not text:
        return "n/a"
    clean = " ".join(str(text).split())
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rstrip() + "…"


def _sparkline(values: list[float], width: int = 14) -> str:
    if not values:
        return "n/a"
    if len(values) > width:
        sampled: list[float] = []
        if width == 1:
            sampled = [values[-1]]
        else:
            for i in range(width):
                idx = round(i * (len(values) - 1) / (width - 1))
                sampled.append(values[idx])
        values = sampled

    lo = min(values)
    hi = max(values)
    if hi <= lo + 1e-12:
        return SPARK_CHARS[0] * len(values)

    chars = []
    for value in values:
        norm = (value - lo) / (hi - lo)
        idx = min(len(SPARK_CHARS) - 1, max(0, int(round(norm * (len(SPARK_CHARS) - 1)))))
        chars.append(SPARK_CHARS[idx])
    return "".join(chars)


def _fmt_progress(value: float) -> str:
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def _fmt_tp_levels(levels: tuple[TPLevel, ...], moonbag_fraction: float) -> str:
    if not levels and moonbag_fraction <= 0:
        return "none"
    parts = [f"{tp.fraction * 100:.0f}% at {_fmt_progress(tp.progress)}" for tp in levels]
    parts.append(f"moonbag{moonbag_fraction * 100:.0f}%")
    return ", ".join(parts)


def _fmt_market_score_tiers(tiers: tuple[tuple[float, float], ...]) -> str:
    if not tiers:
        return "disabled"
    return ", ".join(f">={score:.2f} -> {_fmt_money(stake, 4)}" for score, stake in tiers)


def _fmt_scoring_weights(weights: tuple[tuple[str, float], ...]) -> str:
    if not weights:
        return "legacy formula"
    return ", ".join(f"{name} {weight * 100:.0f}%" for name, weight in weights)


def _fmt_category_weights(weights: dict) -> str:
    if not weights:
        return "n/a"
    parts = [f"{key} {float(value):.2f}" for key, value in sorted(weights.items())]
    return ", ".join(parts)


def _bucket_label(levels: list[float], price: float) -> str:
    if not levels:
        return "all"
    if price <= levels[0]:
        return f"<= {_fmt_price(levels[0])}"
    for left, right in zip(levels, levels[1:]):
        if left < price <= right:
            return f"{_fmt_price(left)} – {_fmt_price(right)}"
    return f"> {_fmt_price(levels[-1])}"


def _hours_bucket(hours_to_close: float | None) -> str:
    if hours_to_close is None:
        return "n/a"
    if hours_to_close < 6:
        return "< 6h"
    if hours_to_close < 24:
        return "6–24h"
    if hours_to_close < 72:
        return "24–72h"
    return ">= 72h"


def _parse_summary_file(path: Path) -> dict:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")

    def _m(pattern: str, cast=int):
        found = re.search(pattern, text, re.MULTILINE)
        if not found:
            return None
        value = found.group(1).replace(",", "")
        return cast(value)

    return {
        "universe": _m(r"Universe \(market/token pairs\)\s*:\s*([0-9,]+)", int),
        "passed_screener": _m(r"Passed screener\s*:\s*([0-9,]+)", int),
        "entry_fills": _m(r"Entry fills\s*:\s*([0-9,]+)", int),
        "tp_fills": _m(r"TP fills\s*:\s*([0-9,]+)", int),
        "positions_opened": _m(r"Positions opened\s*:\s*([0-9,]+)", int),
    }


def _load_config_snapshot(run_dir: Path) -> dict:
    path = run_dir / "config_snapshot.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _collect_run_data(run_dir: Path) -> dict:
    run_dir = run_dir.expanduser().resolve()
    positions_db = run_dir / "positions.db"
    paper_db = run_dir / "paper_trades.db"
    summary_file = run_dir / "summary.txt"

    if not positions_db.exists():
        raise SystemExit(f"positions.db not found: {positions_db}")
    if not paper_db.exists():
        raise SystemExit(f"paper_trades.db not found: {paper_db}")

    config_snapshot = _load_config_snapshot(run_dir)
    run_meta = config_snapshot.get("run") or {}
    bot_config = config_snapshot.get("bot_config") or {}
    mode_name = str(bot_config.get("mode") or run_meta.get("mode") or "")
    mode_config = MODES.get(mode_name)

    con = _conn(positions_db)
    cur = con.cursor()
    dataset_attached = False
    if DB_PATH.exists():
        try:
            con.execute("ATTACH DATABASE ? AS dataset", (str(DB_PATH),))
            dataset_attached = True
        except sqlite3.Error:
            dataset_attached = False

    con2 = _conn(paper_db)
    cur2 = con2.cursor()

    tape_conn = None
    tape_cur = None
    tape_db_value = run_meta.get("tape_db_path")
    if tape_db_value:
        tape_path = Path(str(tape_db_value)).expanduser()
        if tape_path.exists():
            try:
                tape_conn = _conn(tape_path)
                tape_cur = tape_conn.cursor()
            except sqlite3.Error:
                tape_conn = None
                tape_cur = None

    summary_metrics = _parse_summary_file(summary_file)

    overall = _one(cur, """
        select
          count(*) as positions,
          round(sum(entry_size_usdc), 6) as stake,
          round(sum(coalesce(realized_pnl, 0)), 6) as pnl,
          round(sum(coalesce(realized_pnl, 0)) / nullif(sum(entry_size_usdc), 0), 6) as roi_on_stake,
          round(avg(entry_size_usdc), 6) as avg_stake,
          round(avg(entry_price), 6) as avg_entry_price,
          round(sum(entry_price * entry_size_usdc) / nullif(sum(entry_size_usdc), 0), 6) as weighted_avg_entry_price,
          round(avg(coalesce(peak_x, 0)), 6) as avg_peak_x,
          round(max(coalesce(peak_x, 0)), 6) as max_peak_x
        from positions
    """)
    realized_sell = cur.execute(
        """
        select round(sum(case when status='resolved' then case when entry_size_usdc + coalesce(realized_pnl, 0) > 0 then entry_size_usdc + coalesce(realized_pnl, 0) else 0 end else 0 end), 6)
        from positions
        """
    ).fetchone()[0] or 0.0

    winners = cur.execute("select count(*) from positions where status='resolved' and coalesce(realized_pnl, 0) > 0").fetchone()[0]
    losers = cur.execute("select count(*) from positions where status='resolved' and coalesce(realized_pnl, 0) <= 0").fetchone()[0]
    open_positions = cur.execute("select count(*) from positions where status='open'").fetchone()[0]
    resolved_positions = cur.execute("select count(*) from positions where status='resolved'").fetchone()[0]
    distinct_markets = cur.execute("select count(distinct market_id) from positions").fetchone()[0]
    distinct_tokens = cur.execute("select count(distinct token_id) from positions").fetchone()[0]

    rows = cur.execute("select id, ts, delta_usdc, note from paper_balance_events order by id").fetchall()
    balance = 0.0
    min_balance = (10**18, None)
    max_balance = (-10**18, None)
    for row in rows:
        balance += row["delta_usdc"]
        if balance < min_balance[0]:
            min_balance = (balance, dict(row))
        if balance > max_balance[0]:
            max_balance = (balance, dict(row))

    initial_balance = 0.0
    initial_row = cur.execute(
        """
        select delta_usdc, note
        from paper_balance_events
        where note like 'initial funding%' or note like 'initial balance%'
        order by id asc
        limit 1
        """
    ).fetchone()
    if initial_row is not None:
        initial_balance = float(initial_row["delta_usdc"] or 0.0)

    open_cost = cur.execute("select coalesce(sum(entry_size_usdc), 0) from positions where status='open'").fetchone()[0] or 0.0
    final_equity = balance + float(open_cost)
    bankroll_return_pct = ((final_equity - initial_balance) / initial_balance * 100.0) if initial_balance > 0 else None

    passed_screener = summary_metrics.get("passed_screener")
    if passed_screener is None:
        passed_screener = cur.execute(
            "select count(distinct candidate_id) from resting_orders where candidate_id is not null"
        ).fetchone()[0]

    entry_fills = summary_metrics.get("entry_fills")
    if entry_fills is None:
        entry_fills = cur.execute(
            "select count(*) from paper_balance_events where note like 'entry fill %'"
        ).fetchone()[0]

    tp_fills = summary_metrics.get("tp_fills")
    if tp_fills is None:
        tp_fills = cur.execute(
            "select count(*) from paper_balance_events where note like 'tp fill %'"
        ).fetchone()[0]

    metrics = {
        "universe": summary_metrics.get("universe"),
        "passed_screener": passed_screener,
        "entry_fills": entry_fills,
        "tp_fills": tp_fills,
        "fill_rate_pct": (_safe_div(entry_fills, passed_screener) * 100.0) if passed_screener else None,
        "positions": overall.get("positions"),
        "resolved_positions": resolved_positions,
        "open_positions": open_positions,
        "winners": winners,
        "losers": losers,
        "win_rate_pct": (_safe_div(winners, resolved_positions) * 100.0) if resolved_positions else None,
        "distinct_markets": distinct_markets,
        "distinct_tokens": distinct_tokens,
        "stake": overall.get("stake"),
        "buy": overall.get("stake"),
        "sell": realized_sell,
        "pnl": overall.get("pnl"),
        "roi_on_stake_pct": (overall.get("roi_on_stake") * 100.0) if overall.get("roi_on_stake") is not None else None,
        "roi_on_buy_pct": (overall.get("roi_on_stake") * 100.0) if overall.get("roi_on_stake") is not None else None,
        "initial_balance": initial_balance,
        "final_equity": final_equity,
        "bankroll_return_pct": bankroll_return_pct,
        "avg_stake": overall.get("avg_stake"),
        "avg_entry_price": overall.get("avg_entry_price"),
        "weighted_avg_entry_price": overall.get("weighted_avg_entry_price"),
        "avg_peak_x": overall.get("avg_peak_x"),
        "max_peak_x": overall.get("max_peak_x"),
    }

    return {
        "run_dir": run_dir,
        "positions_db": positions_db,
        "paper_db": paper_db,
        "summary_file": summary_file if summary_file.exists() else None,
        "summary_metrics": summary_metrics,
        "config_snapshot": config_snapshot,
        "run_meta": run_meta,
        "bot_config": bot_config,
        "mode_config": mode_config,
        "dataset_attached": dataset_attached,
        "metrics": metrics,
        "balance": {
            "final_balance_from_events": round(balance, 6),
            "open_position_cost_basis": round(float(open_cost), 6),
            "final_equity": round(final_equity, 6),
            "bankroll_return_pct": round(bankroll_return_pct, 6) if bankroll_return_pct is not None else None,
            "min_balance": round(min_balance[0], 6),
            "min_at": min_balance[1],
            "max_balance": round(max_balance[0], 6),
            "max_at": max_balance[1],
        },
        "con": con,
        "cur": cur,
        "con2": con2,
        "cur2": cur2,
        "tape_conn": tape_conn,
        "tape_cur": tape_cur,
    }


def _close_data(data: dict) -> None:
    data["con"].close()
    data["con2"].close()
    if data.get("tape_conn") is not None:
        data["tape_conn"].close()


def _realized_sell_value(entry_size_usdc: float | None, realized_pnl: float | None, status: str | None) -> float:
    if status != "resolved":
        return 0.0
    buy = float(entry_size_usdc or 0.0)
    pnl = float(realized_pnl or 0.0)
    return max(0.0, buy + pnl)


def _entry_price_bucket_rows(cur: sqlite3.Cursor, levels: tuple[float, ...]) -> list[dict]:
    rows = _rows(
        cur,
        """
        select entry_price, entry_size_usdc, status, is_winner, coalesce(realized_pnl, 0) as realized_pnl
        from positions
        order by entry_price
        """,
    )
    sorted_levels = sorted(set(float(level) for level in levels))
    buckets: dict[str, dict] = {}

    for row in rows:
        price = float(row["entry_price"])
        label = _bucket_label(sorted_levels, price)
        bucket = buckets.setdefault(
            label,
            {
                "bucket": label,
                "positions": 0,
                "buy": 0.0,
                "sell": 0.0,
                "pnl": 0.0,
                "resolved": 0,
                "winners": 0,
                "price_weighted_sum": 0.0,
            },
        )
        bucket["positions"] += 1
        bucket["buy"] += float(row["entry_size_usdc"] or 0.0)
        bucket["sell"] += _realized_sell_value(row["entry_size_usdc"], row["realized_pnl"], row["status"])
        bucket["pnl"] += float(row["realized_pnl"] or 0.0)
        bucket["price_weighted_sum"] += float(row["entry_price"] or 0.0) * float(row["entry_size_usdc"] or 0.0)
        if row["status"] == "resolved":
            bucket["resolved"] += 1
            if int(row["is_winner"] or 0) == 1:
                bucket["winners"] += 1

    ordered_labels = []
    if sorted_levels:
        ordered_labels.append(f"<= {_fmt_price(sorted_levels[0])}")
        ordered_labels.extend(
            f"{_fmt_price(left)} – {_fmt_price(right)}" for left, right in zip(sorted_levels, sorted_levels[1:])
        )
        ordered_labels.append(f"> {_fmt_price(sorted_levels[-1])}")
    else:
        ordered_labels.append("all")

    result = []
    for label in ordered_labels:
        if label not in buckets:
            continue
        bucket = buckets[label]
        bucket["win_rate_pct"] = (_safe_div(bucket["winners"], bucket["resolved"]) * 100.0) if bucket["resolved"] else None
        bucket["roi_on_buy_pct"] = (_safe_div(bucket["pnl"], bucket["buy"]) * 100.0) if bucket["buy"] else None
        bucket["weighted_avg_entry"] = _safe_div(bucket["price_weighted_sum"], bucket["buy"])
        result.append(bucket)
    return result


def _winner_economics(cur: sqlite3.Cursor) -> dict:
    winners = _one(
        cur,
        """
        select
          count(*) as positions,
          round(sum(realized_pnl), 6) as pnl,
          round(avg(realized_pnl), 6) as avg_pnl,
          round(avg((entry_size_usdc + realized_pnl) / nullif(entry_size_usdc, 0)), 6) as avg_multiple
        from positions
        where status='resolved' and coalesce(realized_pnl, 0) > 0
        """,
    )
    losers = _one(
        cur,
        """
        select
          count(*) as positions,
          round(sum(realized_pnl), 6) as pnl,
          round(avg(realized_pnl), 6) as avg_pnl,
          round(avg(realized_pnl / nullif(entry_size_usdc, 0)), 6) as avg_roi
        from positions
        where status='resolved' and coalesce(realized_pnl, 0) <= 0
        """,
    )
    avg_winner = winners.get("avg_pnl")
    avg_loser_abs = abs(float(losers.get("avg_pnl") or 0.0)) if losers.get("avg_pnl") is not None else None
    break_even = None
    if avg_winner is not None and avg_loser_abs is not None and (avg_winner + avg_loser_abs) > 0:
        break_even = avg_loser_abs / (avg_winner + avg_loser_abs) * 100.0
    return {
        "winners": winners,
        "losers": losers,
        "break_even_win_rate_pct": break_even,
    }


def _category_rows(cur: sqlite3.Cursor, category_weights: dict) -> list[dict]:
    rows = _rows(
        cur,
        """
        select
          coalesce(m.category, 'unknown') as category,
          count(*) as positions,
          sum(case when p.status='resolved' then 1 else 0 end) as resolved,
          sum(case when p.status='resolved' and coalesce(p.is_winner, 0)=1 then 1 else 0 end) as winners,
          round(sum(p.entry_size_usdc), 6) as buy,
          round(sum(case when p.status='resolved' then case when p.entry_size_usdc + coalesce(p.realized_pnl, 0) > 0 then p.entry_size_usdc + coalesce(p.realized_pnl, 0) else 0 end else 0 end), 6) as sell,
          round(sum(coalesce(p.realized_pnl, 0)), 6) as pnl,
          round(avg(p.entry_price), 6) as avg_entry_price,
          round(avg(case when m.end_date is not null then (m.end_date - p.opened_at) / 3600.0 end), 2) as avg_hours_to_close,
          round(avg(coalesce(m.volume, 0)), 2) as avg_volume_usdc
        from positions p
        left join dataset.markets m on cast(m.id as text) = p.market_id
        group by coalesce(m.category, 'unknown')
        order by buy desc, positions desc
        """,
    )
    for row in rows:
        row["category_weight"] = category_weights.get(row["category"])
        row["win_rate_pct"] = (_safe_div(row["winners"], row["resolved"]) * 100.0) if row["resolved"] else None
        row["roi_on_buy_pct"] = (_safe_div(row["pnl"], row["buy"]) * 100.0) if row["buy"] else None
    return rows


def _time_bucket_rows(cur: sqlite3.Cursor) -> list[dict]:
    rows = _rows(
        cur,
        """
        select
          case
            when m.end_date is null then 'n/a'
            when (m.end_date - p.opened_at) / 3600.0 < 6 then '< 6h'
            when (m.end_date - p.opened_at) / 3600.0 < 24 then '6–24h'
            when (m.end_date - p.opened_at) / 3600.0 < 72 then '24–72h'
            else '>= 72h'
          end as bucket,
          count(*) as positions,
          sum(case when p.status='resolved' then 1 else 0 end) as resolved,
          sum(case when p.status='resolved' and coalesce(p.is_winner, 0)=1 then 1 else 0 end) as winners,
          round(sum(p.entry_size_usdc), 6) as buy,
          round(sum(case when p.status='resolved' then case when p.entry_size_usdc + coalesce(p.realized_pnl, 0) > 0 then p.entry_size_usdc + coalesce(p.realized_pnl, 0) else 0 end else 0 end), 6) as sell,
          round(sum(coalesce(p.realized_pnl, 0)), 6) as pnl,
          round(avg(p.entry_price), 6) as avg_entry_price
        from positions p
        left join dataset.markets m on cast(m.id as text) = p.market_id
        group by bucket
        """,
    )
    order = {"< 6h": 0, "6–24h": 1, "24–72h": 2, ">= 72h": 3, "n/a": 4}
    rows.sort(key=lambda row: order.get(row["bucket"], 99))
    for row in rows:
        row["win_rate_pct"] = (_safe_div(row["winners"], row["resolved"]) * 100.0) if row["resolved"] else None
        row["roi_on_buy_pct"] = (_safe_div(row["pnl"], row["buy"]) * 100.0) if row["buy"] else None
    return rows


def _representative_position(cur: sqlite3.Cursor, market_id: str) -> dict:
    return _one(
        cur,
        """
        select token_id, outcome_name, entry_price, entry_size_usdc, coalesce(realized_pnl, 0) as realized_pnl,
               status, is_winner
        from positions
        where market_id = ?
        order by abs(coalesce(realized_pnl, 0)) desc, entry_size_usdc desc, opened_at asc
        limit 1
        """,
        (market_id,),
    )


def _market_sell_ladder(cur: sqlite3.Cursor, market_id: str) -> list[dict]:
    rows = _rows(
        cur,
        """
        select
          t.label as label,
          round(sum(coalesce(t.sell_quantity, 0)), 6) as sell_qty,
          round(sum(
            coalesce(t.filled_quantity, 0) * coalesce(t.sell_price, 0)
            + case
                when p.status='resolved' and coalesce(p.is_winner, 0)=1
                  then max(coalesce(t.sell_quantity, 0) - coalesce(t.filled_quantity, 0), 0)
                else 0
              end
          ), 6) as sell_usdc
        from tp_orders t
        join positions p on p.position_id = t.position_id
        where p.market_id = ?
        group by t.label
        order by
          case
            when t.label = 'tp_p10' then 1
            when t.label = 'tp_p50' then 2
            when t.label = 'moonbag_resolution' then 3
            else 9
          end,
          t.label
        """,
        (market_id,),
    )
    return rows


def _fmt_ladder_label(label: object | None) -> str:
    raw = str(label or "").strip()
    mapping = {
        "tp_p10": "tp10",
        "tp_p50": "tp50",
        "moonbag_resolution": "moonbag",
    }
    return mapping.get(raw, raw or "n/a")


def _token_price_path(tape_cur: sqlite3.Cursor | None, market_id: str, token_id: str) -> tuple[str, str]:
    if tape_cur is None or not token_id:
        return ("n/a", "n/a")
    rows = _rows(
        tape_cur,
        """
        select timestamp, price
        from tape
        where market_id = ? and token_id = ?
        order by timestamp
        """,
        (market_id, token_id),
    )
    if not rows:
        return ("n/a", "n/a")

    first = rows[0]
    last = rows[-1]
    low = min(rows, key=lambda row: row["price"])
    high = max(rows, key=lambda row: row["price"])

    points: list[tuple[str, dict]] = [("start", first)]
    for label, point in sorted([("low", low), ("high", high)], key=lambda item: item[1]["timestamp"]):
        prev_ts = points[-1][1]["timestamp"]
        prev_price = points[-1][1]["price"]
        if point["timestamp"] == prev_ts and abs(float(point["price"]) - float(prev_price)) < 1e-12:
            continue
        points.append((label, point))
    if last["timestamp"] != points[-1][1]["timestamp"] or abs(float(last["price"]) - float(points[-1][1]["price"])) > 1e-12:
        points.append(("last", last))

    summary = " → ".join(f"{label} {_fmt_price(point['price'])}" for label, point in points)
    spark = _sparkline([float(row["price"]) for row in rows])
    return (summary, spark)


def _fmt_neg_risk_badge(group_id: object | None) -> str:
    raw = str(group_id or "").strip()
    if not raw:
        return "NEG-RISK"
    if raw.lower().startswith("0x"):
        raw = raw[2:]
    raw = re.sub(r"[^0-9A-Za-z]+", "", raw)
    if not raw:
        return "NEG-RISK"
    return f"NR-{raw[:7].upper()}"


def _top_market_rows(cur: sqlite3.Cursor, top: int, *, winners: bool) -> list[dict]:
    having = "> 0" if winners else "< 0"
    ordering = "desc" if winners else "asc"
    rows = _rows(
        cur,
        f"""
        select
          p.market_id,
          coalesce(m.question, p.market_id) as question,
          coalesce(m.category, 'unknown') as category,
          coalesce(m.neg_risk, 0) as neg_risk,
          m.neg_risk_market_id as neg_risk_group_id,
          m.start_date as start_date_ts,
          m.end_date as end_date_ts,
          round(coalesce(m.volume, 0), 2) as volume_usdc,
          count(*) as positions,
          sum(case when p.status='resolved' then 1 else 0 end) as resolved_positions,
          sum(case when p.status='open' then 1 else 0 end) as open_positions,
          sum(case when coalesce(p.is_winner, 0)=1 then 1 else 0 end) as winners,
          round(sum(p.entry_size_usdc), 6) as buy,
          round(sum(case when p.status='resolved' then case when p.entry_size_usdc + coalesce(p.realized_pnl, 0) > 0 then p.entry_size_usdc + coalesce(p.realized_pnl, 0) else 0 end else 0 end), 6) as sell,
          round(sum(coalesce(p.realized_pnl, 0)), 6) as pnl,
          round(avg(p.entry_price), 6) as avg_entry_price,
          round(min(p.entry_price), 6) as min_entry_price,
          round(max(p.entry_price), 6) as max_entry_price,
          min(p.opened_at) as first_opened_at,
          max(coalesce(p.closed_at, p.opened_at)) as last_activity_at
        from positions p
        left join dataset.markets m on cast(m.id as text) = p.market_id
        group by p.market_id, question, category, neg_risk, neg_risk_group_id, start_date_ts, end_date_ts, volume_usdc
        having sum(coalesce(p.realized_pnl, 0)) {having}
        order by pnl {ordering}, buy desc
        limit {int(top)}
        """
    )
    return rows


def _top_neg_risk_groups(cur: sqlite3.Cursor, top: int) -> list[dict]:
    top_rows = _top_market_rows(cur, top, winners=True) + _top_market_rows(cur, top, winners=False)
    group_ids: list[str] = []
    seen: set[str] = set()
    top_market_ids = sorted({str(row.get("market_id") or "").strip() for row in top_rows if str(row.get("market_id") or "").strip()})
    for row in top_rows:
        if int(row.get("neg_risk") or 0) != 1:
            continue
        group_id = str(row.get("neg_risk_group_id") or "").strip()
        if not group_id or group_id in seen:
            continue
        seen.add(group_id)
        group_ids.append(group_id)

    priority_sql = "when 0 then 0"
    priority_params: tuple[object, ...] = ()
    if top_market_ids:
        priority_sql = f"when p.market_id in ({','.join('?' for _ in top_market_ids)}) then 0"
        priority_params = tuple(top_market_ids)

    groups: list[dict] = []
    for group_id in group_ids:
        rows = _rows(
            cur,
            f"""
            select
              p.market_id,
              coalesce(m.question, p.market_id) as question,
              round(sum(coalesce(p.realized_pnl, 0)), 6) as pnl,
              max(case when p.status='resolved' and coalesce(p.is_winner, 0)=1 then 1 else 0 end) as is_winner,
              max(case when p.status='resolved' and coalesce(p.is_winner, 0)=0 then 1 else 0 end) as is_loser,
              max(case when p.status='open' then 1 else 0 end) as is_open,
              count(p.position_id) as positions,
              round(sum(coalesce(p.entry_size_usdc, 0)), 6) as buy
            from positions p
            left join dataset.markets m on cast(m.id as text) = p.market_id
            where coalesce(m.neg_risk, 0) = 1 and m.neg_risk_market_id = ?
            group by p.market_id, question
            order by
              case
                {priority_sql}
                when max(case when p.status='resolved' and coalesce(p.is_winner, 0)=1 then 1 else 0 end) = 1 then 1
                when max(case when p.status='resolved' and coalesce(p.is_winner, 0)=0 then 1 else 0 end) = 1 then 2
                else 3
              end,
              question collate nocase,
              p.market_id
            """,
            (group_id, *priority_params),
        )
        if rows:
            groups.append({"group_id": group_id, "rows": rows})
    return groups


def _print_neg_risk_section(data: dict, top: int) -> None:
    _print_section("neg-risk")
    if not data.get("dataset_attached"):
        _print_key_value("dataset metadata", "not available")
        return

    groups = _top_neg_risk_groups(data["cur"], top)
    if not groups:
        print("- none")
        return

    for group in groups:
        badge = _fmt_neg_risk_badge(group.get("group_id"))
        print(f"- {badge}:")
        for idx, row in enumerate(group.get("rows", []), 1):
            tags: list[str] = []
            if float(row.get("buy") or 0) > 0:
                tags.append("bet")
            if int(row.get("is_winner") or 0) == 1:
                tags.append("win")
            elif float(row.get("buy") or 0) > 0 and int(row.get("is_loser") or 0) == 1:
                tags.append("loss")
            elif int(row.get("is_open") or 0) == 1:
                tags.append("open")
            tag_text = f" [{' | '.join(tags)}]" if tags else ""
            print(f"  - {idx}. {row.get('question') or row.get('market_id') or 'n/a'}{tag_text}")


def _print_key_value(label: str, value: str) -> None:
    print(f"- {label}: {value}")


def _print_run_header(data: dict) -> None:
    run_dir = data["run_dir"]
    run_meta = data.get("run_meta") or {}
    print(f"report_for: {run_dir.name}")
    _print_key_value("run kind", str(run_meta.get("kind") or "n/a"))
    _print_key_value("window", f"{run_meta.get('start', 'n/a')} → {run_meta.get('end', 'n/a')}")
    _print_key_value("mode", str(run_meta.get("mode") or data.get("bot_config", {}).get("mode") or "n/a"))
    _print_key_value("files", "positions.db, paper_trades.db, config_snapshot.json")


def _print_headline(data: dict) -> None:
    m = data["metrics"]
    _print_section("headline")
    _print_key_value("final equity", f"{_fmt_money(m.get('final_equity'))} from {_fmt_money(m.get('initial_balance'))}")
    _print_key_value("bankroll return", _fmt_pct(m.get("bankroll_return_pct"), 1))
    _print_key_value(
        "realized trade flow",
        f"buy {_fmt_money(m.get('buy'))} → sell {_fmt_money(m.get('sell'))} | pnl {_fmt_money(m.get('pnl'))} | ROI on buy {_fmt_pct(m.get('roi_on_buy_pct'), 1)}",
    )
    _print_key_value(
        "positions",
        f"{_fmt_int(m.get('positions'))} total, {_fmt_int(m.get('resolved_positions'))} resolved, {_fmt_int(m.get('open_positions'))} open",
    )
    _print_key_value(
        "winners / losers",
        f"{_fmt_int(m.get('winners'))} / {_fmt_int(m.get('losers'))} (resolved win rate {_fmt_pct(m.get('win_rate_pct'), 1)})",
    )
    _print_key_value(
        "screener / matched fills",
        f"passed screener candidates {_fmt_int(m.get('passed_screener'))}, matched entry fills {_fmt_int(m.get('entry_fills'))}, matched tp fills {_fmt_int(m.get('tp_fills'))}",
    )


def _print_config_highlights(data: dict) -> None:
    _print_section("config highlights")
    bot_config = data.get("bot_config") or {}
    mode_config: ModeConfig | None = data.get("mode_config")

    _print_key_value("PAPER_INITIAL_BALANCE_USDC", _fmt_money(bot_config.get("paper_initial_balance_usdc"), 4))
    if mode_config is None:
        _print_key_value("mode details", "n/a")
        return

    _print_key_value("max_exposure_per_market", _fmt_money(mode_config.max_exposure_per_market, 4))
    _print_key_value("entry_price_levels", ", ".join(_fmt_price(level) for level in mode_config.entry_price_levels))
    _print_key_value("tp_levels + moonbag_fraction", _fmt_tp_levels(mode_config.tp_levels, mode_config.moonbag_fraction))
    _print_key_value("max_open_positions", _fmt_int(mode_config.max_open_positions))
    _print_key_value("max_resting_markets", _fmt_int(mode_config.max_resting_markets))
    _print_key_value(
        "market duration limits",
        f"min {_fmt_hours(mode_config.min_hours_to_close)}, max {_fmt_hours(mode_config.max_hours_to_close)}",
    )
    _print_key_value("market_score_tiers", _fmt_market_score_tiers(mode_config.market_score_tiers))
    _print_key_value("scoring_weights", _fmt_scoring_weights(mode_config.scoring_weights))
    _print_key_value("category_weights", _fmt_category_weights(bot_config.get("category_weights") or {}))


def _print_capital_path(data: dict) -> None:
    _print_section("capital path")
    balance = data["balance"]
    cur = data["cur"]
    classes = _rows(
        cur,
        """
        select
          case
            when note like 'initial funding%' or note like 'initial balance%' then 'initial_funding'
            when note like 'entry fill %' then 'entry_fill'
            when note like 'tp fill %' then 'tp_fill'
            when note like 'resolution % winner=True' then 'resolution_win'
            when note like 'resolution % winner=False' then 'resolution_loss'
            else 'other'
          end as cls,
          count(*) as c,
          round(sum(delta_usdc), 6) as delta
        from paper_balance_events
        group by cls
        order by c desc
        """,
    )

    _print_key_value("ending cash", _fmt_money(balance.get("final_balance_from_events")))
    _print_key_value("open-position cost basis", _fmt_money(balance.get("open_position_cost_basis")))
    _print_key_value("final equity", _fmt_money(balance.get("final_equity")))
    _print_key_value(
        "min cash",
        f"{_fmt_money(balance.get('min_balance'))} at {_fmt_ts((balance.get('min_at') or {}).get('ts'))}",
    )
    max_at = balance.get('max_at') or {}
    max_cash_note = "run init" if str(max_at.get('note') or '').startswith('initial balance') else _fmt_ts(max_at.get('ts'))
    _print_key_value(
        "max cash",
        f"{_fmt_money(balance.get('max_balance'))} at {max_cash_note}",
    )

    if classes:
        print("- cash ledger event mix:")
        for row in classes:
            print(f"  - {row['cls']}: {_fmt_int(row['c'])} events, net cash delta {_fmt_money(row['delta'])}")


def _print_execution_breakdown(data: dict) -> None:
    _print_section("execution breakdown")
    cur = data["cur"]
    cur2 = data["cur2"]

    resting = _rows(
        cur,
        """
        select status, count(*) as c, round(sum(size), 6) as qty
        from resting_orders
        group by status
        order by status
        """,
    )
    paper_orders = _rows(
        cur2,
        """
        select side, status, count(*) as c, round(sum(size), 6) as qty
        from paper_orders
        group by side, status
        order by side, status
        """,
    )
    tp_orders = _rows(
        cur,
        """
        select label, status, count(*) as c, round(sum(sell_quantity), 6) as qty
        from tp_orders
        group by label, status
        order by label, status
        """,
    )

    if resting:
        print("- resting orders:")
        for row in resting:
            print(f"  - {row['status']}: {_fmt_int(row['c'])} orders, token qty {_fmt_number(row['qty'], 4)}")
    if paper_orders:
        print("- paper orders:")
        for row in paper_orders:
            print(f"  - {row['side']} {row['status']}: {_fmt_int(row['c'])} orders, token qty {_fmt_number(row['qty'], 4)}")
    if tp_orders:
        print("- tp orders:")
        for row in tp_orders:
            print(f"  - {row['label']} / {row['status']}: {_fmt_int(row['c'])} orders, sell token qty {_fmt_number(row['qty'], 4)}")


def _print_entry_price_analysis(data: dict) -> None:
    _print_section("entry price analysis")
    cur = data["cur"]
    mode_config: ModeConfig | None = data.get("mode_config")
    levels = mode_config.entry_price_levels if mode_config is not None else ()
    rows = _entry_price_bucket_rows(cur, levels)
    weighted_avg = data["metrics"].get("weighted_avg_entry_price")

    _print_key_value("configured entry levels", ", ".join(_fmt_price(level) for level in levels) if levels else "n/a")
    _print_key_value("simple avg entry", _fmt_price(data["metrics"].get("avg_entry_price")))
    _print_key_value("weighted avg entry", _fmt_price(weighted_avg))

    if rows:
        print("- buckets:")
        for row in rows:
            print(
                "  - "
                f"{row['bucket']}: positions {_fmt_int(row['positions'])}, "
                f"buy {_fmt_money(row['buy'])}, sell {_fmt_money(row['sell'])}, pnl {_fmt_money(row['pnl'])}, "
                f"ROI {_fmt_pct(row['roi_on_buy_pct'], 1)}, resolved WR {_fmt_pct(row['win_rate_pct'], 1)}"
            )

        dominant = max(rows, key=lambda row: (row["positions"], row["buy"]))
        best = max(rows, key=lambda row: row["roi_on_buy_pct"] if row["roi_on_buy_pct"] is not None else -10**9)
        worst = min(rows, key=lambda row: row["roi_on_buy_pct"] if row["roi_on_buy_pct"] is not None else 10**9)

        print("- signals:")
        print(
            f"  - largest fill bucket: {dominant['bucket']} | positions {_fmt_int(dominant['positions'])} | "
            f"buy {_fmt_money(dominant['buy'])} | ROI {_fmt_pct(dominant['roi_on_buy_pct'], 1)}"
        )
        print(
            f"  - best bucket by ROI: {best['bucket']} | sell {_fmt_money(best['sell'])} | pnl {_fmt_money(best['pnl'])} | "
            f"ROI {_fmt_pct(best['roi_on_buy_pct'], 1)} | resolved WR {_fmt_pct(best['win_rate_pct'], 1)}"
        )
        print(
            f"  - worst bucket by ROI: {worst['bucket']} | sell {_fmt_money(worst['sell'])} | pnl {_fmt_money(worst['pnl'])} | "
            f"ROI {_fmt_pct(worst['roi_on_buy_pct'], 1)} | resolved WR {_fmt_pct(worst['win_rate_pct'], 1)}"
        )


def _print_winner_economics(data: dict) -> None:
    _print_section("winner economics")
    econ = _winner_economics(data["cur"])
    winners = econ["winners"]
    losers = econ["losers"]

    _print_key_value(
        "resolved winners",
        f"{_fmt_int(winners.get('positions'))} | total {_fmt_money(winners.get('pnl'))} | avg {_fmt_money(winners.get('avg_pnl'))}",
    )
    _print_key_value(
        "resolved losers",
        f"{_fmt_int(losers.get('positions'))} | total {_fmt_money(losers.get('pnl'))} | avg {_fmt_money(losers.get('avg_pnl'))}",
    )
    _print_key_value("actual win rate", _fmt_pct(data['metrics'].get('win_rate_pct'), 1))
    _print_key_value("break-even win rate", _fmt_pct(econ.get('break_even_win_rate_pct'), 1))
    _print_key_value("avg winner multiple", _fmt_number(winners.get('avg_multiple'), 3))


def _print_selection_profile(data: dict) -> None:
    _print_section("selection profile")
    if not data.get("dataset_attached"):
        _print_key_value("dataset metadata", "not available")
        return

    category_rows = _category_rows(data["cur"], data.get("bot_config", {}).get("category_weights") or {})
    time_rows = _time_bucket_rows(data["cur"])

    if category_rows:
        print("- by category:")
        for row in category_rows:
            print(
                "  - "
                f"{row['category']}: cfg weight {row['category_weight'] if row['category_weight'] is not None else 'n/a'}, "
                f"positions {_fmt_int(row['positions'])}, buy {_fmt_money(row['buy'])}, sell {_fmt_money(row['sell'])}, pnl {_fmt_money(row['pnl'])}, "
                f"resolved WR {_fmt_pct(row['win_rate_pct'], 1)}, avg entry {_fmt_price(row['avg_entry_price'])}, "
                f"avg time-to-close {_fmt_hours(row['avg_hours_to_close'])}"
            )

    if time_rows:
        print("- by first-entry time-to-close:")
        for row in time_rows:
            print(
                "  - "
                f"{row['bucket']}: positions {_fmt_int(row['positions'])}, buy {_fmt_money(row['buy'])}, "
                f"sell {_fmt_money(row['sell'])}, pnl {_fmt_money(row['pnl'])}, resolved WR {_fmt_pct(row['win_rate_pct'], 1)}, "
                f"avg entry {_fmt_price(row['avg_entry_price'])}"
            )


def _print_market_cards(data: dict, *, winners: bool, top: int) -> None:
    title = f"top {int(top)} {'winning' if winners else 'losing'} markets"
    _print_section(title)
    if not data.get("dataset_attached"):
        _print_key_value("dataset metadata", "not available")
        return

    cur = data["cur"]
    tape_cur = data.get("tape_cur")
    category_weights = data.get("bot_config", {}).get("category_weights") or {}
    rows = _top_market_rows(cur, top, winners=winners)
    if not rows:
        print("- none")
        return

    for idx, row in enumerate(rows, 1):
        rep = _representative_position(cur, str(row["market_id"]))
        hours_to_close = None
        if row.get("end_date_ts") is not None and row.get("first_opened_at") is not None:
            hours_to_close = (float(row["end_date_ts"]) - float(row["first_opened_at"])) / 3600.0
        market_duration = None
        if row.get("start_date_ts") is not None and row.get("end_date_ts") is not None:
            market_duration = (float(row["end_date_ts"]) - float(row["start_date_ts"])) / 3600.0
        path_summary, spark = _token_price_path(tape_cur, str(row["market_id"]), str(rep.get("token_id") or ""))
        sell_ladder = _market_sell_ladder(cur, str(row["market_id"]))
        category_weight = category_weights.get(row["category"])

        print(f"- [{idx}] {row['market_id']} | {_short_text(row['question'], 120)}")
        category_label = str(row['category'])
        if int(row.get('neg_risk') or 0):
            category_label += f" | {_fmt_neg_risk_badge(row.get('neg_risk_group_id'))}"
        print(
            "  - "
            f"category: {category_label}"
            f" | category_weight: {category_weight if category_weight is not None else 'n/a'}"
            f" | closes: {_fmt_ts(row.get('end_date_ts'))}"
            f" | volume: {_fmt_money(row.get('volume_usdc'), 2)}"
        )
        print(
            "  - "
            f"positions: {_fmt_int(row['positions'])}"
            f" | resolved: {_fmt_int(row['resolved_positions'])}"
            f" | winners: {_fmt_int(row['winners'])}"
            f" | open: {_fmt_int(row['open_positions'])}"
            f" | market duration: {_fmt_hours(market_duration)}"
            f" | first-entry time-to-close: {_fmt_hours(hours_to_close)}"
        )
        ladder_text = ", ".join(
            f"{_fmt_ladder_label(item.get('label'))} {_fmt_money(item.get('sell_usdc'))}"
            for item in sell_ladder
        ) or "n/a"
        print(
            "  - "
            f"bag: {_fmt_money(row['buy'])}"
            f" | avg entry: {_fmt_price(row['avg_entry_price'])}"
            f" | entry range: {_fmt_price(row['min_entry_price'])} → {_fmt_price(row['max_entry_price'])}"
        )
        print(
            "  - "
            f"sell: {_fmt_money(row['sell'])}"
            f" | ladder: {ladder_text}"
            f" | pnl: {_fmt_money(row['pnl'])}"
        )
        token_label = rep.get('outcome_name') or 'n/a'
        print(f"  - token: {token_label}")
        print(f"  - price path ({token_label} token): {path_summary}")
        print(f"  - sparkline ({token_label} token): {spark}")


def _print_single_run(data: dict, top: int) -> None:
    _print_run_header(data)
    _print_headline(data)
    _print_config_highlights(data)
    _print_capital_path(data)
    _print_execution_breakdown(data)
    _print_entry_price_analysis(data)
    _print_winner_economics(data)
    _print_selection_profile(data)
    _print_market_cards(data, winners=True, top=top)
    _print_market_cards(data, winners=False, top=top)
    _print_neg_risk_section(data, top)


def _capture_single_run_text(data: dict, top: int) -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        _print_single_run(data, top)
    return buf.getvalue().rstrip() + "\n"


def _flatten(prefix: str, value, out: dict[str, object]) -> None:
    if isinstance(value, dict):
        for key in sorted(value):
            _flatten(f"{prefix}.{key}" if prefix else str(key), value[key], out)
    else:
        out[prefix] = value


def _compare_runs(run1: Path, run2: Path) -> None:
    data1 = _collect_run_data(run1)
    data2 = _collect_run_data(run2)
    try:
        print(f"run1: {data1['run_dir'].name}")
        print(f"run2: {data2['run_dir'].name}")

        _print_section("headline compare")
        for label, key, kind in QUICK_ROWS:
            v1 = _fmt_quick(data1['metrics'].get(key), kind)
            v2 = _fmt_quick(data2['metrics'].get(key), kind)
            print(f"- {_labelize(label)}: run1={v1} | run2={v2}")

        _print_section("differing config values")
        snap1 = data1.get("config_snapshot") or {}
        snap2 = data2.get("config_snapshot") or {}
        if not snap1 or not snap2:
            print("- config_snapshot.json missing in one or both runs")
            return

        flat1: dict[str, object] = {}
        flat2: dict[str, object] = {}
        _flatten("run", snap1.get("run", {}), flat1)
        _flatten("bot_config", snap1.get("bot_config", {}), flat1)
        _flatten("run", snap2.get("run", {}), flat2)
        _flatten("bot_config", snap2.get("bot_config", {}), flat2)

        any_diff = False
        for key in sorted(set(flat1) | set(flat2)):
            v1 = flat1.get(key)
            v2 = flat2.get(key)
            if v1 != v2:
                any_diff = True
                print(f"- {key}: run1={_fmt_compact(v1)} | run2={_fmt_compact(v2)}")
        if not any_diff:
            print("- no config differences")
    finally:
        _close_data(data1)
        _close_data(data2)


def _capture_compare_text(run1: Path, run2: Path) -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        _compare_runs(run1, run2)
    return buf.getvalue().rstrip() + "\n"


def _split_label_value(text: str) -> tuple[str | None, str]:
    if ": " not in text:
        return None, text
    label, value = text.split(": ", 1)
    return label.strip(), value.strip()


def _parse_text_report(text: str) -> tuple[str, list[str], list[tuple[str, list[str]]]]:
    report_name = "Replay report"
    header_lines: list[str] = []
    sections: list[tuple[str, list[str]]] = []
    current_title: str | None = None
    current_lines: list[str] = []

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("report_for:"):
            report_name = stripped.split(":", 1)[1].strip()
            continue
        if stripped.startswith("==") and stripped.endswith("=="):
            if current_title is not None:
                sections.append((current_title, current_lines))
            current_title = stripped.strip("=").strip()
            current_lines = []
            continue
        if current_title is None:
            header_lines.append(line)
        else:
            current_lines.append(line)

    if current_title is not None:
        sections.append((current_title, current_lines))

    return report_name, header_lines, sections


def _parse_section_items(lines: list[str]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    current: dict[str, object] | None = None

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if stripped.startswith("- "):
            content = stripped[2:]
            if indent == 0:
                current = {"text": content, "children": []}
                items.append(current)
            else:
                if current is None:
                    current = {"text": "", "children": []}
                    items.append(current)
                current.setdefault("children", []).append(content)
        else:
            if current is None:
                current = {"text": stripped, "children": []}
                items.append(current)
            else:
                current.setdefault("children", []).append(stripped)

    return items


def _render_value_html(key: str, value: str) -> str:
    stripped = value.strip()
    key_norm = key.strip().lower()
    if key_norm == "token":
        if stripped.lower() == "yes":
            return '<span class="token token-yes">Yes</span>'
        if stripped.lower() == "no":
            return '<span class="token token-no">No</span>'
        return f'<span class="token token-generic">{escape(stripped)}</span>'
    if key_norm == "category":
        match = re.match(r"^(.*?)(\s*\|\s*)(NR-[A-Z0-9]+|NEG-RISK)(.*)$", stripped)
        if match:
            left, sep, badge, tail = match.groups()
            return f'{escape(left.strip())}{escape(sep)}<strong class="neg-risk">{escape(badge)}</strong>{escape(tail)}'
    return escape(value)



def _render_kv_table(rows: list[tuple[str, str]], extra_class: str = "") -> str:
    if not rows:
        return ""
    cls = f"kv-table {extra_class}".strip()
    parts = [f'<table class="{cls}"><tbody>']
    for key, value in rows:
        parts.append(
            "<tr>"
            f"<th>{escape(key)}</th>"
            f"<td>{_render_value_html(key, value)}</td>"
            "</tr>"
        )
    parts.append("</tbody></table>")
    return "".join(parts)


def _render_detail_rows(lines: list[str]) -> str:
    rows = []
    for line in lines:
        key, value = _split_label_value(line)
        if key is None:
            rows.append(("detail", line))
        else:
            rows.append((key, value))
    return _render_kv_table(rows, "detail-table")


def _render_subgroup(item: dict[str, object]) -> str:
    title = str(item.get("text") or "").rstrip(":")
    children = [str(child) for child in item.get("children", [])]
    html = [f'<div class="subcard"><h3>{escape(title)}</h3>']
    if children:
        html.append(_render_detail_rows(children))
    html.append("</div>")
    return "".join(html)


def _render_sparkline_html(text: str) -> str:
    if not text:
        return ""
    chars = list(text)
    if len(chars) == 1:
        return f'<span class="spark-point">{escape(chars[0])}</span>'
    first = f'<span class="spark-start">{escape(chars[0])}</span>'
    middle = ''.join(escape(ch) for ch in chars[1:-1])
    last = f'<span class="spark-end">{escape(chars[-1])}</span>'
    return first + middle + last



def _render_market_card(item: dict[str, object]) -> str:
    header = str(item.get("text") or "")
    children = [str(child) for child in item.get("children", [])]

    badge_html = ""
    title = header
    if header.startswith("[") and "]" in header:
        idx, rest = header.split("]", 1)
        badge_html = f'<span class="badge">{escape(idx[1:])}</span>'
        title = rest.strip()

    path_line = ""
    spark_line = ""
    detail_lines: list[str] = []
    for child in children:
        key, value = _split_label_value(child)
        if key and key.startswith("price path"):
            path_line = value
        elif key and key.startswith("sparkline"):
            spark_line = value
        else:
            detail_lines.append(child)

    html = [
        '<article class="market-card">',
        f'<div class="market-title">{badge_html}<span>{escape(title)}</span></div>',
    ]
    if detail_lines:
        html.append(_render_detail_rows(detail_lines))
    if path_line:
        html.append(
            '<div class="mono-box">'
            '<div class="mono-label">price path</div>'
            f'<div class="mono-value">{escape(path_line)}</div>'
            '</div>'
        )
    if spark_line:
        html.append(
            '<div class="spark-box">'
            '<div class="mono-label">sparkline</div>'
            f'<div class="sparkline">{_render_sparkline_html(spark_line)}</div>'
            '</div>'
        )
    html.append("</article>")
    return "".join(html)


def _render_section_html(title: str, lines: list[str]) -> str:
    items = _parse_section_items(lines)
    html = [f'<section class="section"><h2>{escape(title)}</h2>']

    if "markets" in title.lower():
        html.append('<div class="market-grid">')
        for item in items:
            html.append(_render_market_card(item))
        html.append("</div>")
        html.append("</section>")
        return "".join(html)

    kv_rows: list[tuple[str, str]] = []
    blocks: list[str] = []

    for item in items:
        text_value = str(item.get("text") or "")
        children = [str(child) for child in item.get("children", [])]
        if children:
            blocks.append(_render_subgroup(item))
        else:
            key, value = _split_label_value(text_value)
            if key is None:
                blocks.append(f'<p class="plain">{escape(text_value)}</p>')
            else:
                kv_rows.append((key, value))

    if kv_rows:
        html.append(_render_kv_table(kv_rows))
    html.extend(blocks)
    html.append("</section>")
    return "".join(html)


def _render_neg_risk_section_from_text(items: list[dict[str, object]]) -> str:
    if not items:
        return ""
    html = ['<section class="section"><h2>neg-risk</h2><div class="neg-risk-groups">']
    for item in items:
        badge = str(item.get("text") or "").rstrip(":")
        children = [str(child) for child in item.get("children", [])]
        html.append(f'<div class="subcard neg-risk-group"><h3>{escape(badge)}</h3><ol class="neg-risk-list">')
        for child in children:
            m = re.match(r"^\d+\.\s+(.*?)(?:\s+\[(.*?)\])?$", child.strip())
            if m:
                question = m.group(1).strip()
                tags = {tag.strip().lower() for tag in (m.group(2) or "").split("|") if tag.strip()}
            else:
                question = child.strip()
                tags = set()
            cls = "nr-neutral"
            if "win" in tags:
                cls = "nr-win"
            elif "loss" in tags:
                cls = "nr-loss"
            elif "bet" in tags:
                cls = "nr-bet"
            elif "open" in tags:
                cls = "nr-open"
            html.append(f'<li class="{cls}"><span class="nr-question">{escape(question)}</span></li>')
        html.append('</ol></div>')
    html.append('</div></section>')
    return ''.join(html)


def _text_report_to_html(text: str, title: str) -> str:
    report_name, header_lines, sections = _parse_text_report(text)

    header_rows: list[tuple[str, str]] = []
    for line in header_lines:
        stripped = line.strip()
        if stripped.startswith("- "):
            key, value = _split_label_value(stripped[2:])
            if key is not None:
                header_rows.append((key, value))

    body = [
        f"<header><h1>{escape(report_name)}</h1><p>{escape(title)}</p></header>",
    ]
    if header_rows:
        body.append('<section class="section hero-meta"><h2>run meta</h2>')
        body.append(_render_kv_table(header_rows, "hero-table"))
        body.append("</section>")

    for section_title, section_lines in sections:
        if section_title.strip().lower() == "neg-risk":
            body.append(_render_neg_risk_section_from_text(_parse_section_items(section_lines)))
        else:
            body.append(_render_section_html(section_title, section_lines))

    css = """
    :root {
      --bg: #0b1020;
      --panel: #11192d;
      --panel-2: #16223d;
      --panel-3: #0e1630;
      --text: #e8ecf3;
      --muted: #9fb0d0;
      --line: rgba(255,255,255,0.08);
      --blue: #8ec5ff;
      --blue-2: #5ea9ff;
      --accent: #9bffd0;
      --shadow: 0 12px 32px rgba(0,0,0,0.25);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 28px;
      background:
        radial-gradient(circle at top right, rgba(94,169,255,0.12), transparent 28%),
        linear-gradient(180deg, #0a0f1d 0%, var(--bg) 100%);
      color: var(--text);
      font: 15px/1.6 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    .wrap {
      max-width: 1280px;
      margin: 0 auto;
    }
    header {
      margin-bottom: 22px;
      padding: 28px 32px;
      border-radius: 22px;
      background: linear-gradient(135deg, #192542 0%, #0d1528 100%);
      box-shadow: var(--shadow);
      border: 1px solid rgba(255,255,255,0.08);
    }
    h1 {
      margin: 0 0 8px;
      font-size: 34px;
      line-height: 1.12;
      letter-spacing: -0.02em;
    }
    header p {
      margin: 0;
      font-size: 15px;
      color: var(--muted);
    }
    .section {
      margin: 18px 0;
      padding: 20px 22px 22px;
      border-radius: 20px;
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }
    .hero-meta {
      background: linear-gradient(180deg, rgba(94,169,255,0.08), rgba(255,255,255,0.02));
    }
    h2 {
      margin: 0 0 14px;
      font-size: 20px;
      line-height: 1.2;
      letter-spacing: -0.01em;
      color: var(--blue);
      text-transform: none;
    }
    h3 {
      margin: 0 0 10px;
      font-size: 15px;
      line-height: 1.3;
      color: var(--accent);
      font-weight: 700;
    }
    .kv-table {
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
      border-radius: 14px;
      background: rgba(255,255,255,0.02);
    }
    .kv-table th,
    .kv-table td {
      padding: 11px 14px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }
    .kv-table tr:last-child th,
    .kv-table tr:last-child td {
      border-bottom: 0;
    }
    .kv-table th {
      width: 30%;
      text-align: left;
      color: #c9d8f3;
      font-weight: 700;
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      background: rgba(255,255,255,0.025);
    }
    .kv-table td {
      color: var(--text);
      font-size: 14px;
    }
    .subcard {
      margin-top: 16px;
      padding: 16px 16px 14px;
      border-radius: 16px;
      background: var(--panel-3);
      border: 1px solid var(--line);
    }
    .detail-table th { width: 26%; }
    .market-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
      gap: 16px;
    }
    .market-card {
      padding: 18px;
      border-radius: 18px;
      background: linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.015));
      border: 1px solid var(--line);
      box-shadow: 0 8px 22px rgba(0,0,0,0.20);
    }
    .market-title {
      display: flex;
      align-items: flex-start;
      gap: 10px;
      margin-bottom: 14px;
      font-size: 17px;
      line-height: 1.35;
      font-weight: 800;
      color: #f3f7ff;
    }
    .badge {
      flex: 0 0 auto;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 2rem;
      padding: 0.2rem 0.55rem;
      border-radius: 999px;
      background: linear-gradient(180deg, #2a4676, #21375b);
      color: #ecf4ff;
      font-size: 0.83rem;
      font-weight: 800;
    }
    .token {
      display: inline-block;
      padding: 0.12rem 0.52rem;
      border-radius: 999px;
      font-weight: 800;
      line-height: 1.2;
      letter-spacing: 0.01em;
    }
    .token-yes {
      background: rgba(46, 204, 113, 0.18);
      color: #7df0ae;
      border: 1px solid rgba(46, 204, 113, 0.28);
    }
    .token-no {
      background: rgba(255, 99, 132, 0.16);
      color: #ff9db1;
      border: 1px solid rgba(255, 99, 132, 0.28);
    }
    .token-generic {
      background: rgba(255, 255, 255, 0.06);
      color: #d8e0ee;
      border: 1px solid rgba(255, 255, 255, 0.18);
    }
    .neg-risk {
      color: #ffffff;
      font-weight: 900;
      letter-spacing: 0.02em;
    }
    .mono-box, .spark-box {
      margin-top: 12px;
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(94,169,255,0.07);
      border: 1px solid rgba(94,169,255,0.15);
    }
    .mono-label {
      margin-bottom: 6px;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-weight: 700;
    }
    .mono-value, .sparkline {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .sparkline {
      font-size: 18px;
      line-height: 1.2;
      letter-spacing: 0.08em;
      color: #cfe4ff;
    }
    .spark-start {
      color: #67c6ff;
      font-weight: 800;
    }
    .spark-end {
      color: #ffd84d;
      font-weight: 800;
    }
    .spark-point {
      color: #9fe0ff;
      font-weight: 800;
    }
    .plain {
      margin: 8px 0 0;
      color: var(--text);
    }
    .neg-risk-groups {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 16px;
    }
    .neg-risk-group {
      background: var(--panel-3);
    }
    .neg-risk-list {
      margin: 0;
      padding-left: 1.35rem;
    }
    .neg-risk-list li {
      margin: 0 0 8px;
    }
    .nr-question {
      display: inline-block;
      line-height: 1.45;
    }
    .nr-win .nr-question {
      color: #7df0ae;
      font-weight: 700;
    }
    .nr-loss .nr-question {
      color: #ff9db1;
      font-weight: 700;
    }
    .nr-open .nr-question {
      color: #d8e0ee;
    }
    .nr-bet .nr-question {
      color: #ffd84d;
      font-weight: 700;
    }
    .nr-neutral .nr-question {
      color: #9fb0d0;
    }
    strong { font-weight: 800; }
    @media (max-width: 900px) {
      body { padding: 14px; }
      header { padding: 22px 18px; border-radius: 18px; }
      .section { padding: 16px; border-radius: 16px; }
      .kv-table th, .kv-table td { display: block; width: 100%; }
      .kv-table th { border-bottom: 0; padding-bottom: 4px; }
      .kv-table td { padding-top: 0; }
      h1 { font-size: 28px; }
      .market-grid { grid-template-columns: 1fr; }
    }
    """

    return (
        "<!doctype html>\n"
        "<html lang=\"ru\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\">\n"
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
        f"  <title>{escape(title)}</title>\n"
        f"  <style>{css}</style>\n"
        "</head>\n"
        "<body>\n"
        "  <div class=\"wrap\">\n"
        f"    {''.join(body)}\n"
        "  </div>\n"
        "</body>\n"
        "</html>\n"
    )


def _write_html_report(html_path: Path, text: str, title: str) -> None:
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(_text_report_to_html(text, title), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze a replay run directory")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-dir", help="Replay run directory containing positions.db and paper_trades.db")
    group.add_argument("--compare", nargs=2, metavar=("RUN1", "RUN2"), help="Compare two replay run directories")
    ap.add_argument("--top", type=int, default=10, help="How many winning/losing markets to show")
    ap.add_argument("--html-out", help="Optional path to write an HTML version of the report")
    args = ap.parse_args()

    if args.compare:
        text = _capture_compare_text(Path(args.compare[0]), Path(args.compare[1]))
        print(text, end="")
        if args.html_out:
            _write_html_report(Path(args.html_out), text, "Replay run comparison")
        return

    data = _collect_run_data(Path(args.run_dir))
    try:
        text = _capture_single_run_text(data, args.top)
        print(text, end="")
        if args.html_out:
            title = f"Replay report, {data['run_dir'].name}"
            _write_html_report(Path(args.html_out), text, title)
    finally:
        _close_data(data)


if __name__ == "__main__":
    main()
