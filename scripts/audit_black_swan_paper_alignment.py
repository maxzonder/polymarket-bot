#!/usr/bin/env python3
"""Audit black_swan paper trading alignment with historical analytics.

The script is intentionally read-only. It inspects:
- black_swan runtime config and hard gates;
- source-code wiring for strategy/scorer/screener/execution/universe;
- feature_mart_v1_1 / swans_v2 historical analytics DB;
- current paper-trading runtime DB and screener log DB.

Typical server usage:

    cd /home/polybot/claude-polymarket
    POLYMARKET_DATA_DIR=/home/polybot/.polybot \
      ./.venv/bin/python scripts/audit_black_swan_paper_alignment.py \
      --dataset-db /home/polybot/.polybot/polymarket_dataset.db \
      --paper-db latest \
      --screener-log-db /home/polybot/.polybot/swan_v2/swan_screener_log.sqlite3 \
      --output reports/black_swan_paper_alignment_latest.md

Exit code is 0 unless the script itself fails. Compliance is reported in Markdown.
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import importlib.util
import json
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from typing import Any, Iterable


STATUS_OK = "OK"
STATUS_WARN = "WARN"
STATUS_FAIL = "FAIL"
STATUS_INFO = "INFO"


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def repo_git_sha(repo: Path) -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    except Exception:
        return None


def load_config_module(repo: Path):
    config_path = repo / "config.py"
    sys.path.insert(0, str(repo))
    spec = importlib.util.spec_from_file_location("audit_repo_config", config_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot import {config_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def read_text(repo: Path, rel: str) -> str:
    path = repo / rel
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def latest_paper_db() -> Path | None:
    roots = [
        Path(os.environ.get("POLYBOT_DATA_DIR", "")),
        Path(os.environ.get("POLYMARKET_DATA_DIR", "")) / "swan_v2",
        Path.home() / ".polybot" / "swan_v2",
    ]
    candidates: list[Path] = []
    for root in roots:
        if root and str(root) != ".":
            candidates.extend(Path(p) for p in glob.glob(str(root / "black_swan_v1_paper_*.sqlite3")))
    candidates = [p for p in candidates if p.exists()]
    return max(candidates, key=lambda p: p.stat().st_mtime) if candidates else None


def connect(path: Path | None) -> sqlite3.Connection | None:
    if not path or not path.exists():
        return None
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    return con


def table_exists(con: sqlite3.Connection | None, table: str) -> bool:
    if con is None:
        return False
    row = con.execute("select 1 from sqlite_master where type='table' and name=?", (table,)).fetchone()
    return row is not None


def columns(con: sqlite3.Connection | None, table: str) -> set[str]:
    if con is None or not table_exists(con, table):
        return set()
    return {r[1] for r in con.execute(f"pragma table_info({table})")}


def scalar(con: sqlite3.Connection | None, sql: str, params: Iterable[Any] = ()) -> Any:
    if con is None:
        return None
    row = con.execute(sql, tuple(params)).fetchone()
    if row is None:
        return None
    return row[0]


def rows(con: sqlite3.Connection | None, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    if con is None:
        return []
    return list(con.execute(sql, tuple(params)))


def fmt_num(v: Any, digits: int = 3) -> str:
    if v is None:
        return "n/a"
    if isinstance(v, float):
        if math.isfinite(v):
            return f"{v:.{digits}f}".rstrip("0").rstrip(".")
        return str(v)
    return str(v)


def add_check(checks: list[dict[str, str]], area: str, status: str, check: str, evidence: str, implication: str = "") -> None:
    checks.append({
        "area": area,
        "status": status,
        "check": check,
        "evidence": evidence,
        "implication": implication,
    })


def bullet_list(items: Iterable[str]) -> str:
    items = [x for x in items if x]
    return "\n".join(f"- {x}" for x in items) if items else "- n/a"


def code_bool(text: str, needle: str) -> bool:
    return needle in text


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".", help="Repository root to inspect")
    ap.add_argument("--dataset-db", default=os.environ.get("POLYMARKET_DATASET_DB", "/home/polybot/.polybot/polymarket_dataset.db"))
    ap.add_argument("--paper-db", default="latest", help="Path to black_swan paper runtime DB, or 'latest'")
    ap.add_argument("--screener-log-db", default="/home/polybot/.polybot/swan_v2/swan_screener_log.sqlite3")
    ap.add_argument("--output", default="", help="Write Markdown report to this path")
    ap.add_argument("--max-open-examples", type=int, default=20)
    args = ap.parse_args()

    repo = Path(args.repo).resolve()
    config = load_config_module(repo)
    mode = config.BLACK_SWAN_MODE
    swan_threshold = getattr(config, "SWAN_BUY_PRICE_THRESHOLD", None)
    swan_entry_max = getattr(config, "SWAN_ENTRY_MAX", None)

    paper_db = latest_paper_db() if args.paper_db == "latest" else Path(args.paper_db)
    dataset_db = Path(args.dataset_db) if args.dataset_db else None
    screener_db = Path(args.screener_log_db) if args.screener_log_db else None

    dataset = connect(dataset_db)
    paper = connect(paper_db)
    slog = connect(screener_db)

    checks: list[dict[str, str]] = []

    # Config checks.
    entry_levels = tuple(float(x) for x in mode.entry_price_levels)
    max_level = max(entry_levels) if entry_levels else None
    add_check(
        checks,
        "config",
        STATUS_OK if max_level is not None and swan_threshold is not None and max_level <= swan_threshold else STATUS_FAIL,
        "entry levels are covered by swans_v2 collection threshold",
        f"entry_price_levels={entry_levels}; max={max_level}; SWAN_BUY_PRICE_THRESHOLD={swan_threshold}; SWAN_ENTRY_MAX={swan_entry_max}",
        "Trading above the historical collection ceiling would make scorer/sizing blind.",
    )
    add_check(
        checks,
        "config",
        STATUS_OK if abs(sum(w for _, w in mode.scoring_weights) - 1.0) < 1e-9 else STATUS_WARN,
        "composite scoring weights are normalized",
        f"scoring_weights={mode.scoring_weights}; sum={sum(w for _, w in mode.scoring_weights):.4f}",
    )
    add_check(
        checks,
        "config",
        STATUS_OK if getattr(mode, "min_market_score", 0.0) > 0 else STATUS_WARN,
        "market_score hard gate is enabled for black_swan mode",
        f"min_market_score={getattr(mode, 'min_market_score', None)}",
        "A zero threshold means MarketScorer still ranks/logs candidates, but does not hard-reject low-score markets.",
    )
    add_check(
        checks,
        "config",
        STATUS_OK if mode.min_hours_to_close >= 0.25 and mode.max_hours_to_close <= 168 else STATUS_WARN,
        "time-to-close gates exclude true 15m and cap at 7d",
        f"min_hours_to_close={mode.min_hours_to_close}; max_hours_to_close={mode.max_hours_to_close}; hours_to_close_null_default={mode.hours_to_close_null_default}",
    )
    try:
        category_weights = getattr(config.BotConfig(mode="black_swan_mode"), "category_weights", {})
    except Exception:
        category_weights = {}
    add_check(
        checks,
        "config",
        STATUS_INFO,
        "analytics-derived scoring knobs present",
        f"category_weights_keys={list(category_weights.keys())}; scoring_weights={mode.scoring_weights}; stake_tiers={mode.stake_tiers}",
    )

    # Source wiring checks.
    swan_live = read_text(repo, "v2/short_horizon/swan_live.py")
    scorer_src = read_text(repo, "strategy/market_scorer.py")
    screener_src = read_text(repo, "strategy/screener.py")
    black_strategy_src = read_text(repo, "v2/short_horizon/short_horizon/strategies/black_swan_strategy_v1.py")
    runtime_strategy_src = read_text(repo, "v2/short_horizon/short_horizon/strategies/swan_strategy_v1.py")
    markets_src = read_text(repo, "v2/short_horizon/short_horizon/venue_polymarket/markets.py")

    add_check(
        checks,
        "strategy",
        STATUS_OK if "class BlackSwanStrategyV1" in black_strategy_src and "SwanStrategyV1" in black_strategy_src else STATUS_FAIL,
        "black_swan strategy wraps resting-bid SwanStrategyV1",
        "BlackSwanStrategyV1 source present and inherits/uses SwanStrategyV1" if black_strategy_src else "missing black_swan_strategy_v1.py",
    )
    add_check(
        checks,
        "scorer",
        STATUS_OK if "use_black_swan_label=(strategy_name == \"black_swan\")" in swan_live and "was_black_swan" in scorer_src else STATUS_FAIL,
        "MarketScorer uses strict was_black_swan label in black_swan mode",
        "swan_live passes use_black_swan_label for black_swan; market_scorer switches good_col to was_black_swan" if "was_black_swan" in scorer_src else "was_black_swan not found in scorer",
    )
    add_check(
        checks,
        "screener",
        STATUS_OK if all(x in screener_src for x in ["rejected_hours_to_close_min", "rejected_hours_to_close_max", "rejected_price_above_entry_max", "rejected_market_score"]) else STATUS_FAIL,
        "screener has core hard gates",
        "hours min/max, entry price max, and market_score rejection outcomes are present",
    )
    add_check(
        checks,
        "screener",
        STATUS_WARN if "min_total_duration_hours" not in screener_src else STATUS_OK,
        "min_total_duration_hours is enforced at screener level",
        "min_total_duration_hours appears unused in strategy/screener.py" if "min_total_duration_hours" not in screener_src else "min_total_duration_hours appears in screener source",
        "If only time_remaining is enforced, short-lived markets can enter unless discovery excludes them elsewhere.",
    )
    add_check(
        checks,
        "universe",
        STATUS_OK if "duration_metric=\"time_remaining\"" in swan_live and "max_seconds_to_end=_max_secs" in swan_live else STATUS_WARN,
        "universe discovery uses time_remaining window",
        "swan_live builds DurationWindow from min/max hours_to_close and max_seconds_to_end",
    )
    add_check(
        checks,
        "universe",
        STATUS_INFO if "UniverseFilter(allowed_assets=())" in swan_live else STATUS_OK,
        "asset/category universe is broad, then filtered by screener/scorer",
        "UniverseFilter(allowed_assets=()) is intentional for black_swan: #196 edge spans weather/geopolitics/politics/entertainment/crypto/etc.; category control belongs in Gamma normalization + screener/scorer gates.",
        "If this changes to a narrow asset allowlist, verify it does not remove non-crypto #196 cohorts.",
    )
    add_check(
        checks,
        "execution",
        STATUS_OK if "post_only=True" in runtime_strategy_src else STATUS_WARN,
        "execution places resting post-only bids",
        "SwanStrategyV1 builds OrderIntent with post_only=True" if "post_only=True" in runtime_strategy_src else "post_only=True not found",
    )

    # Feature mart / historical analytics.
    feature_cols = columns(dataset, "feature_mart_v1_1")
    swans_cols = columns(dataset, "swans_v2")
    add_check(
        checks,
        "feature_mart",
        STATUS_OK if "was_black_swan" in feature_cols else STATUS_FAIL,
        "feature_mart_v1_1 includes strict black-swan label",
        f"feature_mart_v1_1 columns include was_black_swan={ 'was_black_swan' in feature_cols }; dataset_db={dataset_db}",
    )
    add_check(
        checks,
        "analytics_db",
        STATUS_OK if "black_swan" in swans_cols else STATUS_FAIL,
        "swans_v2 includes strict black_swan label",
        f"swans_v2 columns include black_swan={ 'black_swan' in swans_cols }",
    )

    hist_summary: list[str] = []
    if dataset and table_exists(dataset, "feature_mart_v1_1"):
        total = scalar(dataset, "select count(*) from feature_mart_v1_1")
        strict = scalar(dataset, "select coalesce(sum(was_black_swan),0) from feature_mart_v1_1")
        true15 = scalar(dataset, "select coalesce(sum(was_black_swan),0) from feature_mart_v1_1 where duration_hours is not null and abs(duration_hours - 0.25) <= 1e-6")
        hist_summary.append(f"feature_mart_v1_1 rows={total}; was_black_swan={strict}; true_15m_black_swans={true15}")
        for r in rows(dataset, """
            select coalesce(category,'null') category,
                   case when duration_hours is null then 'unknown'
                        when abs(duration_hours - 0.25) <= 1e-6 then 'true15m'
                        when duration_hours <= 1 then '<=1h'
                        when duration_hours <= 6 then '<=6h'
                        when duration_hours <= 168 then '<=7d'
                        else '>7d' end bucket,
                   count(*) markets,
                   sum(was_black_swan) strict
            from feature_mart_v1_1
            group by category,bucket
            having strict > 0
            order by strict desc
            limit 12
        """):
            hist_summary.append(f"{r['category']} {r['bucket']}: strict={r['strict']} markets={r['markets']}")

    # Current paper DB.
    paper_summary: list[str] = []
    open_market_ids: list[str] = []
    if paper and table_exists(paper, "orders"):
        run = rows(paper, "select run_id, started_at, finished_at, mode, strategy_id, git_sha from runs order by started_at desc limit 1")
        if run:
            paper_summary.append("run=" + json.dumps(dict(run[0]), ensure_ascii=False))
        counts = {t: scalar(paper, f"select count(*) from {t}") for t in ["markets", "orders", "fills", "events_log"] if table_exists(paper, t)}
        paper_summary.append("counts=" + json.dumps(counts, ensure_ascii=False))
        for r in rows(paper, "select state, count(*) n, sum(size) size, sum(price*size) notional from orders group by state order by n desc"):
            paper_summary.append(f"orders state={r['state']} n={r['n']} size={fmt_num(r['size'])} notional={fmt_num(r['notional'])}")
        for r in rows(paper, "select price, count(*) n, sum(size) size from orders group by price order by price"):
            paper_summary.append(f"orders price={fmt_num(r['price'])} n={r['n']} size={fmt_num(r['size'])}")

        open_filter = "coalesce(remaining_size,size)>0 and upper(state) not in ('CANCELED','CANCEL_CONFIRMED','REJECTED','FILLED')"
        open_market_ids = [r[0] for r in rows(paper, f"select distinct market_id from orders where {open_filter}")]
        open_count = len(open_market_ids)
        max_order_price = scalar(paper, f"select max(price) from orders where {open_filter}")
        non_post_only = scalar(paper, "select count(*) from orders where coalesce(post_only,0) != 1") if "post_only" in columns(paper, "orders") else None
        add_check(
            checks,
            "paper_state",
            STATUS_OK if open_count > 0 else STATUS_WARN,
            "paper DB has active/open simulated orders",
            f"paper_db={paper_db}; open_markets={open_count}; max_open_order_price={max_order_price}",
        )
        add_check(
            checks,
            "paper_state",
            STATUS_OK if max_order_price is not None and max_order_price <= mode.entry_price_max else STATUS_FAIL,
            "open order prices obey black_swan entry max",
            f"max_open_order_price={max_order_price}; entry_price_max={mode.entry_price_max}",
        )
        if non_post_only is not None:
            add_check(
                checks,
                "execution",
                STATUS_OK if non_post_only == 0 else STATUS_FAIL,
                "runtime orders are post-only",
                f"non_post_only_orders={non_post_only}",
            )
        for r in rows(paper, f"""
            select case when m.duration_seconds_snapshot is null then 'unknown_total_duration'
                        when m.duration_seconds_snapshot/3600.0 <= 0.25 then 'total<=15m'
                        when m.duration_seconds_snapshot/3600.0 <= 1 then 'total<=1h'
                        when m.duration_seconds_snapshot/3600.0 <= 6 then 'total<=6h'
                        when m.duration_seconds_snapshot/3600.0 <= 168 then 'total<=7d'
                        else 'total>7d' end bucket,
                   count(distinct o.market_id) markets,
                   count(*) orders
            from orders o left join markets m using(market_id)
            where {open_filter}
            group by bucket order by bucket
        """):
            paper_summary.append(f"open total-duration bucket={r['bucket']} markets={r['markets']} orders={r['orders']}")
        examples = rows(paper, f"""
            select o.market_id, substr(m.question,1,110) question,
                   m.duration_seconds_snapshot/3600.0 duration_h,
                   m.end_time_latest,
                   count(*) orders,
                   group_concat(distinct o.price) prices,
                   max(o.intent_created_at) latest_intent
            from orders o left join markets m using(market_id)
            where {open_filter}
            group by o.market_id
            order by latest_intent desc
            limit ?
        """, (args.max_open_examples,))
        for r in examples:
            paper_summary.append(f"open {r['market_id']}: prices={r['prices']} duration_h={fmt_num(r['duration_h'])} end={r['end_time_latest']} q={r['question']}")

    # Screener log checks for open markets.
    screener_summary: list[str] = []
    open_score_rows: list[sqlite3.Row] = []
    if slog and table_exists(slog, "screener_log") and open_market_ids:
        placeholders = ",".join("?" for _ in open_market_ids)
        open_score_rows = rows(slog, f"""
            select market_id, question, category, current_price, hours_to_close, volume_usdc,
                   outcome, market_score, max(scanned_at) scanned_at
            from screener_log
            where market_id in ({placeholders})
            group by market_id
            order by scanned_at desc
        """, open_market_ids)
        matched = len(open_score_rows)
        scores = [float(r["market_score"]) for r in open_score_rows if r["market_score"] is not None]
        htc = [float(r["hours_to_close"]) for r in open_score_rows if r["hours_to_close"] is not None]
        below_score = sum(1 for x in scores if x < mode.min_market_score)
        low_htc = sum(1 for x in htc if x < mode.min_hours_to_close)
        high_htc = sum(1 for x in htc if x > mode.max_hours_to_close)
        add_check(
            checks,
            "paper_vs_screener",
            STATUS_OK if matched == len(open_market_ids) else STATUS_WARN,
            "open paper markets have screener provenance rows",
            f"matched={matched}/{len(open_market_ids)} in screener_log={screener_db}",
        )
        add_check(
            checks,
            "paper_vs_screener",
            STATUS_OK if below_score == 0 else STATUS_WARN,
            "latest screener score for open markets passes min_market_score",
            f"min_market_score={mode.min_market_score}; below={below_score}; score_min={fmt_num(min(scores) if scores else None)}; score_avg={fmt_num(sum(scores)/len(scores) if scores else None)}; score_max={fmt_num(max(scores) if scores else None)}",
            "WARN can happen when an already-open order later receives a lower-score/rejected log row; inspect examples.",
        )
        add_check(
            checks,
            "paper_vs_screener",
            STATUS_OK if low_htc == 0 and high_htc == 0 else STATUS_FAIL,
            "latest screener hours_to_close for open markets is inside gates",
            f"min={mode.min_hours_to_close}; max={mode.max_hours_to_close}; below={low_htc}; above={high_htc}; htc_min={fmt_num(min(htc) if htc else None)}; htc_max={fmt_num(max(htc) if htc else None)}",
        )
        cats: dict[str, int] = {}
        outcomes: dict[str, int] = {}
        suspicious_categories: list[str] = []
        for r in open_score_rows:
            cats[str(r["category"])] = cats.get(str(r["category"]), 0) + 1
            outcomes[str(r["outcome"])] = outcomes.get(str(r["outcome"]), 0) + 1
            ql = str(r["question"] or "").lower()
            cat = str(r["category"] or "")
            if ("temperature" in ql or "°" in ql or "fahrenheit" in ql) and cat != "weather":
                suspicious_categories.append(f"{r['market_id']}:{cat}:weather-like question")
            if "eurovision" in ql and cat == "crypto":
                suspicious_categories.append(f"{r['market_id']}:{cat}:eurovision question")
        non_pass_latest = sum(n for outcome, n in outcomes.items() if outcome != "passed_to_order_manager")
        add_check(
            checks,
            "paper_vs_screener",
            STATUS_OK if non_pass_latest == 0 else STATUS_WARN,
            "latest screener outcome for open markets is still pass_to_order_manager",
            f"latest_outcomes={json.dumps(outcomes, ensure_ascii=False, sort_keys=True)}; non_pass_latest={non_pass_latest}",
            "Open orders whose latest screener row is a rejection may be acceptable under TTL, but they are not immediately aligned with the current screener view.",
        )
        add_check(
            checks,
            "weights",
            STATUS_OK if not suspicious_categories else STATUS_WARN,
            "active market categories look consistent with question text",
            f"suspicious_count={len(suspicious_categories)}; examples={suspicious_categories[:10]}",
            "Category drives category_weights and cluster multipliers; bad Gamma categories can silently apply the wrong analytics matrix.",
        )
        screener_summary.append("open categories=" + json.dumps(cats, ensure_ascii=False, sort_keys=True))
        screener_summary.append("latest outcomes=" + json.dumps(outcomes, ensure_ascii=False, sort_keys=True))
        for r in open_score_rows[: args.max_open_examples]:
            screener_summary.append(
                f"{r['market_id']} cat={r['category']} price={fmt_num(r['current_price'])} htc={fmt_num(r['hours_to_close'])} score={fmt_num(r['market_score'])} outcome={r['outcome']} q={str(r['question'])[:100]}"
            )

    # Market categories currently traded vs configured weights.
    if open_score_rows:
        active_cats = {str(r["category"]) for r in open_score_rows if r["category"] is not None}
        missing_weight = sorted(c for c in active_cats if c not in config.BotConfig().category_weights)
        add_check(
            checks,
            "weights",
            STATUS_OK if not missing_weight else STATUS_WARN,
            "active paper categories have explicit category_weights",
            f"active_categories={sorted(active_cats)}; missing_weight={missing_weight}",
        )

    # Render report.
    by_status: dict[str, int] = {}
    for c in checks:
        by_status[c["status"]] = by_status.get(c["status"], 0) + 1
    blocking = [c for c in checks if c["status"] == STATUS_FAIL]
    warnings = [c for c in checks if c["status"] == STATUS_WARN]

    lines: list[str] = []
    lines.append("# Black Swan Paper Trading Alignment Audit")
    lines.append("")
    lines.append(f"Generated: {utc_now()}")
    lines.append(f"Repo: `{repo}`")
    lines.append(f"Repo git SHA: `{repo_git_sha(repo) or 'unknown'}`")
    lines.append(f"Dataset DB: `{dataset_db}`")
    lines.append(f"Paper DB: `{paper_db}`")
    lines.append(f"Screener log DB: `{screener_db}`")
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    if blocking:
        lines.append(f"FAIL: {len(blocking)} hard alignment check(s) failed; paper trading should not be considered analytically aligned until fixed.")
    elif warnings:
        lines.append(f"PARTIAL: no hard failures, but {len(warnings)} warning(s) need review before claiming full alignment.")
    else:
        lines.append("PASS: no hard failures or warnings detected by this checklist.")
    lines.append("")
    lines.append("Status counts: " + ", ".join(f"{k}={v}" for k, v in sorted(by_status.items())))
    lines.append("")
    if warnings or blocking:
        lines.append("## Main findings")
        lines.append("")
        for c in [*blocking, *warnings]:
            lines.append(f"- {c['status']} [{c['area']}] {c['check']}: {c['evidence']}")
            if c.get("implication"):
                lines.append(f"  - Why it matters: {c['implication']}")
        lines.append("")

    lines.append("## Checklist results")
    lines.append("")
    for c in checks:
        lines.append(f"- {c['status']} [{c['area']}] {c['check']}")
        lines.append(f"  - Evidence: {c['evidence']}")
        if c.get("implication"):
            lines.append(f"  - Method/implication: {c['implication']}")
    lines.append("")

    lines.append("## Historical analytics snapshot")
    lines.append("")
    lines.append(bullet_list(hist_summary))
    lines.append("")

    lines.append("## Current paper trading snapshot")
    lines.append("")
    lines.append(bullet_list(paper_summary))
    lines.append("")

    lines.append("## Screener provenance snapshot")
    lines.append("")
    lines.append(bullet_list(screener_summary))
    lines.append("")

    lines.append("## Reusable manual method")
    lines.append("")
    lines.append("- Start from `config.BLACK_SWAN_MODE`: entry levels, max price, time gates, min score, category/cluster/duration/phase multipliers.")
    lines.append("- Verify `analyzer/swan_analyzer.py` and `feature_mart_v1_1` use the same strict `black_swan` definition and that true 15m markets are either explicitly excluded or separately reported.")
    lines.append("- Verify `MarketScorer(use_black_swan_label=True)` is wired for black_swan live/paper mode and uses `was_black_swan`, not broad `label_20x`.")
    lines.append("- Verify the screener applies hours-to-close, price, market-score, dead-market, pattern, sports subtype, neg-risk cohort-size, and entry-level gates before order creation.")
    lines.append("- Verify universe discovery does not silently widen beyond the analytical universe without downstream filters catching it.")
    lines.append("- Verify execution creates post-only resting bids at configured levels and cancels stale/invalid markets according to strategy TTL and cleanup rules.")
    lines.append("- Join current `orders` to `screener_log` and check every open market has recent provenance, score >= min, hours in range, price <= entry max, and category/duration buckets covered by historical matrices.")
    lines.append("- Inspect examples where latest screener outcome is rejection but orders remain open; decide whether cleanup should cancel them immediately or TTL is acceptable.")
    lines.append("")

    report = "\n".join(lines).rstrip() + "\n"
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report, encoding="utf-8")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
