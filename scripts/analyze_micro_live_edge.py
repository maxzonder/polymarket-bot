#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Iterable


@dataclass(frozen=True)
class EdgeTrade:
    run_id: str
    config_hash: str | None
    db_path: str
    market_id: str
    token_id: str
    question: str | None
    asset: str
    direction: str
    entry_price: float
    price_level: float | None
    lifecycle_fraction: float | None
    filled_size: float
    cost_usdc: float
    fee_paid_usdc: float
    fee_rate_bps: float
    gross_pnl_usdc: float
    estimated_fee_usdc: float
    net_pnl_after_estimated_fees: float
    outcome_price: float
    pnl_usdc: float
    pnl_per_usdc: float | None
    break_even_hit_rate: float | None
    resolved_at: str | None

    @property
    def win(self) -> bool:
        return self.net_pnl_after_estimated_fees > 0


@dataclass(frozen=True)
class EdgeBucket:
    name: str
    trades: int
    wins: int
    hit_rate: float | None
    pnl_usdc: float
    gross_pnl_usdc: float
    estimated_fee_usdc: float
    net_pnl_after_estimated_fees: float
    cost_usdc: float
    ev_per_trade_usdc: float | None
    ev_per_usdc: float | None
    break_even_hit_rate: float | None
    ci95_low_usdc: float | None
    ci95_high_usdc: float | None


@dataclass(frozen=True)
class EdgeReport:
    db_paths: list[str]
    trades: list[EdgeTrade]
    overall: EdgeBucket
    by_asset: list[EdgeBucket]
    by_direction: list[EdgeBucket]
    by_price_level: list[EdgeBucket]
    by_lifecycle_bucket: list[EdgeBucket]
    by_run: list[EdgeBucket]
    min_trades_for_go: int

    @property
    def recommendation(self) -> str:
        if self.overall.trades < self.min_trades_for_go:
            return "NO-GO: insufficient sample; keep stake ramp frozen"
        if self.overall.ci95_low_usdc is not None and self.overall.ci95_low_usdc > 0:
            return "GO: aggregate per-trade EV CI lower bound is positive"
        return "NO-GO: aggregate per-trade EV CI lower bound is not positive"

    def as_json(self) -> dict[str, Any]:
        return {
            "db_paths": self.db_paths,
            "min_trades_for_go": self.min_trades_for_go,
            "recommendation": self.recommendation,
            "overall": asdict(self.overall),
            "by_asset": [asdict(item) for item in self.by_asset],
            "by_direction": [asdict(item) for item in self.by_direction],
            "by_price_level": [asdict(item) for item in self.by_price_level],
            "by_lifecycle_bucket": [asdict(item) for item in self.by_lifecycle_bucket],
            "by_run": [asdict(item) for item in self.by_run],
            "trades": [asdict(item) for item in self.trades],
        }

    def as_markdown(self) -> str:
        lines = [
            "# Phase 6 P6-1 edge report",
            "",
            "## Recommendation",
            f"- **{self.recommendation}**",
            f"- Resolved trades analyzed: `{self.overall.trades}` / required `{self.min_trades_for_go}`",
            f"- Total cost: `{self.overall.cost_usdc:.4f} USDC`",
            f"- Gross PnL before estimated taker fees: `{self.overall.gross_pnl_usdc:.4f} USDC`",
            f"- Estimated taker fees: `{self.overall.estimated_fee_usdc:.4f} USDC`",
            f"- Net PnL after estimated fees: `{self.overall.net_pnl_after_estimated_fees:.4f} USDC`",
            f"- Net EV/trade: `{_fmt_optional(self.overall.ev_per_trade_usdc)} USDC`",
            f"- EV/trade 95% CI: `{_fmt_optional(self.overall.ci95_low_usdc)} .. {_fmt_optional(self.overall.ci95_high_usdc)} USDC`",
            f"- EV per 1 USDC deployed: `{_fmt_optional(self.overall.ev_per_usdc)}`",
            f"- Hit rate: `{_fmt_rate(self.overall.hit_rate)}`",
            f"- Break-even hit rate: `{_fmt_rate(self.overall.break_even_hit_rate)}`",
            "",
            "Interpretation:",
        ]
        if self.overall.trades < self.min_trades_for_go:
            lines.append("- This is not enough for a formal P6-1 GO, but it is enough to block stake scaling while selection is investigated.")
        if self.overall.net_pnl_after_estimated_fees < 0:
            lines.append("- The observed live sample is materially negative; do not run the stake ramp from this baseline.")
        elif self.overall.ci95_low_usdc is not None and self.overall.ci95_low_usdc <= 0:
            lines.append("- Aggregate PnL is non-negative, but the confidence interval does not clear the P6-1 GO bar.")
        lines.extend([
            "- Costs use actual filled notional, so venue min-size upscaling is reflected in EV/capital efficiency.",
            "- Estimated fees are computed from stored `fee_rate_bps_latest`; missing fee metadata is a hard analyzer error.",
            "",
            "## Input DBs",
        ])
        for path in self.db_paths:
            lines.append(f"- `{path}`")
        lines.extend(["", "## Breakdown by run"])
        lines.extend(_bucket_lines(self.by_run))
        lines.extend(["", "## Breakdown by asset"])
        lines.extend(_bucket_lines(self.by_asset))
        lines.extend(["", "## Breakdown by direction"])
        lines.extend(_bucket_lines(self.by_direction))
        lines.extend(["", "## Breakdown by entry price level"])
        lines.extend(_bucket_lines(self.by_price_level))
        lines.extend(["", "## Breakdown by lifecycle bucket"])
        lines.extend(_bucket_lines(self.by_lifecycle_bucket))
        lines.extend(["", "## Per-trade records"])
        for trade in self.trades:
            lines.append(
                f"- `{trade.run_id}` `{trade.market_id}` {trade.asset}/{trade.direction} "
                f"level=`{_fmt_optional(trade.price_level)}` lifecycle=`{_fmt_optional(trade.lifecycle_fraction)}` "
                f"cost=`{trade.cost_usdc:.4f}` fee=`{trade.estimated_fee_usdc:.4f}` outcome=`{trade.outcome_price:.4f}` "
                f"gross_pnl=`{trade.gross_pnl_usdc:.4f}` net_pnl=`{trade.net_pnl_after_estimated_fees:.4f}` "
                f"question={trade.question!r}"
            )
        return "\n".join(lines)


def analyze_edge(db_paths: Iterable[str | Path], *, min_trades_for_go: int = 50) -> EdgeReport:
    paths = [Path(path) for path in db_paths]
    trades: list[EdgeTrade] = []
    for path in paths:
        trades.extend(_extract_trades(path))
    trades.sort(key=lambda item: (item.resolved_at or "", item.run_id, item.market_id, item.token_id))
    return EdgeReport(
        db_paths=[str(path) for path in paths],
        trades=trades,
        overall=_summarize_bucket("overall", trades),
        by_asset=_summarize_group(trades, lambda trade: trade.asset),
        by_direction=_summarize_group(trades, lambda trade: trade.direction),
        by_price_level=_summarize_group(trades, lambda trade: _fmt_group_float(trade.price_level)),
        by_lifecycle_bucket=_summarize_group(trades, lambda trade: _lifecycle_bucket(trade.lifecycle_fraction)),
        by_run=_summarize_group(trades, lambda trade: trade.run_id),
        min_trades_for_go=int(min_trades_for_go),
    )


def _extract_trades(db_path: Path) -> list[EdgeTrade]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    run_row = conn.execute("SELECT * FROM runs ORDER BY started_at LIMIT 1").fetchone()
    run_id = str(run_row["run_id"]) if run_row is not None else db_path.stem
    config_hash = str(run_row["config_hash"]) if run_row is not None and run_row["config_hash"] is not None else None
    markets = {str(row["market_id"]): row for row in conn.execute("SELECT * FROM markets")}
    market_state = _market_state_metadata(conn, run_id)
    intents = _order_intents(conn, run_id)
    fill_costs = _fill_costs(conn, run_id)
    resolved = _resolved_events(conn, run_id)

    trades: list[EdgeTrade] = []
    for item in resolved:
        market_id = str(item.get("market_id") or "")
        token_id = str(item.get("token_id") or "")
        if not market_id or not token_id:
            continue
        key = (market_id, token_id)
        market = markets.get(market_id)
        state_meta = market_state.get(market_id, {})
        intent = intents.get(key) or {}
        fill = fill_costs.get(key, {})
        entry_price = _float_or_none(item.get("average_entry_price"))
        if entry_price is None:
            entry_price = _float_or_none(intent.get("entry_price"))
        if entry_price is None:
            entry_price = 0.0
        filled_size = _float_or_none(item.get("size")) or float(fill.get("size") or 0.0)
        fee_rate_bps = _fee_rate_bps_for_market(market=market, state_meta=state_meta, market_id=market_id, db_path=db_path)
        fee_paid = float(fill.get("fee_paid_usdc") or 0.0)
        cost = float(fill.get("cost_usdc") or 0.0)
        if cost <= 0 and filled_size > 0:
            cost = filled_size * entry_price
        gross_pnl = _float_or_none(item.get("estimated_pnl_usdc"))
        outcome = _float_or_none(item.get("outcome_price"))
        if gross_pnl is None:
            gross_pnl = ((outcome or 0.0) * filled_size) - cost
        estimated_fee = cost * fee_rate_bps / 10000.0
        net_pnl = gross_pnl - estimated_fee
        question = str(market["question"]) if market is not None and market["question"] is not None else None
        trades.append(
            EdgeTrade(
                run_id=run_id,
                config_hash=config_hash,
                db_path=str(db_path),
                market_id=market_id,
                token_id=token_id,
                question=question,
                asset=_asset_from_question(question, state_meta=state_meta),
                direction=_direction_from_market(market, token_id, state_meta=state_meta),
                entry_price=entry_price,
                price_level=_float_or_none(intent.get("level")) or _rounded_level(entry_price),
                lifecycle_fraction=_float_or_none(intent.get("lifecycle_fraction")),
                filled_size=filled_size,
                cost_usdc=cost,
                fee_paid_usdc=fee_paid,
                fee_rate_bps=fee_rate_bps,
                gross_pnl_usdc=gross_pnl,
                estimated_fee_usdc=estimated_fee,
                net_pnl_after_estimated_fees=net_pnl,
                outcome_price=outcome or 0.0,
                pnl_usdc=net_pnl,
                pnl_per_usdc=(net_pnl / cost) if cost > 0 else None,
                break_even_hit_rate=((cost + estimated_fee) / filled_size) if filled_size > 0 else None,
                resolved_at=str(item.get("event_time") or "") or None,
            )
        )
    return trades


def _market_state_metadata(conn: sqlite3.Connection, run_id: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in conn.execute(
        "SELECT market_id, payload_json FROM events_log WHERE run_id = ? AND event_type = 'MarketStateUpdate' ORDER BY event_time",
        (run_id,),
    ):
        market_id = str(row["market_id"] or "")
        if not market_id or market_id in result:
            continue
        payload = _loads(row["payload_json"])
        if payload:
            result[market_id] = payload
    return result


def _order_intents(conn: sqlite3.Connection, run_id: str) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in conn.execute(
        "SELECT market_id, token_id, payload_json FROM events_log WHERE run_id = ? AND event_type = 'OrderIntent' ORDER BY event_time",
        (run_id,),
    ):
        payload = _loads(row["payload_json"])
        result[(str(row["market_id"]), str(row["token_id"]))] = payload
    return result


def _fill_costs(conn: sqlite3.Connection, run_id: str) -> dict[tuple[str, str], dict[str, float]]:
    result: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: {"cost_usdc": 0.0, "size": 0.0, "fee_paid_usdc": 0.0})
    for row in conn.execute("SELECT * FROM fills WHERE run_id = ?", (run_id,)):
        key = (str(row["market_id"]), str(row["token_id"]))
        result[key]["cost_usdc"] += float(row["price"] or 0.0) * float(row["size"] or 0.0)
        result[key]["size"] += float(row["size"] or 0.0)
        result[key]["fee_paid_usdc"] += float(row["fee_paid_usdc"] or 0.0)
    return dict(result)


def _resolved_events(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in conn.execute(
        "SELECT payload_json FROM events_log WHERE run_id = ? AND event_type = 'MarketResolvedWithInventory' ORDER BY event_time",
        (run_id,),
    ):
        payload = _loads(row["payload_json"])
        if payload:
            items.append(payload)
    return items


def _summarize_group(trades: list[EdgeTrade], key_func) -> list[EdgeBucket]:
    grouped: dict[str, list[EdgeTrade]] = defaultdict(list)
    for trade in trades:
        grouped[str(key_func(trade) or "unknown")].append(trade)
    return sorted((_summarize_bucket(name, rows) for name, rows in grouped.items()), key=lambda item: (-item.trades, item.name))


def _summarize_bucket(name: str, trades: list[EdgeTrade]) -> EdgeBucket:
    n = len(trades)
    pnl_values = [trade.net_pnl_after_estimated_fees for trade in trades]
    wins = sum(1 for trade in trades if trade.win)
    gross_pnl = sum(trade.gross_pnl_usdc for trade in trades)
    estimated_fee = sum(trade.estimated_fee_usdc for trade in trades)
    net_pnl = sum(pnl_values)
    cost = sum(trade.cost_usdc for trade in trades)
    filled_size = sum(trade.filled_size for trade in trades)
    avg = mean(pnl_values) if pnl_values else None
    ci_low: float | None = None
    ci_high: float | None = None
    if n == 1:
        ci_low = pnl_values[0]
        ci_high = pnl_values[0]
    elif n > 1:
        half_width = 1.96 * stdev(pnl_values) / math.sqrt(n)
        ci_low = float(avg - half_width) if avg is not None else None
        ci_high = float(avg + half_width) if avg is not None else None
    return EdgeBucket(
        name=name,
        trades=n,
        wins=wins,
        hit_rate=(wins / n) if n else None,
        pnl_usdc=net_pnl,
        gross_pnl_usdc=gross_pnl,
        estimated_fee_usdc=estimated_fee,
        net_pnl_after_estimated_fees=net_pnl,
        cost_usdc=cost,
        ev_per_trade_usdc=avg,
        ev_per_usdc=(net_pnl / cost) if cost > 0 else None,
        break_even_hit_rate=((cost + estimated_fee) / filled_size) if filled_size > 0 else None,
        ci95_low_usdc=ci_low,
        ci95_high_usdc=ci_high,
    )


def _loads(value: str | bytes | None) -> dict[str, Any]:
    if value is None:
        return {}
    try:
        item = json.loads(value)
    except Exception:
        return {}
    return item if isinstance(item, dict) else {}


def _fee_rate_bps_for_market(*, market: sqlite3.Row | None, state_meta: dict[str, Any], market_id: str, db_path: Path) -> float:
    value: Any = None
    if market is not None and "fee_rate_bps_latest" in market.keys():
        value = market["fee_rate_bps_latest"]
    if value is None:
        value = state_meta.get("fee_rate_bps")
    fee_rate_bps = _float_or_none(value)
    if fee_rate_bps is None:
        raise ValueError(f"missing fee_rate_bps_latest for market_id={market_id} in {db_path}")
    return fee_rate_bps


def _asset_from_question(question: str | None, *, state_meta: dict[str, Any] | None = None) -> str:
    asset_slug = str((state_meta or {}).get("asset_slug") or "").lower()
    if asset_slug in {"bitcoin", "btc"}:
        return "BTC"
    if asset_slug in {"ethereum", "eth"}:
        return "ETH"
    text = (question or "").lower()
    if "bitcoin" in text or "btc" in text:
        return "BTC"
    if "ethereum" in text or "eth" in text:
        return "ETH"
    return "unknown"


def _direction_from_market(market: sqlite3.Row | None, token_id: str, *, state_meta: dict[str, Any] | None = None) -> str:
    yes_token = (state_meta or {}).get("token_yes_id")
    no_token = (state_meta or {}).get("token_no_id")
    if (yes_token is None or no_token is None) and market is not None:
        if "token_yes_id" in market.keys():
            yes_token = market["token_yes_id"]
        if "token_no_id" in market.keys():
            no_token = market["token_no_id"]
    if yes_token is not None and str(yes_token) == token_id:
        return "UP/YES"
    if no_token is not None and str(no_token) == token_id:
        return "DOWN/NO"
    return "unknown"


def _lifecycle_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    lower = math.floor(value * 10) / 10
    upper = lower + 0.1
    return f"{lower:.1f}-{upper:.1f}"


def _rounded_level(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _fmt_group_float(value: float | None) -> str:
    return "unknown" if value is None else f"{value:.2f}"


def _fmt_optional(value: float | None) -> str:
    return "unknown" if value is None else f"{value:.4f}"


def _fmt_rate(value: float | None) -> str:
    return "unknown" if value is None else f"{value * 100:.1f}%"


def _bucket_lines(buckets: list[EdgeBucket]) -> list[str]:
    if not buckets:
        return ["- none"]
    lines: list[str] = []
    for bucket in buckets:
        lines.append(
            f"- `{bucket.name}`: trades=`{bucket.trades}`, wins=`{bucket.wins}`, "
            f"hit=`{_fmt_rate(bucket.hit_rate)}`, cost=`{bucket.cost_usdc:.4f}`, "
            f"gross_pnl=`{bucket.gross_pnl_usdc:.4f}`, fee=`{bucket.estimated_fee_usdc:.4f}`, "
            f"net_pnl=`{bucket.net_pnl_after_estimated_fees:.4f}`, EV/trade=`{_fmt_optional(bucket.ev_per_trade_usdc)}`, "
            f"CI95=`{_fmt_optional(bucket.ci95_low_usdc)}..{_fmt_optional(bucket.ci95_high_usdc)}`, "
            f"EV/USDC=`{_fmt_optional(bucket.ev_per_usdc)}`, break_even_hit=`{_fmt_rate(bucket.break_even_hit_rate)}`"
        )
    return lines


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze realized edge for short-horizon micro-live probe DBs")
    parser.add_argument("db_paths", nargs="+", help="One or more probe SQLite DB paths")
    parser.add_argument("--min-trades-for-go", type=int, default=50, help="Required resolved trade sample for P6-1 GO")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON instead of Markdown")
    parser.add_argument("--out", default=None, help="Optional output path")
    args = parser.parse_args()

    report = analyze_edge(args.db_paths, min_trades_for_go=args.min_trades_for_go)
    output = json.dumps(report.as_json(), indent=2, sort_keys=True) if args.json else report.as_markdown()
    if args.out:
        Path(args.out).write_text(output + "\n")
    else:
        print(output)


if __name__ == "__main__":
    main()
