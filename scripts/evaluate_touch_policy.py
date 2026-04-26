#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import random
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

DEFAULT_SCORES_PATH = Path("v2/short_horizon/data/touch_model_scores.sqlite3")
DEFAULT_REPORT_PATH = Path("v2/short_horizon/docs/phase6/p6-2c_policy_report.md")
DEFAULT_SEED = 2602


@dataclass(frozen=True)
class PolicyCandidate:
    probe_id: str
    market_id: str
    touch_time_iso: str
    split: str
    resolves_yes: int
    market_price: float
    model_prob: float
    fee_rate_bps: float
    would_be_cost_after_min_shares: float | None = None
    survived_ms: float | None = None
    asset_slug: str = ""
    direction: str = ""
    lifecycle_fraction: float | None = None
    spot_implied_prob: float | None = None
    spot_implied_prob_minus_market_prob: float | None = None
    spot_source: str = ""
    fit_10_usdc: str | None = None
    tick_size: float | None = None


@dataclass(frozen=True)
class PolicyDecision:
    candidate: PolicyCandidate
    action: str
    reason: str
    stake_usdc: float
    edge: float
    required_gap: float
    net_pnl_usdc: float
    net_ev_per_usdc: float | None


@dataclass(frozen=True)
class PolicySummary:
    scores_path: str
    report_path: str
    split: str
    input_rows: int
    accepted_trades: int
    skipped_rows: int
    gross_stake_usdc: float
    net_pnl_usdc: float
    net_ev_per_usdc: float | None
    net_ev_per_trade: float | None
    bootstrap_ci_95: dict[str, float | None]
    reason_counts: dict[str, int]
    go_gate: str

    def as_json(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_touch_policy_from_scores(
    scores_path: str | Path = DEFAULT_SCORES_PATH,
    *,
    report_path: str | Path = DEFAULT_REPORT_PATH,
    touch_dataset_path: str | Path | None = None,
    spot_features_path: str | Path | None = None,
    split: str = "test",
    stake_usdc: float = 10.0,
    edge_buffer_bps: float = 200.0,
    min_size_policy: str = "upscale",
    max_orders_per_market_per_run: int = 1,
    daily_loss_cap_usdc: float = 30.0,
    min_survived_ms: float = 0.0,
    asset_allowlist: set[str] | None = None,
    min_lifecycle_fraction: float | None = None,
    min_spot_implied_prob_minus_market_prob: float | None = None,
    direction_allowlist: set[str] | None = None,
    spot_source_prefix_allowlist: set[str] | None = None,
    fit_10_allowlist: set[str] | None = None,
    use_fit_10_entry_price: bool = False,
    edge_probability_field: str = "model_prob",
    bootstrap_samples: int = 1000,
    seed: int = DEFAULT_SEED,
) -> PolicySummary:
    candidates = load_candidates(
        scores_path,
        split=split,
        touch_dataset_path=touch_dataset_path,
        spot_features_path=spot_features_path,
    )
    decisions = evaluate_candidates(
        candidates,
        stake_usdc=stake_usdc,
        edge_buffer_bps=edge_buffer_bps,
        min_size_policy=min_size_policy,
        max_orders_per_market_per_run=max_orders_per_market_per_run,
        daily_loss_cap_usdc=daily_loss_cap_usdc,
        min_survived_ms=min_survived_ms,
        asset_allowlist=asset_allowlist,
        min_lifecycle_fraction=min_lifecycle_fraction,
        min_spot_implied_prob_minus_market_prob=min_spot_implied_prob_minus_market_prob,
        direction_allowlist=direction_allowlist,
        spot_source_prefix_allowlist=spot_source_prefix_allowlist,
        fit_10_allowlist=fit_10_allowlist,
        use_fit_10_entry_price=use_fit_10_entry_price,
        edge_probability_field=edge_probability_field,
    )
    summary = summarize_decisions(
        decisions,
        scores_path=scores_path,
        report_path=report_path,
        split=split,
        bootstrap_samples=bootstrap_samples,
        seed=seed,
    )
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        render_policy_report(
            summary,
            decisions,
            stake_usdc=stake_usdc,
            edge_buffer_bps=edge_buffer_bps,
            min_size_policy=min_size_policy,
            max_orders_per_market_per_run=max_orders_per_market_per_run,
            daily_loss_cap_usdc=daily_loss_cap_usdc,
            min_survived_ms=min_survived_ms,
            asset_allowlist=asset_allowlist,
            min_lifecycle_fraction=min_lifecycle_fraction,
            min_spot_implied_prob_minus_market_prob=min_spot_implied_prob_minus_market_prob,
            direction_allowlist=direction_allowlist,
            spot_source_prefix_allowlist=spot_source_prefix_allowlist,
            fit_10_allowlist=fit_10_allowlist,
            use_fit_10_entry_price=use_fit_10_entry_price,
            edge_probability_field=edge_probability_field,
        ),
        encoding="utf-8",
    )
    return summary


def load_candidates(
    scores_path: str | Path,
    *,
    split: str = "test",
    touch_dataset_path: str | Path | None = None,
    spot_features_path: str | Path | None = None,
) -> list[PolicyCandidate]:
    touch_by_probe = _load_optional_table_by_probe_id(touch_dataset_path, "touch_dataset")
    spot_by_probe = _load_optional_table_by_probe_id(spot_features_path, "spot_features")
    conn = sqlite3.connect(scores_path)
    conn.row_factory = sqlite3.Row
    try:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='touch_model_scores'").fetchone() is None:
            raise ValueError(f"{scores_path} does not contain table touch_model_scores")
        rows = conn.execute(
            """
            SELECT probe_id, market_id, touch_time_iso, split, resolves_yes, market_price,
                   model_prob, fee_rate_bps, would_be_cost_after_min_shares, survived_ms,
                   asset_slug, direction
            FROM touch_model_scores
            WHERE split = ?
            ORDER BY event_time_ms, probe_id
            """,
            (split,),
        ).fetchall()
        return [
            _candidate_from_score_row(row, touch_by_probe.get(str(row["probe_id"]), {}), spot_by_probe.get(str(row["probe_id"]), {}))
            for row in rows
        ]
    finally:
        conn.close()


def evaluate_candidates(
    candidates: Sequence[PolicyCandidate],
    *,
    stake_usdc: float,
    edge_buffer_bps: float,
    min_size_policy: str,
    max_orders_per_market_per_run: int,
    daily_loss_cap_usdc: float,
    min_survived_ms: float,
    asset_allowlist: set[str] | None = None,
    min_lifecycle_fraction: float | None = None,
    min_spot_implied_prob_minus_market_prob: float | None = None,
    direction_allowlist: set[str] | None = None,
    spot_source_prefix_allowlist: set[str] | None = None,
    fit_10_allowlist: set[str] | None = None,
    use_fit_10_entry_price: bool = False,
    edge_probability_field: str = "model_prob",
) -> list[PolicyDecision]:
    if min_size_policy not in {"upscale", "skip"}:
        raise ValueError("min_size_policy must be 'upscale' or 'skip'")
    if edge_probability_field not in {"model_prob", "spot_implied_prob"}:
        raise ValueError("edge_probability_field must be 'model_prob' or 'spot_implied_prob'")
    asset_allowlist = _normalize_set(asset_allowlist)
    direction_allowlist = _normalize_set(direction_allowlist)
    spot_source_prefix_allowlist = _normalize_set(spot_source_prefix_allowlist)
    fit_10_allowlist = set(fit_10_allowlist or set()) or None
    decisions: list[PolicyDecision] = []
    orders_by_market: dict[str, int] = {}
    realized_by_day: dict[str, float] = {}
    for candidate in candidates:
        date_key = candidate.touch_time_iso[:10]
        entry_price = effective_entry_price(candidate, use_fit_10_entry_price=use_fit_10_entry_price)
        edge_probability = _edge_probability(candidate, edge_probability_field)
        edge = (edge_probability - entry_price) if edge_probability is not None and entry_price is not None else float("-inf")
        fee_gap = break_even_fee_gap(entry_price, candidate.fee_rate_bps) if entry_price is not None else float("inf")
        required_gap = fee_gap + edge_buffer_bps / 10000.0
        min_cost = candidate.would_be_cost_after_min_shares or max(stake_usdc, (entry_price or candidate.market_price) * 5.0)
        actual_stake = max(stake_usdc, min_cost) if min_size_policy == "upscale" else stake_usdc
        reason = "accepted"
        action = "accept"
        if asset_allowlist is not None and candidate.asset_slug.lower() not in asset_allowlist:
            action = "skip"
            reason = "asset_not_allowed"
            actual_stake = 0.0
        elif min_lifecycle_fraction is not None and (candidate.lifecycle_fraction is None or candidate.lifecycle_fraction < min_lifecycle_fraction):
            action = "skip"
            reason = "lifecycle_fraction_below_min"
            actual_stake = 0.0
        elif min_spot_implied_prob_minus_market_prob is not None and (
            candidate.spot_implied_prob_minus_market_prob is None
            or candidate.spot_implied_prob_minus_market_prob < min_spot_implied_prob_minus_market_prob
        ):
            action = "skip"
            reason = "spot_gap_below_min"
            actual_stake = 0.0
        elif direction_allowlist is not None and candidate.direction.lower() not in direction_allowlist:
            action = "skip"
            reason = "direction_not_allowed"
            actual_stake = 0.0
        elif spot_source_prefix_allowlist is not None and not _has_allowed_prefix(candidate.spot_source, spot_source_prefix_allowlist):
            action = "skip"
            reason = "spot_source_not_allowed"
            actual_stake = 0.0
        elif fit_10_allowlist is not None and (candidate.fit_10_usdc or "") not in fit_10_allowlist:
            action = "skip"
            reason = "fit_10_not_allowed"
            actual_stake = 0.0
        elif entry_price is None:
            action = "skip"
            reason = "entry_price_unavailable"
            actual_stake = 0.0
        elif edge_probability is None:
            action = "skip"
            reason = "edge_probability_unavailable"
            actual_stake = 0.0
        elif edge < required_gap:
            action = "skip"
            reason = "model_edge_below_required_gap"
            actual_stake = 0.0
        elif min_size_policy == "skip" and min_cost > stake_usdc:
            action = "skip"
            reason = "below_min_order_shares"
            actual_stake = 0.0
        elif orders_by_market.get(candidate.market_id, 0) >= max_orders_per_market_per_run:
            action = "skip"
            reason = "max_orders_per_market_per_run"
            actual_stake = 0.0
        elif candidate.survived_ms is not None and candidate.survived_ms < min_survived_ms:
            action = "skip"
            reason = "stale_book_guard"
            actual_stake = 0.0
        elif realized_by_day.get(date_key, 0.0) <= -abs(daily_loss_cap_usdc):
            action = "skip"
            reason = "daily_loss_cap_reached"
            actual_stake = 0.0

        net_pnl = 0.0
        net_ev = None
        if action == "accept":
            net_pnl = realized_net_pnl(candidate, actual_stake, entry_price=entry_price)
            net_ev = net_pnl / actual_stake if actual_stake else None
            orders_by_market[candidate.market_id] = orders_by_market.get(candidate.market_id, 0) + 1
            realized_by_day[date_key] = realized_by_day.get(date_key, 0.0) + net_pnl
        decisions.append(
            PolicyDecision(
                candidate=candidate,
                action=action,
                reason=reason,
                stake_usdc=actual_stake,
                edge=edge,
                required_gap=required_gap,
                net_pnl_usdc=net_pnl,
                net_ev_per_usdc=net_ev,
            )
        )
    return decisions


def realized_net_pnl(candidate: PolicyCandidate, stake_usdc: float, *, entry_price: float | None = None) -> float:
    price = entry_price if entry_price is not None else candidate.market_price
    if price <= 0:
        return 0.0
    shares = stake_usdc / price
    payout = shares if candidate.resolves_yes else 0.0
    estimated_fee = stake_usdc * candidate.fee_rate_bps / 10000.0
    return payout - stake_usdc - estimated_fee


def break_even_fee_gap(market_price: float, fee_rate_bps: float) -> float:
    denominator = 10000.0 - fee_rate_bps
    if denominator <= 0:
        return float("inf")
    return market_price * fee_rate_bps / denominator


def summarize_decisions(
    decisions: Sequence[PolicyDecision],
    *,
    scores_path: str | Path,
    report_path: str | Path,
    split: str,
    bootstrap_samples: int,
    seed: int,
) -> PolicySummary:
    accepted = [decision for decision in decisions if decision.action == "accept"]
    stake = sum(decision.stake_usdc for decision in accepted)
    pnl = sum(decision.net_pnl_usdc for decision in accepted)
    ev_per_usdc = pnl / stake if stake else None
    ev_per_trade = pnl / len(accepted) if accepted else None
    ci = bootstrap_ev_ci(accepted, samples=bootstrap_samples, seed=seed)
    reason_counts: dict[str, int] = {}
    for decision in decisions:
        reason_counts[decision.reason] = reason_counts.get(decision.reason, 0) + 1
    go_gate = "GO" if ev_per_usdc is not None and ci.get("lower") is not None and ci["lower"] > 0 else "NO-GO"
    return PolicySummary(
        scores_path=str(scores_path),
        report_path=str(report_path),
        split=split,
        input_rows=len(decisions),
        accepted_trades=len(accepted),
        skipped_rows=len(decisions) - len(accepted),
        gross_stake_usdc=stake,
        net_pnl_usdc=pnl,
        net_ev_per_usdc=ev_per_usdc,
        net_ev_per_trade=ev_per_trade,
        bootstrap_ci_95=ci,
        reason_counts=dict(sorted(reason_counts.items())),
        go_gate=go_gate,
    )


def bootstrap_ev_ci(decisions: Sequence[PolicyDecision], *, samples: int = 1000, seed: int = DEFAULT_SEED) -> dict[str, float | None]:
    if not decisions:
        return {"lower": None, "median": None, "upper": None}
    rng = random.Random(seed)
    values: list[float] = []
    n = len(decisions)
    for _ in range(samples):
        draw = [decisions[rng.randrange(n)] for _ in range(n)]
        stake = sum(item.stake_usdc for item in draw)
        pnl = sum(item.net_pnl_usdc for item in draw)
        values.append(pnl / stake if stake else 0.0)
    values.sort()
    return {
        "lower": _percentile(values, 0.025),
        "median": _percentile(values, 0.5),
        "upper": _percentile(values, 0.975),
    }


def render_policy_report(
    summary: PolicySummary,
    decisions: Sequence[PolicyDecision],
    *,
    stake_usdc: float,
    edge_buffer_bps: float,
    min_size_policy: str,
    max_orders_per_market_per_run: int,
    daily_loss_cap_usdc: float,
    min_survived_ms: float,
    asset_allowlist: set[str] | None,
    min_lifecycle_fraction: float | None,
    min_spot_implied_prob_minus_market_prob: float | None,
    direction_allowlist: set[str] | None,
    spot_source_prefix_allowlist: set[str] | None,
    fit_10_allowlist: set[str] | None,
    use_fit_10_entry_price: bool,
    edge_probability_field: str,
) -> str:
    accepted = [decision for decision in decisions if decision.action == "accept"]
    lines = [
        "# P6-2c held-out policy evaluation",
        "",
        f"Generated: `{datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}`",
        "",
        "## Policy",
        "",
        f"- Split: `{summary.split}`",
        f"- Stake before min-size policy: `{stake_usdc:.4f} USDC`",
        f"- Edge buffer after fee break-even: `{edge_buffer_bps:.1f} bps`",
        f"- Min-size policy: `{min_size_policy}`",
        f"- Max orders per market per run: `{max_orders_per_market_per_run}`",
        f"- Daily loss cap: `{daily_loss_cap_usdc:.4f} USDC`",
        f"- Minimum survived_ms stale-book guard: `{min_survived_ms:.1f}`",
        f"- Edge probability field: `{edge_probability_field}`",
        f"- Fit-10 slippage-adjusted entry price: `{use_fit_10_entry_price}`",
        f"- Asset allowlist: `{_fmt_set(asset_allowlist)}`",
        f"- Minimum lifecycle_fraction: `{_fmt(min_lifecycle_fraction)}`",
        f"- Minimum spot_implied_prob_minus_market_prob: `{_fmt(min_spot_implied_prob_minus_market_prob)}`",
        f"- Direction allowlist: `{_fmt_set(direction_allowlist)}`",
        f"- Spot source prefix allowlist: `{_fmt_set(spot_source_prefix_allowlist)}`",
        f"- fit_10_usdc allowlist: `{_fmt_set(fit_10_allowlist)}`",
        "",
        "## Result",
        "",
        f"- Input rows: `{summary.input_rows}`",
        f"- Accepted trades: `{summary.accepted_trades}`",
        f"- Skipped rows: `{summary.skipped_rows}`",
        f"- Gross stake: `{summary.gross_stake_usdc:.4f} USDC`",
        f"- Net PnL after estimated fees: `{summary.net_pnl_usdc:.6f} USDC`",
        f"- Net EV / USDC: `{_fmt(summary.net_ev_per_usdc)}`",
        f"- Net EV / trade: `{_fmt(summary.net_ev_per_trade)}`",
        f"- Bootstrap 95% CI on net EV / USDC: `[{_fmt(summary.bootstrap_ci_95.get('lower'))}, {_fmt(summary.bootstrap_ci_95.get('upper'))}]`",
        f"- Gate: **{summary.go_gate}**",
        "",
        "## Skip / decision reasons",
        "",
    ]
    for reason, count in summary.reason_counts.items():
        lines.append(f"- `{reason}`: `{count}`")
    lines.extend(["", "## Top accepted trades by model edge", ""])
    for decision in sorted(accepted, key=lambda item: item.edge, reverse=True)[:25]:
        lines.append(
            f"- `{decision.candidate.probe_id}` `{decision.candidate.touch_time_iso}` market `{decision.candidate.market_id}`: "
            f"prob `{decision.candidate.model_prob:.4f}`, price `{decision.candidate.market_price:.4f}`, "
            f"edge `{decision.edge:.4f}`, stake `{decision.stake_usdc:.4f}`, pnl `{decision.net_pnl_usdc:.4f}`"
        )
    if summary.go_gate != "GO":
        lines.extend(
            [
                "",
                "## NO-GO note",
                "",
                "The held-out policy does not clear the required gate unless the bootstrap CI lower bound is above zero after fees, min-size handling, and the configured edge buffer. P6-2 should remain offline until a feature/data update clears this report.",
            ]
        )
    return "\n".join(lines) + "\n"


def _candidate_from_score_row(row: sqlite3.Row, touch: dict[str, Any], spot: dict[str, Any]) -> PolicyCandidate:
    return PolicyCandidate(
        probe_id=str(row["probe_id"]),
        market_id=str(row["market_id"] or ""),
        touch_time_iso=str(row["touch_time_iso"]),
        split=str(row["split"]),
        resolves_yes=int(row["resolves_yes"]),
        market_price=float(row["market_price"]),
        model_prob=float(row["model_prob"]),
        fee_rate_bps=float(row["fee_rate_bps"] or 0.0),
        would_be_cost_after_min_shares=_optional_float(row["would_be_cost_after_min_shares"]),
        survived_ms=_optional_float(row["survived_ms"]),
        asset_slug=str(row["asset_slug"] or touch.get("asset_slug") or "").lower(),
        direction=str(row["direction"] or touch.get("direction") or ""),
        lifecycle_fraction=_optional_float(touch.get("lifecycle_fraction")),
        spot_implied_prob=_optional_float(spot.get("spot_implied_prob")),
        spot_implied_prob_minus_market_prob=_optional_float(spot.get("spot_implied_prob_minus_market_prob")),
        spot_source=str(spot.get("source") or ""),
        fit_10_usdc=str(touch.get("fit_10_usdc") or "") or None,
        tick_size=_optional_float(touch.get("tick_size")),
    )


def _load_optional_table_by_probe_id(path: str | Path | None, table_name: str) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone() is None:
            raise ValueError(f"{path} does not contain table {table_name}")
        columns = _table_columns(conn, table_name)
        if "probe_id" not in columns:
            raise ValueError(f"{path}:{table_name} is missing probe_id")
        return {str(row["probe_id"]): dict(row) for row in conn.execute(f"SELECT * FROM {table_name}")}
    finally:
        conn.close()


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})")}


def effective_entry_price(candidate: PolicyCandidate, *, use_fit_10_entry_price: bool) -> float | None:
    if not use_fit_10_entry_price:
        return candidate.market_price
    fit = candidate.fit_10_usdc or ""
    if fit == "+0_tick":
        return candidate.market_price
    if fit == "+1_tick":
        tick = candidate.tick_size if candidate.tick_size is not None and candidate.tick_size > 0 else 0.01
        return min(0.99, candidate.market_price + tick)
    return None


def _edge_probability(candidate: PolicyCandidate, field: str) -> float | None:
    if field == "model_prob":
        return candidate.model_prob
    if field == "spot_implied_prob":
        return candidate.spot_implied_prob
    raise ValueError(f"unsupported edge probability field: {field}")


def _normalize_set(values: set[str] | None) -> set[str] | None:
    if values is None:
        return None
    normalized = {value.strip().lower() for value in values if value.strip()}
    return normalized or None


def _has_allowed_prefix(value: str, prefixes: set[str]) -> bool:
    text = value.strip().lower()
    return any(text.startswith(prefix) for prefix in prefixes)


def _parse_csv_set(value: str | None) -> set[str] | None:
    if value is None or value.strip() == "":
        return None
    return {part.strip() for part in value.split(",") if part.strip()}


def _fmt_set(value: set[str] | None) -> str:
    if value is None:
        return "n/a"
    return ",".join(sorted(value))


def _percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    pos = (len(values) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return values[int(pos)]
    return values[lo] * (hi - pos) + values[hi] * (pos - lo)


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate P6-2c held-out touch policy from model scores")
    parser.add_argument("--scores", default=str(DEFAULT_SCORES_PATH), help="SQLite scores DB from train_touch_model.py")
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH), help="Output markdown policy report")
    parser.add_argument("--touch-dataset", default=None, help="Optional touch_dataset SQLite DB for lifecycle/fit/tick filters")
    parser.add_argument("--spot-features", default=None, help="Optional spot_features SQLite DB for spot-gap/source filters")
    parser.add_argument("--split", default="test", help="Scores split to evaluate")
    parser.add_argument("--stake-usdc", type=float, default=10.0)
    parser.add_argument("--edge-buffer-bps", type=float, default=200.0)
    parser.add_argument("--min-size-policy", choices=["upscale", "skip"], default="upscale")
    parser.add_argument("--max-orders-per-market-per-run", type=int, default=1)
    parser.add_argument("--daily-loss-cap-usdc", type=float, default=30.0)
    parser.add_argument("--min-survived-ms", type=float, default=0.0)
    parser.add_argument("--asset-allowlist", default=None, help="Comma-separated allowed asset_slug values, e.g. btc,eth,sol,xrp")
    parser.add_argument("--min-lifecycle-fraction", type=float, default=None)
    parser.add_argument(
        "--min-spot-implied-prob-minus-market-prob",
        "--min-spot-gap",
        dest="min_spot_implied_prob_minus_market_prob",
        type=float,
        default=None,
    )
    parser.add_argument(
        "--spot-source-prefix-allowlist",
        default=None,
        help="Comma-separated allowed spot source prefixes, e.g. binance",
    )
    parser.add_argument("--direction-allowlist", default=None, help="Comma-separated allowed direction values, e.g. DOWN/NO")
    parser.add_argument("--fit-10-allowlist", default=None, help="Comma-separated allowed fit_10_usdc labels")
    parser.add_argument("--use-fit-10-entry-price", action="store_true", help="Use +0/+1 tick fit_10 slippage to adjust entry price")
    parser.add_argument(
        "--edge-probability-field",
        choices=["model_prob", "spot_implied_prob"],
        default="model_prob",
        help="Probability used for the fee+edge gate",
    )
    parser.add_argument("--bootstrap-samples", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()
    summary = evaluate_touch_policy_from_scores(
        args.scores,
        report_path=args.report,
        touch_dataset_path=args.touch_dataset,
        spot_features_path=args.spot_features,
        split=args.split,
        stake_usdc=args.stake_usdc,
        edge_buffer_bps=args.edge_buffer_bps,
        min_size_policy=args.min_size_policy,
        max_orders_per_market_per_run=args.max_orders_per_market_per_run,
        daily_loss_cap_usdc=args.daily_loss_cap_usdc,
        min_survived_ms=args.min_survived_ms,
        asset_allowlist=_parse_csv_set(args.asset_allowlist),
        min_lifecycle_fraction=args.min_lifecycle_fraction,
        min_spot_implied_prob_minus_market_prob=args.min_spot_implied_prob_minus_market_prob,
        direction_allowlist=_parse_csv_set(args.direction_allowlist),
        spot_source_prefix_allowlist=_parse_csv_set(args.spot_source_prefix_allowlist),
        fit_10_allowlist=_parse_csv_set(args.fit_10_allowlist),
        use_fit_10_entry_price=args.use_fit_10_entry_price,
        edge_probability_field=args.edge_probability_field,
        bootstrap_samples=args.bootstrap_samples,
        seed=args.seed,
    )
    print(json.dumps(summary.as_json(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
