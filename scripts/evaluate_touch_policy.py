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
    split: str = "test",
    stake_usdc: float = 10.0,
    edge_buffer_bps: float = 200.0,
    min_size_policy: str = "upscale",
    max_orders_per_market_per_run: int = 1,
    daily_loss_cap_usdc: float = 30.0,
    min_survived_ms: float = 0.0,
    bootstrap_samples: int = 1000,
    seed: int = DEFAULT_SEED,
) -> PolicySummary:
    candidates = load_candidates(scores_path, split=split)
    decisions = evaluate_candidates(
        candidates,
        stake_usdc=stake_usdc,
        edge_buffer_bps=edge_buffer_bps,
        min_size_policy=min_size_policy,
        max_orders_per_market_per_run=max_orders_per_market_per_run,
        daily_loss_cap_usdc=daily_loss_cap_usdc,
        min_survived_ms=min_survived_ms,
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
        ),
        encoding="utf-8",
    )
    return summary


def load_candidates(scores_path: str | Path, *, split: str = "test") -> list[PolicyCandidate]:
    conn = sqlite3.connect(scores_path)
    conn.row_factory = sqlite3.Row
    try:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='touch_model_scores'").fetchone() is None:
            raise ValueError(f"{scores_path} does not contain table touch_model_scores")
        rows = conn.execute(
            """
            SELECT probe_id, market_id, touch_time_iso, split, resolves_yes, market_price,
                   model_prob, fee_rate_bps, would_be_cost_after_min_shares, survived_ms
            FROM touch_model_scores
            WHERE split = ?
            ORDER BY event_time_ms, probe_id
            """,
            (split,),
        ).fetchall()
        return [
            PolicyCandidate(
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
            )
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
) -> list[PolicyDecision]:
    if min_size_policy not in {"upscale", "skip"}:
        raise ValueError("min_size_policy must be 'upscale' or 'skip'")
    decisions: list[PolicyDecision] = []
    orders_by_market: dict[str, int] = {}
    realized_by_day: dict[str, float] = {}
    for candidate in candidates:
        date_key = candidate.touch_time_iso[:10]
        edge = candidate.model_prob - candidate.market_price
        fee_gap = break_even_fee_gap(candidate.market_price, candidate.fee_rate_bps)
        required_gap = fee_gap + edge_buffer_bps / 10000.0
        min_cost = candidate.would_be_cost_after_min_shares or max(stake_usdc, candidate.market_price * 5.0)
        actual_stake = max(stake_usdc, min_cost) if min_size_policy == "upscale" else stake_usdc
        reason = "accepted"
        action = "accept"
        if edge < required_gap:
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
            net_pnl = realized_net_pnl(candidate, actual_stake)
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


def realized_net_pnl(candidate: PolicyCandidate, stake_usdc: float) -> float:
    if candidate.market_price <= 0:
        return 0.0
    shares = stake_usdc / candidate.market_price
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
    parser.add_argument("--split", default="test", help="Scores split to evaluate")
    parser.add_argument("--stake-usdc", type=float, default=10.0)
    parser.add_argument("--edge-buffer-bps", type=float, default=200.0)
    parser.add_argument("--min-size-policy", choices=["upscale", "skip"], default="upscale")
    parser.add_argument("--max-orders-per-market-per-run", type=int, default=1)
    parser.add_argument("--daily-loss-cap-usdc", type=float, default=30.0)
    parser.add_argument("--min-survived-ms", type=float, default=0.0)
    parser.add_argument("--bootstrap-samples", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()
    summary = evaluate_touch_policy_from_scores(
        args.scores,
        report_path=args.report,
        split=args.split,
        stake_usdc=args.stake_usdc,
        edge_buffer_bps=args.edge_buffer_bps,
        min_size_policy=args.min_size_policy,
        max_orders_per_market_per_run=args.max_orders_per_market_per_run,
        daily_loss_cap_usdc=args.daily_loss_cap_usdc,
        min_survived_ms=args.min_survived_ms,
        bootstrap_samples=args.bootstrap_samples,
        seed=args.seed,
    )
    print(json.dumps(summary.as_json(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
