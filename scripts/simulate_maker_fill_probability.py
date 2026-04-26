#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

DEFAULT_TOUCH_DB = Path("/home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3")
DEFAULT_REPORT_PATH = Path("v2/short_horizon/docs/phase6/m_maker_fill_sim.md")
ASSET_ALLOWLIST = {"btc", "eth", "sol", "xrp"}
LEVELS = {0.55, 0.65, 0.70}
STAKES_USDC = (1.0, 5.0, 10.0, 25.0)
QUEUE_FACTORS = (1.0, 2.0, 5.0)
MIN_SURVIVED_MS = (0.0, 100.0, 500.0, 2000.0)
DEFAULT_MIN_ORDER_SHARES = 5.0
DEFAULT_MAKER_FEE_RATE = 0.0
DEFAULT_SEED = 15903


@dataclass(frozen=True)
class TouchRow:
    probe_id: str
    market_id: str
    touch_time_iso: str
    asset_slug: str
    direction: str
    touch_level: float
    lifecycle_fraction: float
    ask_size_at_touch_level: float | None
    survived_ms: float | None
    resolves_yes: int


@dataclass(frozen=True)
class SimConfig:
    universe: str
    stake_usdc: float
    queue_factor: float
    min_survived_ms: float
    min_order_shares: float = DEFAULT_MIN_ORDER_SHARES
    maker_fee_rate: float = DEFAULT_MAKER_FEE_RATE


@dataclass(frozen=True)
class SimFill:
    row: TouchRow
    order_shares: float
    order_cost: float
    net_pnl: float
    accepted: bool


@dataclass(frozen=True)
class SimSummary:
    universe: str
    candidate_n: int
    accepted_n: int
    fill_rate: float | None
    stake_usdc: float
    queue_factor: float
    min_survived_ms: float
    gross_cost_usdc: float
    net_pnl_usdc: float
    net_ev_per_usdc: float | None
    bootstrap_ci_95: tuple[float | None, float | None]
    per_day_rows: list[dict[str, float | int | str | None]]


def order_shares_for_stake(*, stake_usdc: float, entry_price: float, min_order_shares: float = DEFAULT_MIN_ORDER_SHARES) -> float:
    if stake_usdc <= 0:
        raise ValueError("stake_usdc must be positive")
    if entry_price <= 0:
        raise ValueError("entry_price must be positive")
    if min_order_shares <= 0:
        raise ValueError("min_order_shares must be positive")
    return max(min_order_shares, stake_usdc / entry_price)


def maker_fill_decision(
    row: TouchRow,
    *,
    stake_usdc: float,
    queue_factor: float,
    min_survived_ms: float,
    min_order_shares: float = DEFAULT_MIN_ORDER_SHARES,
    maker_fee_rate: float = DEFAULT_MAKER_FEE_RATE,
) -> SimFill:
    order_shares = order_shares_for_stake(
        stake_usdc=stake_usdc,
        entry_price=row.touch_level,
        min_order_shares=min_order_shares,
    )
    order_cost = order_shares * row.touch_level
    enough_size = (row.ask_size_at_touch_level or 0.0) >= order_shares * queue_factor
    enough_time = (row.survived_ms or 0.0) >= min_survived_ms
    accepted = bool(enough_size and enough_time)
    payout = order_shares if row.resolves_yes else 0.0
    fee = order_cost * maker_fee_rate
    net_pnl = payout - order_cost - fee if accepted else 0.0
    return SimFill(row=row, order_shares=order_shares, order_cost=order_cost if accepted else 0.0, net_pnl=net_pnl, accepted=accepted)


def load_touch_rows(path: str | Path, *, universe: str) -> list[TouchRow]:
    if universe not in {"a2_down_no", "hres_baseline"}:
        raise ValueError("universe must be a2_down_no or hres_baseline")
    direction_filter = "AND direction = 'DOWN/NO'" if universe == "a2_down_no" else ""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='touch_dataset'").fetchone() is None:
            raise ValueError(f"{path} does not contain touch_dataset")
        rows = conn.execute(
            f"""
            SELECT probe_id, market_id, touch_time_iso, asset_slug, direction, touch_level,
                   lifecycle_fraction, ask_size_at_touch_level, survived_ms, resolves_yes
            FROM touch_dataset
            WHERE asset_slug IN ('btc', 'eth', 'sol', 'xrp')
              AND lifecycle_fraction >= 0.60
              AND touch_level IN (0.55, 0.65, 0.70)
              {direction_filter}
            ORDER BY touch_time_iso, probe_id
            """
        ).fetchall()
        return [
            TouchRow(
                probe_id=str(row["probe_id"]),
                market_id=str(row["market_id"]),
                touch_time_iso=str(row["touch_time_iso"]),
                asset_slug=str(row["asset_slug"]),
                direction=str(row["direction"]),
                touch_level=float(row["touch_level"]),
                lifecycle_fraction=float(row["lifecycle_fraction"]),
                ask_size_at_touch_level=_float_or_none(row["ask_size_at_touch_level"]),
                survived_ms=_float_or_none(row["survived_ms"]),
                resolves_yes=int(row["resolves_yes"]),
            )
            for row in rows
        ]
    finally:
        conn.close()


def simulate_config(rows: Sequence[TouchRow], config: SimConfig, *, seed: int = DEFAULT_SEED) -> SimSummary:
    fills = [
        maker_fill_decision(
            row,
            stake_usdc=config.stake_usdc,
            queue_factor=config.queue_factor,
            min_survived_ms=config.min_survived_ms,
            min_order_shares=config.min_order_shares,
            maker_fee_rate=config.maker_fee_rate,
        )
        for row in rows
    ]
    accepted = [fill for fill in fills if fill.accepted]
    gross_cost = sum(fill.order_cost for fill in accepted)
    net_pnl = sum(fill.net_pnl for fill in accepted)
    ev = net_pnl / gross_cost if gross_cost else None
    ci = bootstrap_ev_ci(accepted, seed=seed)
    return SimSummary(
        universe=config.universe,
        candidate_n=len(rows),
        accepted_n=len(accepted),
        fill_rate=(len(accepted) / len(rows)) if rows else None,
        stake_usdc=config.stake_usdc,
        queue_factor=config.queue_factor,
        min_survived_ms=config.min_survived_ms,
        gross_cost_usdc=gross_cost,
        net_pnl_usdc=net_pnl,
        net_ev_per_usdc=ev,
        bootstrap_ci_95=ci,
        per_day_rows=per_day_summary(accepted),
    )


def per_day_summary(fills: Sequence[SimFill]) -> list[dict[str, float | int | str | None]]:
    by_day: dict[str, list[SimFill]] = {}
    for fill in fills:
        day = fill.row.touch_time_iso[:10]
        by_day.setdefault(day, []).append(fill)
    rows: list[dict[str, float | int | str | None]] = []
    for day, items in sorted(by_day.items()):
        cost = sum(item.order_cost for item in items)
        pnl = sum(item.net_pnl for item in items)
        rows.append(
            {
                "day": day,
                "accepted_n": len(items),
                "gross_cost_usdc": cost,
                "net_pnl_usdc": pnl,
                "net_ev_per_usdc": (pnl / cost) if cost else None,
            }
        )
    return rows


def bootstrap_ev_ci(fills: Sequence[SimFill], *, samples: int = 2000, seed: int = DEFAULT_SEED) -> tuple[float | None, float | None]:
    if not fills:
        return None, None
    rng = random.Random(seed)
    values: list[float] = []
    n = len(fills)
    for _ in range(samples):
        cost = 0.0
        pnl = 0.0
        for _idx in range(n):
            item = fills[rng.randrange(n)]
            cost += item.order_cost
            pnl += item.net_pnl
        values.append(pnl / cost if cost else 0.0)
    values.sort()
    return values[max(0, int(0.025 * samples) - 1)], values[min(samples - 1, int(0.975 * samples) - 1)]


def run_sweep(touch_db: str | Path, *, seed: int = DEFAULT_SEED) -> list[SimSummary]:
    summaries: list[SimSummary] = []
    rows_by_universe = {
        universe: load_touch_rows(touch_db, universe=universe)
        for universe in ("a2_down_no", "hres_baseline")
    }
    for universe, rows in rows_by_universe.items():
        for stake_usdc in STAKES_USDC:
            for queue_factor in QUEUE_FACTORS:
                for min_survived_ms in MIN_SURVIVED_MS:
                    summaries.append(
                        simulate_config(
                            rows,
                            SimConfig(
                                universe=universe,
                                stake_usdc=stake_usdc,
                                queue_factor=queue_factor,
                                min_survived_ms=min_survived_ms,
                            ),
                            seed=seed,
                        )
                    )
    return summaries


def render_report(summaries: Sequence[SimSummary], *, touch_db: str | Path, generated_at: datetime | None = None) -> str:
    generated_at = generated_at or datetime.now(timezone.utc)
    a2 = [summary for summary in summaries if summary.universe == "a2_down_no"]
    baseline = [summary for summary in summaries if summary.universe == "hres_baseline"]
    a2_pass = [summary for summary in a2 if summary.accepted_n >= 50 and (summary.bootstrap_ci_95[0] is not None and summary.bootstrap_ci_95[0] > 0)]
    baseline_pass = [summary for summary in baseline if summary.accepted_n >= 50 and (summary.bootstrap_ci_95[0] is not None and summary.bootstrap_ci_95[0] > 0)]
    best_a2 = sorted(a2, key=_summary_sort_key, reverse=True)[:8]
    best_baseline = sorted(baseline, key=_summary_sort_key, reverse=True)[:8]

    lines: list[str] = []
    lines.append("# M-3 maker fill-probability simulation")
    lines.append("")
    lines.append(f"Generated at: `{generated_at.isoformat()}`")
    lines.append(f"Touch DB: `{touch_db}`")
    lines.append("")
    lines.append("## Model")
    lines.append("")
    lines.append("This is a coarse offline proxy, not an L2 queue replay.")
    lines.append("")
    lines.append("Filters/universes:")
    lines.append("- A2: BTC/ETH/SOL/XRP, `DOWN/NO`, `lifecycle_fraction >= 0.60`, levels `{0.55, 0.65, 0.70}`")
    lines.append("- HRES baseline: same assets/lifecycle/levels, both directions")
    lines.append("")
    lines.append("Fill model:")
    lines.append("- `order_shares = max(5, stake_usdc / touch_level)`")
    lines.append("- `fill = ask_size_at_touch_level >= order_shares * queue_factor AND survived_ms >= T_min`")
    lines.append("- maker fee = `0`, from M-1")
    lines.append("- realized PnL uses resolved token outcome at touch level")
    lines.append("")
    lines.append("Sweeps:")
    lines.append("- `stake_usdc in {1, 5, 10, 25}`")
    lines.append("- `queue_factor in {1, 2, 5}`")
    lines.append("- `T_min in {0, 100, 500, 2000} ms`")
    lines.append("")
    lines.append("## Gate result")
    lines.append("")
    lines.append(f"- A2 configs passing `accepted_n >= 50` and CI lower `> 0`: `{len(a2_pass)}` / `{len(a2)}`")
    lines.append(f"- HRES configs passing same gate: `{len(baseline_pass)}` / `{len(baseline)}`")
    if a2_pass:
        rec = sorted(a2_pass, key=lambda s: (s.queue_factor, s.min_survived_ms, -s.accepted_n, -float(s.net_ev_per_usdc or 0)))[0]
        lines.append(_summary_bullet("Recommended least-heroic A2 pass", rec))
    else:
        lines.append("- A2 has no passing config under this coarse model.")
    lines.append("")
    lines.append("## Top A2 configs")
    lines.append("")
    for summary in best_a2:
        lines.append(_summary_bullet("A2", summary))
    lines.append("")
    lines.append("## A2 robustness checkpoints")
    lines.append("")
    for stake_usdc, queue_factor, min_survived_ms in (
        (1.0, 2.0, 500.0),
        (1.0, 2.0, 2000.0),
        (1.0, 5.0, 500.0),
        (1.0, 5.0, 2000.0),
        (5.0, 2.0, 500.0),
        (5.0, 5.0, 500.0),
    ):
        summary = _find_summary(
            a2,
            stake_usdc=stake_usdc,
            queue_factor=queue_factor,
            min_survived_ms=min_survived_ms,
        )
        if summary is not None:
            lines.append(_summary_bullet("A2", summary))
    lines.append("")
    lines.append("## Top HRES baseline configs")
    lines.append("")
    for summary in best_baseline:
        lines.append(_summary_bullet("HRES", summary))
    lines.append("")
    lines.append("## Per-day check for recommended A2 config")
    lines.append("")
    if a2_pass:
        rec = sorted(a2_pass, key=lambda s: (s.queue_factor, s.min_survived_ms, -s.accepted_n, -float(s.net_ev_per_usdc or 0)))[0]
        for row in rec.per_day_rows:
            lines.append(
                f"- `{row['day']}`: accepted `{row['accepted_n']}`, cost `{_num(row['gross_cost_usdc'], 2)}`, "
                f"PnL `{_num(row['net_pnl_usdc'], 2)}`, EV/USDC `{_pct(row['net_ev_per_usdc'])}`"
            )
    else:
        lines.append("- n/a")
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    if a2_pass:
        lines.append("M-3 passes the coarse offline gate for A2: at least one realistic-looking config retains enough fills and positive bootstrap lower bound.")
        lines.append("This is still not a live GO. The model uses `ask_size_at_touch_level` as a proxy for fillable seller flow and does not know true queue priority. M-4 L2 replay is required before any live maker probe.")
    else:
        lines.append("M-3 fails the coarse offline gate for A2; maker mode should not proceed without changing assumptions or calibrating with L2 replay.")
    lines.append("")
    return "\n".join(lines)


def _summary_sort_key(summary: SimSummary) -> tuple[float, int, float]:
    ci_low = summary.bootstrap_ci_95[0]
    return (float(ci_low if ci_low is not None else -999), summary.accepted_n, float(summary.net_ev_per_usdc or -999))


def _find_summary(
    summaries: Sequence[SimSummary],
    *,
    stake_usdc: float,
    queue_factor: float,
    min_survived_ms: float,
) -> SimSummary | None:
    for summary in summaries:
        if (
            summary.stake_usdc == stake_usdc
            and summary.queue_factor == queue_factor
            and summary.min_survived_ms == min_survived_ms
        ):
            return summary
    return None


def _summary_bullet(label: str, summary: SimSummary) -> str:
    ci_low, ci_high = summary.bootstrap_ci_95
    return (
        f"- {label}: stake `{summary.stake_usdc:g}`, q `{summary.queue_factor:g}`, T `{summary.min_survived_ms:g}ms`, "
        f"accepted `{summary.accepted_n:,}` / `{summary.candidate_n:,}` (`{_pct(summary.fill_rate)}`), "
        f"cost `{_num(summary.gross_cost_usdc, 2)}`, PnL `{_num(summary.net_pnl_usdc, 2)}`, "
        f"EV/USDC `{_pct(summary.net_ev_per_usdc)}`, CI `[{_pct(ci_low)}, {_pct(ci_high)}]`"
    )


def _pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _num(value: float | int | None, digits: int = 0) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}"


def _float_or_none(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="M-3 maker fill probability simulation")
    parser.add_argument("--touch-db", type=Path, default=DEFAULT_TOUCH_DB)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summaries = run_sweep(args.touch_db, seed=args.seed)
    report = render_report(summaries, touch_db=args.touch_db)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
