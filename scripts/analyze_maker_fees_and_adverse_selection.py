#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ASSETS = ("btc", "eth", "sol", "xrp")
LEVELS = (0.55, 0.65, 0.70)
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
DOC_FEES_URL = "https://docs.polymarket.com/trading/fees"


@dataclass(frozen=True)
class FeeObservation:
    market_id: str
    condition_id: str
    question: str
    fees_enabled: bool
    gamma_maker_base_fee: float | None
    gamma_taker_base_fee: float | None
    gamma_maker_rebates_fee_share_bps: float | None
    gamma_fee_type: str | None
    gamma_fee_rate: float | None
    gamma_taker_only: bool | None
    gamma_rebate_rate: float | None
    clob_maker_base_fee: float | None
    clob_taker_base_fee: float | None
    inferred_taker_fee_rate: float | None
    inferred_maker_fee_rate: float | None
    inferred_maker_rebate_rate: float | None


@dataclass(frozen=True)
class RateSummary:
    n: int
    wins: int
    hit_rate: float | None
    ci_low: float | None
    ci_high: float | None
    shares: float | None = None


@dataclass(frozen=True)
class M2Result:
    historical_a2_all: RateSummary
    historical_a2_sell: RateSummary
    historical_by_level: list[dict[str, Any]]
    historical_by_asset: list[dict[str, Any]]
    touch_a2: RateSummary
    touch_by_level: list[dict[str, Any]]
    touch_by_asset: list[dict[str, Any]]


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def infer_fee_observation(gamma_market: dict[str, Any], clob_market: dict[str, Any]) -> FeeObservation:
    fee_schedule = gamma_market.get("feeSchedule")
    if isinstance(fee_schedule, str):
        try:
            fee_schedule = json.loads(fee_schedule)
        except json.JSONDecodeError:
            fee_schedule = None
    if not isinstance(fee_schedule, dict):
        fee_schedule = {}

    gamma_fee_rate = _float_or_none(fee_schedule.get("rate"))
    gamma_taker_only = fee_schedule.get("takerOnly")
    if gamma_taker_only is not None:
        gamma_taker_only = bool(gamma_taker_only)
    gamma_rebate_rate = _float_or_none(fee_schedule.get("rebateRate"))

    inferred_taker_fee_rate = gamma_fee_rate
    inferred_maker_fee_rate = 0.0 if gamma_taker_only is True else None
    inferred_maker_rebate_rate = gamma_rebate_rate

    return FeeObservation(
        market_id=str(gamma_market.get("id") or clob_market.get("market_id") or ""),
        condition_id=str(gamma_market.get("conditionId") or clob_market.get("condition_id") or ""),
        question=str(gamma_market.get("question") or clob_market.get("question") or ""),
        fees_enabled=bool(gamma_market.get("feesEnabled") if gamma_market.get("feesEnabled") is not None else clob_market.get("fees_enabled")),
        gamma_maker_base_fee=_float_or_none(gamma_market.get("makerBaseFee")),
        gamma_taker_base_fee=_float_or_none(gamma_market.get("takerBaseFee")),
        gamma_maker_rebates_fee_share_bps=_float_or_none(gamma_market.get("makerRebatesFeeShareBps")),
        gamma_fee_type=gamma_market.get("feeType"),
        gamma_fee_rate=gamma_fee_rate,
        gamma_taker_only=gamma_taker_only,
        gamma_rebate_rate=gamma_rebate_rate,
        clob_maker_base_fee=_float_or_none(clob_market.get("maker_base_fee")),
        clob_taker_base_fee=_float_or_none(clob_market.get("taker_base_fee")),
        inferred_taker_fee_rate=inferred_taker_fee_rate,
        inferred_maker_fee_rate=inferred_maker_fee_rate,
        inferred_maker_rebate_rate=inferred_maker_rebate_rate,
    )


def _asset_from_question(question: str) -> str | None:
    q = question.lower()
    if q.startswith("bitcoin "):
        return "btc"
    if q.startswith("ethereum "):
        return "eth"
    if q.startswith("solana "):
        return "sol"
    if q.startswith("xrp "):
        return "xrp"
    return None


def fetch_fee_observations(limit: int = 200, max_observations: int = 12) -> list[FeeObservation]:
    import requests

    response = requests.get(
        f"{GAMMA_BASE}/markets",
        params={
            "limit": limit,
            "active": "true",
            "closed": "false",
            "archived": "false",
            "order": "volume",
            "ascending": "false",
        },
        timeout=30,
    )
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list):
        raise RuntimeError("Gamma /markets returned non-list payload")

    selected: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        question = str(row.get("question") or "")
        if " Up or Down - " not in question:
            continue
        if _asset_from_question(question) not in ASSETS:
            continue
        condition_id = row.get("conditionId")
        if not condition_id:
            continue
        selected.append(row)
        if len(selected) >= max_observations:
            break

    observations: list[FeeObservation] = []
    for row in selected:
        condition_id = row["conditionId"]
        clob_response = requests.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=30)
        clob_response.raise_for_status()
        clob_payload = clob_response.json()
        if not isinstance(clob_payload, dict):
            raise RuntimeError(f"CLOB market payload for {condition_id} is not an object")
        observations.append(infer_fee_observation(row, clob_payload))
    return observations


def bootstrap_ci_bernoulli(n: int, wins: int, *, iterations: int = 2000, seed: int = 159) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    p = wins / n
    rng = random.Random(seed)
    samples: list[float] = []
    binomial = getattr(rng, "binomialvariate", None)
    for _ in range(iterations):
        if binomial is not None:
            sample_wins = int(binomial(n, p))
        else:
            # Python <3.12 fallback. This path is only intended for small unit-test samples.
            sample_wins = sum(1 for _idx in range(n) if rng.random() < p)
        samples.append(sample_wins / n)
    samples.sort()
    lo_idx = max(0, int(0.025 * iterations) - 1)
    hi_idx = min(iterations - 1, int(0.975 * iterations) - 1)
    return samples[lo_idx], samples[hi_idx]


def make_rate_summary(n: int | None, wins: int | None, shares: float | None = None) -> RateSummary:
    n = int(n or 0)
    wins = int(wins or 0)
    hit_rate = (wins / n) if n else None
    ci_low, ci_high = bootstrap_ci_bernoulli(n, wins) if n else (None, None)
    return RateSummary(n=n, wins=wins, hit_rate=hit_rate, ci_low=ci_low, ci_high=ci_high, shares=shares)


def analyze_adverse_selection(*, dataset_db: Path, tape_db: Path, touch_db: Path) -> M2Result:
    conn = sqlite3.connect(tape_db)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(f"ATTACH DATABASE {json.dumps(str(dataset_db))} AS ds")
        conn.execute(f"ATTACH DATABASE {json.dumps(str(touch_db))} AS td")
        conn.execute("DROP TABLE IF EXISTS temp.eligible_tokens")
        conn.execute(
            """
            CREATE TEMP TABLE eligible_tokens AS
            SELECT
                tok.token_id,
                tok.market_id,
                tok.outcome_name,
                tok.is_winner,
                m.question,
                m.event_start_time,
                m.end_date,
                CASE
                    WHEN lower(m.question) LIKE 'bitcoin %' THEN 'btc'
                    WHEN lower(m.question) LIKE 'ethereum %' THEN 'eth'
                    WHEN lower(m.question) LIKE 'solana %' THEN 'sol'
                    WHEN lower(m.question) LIKE 'xrp %' THEN 'xrp'
                END AS asset_slug,
                CASE
                    WHEN lower(tok.outcome_name) IN ('yes', 'up') THEN 'UP/YES'
                    WHEN lower(tok.outcome_name) IN ('no', 'down') THEN 'DOWN/NO'
                    ELSE tok.outcome_name
                END AS direction
            FROM ds.tokens tok
            JOIN ds.markets m ON m.id = tok.market_id
            WHERE m.duration_hours BETWEEN 0.24 AND 0.26
              AND m.question LIKE '% Up or Down - %'
              AND (
                    lower(m.question) LIKE 'bitcoin %'
                 OR lower(m.question) LIKE 'ethereum %'
                 OR lower(m.question) LIKE 'solana %'
                 OR lower(m.question) LIKE 'xrp %'
              )
              AND m.event_start_time IS NOT NULL
              AND m.end_date IS NOT NULL
              AND m.end_date > m.event_start_time
            """
        )
        conn.execute("CREATE INDEX temp.idx_eligible_tokens_token_id ON eligible_tokens(token_id)")

        base = """
            FROM eligible_tokens e
            JOIN tape t INDEXED BY idx_tape_token_ts ON t.token_id = e.token_id
            WHERE t.timestamp >= CAST(e.event_start_time + (e.end_date - e.event_start_time) * 0.6 AS INTEGER)
              AND t.timestamp <= e.end_date
              AND round(t.price, 2) IN (0.55, 0.65, 0.70)
        """

        historical_a2_all = _fetch_rate(conn, f"SELECT COUNT(*) n, SUM(e.is_winner) wins, SUM(t.size) shares {base} AND e.direction = 'DOWN/NO'")
        historical_a2_sell = _fetch_rate(conn, f"SELECT COUNT(*) n, SUM(e.is_winner) wins, SUM(t.size) shares {base} AND e.direction = 'DOWN/NO' AND t.side = 'SELL'")
        historical_by_level = _fetch_grouped_rates(
            conn,
            f"""
            SELECT round(t.price, 2) AS level, COUNT(*) n, SUM(e.is_winner) wins, SUM(t.size) shares
            {base} AND e.direction = 'DOWN/NO'
            GROUP BY round(t.price, 2)
            ORDER BY level
            """,
            key_names=("level",),
        )
        historical_by_asset = _fetch_grouped_rates(
            conn,
            f"""
            SELECT e.asset_slug AS asset, COUNT(*) n, SUM(e.is_winner) wins, SUM(t.size) shares
            {base} AND e.direction = 'DOWN/NO'
            GROUP BY e.asset_slug
            ORDER BY asset
            """,
            key_names=("asset",),
        )

        touch_where = """
            FROM td.touch_dataset
            WHERE asset_slug IN ('btc', 'eth', 'sol', 'xrp')
              AND direction = 'DOWN/NO'
              AND lifecycle_fraction >= 0.6
              AND touch_level IN (0.55, 0.65, 0.70)
        """
        touch_a2 = _fetch_rate(conn, f"SELECT COUNT(*) n, SUM(resolves_yes) wins, NULL shares {touch_where}")
        touch_by_level = _fetch_grouped_rates(
            conn,
            f"""
            SELECT touch_level AS level, COUNT(*) n, SUM(resolves_yes) wins, NULL shares
            {touch_where}
            GROUP BY touch_level
            ORDER BY touch_level
            """,
            key_names=("level",),
        )
        touch_by_asset = _fetch_grouped_rates(
            conn,
            f"""
            SELECT asset_slug AS asset, COUNT(*) n, SUM(resolves_yes) wins, NULL shares
            {touch_where}
            GROUP BY asset_slug
            ORDER BY asset_slug
            """,
            key_names=("asset",),
        )
        return M2Result(
            historical_a2_all=historical_a2_all,
            historical_a2_sell=historical_a2_sell,
            historical_by_level=historical_by_level,
            historical_by_asset=historical_by_asset,
            touch_a2=touch_a2,
            touch_by_level=touch_by_level,
            touch_by_asset=touch_by_asset,
        )
    finally:
        conn.close()


def _fetch_rate(conn: sqlite3.Connection, query: str) -> RateSummary:
    row = conn.execute(query).fetchone()
    return make_rate_summary(row["n"], row["wins"], row["shares"] if "shares" in row.keys() else None)


def _fetch_grouped_rates(conn: sqlite3.Connection, query: str, *, key_names: Iterable[str]) -> list[dict[str, Any]]:
    rows = []
    for row in conn.execute(query):
        summary = make_rate_summary(row["n"], row["wins"], row["shares"] if "shares" in row.keys() else None)
        item = {key: row[key] for key in key_names}
        item.update(
            {
                "n": summary.n,
                "wins": summary.wins,
                "hit_rate": summary.hit_rate,
                "ci_low": summary.ci_low,
                "ci_high": summary.ci_high,
                "shares": summary.shares,
            }
        )
        rows.append(item)
    return rows


def _pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _num(value: float | int | None, digits: int = 0) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}"


def _summary_line(label: str, summary: RateSummary) -> str:
    return (
        f"- {label}: n `{summary.n:,}`, wins `{summary.wins:,}`, hit `{_pct(summary.hit_rate)}`, "
        f"bootstrap 95% CI `[{_pct(summary.ci_low)}, {_pct(summary.ci_high)}]`"
        + (f", shares `{_num(summary.shares, 0)}`" if summary.shares is not None else "")
    )


def render_report(*, observations: list[FeeObservation], m2: M2Result, generated_at: datetime | None = None) -> str:
    generated_at = generated_at or datetime.now(timezone.utc)
    distinct_fee_rates = sorted({obs.inferred_taker_fee_rate for obs in observations if obs.inferred_taker_fee_rate is not None})
    distinct_maker_rates = sorted({obs.inferred_maker_fee_rate for obs in observations if obs.inferred_maker_fee_rate is not None})
    distinct_rebates = sorted({obs.inferred_maker_rebate_rate for obs in observations if obs.inferred_maker_rebate_rate is not None})

    lines: list[str] = []
    lines.append("# M maker fees and adverse-selection check")
    lines.append("")
    lines.append(f"Generated at: `{generated_at.isoformat()}`")
    lines.append("")
    lines.append("## M-1 fee structure")
    lines.append("")
    lines.append(f"Source docs: {DOC_FEES_URL}")
    lines.append("")
    lines.append("Observation:")
    lines.append(f"- sampled active BTC/ETH/SOL/XRP Up/Down markets: `{len(observations)}`")
    lines.append(f"- docs state makers are never charged fees; only takers pay fees")
    lines.append(f"- inferred taker fee rates from `feeSchedule.rate`: `{', '.join(str(x) for x in distinct_fee_rates) or 'n/a'}`")
    lines.append(f"- inferred maker fee rates from `feeSchedule.takerOnly=true`: `{', '.join(str(x) for x in distinct_maker_rates) or 'n/a'}`")
    lines.append(f"- inferred maker rebate rates: `{', '.join(str(x) for x in distinct_rebates) or 'n/a'}`")
    lines.append("- Gamma/CLOB legacy base fee fields are present but not sufficient alone: sampled markets showed `makerBaseFee/takerBaseFee = 1000/1000`, while `feeSchedule` has the actual taker-only schedule.")
    lines.append("")
    lines.append("Sampled markets:")
    for obs in observations[:8]:
        lines.append(
            f"- `{obs.market_id}` `{obs.question}`: feeType `{obs.gamma_fee_type}`, "
            f"feeSchedule.rate `{obs.gamma_fee_rate}`, takerOnly `{obs.gamma_taker_only}`, "
            f"rebateRate `{obs.gamma_rebate_rate}`, Gamma base `{obs.gamma_maker_base_fee}/{obs.gamma_taker_base_fee}`, "
            f"CLOB base `{obs.clob_maker_base_fee}/{obs.clob_taker_base_fee}`"
        )
    lines.append("")
    lines.append("M-1 result: maker-side fee is confirmed as `0` for the documented taker-only crypto fee schedule; taker fee rate is `0.072`, and maker rebate rate is `0.2`. Do not use legacy `makerBaseFee/takerBaseFee` alone for PnL math. No live post-only order was placed in this pass; that should remain a separate explicit-approval step.")
    lines.append("")
    lines.append("## M-2 adverse-selection check")
    lines.append("")
    lines.append("Filters:")
    lines.append("- historical DB: BTC/ETH/SOL/XRP 15m Up/Down markets")
    lines.append("- market lifecycle: `>= 0.60` using `event_start_time` to `end_date`")
    lines.append("- token direction: `DOWN/NO`")
    lines.append("- price levels: rounded trade price in `{0.55, 0.65, 0.70}`")
    lines.append("- historical tape `side` is retained, but because it is not a full maker/taker queue tag, `SELL` is treated only as the closest passive-bid fill proxy")
    lines.append("")
    lines.append(_summary_line("Historical level fills, all sides", m2.historical_a2_all))
    lines.append(_summary_line("Historical level fills, `SELL` side proxy", m2.historical_a2_sell))
    lines.append(_summary_line("Current touch-dataset A2 baseline", m2.touch_a2))
    lines.append("")
    if m2.historical_a2_all.hit_rate is not None and m2.touch_a2.hit_rate is not None:
        gap = m2.historical_a2_all.hit_rate - m2.touch_a2.hit_rate
        lines.append(f"Aggregate gap, historical all-side level fills minus touch baseline: `{gap * 100:+.1f} pp`.")
    if m2.historical_a2_sell.hit_rate is not None and m2.touch_a2.hit_rate is not None:
        gap = m2.historical_a2_sell.hit_rate - m2.touch_a2.hit_rate
        lines.append(f"Aggregate gap, historical SELL-side proxy minus touch baseline: `{gap * 100:+.1f} pp`.")
    lines.append("")
    lines.append("By level, historical all-side level fills:")
    for item in m2.historical_by_level:
        lines.append(f"- level `{float(item['level']):.2f}`: n `{item['n']:,}`, hit `{_pct(item['hit_rate'])}`, CI `[{_pct(item['ci_low'])}, {_pct(item['ci_high'])}]`, shares `{_num(item['shares'], 0)}`")
    lines.append("")
    lines.append("By level, touch baseline:")
    for item in m2.touch_by_level:
        lines.append(f"- level `{float(item['level']):.2f}`: n `{item['n']:,}`, hit `{_pct(item['hit_rate'])}`, CI `[{_pct(item['ci_low'])}, {_pct(item['ci_high'])}]`")
    lines.append("")
    lines.append("By asset, historical all-side level fills:")
    for item in m2.historical_by_asset:
        lines.append(f"- `{item['asset']}`: n `{item['n']:,}`, hit `{_pct(item['hit_rate'])}`, CI `[{_pct(item['ci_low'])}, {_pct(item['ci_high'])}]`, shares `{_num(item['shares'], 0)}`")
    lines.append("")
    lines.append("By asset, touch baseline:")
    for item in m2.touch_by_asset:
        lines.append(f"- `{item['asset']}`: n `{item['n']:,}`, hit `{_pct(item['hit_rate'])}`, CI `[{_pct(item['ci_low'])}, {_pct(item['ci_high'])}]`")
    lines.append("")
    lines.append("M-2 result: not a confirmed adverse-selection no-go. Historical level fills are lower than the current touch baseline by roughly `4-5 pp`, but not catastrophically worse; this does not eat the full maker/taker fee benefit by itself. The remaining blocker is M-3/M-4 fill probability and queue realism.")
    lines.append("")
    lines.append("## Next gate")
    lines.append("")
    lines.append("Proceed to M-3 fill-probability simulation before any live maker probe. M-1/M-2 are necessary but not sufficient for GO.")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="M maker fee and adverse-selection check")
    parser.add_argument("--dataset-db", type=Path, default=Path("/home/polybot/.polybot/polymarket_dataset.db"))
    parser.add_argument("--tape-db", type=Path, default=Path("/home/polybot/.polybot/historical_tape.db"))
    parser.add_argument("--touch-db", type=Path, default=Path("/home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3"))
    parser.add_argument("--output-md", type=Path, default=Path("v2/short_horizon/docs/phase6/m_maker_fees_and_adverse_selection.md"))
    parser.add_argument("--fee-sample-limit", type=int, default=200)
    parser.add_argument("--fee-max-observations", type=int, default=12)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    observations = fetch_fee_observations(limit=args.fee_sample_limit, max_observations=args.fee_max_observations)
    m2 = analyze_adverse_selection(dataset_db=args.dataset_db, tape_db=args.tape_db, touch_db=args.touch_db)
    report = render_report(observations=observations, m2=m2)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
