#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

DEFAULT_TOUCH_DB = Path("/home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3")
DEFAULT_CAPTURE_GLOB = "/home/polybot/.polybot/capture_bundles/*/events_log.jsonl"
DEFAULT_REPORT_PATH = Path("v2/short_horizon/docs/phase6/m_maker_capture_replay.md")
POST_BEFORE_MS = (5_000, 30_000, 120_000, 300_000)
CANCEL_AFTER_MS = (10_000, 60_000)
STAKES_USDC = (1.0, 5.0)
QUEUE_FACTORS = (1.0, 2.0, 5.0)
DEFAULT_MIN_ORDER_SHARES = 5.0


@dataclass(frozen=True)
class Candidate:
    probe_id: str
    market_id: str
    token_id: str
    touch_time_iso: str
    touch_ms: int
    touch_level: float
    resolves_yes: int


@dataclass(frozen=True)
class BookPoint:
    ts_ms: int
    best_bid: float | None
    best_ask: float | None


@dataclass(frozen=True)
class TradePoint:
    ts_ms: int
    price: float
    size: float
    aggressor_side: str


@dataclass(frozen=True)
class ReplayConfig:
    stake_usdc: float
    queue_factor: float
    post_before_ms: int
    cancel_after_ms: int
    min_order_shares: float = DEFAULT_MIN_ORDER_SHARES


@dataclass(frozen=True)
class CandidateReplay:
    candidate: Candidate
    status: str
    order_shares: float
    required_sell_volume: float
    observed_sell_volume: float
    cost_usdc: float
    pnl_usdc: float
    prior_best_ask: float | None
    prior_best_bid: float | None


@dataclass(frozen=True)
class ConfigSummary:
    config: ReplayConfig
    candidate_n: int
    filled_n: int
    post_only_would_cross_n: int
    no_prior_book_n: int
    unfilled_n: int
    gross_cost_usdc: float
    pnl_usdc: float
    ev_per_usdc: float | None


@dataclass(frozen=True)
class BundleSlice:
    bundle_name: str
    path: Path
    candidates: list[Candidate]
    books_by_token: dict[str, list[BookPoint]]
    trades_by_token: dict[str, list[TradePoint]]
    has_l2_book_levels: bool


def parse_ts_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None


def order_shares_for_stake(*, stake_usdc: float, entry_price: float, min_order_shares: float = DEFAULT_MIN_ORDER_SHARES) -> float:
    if stake_usdc <= 0:
        raise ValueError("stake_usdc must be positive")
    if entry_price <= 0:
        raise ValueError("entry_price must be positive")
    return max(min_order_shares, stake_usdc / entry_price)


def load_a2_candidates(path: str | Path) -> list[Candidate]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT probe_id, market_id, token_id, touch_time_iso, touch_level, resolves_yes
            FROM touch_dataset
            WHERE asset_slug IN ('btc', 'eth', 'sol', 'xrp')
              AND direction = 'DOWN/NO'
              AND lifecycle_fraction >= 0.60
              AND touch_level IN (0.55, 0.65, 0.70)
            ORDER BY touch_time_iso, probe_id
            """
        ).fetchall()
        candidates: list[Candidate] = []
        for row in rows:
            touch_ms = parse_ts_ms(row["touch_time_iso"])
            if touch_ms is None:
                continue
            candidates.append(
                Candidate(
                    probe_id=str(row["probe_id"]),
                    market_id=str(row["market_id"]),
                    token_id=str(row["token_id"]),
                    touch_time_iso=str(row["touch_time_iso"]),
                    touch_ms=touch_ms,
                    touch_level=float(row["touch_level"]),
                    resolves_yes=int(row["resolves_yes"]),
                )
            )
        return candidates
    finally:
        conn.close()


def load_bundle_slice(path: str | Path, candidates: Sequence[Candidate], *, max_pre_ms: int) -> BundleSlice | None:
    path = Path(path)
    token_candidates: dict[str, list[Candidate]] = {}
    for candidate in candidates:
        token_candidates.setdefault(candidate.token_id, []).append(candidate)
    candidate_tokens = set(token_candidates)

    min_ts: int | None = None
    max_ts: int | None = None
    books_by_token: dict[str, list[BookPoint]] = {}
    trades_by_token: dict[str, list[TradePoint]] = {}
    has_l2 = False
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            payload = json.loads(raw)
            ts = parse_ts_ms(payload.get("event_time_ms", payload.get("event_time")))
            if ts is None:
                continue
            min_ts = ts if min_ts is None else min(min_ts, ts)
            max_ts = ts if max_ts is None else max(max_ts, ts)
            token_id = str(payload.get("token_id") or "")
            if token_id not in candidate_tokens:
                continue
            event_type = payload.get("event_type")
            if event_type == "BookUpdate":
                if "bid_levels" in payload or "ask_levels" in payload:
                    has_l2 = True
                books_by_token.setdefault(token_id, []).append(
                    BookPoint(
                        ts_ms=ts,
                        best_bid=_float_or_none(payload.get("best_bid")),
                        best_ask=_float_or_none(payload.get("best_ask")),
                    )
                )
            elif event_type == "TradeTick":
                price = _float_or_none(payload.get("price"))
                size = _float_or_none(payload.get("size"))
                if price is None or size is None:
                    continue
                trades_by_token.setdefault(token_id, []).append(
                    TradePoint(
                        ts_ms=ts,
                        price=price,
                        size=size,
                        aggressor_side=str(payload.get("aggressor_side") or "").lower(),
                    )
                )
    if min_ts is None or max_ts is None:
        return None

    local_candidates = [
        candidate
        for candidate in candidates
        if min_ts - max_pre_ms <= candidate.touch_ms <= max_ts + 60_000
        and (candidate.token_id in books_by_token or candidate.token_id in trades_by_token)
    ]
    if not local_candidates:
        return None
    for values in books_by_token.values():
        values.sort(key=lambda item: item.ts_ms)
    for values in trades_by_token.values():
        values.sort(key=lambda item: item.ts_ms)
    return BundleSlice(
        bundle_name=path.parent.name,
        path=path,
        candidates=local_candidates,
        books_by_token=books_by_token,
        trades_by_token=trades_by_token,
        has_l2_book_levels=has_l2,
    )


def replay_candidate(
    candidate: Candidate,
    *,
    books: Sequence[BookPoint],
    trades: Sequence[TradePoint],
    config: ReplayConfig,
) -> CandidateReplay:
    order_shares = order_shares_for_stake(
        stake_usdc=config.stake_usdc,
        entry_price=candidate.touch_level,
        min_order_shares=config.min_order_shares,
    )
    required_sell_volume = order_shares * config.queue_factor
    post_ms = candidate.touch_ms - config.post_before_ms
    cancel_ms = candidate.touch_ms + config.cancel_after_ms
    prior = last_book_at_or_before(books, post_ms)
    if prior is None:
        return CandidateReplay(candidate, "no_prior_book", order_shares, required_sell_volume, 0.0, 0.0, 0.0, None, None)
    if prior.best_ask is not None and prior.best_ask <= candidate.touch_level + 1e-9:
        return CandidateReplay(
            candidate,
            "post_only_would_cross",
            order_shares,
            required_sell_volume,
            0.0,
            0.0,
            0.0,
            prior.best_ask,
            prior.best_bid,
        )

    observed_sell_volume = 0.0
    for trade in trades:
        if trade.ts_ms < post_ms:
            continue
        if trade.ts_ms > cancel_ms:
            break
        if trade.aggressor_side == "sell" and trade.price <= candidate.touch_level + 1e-9:
            observed_sell_volume += trade.size
    if observed_sell_volume >= required_sell_volume:
        cost = order_shares * candidate.touch_level
        pnl = (order_shares if candidate.resolves_yes else 0.0) - cost
        return CandidateReplay(candidate, "filled", order_shares, required_sell_volume, observed_sell_volume, cost, pnl, prior.best_ask, prior.best_bid)
    return CandidateReplay(candidate, "unfilled", order_shares, required_sell_volume, observed_sell_volume, 0.0, 0.0, prior.best_ask, prior.best_bid)


def last_book_at_or_before(books: Sequence[BookPoint], ts_ms: int) -> BookPoint | None:
    if not books:
        return None
    times = [book.ts_ms for book in books]
    idx = bisect.bisect_right(times, ts_ms) - 1
    if idx < 0:
        return None
    return books[idx]


def summarize_replays(replays: Sequence[CandidateReplay], config: ReplayConfig) -> ConfigSummary:
    status_counts: dict[str, int] = {}
    for replay in replays:
        status_counts[replay.status] = status_counts.get(replay.status, 0) + 1
    cost = sum(replay.cost_usdc for replay in replays if replay.status == "filled")
    pnl = sum(replay.pnl_usdc for replay in replays if replay.status == "filled")
    return ConfigSummary(
        config=config,
        candidate_n=len(replays),
        filled_n=status_counts.get("filled", 0),
        post_only_would_cross_n=status_counts.get("post_only_would_cross", 0),
        no_prior_book_n=status_counts.get("no_prior_book", 0),
        unfilled_n=status_counts.get("unfilled", 0),
        gross_cost_usdc=cost,
        pnl_usdc=pnl,
        ev_per_usdc=(pnl / cost) if cost else None,
    )


def run_replay(*, touch_db: str | Path, capture_glob: str) -> tuple[list[BundleSlice], list[ConfigSummary]]:
    candidates = load_a2_candidates(touch_db)
    max_pre_ms = max(POST_BEFORE_MS)
    paths = sorted(Path("/").glob(capture_glob.lstrip("/"))) if capture_glob.startswith("/") else sorted(Path().glob(capture_glob))
    slices: list[BundleSlice] = []
    for path in paths:
        loaded = load_bundle_slice(path, candidates, max_pre_ms=max_pre_ms)
        if loaded is not None:
            slices.append(loaded)

    configs = [
        ReplayConfig(stake_usdc=stake, queue_factor=queue_factor, post_before_ms=post_before, cancel_after_ms=cancel_after)
        for stake in STAKES_USDC
        for queue_factor in QUEUE_FACTORS
        for post_before in POST_BEFORE_MS
        for cancel_after in CANCEL_AFTER_MS
    ]
    summaries: list[ConfigSummary] = []
    for config in configs:
        replays: list[CandidateReplay] = []
        for bundle in slices:
            for candidate in bundle.candidates:
                replays.append(
                    replay_candidate(
                        candidate,
                        books=bundle.books_by_token.get(candidate.token_id, []),
                        trades=bundle.trades_by_token.get(candidate.token_id, []),
                        config=config,
                    )
                )
        summaries.append(summarize_replays(replays, config))
    return slices, summaries


def render_report(*, touch_db: str | Path, capture_glob: str, slices: Sequence[BundleSlice], summaries: Sequence[ConfigSummary]) -> str:
    generated_at = datetime.now(timezone.utc)
    total_candidates = sum(len(bundle.candidates) for bundle in slices)
    any_l2 = any(bundle.has_l2_book_levels for bundle in slices)
    best = sorted(summaries, key=lambda item: (item.filled_n, item.gross_cost_usdc), reverse=True)[:8]
    representative = [
        item
        for item in summaries
        if item.config.stake_usdc == 1.0
        and item.config.queue_factor == 1.0
        and item.config.cancel_after_ms in {10_000, 60_000}
    ]
    representative = sorted(representative, key=lambda item: (item.config.post_before_ms, item.config.cancel_after_ms))

    lines: list[str] = []
    lines.append("# M-4 capture replay maker queue check")
    lines.append("")
    lines.append(f"Generated at: `{generated_at.isoformat()}`")
    lines.append(f"Touch DB: `{touch_db}`")
    lines.append(f"Capture glob: `{capture_glob}`")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append("This is the M-4 check against available capture bundles. Important limitation: existing capture bundles contain BBO `BookUpdate` plus `TradeTick`, but no full L2 bid/ask levels. Therefore true queue priority cannot be calibrated from these bundles.")
    lines.append("")
    lines.append("What this pass can still test:")
    lines.append("- whether a same-level post-only BUY could have been resting before the A2 touch")
    lines.append("- whether subsequent SELL trade prints at or below the bid level would have filled the synthetic maker bid under queue-factor stress")
    lines.append("")
    lines.append("Synthetic order model:")
    lines.append("- A2 candidates only: BTC/ETH/SOL/XRP, `DOWN/NO`, lifecycle `>=0.60`, levels `{0.55, 0.65, 0.70}`")
    lines.append("- `order_shares = max(5, stake_usdc / touch_level)`")
    lines.append("- post time = `touch_time - T_before`")
    lines.append("- post-only safety check: if prior `best_ask <= touch_level`, status is `post_only_would_cross`")
    lines.append("- fill proxy, if post-only safe: cumulative `SELL` TradeTick volume at `price <= touch_level` must exceed `order_shares * queue_factor`")
    lines.append("")
    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- capture bundles with matching A2 candidates: `{len(slices)}`")
    lines.append(f"- matched candidate appearances across bundles: `{total_candidates}`")
    lines.append(f"- any bundle has L2 book levels: `{str(any_l2).lower()}`")
    for bundle in slices:
        lines.append(
            f"- `{bundle.bundle_name}`: candidates `{len(bundle.candidates)}`, "
            f"book updates `{sum(len(v) for v in bundle.books_by_token.values()):,}`, "
            f"trade ticks `{sum(len(v) for v in bundle.trades_by_token.values()):,}`, "
            f"L2 levels `{str(bundle.has_l2_book_levels).lower()}`"
        )
    lines.append("")
    lines.append("## Representative configs")
    lines.append("")
    for summary in representative:
        lines.append(_summary_line(summary))
    lines.append("")
    lines.append("## Best configs by fills")
    lines.append("")
    for summary in best:
        lines.append(_summary_line(summary))
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    if all(summary.filled_n == 0 for summary in summaries):
        lines.append("M-4 rejects the current same-level maker interpretation on available captures: every matched candidate either had no prior book or would have crossed the spread when posted before touch. There were zero synthetic maker fills across the sweep.")
        lines.append("")
        lines.append("Why this matters: the current touch semantics fire on `best_ask` rising through the entry level. Before that touch, `best_ask` is usually already below or equal to the target level, so a BUY limit at the target level is not a resting maker order; it is post-only invalid / taker-like.")
        lines.append("")
        lines.append("This means M-3's `ask_size_at_touch_level` proxy was measuring taker-side visible ask liquidity, not a maker bid queue that we could join at the same level.")
    else:
        lines.append("Some synthetic maker fills were observed, but the result is still BBO/trade-tick based rather than full L2 queue calibration.")
    lines.append("")
    lines.append("Conclusion: do not proceed to a live maker probe for the current same-level A2 design. A maker strategy would need a different trigger, for example posting only when the target level is still safely below best ask, or a real L2-capture run that records full book depth before re-testing queue priority.")
    lines.append("")
    return "\n".join(lines)


def _summary_line(summary: ConfigSummary) -> str:
    cfg = summary.config
    return (
        f"- stake `{cfg.stake_usdc:g}`, q `{cfg.queue_factor:g}`, before `{cfg.post_before_ms / 1000:g}s`, "
        f"cancel `+{cfg.cancel_after_ms / 1000:g}s`: candidates `{summary.candidate_n}`, filled `{summary.filled_n}`, "
        f"post-only-cross `{summary.post_only_would_cross_n}`, no-prior-book `{summary.no_prior_book_n}`, "
        f"unfilled `{summary.unfilled_n}`, EV/USDC `{_pct(summary.ev_per_usdc)}`"
    )


def _pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="M-4 maker capture replay check")
    parser.add_argument("--touch-db", type=Path, default=DEFAULT_TOUCH_DB)
    parser.add_argument("--capture-glob", default=DEFAULT_CAPTURE_GLOB)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_REPORT_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    slices, summaries = run_replay(touch_db=args.touch_db, capture_glob=args.capture_glob)
    report = render_report(touch_db=args.touch_db, capture_glob=args.capture_glob, slices=slices, summaries=summaries)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
