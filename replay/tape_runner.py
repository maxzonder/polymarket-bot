from __future__ import annotations

import sqlite3
import time
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import execution.order_manager as om_module
import strategy.screener as screener_module

from api.clob_client import ClobClient
from config import BotConfig
from execution.order_manager import OrderManager
from execution.position_monitor import PositionMonitor
from replay.offline_dryrun import OfflineDryRunState, load_all_markets
from replay.tape_feed import (
    DEFAULT_TAPE_DB_PATH,
    has_valid_tape_db,
    iter_tape_batches,
    iter_tape_batches_db,
)
from strategy.market_scorer import MarketScorer
from strategy.risk_manager import RiskManager
from strategy.scorer import EntryFillScorer, ResolutionScorer
from strategy.screener import Screener
from utils.logger import setup_logger
from utils.paths import DB_PATH, DATA_DIR

logger = setup_logger("tape_runner")


def _write_runtime_snapshot(
    output_dir: Path,
    *,
    start: date,
    end: date,
    mode: str,
    limit_markets: Optional[int],
    batch_seconds: int,
    tape_db_path: Optional[Path],
    config: BotConfig,
) -> None:
    import json

    snapshot = {
        "run": {
            "kind": "tape_dryrun",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "mode": mode,
            "limit_markets": limit_markets,
            "batch_seconds": batch_seconds,
            "tape_db_path": str(tape_db_path) if tape_db_path is not None else None,
            "output_dir": str(output_dir),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        },
        "bot_config": {**asdict(config), "mode_config": asdict(config.mode_config)},
    }
    (output_dir / "config_snapshot.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


@dataclass
class TapeRunnerStats:
    batches: int = 0
    empty_batches: int = 0
    trades: int = 0
    screener_cycles: int = 0
    screener_skipped: int = 0
    candidates_seen: int = 0
    orders_placed: int = 0
    resolutions: int = 0


class TapeDrivenDryRunRunner:
    def __init__(
        self,
        *,
        start: date,
        end: date,
        mode: str,
        output_dir: Path,
        limit_markets: Optional[int] = None,
        batch_seconds: int = 300,
        tape_db_path: Optional[Path] = None,
    ):
        self.start = start
        self.end = end
        self.mode = mode
        self.output_dir = output_dir
        self.limit_markets = limit_markets
        self.batch_seconds = batch_seconds
        self.tape_db_path = tape_db_path if tape_db_path is not None else DEFAULT_TAPE_DB_PATH

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.positions_db = self.output_dir / "positions.db"
        self.paper_db = self.output_dir / "paper_trades.db"
        om_module.POSITIONS_DB = self.positions_db

        self.config = BotConfig(mode=mode, dry_run=True)
        self.mc = self.config.mode_config
        _write_runtime_snapshot(
            self.output_dir,
            start=start,
            end=end,
            mode=mode,
            limit_markets=limit_markets,
            batch_seconds=batch_seconds,
            tape_db_path=self.tape_db_path,
            config=self.config,
        )
        self.start_ts = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
        self.end_ts = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = load_all_markets(conn, self.start_ts, self.end_ts, self.config)
        conn.close()

        if limit_markets is not None:
            grouped: dict[str, list[dict]] = {}
            for row in rows:
                grouped.setdefault(str(row["market_id"]), []).append(row)
            rows = []
            for market_id in list(grouped.keys())[:limit_markets]:
                rows.extend(grouped[market_id])

        self.state = OfflineDryRunState.from_rows(rows, max_cohort_size=self.mc.max_cohort_size)
        self.market_ids = set(self.state.markets.keys())
        self.token_ids = set(self.state.tokens.keys())

        self.clob = ClobClient(private_key="tape_dummy", dry_run=True, paper_db_path=self.paper_db)
        risk = RiskManager(self.mc, balance_usdc=self.config.paper_initial_balance_usdc)
        self.order_manager = OrderManager(self.config, self.clob, risk, disable_scan_log=True)
        self.monitor = PositionMonitor(self.config, self.clob, self.order_manager)

        ef_price_max = max(self.mc.entry_price_levels) if self.mc.entry_price_levels else self.config.scorer_entry_price_max
        self.ef_scorer = EntryFillScorer(entry_price_max=ef_price_max, min_samples=self.config.scorer_min_samples)
        self.res_scorer = ResolutionScorer(min_samples=self.config.scorer_min_samples)
        self.market_scorer: Optional[MarketScorer] = None
        if self.mc.min_market_score > 0 or self.mc.market_score_tiers:
            self.market_scorer = MarketScorer(db_path=DB_PATH, min_score=self.mc.min_market_score)
        self.screener = Screener(
            config=self.config,
            entry_fill_scorer=self.ef_scorer,
            resolution_scorer=self.res_scorer,
            db_path=self.positions_db,
            market_scorer=self.market_scorer,
            skip_logging=True,
        )

        self.stats = TapeRunnerStats()
        self._resolved_market_ids: set[str] = set()
        # Sorted list of (end_date_ts, market_id) for O(log n) expiry scan.
        self._expiry_events: list[tuple[int, str]] = sorted(
            (m.end_date_ts, mid)
            for mid, m in self.state.markets.items()
            if m.end_date_ts is not None
        )
        self._expiry_idx: int = 0

    def _resolve_expired_markets(self, now_ts: int) -> int:
        """
        Resolve markets whose end_date_ts has passed using dataset is_winner labels.
        Uses a pre-sorted expiry event list for O(expired_count) work per call
        instead of O(total_markets) — critical with 23k+ market universes.
        Returns count of token resolutions triggered.
        """
        resolved = 0
        while (
            self._expiry_idx < len(self._expiry_events)
            and self._expiry_events[self._expiry_idx][0] <= now_ts
        ):
            end_ts, market_id = self._expiry_events[self._expiry_idx]
            self._expiry_idx += 1
            if market_id in self._resolved_market_ids:
                continue
            self._resolved_market_ids.add(market_id)
            market = self.state.markets.get(market_id)
            if market is None:
                continue
            for token_id in market.token_ids:
                snap = self.state.tokens.get(token_id)
                if snap is None:
                    continue
                is_winner = snap.is_winner or False
                self.order_manager.on_market_resolved(token_id, is_winner=is_winner)
                resolved += 1
        return resolved

    def _patched_runtime(self, ts: int):
        _noop = lambda *a, **kw: None
        stack = ExitStack()
        stack.enter_context(patch("time.time", return_value=float(ts)))
        stack.enter_context(patch.object(screener_module, "fetch_open_markets", side_effect=self.state.fetch_open_markets))
        stack.enter_context(patch.object(screener_module, "get_last_trade_ts", side_effect=self.state.get_last_trade_ts))
        stack.enter_context(patch.object(screener_module, "get_recent_trades", side_effect=self.state.get_recent_trades))
        stack.enter_context(patch("execution.order_manager.get_orderbook", side_effect=self.state.get_orderbook))
        stack.enter_context(patch("execution.position_monitor.get_orderbook", side_effect=self.state.get_orderbook))
        stack.enter_context(patch("strategy.screener.get_orderbook", side_effect=self.state.get_orderbook))
        stack.enter_context(patch.object(screener_module, "tg_alert", _noop))
        stack.enter_context(patch.object(om_module, "send_message", _noop))
        return stack

    def run(self) -> dict:
        t0 = time.monotonic()
        use_tape_db = has_valid_tape_db(self.tape_db_path)
        if self.tape_db_path.exists() and not use_tape_db:
            logger.warning(
                f"Tape DB path exists but is missing required schema, falling back to JSON tape: {self.tape_db_path}"
            )
        logger.info(
            f"Tape-driven dryrun: {self.start} -> {self.end} | mode={self.mode} | "
            f"markets={len(self.market_ids)} tokens={len(self.token_ids)} batch_seconds={self.batch_seconds} "
            f"source={'sqlite' if use_tape_db else 'json'}"
        )

        batch_iter = (
            iter_tape_batches_db(
                batch_seconds=self.batch_seconds,
                start_ts=self.start_ts,
                end_ts=self.end_ts,
                tape_db_path=self.tape_db_path,
                selected_markets=self.market_ids,
                selected_tokens=self.token_ids,
            )
            if use_tape_db
            else iter_tape_batches(
                batch_seconds=self.batch_seconds,
                start_ts=self.start_ts,
                end_ts=self.end_ts,
                selected_markets=self.market_ids,
                selected_tokens=self.token_ids,
            )
        )

        screener_blocked = False

        for batch in batch_iter:
            self.state.apply_batch(batch)
            self.stats.batches += 1
            self.stats.trades += len(batch.trades)

            if not batch.trades:
                self.stats.empty_batches += 1
                continue

            with self._patched_runtime(batch.batch_end_ts):
                # 1. Fills first: TP fills credit balance before screener sees it.
                had_fills = self.monitor._check_fills_dry_run(dirty_tokens=self.state.dirty_tokens)
                self.monitor._update_peak_prices(dirty_tokens=self.state.dirty_tokens)

                # 2. Resolve expired markets: credits capital back to balance.
                resolved_count = self._resolve_expired_markets(batch.batch_end_ts)
                if resolved_count > 0:
                    self.stats.resolutions += resolved_count
                    screener_blocked = False

                if had_fills:
                    screener_blocked = False  # TP fills may have freed balance

                # 3. Screener: skip entirely when balance is known exhausted.
                if screener_blocked:
                    self.stats.screener_skipped += 1
                    continue

                cycle_context = self.order_manager.build_cycle_context()
                candidates = self.screener.scan(allowed_market_ids=self.state.dirty_markets)
                self.stats.screener_cycles += 1
                self.stats.candidates_seen += len(candidates)

                placed = 0
                for candidate in candidates:
                    if cycle_context.balance_exhausted:
                        break
                    if self.mc.scanner_entry and candidate.current_price <= self.mc.entry_price_max:
                        placed += len(self.order_manager.process_scanner_entry(candidate, context=cycle_context))
                    elif self.mc.use_resting_bids:
                        placed += len(self.order_manager.process_candidate(candidate, context=cycle_context))
                self.stats.orders_placed += placed

                if cycle_context.balance_exhausted:
                    screener_blocked = True

        elapsed = time.monotonic() - t0
        logger.info(
            f"Tape runner finished in {elapsed:.1f}s | batches={self.stats.batches} "
            f"empty={self.stats.empty_batches} screener_cycles={self.stats.screener_cycles} "
            f"screener_skipped={self.stats.screener_skipped} resolutions={self.stats.resolutions} "
            f"trades={self.stats.trades} candidates={self.stats.candidates_seen} orders={self.stats.orders_placed}"
        )
        return {
            "elapsed_s": elapsed,
            "batches": self.stats.batches,
            "empty_batches": self.stats.empty_batches,
            "screener_cycles": self.stats.screener_cycles,
            "screener_skipped": self.stats.screener_skipped,
            "resolutions": self.stats.resolutions,
            "trades": self.stats.trades,
            "candidates_seen": self.stats.candidates_seen,
            "orders_placed": self.stats.orders_placed,
            "positions_db": str(self.positions_db),
            "paper_db": str(self.paper_db),
        }
