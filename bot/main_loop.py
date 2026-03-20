"""
Main Bot Loop — asyncio-based orchestration.

Three concurrent loops:
  1. Screener loop (every 5 min):
     - Scan Gamma API for open markets
     - Score candidates with EntryFillScorer + ResolutionScorer
     - Hand off candidates to OrderManager

  2. Monitor loop (every 90 sec):
     - Check CLOB for fills (real) or simulate fills (dry_run)
     - Check Gamma for market resolutions
     - Log current PnL

  3. Cleanup loop (every 1 hour):
     - Cancel stale/expired resting bids
     - Refresh scorer caches from DB

Shutdown: Ctrl+C triggers graceful shutdown after current cycle completes.
"""

from __future__ import annotations

import asyncio
import signal
import time
from datetime import datetime, timezone
from typing import Optional

from api.clob_client import ClobClient
from config import BotConfig, load_config
from execution.order_manager import OrderManager, POSITIONS_DB
from execution.position_monitor import PositionMonitor
from strategy.risk_manager import RiskManager
from strategy.scorer import EntryFillScorer, ResolutionScorer
from strategy.screener import Screener
from utils.logger import setup_logger
from utils import telegram

logger = setup_logger("main_loop")


class BotRunner:
    """
    Assembles all components and runs the main event loop.

    Usage:
        runner = BotRunner(config)
        asyncio.run(runner.run())
    """

    def __init__(self, config: Optional[BotConfig] = None):
        self.config = config or load_config()
        self._shutdown = False

        logger.info(
            f"Bot starting | mode={self.config.mode} dry_run={self.config.dry_run} "
            f"pid={__import__('os').getpid()}"
        )

        # ── Components ────────────────────────────────────────────────────────
        mc = self.config.mode_config
        ef_price_max = max(mc.entry_price_levels) if mc.entry_price_levels else self.config.scorer_entry_price_max
        self.ef_scorer = EntryFillScorer(
            entry_price_max=ef_price_max,
            min_samples=self.config.scorer_min_samples,
        )
        self.res_scorer = ResolutionScorer(
            min_samples=self.config.scorer_min_samples,
        )
        self.screener = Screener(
            config=self.config,
            entry_fill_scorer=self.ef_scorer,
            resolution_scorer=self.res_scorer,
            db_path=POSITIONS_DB,
        )
        self.clob = ClobClient(
            private_key=self.config.private_key,
            dry_run=self.config.dry_run,
        )
        # Build OrderManager first so DB / paper_balance tables are initialised.
        # Then load persisted cash_balance for RiskManager sizing.
        self.order_manager = OrderManager(
            config=self.config,
            clob=self.clob,
            risk_manager=None,  # set below after balance loaded
        )
        cash_balance = (
            self.order_manager.get_cash_balance()
            if self.config.dry_run
            else 10.0  # TODO: fetch real balance from CLOB API
        )
        self.risk = RiskManager(
            mode_config=self.config.mode_config,
            balance_usdc=cash_balance,
        )
        self.order_manager.risk = self.risk
        logger.info(f"Paper balance loaded: cash=${cash_balance:.4f}")
        self.monitor = PositionMonitor(
            config=self.config,
            clob=self.clob,
            order_manager=self.order_manager,
        )
        self._last_report_ts: float = 0.0

    async def run(self) -> None:
        """Start all loops concurrently. Runs until shutdown."""
        loop = asyncio.get_event_loop()

        def _handle_signal():
            logger.info("Shutdown signal received, stopping after current cycle...")
            self._shutdown = True

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _handle_signal)
            except (NotImplementedError, RuntimeError):
                pass  # Windows doesn't support add_signal_handler

        logger.info("Performing initial scorer warmup...")
        self.ef_scorer.refresh()
        self.res_scorer.refresh()
        logger.info("Scorers ready.")

        await asyncio.gather(
            self._screener_loop(),
            self._monitor_loop(),
            self._cleanup_loop(),
        )

    # ── Screener loop ─────────────────────────────────────────────────────────

    async def _screener_loop(self) -> None:
        logger.info(f"Screener loop started | interval={self.config.screener_interval}s")
        while not self._shutdown:
            try:
                await self._run_screener_cycle()
            except Exception as e:
                logger.error(f"Screener cycle error: {e}", exc_info=True)

            for _ in range(self.config.screener_interval):
                if self._shutdown:
                    break
                await asyncio.sleep(1)

    async def _run_screener_cycle(self) -> None:
        t0 = time.monotonic()
        candidates = await asyncio.to_thread(self.screener.scan)
        elapsed = time.monotonic() - t0

        open_count = self.order_manager.get_open_position_count()
        resting_count = self.order_manager.get_live_resting_order_count()

        logger.info(
            f"Screener: found {len(candidates)} candidates in {elapsed:.1f}s | "
            f"open_positions={open_count} resting_bids={resting_count}"
        )

        if not candidates:
            return

        mc = self.config.mode_config
        placed_count = 0

        for candidate in candidates:
            if self._shutdown:
                break

            # Skip scanner-only entry if mode uses resting bids only
            if not mc.scanner_entry and not mc.use_resting_bids:
                continue

            # For scanner entry (fast_tp / balanced): token already in valid entry zone.
            # Execute immediately at best ask — do NOT use resting-bid ladder semantics.
            if mc.scanner_entry and candidate.current_price <= mc.entry_price_max:
                results = await asyncio.to_thread(
                    self.order_manager.process_scanner_entry, candidate
                )
                placed_count += len(results)

            # For resting bids (balanced / big_swan): pre-position below current price.
            # Only fires when price is above entry zone (scanner path did not trigger).
            elif mc.use_resting_bids:
                results = await asyncio.to_thread(
                    self.order_manager.process_candidate, candidate
                )
                placed_count += len(results)

        if placed_count:
            logger.info(f"Screener placed {placed_count} new orders")

    # ── Monitor loop ──────────────────────────────────────────────────────────

    async def _monitor_loop(self) -> None:
        logger.info(f"Monitor loop started | interval={self.config.monitor_interval}s")
        while not self._shutdown:
            try:
                await asyncio.to_thread(self.monitor.check_all)
                stats = await asyncio.to_thread(self.monitor.get_stats)
                logger.info(
                    f"Monitor: positions={stats['open_positions']} "
                    f"resting={stats['live_resting_bids']} "
                    f"tp_orders={stats['live_tp_orders']} "
                    f"resolved={stats['resolved_positions']} "
                    f"pnl=${stats['total_realized_pnl']:.4f}"
                )
            except Exception as e:
                logger.error(f"Monitor cycle error: {e}", exc_info=True)

            for _ in range(self.config.monitor_interval):
                if self._shutdown:
                    break
                await asyncio.sleep(1)

    # ── Cleanup loop ──────────────────────────────────────────────────────────

    async def _cleanup_loop(self) -> None:
        logger.info(f"Cleanup loop started | interval={self.config.resting_cleanup_interval}s")
        while not self._shutdown:
            try:
                cancelled = await asyncio.to_thread(self.order_manager.cancel_stale_orders)
                if cancelled:
                    logger.info(f"Cleanup: cancelled {cancelled} stale resting orders")

                # Refresh scorers from updated DB
                await asyncio.to_thread(self.ef_scorer.refresh)
                await asyncio.to_thread(self.res_scorer.refresh)

                # Sync persisted cash_balance into RiskManager
                if self.config.dry_run:
                    self.risk.balance_usdc = self.order_manager.get_cash_balance()

                # Hourly Telegram report — fires at :01 of every hour (wall clock)
                now_dt = datetime.now(timezone.utc)
                current_hour_ts = now_dt.replace(minute=0, second=0, microsecond=0).timestamp()
                if now_dt.minute >= 1 and self._last_report_ts < current_hour_ts:
                    await asyncio.to_thread(self._send_hourly_report)
                    self._last_report_ts = time.time()

            except Exception as e:
                logger.error(f"Cleanup cycle error: {e}", exc_info=True)

            for _ in range(self.config.resting_cleanup_interval):
                if self._shutdown:
                    break
                await asyncio.sleep(1)

    def _send_hourly_report(self) -> None:
        """Build and send a compact Telegram status snapshot."""
        try:
            stats = self.monitor.get_stats()
            bal = self.order_manager._get_balance_snapshot()
            blocked = bal["free_balance"] <= 0

            deployed = bal["reserved_positions"]
            pnl = stats["total_realized_pnl"]
            roi = 100.0 * pnl / deployed if deployed > 0 else 0.0

            lines = [
                "📊 <b>Hourly status</b>",
                f"Cash (free):   ${bal['free_balance']:.4f}",
                f"Reserved (resting orders): ${bal['reserved_resting']:.4f}",
                f"Deployed (open positions): ${bal['reserved_positions']:.4f}  [info]",
                f"Resting orders: {stats['live_resting_bids']}",
                f"Open positions: {stats['open_positions']}",
                f"Realized PnL:   ${pnl:+.4f}  ROI: {roi:+.1f}%",
                f"Trading: {'🔴 BLOCKED' if blocked else '🟢 ACTIVE'}",
            ]
            telegram.send_message("\n".join(lines))
        except Exception as e:
            logger.warning(f"Hourly report failed: {e}", exc_info=True)
