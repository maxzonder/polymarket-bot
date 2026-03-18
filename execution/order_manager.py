"""
Order Manager — places and manages resting limit bids and TP orders.

For big_swan_mode:
  - Places resting limit BUY orders at entry_price_levels BEFORE market dips
  - Cancels stale resting bids (older than resting_order_ttl) for closed markets
  - Avoids duplicate bids: max one active bid per (token_id, price_level)

Anti-garbage-market protection:
  - Before placing bid: check orderbook depth via CLOB API
  - Only place bid if there's at least some ask volume at our price level
    (indicates someone would actually sell to us there eventually)

Stale order cleanup:
  - Periodically calls Gamma to check if markets are still active
  - Cancels bids for resolved / closed / archived markets
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

from api.clob_client import ClobClient, OrderResult, get_orderbook
from api.gamma_client import fetch_market
from config import BotConfig
from execution import paper_balance as pb
from strategy.screener import EntryCandidate
from strategy.risk_manager import RiskManager, SizedPosition, TPOrder
from utils.logger import setup_logger
from utils.paths import DATA_DIR, ensure_runtime_dirs
from utils.telegram import send_message

ensure_runtime_dirs()
logger = setup_logger("order_manager")

POSITIONS_DB = DATA_DIR / "positions.db"

# Minimum bid size at the top-of-book required to place a resting bid.
# We check top-of-book activity, not depth at our (future) bid level,
# because resting bids are placed BELOW current price — ask depth at our
# price level will naturally be zero until the market dips there.
MIN_TOP_BOOK_TOKENS = 1.0


def _init_positions_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS resting_orders (
            order_id       TEXT PRIMARY KEY,
            token_id       TEXT NOT NULL,
            market_id      TEXT NOT NULL,
            side           TEXT NOT NULL DEFAULT 'BUY',
            price          REAL NOT NULL,
            size           REAL NOT NULL,
            status         TEXT NOT NULL DEFAULT 'live',
            created_at     INTEGER NOT NULL,
            expires_at     INTEGER NOT NULL,
            label          TEXT,
            mode           TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_resting_token ON resting_orders(token_id);
        CREATE INDEX IF NOT EXISTS idx_resting_status ON resting_orders(status);

        CREATE TABLE IF NOT EXISTS positions (
            position_id      TEXT PRIMARY KEY,
            token_id         TEXT NOT NULL,
            market_id        TEXT NOT NULL,
            outcome_name     TEXT,
            entry_order_id   TEXT,
            entry_price      REAL NOT NULL,
            entry_size_usdc  REAL NOT NULL,
            token_quantity   REAL NOT NULL,
            moonbag_quantity REAL NOT NULL DEFAULT 0,
            status           TEXT NOT NULL DEFAULT 'open',
            opened_at        INTEGER NOT NULL,
            closed_at        INTEGER,
            realized_pnl     REAL,
            mode             TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pos_token ON positions(token_id);
        CREATE INDEX IF NOT EXISTS idx_pos_status ON positions(status);

        CREATE TABLE IF NOT EXISTS tp_orders (
            order_id         TEXT PRIMARY KEY,
            position_id      TEXT NOT NULL,
            token_id         TEXT NOT NULL,
            sell_price       REAL NOT NULL,
            sell_quantity    REAL NOT NULL,
            filled_quantity  REAL NOT NULL DEFAULT 0,
            label            TEXT,
            status           TEXT NOT NULL DEFAULT 'live',
            created_at       INTEGER NOT NULL,
            FOREIGN KEY (position_id) REFERENCES positions(position_id)
        );
        CREATE INDEX IF NOT EXISTS idx_tp_position ON tp_orders(position_id);

        CREATE TABLE IF NOT EXISTS scan_log (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            scanned_at     INTEGER NOT NULL,
            token_id       TEXT NOT NULL,
            market_id      TEXT NOT NULL,
            question       TEXT,
            current_price  REAL,
            total_score    REAL,
            ef_score       REAL,
            res_score      REAL,
            entry_level    REAL NOT NULL,
            outcome        TEXT NOT NULL,
            order_id       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_scan_token ON scan_log(token_id);
        CREATE INDEX IF NOT EXISTS idx_scan_at ON scan_log(scanned_at);
    """)
    pb.init_tables(conn)
    conn.commit()


class OrderManager:
    """
    Manages the full lifecycle of resting bids and TP orders.

    Responsibilities:
    1. Accept EntryCandidate from Screener
    2. Check if we already have a resting bid for this (token_id, price) → skip duplicates
    3. Check orderbook depth before placing
    4. Place GTC resting limit BUY orders
    5. After fills: create positions record and place TP SELL orders
    6. Periodically cancel stale/orphaned resting bids
    """

    def __init__(self, config: BotConfig, clob: ClobClient, risk_manager: RiskManager):
        self.config = config
        self.mc = config.mode_config
        self.clob = clob
        self.risk = risk_manager
        self._db_path = str(POSITIONS_DB)
        self._last_balance_alert_ts: int = 0
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self._db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        _init_positions_db(conn)
        # Migration: add filled_quantity if this DB predates the column.
        try:
            conn.execute("ALTER TABLE tp_orders ADD COLUMN filled_quantity REAL NOT NULL DEFAULT 0")
            conn.commit()
        except Exception:
            pass  # column already exists
        pb.ensure_seeded(conn)
        conn.commit()
        conn.close()

    def get_cash_balance(self) -> float:
        """Return current paper cash_balance for RiskManager / startup use."""
        conn = self._conn()
        row = conn.execute("SELECT cash_balance FROM paper_balance WHERE id=1").fetchone()
        conn.close()
        return float(row["cash_balance"]) if row else pb.INITIAL_BALANCE_USDC

    def _log_scan(
        self,
        conn: sqlite3.Connection,
        candidate: "EntryCandidate",
        entry_level: float,
        outcome: str,
        order_id: Optional[str] = None,
    ) -> None:
        """Write one row to scan_log for observability / paper-trading report."""
        conn.execute(
            """
            INSERT INTO scan_log
                (scanned_at, token_id, market_id, question, current_price,
                 total_score, ef_score, res_score, entry_level, outcome, order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(time.time()),
                candidate.token_id,
                candidate.market_info.market_id,
                (candidate.market_info.question or "")[:120],
                candidate.current_price,
                candidate.total_score,
                candidate.entry_fill_score,
                candidate.resolution_score,
                entry_level,
                outcome,
                order_id,
            ),
        )
        # Do NOT close conn here — caller owns the connection lifecycle.

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    # ── Entry ─────────────────────────────────────────────────────────────────

    def process_candidate(self, candidate: EntryCandidate) -> list[OrderResult]:
        """
        Place resting limit BUY orders for a candidate.
        Skips levels where we already have an active order.
        Returns list of OrderResult (one per placed order).
        """
        results: list[OrderResult] = []
        now = int(time.time())
        expires_at = now + self.config.resting_order_ttl

        conn = self._conn()
        active_prices = self._get_active_resting_prices(conn, candidate.token_id)

        for price_level in candidate.suggested_entry_levels:
            if price_level in active_prices:
                logger.debug(f"Skip duplicate resting bid: {candidate.token_id} @ ${price_level}")
                self._log_scan(conn, candidate, price_level, "duplicate")
                continue

            # Gauge position size for this specific price level
            res_score = None  # we don't have it here easily — use a default
            from strategy.scorer import ResolutionScore
            dummy_res = ResolutionScore(
                category=candidate.market_info.category,
                sample_count=10,
                p_winner=candidate.resolution_score,
                avg_real_x=5.0,
                p_20x=0.05, p_50x=0.02, p_100x=0.01,
                avg_resolution_x=10.0,
                tail_ev=candidate.resolution_score * 10,
                score=candidate.resolution_score,
            )

            sized = self.risk.size_position(
                token_id=candidate.token_id,
                entry_price=price_level,
                resolution_score=dummy_res,
                open_positions=self._count_open_positions(conn),
            )
            if sized is None:
                logger.debug(f"Risk rejected position: {candidate.token_id} @ ${price_level}")
                self._log_scan(conn, candidate, price_level, "risk_rejected")
                continue

            if self._count_open_positions(conn) >= self.mc.max_open_positions:
                logger.info(f"Max open positions reached ({self.mc.max_open_positions}), skipping")
                self._log_scan(conn, candidate, price_level, "max_positions")
                break

            # ── Paper balance gate (dry_run only) ─────────────────────────
            if self.config.dry_run:
                bal = pb.get_balance(conn)
                if bal["free_balance"] < sized.stake_usdc:
                    logger.info(
                        f"Balance gate: free=${bal['free_balance']:.4f} < "
                        f"stake=${sized.stake_usdc:.4f} — skipping"
                    )
                    self._log_scan(conn, candidate, price_level, "balance_exhausted")
                    self._maybe_send_balance_alert(bal)
                    conn.commit()
                    conn.close()
                    return results

            # ── Depth / liquidity gate ─────────────────────────────────────
            # Fetch real per-token orderbook. Reject dead / garbage books.
            # We check top-of-book activity, NOT depth at our bid price level:
            # resting bids are placed below current price, so ask depth at the
            # bid level is normally zero — that is expected, not a rejection signal.
            try:
                book = get_orderbook(candidate.token_id)
                if book.best_ask is None or book.best_bid is None:
                    logger.debug(
                        f"Depth gate: dead book (no asks or bids) for "
                        f"{candidate.token_id[:16]} @ ${price_level:.5f}"
                    )
                    self._log_scan(conn, candidate, price_level, "depth_gate_dead_book")
                    continue
                top_bid_depth = book.bid_depth_at(book.best_bid)
                if top_bid_depth < MIN_TOP_BOOK_TOKENS:
                    logger.debug(
                        f"Depth gate: thin top-of-book for {candidate.token_id[:16]} "
                        f"top_bid_depth={top_bid_depth:.2f} < {MIN_TOP_BOOK_TOKENS}"
                    )
                    self._log_scan(conn, candidate, price_level, "depth_gate_thin")
                    continue
            except Exception as e:
                logger.debug(f"Orderbook check failed for {candidate.token_id[:16]}: {e}")
                self._log_scan(conn, candidate, price_level, "depth_gate_error")
                continue

            label = f"resting_{candidate.market_info.market_id}_{price_level}"
            result = self.clob.place_limit_order(
                token_id=candidate.token_id,
                side="BUY",
                price=price_level,
                size=sized.token_quantity,
                label=label,
            )

            if result.status in ("live", "matched"):
                self._save_resting_order(
                    conn=conn,
                    order_id=result.order_id,
                    token_id=candidate.token_id,
                    market_id=candidate.market_info.market_id,
                    price=price_level,
                    size=sized.token_quantity,
                    expires_at=expires_at,
                    mode=self.mc.name,
                )
                self._log_scan(conn, candidate, price_level, "placed", order_id=result.order_id)
                logger.info(
                    f"Placed resting BUY: {candidate.market_info.question[:50]!r} "
                    f"token={candidate.token_id[:16]} @ ${price_level:.5f} "
                    f"qty={sized.token_quantity:.2f} order_id={result.order_id}"
                )
                results.append(result)
            else:
                self._log_scan(conn, candidate, price_level, "order_failed")
                logger.warning(f"Order failed: {result.error} | {candidate.token_id} @ ${price_level}")

        conn.commit()
        conn.close()
        return results

    def process_scanner_entry(self, candidate: EntryCandidate) -> list[OrderResult]:
        """
        Immediate BUY for scanner-entry modes (fast_tp, balanced) when the token
        is already inside the valid entry zone.

        Executes at the real per-token best_ask from the orderbook — not at a
        pre-computed resting-bid ladder level. Does NOT iterate suggested_entry_levels.
        """
        conn = self._conn()
        now = int(time.time())

        if self._count_open_positions(conn) >= self.mc.max_open_positions:
            logger.info(f"Max open positions reached ({self.mc.max_open_positions}), skipping scanner entry")
            conn.close()
            return []

        try:
            book = get_orderbook(candidate.token_id)
            if book.best_ask is None or book.best_bid is None:
                logger.debug(f"Scanner entry: dead book for {candidate.token_id[:16]}")
                conn.close()
                return []
            top_bid_depth = book.bid_depth_at(book.best_bid)
            if top_bid_depth < MIN_TOP_BOOK_TOKENS:
                logger.debug(
                    f"Scanner entry: thin top-of-book for {candidate.token_id[:16]} "
                    f"top_bid_depth={top_bid_depth:.2f}"
                )
                conn.close()
                return []
            execution_price = book.best_ask
        except Exception as e:
            logger.debug(f"Orderbook check failed (scanner entry) for {candidate.token_id[:16]}: {e}")
            conn.close()
            return []

        from strategy.scorer import ResolutionScore
        dummy_res = ResolutionScore(
            category=candidate.market_info.category,
            sample_count=10,
            p_winner=candidate.resolution_score,
            avg_real_x=5.0,
            p_20x=0.05, p_50x=0.02, p_100x=0.01,
            avg_resolution_x=10.0,
            tail_ev=candidate.resolution_score * 10,
            score=candidate.resolution_score,
        )
        sized = self.risk.size_position(
            token_id=candidate.token_id,
            entry_price=execution_price,
            resolution_score=dummy_res,
            open_positions=self._count_open_positions(conn),
        )
        if sized is None:
            logger.debug(f"Risk rejected scanner entry: {candidate.token_id[:16]} @ ${execution_price:.5f}")
            conn.close()
            return []

        # Paper balance gate (dry_run only)
        if self.config.dry_run:
            bal = pb.get_balance(conn)
            if bal["free_balance"] < sized.stake_usdc:
                logger.info(
                    f"Balance gate (scanner): free=${bal['free_balance']:.4f} < "
                    f"stake=${sized.stake_usdc:.4f} — skipping"
                )
                self._maybe_send_balance_alert(bal)
                conn.close()
                return []

        label = f"scanner_{candidate.market_info.market_id}"
        result = self.clob.place_limit_order(
            token_id=candidate.token_id,
            side="BUY",
            price=execution_price,
            size=sized.token_quantity,
            label=label,
        )

        if result.status in ("live", "matched"):
            self._save_resting_order(
                conn=conn,
                order_id=result.order_id,
                token_id=candidate.token_id,
                market_id=candidate.market_info.market_id,
                price=execution_price,
                size=sized.token_quantity,
                expires_at=now + self.config.resting_order_ttl,
                mode=self.mc.name,
            )
            logger.info(
                f"Scanner BUY: {candidate.market_info.question[:50]!r} "
                f"token={candidate.token_id[:16]} @ ${execution_price:.5f} "
                f"qty={sized.token_quantity:.2f} order_id={result.order_id}"
            )
        else:
            logger.warning(f"Scanner entry failed: {result.error} | {candidate.token_id[:16]}")

        conn.commit()
        conn.close()
        return [result] if result.status in ("live", "matched") else []

    def _maybe_send_balance_alert(self, bal: dict) -> None:
        """Send a Telegram alert when paper balance is exhausted. Rate-limited to once per hour."""
        now = int(time.time())
        if now - self._last_balance_alert_ts < 3600:
            return
        self._last_balance_alert_ts = now
        send_message(
            f"⚠️ <b>Paper balance exhausted</b>\n"
            f"Cash: ${bal['cash_balance']:.4f}\n"
            f"Free: ${bal['free_balance']:.4f}\n"
            f"Reserved (resting): ${bal['reserved_resting']:.4f}\n"
            f"New entries are blocked. Use:\n"
            f"  python scripts/paper_balance.py topup --amount 10"
        )
        logger.warning(
            f"Balance exhausted: free=${bal['free_balance']:.4f} "
            f"cash=${bal['cash_balance']:.4f} reserved=${bal['reserved']:.4f}"
        )

    def _get_active_resting_prices(self, conn: sqlite3.Connection, token_id: str) -> set[float]:
        rows = conn.execute(
            "SELECT price FROM resting_orders WHERE token_id=? AND status='live'",
            (token_id,),
        ).fetchall()
        return {float(r["price"]) for r in rows}

    def _count_open_positions(self, conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT COUNT(*) FROM positions WHERE status='open'").fetchone()
        return int(row[0]) if row else 0

    def _save_resting_order(
        self,
        conn: sqlite3.Connection,
        order_id: str,
        token_id: str,
        market_id: str,
        price: float,
        size: float,
        expires_at: int,
        mode: str,
    ) -> None:
        now = int(time.time())
        conn.execute(
            "INSERT OR REPLACE INTO resting_orders "
            "(order_id, token_id, market_id, side, price, size, status, created_at, expires_at, mode) "
            "VALUES (?, ?, ?, 'BUY', ?, ?, 'live', ?, ?, ?)",
            (order_id, token_id, market_id, price, size, now, expires_at, mode),
        )

    # ── On fill ───────────────────────────────────────────────────────────────

    def on_entry_filled(
        self,
        order_id: str,
        token_id: str,
        market_id: str,
        fill_price: float,
        fill_quantity: float,
        outcome_name: str = "",
    ) -> None:
        """
        Called when a resting BUY order gets filled.
        Creates a position record and places TP SELL orders.
        """
        import uuid
        position_id = str(uuid.uuid4())
        now = int(time.time())
        stake_usdc = fill_price * fill_quantity

        from strategy.scorer import ResolutionScore
        dummy_res = ResolutionScore(
            category=None, sample_count=0, p_winner=0.1, avg_real_x=5.0,
            p_20x=0.05, p_50x=0.02, p_100x=0.01, avg_resolution_x=10.0,
            tail_ev=1.0, score=0.1,
        )
        sized = SizedPosition(
            token_id=token_id,
            entry_price=fill_price,
            stake_usdc=stake_usdc,
            token_quantity=fill_quantity,
            tp_levels=list(self.mc.tp_levels),
            moonbag_fraction=self.mc.moonbag_fraction,
            rationale="filled",
        )
        moonbag_qty = fill_quantity * self.mc.moonbag_fraction

        conn = self._conn()

        # Mark resting order as matched
        conn.execute(
            "UPDATE resting_orders SET status='matched' WHERE order_id=?",
            (order_id,),
        )

        # Debit paper balance (entry fill = USDC spent)
        if self.config.dry_run:
            pb.debit(conn, stake_usdc, f"entry fill {order_id[:12]}")

        # Create position
        conn.execute(
            "INSERT OR IGNORE INTO positions "
            "(position_id, token_id, market_id, outcome_name, entry_order_id, "
            "entry_price, entry_size_usdc, token_quantity, moonbag_quantity, status, opened_at, mode) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)",
            (position_id, token_id, market_id, outcome_name, order_id,
             fill_price, stake_usdc, fill_quantity, moonbag_qty, now, self.mc.name),
        )

        # Place TP orders
        tp_orders = self.risk.build_tp_orders(sized)
        for tp in tp_orders:
            if tp.label == "moonbag_resolution":
                # Don't place a market order for moonbag — just record it
                conn.execute(
                    "INSERT OR IGNORE INTO tp_orders "
                    "(order_id, position_id, token_id, sell_price, sell_quantity, label, status, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'moonbag', ?)",
                    (f"moonbag_{position_id}", position_id, token_id,
                     tp.sell_price, tp.sell_quantity, tp.label, now),
                )
                continue

            tp_result = self.clob.place_limit_order(
                token_id=tp.token_id,
                side="SELL",
                price=tp.sell_price,
                size=tp.sell_quantity,
                label=tp.label,
            )
            conn.execute(
                "INSERT OR IGNORE INTO tp_orders "
                "(order_id, position_id, token_id, sell_price, sell_quantity, label, status, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (tp_result.order_id, position_id, tp.token_id,
                 tp.sell_price, tp.sell_quantity, tp.label,
                 tp_result.status, now),
            )
            logger.info(
                f"Placed TP SELL: token={token_id[:16]} {tp.label} "
                f"@ ${tp.sell_price:.5f} qty={tp.sell_quantity:.2f} id={tp_result.order_id}"
            )

        conn.commit()
        conn.close()

        logger.info(
            f"Position opened: {position_id[:8]} token={token_id[:16]} "
            f"entry=${fill_price:.5f} qty={fill_quantity:.2f} stake=${stake_usdc:.4f} "
            f"moonbag={moonbag_qty:.2f}"
        )

    def on_tp_filled(self, order_id: str, fill_price: float, fill_quantity: float) -> None:
        """Called when a TP SELL order gets filled. Accumulates partial PnL into position."""
        now = int(time.time())
        conn = self._conn()
        conn.execute(
            "UPDATE tp_orders SET status='matched', filled_quantity = filled_quantity + ? WHERE order_id=?",
            (fill_quantity, order_id),
        )
        # Find position and persist partial PnL
        row = conn.execute(
            "SELECT p.position_id, p.entry_price FROM tp_orders t "
            "JOIN positions p ON t.position_id = p.position_id "
            "WHERE t.order_id=?",
            (order_id,),
        ).fetchone()
        if row:
            entry_price = float(row["entry_price"])
            position_id = row["position_id"]
            pnl = (fill_price - entry_price) * fill_quantity
            conn.execute(
                "UPDATE positions SET realized_pnl = COALESCE(realized_pnl, 0) + ? "
                "WHERE position_id=?",
                (pnl, position_id),
            )
            # Credit paper balance (TP fill = USDC received from selling tokens)
            if self.config.dry_run:
                proceeds = fill_price * fill_quantity
                pb.credit(conn, proceeds, f"TP fill {order_id[:12]}")
            logger.info(
                f"TP filled: order={order_id[:8]} @ ${fill_price:.5f} "
                f"qty={fill_quantity:.2f} pnl=${pnl:.4f}"
            )
        conn.commit()
        conn.close()

    def on_market_resolved(self, token_id: str, is_winner: bool) -> None:
        """Called when a market resolves. Close all positions for this token."""
        resolution_price = 1.0 if is_winner else 0.0
        now = int(time.time())
        conn = self._conn()

        positions = conn.execute(
            "SELECT * FROM positions WHERE token_id=? AND status='open'",
            (token_id,),
        ).fetchall()

        for pos in positions:
            moonbag_qty = float(pos["moonbag_quantity"])
            entry_price = float(pos["entry_price"])
            # Moonbag delta: add on top of TP PnL already accumulated in realized_pnl
            moonbag_pnl = (resolution_price - entry_price) * moonbag_qty

            # Live TP tokens (unfilled at resolution) must be valued at resolution_price,
            # not silently discarded. For losers this is a small negative correction
            # (entry_price * remaining_qty). For winners it captures tokens that
            # skipped the TP price and resolved directly at $1.00.
            live_tps = conn.execute(
                "SELECT sell_quantity, filled_quantity FROM tp_orders "
                "WHERE position_id=? AND status='live'",
                (pos["position_id"],),
            ).fetchall()
            tp_residual_pnl = sum(
                (resolution_price - entry_price) * (float(r["sell_quantity"]) - float(r["filled_quantity"]))
                for r in live_tps
            )

            total_resolution_pnl = moonbag_pnl + tp_residual_pnl

            # Credit paper balance: proceeds from moonbag + unfilled TP tokens at resolution
            if self.config.dry_run:
                live_tp_remaining = sum(
                    float(r["sell_quantity"]) - float(r["filled_quantity"])
                    for r in live_tps
                )
                resolution_proceeds = resolution_price * (moonbag_qty + live_tp_remaining)
                pb.credit(conn, resolution_proceeds, f"resolution {token_id[:16]} winner={is_winner}")

            conn.execute(
                "UPDATE positions SET status='resolved', closed_at=?, "
                "realized_pnl = COALESCE(realized_pnl, 0) + ? "
                "WHERE position_id=?",
                (now, total_resolution_pnl, pos["position_id"]),
            )
            # Mark live TP orders as 'resolved' (distinct from 'cancelled' by stale cleanup)
            conn.execute(
                "UPDATE tp_orders SET status='resolved' WHERE position_id=? AND status='live'",
                (pos["position_id"],),
            )
            # Moonbag records also resolved
            conn.execute(
                "UPDATE tp_orders SET status='resolved' WHERE position_id=? AND status='moonbag'",
                (pos["position_id"],),
            )
            logger.info(
                f"Position resolved: {pos['position_id'][:8]} token={token_id[:16]} "
                f"winner={is_winner} moonbag_pnl=${moonbag_pnl:.4f} "
                f"tp_residual_pnl=${tp_residual_pnl:.4f}"
            )

        conn.commit()
        conn.close()

        self.clob.paper_record_resolution(token_id, 1.0 if is_winner else 0.0)

    # ── Stale order cleanup ───────────────────────────────────────────────────

    def cancel_stale_orders(self) -> int:
        """
        Cancel resting BUY orders that are:
        1. Older than resting_order_ttl (config)
        2. Or their market has resolved/closed

        Returns count of cancelled orders.
        """
        now = int(time.time())
        conn = self._conn()

        # Expired by TTL
        expired = conn.execute(
            "SELECT order_id, token_id, market_id FROM resting_orders "
            "WHERE status='live' AND expires_at < ?",
            (now,),
        ).fetchall()

        cancelled = 0
        for row in expired:
            if self.clob.cancel_order(row["order_id"]):
                conn.execute(
                    "UPDATE resting_orders SET status='cancelled' WHERE order_id=?",
                    (row["order_id"],),
                )
                cancelled += 1
                logger.info(f"Cancelled stale resting bid: {row['order_id'][:8]} token={row['token_id'][:16]}")

        # Check market status for live orders
        live_orders = conn.execute(
            "SELECT DISTINCT market_id FROM resting_orders WHERE status='live'"
        ).fetchall()

        for row in live_orders:
            market_id = row["market_id"]
            try:
                market = fetch_market(market_id)
                if market is None:
                    # Market no longer exists / closed
                    orders_to_cancel = conn.execute(
                        "SELECT order_id FROM resting_orders WHERE market_id=? AND status='live'",
                        (market_id,),
                    ).fetchall()
                    for o in orders_to_cancel:
                        if self.clob.cancel_order(o["order_id"]):
                            conn.execute(
                                "UPDATE resting_orders SET status='cancelled' WHERE order_id=?",
                                (o["order_id"],),
                            )
                            cancelled += 1
            except Exception:
                pass

        conn.commit()
        conn.close()

        if cancelled:
            logger.info(f"Stale order cleanup: cancelled {cancelled} orders")
        return cancelled

    def _get_balance_snapshot(self) -> dict:
        """Return current paper balance snapshot (for reporting)."""
        conn = self._conn()
        bal = pb.get_balance(conn)
        conn.close()
        return bal

    def get_open_position_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) FROM positions WHERE status='open'").fetchone()
        conn.close()
        return int(row[0]) if row else 0

    def get_live_resting_order_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) FROM resting_orders WHERE status='live'").fetchone()
        conn.close()
        return int(row[0]) if row else 0
