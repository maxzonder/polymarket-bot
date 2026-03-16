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
from strategy.screener import EntryCandidate
from strategy.risk_manager import RiskManager, SizedPosition, TPOrder
from utils.logger import setup_logger
from utils.paths import DATA_DIR, ensure_runtime_dirs

ensure_runtime_dirs()
logger = setup_logger("order_manager")

POSITIONS_DB = DATA_DIR / "positions.db"

# Minimum total ask size (tokens) available at-or-below price_level to place a bid.
# Prevents placing orders in empty/garbage markets.
MIN_ASK_DEPTH_TOKENS = 1.0


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
            order_id      TEXT PRIMARY KEY,
            position_id   TEXT NOT NULL,
            token_id      TEXT NOT NULL,
            sell_price    REAL NOT NULL,
            sell_quantity REAL NOT NULL,
            label         TEXT,
            status        TEXT NOT NULL DEFAULT 'live',
            created_at    INTEGER NOT NULL,
            FOREIGN KEY (position_id) REFERENCES positions(position_id)
        );
        CREATE INDEX IF NOT EXISTS idx_tp_position ON tp_orders(position_id);
    """)
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
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self._db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        _init_positions_db(conn)
        conn.close()

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
                continue

            if self._count_open_positions(conn) >= self.mc.max_open_positions:
                logger.info(f"Max open positions reached ({self.mc.max_open_positions}), skipping")
                break

            # ── Depth / liquidity gate ─────────────────────────────────────
            # Fetch real per-token orderbook; reject if insufficient ask depth
            # at our price level (protects against garbage / un-tradable markets).
            try:
                book = get_orderbook(candidate.token_id)
                ask_depth = book.ask_depth_at(price_level)
                if ask_depth < MIN_ASK_DEPTH_TOKENS:
                    logger.debug(
                        f"Depth gate: skip {candidate.token_id[:16]} @ ${price_level:.5f} "
                        f"ask_depth={ask_depth:.2f} < {MIN_ASK_DEPTH_TOKENS}"
                    )
                    continue
            except Exception as e:
                logger.debug(f"Orderbook check failed for {candidate.token_id[:16]}: {e}")
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
                logger.info(
                    f"Placed resting BUY: {candidate.market_info.question[:50]!r} "
                    f"token={candidate.token_id[:16]} @ ${price_level:.5f} "
                    f"qty={sized.token_quantity:.2f} order_id={result.order_id}"
                )
                results.append(result)
            else:
                logger.warning(f"Order failed: {result.error} | {candidate.token_id} @ ${price_level}")

        conn.commit()
        conn.close()
        return results

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
            "UPDATE tp_orders SET status='matched' WHERE order_id=?",
            (order_id,),
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

            conn.execute(
                "UPDATE positions SET status='resolved', closed_at=?, "
                "realized_pnl = COALESCE(realized_pnl, 0) + ? "
                "WHERE position_id=?",
                (now, moonbag_pnl, pos["position_id"]),
            )
            # Cancel any remaining TP orders
            conn.execute(
                "UPDATE tp_orders SET status='cancelled' WHERE position_id=? AND status='live'",
                (pos["position_id"],),
            )
            logger.info(
                f"Position resolved: {pos['position_id'][:8]} token={token_id[:16]} "
                f"winner={is_winner} moonbag_pnl=${moonbag_pnl:.4f}"
            )

        conn.commit()
        conn.close()

        if is_winner:
            self.clob.paper_record_resolution(token_id, 1.0)

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
