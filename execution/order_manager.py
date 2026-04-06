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
from execution.exposure_manager import ExposureManager
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
            mode           TEXT,
            candidate_id   TEXT,
            neg_risk_group_id TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_resting_token ON resting_orders(token_id);
        CREATE INDEX IF NOT EXISTS idx_resting_status ON resting_orders(status);
        CREATE INDEX IF NOT EXISTS idx_resting_candidate ON resting_orders(candidate_id);
        CREATE INDEX IF NOT EXISTS idx_resting_group ON resting_orders(neg_risk_group_id);

        CREATE TABLE IF NOT EXISTS positions (
            position_id      TEXT PRIMARY KEY,
            token_id         TEXT NOT NULL,
            market_id        TEXT NOT NULL,
            outcome_name     TEXT,
            entry_order_id   TEXT,
            entry_price      REAL NOT NULL,
            entry_size_usdc  REAL NOT NULL,
            token_quantity   REAL NOT NULL,
            sold_quantity    REAL NOT NULL DEFAULT 0,
            moonbag_quantity REAL NOT NULL DEFAULT 0,
            status           TEXT NOT NULL DEFAULT 'open',
            opened_at        INTEGER NOT NULL,
            closed_at        INTEGER,
            realized_pnl     REAL,
            mode             TEXT,
            is_winner        INTEGER
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
            order_id       TEXT,
            candidate_id   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_scan_token ON scan_log(token_id);
        CREATE INDEX IF NOT EXISTS idx_scan_at ON scan_log(scanned_at);
        CREATE INDEX IF NOT EXISTS idx_scan_candidate ON scan_log(candidate_id);

        CREATE TABLE IF NOT EXISTS screener_log (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            scanned_at     INTEGER NOT NULL,
            market_id      TEXT    NOT NULL,
            token_id       TEXT,
            question       TEXT,
            category       TEXT,
            current_price  REAL,
            hours_to_close REAL,
            volume_usdc    REAL,
            ef_score       REAL,
            res_score      REAL,
            outcome        TEXT    NOT NULL,
            candidate_id   TEXT,
            market_score   REAL
        );
        CREATE INDEX IF NOT EXISTS idx_screener_at        ON screener_log(scanned_at);
        CREATE INDEX IF NOT EXISTS idx_screener_outcome   ON screener_log(outcome);
        CREATE INDEX IF NOT EXISTS idx_screener_candidate ON screener_log(candidate_id);

        CREATE TABLE IF NOT EXISTS ml_outcomes (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            materialized_at     INTEGER NOT NULL,
            candidate_id        TEXT    NOT NULL,
            market_id           TEXT    NOT NULL,
            token_id            TEXT    NOT NULL,
            question            TEXT,
            category            TEXT,
            current_price       REAL,
            hours_to_close      REAL,
            volume_usdc         REAL,
            ef_score            REAL,
            res_score           REAL,
            entry_level         REAL,
            order_id            TEXT,
            got_fill            INTEGER NOT NULL DEFAULT 0,
            is_winner           INTEGER,
            realized_pnl        REAL,
            realized_roi        REAL,
            time_to_fill_hours  REAL,
            tp_5x_hit           INTEGER,
            tp_10x_hit          INTEGER,
            tp_20x_hit          INTEGER,
            tp_moonbag_hit      INTEGER,
            peak_price          REAL,
            peak_x              REAL,
            entry_price         REAL,
            entry_size_usdc     REAL,
            position_status     TEXT,
            opened_at           INTEGER,
            closed_at           INTEGER,
            UNIQUE(candidate_id, entry_level)
        );
        CREATE INDEX IF NOT EXISTS idx_ml_candidate ON ml_outcomes(candidate_id);
        CREATE INDEX IF NOT EXISTS idx_ml_got_fill  ON ml_outcomes(got_fill);
    """)
    pb.init_tables(conn)
    # Migrations for existing DBs
    for migration in [
        "ALTER TABLE screener_log ADD COLUMN market_score REAL",
    ]:
        try:
            conn.execute(migration)
        except Exception:
            pass  # column already exists
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
        self.em = ExposureManager(
            db_path=self._db_path,
            max_per_market=self.mc.max_exposure_per_market or float("inf"),
        )
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self._db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        _init_positions_db(conn)
        # Migrations: add columns that may not exist in older DBs.
        migrations = [
            "ALTER TABLE tp_orders      ADD COLUMN filled_quantity REAL NOT NULL DEFAULT 0",
            "ALTER TABLE tp_orders      ADD COLUMN filled_at INTEGER",
            "ALTER TABLE resting_orders ADD COLUMN candidate_id TEXT",
            "ALTER TABLE resting_orders ADD COLUMN filled_quantity REAL NOT NULL DEFAULT 0",
            "ALTER TABLE resting_orders ADD COLUMN outcome_name TEXT",
            "ALTER TABLE scan_log       ADD COLUMN candidate_id TEXT",
            "ALTER TABLE screener_log   ADD COLUMN candidate_id TEXT",
            "ALTER TABLE positions      ADD COLUMN is_winner INTEGER",
            "ALTER TABLE positions      ADD COLUMN peak_price REAL",
            "ALTER TABLE positions      ADD COLUMN peak_x REAL",
            "ALTER TABLE positions      ADD COLUMN sold_quantity REAL NOT NULL DEFAULT 0",
            "ALTER TABLE ml_outcomes    ADD COLUMN tp_10x_hit INTEGER",
            "ALTER TABLE ml_outcomes    ADD COLUMN tp_20x_hit INTEGER",
            "ALTER TABLE ml_outcomes    ADD COLUMN tp_moonbag_hit INTEGER",
            "ALTER TABLE ml_outcomes    ADD COLUMN peak_price REAL",
            "ALTER TABLE ml_outcomes    ADD COLUMN peak_x REAL",
            "ALTER TABLE resting_orders ADD COLUMN neg_risk_group_id TEXT",
        ]
        for sql in migrations:
            try:
                conn.execute(sql)
                conn.commit()
            except Exception:
                pass  # column already exists
        pb.ensure_seeded(conn, self.config.paper_initial_balance_usdc)
        conn.commit()
        conn.close()

    def get_cash_balance(self) -> float:
        """Return current paper cash_balance for RiskManager / startup use."""
        conn = self._conn()
        row = conn.execute("SELECT cash_balance FROM paper_balance WHERE id=1").fetchone()
        conn.close()
        return float(row["cash_balance"]) if row else self.config.paper_initial_balance_usdc

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
                 total_score, ef_score, res_score, entry_level, outcome, order_id,
                 candidate_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                candidate.candidate_id or None,
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
        # expires_at deprecated: resting bids are GTC (commit 7855cac).
        # Kept in DB schema for historical rows but no longer used as cancellation trigger.
        expires_at = 0

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
                market_score=candidate.market_score,
            )
            if sized is None:
                logger.debug(f"Risk rejected position: {candidate.token_id} @ ${price_level}")
                self._log_scan(conn, candidate, price_level, "risk_rejected")
                continue

            logger.debug(
                f"Sized: {candidate.token_id[:16]} @ ${price_level:.5f} "
                f"stake=${sized.stake_usdc:.2f} qty={sized.token_quantity:.2f} "
                f"ms_tier={candidate.market_score.tier if candidate.market_score else 'n/a'}"
            )

            if self._count_open_positions(conn) >= self.mc.max_open_positions:
                logger.info(f"Max open positions reached ({self.mc.max_open_positions}), skipping")
                self._log_scan(conn, candidate, price_level, "max_positions")
                break

            # ── Resting markets cap ────────────────────────────────────────
            if self.mc.max_resting_markets > 0:
                n_resting_markets = conn.execute(
                    "SELECT COUNT(DISTINCT market_id) FROM resting_orders WHERE status='live'"
                ).fetchone()[0]
                if n_resting_markets >= self.mc.max_resting_markets:
                    logger.info(
                        f"Max resting markets reached ({n_resting_markets}/{self.mc.max_resting_markets}), skipping"
                    )
                    self._log_scan(conn, candidate, price_level, "max_resting_markets")
                    conn.commit()
                    conn.close()
                    return results

            # ── Cluster cap ────────────────────────────────────────────────
            if self.mc.max_resting_per_cluster > 0:
                cluster_key = candidate.market_info.neg_risk_group_id
                if cluster_key is not None:
                    cluster_count = conn.execute(
                        "SELECT COUNT(DISTINCT market_id) FROM resting_orders "
                        "WHERE status='live' AND neg_risk_group_id=?",
                        (cluster_key,),
                    ).fetchone()[0]
                    if cluster_count >= self.mc.max_resting_per_cluster:
                        logger.debug(
                            f"Cluster cap: market {candidate.market_info.market_id} "
                            f"group={cluster_key} already has {cluster_count} markets"
                        )
                        self._log_scan(conn, candidate, price_level, "cluster_cap")
                        continue

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

                # Spread gate: reject if bid-ask spread is too wide.
                # Adaptive threshold: low-price tokens always have wide spreads
                # due to tick constraints, so we allow up to 80% below $0.05.
                mid = (book.best_bid + book.best_ask) / 2.0
                max_spread = 0.30 if mid >= 0.05 else 0.80
                spread_pct = (book.best_ask - book.best_bid) / mid if mid > 0 else 1.0
                if spread_pct > max_spread:
                    logger.debug(
                        f"Spread gate: {candidate.token_id[:16]} spread={spread_pct:.2%} "
                        f"> {max_spread:.0%} (mid={mid:.4f})"
                    )
                    self._log_scan(conn, candidate, price_level, "spread_gate")
                    continue
            except Exception as e:
                logger.debug(f"Orderbook check failed for {candidate.token_id[:16]}: {e}")
                self._log_scan(conn, candidate, price_level, "depth_gate_error")
                continue

            # ── Exposure cap gate ──────────────────────────────────────────
            if self.mc.max_exposure_per_market > 0 and not self.em.can_add(
                candidate.market_info.market_id, candidate.token_id, sized.stake_usdc
            ):
                logger.debug(
                    f"Exposure cap: {candidate.token_id[:16]} market={candidate.market_info.market_id[:16]} "
                    f"stake=${sized.stake_usdc:.2f} would exceed max_exposure_per_market=${self.mc.max_exposure_per_market:.2f}"
                )
                self._log_scan(conn, candidate, price_level, "exposure_cap")
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
                    candidate_id=candidate.candidate_id or None,
                    outcome_name=candidate.outcome_name or "",
                    neg_risk_group_id=candidate.market_info.neg_risk_group_id,
                )
                self._log_scan(conn, candidate, price_level, "placed", order_id=result.order_id)
                neg_risk_tag = (
                    f" [neg-risk grp={candidate.market_info.neg_risk_group_id[:12]}]"
                    if candidate.market_info.neg_risk_group_id else ""
                )
                ms_tier = candidate.market_score.tier if candidate.market_score else "n/a"
                logger.info(
                    f"Placed resting BUY: {candidate.market_info.question[:50]!r} "
                    f"token={candidate.token_id[:16]} @ ${price_level:.5f} "
                    f"stake=${sized.stake_usdc:.2f} qty={sized.token_quantity:.2f} "
                    f"tier={ms_tier} score={candidate.total_score:.3f} "
                    f"order_id={result.order_id}{neg_risk_tag}"
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

        # Dedupe: skip if there is already a live resting order or open position on this token.
        existing = conn.execute(
            "SELECT 1 FROM resting_orders WHERE token_id=? AND status='live' "
            "UNION SELECT 1 FROM positions WHERE token_id=? AND status='open' LIMIT 1",
            (candidate.token_id, candidate.token_id),
        ).fetchone()
        if existing:
            logger.debug(f"Scanner entry: token {candidate.token_id[:16]} already has live order or open position — skipping")
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
            market_score=candidate.market_score,
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

        # Exposure cap gate
        if self.mc.max_exposure_per_market > 0 and not self.em.can_add(
            candidate.market_info.market_id, candidate.token_id, sized.stake_usdc
        ):
            logger.debug(
                f"Exposure cap (scanner): {candidate.token_id[:16]} "
                f"stake=${sized.stake_usdc:.2f} would exceed max_exposure_per_market=${self.mc.max_exposure_per_market:.2f}"
            )
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
                expires_at=0,  # GTC — no TTL (commit 7855cac)
                mode=self.mc.name,
                candidate_id=candidate.candidate_id or None,
                outcome_name=candidate.outcome_name or "",
                neg_risk_group_id=candidate.market_info.neg_risk_group_id,
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
            f"cash=${bal['cash_balance']:.4f} reserved=${bal['reserved_resting']:.4f}"
        )

    def _get_active_resting_prices(self, conn: sqlite3.Connection, token_id: str) -> set[float]:
        """Return price levels that are already covered — either live resting bid or open position."""
        resting = conn.execute(
            "SELECT price FROM resting_orders WHERE token_id=? AND status='live'",
            (token_id,),
        ).fetchall()
        filled = conn.execute(
            "SELECT entry_price FROM positions WHERE token_id=? AND status='open'",
            (token_id,),
        ).fetchall()
        return {float(r["price"]) for r in resting} | {float(r["entry_price"]) for r in filled}

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
        candidate_id: Optional[str] = None,
        outcome_name: str = "",
        neg_risk_group_id: Optional[str] = None,
    ) -> None:
        now = int(time.time())
        conn.execute(
            "INSERT OR REPLACE INTO resting_orders "
            "(order_id, token_id, market_id, side, price, size, status, created_at, expires_at, mode, candidate_id, outcome_name, neg_risk_group_id) "
            "VALUES (?, ?, ?, 'BUY', ?, ?, 'live', ?, ?, ?, ?, ?, ?)",
            (order_id, token_id, market_id, price, size, now, expires_at, mode, candidate_id, outcome_name, neg_risk_group_id),
        )

    def _get_open_position(self, conn: sqlite3.Connection, token_id: str, market_id: str) -> Optional[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM positions WHERE token_id=? AND market_id=? AND status='open' ORDER BY opened_at LIMIT 1",
            (token_id, market_id),
        ).fetchone()

    def _planned_entry_quantity(self, conn: sqlite3.Connection, token_id: str, market_id: str) -> float:
        row = conn.execute(
            "SELECT COALESCE(SUM(size), 0) FROM resting_orders WHERE token_id=? AND market_id=? AND status != 'cancelled'",
            (token_id, market_id),
        ).fetchone()
        return float(row[0] or 0.0)

    def _tp_activation_passed(self, conn: sqlite3.Connection, token_id: str, market_id: str, total_qty: float) -> bool:
        planned_qty = self._planned_entry_quantity(conn, token_id, market_id)
        if planned_qty <= 0:
            return True
        return (total_qty / planned_qty) >= 0.25

    def _reconcile_tp_orders(self, conn: sqlite3.Connection, position_id: str, token_id: str, market_id: str) -> None:
        pos = conn.execute(
            "SELECT * FROM positions WHERE position_id=?",
            (position_id,),
        ).fetchone()
        if pos is None or pos["status"] != "open":
            return

        total_qty = float(pos["token_quantity"] or 0.0)
        if total_qty <= 0:
            return

        sized = SizedPosition(
            token_id=token_id,
            entry_price=float(pos["entry_price"]),
            stake_usdc=float(pos["entry_size_usdc"]),
            token_quantity=total_qty,
            tp_levels=list(self.mc.tp_levels),
            moonbag_fraction=self.mc.moonbag_fraction,
            rationale="accumulated",
        )
        target_orders = self.risk.build_tp_orders(sized)
        moonbag_qty = sum(tp.sell_quantity for tp in target_orders if tp.label == "moonbag_resolution")
        conn.execute(
            "UPDATE positions SET moonbag_quantity=? WHERE position_id=?",
            (moonbag_qty, position_id),
        )

        moonbag_id = f"moonbag_{position_id}"
        existing_moonbag = conn.execute(
            "SELECT 1 FROM tp_orders WHERE order_id=?",
            (moonbag_id,),
        ).fetchone()
        if existing_moonbag:
            conn.execute(
                "UPDATE tp_orders SET sell_quantity=?, sell_price=1.0, status='moonbag' WHERE order_id=?",
                (moonbag_qty, moonbag_id),
            )
        elif moonbag_qty >= 0.0001:
            conn.execute(
                "INSERT INTO tp_orders (order_id, position_id, token_id, sell_price, sell_quantity, label, status, created_at) "
                "VALUES (?, ?, ?, 1.0, ?, 'moonbag_resolution', 'moonbag', ?)",
                (moonbag_id, position_id, token_id, moonbag_qty, int(time.time())),
            )

        if not self._tp_activation_passed(conn, token_id, market_id, total_qty):
            return

        for tp in target_orders:
            if tp.label == "moonbag_resolution":
                continue

            allocated = conn.execute(
                "SELECT COALESCE(SUM(sell_quantity), 0) FROM tp_orders "
                "WHERE position_id=? AND label=? AND status IN ('live', 'matched')",
                (position_id, tp.label),
            ).fetchone()[0]
            allocated = float(allocated or 0.0)
            missing_qty = round(tp.sell_quantity - allocated, 4)
            if missing_qty < 0.0001:
                continue
            if allocated > 0 and (missing_qty / allocated) < 0.10:
                continue

            tp_result = self.clob.place_limit_order(
                token_id=tp.token_id,
                side="SELL",
                price=tp.sell_price,
                size=missing_qty,
                label=tp.label,
            )
            conn.execute(
                "INSERT OR IGNORE INTO tp_orders "
                "(order_id, position_id, token_id, sell_price, sell_quantity, label, status, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (tp_result.order_id, position_id, tp.token_id, tp.sell_price, missing_qty, tp.label, tp_result.status, int(time.time())),
            )
            logger.info(
                f"Placed TP SELL: token={token_id[:16]} {tp.label} "
                f"@ ${tp.sell_price:.5f} qty={missing_qty:.2f} id={tp_result.order_id}"
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
        """Apply a BUY fill delta to the accumulated open position."""
        import uuid

        now = int(time.time())
        stake_usdc = fill_price * fill_quantity
        conn = self._conn()

        resting = conn.execute(
            "SELECT size, filled_quantity, status FROM resting_orders WHERE order_id=?",
            (order_id,),
        ).fetchone()
        if resting is not None:
            prev_filled = float(resting["filled_quantity"] or 0.0)
            prev_status = str(resting["status"] or "live")
            new_filled = min(float(resting["size"]), prev_filled + fill_quantity)
            is_done = new_filled >= float(resting["size"]) * 0.99
            conn.execute(
                "UPDATE resting_orders SET filled_quantity=?, status=? WHERE order_id=?",
                (new_filled, "matched" if is_done else prev_status, order_id),
            )

        if self.config.dry_run:
            pb.debit(conn, stake_usdc, f"entry fill {order_id[:12]}")

        pos = self._get_open_position(conn, token_id, market_id)
        if pos is None:
            position_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO positions "
                "(position_id, token_id, market_id, outcome_name, entry_order_id, "
                "entry_price, entry_size_usdc, token_quantity, sold_quantity, moonbag_quantity, status, opened_at, mode) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 'open', ?, ?)",
                (position_id, token_id, market_id, outcome_name, order_id, fill_price, stake_usdc, fill_quantity, now, self.mc.name),
            )
        else:
            position_id = pos["position_id"]
            total_usdc = float(pos["entry_size_usdc"] or 0.0) + stake_usdc
            total_qty = float(pos["token_quantity"] or 0.0) + fill_quantity
            avg_entry = total_usdc / max(total_qty, 1e-12)
            conn.execute(
                "UPDATE positions SET outcome_name=COALESCE(NULLIF(outcome_name, ''), ?), "
                "entry_price=?, entry_size_usdc=?, token_quantity=? WHERE position_id=?",
                (outcome_name, avg_entry, total_usdc, total_qty, position_id),
            )

        self._reconcile_tp_orders(conn, position_id, token_id, market_id)
        pos = conn.execute("SELECT * FROM positions WHERE position_id=?", (position_id,)).fetchone()

        conn.commit()
        conn.close()

        self.em.record_fill(market_id, token_id, fill_price, fill_quantity, fill_ts=now)

        logger.info(
            f"Position accumulated: {position_id[:8]} token={token_id[:16]} "
            f"fill=${fill_price:.5f} delta_qty={fill_quantity:.2f} "
            f"total_qty={float(pos['token_quantity']):.2f} avg_entry=${float(pos['entry_price']):.5f}"
        )

    def on_tp_filled(self, order_id: str, fill_price: float, fill_quantity: float) -> None:
        """Called when a TP SELL order gets filled. Accumulates partial PnL into position."""
        now = int(time.time())
        conn = self._conn()

        # Determine new cumulative fill and whether the order is fully matched.
        tp_row = conn.execute(
            "SELECT sell_quantity, filled_quantity FROM tp_orders WHERE order_id=?",
            (order_id,),
        ).fetchone()
        if tp_row is None:
            conn.close()
            return
        new_filled = float(tp_row["filled_quantity"] or 0) + fill_quantity
        is_done = new_filled >= float(tp_row["sell_quantity"]) * 0.99
        new_status = "matched" if is_done else "live"

        conn.execute(
            "UPDATE tp_orders SET status=?, filled_quantity=?, filled_at=? WHERE order_id=?",
            (new_status, new_filled, now, order_id),
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
                "UPDATE positions SET realized_pnl = COALESCE(realized_pnl, 0) + ?, "
                "sold_quantity = COALESCE(sold_quantity, 0) + ? WHERE position_id=?",
                (pnl, fill_quantity, position_id),
            )
            # Credit paper balance (TP fill = USDC received from selling tokens)
            if self.config.dry_run:
                proceeds = fill_price * fill_quantity
                pb.credit(conn, proceeds, f"TP fill {order_id[:12]}")
            logger.info(
                f"TP {'fully' if is_done else 'partially'} filled: order={order_id[:8]} "
                f"@ ${fill_price:.5f} qty={fill_quantity:.2f} "
                f"cumulative={new_filled:.2f}/{float(tp_row['sell_quantity']):.2f} "
                f"pnl=${pnl:.4f}"
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
        resolved_markets = {(str(pos["market_id"]), str(pos["token_id"])) for pos in positions}

        for pos in positions:
            moonbag_row = conn.execute(
                "SELECT sell_quantity FROM tp_orders WHERE position_id=? AND label='moonbag_resolution' LIMIT 1",
                (pos["position_id"],),
            ).fetchone()
            moonbag_qty = (
                float(moonbag_row["sell_quantity"])
                if moonbag_row is not None
                else float(pos["moonbag_quantity"])
            )
            entry_price = float(pos["entry_price"])
            live_tps = conn.execute(
                "SELECT sell_quantity, filled_quantity FROM tp_orders "
                "WHERE position_id=? AND status='live'",
                (pos["position_id"],),
            ).fetchall()
            live_tp_remaining = sum(
                float(r["sell_quantity"]) - float(r["filled_quantity"])
                for r in live_tps
            )
            sold_quantity = float(pos["sold_quantity"] or 0.0)
            unscheduled_qty = max(
                0.0,
                float(pos["token_quantity"] or 0.0) - sold_quantity - moonbag_qty - live_tp_remaining,
            )
            remaining_qty = moonbag_qty + live_tp_remaining + unscheduled_qty
            total_resolution_pnl = (resolution_price - entry_price) * remaining_qty

            if self.config.dry_run:
                resolution_proceeds = resolution_price * remaining_qty
                pb.credit(conn, resolution_proceeds, f"resolution {token_id[:16]} winner={is_winner}")

            conn.execute(
                "UPDATE positions SET status='resolved', closed_at=?, is_winner=?, "
                "realized_pnl = COALESCE(realized_pnl, 0) + ? "
                "WHERE position_id=?",
                (now, 1 if is_winner else 0, total_resolution_pnl, pos["position_id"]),
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
            total_pnl = total_resolution_pnl + float(pos["realized_pnl"] or 0)
            outcome_icon = "🏆" if is_winner else "💀"
            logger.info(
                f"Position resolved: {pos['position_id'][:8]} token={token_id[:16]} "
                f"winner={is_winner} remaining_qty={remaining_qty:.2f} "
                f"resolution_pnl=${total_resolution_pnl:.4f}"
            )
            # Resolution alert disabled for data-collection mode (issue #57).
            # Only hourly summary is sent — see main_loop._hourly_report_loop.

        # Cancel unresolved resting BUYs for resolved market/token pairs so
        # dry-run reserved_resting is released and the book does not retain
        # impossible post-resolution entries.
        for market_id, token_id_for_cancel in resolved_markets:
            live_resting = conn.execute(
                "SELECT order_id FROM resting_orders WHERE market_id=? AND token_id=? AND status='live'",
                (market_id, token_id_for_cancel),
            ).fetchall()
            for row in live_resting:
                self.clob.cancel_order(row["order_id"])
                conn.execute(
                    "UPDATE resting_orders SET status='cancelled' WHERE order_id=?",
                    (row["order_id"],),
                )

        conn.commit()
        conn.close()

        self.clob.paper_record_resolution(token_id, 1.0 if is_winner else 0.0)

    # ── Stale order cleanup ───────────────────────────────────────────────────

    def cancel_stale_orders(self) -> int:
        """
        Cancel resting BUY orders whose market has resolved or closed.
        No time-based TTL — resting bids are GTC and stay live until:
          1. Market is gone from Gamma (market is None)
          2. Market has closed (hours_to_close <= 0)

        Returns count of cancelled orders.
        """
        conn = self._conn()

        live_markets = conn.execute(
            "SELECT DISTINCT market_id FROM resting_orders WHERE status='live'"
        ).fetchall()

        cancelled = 0
        for row in live_markets:
            market_id = row["market_id"]
            try:
                market = fetch_market(market_id)
                closed = (
                    market is None
                    or (market.hours_to_close is not None and market.hours_to_close <= 0)
                )
                if not closed:
                    continue

                reason = "vanished" if market is None else "closed"
                orders_to_cancel = conn.execute(
                    "SELECT order_id, token_id FROM resting_orders "
                    "WHERE market_id=? AND status='live'",
                    (market_id,),
                ).fetchall()
                for o in orders_to_cancel:
                    if self.clob.cancel_order(o["order_id"]):
                        conn.execute(
                            "UPDATE resting_orders SET status='cancelled' WHERE order_id=?",
                            (o["order_id"],),
                        )
                        cancelled += 1
                        logger.info(
                            f"Cancelled resting bid ({reason}): "
                            f"{o['order_id'][:8]} token={o['token_id'][:16]}"
                        )
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
