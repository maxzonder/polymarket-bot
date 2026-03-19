"""
Position Monitor — REST polling for fills and market resolutions.

Runs every ~90 seconds. Checks:
1. Resting BUY fills  → triggers on_entry_filled
2. TP SELL fills      → triggers on_tp_filled
3. Market resolutions → triggers on_market_resolved

No WebSocket. REST is sufficient: our positions live hours, 90s latency is irrelevant.

Fill emulation (dry_run):
  Checks current market prices via CLOB orderbook and simulates fills
  when best_ask <= our resting bid price (BUY fill)
  or best_bid >= our TP sell price (SELL fill).
"""

from __future__ import annotations

import sqlite3
import time
from typing import Optional

import requests

from api.clob_client import ClobClient, get_orderbook
from api.gamma_client import fetch_market
from config import BotConfig
from execution.order_manager import OrderManager, POSITIONS_DB
from utils.logger import setup_logger

logger = setup_logger("position_monitor")

GAMMA_BASE = "https://gamma-api.polymarket.com"


class PositionMonitor:
    """
    Polls CLOB API for order status changes and Gamma API for market resolutions.
    Delegates state changes to OrderManager.
    """

    def __init__(self, config: BotConfig, clob: ClobClient, order_manager: OrderManager):
        self.config = config
        self.clob = clob
        self.om = order_manager
        self._db_path = str(POSITIONS_DB)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def check_all(self) -> None:
        """Run one full monitoring cycle."""
        if self.config.dry_run:
            self._check_fills_dry_run()
        else:
            self._check_fills_real()
        self._check_resolutions()

    # ── Fill detection ────────────────────────────────────────────────────────

    def _check_fills_real(self) -> None:
        """Use CLOB API to check actual order statuses."""
        try:
            all_orders = self.clob.get_all_orders()
        except Exception as e:
            logger.warning(f"get_all_orders failed: {e}")
            return

        matched = [o for o in all_orders if o.get("status") in ("ORDER_STATUS_MATCHED", "matched")]

        conn = self._conn()
        for order in matched:
            order_id = order.get("id") or order.get("orderID")
            if not order_id:
                continue

            # Check if it's a resting BUY
            resting = conn.execute(
                "SELECT * FROM resting_orders WHERE order_id=? AND status='live'",
                (order_id,),
            ).fetchone()
            if resting:
                fill_price = float(order.get("price", resting["price"]))
                fill_qty = float(order.get("size_matched", resting["size"]))
                conn.close()
                self.om.on_entry_filled(
                    order_id=order_id,
                    token_id=resting["token_id"],
                    market_id=resting["market_id"],
                    fill_price=fill_price,
                    fill_quantity=fill_qty,
                    outcome_name=resting["outcome_name"] or "",
                )
                conn = self._conn()
                continue

            # Check if it's a TP SELL
            tp = conn.execute(
                "SELECT * FROM tp_orders WHERE order_id=? AND status='live'",
                (order_id,),
            ).fetchone()
            if tp:
                fill_price = float(order.get("price", tp["sell_price"]))
                fill_qty = float(order.get("size_matched", tp["sell_quantity"]))
                conn.close()
                self.om.on_tp_filled(order_id, fill_price, fill_qty)
                conn = self._conn()

        conn.close()

    def _check_fills_dry_run(self) -> None:
        """
        Simulate fills by checking current orderbook prices against our orders.

        BUY fill: current best_ask <= our bid price
        SELL fill: current best_bid >= our sell price
        """
        conn = self._conn()

        # Check resting BUY orders
        resting_live = conn.execute(
            "SELECT * FROM resting_orders WHERE status='live'"
        ).fetchall()

        for order in resting_live:
            token_id = order["token_id"]
            bid_price = float(order["price"])
            order_size = float(order["size"])
            filled_so_far = float(order["filled_quantity"] or 0)
            remaining = order_size - filled_so_far
            if remaining < 0.0001:
                continue  # shouldn't happen but guard anyway

            try:
                book = get_orderbook(token_id)
            except Exception:
                continue

            best_ask = book.best_ask
            if best_ask is None or best_ask > bid_price:
                continue

            available_depth = book.ask_depth_at(bid_price)
            this_fill = min(remaining, available_depth)
            if this_fill < 0.0001:
                continue  # no real depth at this level — skip

            new_filled = filled_so_far + this_fill

            if new_filled >= order_size * 0.99:
                # Fully filled — create position and TP orders
                self.clob.paper_simulate_fill(order["order_id"], best_ask, fill_size=this_fill)
                conn.close()
                self.om.on_entry_filled(
                    order_id=order["order_id"],
                    token_id=token_id,
                    market_id=order["market_id"],
                    fill_price=bid_price,
                    fill_quantity=order_size,  # always use full intended size
                    outcome_name=order["outcome_name"] or "",
                )
                logger.info(
                    f"[DRY] BUY fully filled: {order['order_id'][:8]} "
                    f"@ ${bid_price:.5f} qty={order_size:.2f}"
                )
                conn = self._conn()
            else:
                # Partial fill — accumulate, keep order live
                conn.execute(
                    "UPDATE resting_orders SET filled_quantity=? WHERE order_id=?",
                    (new_filled, order["order_id"]),
                )
                conn.commit()
                logger.info(
                    f"[DRY] BUY partial fill: {order['order_id'][:8]} "
                    f"@ ${bid_price:.5f} filled={new_filled:.2f}/{order_size:.2f} "
                    f"({100*new_filled/order_size:.0f}%)"
                )

        # Check TP SELL orders
        tp_live = conn.execute(
            "SELECT * FROM tp_orders WHERE status='live'"
        ).fetchall()

        for tp in tp_live:
            token_id = tp["token_id"]
            sell_price = float(tp["sell_price"])
            try:
                book = get_orderbook(token_id)
            except Exception:
                continue

            best_bid = book.best_bid
            if best_bid is not None and best_bid >= sell_price:
                # Cap fill quantity at available bid depth — same realism principle as BUY side.
                available_bid_depth = book.bid_depth_at(sell_price)
                fill_qty = min(float(tp["sell_quantity"]), available_bid_depth)
                if fill_qty < 0.0001:
                    continue  # no real bid depth at sell level — skip
                self.clob.paper_simulate_fill(tp["order_id"], best_bid, fill_size=fill_qty)
                conn.close()
                self.om.on_tp_filled(tp["order_id"], sell_price, fill_qty)
                logger.info(
                    f"[DRY] Simulated SELL fill: {tp['order_id'][:8]} "
                    f"{tp['label']} @ ${sell_price:.5f} qty={fill_qty:.2f} "
                    f"(bid=${best_bid:.5f} depth={available_bid_depth:.2f})"
                )
                conn = self._conn()

        conn.close()

    # ── Resolution detection ──────────────────────────────────────────────────

    def _check_resolutions(self) -> None:
        """
        Check if any markets with open positions have resolved.
        Uses Gamma API market object: closed=true + outcomePrices.
        """
        conn = self._conn()
        open_positions = conn.execute(
            "SELECT DISTINCT market_id, token_id FROM positions WHERE status='open'"
        ).fetchall()
        conn.close()

        for row in open_positions:
            market_id = row["market_id"]
            token_id = row["token_id"]

            try:
                market = fetch_market(market_id)
            except Exception as e:
                logger.debug(f"fetch_market({market_id}) failed: {e}")
                continue

            if market is None:
                # Market disappeared from API — probably closed
                logger.info(f"Market {market_id} no longer found — treating as resolved (loser)")
                self.om.on_market_resolved(token_id, is_winner=False)
                continue

            # Market still active
            if market.hours_to_close is not None and market.hours_to_close > 0:
                continue

            # Market is closed — determine winner from token prices
            import json
            try:
                resp = requests.get(
                    f"{GAMMA_BASE}/markets/{market_id}",
                    timeout=15,
                )
                resp.raise_for_status()
                raw = resp.json()
                if isinstance(raw, list):
                    raw = raw[0] if raw else {}

                outcome_prices = raw.get("outcomePrices", "[]")
                if isinstance(outcome_prices, str):
                    outcome_prices = json.loads(outcome_prices)

                token_ids_raw = raw.get("clobTokenIds", "[]")
                if isinstance(token_ids_raw, str):
                    token_ids_raw = json.loads(token_ids_raw)

                is_winner = False
                for i, tid in enumerate(token_ids_raw):
                    if str(tid) == token_id:
                        try:
                            is_winner = float(outcome_prices[i]) >= 0.99
                        except (IndexError, ValueError):
                            pass
                        break

                self.om.on_market_resolved(token_id, is_winner=is_winner)
                logger.info(
                    f"Market resolved: {market_id} token={token_id[:16]} winner={is_winner}"
                )
            except Exception as e:
                logger.warning(f"Resolution check failed for {market_id}: {e}")

    def get_stats(self) -> dict:
        """Return summary statistics for logging."""
        conn = self._conn()
        open_pos = conn.execute(
            "SELECT COUNT(*) FROM positions WHERE status='open'"
        ).fetchone()[0]
        live_resting = conn.execute(
            "SELECT COUNT(*) FROM resting_orders WHERE status='live'"
        ).fetchone()[0]
        live_tp = conn.execute(
            "SELECT COUNT(*) FROM tp_orders WHERE status='live'"
        ).fetchone()[0]
        resolved = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(realized_pnl), 0) FROM positions WHERE status='resolved'"
        ).fetchone()
        conn.close()
        return {
            "open_positions": open_pos,
            "live_resting_bids": live_resting,
            "live_tp_orders": live_tp,
            "resolved_positions": resolved[0],
            "total_realized_pnl": round(float(resolved[1]), 6),
        }
