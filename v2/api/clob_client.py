"""
CLOB API Client — orderbook queries and order placement.

Wraps py-clob-client for order execution.
In dry_run mode: logs orders to paper_trades.db instead of submitting.

Authentication: requires POLY_PRIVATE_KEY in env.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

CLOB_BASE = "https://clob.polymarket.com"
DEFAULT_TIMEOUT = 15

# Polygon chain ID
POLYGON_CHAIN_ID = 137


@dataclass
class OrderbookLevel:
    price: float
    size: float


@dataclass
class Orderbook:
    token_id: str
    bids: list[OrderbookLevel]   # sorted desc by price
    asks: list[OrderbookLevel]   # sorted asc by price
    best_bid: Optional[float]
    best_ask: Optional[float]

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None

    def bid_depth_at(self, price: float) -> float:
        """Total bid size available at or above given price."""
        return sum(lv.size for lv in self.bids if lv.price >= price)

    def ask_depth_at(self, price: float) -> float:
        """Total ask size (tokens for sale) at or below given price."""
        return sum(lv.size for lv in self.asks if lv.price <= price)


@dataclass
class OrderResult:
    order_id: str
    status: str       # "live" | "matched" | "error"
    token_id: str
    side: str         # "BUY" | "SELL"
    price: float
    size: float
    filled_size: float = 0.0
    error: str = ""


def get_orderbook(token_id: str) -> Orderbook:
    """Public endpoint — no auth required."""
    resp = requests.get(
        f"{CLOB_BASE}/book",
        params={"token_id": token_id},
        timeout=DEFAULT_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()

    def _parse_levels(raw_levels: list) -> list[OrderbookLevel]:
        levels = []
        for lv in (raw_levels or []):
            try:
                levels.append(OrderbookLevel(
                    price=float(lv["price"]),
                    size=float(lv["size"]),
                ))
            except (KeyError, ValueError, TypeError):
                continue
        return levels

    bids = _parse_levels(data.get("bids", []))
    asks = _parse_levels(data.get("asks", []))
    bids.sort(key=lambda x: x.price, reverse=True)
    asks.sort(key=lambda x: x.price)

    best_bid = bids[0].price if bids else None
    best_ask = asks[0].price if asks else None

    return Orderbook(
        token_id=token_id,
        bids=bids,
        asks=asks,
        best_bid=best_bid,
        best_ask=best_ask,
    )


class ClobClient:
    """
    Authenticated CLOB client.

    In dry_run=True mode: records all orders in paper_trades.db, returns fake OrderResults.
    In dry_run=False mode: uses py-clob-client to submit signed orders.
    """

    def __init__(self, private_key: str, dry_run: bool = True, paper_db_path: Optional[Path] = None):
        self.private_key = private_key
        self.dry_run = dry_run
        self._clob = None   # lazy-init real client

        if paper_db_path is None:
            from ..utils.paths import DATA_DIR
            paper_db_path = DATA_DIR / "paper_trades.db"
        self.paper_db_path = paper_db_path

        if dry_run:
            self._init_paper_db()
        else:
            self._init_real_client()

    def _init_paper_db(self) -> None:
        self.paper_db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.paper_db_path))
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS paper_orders (
                order_id     TEXT PRIMARY KEY,
                token_id     TEXT NOT NULL,
                side         TEXT NOT NULL,
                price        REAL NOT NULL,
                size         REAL NOT NULL,
                filled_size  REAL NOT NULL DEFAULT 0,
                status       TEXT NOT NULL DEFAULT 'live',
                created_at   INTEGER NOT NULL,
                updated_at   INTEGER NOT NULL,
                label        TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_paper_orders_token ON paper_orders(token_id);
            CREATE INDEX IF NOT EXISTS idx_paper_orders_status ON paper_orders(status);

            CREATE TABLE IF NOT EXISTS paper_pnl (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id     TEXT NOT NULL,
                token_id     TEXT NOT NULL,
                entry_price  REAL NOT NULL,
                exit_price   REAL NOT NULL,
                size         REAL NOT NULL,
                pnl_usdc     REAL NOT NULL,
                recorded_at  INTEGER NOT NULL
            );
        """)
        conn.commit()
        conn.close()

    def _init_real_client(self) -> None:
        try:
            from py_clob_client.client import ClobClient as _RealClobClient
            from py_clob_client.constants import POLYGON
            self._clob = _RealClobClient(
                host=CLOB_BASE,
                key=self.private_key,
                chain_id=POLYGON,
            )
            self._clob.set_api_creds(self._clob.create_or_derive_api_creds())
        except ImportError:
            raise RuntimeError(
                "py-clob-client not installed. Run: pip install py-clob-client"
            )

    # ── Order placement ───────────────────────────────────────────────────────

    def place_limit_order(
        self,
        token_id: str,
        side: str,           # "BUY" | "SELL"
        price: float,
        size: float,         # in USDC (for BUY) or tokens (for SELL)
        label: str = "",
    ) -> OrderResult:
        """Place a GTC limit order. Returns OrderResult."""
        if self.dry_run:
            return self._paper_place(token_id, side, price, size, label)
        return self._real_place(token_id, side, price, size)

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order. Returns True on success."""
        if self.dry_run:
            return self._paper_cancel(order_id)
        return self._real_cancel(order_id)

    def get_open_orders(self, token_id: Optional[str] = None) -> list[dict]:
        """Return list of open orders (status=live)."""
        if self.dry_run:
            return self._paper_get_orders(status="live", token_id=token_id)
        return self._real_get_orders(token_id=token_id)

    def get_all_orders(self, token_id: Optional[str] = None) -> list[dict]:
        """Return all orders including filled/cancelled."""
        if self.dry_run:
            return self._paper_get_orders(status=None, token_id=token_id)
        return self._real_get_orders(token_id=token_id, active=False)

    # ── Paper trading ─────────────────────────────────────────────────────────

    def _paper_place(self, token_id: str, side: str, price: float, size: float, label: str) -> OrderResult:
        import uuid
        order_id = str(uuid.uuid4())
        now = int(time.time())
        conn = sqlite3.connect(str(self.paper_db_path))
        conn.execute(
            "INSERT INTO paper_orders (order_id, token_id, side, price, size, status, created_at, updated_at, label) "
            "VALUES (?, ?, ?, ?, ?, 'live', ?, ?, ?)",
            (order_id, token_id, side, price, size, now, now, label),
        )
        conn.commit()
        conn.close()
        return OrderResult(
            order_id=order_id,
            status="live",
            token_id=token_id,
            side=side,
            price=price,
            size=size,
        )

    def _paper_cancel(self, order_id: str) -> bool:
        now = int(time.time())
        conn = sqlite3.connect(str(self.paper_db_path))
        cur = conn.execute(
            "UPDATE paper_orders SET status='cancelled', updated_at=? WHERE order_id=? AND status='live'",
            (now, order_id),
        )
        changed = cur.rowcount > 0
        conn.commit()
        conn.close()
        return changed

    def _paper_get_orders(self, status: Optional[str], token_id: Optional[str]) -> list[dict]:
        conn = sqlite3.connect(str(self.paper_db_path))
        conn.row_factory = sqlite3.Row
        clauses, params = [], []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if token_id:
            clauses.append("token_id = ?")
            params.append(token_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = conn.execute(f"SELECT * FROM paper_orders {where}", params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def paper_simulate_fill(
        self,
        order_id: str,
        fill_price: float,
        fill_size: Optional[float] = None,
    ) -> None:
        """
        Called by FillEmulator: mark paper order as matched.
        Only fills if fill_price crosses the order price.

        fill_size: actual filled quantity (depth-capped). If None, uses full order size.
        This keeps paper_orders.filled_size consistent with what on_entry_filled /
        on_tp_filled receive, preventing position-state divergence.
        """
        now = int(time.time())
        conn = sqlite3.connect(str(self.paper_db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM paper_orders WHERE order_id = ? AND status = 'live'",
            (order_id,),
        ).fetchone()
        if not row:
            conn.close()
            return
        # BUY fills when ask drops to our price; SELL fills when bid rises to our price
        side = row["side"]
        order_price = row["price"]
        should_fill = (
            (side == "BUY" and fill_price <= order_price)
            or (side == "SELL" and fill_price >= order_price)
        )
        if should_fill:
            actual_filled = fill_size if fill_size is not None else float(row["size"])
            conn.execute(
                "UPDATE paper_orders SET status='matched', filled_size=?, updated_at=? WHERE order_id=?",
                (actual_filled, now, order_id),
            )
        conn.commit()
        conn.close()

    def paper_record_resolution(self, token_id: str, resolved_at: float) -> None:
        """
        Mark all live SELL orders for this token as resolved.
        resolved_at: $1.0 if winner, $0.0 if loser
        """
        now = int(time.time())
        conn = sqlite3.connect(str(self.paper_db_path))
        conn.row_factory = sqlite3.Row
        # Mark open SELL orders
        conn.execute(
            "UPDATE paper_orders SET status='resolved', updated_at=? "
            "WHERE token_id=? AND side='SELL' AND status='live'",
            (now, token_id),
        )
        # Record PnL for BUY positions still open
        buys = conn.execute(
            "SELECT * FROM paper_orders WHERE token_id=? AND side='BUY' AND status='matched'",
            (token_id,),
        ).fetchall()
        for buy in buys:
            # Use filled_size (actual quantity received) not order size
            filled = buy["filled_size"] if buy["filled_size"] else buy["size"]
            pnl = (resolved_at - buy["price"]) * filled
            conn.execute(
                "INSERT INTO paper_pnl (order_id, token_id, entry_price, exit_price, size, pnl_usdc, recorded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (buy["order_id"], token_id, buy["price"], resolved_at, filled, pnl, now),
            )
        conn.commit()
        conn.close()

    # ── Real trading ──────────────────────────────────────────────────────────

    def _real_place(self, token_id: str, side: str, price: float, size: float) -> OrderResult:
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            side_enum = "BUY" if side == "BUY" else "SELL"
            result = self._clob.create_and_post_order(OrderArgs(
                price=price,
                size=size,
                side=side_enum,
                token_id=token_id,
            ))
            order_id = result.get("orderID") or result.get("id") or "unknown"
            status = result.get("status", "live")
            return OrderResult(
                order_id=str(order_id),
                status=status,
                token_id=token_id,
                side=side,
                price=price,
                size=size,
            )
        except Exception as e:
            return OrderResult(
                order_id="",
                status="error",
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                error=str(e),
            )

    def _real_cancel(self, order_id: str) -> bool:
        try:
            self._clob.cancel(order_id)
            return True
        except Exception:
            return False

    def _real_get_orders(self, token_id: Optional[str] = None, active: bool = True) -> list[dict]:
        try:
            result = self._clob.get_orders(
                **({"asset_id": token_id} if token_id else {})
            )
            orders = result if isinstance(result, list) else result.get("data", [])
            if active:
                orders = [o for o in orders if o.get("status") in ("ORDER_STATUS_LIVE", "live")]
            return orders
        except Exception:
            return []
