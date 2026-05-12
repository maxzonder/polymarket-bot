from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from ..core.events import (
    BookUpdate,
    MarketResolvedWithInventory,
    MarketStateUpdate,
    NormalizedEvent,
    OrderAccepted,
    OrderCanceled,
    OrderFilled,
    OrderIntentEvent,
    OrderRejected,
    SkipDecisionEvent,
    SpotPriceUpdate,
    TimerEvent,
    TradeTick,
)
from ..core.models import OrderIntent
from ..core.order_state import OrderState


NON_TERMINAL_ORDER_STATES = (
    OrderState.INTENT.value,
    OrderState.PENDING_SEND.value,
    OrderState.ACCEPTED.value,
    OrderState.PARTIALLY_FILLED.value,
    OrderState.CANCEL_REQUESTED.value,
    OrderState.REPLACE_REQUESTED.value,
    OrderState.UNKNOWN.value,
)


@dataclass(frozen=True)
class RunContext:
    run_id: str
    strategy_id: str
    mode: str = "replay"
    config_hash: str = "dev"
    started_at: str = field(default_factory=lambda: utc_now_iso())
    git_sha: str | None = None
    notes: str | None = None


class RuntimeStore(Protocol):
    @property
    def current_run_id(self) -> str:
        ...

    def append_event(self, event: NormalizedEvent) -> None:
        ...

    def upsert_market_state(self, event: MarketStateUpdate) -> None:
        ...

    def record_first_touch(self, *, market_id: str, token_id: str, level: float, event_time_ms: int) -> None:
        ...

    def persist_intent(self, intent: OrderIntent) -> None:
        ...

    def append_event_log(self, event: NormalizedEvent, *, order_id: str | None = None) -> None:
        ...

    def insert_order(
        self,
        *,
        order_id: str,
        market_id: str,
        token_id: str,
        side: str,
        price: float | None,
        size: float | None,
        state: OrderState | str,
        client_order_id: str | None,
        venue_order_id: str | None = None,
        intent_created_at_ms: int,
        last_state_change_at_ms: int,
        remaining_size: float | None = None,
        parent_order_id: str | None = None,
        venue_order_status: str | None = None,
        reconciliation_required: bool = False,
    ) -> None:
        ...

    def update_order_state(
        self,
        *,
        order_id: str,
        state: OrderState | str,
        event_time_ms: int,
        venue_order_id: str | None = None,
        venue_order_status: str | None = None,
        cumulative_filled_size: float | None = None,
        remaining_size: float | None = None,
        reconciliation_required: bool | None = None,
        reject_code: str | None = None,
        reject_reason: str | None = None,
    ) -> None:
        ...

    def insert_fill(
        self,
        *,
        fill_id: str,
        order_id: str,
        market_id: str,
        token_id: str,
        price: float,
        size: float,
        filled_at_ms: int,
        source: str,
        fee_paid_usdc: float | None = None,
        liquidity_role: str | None = None,
        venue_fill_id: str | None = None,
    ) -> None:
        ...

    def load_fill(self, fill_id: str) -> dict[str, Any] | None:
        ...

    def load_order(self, order_id: str) -> dict[str, Any] | None:
        ...

    def load_order_by_client_order_id(self, client_order_id: str) -> dict[str, Any] | None:
        ...

    def load_order_by_venue_order_id(self, venue_order_id: str) -> dict[str, Any] | None:
        ...

    def load_non_terminal_orders(self) -> list[dict[str, Any]]:
        ...

    def load_all_orders(self) -> list[dict[str, Any]]:
        ...

    def load_fills(self) -> list[dict[str, Any]]:
        ...

    def has_unknown_order_for_market(self, market_id: str) -> bool:
        ...

    def load_latest_market_state(self, market_id: str) -> MarketStateUpdate | None:
        ...


@dataclass
class InMemoryIntentStore:
    intents: list[OrderIntent] = field(default_factory=list)
    events: list[NormalizedEvent] = field(default_factory=list)
    market_updates: list[MarketStateUpdate] = field(default_factory=list)
    market_state_by_id: dict[str, MarketStateUpdate] = field(default_factory=dict)
    strategy_state: dict[tuple[str, str | None, str], dict[str, Any]] = field(default_factory=dict)
    orders: dict[str, dict[str, Any]] = field(default_factory=dict)
    fills: dict[str, dict[str, Any]] = field(default_factory=dict)
    run_id: str = "run_in_memory"

    @property
    def current_run_id(self) -> str:
        return self.run_id

    def append_event(self, event: NormalizedEvent) -> None:
        self.events.append(event)

    def append_event_log(self, event: NormalizedEvent, *, order_id: str | None = None) -> None:
        self.events.append(event)

    def upsert_market_state(self, event: MarketStateUpdate) -> None:
        self.market_updates.append(event)
        self.market_state_by_id[event.market_id] = event

    def record_first_touch(self, *, market_id: str, token_id: str, level: float, event_time_ms: int) -> None:
        self.strategy_state[(market_id, token_id, f"first_touch_fired:{level:.2f}")] = {
            "event_time": iso_from_ms(event_time_ms),
            "level": level,
        }

    def persist_intent(self, intent: OrderIntent) -> None:
        self.intents.append(intent)
        size = intent.size_shares if intent.size_shares is not None else (intent.notional_usdc / intent.entry_price if intent.entry_price > 0 else None)
        self.insert_order(
            order_id=intent.intent_id,
            market_id=intent.market_id,
            token_id=intent.token_id,
            side=str(intent.side),
            price=intent.entry_price,
            size=size,
            state=OrderState.INTENT,
            client_order_id=intent.intent_id,
            intent_created_at_ms=intent.event_time_ms,
            last_state_change_at_ms=intent.event_time_ms,
            remaining_size=size,
            time_in_force=intent.time_in_force,
            post_only=intent.post_only,
        )

    def insert_order(
        self,
        *,
        order_id: str,
        market_id: str,
        token_id: str,
        side: str,
        price: float | None,
        size: float | None,
        state: OrderState | str,
        client_order_id: str | None,
        venue_order_id: str | None = None,
        intent_created_at_ms: int,
        last_state_change_at_ms: int,
        remaining_size: float | None = None,
        parent_order_id: str | None = None,
        venue_order_status: str | None = None,
        time_in_force: str | None = None,
        post_only: bool | None = None,
        reconciliation_required: bool = False,
    ) -> None:
        state_value = state.value if isinstance(state, OrderState) else str(state)
        self.orders[order_id] = {
            "order_id": order_id,
            "run_id": self.run_id,
            "market_id": market_id,
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
            "state": state_value,
            "client_order_id": client_order_id,
            "venue_order_id": venue_order_id,
            "parent_order_id": parent_order_id,
            "time_in_force": time_in_force,
            "post_only": post_only,
            "intent_created_at": iso_from_ms(intent_created_at_ms),
            "last_state_change_at": iso_from_ms(last_state_change_at_ms),
            "venue_order_status": venue_order_status,
            "cumulative_filled_size": 0.0,
            "remaining_size": remaining_size,
            "reconciliation_required": reconciliation_required,
            "last_reject_code": None,
            "last_reject_reason": None,
        }

    def update_order_state(
        self,
        *,
        order_id: str,
        state: OrderState | str,
        event_time_ms: int,
        venue_order_id: str | None = None,
        venue_order_status: str | None = None,
        cumulative_filled_size: float | None = None,
        remaining_size: float | None = None,
        reconciliation_required: bool | None = None,
        reject_code: str | None = None,
        reject_reason: str | None = None,
    ) -> None:
        row = self.orders[order_id]
        row["state"] = state.value if isinstance(state, OrderState) else str(state)
        row["last_state_change_at"] = iso_from_ms(event_time_ms)
        if venue_order_id is not None:
            row["venue_order_id"] = venue_order_id
        if venue_order_status is not None:
            row["venue_order_status"] = venue_order_status
        if cumulative_filled_size is not None:
            row["cumulative_filled_size"] = cumulative_filled_size
        if remaining_size is not None:
            row["remaining_size"] = remaining_size
        if reconciliation_required is not None:
            row["reconciliation_required"] = reconciliation_required
        if reject_code is not None:
            row["last_reject_code"] = reject_code
        if reject_reason is not None:
            row["last_reject_reason"] = reject_reason

    def insert_fill(
        self,
        *,
        fill_id: str,
        order_id: str,
        market_id: str,
        token_id: str,
        price: float,
        size: float,
        filled_at_ms: int,
        source: str,
        fee_paid_usdc: float | None = None,
        liquidity_role: str | None = None,
        venue_fill_id: str | None = None,
    ) -> None:
        self.fills.setdefault(fill_id, {
            "fill_id": fill_id,
            "order_id": order_id,
            "run_id": self.run_id,
            "market_id": market_id,
            "token_id": token_id,
            "price": price,
            "size": size,
            "fee_paid_usdc": fee_paid_usdc,
            "liquidity_role": liquidity_role,
            "filled_at": iso_from_ms(filled_at_ms),
            "source": source,
            "venue_fill_id": venue_fill_id,
        })

    def load_fill(self, fill_id: str) -> dict[str, Any] | None:
        row = self.fills.get(fill_id)
        return dict(row) if row is not None else None

    def load_order(self, order_id: str) -> dict[str, Any] | None:
        row = self.orders.get(order_id)
        return dict(row) if row is not None else None

    def load_non_terminal_orders(self) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.orders.values()
            if row["state"] in NON_TERMINAL_ORDER_STATES
        ]

    def load_all_orders(self) -> list[dict[str, Any]]:
        rows = [dict(row) for row in self.orders.values()]
        rows.sort(key=lambda row: (str(row.get("last_state_change_at") or row.get("intent_created_at") or ""), str(row.get("order_id") or "")))
        return rows

    def load_fills(self) -> list[dict[str, Any]]:
        rows = [dict(row) for row in self.fills.values()]
        rows.sort(key=lambda row: (str(row.get("filled_at") or ""), str(row.get("fill_id") or "")))
        return rows

    def has_unknown_order_for_market(self, market_id: str) -> bool:
        return any(
            row["market_id"] == market_id and row["state"] == OrderState.UNKNOWN.value
            for row in self.orders.values()
        )

    def load_latest_market_state(self, market_id: str) -> MarketStateUpdate | None:
        return self.market_state_by_id.get(market_id)

    def load_order_by_client_order_id(self, client_order_id: str) -> dict[str, Any] | None:
        for row in reversed(list(self.orders.values())):
            if row.get("client_order_id") == client_order_id:
                return dict(row)
        return None

    def load_order_by_venue_order_id(self, venue_order_id: str) -> dict[str, Any] | None:
        for row in reversed(list(self.orders.values())):
            if row.get("venue_order_id") == venue_order_id:
                return dict(row)
        return None


class SQLiteRuntimeStore:
    def __init__(self, path: str | Path, *, run: RunContext):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.run = run
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self._closed = False
        self._latest_market_state_by_id: dict[str, MarketStateUpdate] = {}
        self._initialize_schema()
        self._ensure_run()
        row = self.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM events_log WHERE run_id = ?",
            (self.run.run_id,),
        ).fetchone()
        self._next_seq_value = int(row["max_seq"] or 0) + 1

    @property
    def current_run_id(self) -> str:
        return self.run.run_id

    def close(self) -> None:
        if self._closed:
            return
        self._finalize_run()
        self.conn.close()
        self._closed = True

    def append_event(self, event: NormalizedEvent) -> None:
        self.append_event_log(event)

    def append_event_log(self, event: NormalizedEvent, *, order_id: str | None = None) -> None:
        market_id, token_id = _event_market_token(event)
        if market_id is not None:
            self._ensure_market_stub(market_id)
        payload = normalize_event_payload(event)
        self.conn.execute(
            """
            INSERT INTO events_log (
                run_id, seq, event_type, event_time, ingest_time, source,
                market_id, token_id, order_id, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.run.run_id,
                self._next_seq(),
                payload["event_type"],
                payload["event_time"],
                payload["ingest_time"],
                payload["source"],
                market_id,
                token_id,
                order_id,
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()

    def upsert_market_state(self, event: MarketStateUpdate) -> None:
        self._latest_market_state_by_id[event.market_id] = event
        market_status = "active" if event.is_active else "closed"
        fee_info_json = json.dumps(asdict(event.fee_info), sort_keys=True) if event.fee_info is not None else None
        self.conn.execute(
            """
            INSERT INTO markets (
                market_id,
                condition_id,
                question,
                market_status,
                duration_seconds_snapshot,
                start_time_latest,
                end_time_latest,
                fee_rate_bps_latest,
                fee_fetched_at,
                fees_enabled,
                fee_info_json,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(market_id) DO UPDATE SET
                condition_id = excluded.condition_id,
                question = excluded.question,
                market_status = excluded.market_status,
                duration_seconds_snapshot = excluded.duration_seconds_snapshot,
                start_time_latest = excluded.start_time_latest,
                end_time_latest = excluded.end_time_latest,
                fee_rate_bps_latest = excluded.fee_rate_bps_latest,
                fee_fetched_at = excluded.fee_fetched_at,
                fees_enabled = excluded.fees_enabled,
                fee_info_json = excluded.fee_info_json,
                updated_at = excluded.updated_at
            """,
            (
                event.market_id,
                event.condition_id,
                event.question,
                market_status,
                max(0, int((event.end_time_ms - event.start_time_ms) / 1000)),
                iso_from_ms(event.start_time_ms),
                iso_from_ms(event.end_time_ms),
                event.fee_rate_bps,
                iso_from_ms(event.ingest_time_ms) if event.fee_rate_bps is not None else None,
                1 if event.fee_rate_bps is not None else None,
                fee_info_json,
                iso_from_ms(event.ingest_time_ms),
            ),
        )
        self.conn.commit()

    def record_first_touch(self, *, market_id: str, token_id: str, level: float, event_time_ms: int) -> None:
        self._ensure_market_stub(market_id)
        self.conn.execute(
            """
            INSERT INTO strategy_state (
                run_id, strategy_id, market_id, token_id, state_key, state_value_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, strategy_id, market_id, token_id, state_key) DO UPDATE SET
                state_value_json = excluded.state_value_json,
                updated_at = excluded.updated_at
            """,
            (
                self.run.run_id,
                self.run.strategy_id,
                market_id,
                token_id,
                f"first_touch_fired:{level:.2f}",
                json.dumps({"level": level, "event_time": iso_from_ms(event_time_ms)}, sort_keys=True),
                iso_from_ms(event_time_ms),
            ),
        )
        self.conn.commit()

    def persist_intent(self, intent: OrderIntent) -> None:
        size = intent.size_shares if intent.size_shares is not None else (intent.notional_usdc / intent.entry_price if intent.entry_price > 0 else None)
        self.insert_order(
            order_id=intent.intent_id,
            market_id=intent.market_id,
            token_id=intent.token_id,
            side=str(intent.side),
            price=intent.entry_price,
            size=size,
            state=OrderState.INTENT,
            client_order_id=intent.intent_id,
            intent_created_at_ms=intent.event_time_ms,
            last_state_change_at_ms=intent.event_time_ms,
            remaining_size=size,
            time_in_force=intent.time_in_force,
            post_only=intent.post_only,
        )

    def insert_order(
        self,
        *,
        order_id: str,
        market_id: str,
        token_id: str,
        side: str,
        price: float | None,
        size: float | None,
        state: OrderState | str,
        client_order_id: str | None,
        venue_order_id: str | None = None,
        intent_created_at_ms: int,
        last_state_change_at_ms: int,
        remaining_size: float | None = None,
        parent_order_id: str | None = None,
        venue_order_status: str | None = None,
        time_in_force: str | None = None,
        post_only: bool | None = None,
        reconciliation_required: bool = False,
    ) -> None:
        self._ensure_market_stub(market_id)
        state_value = state.value if isinstance(state, OrderState) else str(state)
        self.conn.execute(
            """
            INSERT INTO orders (
                order_id,
                run_id,
                market_id,
                token_id,
                side,
                price,
                size,
                state,
                client_order_id,
                venue_order_id,
                parent_order_id,
                time_in_force,
                post_only,
                intent_created_at,
                last_state_change_at,
                venue_order_status,
                cumulative_filled_size,
                remaining_size,
                reconciliation_required
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                side = excluded.side,
                price = excluded.price,
                size = excluded.size,
                state = excluded.state,
                client_order_id = COALESCE(excluded.client_order_id, orders.client_order_id),
                venue_order_id = COALESCE(excluded.venue_order_id, orders.venue_order_id),
                parent_order_id = excluded.parent_order_id,
                time_in_force = COALESCE(excluded.time_in_force, orders.time_in_force),
                post_only = COALESCE(excluded.post_only, orders.post_only),
                last_state_change_at = excluded.last_state_change_at,
                venue_order_status = COALESCE(excluded.venue_order_status, orders.venue_order_status),
                remaining_size = excluded.remaining_size,
                reconciliation_required = excluded.reconciliation_required
            """,
            (
                order_id,
                self.run.run_id,
                market_id,
                token_id,
                side,
                price,
                size,
                state_value,
                client_order_id,
                venue_order_id,
                parent_order_id,
                time_in_force,
                int(post_only) if post_only is not None else None,
                iso_from_ms(intent_created_at_ms),
                iso_from_ms(last_state_change_at_ms),
                venue_order_status,
                remaining_size,
                int(reconciliation_required),
            ),
        )
        self.conn.commit()

    def update_order_state(
        self,
        *,
        order_id: str,
        state: OrderState | str,
        event_time_ms: int,
        venue_order_id: str | None = None,
        venue_order_status: str | None = None,
        cumulative_filled_size: float | None = None,
        remaining_size: float | None = None,
        reconciliation_required: bool | None = None,
        reject_code: str | None = None,
        reject_reason: str | None = None,
    ) -> None:
        state_value = state.value if isinstance(state, OrderState) else str(state)
        self.conn.execute(
            """
            UPDATE orders
            SET state = ?,
                last_state_change_at = ?,
                venue_order_id = COALESCE(?, venue_order_id),
                venue_order_status = COALESCE(?, venue_order_status),
                cumulative_filled_size = COALESCE(?, cumulative_filled_size),
                remaining_size = COALESCE(?, remaining_size),
                reconciliation_required = COALESCE(?, reconciliation_required),
                last_reject_code = COALESCE(?, last_reject_code),
                last_reject_reason = COALESCE(?, last_reject_reason)
            WHERE order_id = ? AND run_id = ?
            """,
            (
                state_value,
                iso_from_ms(event_time_ms),
                venue_order_id,
                venue_order_status,
                cumulative_filled_size,
                remaining_size,
                None if reconciliation_required is None else int(reconciliation_required),
                reject_code,
                reject_reason,
                order_id,
                self.run.run_id,
            ),
        )
        self.conn.commit()

    def insert_fill(
        self,
        *,
        fill_id: str,
        order_id: str,
        market_id: str,
        token_id: str,
        price: float,
        size: float,
        filled_at_ms: int,
        source: str,
        fee_paid_usdc: float | None = None,
        liquidity_role: str | None = None,
        venue_fill_id: str | None = None,
    ) -> None:
        self._ensure_market_stub(market_id)
        self.conn.execute(
            """
            INSERT OR IGNORE INTO fills (
                fill_id, order_id, run_id, market_id, token_id, price, size,
                fee_paid_usdc, liquidity_role, filled_at, source, venue_fill_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fill_id,
                order_id,
                self.run.run_id,
                market_id,
                token_id,
                price,
                size,
                fee_paid_usdc,
                liquidity_role,
                iso_from_ms(filled_at_ms),
                source,
                venue_fill_id,
            ),
        )
        self.conn.commit()

    def load_fill(self, fill_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM fills WHERE run_id = ? AND fill_id = ? LIMIT 1",
            (self.run.run_id, fill_id),
        ).fetchone()
        return dict(row) if row is not None else None

    def load_non_terminal_orders(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            f"""
            SELECT * FROM orders
            WHERE run_id = ? AND state IN ({','.join('?' for _ in NON_TERMINAL_ORDER_STATES)})
            ORDER BY intent_created_at ASC, order_id ASC
            """,
            (self.run.run_id, *NON_TERMINAL_ORDER_STATES),
        ).fetchall()
        return [dict(row) for row in rows]

    def load_all_orders(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM orders
            WHERE run_id = ?
            ORDER BY last_state_change_at ASC, intent_created_at ASC, order_id ASC
            """,
            (self.run.run_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def load_fills(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM fills
            WHERE run_id = ?
            ORDER BY filled_at ASC, fill_id ASC
            """,
            (self.run.run_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def has_unknown_order_for_market(self, market_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM orders WHERE run_id = ? AND market_id = ? AND state = ? LIMIT 1",
            (self.run.run_id, market_id, OrderState.UNKNOWN.value),
        ).fetchone()
        return row is not None

    def load_order(self, order_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM orders WHERE run_id = ? AND order_id = ?",
            (self.run.run_id, order_id),
        ).fetchone()
        return dict(row) if row is not None else None

    def load_order_by_client_order_id(self, client_order_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM orders WHERE run_id = ? AND client_order_id = ? ORDER BY last_state_change_at DESC, order_id DESC LIMIT 1",
            (self.run.run_id, client_order_id),
        ).fetchone()
        return dict(row) if row is not None else None

    def load_order_by_venue_order_id(self, venue_order_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM orders WHERE run_id = ? AND venue_order_id = ? ORDER BY last_state_change_at DESC, order_id DESC LIMIT 1",
            (self.run.run_id, venue_order_id),
        ).fetchone()
        return dict(row) if row is not None else None

    def load_latest_market_state(self, market_id: str) -> MarketStateUpdate | None:
        return self._latest_market_state_by_id.get(market_id)

    def _initialize_schema(self) -> None:
        schema_path = Path(__file__).resolve().parent / "schema.sql"
        self._migrate_runs_schema()
        self._migrate_orders_schema(precreate=True)
        self.conn.executescript(schema_path.read_text())
        self._migrate_orders_schema(precreate=False)
        self._migrate_markets_schema()
        self.conn.commit()

    def _migrate_runs_schema(self) -> None:
        row = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='runs'"
        ).fetchone()
        if row is None or "'dry_run'" in row[0]:
            return  # table missing (will be created) or already migrated
        # Recreate runs with expanded mode CHECK; disable FK enforcement during rename
        self.conn.execute("PRAGMA foreign_keys=OFF")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS _runs_new (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                mode TEXT NOT NULL CHECK (mode IN ('replay', 'live', 'dry_run', 'synthetic')),
                strategy_id TEXT NOT NULL,
                git_sha TEXT,
                config_hash TEXT NOT NULL,
                notes TEXT
            );
            INSERT OR IGNORE INTO _runs_new SELECT * FROM runs;
            DROP TABLE runs;
            ALTER TABLE _runs_new RENAME TO runs;
        """)
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.commit()

    def _migrate_markets_schema(self) -> None:
        table_exists = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'markets'"
        ).fetchone() is not None
        if not table_exists:
            return
        columns = {
            str(row[1])
            for row in self.conn.execute("PRAGMA table_info(markets)").fetchall()
        }
        if "fee_info_json" not in columns:
            self.conn.execute("ALTER TABLE markets ADD COLUMN fee_info_json TEXT")

    def _migrate_orders_schema(self, *, precreate: bool) -> None:
        table_exists = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'orders'"
        ).fetchone() is not None
        if not table_exists:
            return
        columns = {
            str(row[1])
            for row in self.conn.execute("PRAGMA table_info(orders)").fetchall()
        }
        if "venue_order_id" not in columns:
            self.conn.execute("ALTER TABLE orders ADD COLUMN venue_order_id TEXT")
        if "time_in_force" not in columns:
            self.conn.execute("ALTER TABLE orders ADD COLUMN time_in_force TEXT")
        if "post_only" not in columns:
            self.conn.execute("ALTER TABLE orders ADD COLUMN post_only INTEGER")
        if precreate:
            return
        self.conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_venue_order_id
            ON orders(venue_order_id)
            WHERE venue_order_id IS NOT NULL
            """
        )

    def _ensure_run(self) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO runs (
                run_id, started_at, mode, strategy_id, git_sha, config_hash, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.run.run_id,
                self.run.started_at,
                self.run.mode,
                self.run.strategy_id,
                self.run.git_sha,
                self.run.config_hash,
                self.run.notes,
            ),
        )
        self.conn.commit()

    def _finalize_run(self) -> None:
        self.conn.execute(
            """
            UPDATE runs
            SET finished_at = COALESCE(finished_at, ?)
            WHERE run_id = ?
            """,
            (utc_now_iso(), self.run.run_id),
        )
        self.conn.commit()

    def _ensure_market_stub(self, market_id: str) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO markets (market_id, market_status, updated_at)
            VALUES (?, 'unknown', ?)
            """,
            (market_id, utc_now_iso()),
        )
        self.conn.commit()

    def _next_seq(self) -> int:
        seq = self._next_seq_value
        self._next_seq_value += 1
        return seq


def normalize_event_payload(event: NormalizedEvent) -> dict[str, Any]:
    if isinstance(event, BookUpdate):
        spread = None
        mid_price = None
        if event.best_bid is not None and event.best_ask is not None:
            spread = event.best_ask - event.best_bid
            mid_price = (event.best_ask + event.best_bid) / 2.0
        return {
            "event_type": "BookUpdate",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "source": event.source,
            "market_id": event.market_id,
            "token_id": event.token_id,
            "best_bid": event.best_bid,
            "best_ask": event.best_ask,
            "spread": spread,
            "mid_price": mid_price,
        }

    if isinstance(event, TradeTick):
        return {
            "event_type": "TradeTick",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "source": event.source,
            "market_id": event.market_id,
            "token_id": event.token_id,
            "price": event.price,
            "size": event.size,
            "trade_id": event.trade_id,
            "aggressor_side": event.aggressor_side,
            "venue_seq": event.venue_seq,
        }

    if isinstance(event, TimerEvent):
        return {
            "event_type": "TimerEvent",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "source": event.source,
            "market_id": event.market_id,
            "token_id": event.token_id,
            "timer_kind": event.timer_kind,
            "deadline_ms": event.deadline_ms,
            "payload": event.payload,
        }

    if isinstance(event, SpotPriceUpdate):
        return {
            "event_type": "SpotPriceUpdate",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "source": event.source,
            "asset_slug": event.asset_slug,
            "spot_price": event.spot_price,
            "bid": event.bid,
            "ask": event.ask,
            "staleness_ms": event.staleness_ms,
            "currency": event.currency,
            "venue": event.venue,
            "run_id": event.run_id,
        }

    if isinstance(event, OrderIntentEvent):
        return {
            "event_type": "OrderIntent",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "source": event.source,
            "order_id": event.order_id,
            "strategy_id": event.strategy_id,
            "market_id": event.market_id,
            "token_id": event.token_id,
            "level": event.level,
            "entry_price": event.entry_price,
            "notional_usdc": event.notional_usdc,
            "lifecycle_fraction": event.lifecycle_fraction,
            "reason": event.reason,
        }

    if isinstance(event, SkipDecisionEvent):
        return {
            "event_type": "SkipDecision",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "source": event.source,
            "reason": event.reason,
            "market_id": event.market_id,
            "token_id": event.token_id,
            "level": event.level,
            "details": event.details,
        }

    if isinstance(event, OrderAccepted):
        return {
            "event_type": "OrderAccepted",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "source": event.source,
            "order_id": event.order_id,
            "client_order_id": event.client_order_id,
            "market_id": event.market_id,
            "token_id": event.token_id,
            "side": event.side,
            "price": event.price,
            "size": event.size,
            "time_in_force": event.time_in_force,
            "post_only": event.post_only,
            "venue_status": event.venue_status,
        }

    if isinstance(event, OrderRejected):
        return {
            "event_type": "OrderRejected",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "source": event.source,
            "client_order_id": event.client_order_id,
            "market_id": event.market_id,
            "token_id": event.token_id,
            "side": event.side,
            "price": event.price,
            "size": event.size,
            "reject_reason_code": event.reject_reason_code,
            "reject_reason_text": event.reject_reason_text,
            "is_retryable": event.is_retryable,
        }

    if isinstance(event, OrderFilled):
        return {
            "event_type": "OrderFilled",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "source": event.source,
            "order_id": event.order_id,
            "client_order_id": event.client_order_id,
            "market_id": event.market_id,
            "token_id": event.token_id,
            "side": event.side,
            "fill_price": event.fill_price,
            "fill_size": event.fill_size,
            "cumulative_filled_size": event.cumulative_filled_size,
            "remaining_size": event.remaining_size,
            "fee_paid_usdc": event.fee_paid_usdc,
            "liquidity_role": event.liquidity_role,
            "venue_fill_id": event.venue_fill_id,
        }

    if isinstance(event, OrderCanceled):
        return {
            "event_type": "OrderCanceled",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "source": event.source,
            "order_id": event.order_id,
            "client_order_id": event.client_order_id,
            "market_id": event.market_id,
            "token_id": event.token_id,
            "cancel_reason": event.cancel_reason,
            "cumulative_filled_size": event.cumulative_filled_size,
            "remaining_size": event.remaining_size,
        }

    if isinstance(event, MarketResolvedWithInventory):
        return {
            "event_type": "MarketResolvedWithInventory",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "source": event.source,
            "market_id": event.market_id,
            "token_id": event.token_id,
            "side": event.side,
            "size": event.size,
            "outcome_price": event.outcome_price,
            "average_entry_price": event.average_entry_price,
            "estimated_pnl_usdc": event.estimated_pnl_usdc,
            "run_id": event.run_id,
        }

    status = "active" if event.is_active else "closed"
    payload = asdict(event)
    payload.update(
        {
            "event_type": "MarketStateUpdate",
            "event_time": iso_from_ms(event.event_time_ms),
            "ingest_time": iso_from_ms(event.ingest_time_ms),
            "start_time": iso_from_ms(event.start_time_ms),
            "end_time": iso_from_ms(event.end_time_ms),
            "status": status,
            "duration_seconds": max(0, int((event.end_time_ms - event.start_time_ms) / 1000)),
        }
    )
    return payload


def _event_market_token(event: NormalizedEvent) -> tuple[str | None, str | None]:
    market_id = getattr(event, "market_id", None)
    token_id = getattr(event, "token_id", None)
    return market_id, token_id


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def iso_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
