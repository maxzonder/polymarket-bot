from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from ..core.events import BookUpdate, MarketStateUpdate, NormalizedEvent
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


@dataclass
class InMemoryIntentStore:
    intents: list[OrderIntent] = field(default_factory=list)
    events: list[NormalizedEvent] = field(default_factory=list)
    market_updates: list[MarketStateUpdate] = field(default_factory=list)
    strategy_state: dict[tuple[str, str | None, str], dict[str, Any]] = field(default_factory=dict)
    run_id: str = "run_in_memory"

    @property
    def current_run_id(self) -> str:
        return self.run_id

    def append_event(self, event: NormalizedEvent) -> None:
        self.events.append(event)

    def upsert_market_state(self, event: MarketStateUpdate) -> None:
        self.market_updates.append(event)

    def record_first_touch(self, *, market_id: str, token_id: str, level: float, event_time_ms: int) -> None:
        self.strategy_state[(market_id, token_id, f"first_touch_fired:{level:.2f}")] = {
            "event_time": iso_from_ms(event_time_ms),
            "level": level,
        }

    def persist_intent(self, intent: OrderIntent) -> None:
        self.intents.append(intent)


class SQLiteRuntimeStore:
    def __init__(self, path: str | Path, *, run: RunContext):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.run = run
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
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
        self.conn.close()

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
        market_status = "active" if event.is_active else "closed"
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
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        size = intent.notional_usdc / intent.entry_price if intent.entry_price > 0 else None
        self.insert_order(
            order_id=intent.intent_id,
            market_id=intent.market_id,
            token_id=intent.token_id,
            side="BUY",
            price=intent.entry_price,
            size=size,
            state=OrderState.INTENT,
            client_order_id=intent.intent_id,
            intent_created_at_ms=intent.event_time_ms,
            last_state_change_at_ms=intent.event_time_ms,
            remaining_size=size,
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
        intent_created_at_ms: int,
        last_state_change_at_ms: int,
        remaining_size: float | None = None,
        parent_order_id: str | None = None,
        venue_order_status: str | None = None,
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
                parent_order_id,
                intent_created_at,
                last_state_change_at,
                venue_order_status,
                cumulative_filled_size,
                remaining_size,
                reconciliation_required
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                side = excluded.side,
                price = excluded.price,
                size = excluded.size,
                state = excluded.state,
                client_order_id = excluded.client_order_id,
                parent_order_id = excluded.parent_order_id,
                last_state_change_at = excluded.last_state_change_at,
                venue_order_status = excluded.venue_order_status,
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
                parent_order_id,
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
            INSERT OR REPLACE INTO fills (
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

    def _initialize_schema(self) -> None:
        schema_path = Path(__file__).resolve().parents[2] / "docs" / "phase0" / "storage_schema.sql"
        self.conn.executescript(schema_path.read_text())
        self.conn.commit()

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
