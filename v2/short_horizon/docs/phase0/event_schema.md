# P0-D — Event vocabulary schema

This document defines the **single shared event contract** between:
- live ingestion (websocket-first in live mode)
- replay ingestion
- strategy evaluation
- execution feedback
- telemetry persistence

Writing or reading replay/live against a different schema is a bug.

## Global rules

### Required fields on every raw normalized event
Every normalized event as it first enters the system must carry:
- `event_type` — string enum
- `event_time` — when the thing happened at the venue or domain source, in RFC3339 UTC with millisecond precision where available
- `ingest_time` — when our system observed/created the event, in RFC3339 UTC with millisecond precision where available
- `source` — source identifier, one of:
  - venue websocket channel name
  - venue REST endpoint name
  - `internal.timer`
  - `internal.execution`
  - `replay.log`
  - `restart_reconciliation`

`run_id` is attached by our ingestion/persistence layer when the event becomes part of a concrete replay/live run. It is not expected from venue-originated raw events.

In live mode, venue websocket sources are expected to be the primary source for market-data and user-order events. Venue REST sources exist mainly for bootstrap, fallback, and reconciliation.

### Time semantics
- `event_time` is domain truth time when known.
- `ingest_time` is our local observation/creation time.
- These fields must never be collapsed into one field.
- Strategy logic should primarily reason on `event_time` ordering.
- Latency and replay/live comparison rely on the gap between them.

### Numeric conventions
- prices are decimal probabilities in `[0.0, 1.0]`
- sizes are token units unless explicitly suffixed with `_usdc`
- fee values use `_bps` or `_usdc` suffixes explicitly
- latency/duration values use `_ms`

### JSON encoding guidance
For event logs:
- timestamps serialized as RFC3339 UTC strings
- decimals serialized as strings when exactness matters, otherwise JSON numbers are acceptable if the implementation guarantees stable formatting
- nullable fields should be explicit `null`, not omitted, when the field is part of the schema

---

## Event: `BookUpdate`

### Purpose
Represents the current visible orderbook state relevant to one token after an orderbook snapshot or delta application.

### Required fields
- `event_type: "BookUpdate"`
- `event_time: string`
- `ingest_time: string`
- `source: string`
- `run_id: string | null`
- `market_id: string`
- `token_id: string`
- `best_bid: number | null`
- `best_ask: number | null`
- `spread: number | null`
- `mid_price: number | null`
- `bid_levels: array`
- `ask_levels: array`
- `book_seq: integer | null`
- `is_snapshot: boolean`

### Level object shape
Each entry in `bid_levels` / `ask_levels`:
- `price: number`
- `size: number`

### Units
- prices: probability price
- size: token units

### Notes
- `bid_levels` sorted descending by price
- `ask_levels` sorted ascending by price
- for MVP logs, depth may be truncated to top N levels, but this must be explicit and stable

### Example JSON
```json
{
  "event_type": "BookUpdate",
  "event_time": "2026-04-18T14:20:31.245Z",
  "ingest_time": "2026-04-18T14:20:31.268Z",
  "source": "clob.ws.book",
  "run_id": null,
  "market_id": "0xabc123",
  "token_id": "123456789",
  "best_bid": 0.64,
  "best_ask": 0.65,
  "spread": 0.01,
  "mid_price": 0.645,
  "bid_levels": [
    {"price": 0.64, "size": 150.0},
    {"price": 0.63, "size": 210.0}
  ],
  "ask_levels": [
    {"price": 0.65, "size": 90.0},
    {"price": 0.66, "size": 140.0}
  ],
  "book_seq": 918273,
  "is_snapshot": false
}
```

---

## Event: `TradeTick`

### Purpose
Represents a trade print observed from venue market data.

### Required fields
- `event_type: "TradeTick"`
- `event_time: string`
- `ingest_time: string`
- `source: string`
- `run_id: string | null`
- `market_id: string`
- `token_id: string`
- `trade_id: string | null`
- `price: number`
- `size: number`
- `aggressor_side: string | null`
- `venue_seq: integer | null`

### Allowed `aggressor_side`
- `buy`
- `sell`
- `null` if venue does not provide it

### Example JSON
```json
{
  "event_type": "TradeTick",
  "event_time": "2026-04-18T14:20:31.311Z",
  "ingest_time": "2026-04-18T14:20:31.332Z",
  "source": "clob.ws.trade",
  "run_id": null,
  "market_id": "0xabc123",
  "token_id": "123456789",
  "trade_id": "tr_778899",
  "price": 0.65,
  "size": 42.0,
  "aggressor_side": "buy",
  "venue_seq": 918274
}
```

---

## Event: `MarketStateUpdate`

### Purpose
Represents market-level metadata required for strategy gating and lifecycle-clock computation.

### Required fields
- `event_type: "MarketStateUpdate"`
- `event_time: string`
- `ingest_time: string`
- `source: string`
- `run_id: string | null`
- `market_id: string`
- `condition_id: string | null`
- `question: string | null`
- `status: string`
- `start_time: string | null`
- `end_time: string | null`
- `duration_seconds: integer | null`
- `token_yes_id: string | null`
- `token_no_id: string | null`
- `fee_rate_bps: number | null` — V1-era headline taker fee in bps. Kept for back-compat on existing capture bundles; V2 captures populate `fee_info` instead and may also mirror `base_fee_bps` here.
- `fee_info: object | null` — V2 dynamic-fee descriptor. Shape: `{base_fee_bps: integer, rate: number, exponent: number, source: string}` where `rate` and `exponent` are the V2 `fd.r` / `fd.e` parameters from `getClobMarketInfo`. Consumers should prefer `fee_info.base_fee_bps` over `fee_rate_bps` when both are present.
- `fee_fetched_at: string | null`
- `fees_enabled: boolean | null`
- `is_ascending_market: boolean | null`
- `market_source_revision: string | null`

### Allowed `status`
- `active`
- `paused`
- `closed`
- `resolved`
- `unknown`

### Notes
- `start_time` and `end_time` are the lifecycle-clock inputs used at decision time
- if `end_time` changes mid-lifecycle, a new `MarketStateUpdate` must be emitted immediately
- replay must consume these updates exactly as live would
- stale or missing `MarketStateUpdate` state must block lifecycle-dependent decisions rather than silently reusing old values

### Example JSON
```json
{
  "event_type": "MarketStateUpdate",
  "event_time": "2026-04-18T14:19:59.000Z",
  "ingest_time": "2026-04-18T14:20:00.014Z",
  "source": "gamma.rest.markets",
  "run_id": null,
  "market_id": "0xabc123",
  "condition_id": "0xcond555",
  "question": "Will BTC be above 95000 at 14:30 UTC?",
  "status": "active",
  "start_time": "2026-04-18T14:15:00.000Z",
  "end_time": "2026-04-18T14:30:00.000Z",
  "duration_seconds": 900,
  "token_yes_id": "123456789",
  "token_no_id": "987654321",
  "fee_rate_bps": 35,
  "fee_info": {"base_fee_bps": 35, "rate": 0.0035, "exponent": 1.0, "source": "v2.clob_market_info"},
  "fee_fetched_at": "2026-04-18T14:19:59.000Z",
  "fees_enabled": true,
  "is_ascending_market": true,
  "market_source_revision": "gamma_offset_1200"
}
```

---

## Event: `TimerEvent`

### Purpose
Represents internally scheduled decision clocks, watchdogs, or maintenance timers.

### Required fields
- `event_type: "TimerEvent"`
- `event_time: string`
- `ingest_time: string`
- `source: string`
- `run_id: string | null`
- `timer_kind: string`
- `market_id: string | null`
- `token_id: string | null`
- `deadline_ms: integer | null`
- `payload: object | null`

### Example `timer_kind`
- `decision_tick`
- `fee_refresh_due`
- `reconciliation_retry`
- `market_data_stale_check`

### Example JSON
```json
{
  "event_type": "TimerEvent",
  "event_time": "2026-04-18T14:20:32.000Z",
  "ingest_time": "2026-04-18T14:20:32.001Z",
  "source": "internal.timer",
  "run_id": "run_20260418_1420",
  "timer_kind": "market_data_stale_check",
  "market_id": "0xabc123",
  "token_id": "123456789",
  "deadline_ms": 2000,
  "payload": {"last_book_event_time": "2026-04-18T14:20:31.245Z"}
}
```

---

## Event: `OrderAccepted`

### Purpose
Represents authoritative confirmation that the venue accepted an order as live/open.

### Required fields
- `event_type: "OrderAccepted"`
- `event_time: string`
- `ingest_time: string`
- `source: string`
- `run_id: string | null`
- `order_id: string`
- `client_order_id: string | null`
- `market_id: string`
- `token_id: string`
- `side: string`
- `price: number`
- `size: number`
- `time_in_force: string | null`
- `post_only: boolean | null`
- `venue_status: string`

### Allowed `side`
- `BUY`
- `SELL`

### Example JSON
```json
{
  "event_type": "OrderAccepted",
  "event_time": "2026-04-18T14:20:31.410Z",
  "ingest_time": "2026-04-18T14:20:31.417Z",
  "source": "clob.rest.place_order",
  "run_id": "run_20260418_1420",
  "order_id": "ord_111",
  "client_order_id": "cli_111",
  "market_id": "0xabc123",
  "token_id": "123456789",
  "side": "BUY",
  "price": 0.65,
  "size": 15.3846,
  "time_in_force": "GTC",
  "post_only": false,
  "venue_status": "live"
}
```

---

## Event: `OrderRejected`

### Purpose
Represents authoritative rejection of an order send or replace attempt.

### Required fields
- `event_type: "OrderRejected"`
- `event_time: string`
- `ingest_time: string`
- `source: string`
- `run_id: string | null`
- `client_order_id: string | null`
- `market_id: string`
- `token_id: string`
- `side: string`
- `price: number | null`
- `size: number | null`
- `reject_reason_code: string | null`
- `reject_reason_text: string | null`
- `is_retryable: boolean | null`

### Example JSON
```json
{
  "event_type": "OrderRejected",
  "event_time": "2026-04-18T14:20:31.411Z",
  "ingest_time": "2026-04-18T14:20:31.419Z",
  "source": "clob.rest.place_order",
  "run_id": "run_20260418_1420",
  "client_order_id": "cli_112",
  "market_id": "0xabc123",
  "token_id": "123456789",
  "side": "BUY",
  "price": 0.65,
  "size": 15.3846,
  "reject_reason_code": "MIN_SIZE",
  "reject_reason_text": "order size below venue minimum",
  "is_retryable": false
}
```

---

## Event: `OrderFilled`

### Purpose
Represents a fill update for an order. Full and partial fills use the same event type; fullness is encoded in cumulative fields.

### Required fields
- `event_type: "OrderFilled"`
- `event_time: string`
- `ingest_time: string`
- `source: string`
- `run_id: string | null`
- `order_id: string`
- `client_order_id: string | null`
- `market_id: string`
- `token_id: string`
- `side: string`
- `fill_price: number`
- `fill_size: number`
- `cumulative_filled_size: number`
- `remaining_size: number`
- `fee_paid_usdc: number | null`
- `liquidity_role: string | null`
- `venue_fill_id: string | null`

### Allowed `liquidity_role`
- `maker`
- `taker`
- `null`

### Example JSON
```json
{
  "event_type": "OrderFilled",
  "event_time": "2026-04-18T14:20:31.489Z",
  "ingest_time": "2026-04-18T14:20:31.507Z",
  "source": "clob.ws.user_fills",
  "run_id": "run_20260418_1420",
  "order_id": "ord_111",
  "client_order_id": "cli_111",
  "market_id": "0xabc123",
  "token_id": "123456789",
  "side": "BUY",
  "fill_price": 0.65,
  "fill_size": 10.0,
  "cumulative_filled_size": 10.0,
  "remaining_size": 5.3846,
  "fee_paid_usdc": 0.0532,
  "liquidity_role": "taker",
  "venue_fill_id": "fill_9001"
}
```

---

## Event: `OrderCanceled`

### Purpose
Represents authoritative confirmation that the venue canceled the remaining order quantity.

### Required fields
- `event_type: "OrderCanceled"`
- `event_time: string`
- `ingest_time: string`
- `source: string`
- `run_id: string | null`
- `order_id: string`
- `client_order_id: string | null`
- `market_id: string`
- `token_id: string`
- `cancel_reason: string | null`
- `cumulative_filled_size: number | null`
- `remaining_size: number | null`

### Example JSON
```json
{
  "event_type": "OrderCanceled",
  "event_time": "2026-04-18T14:20:31.650Z",
  "ingest_time": "2026-04-18T14:20:31.659Z",
  "source": "clob.rest.cancel_order",
  "run_id": "run_20260418_1420",
  "order_id": "ord_111",
  "client_order_id": "cli_111",
  "market_id": "0xabc123",
  "token_id": "123456789",
  "cancel_reason": "user_requested",
  "cumulative_filled_size": 10.0,
  "remaining_size": 5.3846
}
```

---

## Shared replay/live guarantee

A strategy consumer written against these events must not care whether the source is:
- live websocket + REST
- replay log reader
- restart reconciliation synthesizing authoritative corrections

If the strategy path needs extra source-specific branching, the schema is missing something and should be extended here rather than patched downstream.

## Minimum event set needed for MVP strategy

The first MVP slice can run on:
- `MarketStateUpdate`
- `BookUpdate`
- `OrderAccepted`
- `OrderRejected`
- `OrderFilled`
- `OrderCanceled`
- `TimerEvent`

`TradeTick` is still part of the canonical vocabulary because it is useful for replay realism, telemetry, and later strategy extensions, even if the first strategy does not depend on it directly.
