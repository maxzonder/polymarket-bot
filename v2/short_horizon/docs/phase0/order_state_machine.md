# P0-C — Order state machine transition matrix

This document defines the **single canonical order lifecycle** for the short-horizon MVP.

It is intentionally narrower than the legacy swan bot execution model.
The goal is to describe the venue-facing lifecycle of one order clearly enough that:
- storage uses the same states
- replay uses the same states where applicable
- reconciliation uses the same states
- telemetry uses the same state names
- execution code does not invent local variants ad hoc

If code wants a different state name or transition, the design should be updated here first.

## Scope

This state machine covers the MVP order path for:
- fee-aware taker entry orders on the validated #115 slice
- cancel flow where needed
- reconciliation after restart

It does **not** yet attempt to model sophisticated maker workflows, quote ladders, or multi-order replacement trees.

## Canonical states

### Non-terminal states
- `intent`
- `pending_send`
- `accepted`
- `partially_filled`
- `cancel_requested`
- `replace_requested`
- `unknown`

### Terminal states
- `filled`
- `rejected`
- `cancel_confirmed`
- `expired`
- `replaced`

## State meanings

- `intent`
  - Strategy has decided to place an order, but nothing has been sent to the venue yet.
  - This is the first persisted state for a new order intent.

- `pending_send`
  - Execution layer has accepted the intent and is actively attempting to send it.
  - Venue acceptance/rejection is not known yet.

- `accepted`
  - Venue has acknowledged the order as live/open.
  - No fill has been observed yet.

- `partially_filled`
  - At least one fill has been observed, but remaining size is still open.

- `cancel_requested`
  - We have decided to cancel the open remainder and sent a cancel request to the venue.
  - Final outcome is not known yet.

- `replace_requested`
  - We have decided to replace an existing open order.
  - Operationally this is modeled as “cancel old + create successor order”, but the old order passes through this state so telemetry can represent intent cleanly.

- `unknown`
  - Used only in reconciliation or transport-failure situations.
  - Means storage and venue truth are not yet aligned strongly enough to let strategy logic proceed safely.

- `filled`
  - Entire order quantity is filled.

- `rejected`
  - Venue rejected the order, or reconciliation concluded it never became live.

- `cancel_confirmed`
  - Venue confirmed cancellation of the remaining quantity.

- `expired`
  - Order validity ended without full fill and venue marked it expired, or local policy expired it and venue state confirms that outcome.

- `replaced`
  - Order has been superseded by a successor child order after a replace flow.
  - This is terminal for the original parent order id.
  - The replacement child order starts a fresh lifecycle of its own at `intent` and is not merely an alias-state of the parent.

## Event sources

Every transition event named below must come from one of these sources:
- `strategy` — order intent emitted by strategy code
- `execution` — local action taken by execution layer before venue confirmation
- `venue_rest` — venue REST response
- `venue_ws` — venue websocket update
- `timer` — local timeout/expiry handling
- `restart_reconciliation` — startup recovery logic
- `operator` — explicit human/operator intervention when ambiguity cannot be resolved automatically

## Transition matrix

| from | event | to | guard | telemetry_emitted |
|---|---|---|---|---|
| none | strategy_place_intent | intent | risk checks passed at strategy boundary | `OrderIntentCreated` |
| intent | execution_send_started | pending_send | payload built, pre-send validation passed | `OrderSendStarted` |
| pending_send | venue_ack_open | accepted | venue returns accepted/open/live status and venue order id | `OrderAccepted` |
| pending_send | venue_reject | rejected | venue explicitly rejects order | `OrderRejected` |
| pending_send | send_timeout_before_ack | unknown | no authoritative venue answer within timeout budget | `OrderSendTimeout` |
| accepted | venue_fill_partial | partially_filled | cumulative filled size > 0 and remaining size > 0 | `OrderPartiallyFilled` |
| accepted | venue_fill_full | filled | cumulative filled size >= order size | `OrderFilled` |
| accepted | strategy_cancel_intent | cancel_requested | order still open and cancel is allowed by policy | `OrderCancelRequested` |
| accepted | strategy_replace_intent | replace_requested | order still open and replace is allowed by policy | `OrderReplaceRequested` |
| accepted | venue_expired | expired | venue marks order expired | `OrderExpired` |
| partially_filled | venue_fill_partial | partially_filled | cumulative filled size increases but still < total size | `OrderPartiallyFilled` |
| partially_filled | venue_fill_full | filled | cumulative filled size >= order size | `OrderFilled` |
| partially_filled | strategy_cancel_intent | cancel_requested | remaining size > 0 and cancel is allowed by policy | `OrderCancelRequested` |
| partially_filled | strategy_replace_intent | replace_requested | remaining size > 0 and replace is allowed by policy | `OrderReplaceRequested` |
| partially_filled | venue_expired | expired | venue marks residual order expired | `OrderExpired` |
| cancel_requested | venue_cancel_ack | cancel_confirmed | venue confirms cancel of residual quantity | `OrderCanceled` |
| cancel_requested | venue_fill_full_before_cancel | filled | venue reports order fully filled before cancel took effect | `OrderFilled` |
| cancel_requested | venue_fill_partial_before_cancel | partially_filled | additional partial fill arrives before cancel completes | `OrderPartiallyFilled` |
| cancel_requested | venue_cancel_reject_but_order_live | accepted | no fills since request and venue says order remains open | `OrderCancelRejected` |
| cancel_requested | venue_cancel_reject_but_partially_filled_live | partially_filled | partial fills exist and venue says residual remains open | `OrderCancelRejected` |
| cancel_requested | cancel_timeout | unknown | no authoritative final state after timeout | `OrderCancelTimeout` |
| replace_requested | venue_replace_success_old_closed | replaced | old order is definitively superseded by successor order id | `OrderReplaced` |
| replace_requested | venue_replace_reject_order_still_live | accepted | replace failed and original order remains open with zero fills | `OrderReplaceRejected` |
| replace_requested | venue_replace_reject_partial_order_still_live | partially_filled | replace failed and original order remains partially filled/open | `OrderReplaceRejected` |
| replace_requested | replace_timeout | unknown | no authoritative final state after timeout | `OrderReplaceTimeout` |
| unknown | reconciliation_maps_to_accepted | accepted | venue confirms order live/open with zero fills | `OrderReconciled` |
| unknown | reconciliation_maps_to_partially_filled | partially_filled | venue confirms partial fills with remainder open | `OrderReconciled` |
| unknown | reconciliation_maps_to_filled | filled | venue confirms full fill | `OrderReconciled` |
| unknown | reconciliation_maps_to_cancel_confirmed | cancel_confirmed | venue confirms cancel | `OrderReconciled` |
| unknown | reconciliation_maps_to_rejected | rejected | venue confirms rejection or absence consistent with non-live order | `OrderReconciled` |
| unknown | reconciliation_maps_to_expired | expired | venue confirms expiry | `OrderReconciled` |
| unknown | reconciliation_unresolved_operator_blocks_market | unknown | venue truth still ambiguous, market stays blocked | `OrderReconciliationBlocked` |

## Incoming/outgoing sanity by state

### `intent`
- incoming:
  - `strategy_place_intent`
- outgoing:
  - `execution_send_started`

### `pending_send`
- incoming:
  - `execution_send_started`
- outgoing:
  - `venue_ack_open`
  - `venue_reject`
  - `send_timeout_before_ack`

### `accepted`
- incoming:
  - `venue_ack_open`
  - `venue_cancel_reject_but_order_live`
  - `venue_replace_reject_order_still_live`
  - `reconciliation_maps_to_accepted`
- outgoing:
  - `venue_fill_partial`
  - `venue_fill_full`
  - `strategy_cancel_intent`
  - `strategy_replace_intent`
  - `venue_expired`

### `partially_filled`
- incoming:
  - `venue_fill_partial`
  - `venue_cancel_reject_but_partially_filled_live`
  - `venue_replace_reject_partial_order_still_live`
  - `reconciliation_maps_to_partially_filled`
- outgoing:
  - `venue_fill_partial`
  - `venue_fill_full`
  - `strategy_cancel_intent`
  - `strategy_replace_intent`
  - `venue_expired`

### `cancel_requested`
- incoming:
  - `strategy_cancel_intent`
- outgoing:
  - `venue_cancel_ack`
  - `venue_fill_full_before_cancel`
  - `venue_fill_partial_before_cancel`
  - `venue_cancel_reject_but_order_live`
  - `venue_cancel_reject_but_partially_filled_live`
  - `cancel_timeout`

### `replace_requested`
- incoming:
  - `strategy_replace_intent`
- outgoing:
  - `venue_replace_success_old_closed`
  - `venue_replace_reject_order_still_live`
  - `venue_replace_reject_partial_order_still_live`
  - `replace_timeout`

### `unknown`
- incoming:
  - `send_timeout_before_ack`
  - `cancel_timeout`
  - `replace_timeout`
  - restart reconciliation on ambiguous stored non-terminal order
- outgoing:
  - `reconciliation_maps_to_accepted`
  - `reconciliation_maps_to_partially_filled`
  - `reconciliation_maps_to_filled`
  - `reconciliation_maps_to_cancel_confirmed`
  - `reconciliation_maps_to_rejected`
  - `reconciliation_maps_to_expired`
  - `reconciliation_unresolved_operator_blocks_market`

## Terminal states

The following are terminal:
- `filled`
- `rejected`
- `cancel_confirmed`
- `expired`
- `replaced`

No transition may leave a terminal state.
If the venue later disagrees with a terminal state already persisted locally, that is treated as a reconciliation incident and must be logged explicitly rather than silently mutating history.

## Reconciliation rule

### When `unknown` is entered
`unknown` is entered only when the system lacks authoritative truth about a non-terminal order, for example:
- send path timed out after request creation but before acceptance/rejection was confirmed
- cancel request timed out without a clear final venue state
- replace request timed out without a clear final venue state
- system restarted and storage still contains non-terminal orders whose latest venue truth is stale or missing

### Resolution rule
On restart or explicit reconciliation:
1. query venue state for the order
2. map venue truth to the nearest concrete canonical state
3. persist a reconciliation event with both:
   - previous local state
   - reconciled state
4. if venue truth remains ambiguous, keep the order in `unknown`
5. while any order for a market remains `unknown`, strategy dispatch for that market is blocked

### Truth priority
For MVP, venue-confirmed truth wins over stale local state.
Local state is authoritative only when it is consistent with the latest confirmed venue state.

## Design notes specific to MVP

- The MVP entry path is taker-oriented, so many orders may move quickly from `pending_send` to `filled` or `partially_filled` with little time spent in `accepted`.
- `replace_requested` is retained in the canonical model even if early MVP code uses it minimally. This avoids inventing a second state machine later.
- A replace flow closes the parent order lifecycle and creates a fresh child order lifecycle rather than mutating one order id forever.
- `unknown` is intentionally explicit. Silent ambiguity is more dangerous than a temporarily blocked market.

## Invariants

1. Every persisted order starts at `intent`.
2. No persisted order skips directly from `intent` to `accepted` without a recorded `pending_send` transition.
3. No terminal state has outgoing transitions.
4. `filled` implies cumulative filled quantity >= original order quantity within tolerated rounding error.
5. `cancel_confirmed` implies remaining open quantity is zero and order is no longer live at venue.
6. `unknown` implies trading on that market is blocked until reconciliation completes.
7. Reconciliation never silently deletes ambiguity; it either resolves to a concrete state or keeps the order `unknown`.
