# P0-I — Restart reconciliation algorithm

This document defines the exact restart behavior when persisted local order state may have diverged from venue truth.

The goal is safety first:
- do not let strategy logic act on fictional state
- reconcile non-terminal orders before market-level trading resumes
- prefer explicit blocking over silent assumptions

## Scope

This applies on process start, restart after crash, or explicit reconciliation mode.

It is concerned with orders in non-terminal states from the canonical state machine:
- `intent`
- `pending_send`
- `accepted`
- `partially_filled`
- `cancel_requested`
- `replace_requested`
- `unknown`

Terminal states are not re-opened silently.
If venue truth disagrees with a persisted terminal state, that is an incident and must be logged loudly.

## High-level rule

On startup:
1. load all orders in non-terminal states
2. group them by `market_id`
3. block strategy dispatch for every affected market immediately
4. reconcile each order against venue truth
5. only unblock a market when all of its non-terminal orders are reconciled into concrete acceptable states or explicitly operator-blocked

## Truth priority

For MVP:
- fresh venue-confirmed truth wins over stale local state
- local state is authoritative only when it matches recent venue-confirmed truth

If venue truth is ambiguous or unavailable, the order stays `unknown` and the market remains blocked.

## Venue lookup inputs

Reconciliation should try to identify the order using, in order of preference:
1. `order_id`
2. `client_order_id`
3. market-scoped open-order lookup combined with side/price/size matching, if supported

The implementation should use the strongest identifier available and record which identifier path was used.

## Mapping goals

Reconciliation needs to answer two questions:
1. what is the venue's current view of the order?
2. how should that map into our canonical state machine?

## Canonical reconciliation pseudocode

```text
on_startup():
    non_terminal_orders = storage.load_non_terminal_orders()
    affected_markets = unique(non_terminal_orders.market_id)

    for market_id in affected_markets:
        mark_market_blocked(market_id, reason="startup_reconciliation")

    for order in non_terminal_orders ordered by last_state_change_at asc:
        reconcile_order(order)

    for market_id in affected_markets:
        if all orders for market are resolved and none remain unknown:
            clear_market_block(market_id)
        else:
            keep_market_blocked(market_id)
```

```text
reconcile_order(order):
    emit reconciliation_started event

    venue_result = query_venue(order)

    if venue_result == not_found:
        handle_not_found(order)
        return

    if venue_result == transport_error or ambiguous_response:
        move_to_unknown(order, reason="venue_lookup_ambiguous")
        return

    mapped_state = map_venue_status_to_local_state(order, venue_result)

    if mapped_state conflicts with local persisted state:
        log loud reconciliation mismatch

    persist_state_transition(order, mapped_state)
    persist reconciliation event with old_state, new_state, venue payload summary

    if venue_result includes fills not yet recorded locally:
        append missing fill rows
        append corresponding OrderFilled events
```

## Venue status → canonical state mapping

### If venue says order is live/open with zero fills
Map to:
- `accepted`

### If venue says order is open with cumulative fills > 0 and remaining > 0
Map to:
- `partially_filled`

### If venue says cumulative filled >= original size
Map to:
- `filled`

### If venue says canceled
Map to:
- `cancel_confirmed`

### If venue says rejected / invalid / never accepted
Map to:
- `rejected`

### If venue says expired
Map to:
- `expired`

### If venue says old order was superseded by replacement
Map to:
- `replaced`

### If venue response does not cleanly map
Map to:
- `unknown`
- keep market blocked

## Special handling: order not found at venue

This is the highest-risk ambiguous case.
The action depends on the local state.

### Case A — local state was `intent`
Interpretation:
- order never reached send stage or crashed before send

Action:
- mark as `rejected`
- reason: `not_sent_or_not_persisted_at_venue`
- no market block needed after this order resolves, unless other orders remain unresolved

### Case B — local state was `pending_send`
Interpretation:
- could have failed before venue acceptance
- could have been accepted and later disappeared from weak lookup path

Action:
- move to `unknown`
- set `reconciliation_required = 1`
- retry venue lookup using fallback identifier path if available
- if still not found after retry window, move to `rejected` only if there is no evidence of fill and operator policy allows auto-resolution
- otherwise remain `unknown` and keep market blocked

### Case C — local state was `accepted`, `partially_filled`, `cancel_requested`, or `replace_requested`
Interpretation:
- venue has either lost visibility, identifiers are mismatched, or local state is stale enough that we cannot trust it

Action:
- move to `unknown`
- keep market blocked
- retry lookup and/or query market-scoped open orders, recent fills, recent closed orders if venue supports them
- if still unresolved, require operator-visible intervention before unblocking that market

### Case D — local state already `unknown`
Action:
- retry venue lookup
- remain blocked unless resolved into a concrete canonical state

## Fill backfill rule

If venue reports fills that are missing locally:
1. insert missing `fills` rows
2. update `orders.cumulative_filled_size`
3. update `orders.remaining_size`
4. append `OrderFilled` events to `events_log`
5. only then finalize the reconciled order state

Rationale:
- order state should not imply fills that are absent from storage

## Market blocking rule

### Default policy
Reconciliation blocks **market-level strategy dispatch**, not necessarily the entire process.

That means:
- if market `A` has unresolved `unknown` orders, do not trade market `A`
- unrelated markets may continue only if they have no unresolved reconciliation incidents

### Escalate to global block if any of the following happen
- venue adapter is unreachable at startup or cannot authenticate at all
- repeated venue lookup failures across many markets
- storage corruption suspected
- run-level config mismatch detected
- fee metadata system appears broken globally
- operator explicitly requests global safe mode

## Persistence and telemetry requirements

Every reconciliation attempt must append structured telemetry containing at least:
- `run_id`
- `market_id`
- `order_id`
- previous local state
- venue lookup path used
- venue status summary
- new mapped state
- whether market remained blocked
- whether any fills were backfilled

Minimum event types to emit during reconciliation:
- `ReconciliationStarted`
- `OrderReconciled`
- `OrderReconciliationBlocked`
- `ReconciliationFillBackfilled`

These can live inside `events_log` payloads even if they are not part of the first strategy-facing event vocabulary.

## Operator-visible cases

Operator visibility is required when:
- an order stays `unknown` after retry window
- venue reports a state impossible under our local history
- a terminal local state conflicts with venue truth
- fills appear that imply materially different exposure than local storage believed

In these cases:
- keep market blocked
- write explicit reconciliation incident logs
- do not auto-unblock silently

## Concrete decision table

| local state | venue says | action |
|---|---|---|
| `intent` | not found | move to `rejected` |
| `pending_send` | not found | move to `unknown`, retry, maybe later `rejected` if clearly unsent |
| `accepted` | open/no fills | move to `accepted` |
| `accepted` | partial fills/open | move to `partially_filled` and backfill fills |
| `accepted` | fully filled | move to `filled` and backfill fills |
| `accepted` | canceled | move to `cancel_confirmed` |
| `accepted` | rejected | move to `rejected` |
| `accepted` | expired | move to `expired` |
| `partially_filled` | open with remaining | stay `partially_filled`, backfill missing fills |
| `partially_filled` | fully filled | move to `filled`, backfill missing fills |
| `partially_filled` | canceled | move to `cancel_confirmed`, backfill missing fills if needed |
| `cancel_requested` | canceled | move to `cancel_confirmed` |
| `cancel_requested` | fully filled | move to `filled`, backfill missing fills |
| `replace_requested` | replaced | move to `replaced` |
| any non-terminal | ambiguous / transport error | move to or remain `unknown`, keep market blocked |

## Acceptance condition for implementation later

A developer should be able to implement startup reconciliation directly from this document without deciding:
- when to block
- when to trust venue over storage
- when to backfill fills
- when `unknown` can be auto-cleared

Those decisions are now fixed here.
