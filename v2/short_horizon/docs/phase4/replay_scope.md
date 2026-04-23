# Phase 4 replay fidelity scope

Date: 2026-04-23
Issue: #123
Step: P4-0

This document freezes what Phase 4 means by replay fidelity for the short-horizon bot.

The goal is mechanical determinism.

Given one captured live run and the same normalized inputs, replay must drive the same shared `StrategyRuntime` and reproduce the same observable decision sequence as live. Phase 4 is not about profitability, tuning, or proving that every internal log line is identical.

## 1. Fidelity contract

Phase 4 fidelity is **decision-level fidelity**.

Replay is considered faithful when, for one captured run, it reproduces the same ordered observable decision sequence as live across these categories:
- `OrderIntent`
- `SkipDecision`
- `CancelOrder`
- final per-order terminal states

The comparison target is the shared runtime and execution path already used by live:
- `StrategyRuntime`
- `drive_runtime_events`
- the existing order translator
- the existing P0-C order lifecycle state machine

Replay must cover those code paths directly. It must not re-implement strategy logic in a parallel simulator.

## 2. What counts as observable truth

For Phase 4, the truth set for one run is:
- normalized `events_log` rows emitted and/or ingested during live execution
- terminal `orders` rows
- terminal `fills` rows
- captured venue request/response records for execution and reconciliation calls

These are the only artifacts that define replay correctness.

If replay diverges from live on any of them in a way that changes the decision sequence or final order outcome, that is a fidelity failure.

## 3. Canonical capture bundle

A canonical Phase 4 capture bundle is scoped to exactly one `run_id`.

### 3.1 Fidelity-critical payload set

The bundle must contain these fidelity-critical artifacts:
- `events_log.jsonl`
  - normalized Phase-0 events in live order
  - includes `MarketStateUpdate`, `BookUpdate`, `TradeTick`, `TimerEvent` where relevant, and user-stream-derived order lifecycle events such as `OrderAccepted`, `OrderFilled`, `OrderRejected`, `OrderCanceled`
- `orders_final.jsonl`
  - terminal live-observed order rows used as replay ground truth
- `fills_final.jsonl`
  - terminal live-observed fill rows used as replay ground truth
- `venue_responses.jsonl`
  - exact captured request/response records for live execution-facing calls
  - includes place, cancel, and reconciliation/look-up style calls such as `get_order` and `list_open_orders`

### 3.2 Supporting artifacts allowed inside the same bundle

The on-disk bundle may also contain supporting artifacts that make replay deterministic or easier to operate, without expanding the fidelity contract itself:
- `market_state_snapshots.jsonl`
  - ordered `MarketStateUpdate` projection used to hydrate replay deterministically and cheaply
  - treated as a convenience projection derived from the captured run, not as an independent source of truth that can override `events_log`
- `manifest.json`
  - metadata envelope for `run_id`, `strategy_id`, `config_hash`, wall-clock bounds, file names, and per-file counts

This split is intentional.

It lets `P4-2` ship a practical self-describing bundle without weakening the P4-0 rule that fidelity is judged only from normalized events, terminal order/fill truth, and captured venue responses.

## 4. Required replay behavior

A compliant replay runner must:
- consume the captured bundle without any network dependency
- drive the same `StrategyRuntime` event path used by live
- advance decision-path time from captured event timestamps, not from wall clock
- route execution-side calls through a captured-response client backed by `venue_responses.jsonl`
- write replay outputs into a fresh SQLite DB, never mutate the original live DB or bundle

## 5. Required comparator behavior

The Phase 4 comparator must diff live truth vs replay output at the decision level.

Minimum comparison surface:
- order intents, compared by ordered sequence of:
  - `market_id`
  - `token_id`
  - `level`
  - `entry_price`
  - `notional_usdc`
- skip decisions, compared by ordered `reason` sequence against the same event flow
- cancel intents, compared by ordered sequence of:
  - `venue_order_id`
  - `reason`
- final per-order terminal outcome, compared by:
  - terminal state
  - filled quantity
  - keyed by stable order identity (`client_order_id` when present, otherwise the local canonical order id)

## 6. Ordering and tolerance rules

Replay is judged on observable decisions, not on incidental log timing.

Tolerance rules frozen for Phase 4:
- exact millisecond equality of internal log emission timestamps is out of scope
- comparator may tolerate ordering noise among records that share the same causal event timestamp, as long as the observable decision multiset for that timestamp bucket is unchanged
- outside that same-timestamp tolerance, ordered decision sequence must match exactly

This is the only allowed comparator tolerance for Phase 4.

## 7. Explicitly out of scope

Phase 4 replay does **not** attempt to replay or compare:
- wall-clock-dependent telemetry
- logger emission timestamps
- websocket reconnect timing
- ephemeral connection retries
- transport-layer backoff jitter
- operator prompts or approval UX timing
- network-side incidental sequencing that never changes normalized events or decision outputs

Those may be recorded for debugging, but they are not part of the fidelity gate.

## 8. Design constraints for downstream P4 tasks

These constraints are now frozen for the rest of Phase 4:
- `P4-1` must only move decision-path time reads behind an injectable clock, not rewrite telemetry time
- `P4-2` must capture opaque venue responses faithfully, even if the underlying SDK response shape changes later
- `P4-3` must reuse the existing runtime shell rather than introducing a replay-only strategy path
- `P4-4` must fail loudly on unmatched execution calls, because replay-only intents are exactly the drift Phase 4 is meant to expose
- `P4-5` must report divergence in domain terms that are useful for operator audit, not raw JSON blobs only

## 9. P4-0 decisions checklist

- [x] Fidelity is frozen at the decision level, not log-line identity
- [x] Replay must reproduce the same ordered sequence of `OrderIntent`, `SkipDecision`, `CancelOrder`, and terminal order states
- [x] The canonical truth set is `events_log`, terminal `orders`/`fills`, and captured venue responses
- [x] Supporting bundle files such as `market_state_snapshots.jsonl` and `manifest.json` are allowed, but do not expand the comparator truth contract
- [x] Wall-clock telemetry, log timestamps, and ephemeral retry timing are explicitly out of scope
- [x] Replay must run offline and use the same runtime/strategy/execution path as live
- [x] Unmatched replay execution calls are fidelity failures, not best-effort fallbacks
