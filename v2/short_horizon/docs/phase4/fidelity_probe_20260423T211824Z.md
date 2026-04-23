# P4-6 Fidelity Probe — 2026-04-23T21:18:24Z

## Setup

- Branch: `main` at `909e515` (post PR #126 + test fixes).
- Live capture command:
  ```
  bin/live_runner --mode live --execution-mode dry_run \
      --max-runtime-seconds 1800 \
      --run-id fidelity_probe_20260423T211824Z \
      --capture-dir artifacts/phase4_probe/20260423T211824Z/bundle \
      artifacts/phase4_probe/20260423T211824Z/live.sqlite3
  ```
- Replay + compare:
  ```
  bin/replay_runner --run-id replay_fidelity_probe_20260423T211824Z \
      --compare \
      --comparison-report-path artifacts/phase4_probe/20260423T211824Z/comparison_report.txt \
      artifacts/phase4_probe/20260423T211824Z/bundle \
      artifacts/phase4_probe/20260423T211824Z/replay.sqlite3
  ```
- Wall-clock: `2026-04-23T21:18:29Z` → `2026-04-23T21:48:29Z` (1800s).
- Artifacts (not checked in, ~150MB): `v2/short_horizon/artifacts/phase4_probe/20260423T211824Z/`.

## Bundle manifest

| file | count |
| --- | --- |
| events_log | 142881 |
| market_state_snapshots | 228 |
| venue_responses | 0 |
| orders_final | 0 |
| fills_final | 0 |

Live SQLite event breakdown: BookUpdate 140725, TradeTick 1918, MarketStateUpdate 228, SkipDecision 7, OrderIntent 1, OrderAccepted 1, TimerEvent 1.

## Comparator result

Exit code: `1` (DIFF).

```
Order intents:      ok    (matched=1  mismatched=0 live_only=0 replay_only=0)
Skip decisions:     ok    (matched=5  mismatched=0 live_only=0 replay_only=0)
Cancel intents:     ok    (matched=0  mismatched=0 live_only=0 replay_only=0)
Terminal outcomes:  DIFF  (matched=0  mismatched=0 live_only=0 replay_only=1)
  - replay-only order=44d800a0-d265-5d11-8ab1-7c6a42625eaf: state=rejected, filled_qty=0.000000
```

Decision-path sections (OrderIntent, SkipDecision, CancelOrder) all passed byte-for-byte. All 7 skip decisions and the single order intent reproduce identically in replay — the "matched=5" on skips is bucket count, not record count (comparator groups by event timestamp; 7 records collapsed into 5 timestamp buckets, zero mismatches).

## The one diff — root cause

Live-side terminal state of the sole order is `accepted`; replay-side is `rejected`. Two compounding factors produce the surface diff:

1. **`dry_run` synthesizes accepts without touching the venue.** The capture writer wraps the `ExecutionVenueClient`, which `dry_run` never calls, so `venue_responses.jsonl` is empty. At shutdown the order sits in non-terminal `accepted`, and `_load_bundle_rows` filters `orders_final` to `{FILLED, REJECTED, CANCEL_CONFIRMED, EXPIRED, REPLACED}`, so the order is excluded from the bundle's ground-truth set entirely. That's why `live_only=0`.

2. **Replay forces `execution_mode=live` and uses `CapturedResponseExecutionClient`.** When the replay runtime tried to place the reproduced intent, there was no captured `place_order` record to match, so the order path terminated in `rejected`. The replay DB records the terminal state → it shows up as `replay_only`.

A secondary issue surfaced while diagnosing: `_stable_order_identity` prefers `client_order_id` over `order_id`. `client_order_id` is `uuid5(NAMESPACE_URL, f"{seed}:{intent_id}:{market_id}:{token_id}")` where `seed` is the run_id. Live and replay use different run_ids by construction (`fidelity_probe_...` vs `replay_fidelity_probe_...`), so the client_order_ids differ (`798dedcd...` vs `44d800a0...`) even though the underlying compound `order_id` (`{market}:{token}:{level}:{intent_ts}`) is identical. This means even if the bundle *had* carried the live terminal state, the comparator would have keyed them separately and reported two spurious entries rather than one real mismatch.

## Categorization per P4-0 scope

**Terminal-outcome diff: out of scope.** P4-0 `replay_scope.md` defines fidelity as decision-level — `OrderIntent`, `SkipDecision`, `CancelOrder`, and terminal order states for runs that exercise the real venue path. `dry_run` short-circuits the venue and can't produce a replay-comparable bundle. This probe used `dry_run` to avoid real orders during a fidelity smoke test; the venue trace was empty by construction.

**Decision-path fidelity: clean.** All in-scope sections (intents, skips, cancels) matched with zero divergence on 142k+ events.

## Follow-ups worth filing

1. **P4-6 re-run on `--execution-mode live` with a micro stake cap.** Needs real orders to populate `venue_responses.jsonl`; this is properly Phase 5 territory, but a brief `synthetic`-mode probe (which *does* call the captured venue client) would close the loop on the fidelity contract at zero USD risk. Recommend a follow-up task for a synthetic-mode probe before Phase 5.
2. **Comparator bug — stable order identity.** `_stable_order_identity` in `replay/comparator.py` prefers run-seeded `client_order_id` over the stable compound `order_id`. Terminal-outcome comparison should key on `order_id` and carry `client_order_id` as a payload value for diagnostics only. This is independent of the dry_run asymmetry and would prevent spurious `live_only`/`replay_only` splits on any future live→replay probe.
3. **`dry_run` capture asymmetry.** Either (a) route `dry_run`'s synthetic venue responses through `ReplayCaptureWriter` so the bundle is self-sufficient, or (b) make the replay runner fall back to a synthetic execution path when `venue_responses.jsonl` is empty. Pick one; otherwise `dry_run` bundles remain second-class replay inputs.

## Verdict

P4-6 acceptance: probe ran, decision-path sections matched cleanly on a 30-minute real market-data capture, and the single diff is categorized as out-of-scope-by-P4-0 (dry_run asymmetry) plus a recoverable comparator bug that doesn't touch the decision path. Gate treated as **green for decision fidelity**, with follow-ups #1 and #2 filed against Phase 4 before Phase 5 kickoff.
