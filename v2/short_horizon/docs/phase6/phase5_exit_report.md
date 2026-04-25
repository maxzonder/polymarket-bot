# Phase 5 exit report for Phase 6

Date: 2026-04-25
Issue: #137 / P6-0
Scope: cap15/cap30 Phase 5 micro-live probes for `short_horizon_15m_touch_v1`.

## Decision

GO into P6-1 edge validation.

This is not a GO for stake scaling. P6-4 remains blocked until P6-1 proves edge and P6-3/P6-7 sustained-operation gates are green. The separate CLOB V2 migration issue (#138) also remains a stake-ramp dependency before P6-4.

Rationale:

- The final post-fix Phase 5 probe (`live_cap30_20260425_105944`) completed the intended 6h autonomous run, wrote a full capture bundle, and passed replay fidelity with zero mismatches.
- The WS subscribe-send race that killed the earlier cap30 probe was fixed by #136.
- The anti-chase/rejected-attempt loophole observed in older runs was fixed by #135 and #142.
- The venue minimum-size reject problem observed in the final cap30 run was fixed by #143 before this Phase 6 handoff.
- Remaining evidence quality is enough to start P6-1 sample collection, but not enough to scale stake.

## Runs included

Actual DB-backed cap15/cap30 probes found on production:

- `live_cap15_20260424_130208`
  - Config hash: `p5-3-cap15`
  - DB: `/home/polybot/.polybot/short_horizon/probes/micro_live_cap15_20260424_130208.sqlite3`
  - Log: `/home/polybot/.polybot/short_horizon/logs/micro_live_cap15_20260424_130208.log`
  - Capture bundle: `/home/polybot/.polybot/capture_bundles/micro_live_cap15_20260424_130208`
- `live_cap30_20260424_212705`
  - Config hash: `p5-cap30-after-pr135`
  - DB: `/home/polybot/.polybot/short_horizon/probes/micro_live_cap30_20260424_212705.sqlite3`
  - Log: `/home/polybot/.polybot/short_horizon/logs/micro_live_cap30_20260424_212705.log`
  - Capture bundle: `/home/polybot/.polybot/capture_bundles/micro_live_cap30_20260424_212705`
- `live_cap30_20260425_085054`
  - Config hash: `p5-cap30-after-pr136`
  - DB: `/home/polybot/.polybot/short_horizon/probes/micro_live_cap30_20260425_085054.sqlite3`
  - Log: `/home/polybot/.polybot/short_horizon/logs/micro_live_cap30_20260425_085054.log`
  - Capture bundle: `/home/polybot/.polybot/capture_bundles/micro_live_cap30_20260425_085054`
- `live_cap30_20260425_105944`
  - Config hash: `p5-cap30-after-pr142`
  - DB: `/home/polybot/.polybot/short_horizon/probes/micro_live_cap30_20260425_105944.sqlite3`
  - Log: `/home/polybot/.polybot/short_horizon/logs/micro_live_cap30_20260425_105944.log`
  - Capture bundle: `/home/polybot/.polybot/capture_bundles/micro_live_cap30_20260425_105944`

Failed launcher attempts such as `micro_live_cap15_20260424_130107`, `micro_live_cap15_20260424_130142`, `micro_live_cap30_20260424_212650`, and `micro_live_cap30_20260425_085030` did not produce DB-backed live runs and are excluded from trading/fidelity aggregation.

## Aggregate execution summary

Across the four DB-backed cap15/cap30 runs:

- Order intents: `51`
- Accepted orders: `35`
- Final filled orders: `35`
- Final rejected orders: `16`
- Filled notional / cost basis: `33.9106 USDC`
- Resolved-position estimated PnL: `-3.49683 USDC`
- Unresolved filled cost at stop: `1.0075 USDC`
- `ws_subscribe_send_failed` persisted event count: `0`

By run:

- `live_cap15_20260424_130208`
  - Runtime: `2026-04-24T13:02:11.001Z` -> `2026-04-24T19:02:11.422Z`, approximately 6h.
  - Intents / accepted / fills / rejects: `18 / 14 / 14 / 4`.
  - Filled notional: `13.6055 USDC`.
  - Resolved-position estimated PnL: `-5.40383 USDC`.
  - Unresolved filled cost: `0 USDC`.
  - Capture counts: `events_log=7,802,394`, `orders_final=18`, `fills_final=14`, `venue_responses=18`, `markets=52`.
  - Anti-chase status: FAIL in this historical run. Multiple same-market attempts occurred before the later anti-chase/rejected-attempt fixes.
  - WS health: no `ws_subscribe_send_failed` events; log warnings were ordinary disconnect/reconnect noise.
- `live_cap30_20260424_212705`
  - Runtime: `2026-04-24T21:27:06.821Z` -> `2026-04-24T22:01:56.995Z`, approximately 35m.
  - Intents / accepted / fills / rejects: `3 / 3 / 3 / 0`.
  - Filled notional: `3.0243 USDC`.
  - Resolved-position estimated PnL: `-0.44400 USDC`.
  - Unresolved filled cost: `0 USDC`.
  - Capture counts: `events_log=326,492`, `orders_final=3`, `fills_final=3`, `venue_responses=3`, `markets=8`.
  - Stop reason: fatal `LiveEventSource component failed: market_refresh_consumer: no close frame received or sent`.
  - Anti-chase status: PASS.
  - WS health: no `ws_subscribe_send_failed` events persisted; fatal WS source failure was the blocker fixed by #136.
- `live_cap30_20260425_085054`
  - Runtime: `2026-04-25T08:51:03.947Z` -> `2026-04-25T10:58:00.639Z`, approximately 127m.
  - Intents / accepted / fills / rejects: `7 / 5 / 5 / 2`.
  - Filled notional: `4.3887 USDC`.
  - Resolved-position estimated PnL: `+1.74097 USDC`.
  - Unresolved filled cost at manual stop: `1.0075 USDC`.
  - Capture counts: `events_log=884,425`, `orders_final=7`, `fills_final=5`, `venue_responses=7`, `markets=20`.
  - Stop reason: manually stopped to relaunch after #142.
  - Anti-chase status: FAIL by Phase 5 final semantics. A rejected order on market `2066569` did not consume the per-market attempt cap, allowing a later same-market filled attempt.
  - WS health: no `ws_subscribe_send_failed` events; disconnect/reconnect noise did not crash the run.
- `live_cap30_20260425_105944`
  - Runtime: `2026-04-25T10:59:46.370Z` -> `2026-04-25T16:59:46.808Z`, approximately 6h.
  - Intents / accepted / fills / rejects: `23 / 13 / 13 / 10`.
  - Filled notional: `12.8921 USDC`.
  - Resolved-position estimated PnL: `+0.61003 USDC`.
  - Unresolved filled cost: `0 USDC`.
  - Capture counts: `events_log=2,889,396`, `orders_final=23`, `fills_final=13`, `venue_responses=23`, `markets=50`.
  - Completion: configured max-runtime completion.
  - Anti-chase status: PASS after #142.
  - WS health: no `ws_subscribe_send_failed` events; disconnect/reconnect noise did not crash the run.

## Fidelity status

P4 fidelity was checked against the available capture bundles as follows:

- `live_cap30_20260424_212705`: MATCH.
  - Replay report: `/home/polybot/.polybot/replays/p6_exit_quiet_micro_live_cap30_20260424_212705_20260425_195928/comparison_report.txt`.
  - Result: order intents matched `3`, skip decisions matched `12`, terminal outcomes matched `3`, zero mismatches.
- `live_cap30_20260425_105944`: MATCH.
  - Replay report: `/home/polybot/.polybot/replays/p5_audit_live_cap30_20260425_105944_20260425_184104/comparison_report.txt`.
  - Result: order intents matched `23`, skip decisions matched `151`, terminal outcomes matched `23`, zero mismatches.
- `live_cap30_20260425_085054`: intentionally superseded, not a green current-code fidelity gate.
  - Current-code replay exits before consuming the full captured execution trace, with 5 `place_order` records remaining.
  - This is expected after #142 because the live run used the older semantics where rejected same-market attempts did not consume `max_orders_per_market_per_run`; current code blocks those attempts.
  - The run is retained for financial aggregation and regression context, but it is not used as the Phase 5 exit gate.
- `live_cap15_20260424_130208`: historical pre-anti-chase run, not promoted as a current Phase 5 fidelity gate.
  - The run produced a complete bundle and is included in aggregate execution/PnL, but it predates #135/#142 and contains same-market attempt violations that Phase 5 later fixed.
  - Replaying this bundle under final Phase 5 semantics is not the right acceptance signal; the final post-fix cap30 run is the acceptance signal.

Therefore the strict statement is:

- Every run that can be evaluated under the final Phase 5 semantics and is eligible for the exit gate is green.
- Older exploratory runs that directly motivated later fixes are included in aggregation, but are explicitly not used as green gate evidence.

## Open blockers

Closed before Phase 6 handoff:

- #135: same-market anti-chase guard.
- #136: best-effort live subscription sends / WS subscribe race hardening.
- #142: rejected order attempts count toward per-market attempt caps.
- #143: live BUY orders upscale to venue minimum shares instead of producing repeated minimum-size rejects.

Known remaining dependency:

- #138: CLOB V2 migration is still open and blocks the P6-4 stake ramp, but it does not block starting P6-1 edge validation at the current micro-live size.

## P6-0 checklist mirror

- [x] Aggregated cap15/cap30 run results: intents, fills, rejects, PnL, anti-chase observations, and WS subscribe-send counts.
- [x] Confirmed final eligible Phase 5 fidelity gate is green: `live_cap30_20260425_105944` replay MATCH with zero mismatches.
- [x] Confirmed the earlier WS-fatal cap30 run replays deterministically and its operational blocker was fixed by #136.
- [x] Identified historical runs that are not green under final semantics and documented why they are superseded rather than scaling evidence.
- [x] Confirmed no closed Phase 5 bug (#135/#136/#142/#143) remains open as a blocker to P6-1.
- [x] Decision: GO into P6-1 edge validation; NO-GO for stake scaling until P6-1/P6-3/P6-7 and #138 are satisfied.
