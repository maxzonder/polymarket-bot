# V2 migration scope and playbook

Tracks decisions for the Polymarket CLOB V1 → V2 cutover (issue #138). Locked in 2026-04-25.

## Cutover window (external)

- Date: **2026-04-28 ~11:00 UTC** (~3 days from this freeze).
- Duration: ~1h venue downtime.
- Effect on us: all open orders wiped; V1 SDK stops functioning post-cutover.
- Authoritative source: https://docs.polymarket.com/v2-migration

## Strategy: cold cutover (option B)

Two paths were on the table in #138 M-0:

- **A. Hot cutover** — land V2 path before 2026-04-28, pause during the maintenance window, resume on V2 immediately after.
- **B. Cold cutover** — accept being offline through migration; do V2 work over the following 1-2 weeks under no time pressure; resume probes when V2 path is production-validated.

**Decision: B.**

Rationale:

- We are still in micro-live probe phase. Missing a few days of probe data costs almost nothing toward our actual goal (Phase 6 edge validation per #137 P6-1).
- A V2 path rushed onto the cutover date risks a worse failure mode than offline: trading on a misconfigured fee model (M-4 dynamic fees), wrong collateral (M-5 USDC.e → pUSD), or a signing path bug. Any of those silently chew real capital.
- Phase 6 has plenty of work that is V1/V2-independent and can run in parallel: P6-0 (Phase 5 exit gate), P6-1 (edge validation on existing captures), P6-3 (sustained-operation hardening), P6-7 (continuous fidelity audit).
- The only Phase 6 track blocked by V2 is P6-4 (stake ramp), which requires production-validated execution anyway. We were not going to ramp during a SDK transition either way.

## Pre-cutover plan (2026-04-25 → 2026-04-28)

| When | Who | What |
|---|---|---|
| 2026-04-25 → 2026-04-27 | bot | finish currently running cap30 probes if data is useful; do not start new probes that would still be live at cutover. |
| 2026-04-27 evening UTC | operator | stop all live probes. Confirm no open orders. Snapshot wallet balances (USDC.e and any pUSD already held). |
| 2026-04-28 ~10:30 UTC | operator | final confirmation no live processes; tail-watch venue announcement channel for actual maintenance start. |
| 2026-04-28 ~11:00 UTC | venue | maintenance starts. Open orders wiped automatically. We do nothing. |
| 2026-04-28 ~12:00 UTC | venue | maintenance closes (estimated). Do not resume probes. |

## Post-cutover plan (V2 work, ~1-2 weeks)

Sequenced per #138:

- M-3 — venue adapter on `py_clob_client_v2`. New order struct (`timestamp`, `metadata`, `builder`); drop `nonce`/`feeRateBps`/`taker`/`expiration`.
- M-4 — fee model rewrite. Replace static `fee_rate_bps` with `getClobMarketInfo()`. Keep V1 compat shim for replay on existing capture bundles.
- M-5 — collateral migration. New `--wrap-polygon-usdc-to-pusd` flag replacing the deprecated `--bridge-polygon-usdc-to-usdce`.
- M-6 — verify SDK fully owns signing (likely no-op since execution_client uses SDK exclusively).
- M-7 — builder code: defer (we don't participate in builder program).
- M-8 — V2 staging dry-run on `clob-v2.polymarket.com`.
- M-9 — N/A under path B (no live cutover playbook needed).
- M-10 — V2 production smoke; one cap30 probe. Phase 4 fidelity must be green.
- M-11 — V1 cleanup (~2 weeks after M-10).

## "V2 path is green" — acceptance criteria

Before resuming Phase 6 probes on V2 (gate before #137 P6-4):

1. M-3 through M-6 merged.
2. M-8 (V2 staging dry-run) executed end-to-end with capture bundle + Phase 4 fidelity green.
3. M-10 (V2 production smoke probe) completed: ≥1 real V2 fill, capture bundle written, fidelity green, no terminal errors, no `ws_subscribe_send_failed` storms, no signing errors.
4. Replay of at least one V1-era capture bundle still passes Phase 4 fidelity (proves the back-compat shim in M-4 works).

If any of the above fails, V1 probes do not resume; investigation continues until green.

## Rollback

V1 SDK is **unrecoverable** post-cutover. Rollback semantics:

- "V2 path broken" → stop trading until V2 fix lands. There is no V1 fallback.
- This is the asymmetry that justifies the cold-cutover choice: extra days offline now are cheaper than scrambling to fix a live V2 bug under time pressure.

## Out of scope for this migration

- Builder program participation (M-7 defers indefinitely).
- Multi-venue abstraction (#117 framing, not Phase 6).
- Refactoring the SDK shim layer for testability — current single-chokepoint adapter (`venue_polymarket/execution_client.py`) is good enough.

## References

- #138 — V2 migration issue (this scope doc lives under it)
- #137 — Phase 6 roadmap (P6-4 unblock depends on this migration being green)
- https://docs.polymarket.com/v2-migration
