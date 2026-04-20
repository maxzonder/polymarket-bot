# short_horizon — MVP short-horizon Polymarket bot

This directory is the home for the **separate short-horizon bot/framework MVP** described in:
- issue `#117` — design brief
- issue `#118` — Phase 0 + Phase 1 implementation roadmap

## Scope

The MVP is intentionally narrowed to the single validated slice from `#115`:
- `15m` exact markets
- `ascending`
- relative bucket `20_40pct`
- price levels `{0.55, 0.65, 0.70}`
- BTC + ETH asset tier only
- fee-aware taker on first touch
- resolution exit
- all-side aggregate first

Current implementation direction after Phase 0:
- Python-first
- `$10` per signal
- no intra-lifecycle re-arm after first touch

This folder is for the new execution-grade bot path.
It is **separate** from:
- the swan bot
- the existing `v2` observation-only experiments

## Phase 0 artifacts

Phase 0 design and decision artifacts live in:
- `docs/phase0/`

Notable Phase 0 clarification already captured there:
- `trigger_definition_alignment.md` documents the exact gap between historical research (`trade-price` first-touch) and live MVP semantics (`best_ask` first-touch), plus why the current historical dataset cannot honestly remove that gap.
- `taker_slippage_stress_check.md` records the completed `+1 tick` pessimistic entry stress on the strict `20_40pct / ascending / {0.55, 0.65, 0.70}` research slice.

These docs are intended to be concrete enough that a developer can implement Phase 1 without reopening core design debates.
