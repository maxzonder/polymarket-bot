# v2 — short-horizon bot workspace

`v2/` is now reserved for the **new short-horizon Polymarket bot/framework**.

This is a separate bot path with a separate strategy and separate architecture.
It should not inherit accidental structure from the legacy bot or older observation-only experiments.

Current active subdirectory:
- `short_horizon/` — MVP short-horizon bot work from issues `#117` and `#118`

## Current focus

The MVP is intentionally narrowed to the validated slice from `#115`:
- `15m` exact markets
- `ascending`
- relative bucket `20_40pct`
- price levels `{0.55, 0.65, 0.70}`
- fee-aware taker on first touch
- resolution exit
- all-side aggregate first

## Phase 0 artifacts

Current Phase 0 design/implementation artifacts live under:
- `short_horizon/docs/phase0/`
