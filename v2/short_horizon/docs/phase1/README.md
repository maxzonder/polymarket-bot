# Phase 1 skeleton

Phase 1 now has a real code skeleton under:
- `v2/short_horizon/short_horizon/`

Current scope of the skeleton:
- canonical config for the MVP slice
- normalized market-state and book-update events
- lifecycle bucket helper
- first-touch tracker with no intra-lifecycle re-arm
- strategy engine that can turn `BookUpdate` + `MarketStateUpdate` into `OrderIntent`
- runtime persistence path with:
  - in-memory store for fast tests
  - SQLite-backed store aligned to the Phase 0 schema for runs / markets / orders / events_log / strategy_state

This is intentionally the minimum vertical slice needed to start wiring live adapters and replay adapters into the same event model.

What is deliberately not implemented yet:
- venue adapter
- full production persistence lifecycle for fills / execution updates / reconciliation transitions
- execution client
- reconciliation state machine
- replay file reader
- fee lookup integration beyond freshness gating fields
