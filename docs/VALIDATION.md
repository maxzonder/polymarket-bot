# Dry-Run Validation Harness

## How to run

```bash
python3 scripts/validate_dry_run.py
```

No live API calls, no credentials required. Exits 0 if all pass, 1 if any fail.

## Scenarios covered

| # | Name | Mode | What is validated |
|---|------|------|-------------------|
| 1 | `scanner_entry` | `fast_tp_mode` | Token inside entry zone → `process_scanner_entry()` buys at `best_ask`, NOT at ladder levels |
| 2 | `resting_bid` | `big_swan_mode` | Token above zone → `process_candidate()` places resting bids at all levels below current price |
| 3 | `partial_fill_realism` | `big_swan_mode` | Dry-run fill is capped at `ask_depth_at(bid_price)`; `filled_size` in paper DB matches position `token_quantity` |
| 4 | `tp_pnl_accounting` | `big_swan_mode` | TP fill accumulates `realized_pnl`; winner resolution adds moonbag delta on top (additive, not overwrite) |
| 5 | `losing_resolution` | `big_swan_mode` | Loser resolution sets negative moonbag PnL correctly |

## Invariants checked

- `scanner_entry`: order price == `book.best_ask`; no orders at pre-computed ladder levels
- `resting_bid`: all placed order prices < current token price
- `partial_fill_realism`: position `token_quantity` == `min(order_size, ask_depth)` == 0.5
- `tp_pnl_accounting`: `realized_pnl` after TP = `(tp_price - entry) * tp_qty`; after resolution = TP pnl + moonbag delta
- `losing_resolution`: `realized_pnl` == `(0 - entry_price) * moonbag_qty` < 0

## Implementation notes

- All scenarios use isolated `tempfile.TemporaryDirectory()` — no shared state, no files written to the project
- `get_orderbook` is patched in both `execution.order_manager` and `execution.position_monitor` namespaces
- `ClobClient` runs in `dry_run=True` mode with a temp paper DB
- `execution.order_manager.POSITIONS_DB` is patched at module level before `OrderManager` is constructed, so `_init_db()` never touches the real data directory

## Current limitations

- No concurrent / multi-position interaction testing
- Scanner-entry path is validated via direct call, not via the full `main_loop` screener cycle
- Partial-fill scenario drives `position_monitor` directly (no full poll cycle)
- No WebSocket or real-time fill simulation
- `balance_usdc` is hardcoded (100.0 USDC) — not pulled from live CLOB balance
