# P6-2a dataset overview

P6-2a builds the offline dataset used to replace the empty level-crossing trigger with model-gated selection. No live runs are allowed in this phase.

## Primary touch source

Primary input is the long-running depth/survival collector CSV on prod:

- `/home/polybot/.polybot/short_horizon/phase0/live_depth_survival_20260420_004859.csv`

The collector records touch-level microstructure rows with fields such as:

- identifiers: `probe_id`, `recorded_at`, `market_id`, `condition_id`, `token_id`, `outcome`, `question`
- timing: `touch_time_iso`, `start_time_iso`, `end_time_iso`, `duration_seconds`
- fee/market context: `fees_enabled`, `fee_rate_bps`, `tick_size`
- top-of-book/depth: `best_bid_at_touch`, `best_ask_at_touch`, `ask_level_1..5_price`, `ask_level_1..5_size`, `ask_size_at_touch_level`
- execution-depth probes: `fit_10_usdc`, `fit_50_usdc`, `fit_100_usdc`, `survived_ms`, `end_reason`

Capture-bundle reconstruction remains a sanity-check path for executed trades, not the primary source.

## Market resolution backfill

`P6-2a-2` adds `scripts/backfill_market_resolutions.py`.

Default command on prod:

```bash
cd /home/polybot/claude-polymarket
./.venv/bin/python scripts/backfill_market_resolutions.py \
  --csv-path /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_20260420_004859.csv \
  --output /home/polybot/.polybot/short_horizon/phase0/market_resolutions.sqlite3
```

For local smoke tests:

```bash
./.venv/bin/python scripts/backfill_market_resolutions.py \
  --market-id 2031669 \
  --output /tmp/market_resolutions.sqlite3 \
  --sleep-seconds 0
```

### Gamma endpoint contract

For each `market_id`, the script queries:

```text
GET https://gamma-api.polymarket.com/markets?limit=1&closed=true&active=false&id=<market_id>
```

Fields used from the first returned row:

- `id`
- `conditionId`
- `clobTokenIds`
- `outcomes`
- `outcomePrices`
- `closedTime` / `umaEndDate` / `updatedAt` / `endDate`
- `question`

`clobTokenIds`, `outcomes`, and `outcomePrices` may arrive as JSON-encoded arrays. The script normalizes both list and string forms.

### SQLite output schema

Default repo-relative output path is:

- `v2/short_horizon/data/market_resolutions.sqlite3`

For prod artifacts, prefer the runtime data directory:

- `/home/polybot/.polybot/short_horizon/phase0/market_resolutions.sqlite3`

Table: `market_resolutions`

- `market_id TEXT PRIMARY KEY`
- `condition_id TEXT`
- `token_yes_id TEXT`
- `token_no_id TEXT`
- `outcome_resolved_at TEXT`
- `outcome_token_id TEXT`
- `outcome_price_yes REAL`
- `outcome_price_no REAL`
- `question TEXT`
- `raw_json TEXT`
- `fetched_at TEXT NOT NULL`

Rows with no returned/usable resolution are still persisted with `outcome_token_id = NULL`; the touch dataset builder must drop or report them explicitly.

### Idempotency

Reruns skip already-present `market_id` rows unless `--force` is passed. This allows incremental backfills as new collector rows arrive.

## Next step: canonical touch dataset

`P6-2a-3` will join collector rows against `market_resolutions.sqlite3` and emit the canonical touch dataset with:

- labels: whether the touched token resolved to the winning token
- fee-aware counterfactual cost: min-size notional and estimated taker fees
- explicit drop/reporting for unresolved markets
- cross-check against the 36 executed trades from P6-1 bundles
