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

## Canonical touch dataset builder

`P6-2a-3` adds `scripts/build_touch_dataset.py`. It joins collector rows against `market_resolutions.sqlite3` and emits a canonical sqlite dataset. We deliberately use sqlite instead of parquet for now because the repo/runtime has no `pyarrow`/`pandas` dependency; the on-disk contract is documented and can be exported later if P6-2b/P6-2c need columnar files.

Default command on prod:

```bash
cd /home/polybot/claude-polymarket
./.venv/bin/python scripts/build_touch_dataset.py \
  --csv-path /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_20260420_004859.csv \
  --resolutions /home/polybot/.polybot/short_horizon/phase0/market_resolutions.sqlite3 \
  --output /home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3 \
  --summary-out /home/polybot/.polybot/short_horizon/phase0/touch_dataset_summary.json
```

Optional executed-trade cross-check:

```bash
./.venv/bin/python scripts/build_touch_dataset.py \
  --csv-path /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_20260420_004859.csv \
  --resolutions /home/polybot/.polybot/short_horizon/phase0/market_resolutions.sqlite3 \
  --output /home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3 \
  --cross-check-report /home/polybot/.polybot/short_horizon/phase0/touch_dataset_cross_check.json \
  --executed-db /home/polybot/.polybot/short_horizon/probes/micro_live_cap30_20260425_105944.sqlite3 \
  --executed-db /home/polybot/.polybot/short_horizon/probes/p6_edge_cap30_20260425_201000.sqlite3 \
  --executed-db /home/polybot/.polybot/short_horizon/probes/p6_edge_cap200_20260425_230202.sqlite3
```

### Dataset schema

Table: `touch_dataset`

- identifiers: `probe_id`, `recorded_at`, `market_id`, `condition_id`, `token_id`, `asset_slug`, `direction`, `question`
- timing: `touch_level`, `touch_time_iso`, `start_time_iso`, `end_time_iso`, `lifecycle_fraction`, `duration_seconds`
- fee context: `fees_enabled`, `fee_rate_bps`, `tick_size`
- touch microstructure: `best_bid_at_touch`, `best_ask_at_touch`, `ask_level_1..5_price`, `ask_level_1..5_size`, `ask_size_at_touch_level`
- execution-depth probes: `fit_10_usdc`, `fit_50_usdc`, `fit_100_usdc`, `survived_ms`, `end_reason`
- labels: `resolves_yes`, `outcome_resolved_at`, `time_to_resolution_ms`, `outcome_token_id`
- counterfactual cost: `would_be_size_at_min_shares`, `would_be_cost_after_min_shares`, `would_be_estimated_fee_usdc`

Label source of truth is token id: `resolves_yes = 1` iff collector `token_id == market_resolutions.outcome_token_id`. Rows whose market has no usable `outcome_token_id` are dropped and counted in the JSON summary. Rows whose token id does not match either known market token are also dropped and counted, instead of being silently mislabeled.

### Latest prod smoke, 2026-04-26

Before the smoke, the resolution backfill was incrementally refreshed for the still-appending collector CSV:

- `requested=4119`, `already_present=4091`, `fetched=28`
- total resolution rows: `4119`; resolved rows: `4106`; unresolved rows: `13`

Builder artifact paths:

- output DB: `/home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3`
- summary JSON: `/home/polybot/.polybot/short_horizon/phase0/touch_dataset_summary.json`
- cross-check JSON: `/home/polybot/.polybot/short_horizon/phase0/touch_dataset_cross_check.json`

Smoke summary:

- collector rows: `21109`
- output rows: `21056`
- resolves_yes rows: `12305`
- resolves_no rows: `8751`
- dropped unresolved rows: `53`
- dropped token mismatches: `0`
- join hit rate over rows with resolved markets: `100.0%`

Executed-trade cross-check against the 36 P6-1 edge-report trades:

- found in dataset: `36/36`
- strict `touch_time_iso±5s` matches: `22/36`
- relaxed same `(market_id, token_id, level)` matches: `14/36`
- outcome matches: `34/36`
- cost exact matches: `18/36`

Interpretation: all 36 executed trades have corresponding collector rows and 34/36 outcomes match directly. The 14 relaxed matches are expected because the collector records the first touch per token/level, while the trading runtime can submit later against an already-touched level. Cost mismatches are expected for pre-PR143/min-size-upscale trades and relaxed matches; the canonical dataset intentionally stores the P6 counterfactual min-size cost, not necessarily historical filled notional.
