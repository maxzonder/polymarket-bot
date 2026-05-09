# Multilevel collector runbook

Research infrastructure for #161. This collector is measurement-only: it does not place orders.

## What it writes

`measure_live_depth_and_survival.py` writes:

- legacy CSV rows for touch events;
- SQLite sidecar with:
  - `collection_runs`
  - `touch_events`
  - `book_snapshots`
  - `spot_snapshots`

The current default discovery remains narrow crypto short-horizon unless explicitly changed. Universe modes are metadata/config groundwork; they do not by themselves make the collector trade or broaden all discovery logic.

## Useful presets

Crypto wide grid:

```bash
python3 scripts/measure_live_depth_and_survival.py \
  --level-preset crypto_wide \
  --universe-mode crypto_15m \
  --output-csv /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_crypto_wide.csv \
  --output-sqlite /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_crypto_wide.sqlite3 \
  --book-snapshot-interval-ms 1000
```

Black-swan low-tail grid:

```bash
python3 scripts/measure_live_depth_and_survival.py \
  --level-preset black_swan_low_tail \
  --universe-mode black_swan_low_tail \
  --output-csv /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_low_tail.csv \
  --output-sqlite /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_low_tail.sqlite3 \
  --book-snapshot-interval-ms 5000
```

Combined crypto wide + low-tail grid:

```bash
python3 scripts/measure_live_depth_and_survival.py \
  --level-preset crypto_wide_low_tail \
  --universe-mode crypto_15m \
  --output-csv /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_crypto_wide_low_tail.csv \
  --output-sqlite /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_crypto_wide_low_tail.sqlite3 \
  --book-snapshot-interval-ms 1000
```

Crypto multi-horizon majors-only expansion, useful after the first 15m EV join showed wide-spread alt contamination:

```bash
python3 scripts/measure_live_depth_and_survival.py \
  --level-preset crypto_wide \
  --universe-mode crypto_multi_horizon \
  --asset-slug btc,eth,sol,xrp \
  --min-duration-seconds 300 \
  --max-duration-seconds 900 \
  --output-csv /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_crypto_multi_horizon_majors.csv \
  --output-sqlite /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_crypto_multi_horizon_majors.sqlite3 \
  --book-snapshot-interval-ms 1000
```

## Short smoke run

Use a small `--max-events` cap for a quick non-trading smoke check:

```bash
python3 scripts/measure_live_depth_and_survival.py \
  --level-preset crypto_wide \
  --universe-mode crypto_15m \
  --output-csv /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide.csv \
  --output-sqlite /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide.sqlite3 \
  --book-snapshot-interval-ms 1000 \
  --max-events 5
```

If the market is quiet and no touches arrive, use a timeout wrapper for an ingest/schema smoke:

```bash
timeout 15m python3 scripts/measure_live_depth_and_survival.py \
  --level-preset crypto_wide \
  --universe-mode crypto_15m \
  --output-csv /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide.csv \
  --output-sqlite /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide.sqlite3 \
  --book-snapshot-interval-ms 1000
```

## Build heatmap report

```bash
python3 scripts/build_collector_heatmap.py \
  --input-sqlite /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide.sqlite3 \
  --output-md /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide_heatmap.md \
  --output-json /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide_heatmap.json
```

Custom axes:

```bash
python3 scripts/build_collector_heatmap.py \
  --input-sqlite /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide.sqlite3 \
  --axis touch_level \
  --axis asset_slug \
  --axis outcome \
  --axis horizon_bucket
```

Executable-only report:

```bash
python3 scripts/build_collector_heatmap.py \
  --input-sqlite /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide.sqlite3 \
  --executable-only
```

## Run from config

Prefer config-driven runs once a preset is stable:

```bash
python3 scripts/run_collector_from_config.py configs/collector/crypto_multi_horizon_majors.yaml
```

Useful configs:

- `configs/collector/crypto_multi_horizon_majors.yaml`
- `configs/collector/weather_temperature_low_tail.yaml`
- `configs/collector/black_swan_low_tail.yaml`

## Join resolved outcomes and EV

Use this after the collected markets have resolved. It backfills market outcomes from Gamma into a local resolutions DB, joins them to `touch_events`, and reports realized hold-to-resolution EV for hypothetical 5-share and 10 USDC entries.

```bash
python3 scripts/build_collector_outcome_ev.py \
  --input-sqlite /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide.sqlite3 \
  --resolutions-sqlite /home/polybot/.polybot/short_horizon/data/market_resolutions.sqlite3 \
  --output-md /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide_outcome_ev.md \
  --output-json /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide_outcome_ev.json \
  --backfill-resolutions \
  --axis touch_level \
  --axis asset_slug \
  --axis outcome
```

## Build combined signal report

Use this after post-touch enrichment and outcome resolution are available. It applies normal-trade filters (`fit_5_shares`, good freshness/depth flags, max spread), then combines EV, spread, and post-touch hold/reversal metrics.

```bash
python3 scripts/build_collector_signal_report.py \
  --input-sqlite /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide.sqlite3 \
  --resolutions-sqlite /home/polybot/.polybot/short_horizon/data/market_resolutions.sqlite3 \
  --output-md /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide_signal_report.md \
  --output-json /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide_signal_report.json \
  --backfill-resolutions \
  --max-spread 0.10 \
  --axis duration_seconds \
  --axis asset_slug \
  --axis outcome
```

## Quick SQLite inspection

```bash
sqlite3 /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide.sqlite3 \
  "select 'runs', count(*) from collection_runs union all select 'touch_events', count(*) from touch_events union all select 'book_snapshots', count(*) from book_snapshots union all select 'spot_snapshots', count(*) from spot_snapshots;"
```

Check that metadata/freshness fields are present:

```bash
sqlite3 /home/polybot/.polybot/short_horizon/phase0/smoke_crypto_wide.sqlite3 \
  "select touch_level, asset_slug, horizon_bucket, fit_5_shares, spread_at_touch, missing_depth_flag_at_touch, book_stale_flag_at_touch from touch_events limit 5;"
```

## Smoke acceptance

A smoke run is good enough if:

- process starts and subscribes to market WS;
- SQLite sidecar is created;
- `collection_runs` has one row;
- `book_snapshots` grows when `--book-snapshot-interval-ms` is enabled;
- if touches occur, `touch_events` rows include depth/spread/executable/freshness metadata;
- `build_collector_heatmap.py` can read the sidecar and write Markdown/JSON.

If no touches occur during a short smoke window, that is not a collector failure. Treat it as an ingest/schema smoke and run a longer collection window for heatmap analysis.
