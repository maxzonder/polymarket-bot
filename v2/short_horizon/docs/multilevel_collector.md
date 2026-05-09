# Multilevel collector runbook

Research infrastructure for GitHub issue #161. The collector is measurement-only: it does not place orders.

## Finish-line definition

The collector is a strategy-specific data factory, not a strategy and not a trading bot.

Its job is to produce a standard data package for a configured research universe:

- which markets were available;
- what the live book looked like;
- whether a hypothetical entry was executable;
- what happened after a level touch;
- what the market resolved to;
- which context features were present at entry time.

Short runs are smoke/validation runs for this data factory. They are not the end product.

## Current status

- Collector schema v2 is live.
- Config-driven runs are the canonical path.
- `run_collector_from_config.py` runs the collector and then builds the standard report package.
- New rows use explicit horizon buckets: `5m`, `15m`, `1h`, `1d`, `1-7d`, `8-30d`, `>30d`.
- Older DBs may still contain `15m-ish`; do not rewrite those in-place.
- Binance spot polling is connected for crypto configs with `spot_feed: "binance"`.
- Broad `black_swan_low_tail` is now discovery/debug only; clean strategy configs exist for weather, crypto low-tail, and politics/geopolitics.
- Reports handle mixed `None`/string axes after the sort fix.

## Prod paths

- Repo: `/home/polybot/claude-polymarket`
- Data dir: `/home/polybot/.polybot`
- Collector artifacts: `/home/polybot/.polybot/short_horizon/phase0/`
- Collector logs: `/home/polybot/.polybot/short_horizon/logs/`
- Resolutions DB: `/home/polybot/.polybot/short_horizon/data/market_resolutions.sqlite3`

Run prod commands from the repo with the data dir exported:

```bash
cd /home/polybot/claude-polymarket
export POLYMARKET_DATA_DIR=/home/polybot/.polybot
```

## Architecture

1. **Discovery layer**
   - Finds markets for a configured universe.
   - Filters by duration, recurrence, asset, keywords, payoff type, and time-to-end.

2. **Live book collector**
   - Subscribes to Polymarket market websocket.
   - Writes periodic book snapshots when enabled.
   - Detects configured ask-level touches.

3. **Executable context**
   - Records spread/depth/freshness flags.
   - Computes fit for 5 shares and 10/50/100 USDC.
   - This is the basic “could we actually enter?” layer.

4. **Spot context**
   - Optional Binance ticker polling for crypto assets.
   - Writes `spot_snapshots` and spot fields on touch rows.
   - Current supported assets: BTC, ETH, SOL, XRP, BNB, DOGE.

5. **Post-touch context**
   - Enriches touches with hold/reversal and move metrics.
   - Helps separate valid entry states from microprint/noise.

6. **Outcome/EV join**
   - After markets resolve, joins collector rows to outcomes.
   - Computes realized hypothetical hold-to-resolution EV.

7. **Strategy reports**
   - Summarize candidate slices/rules.
   - Do not trade and should not be treated as strategy approval.

## What the collector writes

`measure_live_depth_and_survival.py` writes:

- legacy CSV rows for touch events;
- SQLite sidecar tables:
  - `collection_runs`
  - `touch_events`
  - `book_snapshots`
  - `spot_snapshots`

Important `touch_events` fields:

- metadata: `asset_slug`, `universe_mode`, `horizon_bucket`, `duration_seconds`, `event_subtype`, `payoff_type`;
- executable context: `fit_5_shares`, `fit_10_usdc`, average entry prices, required hit rate;
- book context: spread, mid, microprice, top-N bid/ask levels, imbalance top1/top3/top5;
- quality flags: missing depth, crossed book, stale book, wide spread, zero-size level;
- post-touch fields: survival reason, max favorable/adverse move, held/reversal flags;
- spot fields: spot price/source/latency and 30s/1m/3m/5m returns when spot feed is enabled.

## Canonical run path

Prefer config-driven runs:

```bash
python3 scripts/run_collector_from_config.py configs/collector/crypto_multi_horizon_majors.yaml
```

The runner writes artifacts named from `run_id`:

- `<run_id>.csv`
- `<run_id>.sqlite3`
- `<run_id>_heatmap.md/json`
- `<run_id>_post_touch.md/json`
- `<run_id>_signal_report.md/json`
- `<run_id>_summary.md/json`

If `timeout` stops the collector, exit status `124` is normal.

## Strategy configs

### `configs/collector/crypto_multi_horizon_majors.yaml`

Purpose: 5m/15m crypto majors validation.

- universe: `crypto_multi_horizon`
- assets: BTC/ETH/SOL/XRP
- levels: `crypto_wide`
- duration: 300–900s via `duration_metric=implied_series`
- spot feed: Binance enabled
- report axes: `duration_seconds`, `asset_slug`, `outcome`

Use for validating ETH Down / majors slices across another day.

### `configs/collector/weather_temperature_low_tail.yaml`

Purpose: low-tail temperature markets.

- universe: `weather_temperature`
- levels: `black_swan_low_tail`
- duration: 1h–7d by time remaining
- payoff types: `exact`, `range`, `above`, `below`
- report axes: `horizon_bucket`, `event_subtype`, `payoff_type`, `touch_level`

Use for fill/stability now; EV only after markets resolve.

### `configs/collector/black_swan_crypto_low_tail.yaml`

Purpose: crypto low-tail / MP-style discovery.

- universe: `black_swan_low_tail`
- assets: BTC/ETH/SOL/XRP
- levels: `black_swan_low_tail`
- excludes short-horizon `up or down`
- spot feed: Binance enabled
- report axes: `horizon_bucket`, `asset_slug`, `payoff_type`, `touch_level`

Use as the clean crypto black-swan collector, not the broad debug config.

### `configs/collector/black_swan_politics_low_tail.yaml`

Purpose: politics/geopolitics low-tail discovery.

- keywords: election, major politicians, war/ceasefire/geopolitics terms
- excludes obvious esports map/total-kills noise
- duration: up to 30d by time remaining
- report axes: `horizon_bucket`, `payoff_type`, `event_subtype`, `touch_level`

Use for narrow politics/geopolitics smoke before any long run.

### `configs/collector/black_swan_low_tail.yaml`

Purpose: broad discovery/debug only.

- intentionally short timeout: 30m
- known to mix useful weather markets with sports/esports noise
- do not use for blind long runs

## Manual command examples

Crypto multi-horizon majors:

```bash
python3 scripts/measure_live_depth_and_survival.py \
  --level-preset crypto_wide \
  --universe-mode crypto_multi_horizon \
  --asset-slug btc,eth,sol,xrp \
  --min-duration-seconds 300 \
  --max-duration-seconds 900 \
  --duration-metric implied_series \
  --spot-feed binance \
  --spot-asset-slug btc,eth,sol,xrp \
  --output-csv /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_crypto_multi_horizon_majors.csv \
  --output-sqlite /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_crypto_multi_horizon_majors.sqlite3 \
  --book-snapshot-interval-ms 1000
```

Weather temperature low-tail:

```bash
python3 scripts/measure_live_depth_and_survival.py \
  --level-preset black_swan_low_tail \
  --universe-mode weather_temperature \
  --no-require-recurrence \
  --duration-metric time_remaining \
  --min-duration-seconds 3600 \
  --max-duration-seconds 604800 \
  --max-seconds-to-end 604800 \
  --market-keyword temperature \
  --market-keyword "highest temperature" \
  --market-keyword "low temperature" \
  --payoff-type exact \
  --payoff-type range \
  --payoff-type above \
  --payoff-type below \
  --output-csv /home/polybot/.polybot/short_horizon/phase0/weather_temperature_low_tail.csv \
  --output-sqlite /home/polybot/.polybot/short_horizon/phase0/weather_temperature_low_tail.sqlite3 \
  --book-snapshot-interval-ms 5000
```

Crypto black-swan low-tail:

```bash
python3 scripts/run_collector_from_config.py configs/collector/black_swan_crypto_low_tail.yaml
```

Politics/geopolitics low-tail:

```bash
python3 scripts/run_collector_from_config.py configs/collector/black_swan_politics_low_tail.yaml
```

Broad discovery/debug smoke:

```bash
python3 scripts/run_collector_from_config.py configs/collector/black_swan_low_tail.yaml
```

## Report commands

Build heatmap:

```bash
python3 scripts/build_collector_heatmap.py \
  --input-sqlite /home/polybot/.polybot/short_horizon/phase0/<run_id>.sqlite3 \
  --output-md /home/polybot/.polybot/short_horizon/phase0/<run_id>_heatmap.md \
  --output-json /home/polybot/.polybot/short_horizon/phase0/<run_id>_heatmap.json
```

Build post-touch enrichment:

```bash
python3 scripts/enrich_collector_post_touch.py \
  --input-sqlite /home/polybot/.polybot/short_horizon/phase0/<run_id>.sqlite3 \
  --output-md /home/polybot/.polybot/short_horizon/phase0/<run_id>_post_touch.md \
  --output-json /home/polybot/.polybot/short_horizon/phase0/<run_id>_post_touch.json
```

Join resolved outcomes and EV:

```bash
python3 scripts/build_collector_outcome_ev.py \
  --input-sqlite /home/polybot/.polybot/short_horizon/phase0/<run_id>.sqlite3 \
  --resolutions-sqlite /home/polybot/.polybot/short_horizon/data/market_resolutions.sqlite3 \
  --output-md /home/polybot/.polybot/short_horizon/phase0/<run_id>_outcome_ev.md \
  --output-json /home/polybot/.polybot/short_horizon/phase0/<run_id>_outcome_ev.json \
  --backfill-resolutions \
  --axis duration_seconds \
  --axis asset_slug \
  --axis outcome
```

Build combined signal report:

```bash
python3 scripts/build_collector_signal_report.py \
  --input-sqlite /home/polybot/.polybot/short_horizon/phase0/<run_id>.sqlite3 \
  --resolutions-sqlite /home/polybot/.polybot/short_horizon/data/market_resolutions.sqlite3 \
  --output-md /home/polybot/.polybot/short_horizon/phase0/<run_id>_signal_report.md \
  --output-json /home/polybot/.polybot/short_horizon/phase0/<run_id>_signal_report.json \
  --backfill-resolutions \
  --max-spread 0.10 \
  --axis duration_seconds \
  --axis asset_slug \
  --axis outcome
```

Build compact run summary:

```bash
python3 scripts/build_collector_run_summary.py \
  --input-sqlite /home/polybot/.polybot/short_horizon/phase0/<run_id>.sqlite3 \
  --signal-json /home/polybot/.polybot/short_horizon/phase0/<run_id>_signal_report.json \
  --output-md /home/polybot/.polybot/short_horizon/phase0/<run_id>_summary.md \
  --output-json /home/polybot/.polybot/short_horizon/phase0/<run_id>_summary.json
```

## Smoke acceptance

A smoke run is good enough if:

- process starts and subscribes to market WS;
- SQLite sidecar is created;
- `collection_runs` has one row;
- `book_snapshots` grows when periodic snapshots are enabled;
- if touches occur, `touch_events` rows include depth/spread/executable/freshness metadata;
- if spot feed is enabled, `spot_snapshots` becomes non-zero;
- report scripts can read the sidecar and write Markdown/JSON.

If no touches occur during a short smoke window, that is not a collector failure. Treat it as an ingest/schema smoke.

## Quick inspection on prod

The prod host may not have the `sqlite3` CLI installed. Use Python sqlite:

```bash
python3 - <<'PY'
import sqlite3
from pathlib import Path

path = Path('/home/polybot/.polybot/short_horizon/phase0/<run_id>.sqlite3')
print('exists', path.exists(), 'size', path.stat().st_size if path.exists() else 0)
conn = sqlite3.connect(path)
for table in ['collection_runs', 'touch_events', 'book_snapshots', 'spot_snapshots', 'post_touch_enrichment']:
    try:
        count = conn.execute(f'select count(*) from {table}').fetchone()[0]
    except Exception as exc:
        count = exc
    print(table, count)
conn.close()
PY
```

Sample market questions:

```bash
python3 - <<'PY'
import sqlite3
conn = sqlite3.connect('/home/polybot/.polybot/short_horizon/phase0/<run_id>.sqlite3')
for row in conn.execute('''
    select question, outcome, touch_level, horizon_bucket, payoff_type, event_subtype
    from touch_events
    limit 20
'''):
    print(row)
conn.close()
PY
```

## Completed validation runs

`crypto_wide_8h_20260509_005540`:

- 4458 touch events, 220762 book snapshots, 0 spot snapshots.
- Overall 5-share hold-to-resolution EV was strongly negative.
- Majors were much cleaner than BNB/DOGE/HYPE alts.
- Positive slices existed, notably SOL Up and ETH Down, but raw touch-buy-hold is not a trading signal.

`crypto_multi_horizon_majors_4h_20260509_110331`:

- 4266 touch events, 152448 book snapshots, 0 spot snapshots.
- Majors-only was cleaner than all-crypto, but raw 5-share EV remained negative overall.
- Strongest normal-trade slice was 5m ETH Down.
- Offline prototype rule `ETH Down + 5s held + no 5s reversal` improved the slice, but it needs validation on other days/runs before any trading work.

`weather_temperature_low_tail_2h_20260509_152614`:

- 121 touch events, 162031 book snapshots, 0 spot snapshots.
- All horizon bucket `1-7d`.
- Outcome EV unavailable at collection time because markets had not resolved.
- Useful for fill/stability; profitability requires later outcome join.

`black_swan_low_tail_smoke_20260509_202701`:

- 134 touch events, 133078 book snapshots, 134 post-touch rows.
- Useful as broad discovery smoke.
- Showed useful weather temperature questions plus noisy sports/esports markets.
- This is why broad black-swan remains debug-only and clean configs are preferred.

`spot_smoke_20260509_211100`:

- 1 touch event, 1 book snapshot, 14 spot snapshots.
- Spot assets: BTC/ETH/SOL/XRP.
- Confirms Binance spot feed is connected for crypto collector runs.

## Collector infra status

Done enough as a data factory:

- stable strategy-config runner;
- standard artifact package;
- executable book context;
- post-touch enrichment;
- outcome/EV join;
- compact run summary;
- clean strategy configs for crypto majors, weather, crypto black-swan, politics/geopolitics;
- Binance spot snapshots for crypto runs.

Remaining work is strategy validation, not collector plumbing:

- crypto majors 12–24h validation run with spot enabled;
- weather outcome/EV after resolution;
- narrow black-swan smoke runs from clean configs;
- only then promote candidate slices into strategy research/trading code.
