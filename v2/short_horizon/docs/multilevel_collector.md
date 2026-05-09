# Multilevel collector runbook

Research infrastructure for GitHub issue #161. This collector is measurement-only: it does not place orders.

## Current state

- Collector schema v2 is live and writes both legacy CSV and SQLite sidecar data.
- Config-driven runs are preferred over long hand-written CLI commands.
- `run_collector_from_config.py` now runs the collector and then builds:
  - heatmap report;
  - post-touch enrichment/report;
  - signal report;
  - compact run summary.
- New runs use explicit horizon buckets: `5m`, `15m`, `1h`, `1d`, `1-7d`, `8-30d`, `>30d`.
- Older DBs still contain old bucket labels such as `15m-ish`; do not reinterpret them in-place.
- For exact crypto 5m/15m analysis, prefer `duration_seconds` as a report axis.
- `spot_snapshots` schema exists, but live spot feed is not connected yet, so crypto spot features remain empty.
- Broad `black_swan_low_tail` discovery is still experimental/debug. A smoke run showed it can pull useful weather temperature markets but also noisy sports/esports markets; do not use it as a long production collector config until filters are split/refined.

## Prod paths

- Repo: `/home/polybot/claude-polymarket`
- Data dir: `/home/polybot/.polybot`
- Collector artifacts: `/home/polybot/.polybot/short_horizon/phase0/`
- Collector logs: `/home/polybot/.polybot/short_horizon/logs/`
- Resolutions DB: `/home/polybot/.polybot/short_horizon/data/market_resolutions.sqlite3`

Always run prod collector commands from the repo with the data dir exported:

```bash
cd /home/polybot/claude-polymarket
export POLYMARKET_DATA_DIR=/home/polybot/.polybot
```

## What the collector writes

`measure_live_depth_and_survival.py` writes:

- legacy CSV rows for touch events;
- SQLite sidecar with:
  - `collection_runs`
  - `touch_events`
  - `book_snapshots`
  - `spot_snapshots`

Important touch-event fields include:

- metadata: `asset_slug`, `universe_mode`, `horizon_bucket`, `duration_seconds`, `event_subtype`, `payoff_type`;
- executable context: `fit_5_shares`, `fit_10_usdc`, average entry prices, required hit rate;
- book context: spread, mid, microprice, top-N bid/ask levels, imbalance top1/top3/top5;
- quality flags: missing depth, crossed book, stale book, wide spread, zero-size level;
- post-touch fields: survival reason, max favorable/adverse move, held/reversal flags;
- spot fields: present in schema, currently empty unless spot feed is connected.

## Config-driven runs

Prefer this path once a preset is stable:

```bash
python3 scripts/run_collector_from_config.py configs/collector/crypto_multi_horizon_majors.yaml
```

Useful configs:

- `configs/collector/crypto_multi_horizon_majors.yaml`
  - crypto majors only: BTC/ETH/SOL/XRP;
  - 5m/15m via `duration_seconds`;
  - good default for validating ETH Down / majors slices on another day.
- `configs/collector/weather_temperature_low_tail.yaml`
  - weather temperature low-tail markets;
  - good for fill/stability now, outcome EV only after markets resolve.
- `configs/collector/black_swan_low_tail.yaml`
  - broad discovery/debug only for now;
  - currently too noisy for blind long runs.

The runner writes artifacts named from `run_id`:

- `<run_id>.csv`
- `<run_id>.sqlite3`
- `<run_id>_heatmap.md/json`
- `<run_id>_post_touch.md/json`
- `<run_id>_signal_report.md/json`
- `<run_id>_summary.md/json`

## Manual collector commands

Crypto wide grid:

```bash
python3 scripts/measure_live_depth_and_survival.py \
  --level-preset crypto_wide \
  --universe-mode crypto_15m \
  --output-csv /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_crypto_wide.csv \
  --output-sqlite /home/polybot/.polybot/short_horizon/phase0/live_depth_survival_crypto_wide.sqlite3 \
  --book-snapshot-interval-ms 1000
```

Crypto multi-horizon majors-only:

```bash
python3 scripts/measure_live_depth_and_survival.py \
  --level-preset crypto_wide \
  --universe-mode crypto_multi_horizon \
  --asset-slug btc,eth,sol,xrp \
  --min-duration-seconds 300 \
  --max-duration-seconds 900 \
  --duration-metric implied_series \
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
  --payoff-type range \
  --payoff-type above \
  --payoff-type below \
  --output-csv /home/polybot/.polybot/short_horizon/phase0/weather_temperature_low_tail.csv \
  --output-sqlite /home/polybot/.polybot/short_horizon/phase0/weather_temperature_low_tail.sqlite3 \
  --book-snapshot-interval-ms 5000
```

Broad black-swan low-tail discovery smoke only:

```bash
timeout 30m python3 scripts/measure_live_depth_and_survival.py \
  --level-preset black_swan_low_tail \
  --universe-mode black_swan_low_tail \
  --no-require-recurrence \
  --duration-metric time_remaining \
  --min-duration-seconds 3600 \
  --max-duration-seconds 604800 \
  --max-seconds-to-end 604800 \
  --market-keyword temperature \
  --market-keyword weather \
  --market-keyword bitcoin \
  --market-keyword ethereum \
  --market-keyword nba \
  --market-keyword nfl \
  --market-keyword winner \
  --market-keyword election \
  --exclude-market-keyword "up or down" \
  --output-csv /home/polybot/.polybot/short_horizon/phase0/black_swan_low_tail_smoke.csv \
  --output-sqlite /home/polybot/.polybot/short_horizon/phase0/black_swan_low_tail_smoke.sqlite3 \
  --book-snapshot-interval-ms 5000
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
- `book_snapshots` grows when `--book-snapshot-interval-ms` is enabled;
- if touches occur, `touch_events` rows include depth/spread/executable/freshness metadata;
- report scripts can read the sidecar and write Markdown/JSON.

If no touches occur during a short smoke window, that is not a collector failure. Treat it as an ingest/schema smoke and run a longer collection window for heatmap analysis.

Expected `timeout` exit status is `124`; this is normal for bounded collector runs.

## Quick inspection on prod

The prod host may not have the `sqlite3` CLI installed. Use Python sqlite instead:

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

## Findings from completed runs

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

- Early smoke showed the broad universe is too noisy for blind long runs.
- It collected useful weather temperature questions, but also sports/esports such as total-kills markets.
- Treat broad black-swan config as discovery/debug until split into narrower configs.

## Recommended next work

- Split broad black-swan discovery into narrower configs:
  - weather temperature low-tail;
  - crypto low-tail / MP-style markets;
  - sports low-tail only after stronger subtype filters;
  - politics/geopolitics low-tail separately.
- Run another crypto majors 12h collector to validate ETH Down and other positive slices across days.
- Re-run weather outcome/EV after weather markets resolve.
- Connect live spot feed so `spot_snapshots` and crypto spot-return features become usable.
- Keep all collector outputs under `/home/polybot/.polybot`, not the git repo.
