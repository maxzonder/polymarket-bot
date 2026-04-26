# P6-2b spot features

P6-2b adds spot-derived fair-value features to the canonical touch dataset before model training. No live trading is allowed in this phase.

## P6-2b-1 offline spot backfill

`P6-2b-1` adds `scripts/backfill_spot_features.py`.

Default prod command:

```bash
cd /home/polybot/claude-polymarket
./.venv/bin/python scripts/backfill_spot_features.py \
  --touch-dataset /home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3 \
  --output /home/polybot/.polybot/short_horizon/phase0/spot_features.sqlite3 \
  --cache /home/polybot/.polybot/short_horizon/phase0/spot_ohlc_cache.sqlite3 \
  --summary-out /home/polybot/.polybot/short_horizon/phase0/spot_features_summary.json
```

### Inputs and outputs

Input table: `touch_dataset` from `P6-2a-3`.

Output table: `spot_features`.

- identifiers: `probe_id`, `asset_slug`, `spot_symbol`
- alignment: `touch_time_iso`, `nearest_spot_bar_time_iso`, `nearest_spot_latency_ms`, `start_time_iso`, `nearest_start_bar_time_iso`, `nearest_start_latency_ms`, `end_time_iso`
- market context: `direction`, `best_ask_at_touch`
- spot features: `spot_at_touch`, `spot_at_window_start`, `spot_return_since_window_start`, `spot_realized_vol_recent_60s`, `spot_realized_vol_recent_300s`, `spot_velocity_recent_30s`
- fair-value features: `seconds_to_resolution`, `spot_implied_prob`, `spot_implied_prob_minus_market_prob`
- provenance: `source`

Cache table: `spot_ohlc_cache`, keyed by `(asset_slug, symbol, open_time_ms)`.

### Provider contract

Primary provider for liquid symbols is Binance spot 1-second klines:

```text
GET https://api.binance.com/api/v3/klines?symbol=<SYMBOL>&interval=1s&startTime=<ms>&endTime=<ms>&limit=1000
```

Mappings:

- `btc` -> `BTCUSDT`
- `eth` -> `ETHUSDT`
- `bnb` -> `BNBUSDT`
- `doge` -> `DOGEUSDT`
- `xrp` -> `XRPUSDT`
- `sol` -> `SOLUSDT`

Fallback provider for Hyperliquid/HYPE is CryptoCompare minute bars because Binance spot/alpha did not expose HYPE during P6-2b validation:

```text
GET https://min-api.cryptocompare.com/data/v2/histominute?fsym=HYPE&tsym=USD&limit=2000&toTs=<unix_s>
```

Mappings:

- `hyperliquid` / `hype` -> `CRYPTOCOMPARE:HYPE`

The fallback is intentionally tagged in `source` as `cryptocompare.histominute`, and latency is reported in the JSON summary. Downstream evaluation should treat HYPE spot features as lower temporal resolution than Binance 1-second features.

### Feature definitions

- `spot_at_touch`: nearest spot close to `touch_time_iso`.
- `spot_at_window_start`: nearest spot close to `start_time_iso`.
- `spot_return_since_window_start = spot_at_touch / spot_at_window_start - 1`.
- `spot_realized_vol_recent_60s` and `spot_realized_vol_recent_300s`: standard deviation of log returns over the available spot bars in the trailing window.
- `spot_velocity_recent_30s = (spot_at_touch - spot_30s_ago) / 30`.
- `seconds_to_resolution = end_time_iso - touch_time_iso`.
- `spot_implied_prob`: normal approximation around the start spot as strike, using trailing 300s realized vol and time to resolution. For DOWN/NO rows it is mirrored as `1 - up_prob`.
- `spot_implied_prob_minus_market_prob = spot_implied_prob - best_ask_at_touch`.

### Latest prod smoke, 2026-04-26

Smoke artifact paths:

- output DB: `/home/polybot/.polybot/short_horizon/phase0/spot_features_p6_2b_1_smoke.sqlite3`
- summary JSON: `/home/polybot/.polybot/short_horizon/phase0/spot_features_p6_2b_1_smoke_summary.json`
- cache DB: `/home/polybot/.polybot/short_horizon/phase0/spot_ohlc_cache.sqlite3`

Smoke summary:

- input rows: `21056`
- output rows: `21056`
- skipped unsupported assets: `0`
- skipped missing spot rows: `0`
- coverage: `100.0%`

By asset:

- `bnb`: `3247/3247`, median touch latency `93 ms`
- `btc`: `2764/2764`, median touch latency `251 ms`
- `doge`: `3271/3271`, median touch latency `100 ms`
- `eth`: `2770/2770`, median touch latency `250 ms`
- `hyperliquid`: `3445/3445`, median touch latency `2240 ms`
- `sol`: `2768/2768`, median touch latency `238 ms`
- `xrp`: `2791/2791`, median touch latency `235 ms`

Latency interpretation: Binance-backed assets clear the 1s median alignment sanity check. Hyperliquid/HYPE is intentionally backed by minute bars, so its median latency exceeds 1s and should be treated as lower-resolution in P6-2c.

## P6-2b-2 live spot event plumbing

`P6-2b-2` adds `SpotPriceUpdate` as a normalized input event that is persisted and replayed but does not affect strategy decisions yet. This keeps the live/capture/replay plumbing ready for future spot-aware strategies without changing P6-2c offline evaluation semantics.

Event fields:

- `event_time_ms`, `ingest_time_ms`
- `source`, `venue`
- `asset_slug`
- `spot_price`, optional `bid`, optional `ask`
- optional `staleness_ms`
- `currency`, default `USD`
- optional `run_id`

Plumbing contract:

- `EventType.SPOT_PRICE_UPDATE` and dataclass `SpotPriceUpdate` live in `short_horizon.core.events`.
- `normalize_event_payload()` writes `SpotPriceUpdate` rows to `events_log` with `event_type='SpotPriceUpdate'`.
- Replay `parse_event_record()` round-trips `SpotPriceUpdate` from JSONL.
- Runner treats `SpotPriceUpdate` as an input event: append to store, update `StrategyRuntime.latest_spot_by_asset`, and emit no strategy output.
- `LiveEventSource` accepts an optional async `spot_feed` and merges those events into the live queue alongside market refresh, CLOB websocket, and user-stream events.

Minimal acceptance is synthetic/dry-run plumbing only; no external live spot subscription is enabled by default in this PR.
