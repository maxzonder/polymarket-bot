# +1 tick taker slippage stress check

Date:
- 2026-04-20 UTC

Research run:
- script: `scripts/analyze_taker_resolution.py`
- output DB: `/home/polybot/.polybot/taker_resolution_15m_rel_1tick_20260420_000659.db`
- log: `/home/polybot/.polybot/logs/analyze_taker_resolution_15m_rel_1tick_20260420_000659.log`
- trigger source: `trade_price_first_touch`
- stress: `--entry-slippage-ticks 1 --tick-size 0.01`
- duration filter: `--min-duration-hours 0.25 --max-duration-hours 0.25`
- bucket mode: `--time-bucket-mode relative`

This was the requested pessimistic execution stress for the strict research slice:
- `ascending`
- relative bucket `20_40pct`
- price levels `{0.55, 0.65, 0.70}`

## Aggregated result by level

Across all rows in `taker_resolution_summary` matching:
- `touch_direction = ascending`
- `time_bucket_mode = relative`
- `time_to_close_bucket = 20_40pct`
- `price_level in (0.55, 0.65, 0.70)`

Results:
- `0.55`
  - `n_tokens = 5404`
  - `avg_net_pnl_per_share = +0.001613`
  - `avg_net_edge = +0.001613`
- `0.65`
  - `n_tokens = 4759`
  - `avg_net_pnl_per_share = +0.005274`
  - `avg_net_edge = +0.005274`
- `0.70`
  - `n_tokens = 6290`
  - `avg_net_pnl_per_share = +0.001232`
  - `avg_net_edge = +0.001232`

Combined across all three levels:
- `n_tokens = 16453`
- `avg_net_pnl_per_share = +0.002526`
- `avg_net_edge = +0.002526`

## Interpretation

The important result is simple:
- the `+1 tick` pessimistic entry stress does **not** flip the strict `20_40pct / ascending / {0.55, 0.65, 0.70}` research slice negative in aggregate
- the edge becomes much thinner, but it remains **positive overall**

So the last requested research-side stress check is **passed** in the narrow sense:
- the slice survives a `+1 tick` worse entry assumption

## Caveat

This remains a research result under the already-documented historical trigger semantics:
- research = `trade_price_first_touch`
- live MVP = `best_ask_first_touch`

So this check strengthens confidence in the historical edge robustness, but it does **not** remove the live-vs-research trigger-definition gap.
