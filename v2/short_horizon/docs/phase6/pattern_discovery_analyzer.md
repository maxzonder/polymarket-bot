# Pattern discovery analyzer

## Purpose

`scripts/discover_market_patterns.py` is a research-only analyzer for finding interpretable 15m crypto market movement patterns before turning them into live strategy candidates.

It intentionally separates:

- historical trajectory signal from historical trade tape;
- fee-aware hold-to-resolution estimate at a configurable decision time;
- future collector/live-like validation, which is still required before any trading use.

## Default model

For each eligible token side, the analyzer:

1. loads exact-duration crypto markets from the dataset DB;
2. reconstructs a normalized price trajectory from historical trade tape;
3. uses only the trajectory prefix up to `--decision-fraction` for pattern labels;
4. labels the prefix with heuristic pattern families such as:
   - `monotonic_uptrend`
   - `monotonic_downtrend`
   - `flat_then_breakout_up`
   - `flat_then_breakout_down`
   - `breakout_then_reversal_down`
   - `dip_then_reversal_up`
   - `v_shape_or_dip_recovery`
   - `inverted_v_or_spike_fade`
   - `high_volatility_chop`
   - `choppy_mean_reverting`
5. estimates fee-aware hold-to-resolution PnL per share from the decision price;
6. assigns a chronological train/test split;
7. aggregates by pattern, asset, direction, split, and UTC hour.

## Example command

```bash
./.venv/bin/python scripts/discover_market_patterns.py \
  --market-duration-minutes 15 \
  --assets btc eth sol xrp \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --time-grid 0,10,20,30,40,50,60,70,80,90,100 \
  --decision-fraction 0.40 \
  --validation-train-fraction 0.70 \
  --output-db /home/polybot/.polybot/short_horizon/phase0/pattern_discovery.sqlite3 \
  --report-path /home/polybot/.polybot/short_horizon/phase0/pattern_discovery_report.md
```

## Outputs

SQLite:

- `pattern_run_meta`
- `pattern_features`
- `pattern_aggregates`

Markdown:

- scope and caveats;
- pattern leaderboard;
- BTC/ETH and asset-pattern comparison;
- asset + direction + pattern comparison;
- date-split pattern stability;
- intraday pattern slices;
- label counts;
- next checks.

## Caveats

- Pattern labels are heuristic and should be treated as discovery aids, not final strategy rules.
- Current PnL is a historical per-share hold-to-resolution estimate, not a live execution claim.
- Any promising pattern still needs date-split stability and collector/live-like validation with best-ask/depth/slippage/fees/min-size before trading.
