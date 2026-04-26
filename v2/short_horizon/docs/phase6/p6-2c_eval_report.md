# P6-2c offline model + policy evaluator

P6-2c stays offline: it trains on the canonical touch dataset plus spot features, then evaluates a held-out trading policy before any runtime gate can be enabled.

## Commands

Default prod training command:

```bash
cd /home/polybot/claude-polymarket
./.venv/bin/python scripts/train_touch_model.py \
  --touch-dataset /home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3 \
  --spot-features /home/polybot/.polybot/short_horizon/phase0/spot_features.sqlite3 \
  --artifact /home/polybot/.polybot/short_horizon/phase0/touch_model_artifact.json \
  --scores /home/polybot/.polybot/short_horizon/phase0/touch_model_scores.sqlite3 \
  --report /home/polybot/.polybot/short_horizon/phase0/p6-2c_model_report.md
```

Default held-out policy command:

```bash
cd /home/polybot/claude-polymarket
./.venv/bin/python scripts/evaluate_touch_policy.py \
  --scores /home/polybot/.polybot/short_horizon/phase0/touch_model_scores.sqlite3 \
  --report /home/polybot/.polybot/short_horizon/phase0/p6-2c_policy_report.md \
  --split test \
  --edge-buffer-bps 200 \
  --min-size-policy upscale \
  --max-orders-per-market-per-run 1 \
  --daily-loss-cap-usdc 30
```

Offline candidate gate command for the late-window spot-dislocation pocket (default off; use for validation only):

```bash
cd /home/polybot/claude-polymarket
./.venv/bin/python scripts/evaluate_touch_policy.py \
  --scores /home/polybot/.polybot/short_horizon/phase0/touch_model_scores.sqlite3 \
  --touch-dataset /home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3 \
  --spot-features /home/polybot/.polybot/short_horizon/phase0/spot_features.sqlite3 \
  --report /home/polybot/.polybot/short_horizon/phase0/p6-2c_spot_dislocation_policy_report.md \
  --split test \
  --edge-probability-field spot_implied_prob \
  --edge-buffer-bps 200 \
  --asset-allowlist btc,eth,sol,xrp \
  --direction-allowlist DOWN/NO \
  --min-lifecycle-fraction 0.60 \
  --min-spot-gap 0.06 \
  --spot-source-prefix-allowlist binance \
  --fit-10-allowlist +0_tick,+1_tick \
  --use-fit-10-entry-price \
  --min-size-policy upscale \
  --max-orders-per-market-per-run 1 \
  --daily-loss-cap-usdc 30
```

## Train/test split

- `scripts/train_touch_model.py` joins `touch_dataset` to `spot_features` on `probe_id`.
- Rows are ordered by `touch_time_iso`; default split is the earliest `60%` for training and latest `40%` held out.
- No random row shuffle is used.
- The logistic model uses a fixed seed and writes a JSON artifact plus a SQLite scores table.

## Feature set

- microstructure: bid/ask spread, top-1 ask concentration, top-5 ask depth
- touch dynamics: touch level, price at touch, optional velocity/trade-arrival fields when present
- lifecycle: lifecycle fraction and seconds to resolution
- fair value: spot return, realized vol, spot velocity, spot implied probability minus market probability
- one-hots: BTC, ETH, UP direction

## Policy gate

`evaluate_touch_policy.py` accepts a held-out touch only when:

- `model_prob - market_price >= fee_break_even_gap + edge_buffer`, where default edge buffer is `200 bps`
- optionally, `spot_implied_prob` can replace `model_prob` as the edge probability for explicit spot-dislocation gates
- optional candidate filters pass: asset allowlist, direction allowlist, minimum lifecycle fraction, minimum spot-implied gap, spot source prefix, and `fit_10_usdc` allowlist
- optional `fit_10_usdc` slippage adjustment moves entry from ask to ask+tick for `+1_tick` rows and skips rows without a supported fit label
- min-size policy passes (`upscale` by default, `skip` supported)
- `max_orders_per_market_per_run` is not exceeded
- daily realized loss cap has not been reached
- optional `min_survived_ms` stale-book guard passes

The GO gate is strict: held-out net EV per USDC must have a bootstrap 95% CI lower bound above zero after estimated fees and min-size handling. If not, P6-2 remains offline.

## Latest prod evaluation

Not run in this repository commit. The scripts above generate the model diagnostics and policy report on the prod artifacts under `/home/polybot/.polybot/short_horizon/phase0/`.
