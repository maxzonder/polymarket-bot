# Trigger executable overlay

## Purpose

`scripts/evaluate_trigger_overlay.py` checks whether trigger-discovery candidates survive collector/touch executable context.

This is the bridge between:

- historical trigger discovery from trade tape (`trigger_features`); and
- live-like taker entry constraints from `touch_dataset` (`best_ask_at_touch`, depth fit labels, fees, min-size cost).

It is still research. Positive results are dry-run candidates, not live-trading approval.

## Matching model

For each trigger row, the overlay finds the nearest collector touch row for the same `token_id` within `--match-window-seconds`.

The overlay then scores entry using:

- `best_ask_at_touch` as executable taker BUY price;
- `fee_rate_bps` and `fees_enabled` for entry fee estimate;
- `fit_10_usdc` as a depth/slippage proxy;
- `would_be_cost_after_min_shares` for venue minimum-size viability;
- resolved token outcome from `touch_dataset`.

A match is rejected when:

- ask is missing/invalid;
- ask is too far above the historical trigger price (`--max-ask-slippage-ticks`);
- `fit_10_usdc` is outside `--fit-allowlist`;
- min-order cost is below `--min-cost-usdc`.

## Example

```bash
./.venv/bin/python scripts/evaluate_trigger_overlay.py \
  --trigger-db /home/polybot/.polybot/short_horizon/phase0/trigger_discovery.sqlite3 \
  --touch-db /home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3 \
  --assets btc \
  --trigger-labels compression_breakout_up,chop_to_directional_up,acceleration_up \
  --match-window-seconds 90 \
  --max-ask-slippage-ticks 1 \
  --fit-allowlist +0_tick,+1_tick \
  --output-db /home/polybot/.polybot/short_horizon/phase0/trigger_overlay.sqlite3 \
  --report-path /home/polybot/.polybot/short_horizon/phase0/trigger_overlay_report.md
```

## Outputs

SQLite:

- `overlay_run_meta`
- `overlay_matches`
- `overlay_aggregates`

Markdown:

- executable headline;
- reject reasons;
- executable trigger candidates;
- executable trading-session view;
- all matched trigger candidates.

## Interpretation

- If matched/executable `n` is tiny, continue collecting data rather than trading.
- If a trigger is positive on tape but rejected by ask/depth/min-size, it is not a tradable trigger.
- Promote only trigger + asset + direction + session combinations that survive fresh collector windows and then dry-run validation.
