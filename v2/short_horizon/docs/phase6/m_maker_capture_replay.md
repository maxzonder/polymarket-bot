# M-4 capture replay maker queue check

Generated at: `2026-04-26T19:30:20.893728+00:00`
Touch DB: `/home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3`
Capture glob: `/home/polybot/.polybot/capture_bundles/*/events_log.jsonl`

## Scope

This is the M-4 check against available capture bundles. Important limitation: existing capture bundles contain BBO `BookUpdate` plus `TradeTick`, but no full L2 bid/ask levels. Therefore true queue priority cannot be calibrated from these bundles.

What this pass can still test:
- whether a same-level post-only BUY could have been resting before the A2 touch
- whether subsequent SELL trade prints at or below the bid level would have filled the synthetic maker bid under queue-factor stress

Synthetic order model:
- A2 candidates only: BTC/ETH/SOL/XRP, `DOWN/NO`, lifecycle `>=0.60`, levels `{0.55, 0.65, 0.70}`
- `order_shares = max(5, stake_usdc / touch_level)`
- post time = `touch_time - T_before`
- post-only safety check: if prior `best_ask <= touch_level`, status is `post_only_would_cross`
- fill proxy, if post-only safe: cumulative `SELL` TradeTick volume at `price <= touch_level` must exceed `order_shares * queue_factor`

## Coverage

- capture bundles with matching A2 candidates: `7`
- matched candidate appearances across bundles: `62`
- any bundle has L2 book levels: `false`
- `micro_live_20260424_002031`: candidates `22`, book updates `852,237`, trade ticks `8,542`, L2 levels `false`
- `micro_live_cap15_20260424_130208`: candidates `6`, book updates `283,004`, trade ticks `2,141`, L2 levels `false`
- `micro_live_cap30_20260424_212705`: candidates `5`, book updates `86,789`, trade ticks `850`, L2 levels `false`
- `micro_live_cap30_20260425_085054`: candidates `2`, book updates `23,462`, trade ticks `444`, L2 levels `false`
- `micro_live_cap30_20260425_105944`: candidates `9`, book updates `190,670`, trade ticks `2,780`, L2 levels `false`
- `p6_edge_cap200_20260425_230202`: candidates `14`, book updates `283,238`, trade ticks `3,989`, L2 levels `false`
- `p6_edge_cap30_20260425_201000`: candidates `4`, book updates `94,191`, trade ticks `1,638`, L2 levels `false`

## Representative configs

- stake `1`, q `1`, before `5s`, cancel `+10s`: candidates `62`, filled `0`, post-only-cross `62`, no-prior-book `0`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `5s`, cancel `+60s`: candidates `62`, filled `0`, post-only-cross `62`, no-prior-book `0`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `30s`, cancel `+10s`: candidates `62`, filled `0`, post-only-cross `61`, no-prior-book `1`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `30s`, cancel `+60s`: candidates `62`, filled `0`, post-only-cross `61`, no-prior-book `1`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `120s`, cancel `+10s`: candidates `62`, filled `0`, post-only-cross `60`, no-prior-book `2`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `120s`, cancel `+60s`: candidates `62`, filled `0`, post-only-cross `60`, no-prior-book `2`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `300s`, cancel `+10s`: candidates `62`, filled `0`, post-only-cross `60`, no-prior-book `2`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `300s`, cancel `+60s`: candidates `62`, filled `0`, post-only-cross `60`, no-prior-book `2`, unfilled `0`, EV/USDC `n/a`

## Best configs by fills

- stake `1`, q `1`, before `5s`, cancel `+10s`: candidates `62`, filled `0`, post-only-cross `62`, no-prior-book `0`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `5s`, cancel `+60s`: candidates `62`, filled `0`, post-only-cross `62`, no-prior-book `0`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `30s`, cancel `+10s`: candidates `62`, filled `0`, post-only-cross `61`, no-prior-book `1`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `30s`, cancel `+60s`: candidates `62`, filled `0`, post-only-cross `61`, no-prior-book `1`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `120s`, cancel `+10s`: candidates `62`, filled `0`, post-only-cross `60`, no-prior-book `2`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `120s`, cancel `+60s`: candidates `62`, filled `0`, post-only-cross `60`, no-prior-book `2`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `300s`, cancel `+10s`: candidates `62`, filled `0`, post-only-cross `60`, no-prior-book `2`, unfilled `0`, EV/USDC `n/a`
- stake `1`, q `1`, before `300s`, cancel `+60s`: candidates `62`, filled `0`, post-only-cross `60`, no-prior-book `2`, unfilled `0`, EV/USDC `n/a`

## Interpretation

M-4 rejects the current same-level maker interpretation on available captures: every matched candidate either had no prior book or would have crossed the spread when posted before touch. There were zero synthetic maker fills across the sweep.

Why this matters: the current touch semantics fire on `best_ask` rising through the entry level. Before that touch, `best_ask` is usually already below or equal to the target level, so a BUY limit at the target level is not a resting maker order; it is post-only invalid / taker-like.

This means M-3's `ask_size_at_touch_level` proxy was measuring taker-side visible ask liquidity, not a maker bid queue that we could join at the same level.

Conclusion: do not proceed to a live maker probe for the current same-level A2 design. A maker strategy would need a different trigger, for example posting only when the target level is still safely below best ask, or a real L2-capture run that records full book depth before re-testing queue priority.
