# T3 pre-touch maker trigger

Generated at: `2026-04-27T10:20:48.671241+00:00`
Touch DB: `/home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3`
Capture glob: `/home/polybot/.polybot/capture_bundles/*/events_log.jsonl`

## Scope

This offline replay tests a maker-friendly pre-touch redesign: post a resting BUY at the target level while best ask is still above the target but within two ticks.
The replay uses capture-bundle BBO plus TradeTick data. Maker fee is modeled as `0 bps`; full L2 queue position is unavailable unless future captures include depth levels.

Trigger approximation:
- target levels: `0.55`, `0.65`, `0.70`
- eligible pre-touch book: `touch_level < best_ask <= touch_level + 2*tick_size`
- optional BBO momentum: current mid > mid 30s earlier
- optional spread compression: current spread below median spread over prior 30s
- fill proxy: after trigger, best ask reaches target and cumulative SELL prints at/below target exceed `order_shares * queue_factor` before cancel

## Coverage

- capture bundles: `7`
- candidate appearances: `991`
- replay rows: `11892`
- segments: `84`
- any L2 levels: `false`

## Headline

- Gate: `NO-GO`
- Best segment: `stake=1|q=1|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | level=0.65`
- Best EV/USDC: `0.538462`; fired `1`, filled `1`, fill rate `100.00%`, CI95 `n/a..n/a`

## Segment comparison

| config | asset | direction | lifecycle | level | fired | safe | filled | fill rate | EV/USDC | CI95 | gate |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| stake=1|q=1|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | eth | DOWN/NO | <0.40 | 0.65 | 2 | 2 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 0 | 0.00% | n/a | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | 0.538462 | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | 0.538462 | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | 0.538462 | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | 0.538462 | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | 0.538462 | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | 0.538462 | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 3 | 100.00% | 0.428571 | 0.428571..0.428571 | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 3 | 100.00% | 0.428571 | 0.428571..0.428571 | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 3 | 100.00% | 0.428571 | 0.428571..0.428571 | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 3 | 100.00% | 0.428571 | 0.428571..0.428571 | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 2 | 66.67% | 0.428571 | 0.428571..0.428571 | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 2 | 66.67% | 0.428571 | 0.428571..0.428571 | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 2 | 66.67% | 0.428571 | 0.428571..0.428571 | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 2 | 66.67% | 0.428571 | 0.428571..0.428571 | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 2 | 66.67% | 0.428571 | 0.428571..0.428571 | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 2 | 66.67% | 0.428571 | 0.428571..0.428571 | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 1 | 33.33% | 0.428571 | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | UP/YES | <0.40 | 0.70 | 3 | 3 | 1 | 33.33% | 0.428571 | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 2 | 40.00% | -0.230769 | -1.000000..0.538462 | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 2 | 40.00% | -0.230769 | -1.000000..0.538462 | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.70 | 2 | 2 | 2 | 100.00% | -0.285714 | -1.000000..0.428571 | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 3 | 60.00% | -0.487179 | -1.000000..0.538462 | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 3 | 60.00% | -0.487179 | -1.000000..0.538462 | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 3 | 60.00% | -0.487179 | -1.000000..0.538462 | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 3 | 60.00% | -0.487179 | -1.000000..0.538462 | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 3 | 60.00% | -0.487179 | -1.000000..0.538462 | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 3 | 60.00% | -0.487179 | -1.000000..0.538462 | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=60s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 3 | 60.00% | -0.487179 | -1.000000..0.538462 | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 3 | 60.00% | -0.487179 | -1.000000..0.538462 | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 3 | 60.00% | -0.487179 | -1.000000..0.538462 | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | btc | DOWN/NO | <0.40 | 0.65 | 5 | 5 | 3 | 60.00% | -0.487179 | -1.000000..0.538462 | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 1 | 50.00% | -1.000000 | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 1 | 50.00% | -1.000000 | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 1 | 50.00% | -1.000000 | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 1 | 50.00% | -1.000000 | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 1 | 50.00% | -1.000000 | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.70 | 2 | 2 | 1 | 50.00% | -1.000000 | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | -1.000000 | n/a..n/a | NO-GO |
| stake=1|q=1|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | -1.000000 | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | -1.000000 | n/a..n/a | NO-GO |
| stake=1|q=2|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | -1.000000 | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=60s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | -1.000000 | n/a..n/a | NO-GO |
| stake=1|q=5|ticks=2|lookback=120s|cancel=120s|mom=0|spread=0 | eth | UP/YES | <0.40 | 0.65 | 1 | 1 | 1 | 100.00% | -1.000000 | n/a..n/a | NO-GO |

## Proposed live changes if T3 ever passes offline

- Add a config-gated pre-touch maker trigger separate from ASC touch.
- Emit explicit pre-touch signal events with trigger book state, target level, cancel window, and queue assumptions.
- Require depth/L2 capture before live maker sizing; BBO-only replay is not enough to size queue position.
- Keep maker path disabled unless offline replay clears fill-rate and positive-CI gates on fresh data.
