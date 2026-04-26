# M-3 maker fill-probability simulation

Generated at: `2026-04-26T19:02:20.011346+00:00`
Touch DB: `/home/polybot/.polybot/short_horizon/phase0/touch_dataset.sqlite3`

## Model

This is a coarse offline proxy, not an L2 queue replay.

Filters/universes:
- A2: BTC/ETH/SOL/XRP, `DOWN/NO`, `lifecycle_fraction >= 0.60`, levels `{0.55, 0.65, 0.70}`
- HRES baseline: same assets/lifecycle/levels, both directions

Fill model:
- `order_shares = max(5, stake_usdc / touch_level)`
- `fill = ask_size_at_touch_level >= order_shares * queue_factor AND survived_ms >= T_min`
- maker fee = `0`, from M-1
- realized PnL uses resolved token outcome at touch level

Sweeps:
- `stake_usdc in {1, 5, 10, 25}`
- `queue_factor in {1, 2, 5}`
- `T_min in {0, 100, 500, 2000} ms`

## Gate result

- A2 configs passing `accepted_n >= 50` and CI lower `> 0`: `32` / `48`
- HRES configs passing same gate: `45` / `48`
- Recommended least-heroic A2 pass: stake `1`, q `1`, T `0ms`, accepted `344` / `647` (`53.2%`), cost `1,141.00`, PnL `149.00`, EV/USDC `13.1%`, CI `[6.2%, 19.8%]`

## Top A2 configs

- A2: stake `5`, q `1`, T `0ms`, accepted `269` / `647` (`41.6%`), cost `1,345.00`, PnL `206.30`, EV/USDC `15.3%`, CI `[7.9%, 22.8%]`
- A2: stake `1`, q `2`, T `0ms`, accepted `252` / `647` (`38.9%`), cost `838.50`, PnL `126.50`, EV/USDC `15.1%`, CI `[6.9%, 23.3%]`
- A2: stake `1`, q `1`, T `0ms`, accepted `344` / `647` (`53.2%`), cost `1,141.00`, PnL `149.00`, EV/USDC `13.1%`, CI `[6.2%, 19.8%]`
- A2: stake `5`, q `1`, T `100ms`, accepted `241` / `647` (`37.2%`), cost `1,205.00`, PnL `164.08`, EV/USDC `13.6%`, CI `[5.0%, 22.4%]`
- A2: stake `1`, q `2`, T `100ms`, accepted `227` / `647` (`35.1%`), cost `757.50`, PnL `102.50`, EV/USDC `13.5%`, CI `[4.8%, 21.7%]`
- A2: stake `25`, q `1`, T `0ms`, accepted `122` / `647` (`18.9%`), cost `3,050.00`, PnL `464.49`, EV/USDC `15.2%`, CI `[4.8%, 26.6%]`
- A2: stake `5`, q `5`, T `0ms`, accepted `122` / `647` (`18.9%`), cost `610.00`, PnL `92.90`, EV/USDC `15.2%`, CI `[4.8%, 26.6%]`
- A2: stake `1`, q `1`, T `100ms`, accepted `294` / `647` (`45.4%`), cost `978.25`, PnL `116.75`, EV/USDC `11.9%`, CI `[4.3%, 19.2%]`

## A2 robustness checkpoints

- A2: stake `1`, q `2`, T `500ms`, accepted `195` / `647` (`30.1%`), cost `649.75`, PnL `80.25`, EV/USDC `12.4%`, CI `[3.1%, 21.5%]`
- A2: stake `1`, q `2`, T `2000ms`, accepted `160` / `647` (`24.7%`), cost `534.00`, PnL `66.00`, EV/USDC `12.4%`, CI `[2.3%, 22.2%]`
- A2: stake `1`, q `5`, T `500ms`, accepted `120` / `647` (`18.5%`), cost `401.25`, PnL `53.75`, EV/USDC `13.4%`, CI `[2.1%, 24.3%]`
- A2: stake `1`, q `5`, T `2000ms`, accepted `97` / `647` (`15.0%`), cost `324.75`, PnL `40.25`, EV/USDC `12.4%`, CI `[-1.1%, 25.0%]`
- A2: stake `5`, q `2`, T `500ms`, accepted `164` / `647` (`25.3%`), cost `820.00`, PnL `96.93`, EV/USDC `11.8%`, CI `[1.6%, 21.6%]`
- A2: stake `5`, q `5`, T `500ms`, accepted `103` / `647` (`15.9%`), cost `515.00`, PnL `74.66`, EV/USDC `14.5%`, CI `[2.7%, 26.1%]`

## Top HRES baseline configs

- HRES: stake `25`, q `5`, T `2000ms`, accepted `46` / `1,281` (`3.6%`), cost `1,150.00`, PnL `299.80`, EV/USDC `26.1%`, CI `[7.9%, 42.1%]`
- HRES: stake `25`, q `5`, T `500ms`, accepted `58` / `1,281` (`4.5%`), cost `1,450.00`, PnL `326.72`, EV/USDC `22.5%`, CI `[6.6%, 37.2%]`
- HRES: stake `5`, q `5`, T `100ms`, accepted `270` / `1,281` (`21.1%`), cost `1,350.00`, PnL `181.27`, EV/USDC `13.4%`, CI `[5.8%, 21.7%]`
- HRES: stake `25`, q `1`, T `100ms`, accepted `270` / `1,281` (`21.1%`), cost `6,750.00`, PnL `906.34`, EV/USDC `13.4%`, CI `[5.8%, 21.7%]`
- HRES: stake `5`, q `5`, T `0ms`, accepted `295` / `1,281` (`23.0%`), cost `1,475.00`, PnL `183.24`, EV/USDC `12.4%`, CI `[4.9%, 19.8%]`
- HRES: stake `25`, q `1`, T `0ms`, accepted `295` / `1,281` (`23.0%`), cost `7,375.00`, PnL `916.21`, EV/USDC `12.4%`, CI `[4.9%, 19.8%]`
- HRES: stake `5`, q `5`, T `500ms`, accepted `233` / `1,281` (`18.2%`), cost `1,165.00`, PnL `158.53`, EV/USDC `13.6%`, CI `[4.7%, 22.3%]`
- HRES: stake `25`, q `1`, T `500ms`, accepted `233` / `1,281` (`18.2%`), cost `5,825.00`, PnL `792.63`, EV/USDC `13.6%`, CI `[4.7%, 22.3%]`

## Per-day check for recommended A2 config

- `2026-04-20`: accepted `24`, cost `79.25`, PnL `30.75`, EV/USDC `38.8%`
- `2026-04-21`: accepted `97`, cost `319.00`, PnL `31.00`, EV/USDC `9.7%`
- `2026-04-22`: accepted `50`, cost `165.50`, PnL `-0.50`, EV/USDC `-0.3%`
- `2026-04-23`: accepted `41`, cost `136.25`, PnL `13.75`, EV/USDC `10.1%`
- `2026-04-24`: accepted `56`, cost `185.00`, PnL `0.00`, EV/USDC `0.0%`
- `2026-04-25`: accepted `57`, cost `191.25`, PnL `53.75`, EV/USDC `28.1%`
- `2026-04-26`: accepted `19`, cost `64.75`, PnL `20.25`, EV/USDC `31.3%`

## Interpretation

M-3 passes the coarse offline gate for A2: at least one realistic-looking config retains enough fills and positive bootstrap lower bound.
This is still not a live GO. The model uses `ask_size_at_touch_level` as a proxy for fillable seller flow and does not know true queue priority. M-4 L2 replay is required before any live maker probe.
