# M maker fees and adverse-selection check

Generated at: `2026-04-26T18:50:08.866003+00:00`

## M-1 fee structure

Source docs: https://docs.polymarket.com/trading/fees

Observation:
- sampled active BTC/ETH/SOL/XRP Up/Down markets: `4`
- docs state makers are never charged fees; only takers pay fees
- inferred taker fee rates from `feeSchedule.rate`: `0.072`
- inferred maker fee rates from `feeSchedule.takerOnly=true`: `0.0`
- inferred maker rebate rates: `0.2`
- Gamma/CLOB legacy base fee fields are present but not sufficient alone: sampled markets showed `makerBaseFee/takerBaseFee = 1000/1000`, while `feeSchedule` has the actual taker-only schedule.

Sampled markets:
- `2079267` `Ethereum Up or Down - April 26, 2:45PM-3:00PM ET`: feeType `crypto_fees_v2`, feeSchedule.rate `0.072`, takerOnly `True`, rebateRate `0.2`, Gamma base `1000.0/1000.0`, CLOB base `1000.0/1000.0`
- `2079881` `Bitcoin Up or Down - April 26, 4:50PM-4:55PM ET`: feeType `crypto_fees_v2`, feeSchedule.rate `0.072`, takerOnly `True`, rebateRate `0.2`, Gamma base `1000.0/1000.0`, CLOB base `1000.0/1000.0`
- `2080854` `XRP Up or Down - April 26, 7:15PM-7:20PM ET`: feeType `crypto_fees_v2`, feeSchedule.rate `0.072`, takerOnly `True`, rebateRate `0.2`, Gamma base `1000.0/1000.0`, CLOB base `1000.0/1000.0`
- `2079256` `XRP Up or Down - April 26, 2:45PM-3:00PM ET`: feeType `crypto_fees_v2`, feeSchedule.rate `0.072`, takerOnly `True`, rebateRate `0.2`, Gamma base `1000.0/1000.0`, CLOB base `1000.0/1000.0`

M-1 result: maker-side fee is confirmed as `0` for the documented taker-only crypto fee schedule; taker fee rate is `0.072`, and maker rebate rate is `0.2`. Do not use legacy `makerBaseFee/takerBaseFee` alone for PnL math. No live post-only order was placed in this pass; that should remain a separate explicit-approval step.

## M-2 adverse-selection check

Filters:
- historical DB: BTC/ETH/SOL/XRP 15m Up/Down markets
- market lifecycle: `>= 0.60` using `event_start_time` to `end_date`
- token direction: `DOWN/NO`
- price levels: rounded trade price in `{0.55, 0.65, 0.70}`
- historical tape `side` is retained, but because it is not a full maker/taker queue tag, `SELL` is treated only as the closest passive-bid fill proxy

- Historical level fills, all sides: n `164,878`, wins `111,424`, hit `67.6%`, bootstrap 95% CI `[67.4%, 67.8%]`, shares `4,066,400`
- Historical level fills, `SELL` side proxy: n `33,909`, wins `22,761`, hit `67.1%`, bootstrap 95% CI `[66.6%, 67.6%]`, shares `667,196`
- Current touch-dataset A2 baseline: n `647`, wins `464`, hit `71.7%`, bootstrap 95% CI `[68.2%, 75.3%]`

Aggregate gap, historical all-side level fills minus touch baseline: `-4.1 pp`.
Aggregate gap, historical SELL-side proxy minus touch baseline: `-4.6 pp`.

By level, historical all-side level fills:
- level `0.55`: n `46,912`, hit `57.7%`, CI `[57.2%, 58.1%]`, shares `1,179,314`
- level `0.65`: n `53,978`, hit `68.9%`, CI `[68.5%, 69.3%]`, shares `1,320,190`
- level `0.70`: n `63,988`, hit `73.7%`, CI `[73.4%, 74.1%]`, shares `1,566,895`

By level, touch baseline:
- level `0.55`: n `78`, hit `60.3%`, CI `[50.0%, 70.5%]`
- level `0.65`: n `237`, hit `71.7%`, CI `[65.8%, 77.6%]`
- level `0.70`: n `332`, hit `74.4%`, CI `[69.3%, 79.2%]`

By asset, historical all-side level fills:
- `btc`: n `40,126`, hit `69.1%`, CI `[68.6%, 69.5%]`, shares `1,262,025`
- `eth`: n `72,567`, hit `68.2%`, CI `[67.9%, 68.6%]`, shares `1,769,484`
- `sol`: n `28,917`, hit `65.8%`, CI `[65.2%, 66.3%]`, shares `582,611`
- `xrp`: n `23,268`, hit `65.2%`, CI `[64.6%, 65.8%]`, shares `452,279`

By asset, touch baseline:
- `btc`: n `158`, hit `70.3%`, CI `[62.7%, 77.2%]`
- `eth`: n `143`, hit `75.5%`, CI `[68.5%, 82.5%]`
- `sol`: n `174`, hit `64.4%`, CI `[56.9%, 71.8%]`
- `xrp`: n `172`, hit `77.3%`, CI `[70.9%, 83.7%]`

M-2 result: not a confirmed adverse-selection no-go. Historical level fills are lower than the current touch baseline by roughly `4-5 pp`, but not catastrophically worse; this does not eat the full maker/taker fee benefit by itself. The remaining blocker is M-3/M-4 fill probability and queue realism.

## Next gate

Proceed to M-3 fill-probability simulation before any live maker probe. M-1/M-2 are necessary but not sufficient for GO.
