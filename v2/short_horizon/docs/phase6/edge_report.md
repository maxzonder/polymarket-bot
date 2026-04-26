# Phase 6 P6-1 edge report

## Recommendation
- **NO-GO: insufficient sample; keep stake ramp frozen**
- Resolved trades analyzed: `36` / required `50`
- Total cost: `83.3421 USDC`
- Gross PnL before estimated taker fees: `-10.7600 USDC`
- Estimated taker fees: `6.0006 USDC`
- Net PnL after estimated fees: `-16.7606 USDC`
- Net EV/trade: `-0.4656 USDC`
- EV/trade 95% CI: `-1.0821 .. 0.1509 USDC`
- EV per 1 USDC deployed: `-0.2011`
- Hit rate: `47.2%`
- Break-even hit rate: `65.5%`

Interpretation:
- This is not enough for a formal P6-1 GO, but it is enough to block stake scaling while selection is investigated.
- The observed live sample is materially negative; do not run the stake ramp from this baseline.
- Costs use actual filled notional, so venue min-size upscaling is reflected in EV/capital efficiency.
- Estimated fees are computed from stored `fee_rate_bps_latest`; missing fee metadata is a hard analyzer error.

## Input DBs
- `/home/polybot/.polybot/short_horizon/probes/micro_live_cap30_20260425_105944.sqlite3`
- `/home/polybot/.polybot/short_horizon/probes/p6_edge_cap30_20260425_201000.sqlite3`
- `/home/polybot/.polybot/short_horizon/probes/p6_edge_cap200_20260425_230202.sqlite3`

## Breakdown by run
- `live_p6_edge_cap200_20260425_230202`: trades=`14`, wins=`4`, hit=`28.6%`, cost=`40.8500`, gross_pnl=`-16.5550`, fee=`2.9412`, net_pnl=`-19.4962`, EV/trade=`-1.3926`, CI95=`-2.5472..-0.2379`, EV/USDC=`-0.4773`, break_even_hit=`62.6%`
- `live_cap30_20260425_105944`: trades=`13`, wins=`6`, hit=`46.2%`, cost=`12.8921`, gross_pnl=`0.6100`, fee=`0.9282`, net_pnl=`-0.3182`, EV/trade=`-0.0245`, CI95=`-0.3978..0.3489`, EV/USDC=`-0.0247`, break_even_hit=`64.6%`
- `live_p6_edge_cap30_20260425_201000`: trades=`9`, wins=`7`, hit=`77.8%`, cost=`29.6000`, gross_pnl=`5.1850`, fee=`2.1312`, net_pnl=`3.0538`, EV/trade=`0.3393`, CI95=`-1.0357..1.7144`, EV/USDC=`0.1032`, break_even_hit=`70.5%`

## Breakdown by asset
- `BTC`: trades=`22`, wins=`12`, hit=`54.5%`, cost=`50.7345`, gross_pnl=`0.5944`, fee=`3.6529`, net_pnl=`-3.0585`, EV/trade=`-0.1390`, CI95=`-0.8830..0.6049`, EV/USDC=`-0.0603`, break_even_hit=`65.3%`
- `ETH`: trades=`14`, wins=`5`, hit=`35.7%`, cost=`32.6076`, gross_pnl=`-11.3544`, fee=`2.3477`, net_pnl=`-13.7021`, EV/trade=`-0.9787`, CI95=`-2.0269..0.0695`, EV/USDC=`-0.4202`, break_even_hit=`65.9%`

## Breakdown by direction
- `UP/YES`: trades=`20`, wins=`10`, hit=`50.0%`, cost=`45.1118`, gross_pnl=`3.3689`, fee=`3.2480`, net_pnl=`0.1209`, EV/trade=`0.0060`, CI95=`-0.7527..0.7648`, EV/USDC=`0.0027`, break_even_hit=`66.3%`
- `DOWN/NO`: trades=`16`, wins=`7`, hit=`43.8%`, cost=`38.2303`, gross_pnl=`-14.1289`, fee=`2.7526`, net_pnl=`-16.8815`, EV/trade=`-1.0551`, CI95=`-2.0161..-0.0941`, EV/USDC=`-0.4416`, break_even_hit=`64.6%`

## Breakdown by entry price level
- `0.55`: trades=`16`, wins=`6`, hit=`37.5%`, cost=`33.4312`, gross_pnl=`-11.3748`, fee=`2.4070`, net_pnl=`-13.7819`, EV/trade=`-0.8614`, CI95=`-1.7621..0.0393`, EV/USDC=`-0.4122`, break_even_hit=`58.9%`
- `0.65`: trades=`14`, wins=`8`, hit=`57.1%`, cost=`36.3513`, gross_pnl=`1.7397`, fee=`2.6173`, net_pnl=`-0.8776`, EV/trade=`-0.0627`, CI95=`-1.1064..0.9811`, EV/USDC=`-0.0241`, break_even_hit=`69.4%`
- `0.70`: trades=`6`, wins=`3`, hit=`50.0%`, cost=`13.5596`, gross_pnl=`-1.1248`, fee=`0.9763`, net_pnl=`-2.1011`, EV/trade=`-0.3502`, CI95=`-1.8471..1.1468`, EV/USDC=`-0.1550`, break_even_hit=`75.2%`

## Breakdown by lifecycle bucket
- `0.2-0.3`: trades=`25`, wins=`13`, hit=`52.0%`, cost=`60.0921`, gross_pnl=`-5.6642`, fee=`4.3266`, net_pnl=`-9.9908`, EV/trade=`-0.3996`, CI95=`-1.1416..0.3424`, EV/USDC=`-0.1663`, break_even_hit=`65.6%`
- `0.3-0.4`: trades=`11`, wins=`4`, hit=`36.4%`, cost=`23.2500`, gross_pnl=`-5.0958`, fee=`1.6740`, net_pnl=`-6.7698`, EV/trade=`-0.6154`, CI95=`-1.7721..0.5412`, EV/USDC=`-0.2912`, break_even_hit=`65.3%`

## Per-trade records
- `live_cap30_20260425_105944` `2067011` ETH/UP/YES level=`0.7000` lifecycle=`0.2558` cost=`1.0080` fee=`0.0726` outcome=`0.0000` gross_pnl=`-1.0080` net_pnl=`-1.0806` question='Ethereum Up or Down - April 25, 7:30AM-7:45AM ET'
- `live_cap30_20260425_105944` `2067089` BTC/UP/YES level=`0.5500` lifecycle=`0.2759` cost=`0.9720` fee=`0.0700` outcome=`0.9900` gross_pnl=`0.8100` net_pnl=`0.7400` question='Bitcoin Up or Down - April 25, 7:45AM-8:00AM ET'
- `live_cap30_20260425_105944` `2067352` ETH/UP/YES level=`0.5500` lifecycle=`0.3679` cost=`1.0080` fee=`0.0726` outcome=`0.0000` gross_pnl=`-1.0080` net_pnl=`-1.0806` question='Ethereum Up or Down - April 25, 8:15AM-8:30AM ET'
- `live_cap30_20260425_105944` `2067453` BTC/UP/YES level=`0.5500` lifecycle=`0.2553` cost=`1.0065` fee=`0.0725` outcome=`0.5500` gross_pnl=`0.0000` net_pnl=`-0.0725` question='Bitcoin Up or Down - April 25, 8:45AM-9:00AM ET'
- `live_cap30_20260425_105944` `2067613` BTC/DOWN/NO level=`0.5500` lifecycle=`0.3985` cost=`0.9000` fee=`0.0648` outcome=`0.7100` gross_pnl=`0.3780` net_pnl=`0.3132` question='Bitcoin Up or Down - April 25, 9:15AM-9:30AM ET'
- `live_cap30_20260425_105944` `2067880` BTC/UP/YES level=`0.6500` lifecycle=`0.2558` cost=`1.0075` fee=`0.0725` outcome=`0.9900` gross_pnl=`0.5270` net_pnl=`0.4545` question='Bitcoin Up or Down - April 25, 9:45AM-10:00AM ET'
- `live_cap30_20260425_105944` `2067927` BTC/DOWN/NO level=`0.6500` lifecycle=`0.3036` cost=`0.9920` fee=`0.0714` outcome=`0.0000` gross_pnl=`-0.9920` net_pnl=`-1.0634` question='Bitcoin Up or Down - April 25, 10:00AM-10:15AM ET'
- `live_cap30_20260425_105944` `2068531` BTC/UP/YES level=`0.6500` lifecycle=`0.3847` cost=`0.9920` fee=`0.0714` outcome=`0.6000` gross_pnl=`-0.0620` net_pnl=`-0.1334` question='Bitcoin Up or Down - April 25, 11:00AM-11:15AM ET'
- `live_cap30_20260425_105944` `2068707` BTC/UP/YES level=`0.7000` lifecycle=`0.3626` cost=`1.0080` fee=`0.0726` outcome=`0.7300` gross_pnl=`0.0432` net_pnl=`-0.0294` question='Bitcoin Up or Down - April 25, 11:15AM-11:30AM ET'
- `live_cap30_20260425_105944` `2068755` BTC/DOWN/NO level=`0.5500` lifecycle=`0.2241` cost=`1.0065` fee=`0.0725` outcome=`0.9900` gross_pnl=`0.8052` net_pnl=`0.7327` question='Bitcoin Up or Down - April 25, 11:30AM-11:45AM ET'
- `live_cap30_20260425_105944` `2069169` ETH/UP/YES level=`0.6500` lifecycle=`0.2235` cost=`1.0098` fee=`0.0727` outcome=`0.5500` gross_pnl=`-0.1683` net_pnl=`-0.2410` question='Ethereum Up or Down - April 25, 12:00PM-12:15PM ET'
- `live_cap30_20260425_105944` `2069518` ETH/DOWN/NO level=`0.5500` lifecycle=`0.2698` cost=`0.9882` fee=`0.0712` outcome=`0.9990` gross_pnl=`0.8400` net_pnl=`0.7688` question='Ethereum Up or Down - April 25, 12:15PM-12:30PM ET'
- `live_cap30_20260425_105944` `2069574` ETH/DOWN/NO level=`0.7000` lifecycle=`0.2133` cost=`0.9936` fee=`0.0715` outcome=`0.9990` gross_pnl=`0.4450` net_pnl=`0.3734` question='Ethereum Up or Down - April 25, 12:30PM-12:45PM ET'
- `live_p6_edge_cap30_20260425_201000` `2070908` BTC/UP/YES level=`0.6500` lifecycle=`0.2891` cost=`3.2500` fee=`0.2340` outcome=`0.9990` gross_pnl=`1.7450` net_pnl=`1.5110` question='Bitcoin Up or Down - April 25, 4:15PM-4:30PM ET'
- `live_p6_edge_cap30_20260425_201000` `2070916` ETH/UP/YES level=`0.6500` lifecycle=`0.3075` cost=`3.3000` fee=`0.2376` outcome=`0.9990` gross_pnl=`1.6950` net_pnl=`1.4574` question='Ethereum Up or Down - April 25, 4:15PM-4:30PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071179` ETH/UP/YES level=`0.5500` lifecycle=`0.2785` cost=`2.7500` fee=`0.1980` outcome=`0.0000` gross_pnl=`-2.7500` net_pnl=`-2.9480` question='Ethereum Up or Down - April 25, 4:30PM-4:45PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071422` ETH/DOWN/NO level=`0.7000` lifecycle=`0.3311` cost=`3.5000` fee=`0.2520` outcome=`0.0000` gross_pnl=`-3.5000` net_pnl=`-3.7520` question='Ethereum Up or Down - April 25, 4:45PM-5:00PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071424` BTC/UP/YES level=`0.6500` lifecycle=`0.2669` cost=`3.2500` fee=`0.2340` outcome=`0.9900` gross_pnl=`1.7000` net_pnl=`1.4660` question='Bitcoin Up or Down - April 25, 4:45PM-5:00PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071507` BTC/UP/YES level=`0.6500` lifecycle=`0.3010` cost=`3.2500` fee=`0.2340` outcome=`0.9900` gross_pnl=`1.7000` net_pnl=`1.4660` question='Bitcoin Up or Down - April 25, 5:00PM-5:15PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071617` BTC/DOWN/NO level=`0.7000` lifecycle=`0.2585` cost=`3.5500` fee=`0.2556` outcome=`0.9990` gross_pnl=`1.4450` net_pnl=`1.1894` question='Bitcoin Up or Down - April 25, 5:30PM-5:45PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071679` ETH/UP/YES level=`0.7000` lifecycle=`0.2412` cost=`3.5000` fee=`0.2520` outcome=`0.9900` gross_pnl=`1.4500` net_pnl=`1.1980` question='Ethereum Up or Down - April 25, 5:45PM-6:00PM ET'
- `live_p6_edge_cap30_20260425_201000` `2072247` BTC/DOWN/NO level=`0.6500` lifecycle=`0.2180` cost=`3.2500` fee=`0.2340` outcome=`0.9900` gross_pnl=`1.7000` net_pnl=`1.4660` question='Bitcoin Up or Down - April 25, 6:00PM-6:15PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072717` BTC/UP/YES level=`0.5500` lifecycle=`0.3103` cost=`2.7500` fee=`0.1980` outcome=`0.9900` gross_pnl=`2.2000` net_pnl=`2.0020` question='Bitcoin Up or Down - April 25, 7:15PM-7:30PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072760` BTC/DOWN/NO level=`0.5500` lifecycle=`0.2933` cost=`2.7500` fee=`0.1980` outcome=`0.0000` gross_pnl=`-2.7500` net_pnl=`-2.9480` question='Bitcoin Up or Down - April 25, 7:30PM-7:45PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072825` BTC/DOWN/NO level=`0.6500` lifecycle=`0.2352` cost=`3.2500` fee=`0.2340` outcome=`0.0000` gross_pnl=`-3.2500` net_pnl=`-3.4840` question='Bitcoin Up or Down - April 25, 7:45PM-8:00PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072883` ETH/DOWN/NO level=`0.5500` lifecycle=`0.2601` cost=`2.7500` fee=`0.1980` outcome=`0.4500` gross_pnl=`-0.5000` net_pnl=`-0.6980` question='Ethereum Up or Down - April 25, 8:00PM-8:15PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072866` BTC/DOWN/NO level=`0.6500` lifecycle=`0.2018` cost=`3.2500` fee=`0.2340` outcome=`0.9900` gross_pnl=`1.7000` net_pnl=`1.4660` question='Bitcoin Up or Down - April 25, 8:00PM-8:15PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072950` BTC/UP/YES level=`0.6500` lifecycle=`0.2067` cost=`3.2500` fee=`0.2340` outcome=`0.9990` gross_pnl=`1.7450` net_pnl=`1.5110` question='Bitcoin Up or Down - April 25, 8:30PM-8:45PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072957` ETH/UP/YES level=`0.5500` lifecycle=`0.2101` cost=`2.7500` fee=`0.1980` outcome=`0.9900` gross_pnl=`2.2000` net_pnl=`2.0020` question='Ethereum Up or Down - April 25, 8:30PM-8:45PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073017` BTC/UP/YES level=`0.5500` lifecycle=`0.2155` cost=`2.7500` fee=`0.1980` outcome=`0.3200` gross_pnl=`-1.1500` net_pnl=`-1.3480` question='Bitcoin Up or Down - April 25, 8:45PM-9:00PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073014` ETH/UP/YES level=`0.6500` lifecycle=`0.2016` cost=`3.0500` fee=`0.2196` outcome=`0.0000` gross_pnl=`-3.0500` net_pnl=`-3.2696` question='Ethereum Up or Down - April 25, 8:45PM-9:00PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073062` ETH/DOWN/NO level=`0.5500` lifecycle=`0.2225` cost=`2.7500` fee=`0.1980` outcome=`0.0000` gross_pnl=`-2.7500` net_pnl=`-2.9480` question='Ethereum Up or Down - April 25, 9:00PM-9:15PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073063` BTC/DOWN/NO level=`0.5500` lifecycle=`0.3228` cost=`2.7500` fee=`0.1980` outcome=`0.0000` gross_pnl=`-2.7500` net_pnl=`-2.9480` question='Bitcoin Up or Down - April 25, 9:00PM-9:15PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073097` BTC/DOWN/NO level=`0.5500` lifecycle=`0.3229` cost=`2.8000` fee=`0.2016` outcome=`0.0000` gross_pnl=`-2.8000` net_pnl=`-3.0016` question='Bitcoin Up or Down - April 25, 9:15PM-9:30PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073123` BTC/DOWN/NO level=`0.5500` lifecycle=`0.2552` cost=`2.7500` fee=`0.1980` outcome=`0.1200` gross_pnl=`-2.1500` net_pnl=`-2.3480` question='Bitcoin Up or Down - April 25, 9:30PM-9:45PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073122` ETH/UP/YES level=`0.6500` lifecycle=`0.2465` cost=`3.2500` fee=`0.2340` outcome=`0.0000` gross_pnl=`-3.2500` net_pnl=`-3.4840` question='Ethereum Up or Down - April 25, 9:30PM-9:45PM ET'
