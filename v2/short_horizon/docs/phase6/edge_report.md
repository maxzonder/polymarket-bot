# Phase 6 P6-1 edge report

## Recommendation
- **NO-GO: insufficient sample; keep stake ramp frozen**
- Resolved trades analyzed: `36` / required `50`
- Total cost: `83.3421 USDC`
- Total PnL: `-10.7600 USDC`
- EV/trade: `-0.2989 USDC`
- EV/trade 95% CI: `-0.9133 .. 0.3155 USDC`
- EV per 1 USDC deployed: `-0.1291`
- Hit rate: `50.0%`

Interpretation:
- This is not enough for a formal P6-1 GO, but it is enough to block stake scaling while selection is investigated.
- The observed live sample is materially negative; do not run the stake ramp from this baseline.
- Costs use actual filled notional, so venue min-size upscaling is reflected in EV/capital efficiency.

## Input DBs
- `/home/polybot/.polybot/short_horizon/probes/micro_live_cap30_20260425_105944.sqlite3`
- `/home/polybot/.polybot/short_horizon/probes/p6_edge_cap30_20260425_201000.sqlite3`
- `/home/polybot/.polybot/short_horizon/probes/p6_edge_cap200_20260425_230202.sqlite3`

## Breakdown by run
- `live_p6_edge_cap200_20260425_230202`: trades=`14`, wins=`4`, hit=`28.6%`, cost=`40.8500`, pnl=`-16.5550`, EV/trade=`-1.1825`, CI95=`-2.3374..-0.0276`, EV/USDC=`-0.4053`
- `live_cap30_20260425_105944`: trades=`13`, wins=`7`, hit=`53.8%`, cost=`12.8921`, pnl=`0.6100`, EV/trade=`0.0469`, CI95=`-0.3261..0.4199`, EV/USDC=`0.0473`
- `live_p6_edge_cap30_20260425_201000`: trades=`9`, wins=`7`, hit=`77.8%`, cost=`29.6000`, pnl=`5.1850`, EV/trade=`0.5761`, CI95=`-0.8022..1.9544`, EV/USDC=`0.1752`

## Breakdown by asset
- `BTC`: trades=`22`, wins=`13`, hit=`59.1%`, cost=`50.7345`, pnl=`0.5944`, EV/trade=`0.0270`, CI95=`-0.7180..0.7720`, EV/USDC=`0.0117`
- `ETH`: trades=`14`, wins=`5`, hit=`35.7%`, cost=`32.6076`, pnl=`-11.3544`, EV/trade=`-0.8110`, CI95=`-1.8492..0.2271`, EV/USDC=`-0.3482`

## Breakdown by direction
- `UP/YES`: trades=`20`, wins=`11`, hit=`55.0%`, cost=`45.1118`, pnl=`3.3689`, EV/trade=`0.1684`, CI95=`-0.5954..0.9323`, EV/USDC=`0.0747`
- `DOWN/NO`: trades=`16`, wins=`7`, hit=`43.8%`, cost=`38.2303`, pnl=`-14.1289`, EV/trade=`-0.8831`, CI95=`-1.8322..0.0661`, EV/USDC=`-0.3696`

## Breakdown by entry price level
- `0.55`: trades=`16`, wins=`6`, hit=`37.5%`, cost=`33.4312`, pnl=`-11.3748`, EV/trade=`-0.7109`, CI95=`-1.5970..0.1751`, EV/USDC=`-0.3402`
- `0.65`: trades=`14`, wins=`8`, hit=`57.1%`, cost=`36.3513`, pnl=`1.7397`, EV/trade=`0.1243`, CI95=`-0.9237..1.1722`, EV/USDC=`0.0479`
- `0.70`: trades=`6`, wins=`4`, hit=`66.7%`, cost=`13.5596`, pnl=`-1.1248`, EV/trade=`-0.1875`, CI95=`-1.6821..1.3072`, EV/USDC=`-0.0830`

## Breakdown by lifecycle bucket
- `0.2-0.3`: trades=`25`, wins=`13`, hit=`52.0%`, cost=`60.0921`, pnl=`-5.6642`, EV/trade=`-0.2266`, CI95=`-0.9658..0.5127`, EV/USDC=`-0.0943`
- `0.3-0.4`: trades=`11`, wins=`5`, hit=`45.5%`, cost=`23.2500`, pnl=`-5.0958`, EV/trade=`-0.4633`, CI95=`-1.6159..0.6894`, EV/USDC=`-0.2192`

## Per-trade records
- `live_cap30_20260425_105944` `2067011` ETH/UP/YES level=`0.7000` lifecycle=`0.2558` cost=`1.0080` outcome=`0.0000` pnl=`-1.0080` question='Ethereum Up or Down - April 25, 7:30AM-7:45AM ET'
- `live_cap30_20260425_105944` `2067089` BTC/UP/YES level=`0.5500` lifecycle=`0.2759` cost=`0.9720` outcome=`0.9900` pnl=`0.8100` question='Bitcoin Up or Down - April 25, 7:45AM-8:00AM ET'
- `live_cap30_20260425_105944` `2067352` ETH/UP/YES level=`0.5500` lifecycle=`0.3679` cost=`1.0080` outcome=`0.0000` pnl=`-1.0080` question='Ethereum Up or Down - April 25, 8:15AM-8:30AM ET'
- `live_cap30_20260425_105944` `2067453` BTC/UP/YES level=`0.5500` lifecycle=`0.2553` cost=`1.0065` outcome=`0.5500` pnl=`0.0000` question='Bitcoin Up or Down - April 25, 8:45AM-9:00AM ET'
- `live_cap30_20260425_105944` `2067613` BTC/DOWN/NO level=`0.5500` lifecycle=`0.3985` cost=`0.9000` outcome=`0.7100` pnl=`0.3780` question='Bitcoin Up or Down - April 25, 9:15AM-9:30AM ET'
- `live_cap30_20260425_105944` `2067880` BTC/UP/YES level=`0.6500` lifecycle=`0.2558` cost=`1.0075` outcome=`0.9900` pnl=`0.5270` question='Bitcoin Up or Down - April 25, 9:45AM-10:00AM ET'
- `live_cap30_20260425_105944` `2067927` BTC/DOWN/NO level=`0.6500` lifecycle=`0.3036` cost=`0.9920` outcome=`0.0000` pnl=`-0.9920` question='Bitcoin Up or Down - April 25, 10:00AM-10:15AM ET'
- `live_cap30_20260425_105944` `2068531` BTC/UP/YES level=`0.6500` lifecycle=`0.3847` cost=`0.9920` outcome=`0.6000` pnl=`-0.0620` question='Bitcoin Up or Down - April 25, 11:00AM-11:15AM ET'
- `live_cap30_20260425_105944` `2068707` BTC/UP/YES level=`0.7000` lifecycle=`0.3626` cost=`1.0080` outcome=`0.7300` pnl=`0.0432` question='Bitcoin Up or Down - April 25, 11:15AM-11:30AM ET'
- `live_cap30_20260425_105944` `2068755` BTC/DOWN/NO level=`0.5500` lifecycle=`0.2241` cost=`1.0065` outcome=`0.9900` pnl=`0.8052` question='Bitcoin Up or Down - April 25, 11:30AM-11:45AM ET'
- `live_cap30_20260425_105944` `2069169` ETH/UP/YES level=`0.6500` lifecycle=`0.2235` cost=`1.0098` outcome=`0.5500` pnl=`-0.1683` question='Ethereum Up or Down - April 25, 12:00PM-12:15PM ET'
- `live_cap30_20260425_105944` `2069518` ETH/DOWN/NO level=`0.5500` lifecycle=`0.2698` cost=`0.9882` outcome=`0.9990` pnl=`0.8400` question='Ethereum Up or Down - April 25, 12:15PM-12:30PM ET'
- `live_cap30_20260425_105944` `2069574` ETH/DOWN/NO level=`0.7000` lifecycle=`0.2133` cost=`0.9936` outcome=`0.9990` pnl=`0.4450` question='Ethereum Up or Down - April 25, 12:30PM-12:45PM ET'
- `live_p6_edge_cap30_20260425_201000` `2070908` BTC/UP/YES level=`0.6500` lifecycle=`0.2891` cost=`3.2500` outcome=`0.9990` pnl=`1.7450` question='Bitcoin Up or Down - April 25, 4:15PM-4:30PM ET'
- `live_p6_edge_cap30_20260425_201000` `2070916` ETH/UP/YES level=`0.6500` lifecycle=`0.3075` cost=`3.3000` outcome=`0.9990` pnl=`1.6950` question='Ethereum Up or Down - April 25, 4:15PM-4:30PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071179` ETH/UP/YES level=`0.5500` lifecycle=`0.2785` cost=`2.7500` outcome=`0.0000` pnl=`-2.7500` question='Ethereum Up or Down - April 25, 4:30PM-4:45PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071422` ETH/DOWN/NO level=`0.7000` lifecycle=`0.3311` cost=`3.5000` outcome=`0.0000` pnl=`-3.5000` question='Ethereum Up or Down - April 25, 4:45PM-5:00PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071424` BTC/UP/YES level=`0.6500` lifecycle=`0.2669` cost=`3.2500` outcome=`0.9900` pnl=`1.7000` question='Bitcoin Up or Down - April 25, 4:45PM-5:00PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071507` BTC/UP/YES level=`0.6500` lifecycle=`0.3010` cost=`3.2500` outcome=`0.9900` pnl=`1.7000` question='Bitcoin Up or Down - April 25, 5:00PM-5:15PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071617` BTC/DOWN/NO level=`0.7000` lifecycle=`0.2585` cost=`3.5500` outcome=`0.9990` pnl=`1.4450` question='Bitcoin Up or Down - April 25, 5:30PM-5:45PM ET'
- `live_p6_edge_cap30_20260425_201000` `2071679` ETH/UP/YES level=`0.7000` lifecycle=`0.2412` cost=`3.5000` outcome=`0.9900` pnl=`1.4500` question='Ethereum Up or Down - April 25, 5:45PM-6:00PM ET'
- `live_p6_edge_cap30_20260425_201000` `2072247` BTC/DOWN/NO level=`0.6500` lifecycle=`0.2180` cost=`3.2500` outcome=`0.9900` pnl=`1.7000` question='Bitcoin Up or Down - April 25, 6:00PM-6:15PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072717` BTC/UP/YES level=`0.5500` lifecycle=`0.3103` cost=`2.7500` outcome=`0.9900` pnl=`2.2000` question='Bitcoin Up or Down - April 25, 7:15PM-7:30PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072760` BTC/DOWN/NO level=`0.5500` lifecycle=`0.2933` cost=`2.7500` outcome=`0.0000` pnl=`-2.7500` question='Bitcoin Up or Down - April 25, 7:30PM-7:45PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072825` BTC/DOWN/NO level=`0.6500` lifecycle=`0.2352` cost=`3.2500` outcome=`0.0000` pnl=`-3.2500` question='Bitcoin Up or Down - April 25, 7:45PM-8:00PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072883` ETH/DOWN/NO level=`0.5500` lifecycle=`0.2601` cost=`2.7500` outcome=`0.4500` pnl=`-0.5000` question='Ethereum Up or Down - April 25, 8:00PM-8:15PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072866` BTC/DOWN/NO level=`0.6500` lifecycle=`0.2018` cost=`3.2500` outcome=`0.9900` pnl=`1.7000` question='Bitcoin Up or Down - April 25, 8:00PM-8:15PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072950` BTC/UP/YES level=`0.6500` lifecycle=`0.2067` cost=`3.2500` outcome=`0.9990` pnl=`1.7450` question='Bitcoin Up or Down - April 25, 8:30PM-8:45PM ET'
- `live_p6_edge_cap200_20260425_230202` `2072957` ETH/UP/YES level=`0.5500` lifecycle=`0.2101` cost=`2.7500` outcome=`0.9900` pnl=`2.2000` question='Ethereum Up or Down - April 25, 8:30PM-8:45PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073017` BTC/UP/YES level=`0.5500` lifecycle=`0.2155` cost=`2.7500` outcome=`0.3200` pnl=`-1.1500` question='Bitcoin Up or Down - April 25, 8:45PM-9:00PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073014` ETH/UP/YES level=`0.6500` lifecycle=`0.2016` cost=`3.0500` outcome=`0.0000` pnl=`-3.0500` question='Ethereum Up or Down - April 25, 8:45PM-9:00PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073062` ETH/DOWN/NO level=`0.5500` lifecycle=`0.2225` cost=`2.7500` outcome=`0.0000` pnl=`-2.7500` question='Ethereum Up or Down - April 25, 9:00PM-9:15PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073063` BTC/DOWN/NO level=`0.5500` lifecycle=`0.3228` cost=`2.7500` outcome=`0.0000` pnl=`-2.7500` question='Bitcoin Up or Down - April 25, 9:00PM-9:15PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073097` BTC/DOWN/NO level=`0.5500` lifecycle=`0.3229` cost=`2.8000` outcome=`0.0000` pnl=`-2.8000` question='Bitcoin Up or Down - April 25, 9:15PM-9:30PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073123` BTC/DOWN/NO level=`0.5500` lifecycle=`0.2552` cost=`2.7500` outcome=`0.1200` pnl=`-2.1500` question='Bitcoin Up or Down - April 25, 9:30PM-9:45PM ET'
- `live_p6_edge_cap200_20260425_230202` `2073122` ETH/UP/YES level=`0.6500` lifecycle=`0.2465` cost=`3.2500` outcome=`0.0000` pnl=`-3.2500` question='Ethereum Up or Down - April 25, 9:30PM-9:45PM ET'
