# Trigger definition alignment note — live vs research

This note closes the specific question: **does the current live MVP use the same trigger definition as the historical research baseline?**

Short answer:
- **No.**
- Live MVP is currently **`best_ask` first-touch**.
- The historical research baseline is currently **trade-price first-touch**.

## 1. What the live MVP uses

The short-horizon live/collector path is pinned to executable ask-side touch semantics.

Current definition:
- previous `best_ask < L`
- current `best_ask >= L`

Relevant code/docs:
- `scripts/measure_live_depth_and_survival.py`
- `v2/short_horizon/docs/phase0/mvp_strategy_semantics.md`
- `v2/short_horizon/docs/phase0/mvp_config.yaml`

Why:
- the MVP is taker-oriented
- execution viability depends on visible ask-side entryability, not abstract last-trade movement

## 2. What the historical research uses

The research path behind `scripts/analyze_taker_resolution.py` delegates touch detection to:
- `build_touch_events_for_token(...)`
- in `scripts/analyze_price_resolution.py`

That function operates on `TradePoint` rows from `historical_tape.db` and records a touch when:
- previous trade price and current trade price cross a level
- plus the configured directional lookback condition is satisfied

Operationally this is:
- **trade-price first-touch**, not `best_ask` touch

The taker research script then uses:
- `entry_price = event.touch_price`

So the research entry assumption is tied to the first qualifying **trade print**, not to the first visible executable ask crossing.

## 3. Why this mismatch cannot currently be eliminated honestly

The current historical source available to the research scripts is `historical_tape.db`.

Verified schema on the production dataset:
- tables: `meta`, `source_files`, `tape`
- `tape` columns:
  - `timestamp`
  - `market_id`
  - `token_id`
  - `price`
  - `size`
  - `side`

What is **not** present:
- L2 orderbook snapshots
- best bid / best ask history
- top-of-book state at the touch timestamp
- book deltas that would let us reconstruct historical `best_ask`

Therefore:
- we **cannot honestly rerun** the historical research under exact `best_ask` semantics from the current historical dataset
- any attempt to infer `best_ask` from trade tape would be a heuristic, not the same trigger definition

## 4. Practical consequence

The validated research edge from `analyze_taker_resolution.py` is **not strictly apples-to-apples** with the live MVP trigger.

What is still true:
- the research is still useful as evidence that the slice has predictive structure
- the live collector is still useful for execution viability and depth/survival validation

What is not justified:
- claiming that the historical `trade-price` edge transfers 1:1 to the live `best_ask` trigger without further validation

## 5. Current recommended stance

Treat the trigger-definition gap as:
- **documented and known**
- **not a blocker** for Phase 1 skeleton work
- **still open** for exact research/live equivalence

## 6. Honest ways to close the gap later

There are only two honest options:

1. **Acquire historical orderbook / L2 data**
   - then implement a research path for historical `best_ask` first-touch

2. **Validate the live `best_ask` trigger directly**
   - keep collecting live depth/survival data
   - later run micro-live / shadow-execution validation on the actual live trigger semantics

Until one of those happens, the correct statement is:
- **research trigger = trade-price first-touch**
- **live MVP trigger = best_ask first-touch**
- **the gap is explicit, not resolved**

## 7. Signal-rate discrepancy note

There is also a real **signal-rate discrepancy** between the live collector readout and the historical research estimate.

Observed values from the current Phase 0 work:
- live BTC+ETH first-touch collector readout: about **`902/day`** across all relative buckets
- historical taker research estimate from the strict validated slice: about **`60/day`**

This is roughly a **15x gap**.

Current best explanation is structural, not mysterious:
- the live collector fires on **both YES and NO tokens**
- the live collector currently records **best_ask first-touch** without the research-side `ascending` trade-price construction
- the live collector coverage used **all relative buckets**, while the strict MVP research slice is only **`20_40pct`**
- the historical research path also bundles in its own directional / touch-construction assumptions from trade tape

What this means operationally:
- the live collector should be treated as an **execution-viability superset** signal source
- the historical research estimate should be treated as the **edge-estimation baseline** for the stricter research slice
- the two numbers should **not** be compared as if they were the same trigger population

So the discrepancy is:
- **documented**
- **expected under the current definitions**
- **not a blocker** for Phase 1 module work
