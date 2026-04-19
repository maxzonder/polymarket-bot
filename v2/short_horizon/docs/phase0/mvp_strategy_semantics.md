# P0-E / P0-F — MVP strategy semantics and lifecycle clock

This document removes operational ambiguity from the first MVP strategy.
A developer should be able to implement the strategy without reopening design questions.

## Strategy identity

The MVP strategy is intentionally narrowed to the validated #115 slice:
- `15m` exact markets
- `ascending`
- relative bucket `20_40pct`
- price levels `{0.55, 0.65, 0.70}`
- asset tier filter `BTC + ETH` only for the first trading MVP
- fee-aware taker on first touch
- resolution exit
- all-side aggregate first

Phase 0 language-gate outcome:
- implement the first live MVP in **Python**
- keep the Rust path deferred unless later live evidence materially changes the latency case

This is an **execution-validation MVP**, not a generalized multi-strategy engine yet.
The architecture should remain extensible, but the first implementation target is exactly this slice.

---

## 1. Market discovery

### Goal
Enumerate live markets eligible for the MVP strategy.

### MVP rule
In live mode, the bot maintains a working set of active markets satisfying all of:
- market is active/open for trading
- market duration is exactly 15 minutes within a narrow tolerance
- market belongs to the MVP asset tier filter (`BTC` / `ETH` only)
- market metadata is sufficiently fresh to compute lifecycle fraction
- fee metadata is present and fresh enough for order decisions

### Discovery source
Use Polymarket market metadata as the source of truth for:
- `market_id`
- `condition_id`
- `token ids`
- `start_time`
- `end_time`
- market status
- fee metadata or fee lookup hook

Initial expected source path:
- metadata bootstrap via Gamma API or equivalent Polymarket metadata endpoint
- live orderbook / trades / user execution updates via CLOB websocket-first event flow
- REST only as bootstrap, fallback, or reconciliation support path, not as the main live event path

### MVP asset scope refinement
For the first live trading MVP, the strategy universe is intentionally restricted to the deepest short-horizon asset tier:
- Bitcoin
- Ethereum

Reason:
- live depth measurements showed BTC/ETH are materially thicker than the rest of the short-horizon universe
- at `$10`, the validated `20–40%` lifecycle slice remains tradable with acceptable throughput
- broader asset coverage can come later after the execution path is stable

### Refresh policy
- metadata bootstrap / universe refresh at a fixed interval, default every `30s`
- immediate in-memory update on any relevant `MarketStateUpdate`
- live tradable state should primarily evolve from websocket-driven events, not polling loops
- a market is removed from the tradable set when:
  - status is no longer active
  - lifecycle fraction leaves the target zone before entry
  - fee metadata becomes stale beyond TTL
  - market-data freshness falls beyond stale threshold

### 15m exact definition
For MVP live filtering, “15m exact” means:
- `duration_seconds = end_time - start_time`
- accepted if `duration_seconds` is within `[840, 960]` seconds

This tolerance exists to absorb minor metadata drift and serialization quirks while still meaningfully targeting 15-minute markets.

### Research/live alignment note
The current #115 research pass used a strict duration filter of `0.25` hours in the analyzer, not the live tolerance window above. That means live eligibility and historical research eligibility are close, but not literally identical. Before micro-live strategy conclusions are treated as apples-to-apples with #115, the analyzer should be rerun or checked under the same tolerance policy.

---

## 2. Relative lifecycle bucket semantics

### Core formula
Lifecycle fraction at decision time is:

`lifecycle_fraction = (decision_event_time - start_time_snapshot) / (end_time_snapshot - start_time_snapshot)`

Where:
- `decision_event_time` is the `event_time` of the triggering market-data event
- `start_time_snapshot` and `end_time_snapshot` come from the most recent `MarketStateUpdate` with `event_time <= decision_event_time`

### Target bucket
The MVP strategy only evaluates entries when:
- `0.20 <= lifecycle_fraction < 0.40`

### No future leakage rule
Replay must not use final known market metadata if a different `end_time` was in force at the moment of the historical event.
Only the latest state known **at or before** the event is allowed.

### End-time update rule
If `end_time` changes during market life:
- decisions before the corresponding `MarketStateUpdate` use the old value
- decisions after the update use the new value
- earlier events are never recomputed retroactively

### Unit-test scenario that must exist later
Example scenario:
- market starts at `14:15:00`
- initial end time is `14:30:00`
- at `14:21:00`, venue updates end time to `14:31:00`
- a trigger event at `14:20:00` must use `14:30:00`
- a trigger event at `14:22:00` must use `14:31:00`

Replay is correct only if both events keep those distinct fractions.

---

## 3. "First touch" definition

### Trigger levels
The strategy watches these price levels:
- `0.55`
- `0.65`
- `0.70`

### Ascending touch definition
For a given token and price level `L`, an **ascending first touch** occurs when:
- the immediately preceding observed tradable price state was strictly below `L`
- the current observed tradable entry state is at or above `L`
- no earlier touch at level `L` has already been recorded for that token during the same market lifecycle

### Entry-state source for live detection
For MVP taker execution, the relevant live trigger should be based on visible ask-side entryability:
- primary signal field: `best_ask`
- ascending touch at level `L` means previous `best_ask < L` and current `best_ask >= L`

Reason:
- the strategy is fee-aware taker
- actual executable entry depends on the ask, not mid or last trade alone

### Research/live alignment note
The current `analyze_taker_resolution.py` research path inherits touch detection from `build_touch_events_for_token`, which is trade-price-touch based rather than `best_ask`-touch based. So the live MVP trigger is intentionally more execution-grounded, but it is not a perfect one-to-one copy of the historical trigger definition. This mismatch should stay visible in docs and in any replay/live interpretation.

### First-touch memory scope
First touch is tracked per:
- `market_id`
- `token_id`
- `price_level`
- lifecycle instance

Once a token has fired for a level during a given market lifecycle, that same level does **not** re-arm for that token in the same lifecycle.

### Re-arm policy
For MVP:
- no re-arm after cancel
- no re-arm after reject
- no re-arm after fill
- no re-arm after market-data disconnect recovery if the first-touch flag had already been set before disconnect

This is intentionally conservative and keeps live behavior close to the research framing of “first touch.”

---

## 4. Entry price policy

This is one of the most important live-vs-research clarifications.

### Research baseline
The research pass effectively enters at the touch price when the trigger first occurs.

### MVP live policy
The live MVP uses:
- **takerable limit-buy at the current best ask** when the first touch is observed

Operationally:
- if first touch at level `L` is detected and current `best_ask` is known,
- place a BUY order priced at current `best_ask`
- expected effect is immediate taker execution at that ask or better

### Worse-than-level guard
If, at the moment we detect the touch, the visible `best_ask` has already moved too far above the level, the order is skipped.

#### MVP guard
For level `L`, skip if:
- `best_ask > L + 0.01`

Meaning:
- tolerate up to one tick of visible drift beyond the trigger level
- skip if the market has already moved by more than one cent beyond the intended trigger

This is a conservative compromise between:
- entering too optimistically at stale trigger price
- and missing every slightly-late but still-valid taker opportunity

### Missing-book rule
If touch is inferred but `best_ask` is unavailable or stale beyond threshold:
- do not trade
- log as skipped due to unusable entry book state

---

## 5. Exit policy

### MVP rule
- no stop-loss
- no profit-take
- no discretionary early exit
- hold to resolution

This must be treated as a conscious design choice, not an omitted feature.

### Resolution settlement
At resolution:
- winning token settles to `1.0`
- losing token settles to `0.0`
- realized PnL must include entry fee paid

---

## 6. Position sizing

### MVP default
- fixed notional size per signal: `$10`

### Why fixed notional
- keeps micro-live simple
- keeps replay/live comparison easier
- reduces confounding from dynamic bankroll scaling in the first MVP slice

### Size translation
For a BUY at price `P`:
- token quantity = `target_trade_size_usdc / P`

Subject to:
- venue min-size constraints
- price/size rounding rules
- skip if rounded size becomes invalid or too small

---

## 7. Concurrency limits

### MVP rules
- maximum one open order per `market_id`
- maximum one live position per `market_id`
- maximum one fired first-touch action per `(market_id, token_id, price_level)`
- global open-order and stake caps come from config

### Practical implication
If one level already triggered and produced an entry for a market/token, the bot should not stack multiple entries at the same level during that market lifecycle.

---

## 8. Token side rule

### MVP rule
The strategy buys the token whose own executable ask first crosses the trigger level in the validated slice.

Interpretation:
- the trigger is evaluated on the token being traded
- no extra YES-only or NO-only narrowing is added in the first MVP implementation
- `NO-only` is explicitly deferred as a later refinement step after the all-side aggregate slice has round-tripped replay → micro-live

This matches the current MVP narrowing choice:
- exact validated slice first
- side refinement later if the round-trip supports it

---

## 9. Fee handling at decision time

### MVP rule
An order may only be sent if fee metadata is fresh.

Required behavior:
- fee metadata fetched on market attach / subscribe
- refreshed on any relevant market metadata update path if available
- rejected for new entry if fee metadata age exceeds configured TTL

### Decision-time fee use
Before placing an entry order, execution must compute:
- expected fee rate in bps
- estimated fee in USDC for the intended size
- estimated net edge after fee under the current strategy assumptions

If fee data is missing or stale:
- skip the order
- emit explicit telemetry reason

---

## 10. Staleness and safety gates

The strategy must not act if any of the following are true at decision time:
- latest `BookUpdate` for the token is older than stale threshold
- latest `MarketStateUpdate` for the market is missing or stale beyond acceptable policy
- fee metadata is stale beyond TTL
- market is no longer active
- lifecycle fraction is outside `[0.20, 0.40)`
- best ask is unavailable
- best ask drift exceeds one tick beyond trigger level
- risk limits would be breached

Each skip should produce a concrete reason in telemetry.

---

## 11. Operational decision flow

For each `BookUpdate` on a tracked token:
1. load latest market state snapshot valid at this `event_time`
2. verify market is active and 15m-exact eligible
3. compute lifecycle fraction from as-of-event state
4. check fraction is in `20_40pct`
5. evaluate ascending first-touch condition for levels `{0.55, 0.65, 0.70}`
6. if no first touch, do nothing
7. if first touch detected, verify:
   - market data fresh
   - fee data fresh
   - best ask available
   - best ask not more than one tick above trigger
   - per-market and global risk limits pass
8. compute size from fixed `$10` notional
9. place takerable limit-buy at current best ask
10. mark first-touch memory so the same `(market, token, level, lifecycle)` does not fire again
11. hold resulting position to resolution

---

## 12. Things explicitly out of scope for the first MVP strategy

- NO-only narrowing
- maker entry variants
- stop-loss logic
- trailing exits
- re-arming triggers after cancel/reject/fill
- dynamic position sizing
- 5m or 60m expansion
- multi-level pyramiding inside the same market lifecycle

---

## Bottom line

This MVP strategy is deliberately narrow and operationally strict:
- detect first executable ascending ask touch
- only in the relative `20_40pct` lifecycle window
- only on 15m-exact markets
- only at levels `{0.55, 0.65, 0.70}`
- buy with fixed `$10` fee-aware taker size
- skip if book/fee/lifecycle state is not trustworthy
- hold to resolution

That should be enough to test whether the validated #115 slice survives actual execution without contaminating the result with premature generalization.
