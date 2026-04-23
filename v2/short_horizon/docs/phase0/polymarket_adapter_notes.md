# P0-J — Polymarket adapter research note

This note is meant to eliminate fresh discovery work before Phase 1 / Phase 2 adapter scaffolding starts.

It identifies the concrete API surfaces, likely library choices, and open questions for the short-horizon MVP.

## 1. High-level split of responsibilities

For the MVP, Polymarket integration is naturally split into two planes:

### Metadata plane
Use Polymarket market metadata APIs for:
- market discovery bootstrap
- market status
- `market_id`
- `condition_id`
- `clobTokenIds`
- `outcomes`
- `start_time`
- `end_time`
- broad market metadata required for lifecycle computation

### Live event / execution plane
Use Polymarket CLOB websocket-first infrastructure for:
- orderbook updates
- trade prints
- user order/fill updates
- order placement
- order cancel
- open-order lookup / reconciliation support

Important clarification:
- the new bot should be **websocket-first** for live market data and execution feedback
- metadata APIs are for bootstrap / refresh / fallback support
- REST should not be treated as the main live event path

---

## 2. Known endpoints already used in the repo

### Gamma market metadata
Current code uses:
- base URL: `https://gamma-api.polymarket.com`
- market list endpoint: `GET /markets`

Current repo evidence:
- `v2/api/gamma_client.py`
- `api/gamma_client.py`

Known request parameters already used:
- `closed=false`
- `active=true`
- `include_tag=true`
- `limit`
- `offset`
- volume filters such as `volume_num_min`, `volume_num_max`

Known useful response fields already parsed in repo code:
- `id`
- `conditionId`
- `question`
- `clobTokenIds`
- `outcomes`
- `bestAsk`
- `bestBid`
- `lastTradePrice`
- `liquidity`
- `volumeNum`
- `endDate` / `endDateIso`
- `feesEnabled`
- `tags`
- `events[*].commentCount`

### CLOB REST
Current repo code uses:
- base URL: `https://clob.polymarket.com`
- public orderbook endpoint: `GET /book?token_id=...`

Observed public response structure already handled by repo code:
- `bids[]`
- `asks[]`
- each level with:
  - `price`
  - `size`

For the new MVP this should be treated as:
- bootstrap / debug / fallback / reconciliation support
- not the main live market-data ingestion path

### CLOB authenticated execution via SDK
Current code uses the official Python SDK:
- package: `py-clob-client`

Current repo evidence:
- `requirements.txt`
- `api/clob_client.py`
- `v2/api/clob_client.py`

Methods already used through SDK wrapper:
- `create_or_derive_api_creds()`
- `set_api_creds(...)`
- `create_and_post_order(...)`
- `cancel(order_id)`
- `get_orders(...)`

---

## 3. Websocket expectations for MVP

The short-horizon MVP should be **websocket-first** for live market data and user/order updates.
This is not optional wording, it is the intended architecture.

Expected websocket needs:
- public market-data stream for orderbook / trades
- authenticated user stream for fills / order updates, if available

The current repo is mostly REST/polling oriented, because the legacy bot did not need low-latency event flow.
The new MVP will likely need a dedicated websocket adapter instead of only reusing REST wrappers.

### Expected channel groups
These need to be confirmed against current Polymarket/CLOB docs during implementation, but the adapter should be designed around at least:
- book / orderbook updates
- trade prints
- user order status updates
- user fills

### Why this matters
- `P0-B` (live ask survival measurement) likely depends on websocket ingestion
- the MVP strategy trigger source is currently pinned to `best_ask` touch
- replay/live parity is stronger if the same conceptual event stream exists in both paths

---

## 4. Authentication and signing

### Current practical path in this repo
The existing code relies on:
- `POLY_PRIVATE_KEY` from environment
- official `py-clob-client`

This is already enough to establish the likely MVP execution path for Python-first implementation.

### Signing model
Based on current repo docs and wrapper usage:
- Polymarket CLOB order execution uses signed orders
- signing is handled through the official SDK
- signing flow involves EIP-712 typed data semantics
- the SDK also derives or manages API credentials used for authenticated exchange calls

### Important engineering note
Local repo docs already state a key point:
- the official SDK automatically fetches current taker fee information and injects `feeRateBps` into signed orders

This is important because the MVP is explicitly fee-sensitive.
For Python-first execution, SDK support should be treated as the default path unless measurement work later justifies a different language/runtime.

---

## 5. Recommended library choice by language path

### If Python is chosen after P0-A / P0-B
Recommended execution stack:
- `py-clob-client` for order placement/cancel/open-order queries
- `requests` or `httpx` for Gamma metadata if SDK does not cover it cleanly
- websocket client library to be selected in Phase 1/2 for book/user streams

Recommended signing path:
- stay with `py-clob-client`
- avoid hand-rolling EIP-712 signing unless forced by a missing feature

### If Rust is chosen later
Likely path:
- use `ethers-rs` or equivalent for signing primitives
- implement CLOB HTTP/websocket integration manually or via any maintained Polymarket-specific crate if one is found and deemed trustworthy

But for now this should remain a secondary path.
The current codebase and current research stack make Python-first integration the more immediately supported route.

---

## 6. Adapter responsibilities that Phase 1/2 should keep isolated

The venue adapter should own:
- payload translation between internal order intent and Polymarket order request shape
- price/size rounding to venue constraints
- fee metadata acquisition / freshness tracking
- auth/session/bootstrap details
- order-status normalization into canonical event vocabulary
- mapping of raw book/trade/user events into normalized internal events

The strategy layer should **not** know:
- raw websocket payload shape
- SDK method names
- signature details
- REST endpoint details
- credential layout

---

## 7. Phase 3 close-out answers to the earlier open questions

The Phase 3 implementation now answers the questions that were still open during the original adapter research pass.

### 7.1 Exact websocket endpoints and subscription payloads

This is now implemented, not speculative.

- Public market-data websocket remains the primary live source for book/trade updates.
- Authenticated user-stream endpoint is:
  - `wss://ws-subscriptions-clob.polymarket.com/ws/user`
- User-stream subscription payload shape is:

```json
{
  "auth": {
    "apiKey": "<derived by py-clob-client>",
    "secret": "<derived by py-clob-client>",
    "passphrase": "<derived by py-clob-client>"
  },
  "markets": ["<market_id>", "..."],
  "type": "user"
}
```

- The short-horizon runner resubscribes the current market set after reconnect.
- Raw user frames are normalized into canonical `OrderAccepted`, `OrderFilled`, `OrderRejected`, and `OrderCanceled` events.

### 7.2 Fee metadata retrieval path

The short-horizon path does **not** currently fetch `feeRateBps` from an authenticated SDK execution call.

What is implemented today:
- market discovery / refresh parses fee metadata from Gamma market rows
- authoritative fields currently parsed are:
  - `feeSchedule.rate` → converted to basis points
  - fallback `takerBaseFee`
- fee metadata is emitted into canonical `MarketStateUpdate`
- fee snapshots are refreshed by `FeeMetadataRefreshLoop`

Operational cadence currently implemented:
- `FeesConfig.fee_metadata_ttl_seconds = 60`
- fee refresh loop runs every `min(discovery_refresh_interval, fee_metadata_ttl_seconds)`
- with current defaults that means every `30s`

Important boundary:
- the SDK remains the signing/posting boundary and is still trusted to inject venue fee fields into signed orders
- but the strategy/runtime-side fee freshness check is driven by Gamma-derived metadata snapshots, not by a separate SDK fee query

### 7.3 Open-order / closed-order lookup semantics for reconciliation

Restart reconciliation is now implemented with this lookup order:

1. prefer direct `get_order(venue_order_id)` if we already know the venue id
2. otherwise load `list_open_orders(market_id=...)`
3. match by `client_order_id` first
4. fall back to `(token_id, price, original_size)` when needed

Current consequence:
- open/live venue orders are recoverable enough for startup reconciliation
- orders missing from venue lookup are not silently forgotten; they move to local `unknown` or `rejected` depending on local state
- this is why `venue_order_id` is a first-class nullable column while internal `order_id` stays immutable

### 7.4 Minimum order size and tick constraints

This is now captured concretely in code.

- tick rounding for BUY uses venue tick size and rounds price down
- venue dollar minimum is enforced in the translator
- market-specific `orderMinSize` is parsed from Gamma market metadata and propagated into live translation
- final translated order notional is the max of:
  - strategy target notional
  - venue dollar minimum plus buffer when relevant
  - market-specific minimum shares translated into notional

Practical implication:
- a tiny target like `0.10 USDC` is valid as an intent target
- but the live translated order may still be scaled above that target to satisfy venue constraints

### 7.5 Whether websocket user stream alone is sufficient

Answer: **no, not by itself for restart safety**.

What is implemented now:
- user websocket is the primary path for live accept/fill/cancel/reject updates during a healthy run
- startup reconciliation still consults venue REST truth via `get_order(...)` / `get_orders(...)`
- if a process dies between send / fill / cancel transitions, reconciliation uses venue truth to repair local state on restart

So the current architecture is:
- websocket-first for live updates
- REST-assisted for restart reconciliation and ambiguity recovery

### 7.6 Market discovery source for live 15m exact filtering

This is now confirmed operationally.

- Gamma is still the discovery / refresh source for the short-horizon universe
- it is sufficient for attaching active 15m exact markets on production
- shared discovery + refresh supervision were added after the `P3-9` prod dry-run failure mode where refresh loops could die on unhandled Gamma `429`

Remaining nuance:
- Gamma remains good enough for the MVP universe, but it is also a rate-limited dependency, so refresh loops must keep retry/backoff and loud failure propagation

## 8. Phase 2 and Phase 3 implementation status after the lift-first pass

The short-horizon adapter boundary is now materially implemented through `P2-9`.

What is now concretely wired:
- Gamma market discovery lift into canonical `MarketMetadata`
- recurring market refresh loop emitting canonical `MarketStateUpdate`
- market websocket manager with reconnect and subscription replay
- book-channel normalization for `book`, `price_change`, and `best_bid_ask`
- trade-channel normalization for `last_trade_price`
- fee metadata refresh loop with TTL-compatible snapshots
- composite `LiveEventSource` that merges metadata refresh, fee refresh, and websocket-normalized market data
- `live_runner --mode live` wired to that composite source with **synthetic execution still preserved**
- bounded live probe runs plus post-run cross-validation helpers against the independent collector CSV

Important locked-in policies from the implementation:
- the collector remains independent ground truth and must not be modified as part of the Phase 2 adapter lift
- Gamma metadata is the current source for market attach / refresh / fee snapshots
- CLOB websocket is the current source for live book and trade prints
- out-of-order book frames are dropped by older `timestamp` per token to preserve deterministic replay
- fee freshness is tracked from attach / refresh snapshots and enforced at decision time before any entry

What was intentionally open after `P2-9`, and is now materially closed by Phase 3:
- authenticated user-stream normalization for real fills / order updates
- restart reconciliation semantics against exchange truth
- authoritative minimum-size / tick-constraint capture for real venue sends
- the websocket-vs-REST split: websocket for live updates, REST for restart reconciliation

Additional Phase 3 truths that were only discovered during real prod smokes:
- current v1 collateral path still effectively consumes `USDC.e`, not native Polygon `USDC`
- the runner now has an explicit bridge step for native `USDC -> USDC.e`
- resolved-position settlement is handled as a wallet-level maintenance sweep (`--redeem-resolved`), not as strategy logic
- missing fill telemetry on the normal live stream path needed an explicit fix so `OrderFilled` is appended to `events_log` for both stream-driven and reconciliation-driven fills

---

## 9. Recommended working stance after Phase 3

Until a later architecture review says otherwise:
- keep Python + `py-clob-client` as the only authenticated execution/signing boundary
- keep internal `order_id` immutable and store venue identity separately via `venue_order_id`
- keep websocket-first live ingestion, but do **not** remove REST-assisted reconciliation
- keep fee freshness, market metadata, tick-size, and `orderMinSize` attached to canonical `MarketStateUpdate`
- keep wallet maintenance actions (`approve-allowances`, bridge, redeem sweeps) outside strategy decision logic

That gives the team the cleanest path into Phase 4 without re-discovering execution-boundary facts that are now already known.

---

## 10. Practical starting references inside this repo

Useful local references before coding:
- `v2/api/clob_client.py`
- `v2/api/gamma_client.py`
- `api/clob_client.py`
- `api/gamma_client.py`
- `docs/BEST_PRACTICES.md`
- `requirements.txt`

These do not define the new architecture, but they do reduce adapter discovery work significantly.
