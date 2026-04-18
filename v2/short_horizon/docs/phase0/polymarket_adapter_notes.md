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

## 7. Known open questions

These are not blockers for Phase 0 docs, but they should be treated as concrete follow-up checks during implementation.

1. **Exact websocket endpoints and subscription payloads**
   - need current Polymarket/CLOB docs confirmation
   - especially for book deltas vs full snapshots and user fills/order updates

2. **Fee metadata retrieval path outside SDK**
   - if Python-first with `py-clob-client`, SDK may hide most of this
   - if replay/live tooling needs explicit fee snapshots in storage, need to identify the exact endpoint/field path cleanly

3. **Open-order / closed-order lookup semantics**
   - current wrapper uses `get_orders(...)`
   - need to confirm whether historical/recent closed orders are queryable in a way good enough for restart reconciliation

4. **Minimum order size and price tick constraints**
   - must be captured precisely before execution code is written
   - current docs/code imply they exist, but the exact authoritative source still needs to be pinned

5. **Whether websocket user stream alone is sufficient for reconciliation-sensitive fills**
   - or whether REST backfill is still needed after reconnect/restart

6. **Market discovery source for 15m exact live filtering**
   - Gamma appears sufficient for now, but this should be confirmed against real currently active short-duration markets

---

## 8. Recommended immediate Phase 1/2 stance

Until P0-A / P0-B say otherwise:
- assume Python-first integration is the default implementation path
- use existing repo knowledge and `py-clob-client` as the execution baseline
- keep the adapter boundary clean so a later Rust port remains possible if measurements justify it

That gives the team the fastest path to real replay/live parity without forcing a premature rewrite of the execution stack.

---

## 9. Practical starting references inside this repo

Useful local references before coding:
- `v2/api/clob_client.py`
- `v2/api/gamma_client.py`
- `api/clob_client.py`
- `api/gamma_client.py`
- `docs/BEST_PRACTICES.md`
- `requirements.txt`

These do not define the new architecture, but they do reduce adapter discovery work significantly.
