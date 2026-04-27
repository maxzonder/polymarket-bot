# Phase 3 execution overview

Date: 2026-04-23
Issue: #120

This document is the implementation-side close-out for the short-horizon Phase 3 execution path.

It records what the code actually does now, not what the original roadmap expected in theory.

## 1. High-level execution boundary

The short-horizon live shell now has three execution modes:
- `synthetic`
- `dry_run`
- `live`

Current boundary split:
- strategy/runtime decides whether to emit an `OrderIntent`
- execution layer translates that intent into a venue order request
- `py-clob-client` is the only authenticated signing / posting boundary
- websocket user stream is the primary source for live order/fill/cancel feedback
- REST venue lookup is still used for startup reconciliation and ambiguity recovery

## 2. Exact `py-clob-client` methods used

### Startup/auth sequence

`PolymarketExecutionClient.startup()` currently does this exact sequence:

1. resolve `POLY_PRIVATE_KEY` from environment
2. instantiate SDK client with:
   - `host=https://clob.polymarket.com`
   - `chain_id=137`
3. call `create_or_derive_api_creds()`
4. call `set_api_creds(...)`
5. run `list_open_orders()` as a connectivity/auth healthcheck

So the startup proof is not just object construction, it includes one authenticated venue call.

### Order placement sequence

For a live submit:

1. `ExecutionEngine` translates `OrderIntent` into `VenueOrderRequest`
2. `PolymarketExecutionClient._build_order_args(...)` builds SDK `OrderArgs` with:
   - `price`
   - `size`
   - `side`
   - `token_id`
   - optional `client_order_id`
   - optional `time_in_force`
   - optional `post_only`
3. `create_and_post_order(order_args)` is called
4. venue response is normalized into `VenuePlaceResult`
5. local order row is updated with `venue_order_id`
6. canonical `OrderAccepted` is appended to `events_log`

### Cancel sequence

For a live cancel:

1. local order must already have `venue_order_id`
2. `cancel(venue_order_id)` is called through the SDK wrapper
3. if venue immediately returns a canceled status, local order is terminalized right away
4. otherwise local order moves to `cancel_requested` and waits for authoritative venue/user-stream confirmation

### Venue reconciliation sequence

Startup reconciliation uses:
- `get_order(venue_order_id)` first when we already know the venue id
- `get_orders(market=...)` fallback for open-order scans

This is enough for the current live restart path because reconciliation only needs authoritative current venue state, not a full historical closed-order ledger.

## 3. User-stream endpoint and subscription payload

Authenticated user-stream endpoint:
- `wss://ws-subscriptions-clob.polymarket.com/ws/user`

Current subscription payload shape:

```json
{
  "auth": {
    "apiKey": "<derived api key>",
    "secret": "<derived secret>",
    "passphrase": "<derived passphrase>"
  },
  "markets": ["<market_id>", "..."],
  "type": "user"
}
```

Runtime behavior:
- the stream keeps a subscribed market set
- on reconnect it resends the full current subscription payload
- ping loop sends `PING`
- `PONG` and other control frames are ignored

Current normalization rules:
- `event_type=order` with placement-style payload -> `OrderAccepted`
- `event_type=order` with update matched size delta -> `OrderFilled`
- cancellation payload -> `OrderCanceled`
- rejection payload -> `OrderRejected`
- `event_type=trade` with `MATCHED` -> backfilled `OrderFilled` deltas for taker and relevant maker orders

## 4. Fee handling

There are two different fee truths in the current architecture, and they should not be confused.

### 4.1 Strategy/runtime fee metadata

The short-horizon bot currently gets `fee_rate_bps` from Gamma market metadata, not from a dedicated SDK fee call.

Current source fields:
- `feeSchedule.rate` -> converted to basis points
- fallback `takerBaseFee`

Current refresh path:
- `FeeMetadataRefreshLoop`
- refresh cadence is `min(discovery refresh interval, fee TTL)`
- with current defaults:
  - discovery refresh interval = `30s`
  - fee TTL = `60s`
  - effective fee refresh cadence = `30s`

These fee snapshots are attached to canonical `MarketStateUpdate` and used by runtime/strategy logic for fee freshness checks.

### 4.2 Signed-order fee handling inside SDK

The implementation still treats the SDK as the authoritative signing boundary.

Practical meaning:
- short-horizon code does not hand-roll `feeRateBps` into signed orders
- SDK-side order creation/posting remains responsible for whatever authenticated fee/signature fields Polymarket requires

## 5. Minimum-size and order translation truth

The live translation path now respects all of these together:
- venue tick size
- venue dollar minimum notional
- market-specific `orderMinSize` share minimum

Current BUY behavior:
- price rounds down to tick size
- target notional is increased when needed to satisfy:
  - minimum dollar floor
  - small safety buffer above the floor when relevant
  - market-specific minimum shares translated into notional
- size rounds up to venue size decimals

Practical consequence:
- a configured micro target like `0.10 USDC` is just the strategy target
- the actual submitted live order can be larger if venue constraints require it

## 6. Restart reconciliation behavior

At live startup:

1. runtime store is opened
2. strategy is hydrated from persisted non-terminal orders
3. `PolymarketExecutionClient.startup()` runs
4. `ExecutionEngine.reconcile_persisted_orders()` walks all local non-terminal orders

Per-order reconciliation lookup order:
- try `venue_order_id` via `get_order(...)`
- fallback to `list_open_orders(market_id=...)`
- match by `client_order_id` first
- fallback to `(token_id, price, original_size)` when necessary

Status handling summary:
- venue says live/open + zero fill -> local `accepted`
- venue says live/open + partial fill -> local `partially_filled`, missing fills backfilled into storage/events
- venue says filled -> local `filled`, missing fills backfilled
- venue says canceled -> local `cancel_confirmed`
- venue says rejected / expired -> local terminal state updated accordingly
- venue has no record:
  - local `intent` -> `rejected` with `VENUE_NOT_FOUND`
  - other non-terminal local states -> `unknown` with reconciliation required

### Market blocking rule

If a market has an `unknown` order, runtime blocks new touches for that market with:
- `SkipDecision(reason="market_reconciliation_blocked")`

### Unblock path

A blocked market becomes unblocked only after that ambiguous order leaves `unknown`, for example via:
- later successful startup reconciliation
- authoritative venue/user-stream evidence moving it into a known state
- explicit terminalization or cleanup

This is intentionally conservative: venue truth wins over local optimism.

## 7. Kill-switch behavior

CLI flag:
- `--kill-switch`

Current hard requirements:
- must be combined with `--mode live`
- must be combined with `--execution-mode live`
- must be combined with `--allow-live-execution`
- requires `POLY_PRIVATE_KEY`
- cannot be combined with:
  - `--approve-allowances`
  - `--redeem-resolved`
  - `--redeem-resolved-interval-seconds`
  - `--bridge-polygon-usdc-to-usdce`
  - `--wrap-polygon-usdc-to-pusd`

Current behavior:
1. startup/auth healthcheck
2. list all currently open orders from venue
3. call cancel for each venue order id
4. log per-order success/failure
5. exit immediately

This is an operator maintenance action, not a strategy-level state transition.

## 8. Production-proven operational notes

These are not abstract design notes, they were learned during real prod launches.

### Live launch prerequisites

For the current prod path, the safest launch pattern is:
- run from `/home/polybot/claude-polymarket`
- use repo venv python: `./.venv/bin/python`
- explicitly source repo `.env`
- explicitly set `POLYMARKET_DATA_DIR=/home/polybot/.polybot`
- verify current CLI shape with `live_runner --help`

Why these matter:
- plain system `python3` can miss installed deps like `structlog`
- `live_runner` itself does not guarantee repo `.env` loading, so `POLY_PRIVATE_KEY` may be missing unless sourced explicitly
- long-running server launches should not rely on shell inheritance for data-dir routing
- current `live_runner` requires positional `db_path`; stale flags like `--probe-db-path` can break launches

### Production-only extensions now proven in real use

These are part of the current real execution path even though they were not in the earliest Phase 3 sketch:
- `--approve-allowances`
- `--bridge-polygon-usdc-to-usdce` (deprecated post-2026-04-28 cutover)
- `--bridge-polygon-usdc-amount`
- `--wrap-polygon-usdc-to-pusd` (V2 collateral path)
- `--wrap-polygon-usdc-amount`
- `--redeem-resolved`
- `--redeem-resolved-interval-seconds`

Operational intent:
- allowances = one-time or recovery maintenance
- bridge = legacy V1 collateral prep from native Polygon `USDC` into `USDC.e` (kept for pre-cutover replays only)
- wrap = V2 collateral prep — convert existing `USDC.e` into `pUSD` via the Collateral Onramp `wrap()` call (`0x93070a847efEf7F70739046A929D47a521F5B8ee`)
- redeem = wallet-level resolved-position cleanup before/during a live run

## 9. Telemetry and audit trail truth

Current live execution now records:
- accepted live orders into `events_log`
- dry-run translated orders into `events_log`
- live fills from normal user-stream path into `events_log`
- reconciliation-discovered missing fills into `events_log`

This matters because the original live path had a telemetry gap where fills could reach storage/state without a matching `OrderFilled` event-log row in the ordinary live stream path.
That gap is now closed.

## 10. Practical summary for Phase 4+

If someone starts from Phase 4, the important truths to keep in mind are:
- execution identity is three-layered: immutable local `order_id`, stable `client_order_id`, nullable `venue_order_id`
- live execution is websocket-first, but not websocket-only
- Gamma still provides market metadata, fee snapshots, and `orderMinSize`
- collateral is migrating from V1 `USDC.e` (deprecated) to V2 `pUSD` at the 2026-04-28 cutover; the live runner now exposes `--wrap-polygon-usdc-to-pusd` for the V2 Collateral Onramp `wrap()` flow alongside the legacy bridge helper
- resolved-position settlement is intentionally outside strategy logic
- operational launch hygiene matters as much as pure code correctness for prod reproducibility

## 11. Replay execution hooks (Phase 4 addition)

As of Phase 4, the execution client is wrapped by `ReplayCaptureWriter` during live runs if `--capture-dir` is provided. This wrapper records all request/response payloads at the boundary. During replay, the live SDK is entirely replaced by `CapturedResponseExecutionClient`, which serves those recorded responses deterministically. The core execution engine (`v2/short_horizon/short_horizon/execution/engine.py`) requires no changes to support replay — it simply receives the captured-response client instead of the real one.
