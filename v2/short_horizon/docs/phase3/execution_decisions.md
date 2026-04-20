# Phase 3 execution decisions

Date: 2026-04-20
Issue: #120

## Decisions

- Execution SDK: use `py-clob-client>=0.15.0` as the only signing and authenticated order-placement boundary.
- Signing: stays inside the SDK. We do not hand-roll EIP-712 signing or fee injection.
- Credential source: `POLY_PRIVATE_KEY` from environment only.
- Secret handling: never log the private key, never persist it to SQLite, never echo it into docs or test fixtures.
- Venue access strategy for Phase 3:
  - default validation path is `synthetic` and `dry_run`
  - real venue validation is mainnet only, gated to one explicit tiny live smoke order in `P3-11`
  - no separate testnet/staging path is assumed for Phase 3, because the current repo/docs baseline already targets production CLOB and the roadmap gate is mechanical pipeline validation, not repeated live experimentation

## Why this is the right boundary

- The repo already pins `py-clob-client` in `requirements.txt`.
- Legacy repo code already uses the SDK for `create_or_derive_api_creds()`, `set_api_creds(...)`, `create_and_post_order(...)`, `cancel(...)`, and `get_orders(...)`.
- Reusing the SDK preserves one authoritative auth/signing path and avoids re-discovering venue auth details inside the short-horizon runner.

## Immediate implementation consequences

- Phase 3 wrapper should fail fast if `POLY_PRIVATE_KEY` is absent.
- `live_runner --execution-mode live` must require the env var at startup.
- Most integration validation should happen in `dry_run`, with the real venue touched only at the explicit smoke gate.
