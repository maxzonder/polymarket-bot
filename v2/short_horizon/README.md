# short_horizon — MVP short-horizon Polymarket bot

This directory is the home for the **separate short-horizon bot/framework MVP** described in:
- issue `#117` — design brief
- issue `#118` — Phase 0 + Phase 1 implementation roadmap

## Scope

The MVP is intentionally narrowed to the single validated slice from `#115`:
- `15m` exact markets
- `ascending`
- relative bucket `20_40pct`
- price levels `{0.55, 0.65, 0.70}`
- BTC + ETH asset tier only
- fee-aware taker on first touch
- resolution exit
- all-side aggregate first

Current implementation direction after Phase 0:
- Python-first
- `$10` per signal
- no intra-lifecycle re-arm after first touch

This folder is for the new execution-grade bot path.
It is **separate** from:
- the swan bot
- the existing `v2` observation-only experiments

## Live Polymarket prerequisites

For real `--execution-mode live` runs with an EOA/private key wallet, Polymarket needs one-time Polygon approvals before the first real order can succeed.

- The short-horizon live runner now has a dedicated flag: `--approve-allowances`
- That flag sends Polygon mainnet approvals for:
  - USDC collateral spend
  - conditional-token `setApprovalForAll`
  - all three Polymarket spender contracts used by the exchange / neg-risk path
- These approvals are written on-chain on Polygon, not into the repo or SQLite probe DB:
  - USDC allowance is written on token contract `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`
  - conditional-token operator approval is written on contract `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`
  - spender/operator addresses approved by the bot:
    - `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`
    - `0xC5d563A36AE78145C45a50134d48A1215220f80a`
    - `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296` (neg-risk adapter)
- Use it when:
  - the wallet is new
  - the wallet address changed
  - approvals were revoked
- It is not required on every run once the wallet is already approved.

Example bounded live smoke with explicit approvals:

```bash
POLYMARKET_DATA_DIR=/home/polybot/.polybot ./.venv/bin/python v2/short_horizon/bin/live_runner \
  /home/polybot/.polybot/short_horizon/probes/live_probe.sqlite3 \
  --mode live \
  --execution-mode live \
  --allow-live-execution \
  --approve-allowances \
  --confirm-live-order \
  --max-live-orders-total 1 \
  --max-runtime-seconds 3600
```

## Phase 0 artifacts

Phase 0 design and decision artifacts live in:
- `docs/phase0/`

Notable Phase 0 clarification already captured there:
- `trigger_definition_alignment.md` documents the exact gap between historical research (`trade-price` first-touch) and live MVP semantics (`best_ask` first-touch), plus why the current historical dataset cannot honestly remove that gap.
- `taker_slippage_stress_check.md` records the completed `+1 tick` pessimistic entry stress on the strict `20_40pct / ascending / {0.55, 0.65, 0.70}` research slice.

These docs are intended to be concrete enough that a developer can implement Phase 1 without reopening core design debates.
