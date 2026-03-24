# v2 — Observation Layer

Pure data collection. No trading. Goal: validate two arbitrage hypotheses on live data before building execution.

## Hypotheses

**Path A — Neg-Risk Group Arb**
Sum of YES-token best_ask across all outcomes in a neg-risk event must equal ~$1 at resolution.
If sum_best_ask < 0.97, there's a potential arb gap. Observer tracks how often this happens,
how deep, how long it lasts, and whether dislocated groups resolve differently.

**Path B — Crypto Threshold Mispricing**
Black-Scholes model price for BTC/ETH/SOL binary threshold questions vs Polymarket YES-token price.
Observer tracks the gap to see if Polymarket systematically mis-prices these and whether
the gap direction predicts the outcome.

---

## Scripts

| Script | What it does |
|---|---|
| `run_observers.py` | Scheduler — runs observers in parallel threads |
| `observers/negrisk.py` | Phase 1A: fetches neg-risk markets, picks YES token per leg, computes sum_best_ask, detects dislocations, reconciles resolved groups |
| `observers/crypto.py` | Phase 1B: fetches crypto threshold markets, picks YES token, computes BS model price, reconciles resolved outcomes |
| `smoke_test.py` | Validates API connectivity, DB init, question parser |
| `reports/negrisk_daily.py` | Dislocation report: liquid vs stale split, net gap after fee, persistence buckets, resolved group stats |
| `reports/crypto_daily.py` | Gap report: per-market dedup, accuracy segmented by gap quartile / tte / asset |

---

## Running

All commands from the repo root (`/home/polybot/claude-polymarket`):

```bash
# Both observers in parallel (recommended)
python3 -m v2.run_observers

# Neg-risk only
python3 -m v2.run_observers --only negrisk

# Crypto only
python3 -m v2.run_observers --only crypto
```

In production — run in tmux:
```bash
tmux new-session -d -s v2_negrisk "python3 -m v2.run_observers --only negrisk"
tmux new-session -d -s v2_crypto  "python3 -m v2.run_observers --only crypto"
```

Smoke test (run before first deploy):
```bash
python3 -m v2.smoke_test
```

Daily reports:
```bash
python3 -m v2.reports.negrisk_daily                      # last 24h, liquidity floor $5
python3 -m v2.reports.negrisk_daily --hours 6
python3 -m v2.reports.negrisk_daily --hours 24 --min-size 10   # stricter liquidity gate

python3 -m v2.reports.crypto_daily                       # last 24h, tte≥4h, poly≤0.90
python3 -m v2.reports.crypto_daily --hours 6
python3 -m v2.reports.crypto_daily --min-tte 24          # only markets with >24h left
```

---

## Poll intervals

| Observer | Interval | Reason |
|---|---|---|
| neg-risk | 5 min | CLOB calls per leg are slow; 35k markets need ~10-20 min per cycle |
| crypto | 30 min | CoinGecko rate limits; BS model stable over 30 min |

---

## DB schema

Data stored in `$POLYMARKET_DATA_DIR/v2/` — set in `.env` as `/home/polybot/.polybot`.

### obs_negrisk.db

| Table | Contents |
|---|---|
| `nr_groups` | One row per event slug — event name, n_markets, first_seen_ts |
| `nr_snapshots` | One row per 5-min cycle per group — sum_best_ask, sum_mid, is_dislocated |
| `nr_legs` | One row per YES-token per snapshot — bid/ask/size/last_trade_price |
| `nr_dislocations` | Continuous dislocation episodes — start_ts, end_ts, min_sum_ask, max_gap |
| `nr_resolved` | Groups confirmed closed by Gamma — winner_market_id, had_dislocation |

### obs_crypto.db

| Table | Contents |
|---|---|
| `cr_markets` | One row per parsed market — asset, threshold, direction, start_ts, expiry_ts |
| `cr_snapshots` | One row per 30-min cycle — polymarket_price, model_price, gap, tte_hours, in_garbage_time |
| `cr_resolved` | Resolved markets — outcome, last_gap, was_directionally_correct |

---

## Key design decisions

- **YES token always**: both observers pick the YES outcome token by matching `outcomes[]` labels, not by index or cheapest price. This makes `sum_best_ask` a coherent resolution-consistent basket.
- **Garbage time**: crypto observer skips last 2% of market duration, computed from real `startDate` (not observer first-seen time).
- **Regex parser for crypto**: intentional Phase 1 baseline — high precision, lower recall. LLM parser planned for Phase 2 when recall matters more.
- **Dislocation episodes**: `end_ts=NULL` means open. Only set on exit from dislocation zone, never during continuation.
- **Fee estimate**: sports basket fee ~0.8% (rate=0.03, exp=1, p≈0.33). Shown as "Net" column in negrisk report.

---

## Logs

```
$POLYMARKET_DATA_DIR/v2/logs/negrisk_observer.log   — rotating, 5 MB × 3
$POLYMARKET_DATA_DIR/v2/logs/crypto_observer.log    — rotating, 5 MB × 3
$POLYMARKET_DATA_DIR/v2/logs/run_observers.log      — scheduler thread log
```
