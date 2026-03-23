# v2 — Observation Layer

Pure data collection. No trading. Goal: validate two arbitrage hypotheses on live data before building execution.

## Hypotheses

**Path A — Neg-Risk Group Arb**
Sum of best_ask across all outcomes in a neg-risk event must equal ~$1 at resolution.
If sum_best_ask < 0.97, there's a potential arb gap. Observer tracks how often this happens,
how deep, and how long it lasts.

**Path B — Crypto Threshold Mispricing**
Black-Scholes model price for BTC/ETH/SOL binary threshold questions vs Polymarket price.
Observer tracks the gap to see if Polymarket systematically mis-prices these.

---

## Scripts

| Script | What it does |
|---|---|
| `run_observers.py` | Scheduler — runs observers in parallel threads |
| `observers/negrisk.py` | Phase 1A: fetches neg-risk markets, computes sum_best_ask, detects dislocations |
| `observers/crypto.py` | Phase 1B: fetches crypto threshold markets, computes BS model price, tracks gap |
| `smoke_test.py` | Validates API connectivity, DB init, question parser |
| `reports/negrisk_daily.py` | Dislocation frequency report for neg-risk |
| `reports/crypto_daily.py` | Gap distribution + directional accuracy report for crypto |

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
python3 -m v2.reports.negrisk_daily          # last 24h
python3 -m v2.reports.negrisk_daily --hours 6

python3 -m v2.reports.crypto_daily
python3 -m v2.reports.crypto_daily --hours 6
```

---

## Poll intervals

| Observer | Interval | Reason |
|---|---|---|
| neg-risk | 5 min | CLOB calls per leg are slow; 35k markets need ~10-20 min per cycle |
| crypto | 30 min | CoinGecko rate limits; BS model stable over 30 min |

---

## Data files

Stored in `$POLYMARKET_DATA_DIR/v2/` (defaults to repo root):

| File | Contents |
|---|---|
| `obs_negrisk.db` | nr_groups, nr_snapshots, nr_legs, nr_dislocations |
| `obs_crypto.db` | cr_markets, cr_snapshots, cr_resolved |
| `logs/negrisk_observer.log` | Rotating log, 5 MB × 3 |
| `logs/crypto_observer.log` | Rotating log, 5 MB × 3 |
