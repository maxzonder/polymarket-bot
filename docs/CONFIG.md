# config.py — Reference

## Overview

`config.py` defines all trading strategy parameters. There are two config classes:

- **`ModeConfig`** — immutable strategy config per trading mode (entry, exit, sizing, gates)
- **`BotConfig`** — runtime config loaded from `.env` / environment variables

Active mode is selected via `BOT_MODE` env var (default: `big_swan_mode`).

---

## Trading Modes

| Mode | Strategy | Entry | Moonbag |
|---|---|---|---|
| `big_swan_mode` | Pre-position resting bids at deep floors | resting only | 60% |
| `dip_mode` | Pre-position resting bids at moderate dips | resting only | 40% |
| `balanced_mode` | Resting bids + scanner entry | both | 20% |
| `fast_tp_mode` | Scanner-triggered entry, quick TP | scanner only | 0% |

---

## ModeConfig Fields

### Entry

| Field | Type | Description |
|---|---|---|
| `entry_price_levels` | `tuple[float, ...]` | Price levels where resting limit bids are placed (ascending). Screener only places levels **below** current market price. |
| `entry_price_max` | `float` | Max current price for a market to be screened. Markets trading above this are skipped entirely. |
| `use_resting_bids` | `bool` | Whether to pre-position orders before market dips to the level. |
| `scanner_entry` | `bool` | Whether to enter when scanner sees price already in the entry zone. |

### Exit

| Field | Type | Description |
|---|---|---|
| `tp_levels` | `tuple[TPLevel, ...]` | Take-profit ladder. Each `TPLevel(x, fraction)` sells `fraction` of position when price hits `entry_price * x`. |
| `moonbag_fraction` | `float` | Fraction held through to market resolution. `tp_levels` fractions + `moonbag_fraction` must sum to ≤ 1.0. |

### Scoring Gates

| Field | Type | Description |
|---|---|---|
| `min_entry_fill_score` | `float` | Min historical P(market ever hits entry zone). Filters markets unlikely to ever fill. |
| `min_resolution_score` | `float` | Min resolution score (P(winner) × avg_x signal) to accept candidate. |
| `min_real_x_historical` | `float` | Min observed price multiplier in historical data (excludes noise bounces). |
| `min_market_score` | `float` | v1.1 gate: min composite `market_score` to even consider a market. `0.0` = disabled. |

### Position Sizing

| Field | Type | Description |
|---|---|---|
| `stake_usdc` | `float` | Fallback flat stake (USDC tokens bought at fill). Used when no tier matches. |
| `max_open_positions` | `int` | Hard cap on simultaneous open positions. |
| `max_resting_markets` | `int` | Max distinct markets with live resting bids. |
| `max_resting_per_cluster` | `int` | Max markets per cluster (`market_id // 1000`), prevents cluster concentration. |
| `max_capital_deployed_pct` | `float` | Max fraction of balance in open positions (0.0–1.0). |
| `max_exposure_per_market` | `float` | v1.1: max total USDC across all fills on same `(market_id, token_id)`. `0.0` = disabled. |

### Stake Tiers

Two mutually exclusive sizing schedules — `market_score_tiers` takes priority over `stake_tiers`:

#### `stake_tiers` (price-based, v1 legacy)

```python
stake_tiers: tuple[tuple[float, float], ...] = ()
# format: ((max_entry_price, stake_usdc), ...)
# sorted ascending by price
```

Logic: for a given fill price, find the **first** tier where `fill_price <= max_entry_price` → use that stake. Falls back to `stake_usdc` if no tier matches.

Design intent: deeper floor = higher stake (higher upside potential).

Example (`big_swan_mode`):
```
fill @ 0.001 → $0.50   (1000x upside)
fill @ 0.005 → $0.25
fill @ 0.010 → $0.10
```

#### `market_score_tiers` (score-based, v1.1)

```python
market_score_tiers: tuple[tuple[float, float], ...] = ()
# format: ((min_score_threshold, stake_usdc), ...)
# sorted descending by threshold at runtime
```

Logic: for a given market's composite `market_score`, find the **highest** threshold ≤ score → use that stake. When configured, **completely replaces** `stake_tiers` (not a multiplier).

Example (`big_swan_mode`):
```
score ≥ 0.60 → $0.50   (top ~10%)
score ≥ 0.40 → $0.25   (top ~25%)
score ≥ 0.25 → $0.10   (pass gate)
```

> **⚠️ Known issue — see below**

### Time Window

| Field | Type | Description |
|---|---|---|
| `min_hours_to_close` | `float` | Reject markets closing sooner than this. |
| `max_hours_to_close` | `float` | Reject markets closing later than this. |
| `hours_to_close_null_default` | `float` | If Gamma returns `None` for deadline, treat as this many hours. Prevents unknown-deadline markets from bypassing the filter. Default: 48h. |

### Optimisation Target

| Value | Description |
|---|---|
| `"tail_ev"` | `swan_rate × win_rate × avg_x` — optimise for rare high-multiple outcomes |
| `"ev_total"` | Expected value across all outcomes |
| `"roi_pct"` | Return on investment percent |

---

## BotConfig Fields (Runtime)

Loaded from environment / `.env`. Set via `BOT_MODE=big_swan_mode DRY_RUN=false python main.py`.

| Field | Env var | Default | Description |
|---|---|---|---|
| `mode` | `BOT_MODE` | `big_swan_mode` | Active trading mode |
| `dry_run` | `DRY_RUN` | `true` | Paper trading — no real orders sent |
| `private_key` | `POLY_PRIVATE_KEY` | — | CLOB wallet private key |
| `api_key` | `POLY_API_KEY` | — | Polymarket API key |
| `api_secret` | `POLY_API_SECRET` | — | Polymarket API secret |
| `api_passphrase` | `POLY_PASSPHRASE` | — | Polymarket API passphrase |
| `screener_interval` | — | 300s | How often screener runs |
| `monitor_interval` | — | 90s | How often open positions are checked |
| `resting_cleanup_interval` | — | 3600s | How often stale resting bids are cleaned |
| `min_volume_usdc` | — | 50 | Min market total volume to screen |
| `max_volume_usdc` | — | 300,000 | Max market total volume to screen |
| `dead_market_hours` | — | 48h | Reject markets with no trades in this window |
| `scorer_entry_price_max` | — | 0.02 | Only use `swans_v2` rows ≤ this price for scoring |
| `scorer_min_samples` | — | 5 | Min sample count for a reliable resolution score |

### `category_weights`

Per-category EV multiplier applied to market scores. Derived from `feature_mart_v1_1` data (Dec–Feb 2026), normalized to crypto = 1.0:

| Category | Weight | Rationale |
|---|---|---|
| geopolitics | 1.5 | Highest tail_ev (14.98% swan rate) |
| politics | 1.5 | Strong tail_ev, capped at 1.5 |
| crypto | 1.0 | Base |
| weather | 1.0 | Similar tail_ev to crypto |
| health | 1.0 | Insufficient data, neutral |
| esports | 1.0 | No Dec–Feb data, neutral |
| sports | 0.8 | Below-base tail_ev |
| tech | 0.6 | Low avg_x (23.9) |
| entertainment | 0.5 | Lowest tail_ev (1.77% swan × 4.3% win) |

---

## Global Constants

| Constant | Value | Description |
|---|---|---|
| `SWAN_BUY_PRICE_THRESHOLD` | auto | `max(entry_price_levels)` across all modes — swan_analyzer collection ceiling |
| `SWAN_ENTRY_MAX` | = above | Alias used by feature_mart and rejected_outcomes |
| `SWAN_MIN_BUY_VOLUME` | 1.0 USDC | Min volume at floor (liquidity check) |
| `SWAN_MIN_SELL_VOLUME` | 30.0 USDC | Min volume at exit (fill quality check) |
| `SWAN_MIN_REAL_X` | 5.0× | Min multiplier to count as a real swan event |

`SWAN_BUY_PRICE_THRESHOLD` is computed dynamically so adding a new mode with higher entry levels automatically raises the collection ceiling.

---

## ⚠️ Known Discrepancy: `stake_tiers` vs `market_score_tiers`

### Problem

In `BIG_SWAN_MODE`, both `stake_tiers` and `market_score_tiers` are defined. The comment in `config.py` reads:

> *"Applied as a multiplier on top of stake_tiers"*

But `risk_manager.py` uses `elif`:

```python
if self.mc.market_score_tiers and market_score is not None:
    # market_score_tiers wins → stake_tiers NEVER evaluated
    ...
elif self.mc.stake_tiers:
    ...
```

**`market_score_tiers` completely replaces `stake_tiers`, it does not multiply them.**

This has two consequences:

1. **`stake_tiers` in `BIG_SWAN_MODE` is dead code** — those values are never used.

2. **Price depth is no longer reflected in sizing.** With `stake_tiers`, a fill at 0.001 gets 5× more stake than a fill at 0.010. With `market_score_tiers`, all `entry_price_levels` of a given market get the **same** stake per fill — only the market's score matters, not how deep the floor is.

### Current behaviour (what the code actually does)

For `BIG_SWAN_MODE` with a market scored 0.62:
- Resting bid placed at 0.001 → stake **$0.50**
- Resting bid placed at 0.005 → stake **$0.50**  ← same
- Resting bid placed at 0.010 → stake **$0.50**  ← same
- Total potential exposure: $1.50 (capped by `max_exposure_per_market=2.0`)

### Decision needed

Option A — **Keep current behaviour, fix the comment.** Score-based sizing is the v1.1 intent; price tiers are v1 legacy. Remove `stake_tiers` from `BIG_SWAN_MODE` entirely to avoid confusion.

Option B — **Combine both.** Score selects a multiplier (e.g. 0.5×/1.0×/1.5×), price selects a base stake. Requires changing `risk_manager.py` to multiply instead of `elif`.

Option C — **Restore price-depth awareness inside score tiers.** Define `market_score_tiers` as `(min_score, price→stake_map)` so that both score and fill price influence sizing. More complex.
