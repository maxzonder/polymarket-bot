# Consensus review: harden Big Swan bot skeleton before ML

## Why this issue exists

The current `big_swan_mode` branch is a strong first-pass architecture and implementation skeleton.
It already has the right high-level shape:

- separate `EntryFillScore` and `ResolutionScore`
- dry-run vs live split
- resting bids for big swans
- partial TP + moonbag
- dedicated `positions.db`
- a first attempt at `feature_mart`

This issue is **not** a teardown.
It is a peer-review hardening pass intended to bring the current implementation to a stable consensus baseline **before** adding ML training.

The goal is to preserve the strong ideas, fix the dangerous assumptions, and converge on one robust architecture instead of creating agent-vs-agent churn.

---

## What we should keep

These ideas look directionally correct and should remain the foundation:

- 3 trading modes: `fast_tp_mode`, `balanced_mode`, `big_swan_mode`
- separate entry/fill logic from resolution/tail logic
- use resting bids for `big_swan_mode`
- keep moonbag / hold-to-resolution logic for true tail capture
- keep dry-run and live trading separated
- keep `positions.db` / operational state in SQLite
- keep `build_feature_mart.py` as the bridge toward future ML

---

## Consensus concerns that should be fixed before ML

### 1. Training dataset leakage / selection bias

Current `feature_mart` is built only from `swans_v2`.
That means the training table is built only from already-selected swan events, not from the full candidate universe.

Implications:
- labels such as `tp_5x_hit` become degenerate or near-degenerate
- the model never sees true negatives / failed candidate entries
- the resulting scorer will be structurally over-optimistic

What we want instead:
- `feature_mart` must include **both positives and negatives**
- the unit of training should be closer to: “candidate token-market at a hypothetical/actual entry condition”
- `swans_v2` can stay as a **positive-event layer**, but not as the sole source for model training

Acceptance criteria:
- `feature_mart` is no longer sourced exclusively from `swans_v2`
- there are meaningful negative examples
- label distributions are printed and sane
- `tp_5x_hit` is not a constant
- `tail_bucket=0` exists in data

---

### 2. Token-side pricing logic must be made correct

Current screener logic uses market-level price approximations and derives the NO side as roughly `1 - YES`.
This is too rough for real token-level decision-making.

Implications:
- wrong candidate selection
- broken pricing on thin or special markets
- bad fills in both paper and live logic

What we want instead:
- use real per-token pricing / orderbook data for the token being evaluated
- treat YES and NO as distinct tradeable objects
- scanner and resting-bid logic should operate on token-level truth, not market-level proxy pricing

Acceptance criteria:
- no synthetic `NO = 1 - YES` shortcut in screener decision path
- candidate generation is token-accurate
- multi-outcome / special cases do not silently reuse binary assumptions

---

### 3. Scanner entry semantics should match mode definitions

Current scanner-entry behavior appears narrower than the documented mode behavior.
For example, immediate entry seems tied to `current_price <= min(entry_price_levels)` instead of the broader “already in valid scanner zone” logic.

Implications:
- `fast_tp_mode` and `balanced_mode` may not behave as designed
- scanner-based entries may be under-triggered or mis-triggered

What we want instead:
- make the code behavior exactly match the documented mode semantics
- define scanner-entry conditions explicitly and separately from resting-bid placement

Acceptance criteria:
- scanner entry logic is explicit per mode
- `fast_tp_mode` behaves as documented
- `balanced_mode` behavior is deterministic and understandable

---

### 4. Realized PnL accounting must be made correct

Current PnL accounting appears incomplete.
TP fills and resolution outcomes do not appear to flow into one coherent realized-PnL model.

Implications:
- dry-run performance cannot be trusted
- future profit feedback loop will be trained on bad accounting
- model / scorer iteration will drift from reality

What we want instead:
- one coherent realized PnL model for:
  - partial TP fills
  - remaining moonbag resolution
  - losing positions
  - cancelled / stale orders

Acceptance criteria:
- TP profits are persisted, not just logged
- position-level PnL is correctly accumulated over the whole lifecycle
- dry-run stats reflect full realized PnL
- winner and loser resolution both update accounting consistently

---

### 5. Depth / liquidity gating should be real, not just documented

The code and docs discuss orderbook-depth protection, but it does not appear fully enforced at placement time.

Implications:
- bot may place bids in garbage or non-tradable conditions
- paper results will look better than real tradability

What we want instead:
- explicit depth / liquidity gate before order placement
- at least a minimal executable tradability check for the target token and price level

Acceptance criteria:
- order placement path checks token-level book/depth
- placement is rejected if tradability is obviously fake/thin
- the gate is documented and testable

---

### 6. Dry-run fill model should be made less optimistic

Current dry-run fill logic is a reasonable placeholder, but still too optimistic for reliable evaluation.

Implications:
- paper fill rate may be overstated
- strategy may look better than it is

What we want instead:
- still keep the simulator simple
- but incorporate at least some notion of:
  - available size
n  - partial fills
  - non-guaranteed queue priority

Acceptance criteria:
- fill simulation explicitly documents its assumptions
- size-sensitive fills exist
- paper mode no longer assumes an unrealistically frictionless market

---

## Recommended implementation order

1. Fix token-side pricing and scanner semantics
2. Fix PnL accounting
3. Add real tradability / depth gating
4. Rework `feature_mart` to include negatives
5. Only after that: add ML scoring

---

## Explicit non-goal for this issue

Do **not** add CatBoost/LightGBM/model-training yet.
The current task is to harden the baseline so future ML is trained on sane data and execution semantics.

---

## Desired output from the assignee

Please respond with:

1. a short agreement/disagreement matrix for each concern above
2. the implementation plan you propose
3. the actual code changes
4. a note on what assumptions remain intentionally simplified after this pass

The intent is collaboration and convergence, not argumentative churn.
