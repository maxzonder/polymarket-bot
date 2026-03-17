# Claude follow-up prompt — peer review hardening pass

Please treat this as a **peer-review convergence task**, not a competitive rewrite.

The current bot architecture is directionally strong and should be preserved where possible.
The goal is to harden the implementation before adding ML, not to throw away the design.

Read and address this review issue first:

- `docs/ISSUE_PEER_REVIEW_BIG_SWAN_BOT.md`

## Working mode

Please approach this as:
- keep the strong architecture
- identify where current implementation assumptions are too optimistic or structurally biased
- fix those areas with the smallest clean set of changes
- avoid agent-vs-agent argumentation
- prefer convergence and explicit tradeoffs

## Core intent

We want one shared robust baseline for the Polymarket bot.
Not an endless loop of architectural one-upmanship.

## What to do now

Please implement the review in this order:

1. Fix token-side pricing and scanner-entry semantics
2. Fix realized PnL accounting across partial TP + moonbag resolution
3. Add actual tradability / depth gating before placing bids
4. Rework `feature_mart` so it is not built only from already-selected `swans_v2` positives
5. Do **not** add CatBoost/LightGBM/ML training yet unless the data foundation is fixed first

## Required response format

Reply with:

1. `Agreement matrix`
   - for each issue: agree / partially agree / disagree
   - with one short reason

2. `Implementation plan`
   - what you will change
   - what you intentionally will not change in this pass

3. `Code changes made`
   - concise summary by file

4. `Remaining simplifications`
   - what is still intentionally approximate after this pass

## Important constraints

- Preserve the 3-mode structure
- Preserve separate `EntryFillScore` and `ResolutionScore`
- Preserve resting bids + moonbag logic as first-class concepts
- Prefer surgical fixes over large rewrites unless rewrite is clearly necessary
- If you disagree with any review point, explain concretely from data/API/code semantics

## Success condition

At the end of this pass we should have:
- a more truthful dry-run baseline
- better token-level correctness
- cleaner PnL accounting
- a training pipeline that is not structurally biased from using only preselected positives
- a stable base that is worth building ML on top of later
