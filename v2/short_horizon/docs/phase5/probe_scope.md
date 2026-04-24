# Phase 5: Micro-Live Autonomous Probe Scope

Date: 2026-04-24
Issue: #127

This document locks down the scope, duration, and risk limits for the Phase 5 micro-live autonomous probe.

## 1. Goal
Prove that the short-horizon bot can run autonomously on mainnet, manage its lifecycle, strictly obey risk limits, and produce a perfectly replayable capture bundle. The primary metric of success is **mechanical fidelity**, not profitability.

## 2. Risk Limits & Sizing
The probe runs with aggressively scaled-down risk parameters:
- **`micro_live_cumulative_stake_cap_usdc`**: \$20.0
- **`micro_live_concurrent_open_notional_cap_usdc`**: \$20.0
- **`max_trade_notional_usdc`**: \$1.5 (derived as `1.5 * target_trade_size_usdc` unless overridden)
- **`max_daily_loss_usdc`**: \$10.0
- **`target_trade_size_usdc`**: \$1.0

*Note: These limits must be enforced by the configuration passed to the runtime, and the cumulative stake cap is the source of truth for the "no more than $20 staked in total" promise.*

## 3. Duration
The autonomous run window is capped at **8 hours**. 
The run is considered complete when either 8 hours have elapsed, or a hard risk limit (stake cap or daily loss) halts execution.

## 4. Success Criteria
1. The bot survives the 8-hour window without crashing due to unhandled exceptions.
2. The bot does not exceed the predefined risk caps (no more than \$20 cumulatively staked in total, no single trade over \$1.5 effective submitted notional, max loss hard-stopped at \$10).
3. The bot successfully writes a Phase 4 capture bundle (`--capture-dir`) upon shutdown.
4. Offline replay of the capture bundle produces zero mismatches in the fidelity comparator.

## 5. Emergency Abort
If the bot exhibits rapid fire loops (e.g. failing to recognize open orders and spamming places), or if telemetry alerts indicate a deviation from expected behavior, the operator will manually abort via `tmux` and the kill switch. A manual abort still requires the best-effort capture bundle to be retrieved and audited.
