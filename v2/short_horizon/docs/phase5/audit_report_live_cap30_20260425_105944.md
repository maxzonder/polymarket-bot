# Phase 5 audit report: `live_cap30_20260425_105944`

Date: 2026-04-25
Issue: #127
Run id: `live_cap30_20260425_105944`
Config hash: `p5-cap30-after-pr142`

## Source artifacts

Production paths:

- DB: `/home/polybot/.polybot/short_horizon/probes/micro_live_cap30_20260425_105944.sqlite3`
- log: `/home/polybot/.polybot/short_horizon/logs/micro_live_cap30_20260425_105944.log`
- capture bundle: `/home/polybot/.polybot/capture_bundles/micro_live_cap30_20260425_105944`
- replay output: `/home/polybot/.polybot/replays/p5_audit_live_cap30_20260425_105944_20260425_184104/replay.sqlite3`
- replay comparison report: `/home/polybot/.polybot/replays/p5_audit_live_cap30_20260425_105944_20260425_184104/comparison_report.txt`

## Run summary

- Started: `2026-04-25T10:59:46.370Z`
- Finished: `2026-04-25T16:59:46.808Z`
- Duration: approximately 6 hours
- Completion mode: configured max-runtime completion
- Result: clean autonomous completion
- Final lifecycle events present: `live_run_completed`, `replay_capture_bundle_written`

Capture bundle counts:

- `events_log_count`: `2,889,396`
- `market_state_snapshots_count`: `2,482`
- `orders_final_count`: `23`
- `fills_final_count`: `13`
- `venue_responses_count`: `23`
- DB markets observed: `50`

## P5-4 fidelity audit

Command run on production from `/home/polybot/claude-polymarket`:

```bash
./.venv/bin/python -m v2.short_horizon.short_horizon.replay_runner \
  /home/polybot/.polybot/capture_bundles/micro_live_cap30_20260425_105944 \
  /home/polybot/.polybot/replays/p5_audit_live_cap30_20260425_105944_20260425_184104/replay.sqlite3 \
  --compare \
  --comparison-report-path /home/polybot/.polybot/replays/p5_audit_live_cap30_20260425_105944_20260425_184104/comparison_report.txt
```

Result:

```text
Replay comparison: live_run_id=live_cap30_20260425_105944 replay_run_id=live_cap30_20260425_105944
Result: MATCH
Order intents: ok (matched=23 mismatched=0 live_only=0 replay_only=0)
Skip decisions: ok (matched=151 mismatched=0 live_only=0 replay_only=0)
Cancel intents: ok (matched=0 mismatched=0 live_only=0 replay_only=0)
Terminal outcomes: ok (matched=23 mismatched=0 live_only=0 replay_only=0)
```

Acceptance: **passed**. `compare_fidelity` exited `0` with zero mismatches.

During the first audit attempt, replay failed on captured `MarketResolvedWithInventory` telemetry-derived events being treated as replay inputs. This was fixed in commit `3fcefa5` by filtering resolved-inventory events out of replay input events. The fix is covered by `tests.test_replay_fidelity`.

## P5-5 financial and performance summary

Execution summary:

- Order intents: `23`
- Accepted orders: `13`
- Final filled orders: `13`
- Final rejected orders: `10`
- Fill rate by intent: `13 / 23 = 56.5%`
- Filled notional / cost basis: `12.8921 USDC`
- Filled shares: `21.39`
- Fees recorded in fills table: none (`fee_paid_usdc` was not populated)

Resolved-inventory economics:

- Resolved inventory events: `13`
- Estimated payout: `13.50213 USDC`
- Estimated cost basis: `12.89210 USDC`
- Estimated net PnL: `+0.61003 USDC` before any unmodeled fees/rounding

Reject analysis:

- All `10` rejected orders were venue-side minimum-size rejects.
- The repeated venue error was `Size (...) lower than the minimum: 5`.
- Typical attempted sizes were approximately `1.44`, `1.55`, `1.80`, and `1.83` shares.
- This did not break the run or fidelity, but it reduced realized exposure and should remain a follow-up execution-sizing item before scaling.

Operational observations:

- Websocket disconnect/reconnect noise did not kill the runner after the subscription-send hardening.
- Periodic redeem ran during the probe; logs show multiple `live_redeem_status status=redeemed` events and final `live_redeem_completed error_count=0`.
- The run stayed within the tightened cap30 scope and produced a deterministic replayable bundle.

## Gate conclusion

Phase 5 is green for the intended mechanical acceptance gate:

- P5-3: autonomous mainnet micro-live run completed and produced a full capture bundle.
- P5-4: replay fidelity passed with zero mismatches.
- P5-5: financial/performance summary is documented here.

Recommended follow-up outside Phase 5: fix/decide the venue minimum-size sizing semantics before increasing size or duration materially.
