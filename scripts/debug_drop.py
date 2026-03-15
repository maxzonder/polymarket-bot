from data_collector.analyzer import load_trades, find_zigzag_swans, compute_real_metrics

DATE = "2026-02-28"
MARKET_ID = "1323352"
TOKEN_ID = "45900834520961763856469490579496577263935972652383164286919801069052546628321"
MIN_ENTRY = 10.0
MIN_RECOVERY = 5.0
TARGET_X = 5.0
MIN_DURATION = 10.0
REQ_EXIT = MIN_ENTRY * TARGET_X

trades = load_trades(DATE, MARKET_ID, TOKEN_ID)
print("trades", len(trades))
raw = find_zigzag_swans(trades, MIN_RECOVERY)
print("raw_swans", len(raw))
for i, sw in enumerate(raw, 1):
    print("\nRAW", i, {
        "entry_min_price": sw.entry_min_price,
        "exit_max_price": sw.exit_max_price,
        "possible_x": round(sw.possible_x, 2),
    })
    metrics = compute_real_metrics(trades, sw, MIN_RECOVERY, TARGET_X)
    if metrics is None:
        print("REJECT: compute_real_metrics returned None (no target hit or empty zones)")
        continue
    full_swan_minutes = (metrics["exit_ts_last"] - metrics["entry_ts_first"]) / 60.0 if metrics["entry_ts_first"] and metrics["exit_ts_last"] else 0.0
    reasons = []
    if metrics["entry_volume_usdc"] < MIN_ENTRY:
        reasons.append(f"entry_volume_usdc {metrics['entry_volume_usdc']:.2f} < {MIN_ENTRY}")
    if metrics["exit_volume_usdc"] < REQ_EXIT:
        reasons.append(f"exit_volume_usdc {metrics['exit_volume_usdc']:.2f} < {REQ_EXIT}")
    if full_swan_minutes < MIN_DURATION:
        reasons.append(f"full_swan_minutes {full_swan_minutes:.2f} < {MIN_DURATION}")
    print("metrics", {
        "entry_volume_usdc": round(metrics["entry_volume_usdc"], 2),
        "exit_volume_usdc": round(metrics["exit_volume_usdc"], 2),
        "target_exit_price": round(metrics["target_exit_price"], 6),
        "exit_max_price": round(metrics["exit_max_price"], 6),
        "full_swan_minutes": round(full_swan_minutes, 2),
        "duration_entry_zone_sec": metrics["duration_entry_zone_seconds"],
        "duration_exit_zone_sec": metrics["duration_exit_zone_seconds"],
        "duration_to_target_min": round(metrics["duration_entry_to_target_minutes"], 2),
    })
    if reasons:
        print("REJECT reasons", reasons)
    else:
        print("PASS")
