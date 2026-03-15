from data_collector.analyzer import load_trades, find_zigzag_swans, compute_real_metrics

DATE = "2026-02-28"
MARKET_ID = "1379913"
TOKEN_ID = "55258677824280738308176623321517284225337019988734833764442029999971213908008"
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
        "possible_x": sw.possible_x,
        "entry_min_idx": sw.entry_min_idx,
        "exit_max_idx": sw.exit_max_idx,
    })
    metrics = compute_real_metrics(trades, sw, MIN_RECOVERY, TARGET_X)
    print("metrics", metrics)
    if metrics is None:
        print("REJECT reason = no target hit or no exit trades or no entry trades")
        continue
    reasons = []
    if metrics["entry_volume_usdc"] < MIN_ENTRY:
        reasons.append(f"entry_volume_usdc {metrics['entry_volume_usdc']:.2f} < {MIN_ENTRY}")
    if metrics["exit_volume_usdc"] < REQ_EXIT:
        reasons.append(f"exit_volume_usdc {metrics['exit_volume_usdc']:.2f} < {REQ_EXIT}")
    if metrics["duration_entry_to_target_minutes"] < MIN_DURATION:
        reasons.append(f"duration {metrics['duration_entry_to_target_minutes']:.2f} < {MIN_DURATION}")
    if reasons:
        print("REJECT reasons", reasons)
    else:
        print("PASS")
