import sqlite3, sys
sys.path.insert(0, '/home/polybot/polymarket-bot')
from data_collector.analyzer_v3 import load_trades, find_zigzag_swans, compute_real_metrics

DATE = '2026-02-28'
MARKET_ID = '1455898'
MIN_ENTRY = 10.0
MIN_RECOVERY = 5.0
TARGET_X = 5.0
MIN_DURATION = 10.0
REQ_EXIT = MIN_ENTRY * TARGET_X

conn = sqlite3.connect('/home/polybot/polymarket-bot/polymarket_dataset.db')

print('MARKET')
for row in conn.execute('SELECT id, question, volume FROM markets WHERE id=?', (MARKET_ID,)):
    print('\t'.join(map(str, row)))

print('\nV2_ROWS')
for row in conn.execute(
    '''
    SELECT t.outcome_name, s.token_id, s.entry_min_price, s.entry_volume_usdc,
           s.exit_max_price, s.exit_volume_usdc, s.possible_x,
           s.entry_ts_first, s.exit_ts_last
    FROM token_swans s
    LEFT JOIN tokens t ON t.token_id = s.token_id
    WHERE s.date=? AND s.market_id=?
    ORDER BY s.possible_x DESC, s.exit_volume_usdc DESC
    ''',
    (DATE, MARKET_ID),
):
    print('\t'.join(map(str, row)))

print('\nV3_DEBUG')
for token_id, outcome in conn.execute('SELECT token_id, outcome_name FROM tokens WHERE market_id=?', (MARKET_ID,)):
    print(f'\nTOKEN\t{outcome}\t{token_id}')
    trades = load_trades(DATE, MARKET_ID, token_id)
    print(f'TRADES\t{len(trades)}')
    raw = find_zigzag_swans(trades, MIN_RECOVERY)
    print(f'RAW_SWANS\t{len(raw)}')
    for i, sw in enumerate(raw, 1):
        metrics = compute_real_metrics(trades, sw, MIN_RECOVERY, TARGET_X)
        base = [
            f'RAW\t{i}',
            f'entry={sw.entry_min_price:.6f}',
            f'peak={sw.exit_max_price:.6f}',
            f'x={sw.possible_x:.2f}',
        ]
        print('\t'.join(base))
        if metrics is None:
            print('STATUS\tREJECT\tmetrics=None')
            continue
        full_swan_minutes = (metrics['exit_ts_last'] - metrics['entry_ts_first']) / 60.0 if metrics['entry_ts_first'] and metrics['exit_ts_last'] else 0.0
        reasons = []
        if metrics['entry_volume_usdc'] < MIN_ENTRY:
            reasons.append(f'entry {metrics["entry_volume_usdc"]:.2f} < {MIN_ENTRY}')
        if metrics['exit_volume_usdc'] < REQ_EXIT:
            reasons.append(f'exit {metrics["exit_volume_usdc"]:.2f} < {REQ_EXIT}')
        if full_swan_minutes < MIN_DURATION:
            reasons.append(f'full_minutes {full_swan_minutes:.2f} < {MIN_DURATION}')
        print('METRICS\t' + '\t'.join([
            f'entry_vol={metrics["entry_volume_usdc"]:.2f}',
            f'exit_vol={metrics["exit_volume_usdc"]:.2f}',
            f'target_exit={metrics["target_exit_price"]:.6f}',
            f'exit_max={metrics["exit_max_price"]:.6f}',
            f'full_minutes={full_swan_minutes:.2f}',
            f'to_target_min={metrics["duration_entry_to_target_minutes"]:.2f}',
            f'entry_trades={metrics["entry_trade_count"]}',
            f'exit_trades={metrics["exit_trade_count"]}',
        ]))
        if reasons:
            print('STATUS\tREJECT\t' + '; '.join(reasons))
        else:
            print('STATUS\tPASS')
