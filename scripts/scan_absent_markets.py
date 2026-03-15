import sqlite3, sys
sys.path.insert(0, '/home/polybot/polymarket-bot')
from data_collector.analyzer_v3 import load_trades, find_zigzag_swans, compute_real_metrics

DATE = '2026-02-28'
MIN_ENTRY = 10.0
MIN_RECOVERY = 5.0
TARGET_X = 5.0
MIN_DURATION = 10.0
REQ_EXIT = MIN_ENTRY * TARGET_X

conn = sqlite3.connect('/home/polybot/polymarket-bot/polymarket_dataset.db')
absent = conn.execute('''
WITH v2 AS (
  SELECT market_id, COUNT(*) AS v2_swans
  FROM token_swans
  WHERE date=?
  GROUP BY market_id
),
v3 AS (
  SELECT market_id
  FROM token_swans_v3
  WHERE date=?
  GROUP BY market_id
)
SELECT v2.market_id, v2.v2_swans
FROM v2
LEFT JOIN v3 USING (market_id)
WHERE v3.market_id IS NULL
ORDER BY v2.v2_swans DESC
LIMIT 20
''', (DATE, DATE)).fetchall()

for market_id, v2_swans in absent:
    question = conn.execute('SELECT question FROM markets WHERE id=?', (market_id,)).fetchone()[0]
    counts = {'entry': 0, 'exit': 0, 'duration': 0, 'pass': 0, 'none': 0}
    total_raw = 0
    for token_id, outcome in conn.execute('SELECT token_id, outcome_name FROM tokens WHERE market_id=?', (market_id,)):
        trades = load_trades(DATE, market_id, token_id)
        raw = find_zigzag_swans(trades, MIN_RECOVERY)
        total_raw += len(raw)
        for sw in raw:
            metrics = compute_real_metrics(trades, sw, MIN_RECOVERY, TARGET_X)
            if metrics is None:
                counts['none'] += 1
                continue
            full_swan_minutes = (metrics['exit_ts_last'] - metrics['entry_ts_first']) / 60.0 if metrics['entry_ts_first'] and metrics['exit_ts_last'] else 0.0
            bad = False
            if metrics['entry_volume_usdc'] < MIN_ENTRY:
                counts['entry'] += 1
                bad = True
            if metrics['exit_volume_usdc'] < REQ_EXIT:
                counts['exit'] += 1
                bad = True
            if full_swan_minutes < MIN_DURATION:
                counts['duration'] += 1
                bad = True
            if not bad:
                counts['pass'] += 1
    print(market_id, '|', question, '|', 'v2_swans=', v2_swans, '| raw=', total_raw, '|', counts)
