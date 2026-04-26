-- P0-H — SQLite storage schema for the short-horizon MVP

PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    mode TEXT NOT NULL CHECK (mode IN ('replay', 'live')),
    strategy_id TEXT NOT NULL,
    git_sha TEXT,
    config_hash TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS markets (
    market_id TEXT PRIMARY KEY,
    condition_id TEXT,
    token_yes_id TEXT,
    token_no_id TEXT,
    question TEXT,
    market_status TEXT,
    duration_seconds_snapshot INTEGER,
    start_time_latest TEXT,
    end_time_latest TEXT,
    fee_rate_bps_latest REAL,
    fee_fetched_at TEXT,
    fees_enabled INTEGER,
    market_source_revision TEXT,
    fee_info_json TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_markets_status
ON markets(market_status);

CREATE INDEX IF NOT EXISTS idx_markets_updated_at
ON markets(updated_at);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price REAL,
    size REAL,
    state TEXT NOT NULL CHECK (
        state IN (
            'intent',
            'pending_send',
            'accepted',
            'partially_filled',
            'filled',
            'cancel_requested',
            'cancel_confirmed',
            'cancel_rejected',
            'rejected',
            'expired',
            'unknown',
            'replace_requested',
            'replaced'
        )
    ),
    client_order_id TEXT,
    venue_order_id TEXT,
    parent_order_id TEXT,
    intent_created_at TEXT NOT NULL,
    last_state_change_at TEXT NOT NULL,
    venue_order_status TEXT,
    cumulative_filled_size REAL NOT NULL DEFAULT 0,
    remaining_size REAL,
    last_reject_code TEXT,
    last_reject_reason TEXT,
    reconciliation_required INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (run_id) REFERENCES runs(run_id),
    FOREIGN KEY (market_id) REFERENCES markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_state
ON orders(state);

CREATE INDEX IF NOT EXISTS idx_orders_market_id
ON orders(market_id);

CREATE INDEX IF NOT EXISTS idx_orders_run_id
ON orders(run_id);

CREATE INDEX IF NOT EXISTS idx_orders_client_order_id
ON orders(client_order_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_venue_order_id
ON orders(venue_order_id)
WHERE venue_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_orders_parent_order_id
ON orders(parent_order_id);

CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    price REAL NOT NULL,
    size REAL NOT NULL,
    fee_paid_usdc REAL,
    liquidity_role TEXT,
    filled_at TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('venue_ws', 'venue_rest', 'reconciled', 'replay')),
    venue_fill_id TEXT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id),
    FOREIGN KEY (market_id) REFERENCES markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_fills_order_id
ON fills(order_id);

CREATE INDEX IF NOT EXISTS idx_fills_run_id
ON fills(run_id);

CREATE INDEX IF NOT EXISTS idx_fills_market_id
ON fills(market_id);

CREATE TABLE IF NOT EXISTS events_log (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_time TEXT NOT NULL,
    ingest_time TEXT NOT NULL,
    source TEXT NOT NULL,
    market_id TEXT,
    token_id TEXT,
    order_id TEXT,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (run_id, seq),
    FOREIGN KEY (run_id) REFERENCES runs(run_id),
    FOREIGN KEY (market_id) REFERENCES markets(market_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE INDEX IF NOT EXISTS idx_events_log_run_seq
ON events_log(run_id, seq);

CREATE INDEX IF NOT EXISTS idx_events_log_market_id
ON events_log(market_id);

CREATE INDEX IF NOT EXISTS idx_events_log_order_id
ON events_log(order_id);

CREATE TABLE IF NOT EXISTS strategy_state (
    run_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT,
    state_key TEXT NOT NULL,
    state_value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, strategy_id, market_id, token_id, state_key),
    FOREIGN KEY (run_id) REFERENCES runs(run_id),
    FOREIGN KEY (market_id) REFERENCES markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_strategy_state_market_key
ON strategy_state(market_id, state_key);

-- Suggested state_key examples:
-- - first_touch_fired:0.55
-- - first_touch_fired:0.65
-- - first_touch_fired:0.70
-- - lifecycle_instance
-- - reconciliation_blocked
