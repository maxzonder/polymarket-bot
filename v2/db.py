"""
Initialize observation databases for v2.

obs_negrisk.db  — neg-risk group snapshots
obs_crypto.db   — crypto threshold gap snapshots
"""
from __future__ import annotations

import sqlite3
from .utils.paths import NEGRISK_DB, CRYPTO_DB, ensure_dirs


# ── Neg-Risk schema ────────────────────────────────────────────────────────────

_NEGRISK_DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS nr_groups (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_slug      TEXT NOT NULL,
    event_title     TEXT,
    n_markets       INTEGER,
    first_seen_ts   INTEGER,
    UNIQUE(event_slug)
);

-- One row per 5-min polling cycle per group
CREATE TABLE IF NOT EXISTS nr_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id        INTEGER NOT NULL REFERENCES nr_groups(id),
    ts              INTEGER NOT NULL,
    n_legs          INTEGER,          -- tokens in group at this snapshot
    sum_best_ask    REAL,             -- sum of best_ask across all legs (naive tradeable cost)
    sum_mid         REAL,             -- sum of mid prices (informational)
    total_group_vol REAL,             -- sum of individual market volumes
    is_dislocated   INTEGER DEFAULT 0 -- 1 if sum_best_ask < 0.97
);

-- One row per leg per snapshot — preserves executable detail for Phase 1.5
CREATE TABLE IF NOT EXISTS nr_legs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id     INTEGER NOT NULL REFERENCES nr_snapshots(id),
    market_id       TEXT NOT NULL,
    token_id        TEXT NOT NULL,
    best_bid        REAL,
    best_ask        REAL,
    best_bid_size   REAL,
    best_ask_size   REAL,
    market_volume   REAL
);

-- Dislocation episodes: start/end of continuous sum_best_ask < threshold
CREATE TABLE IF NOT EXISTS nr_dislocations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id        INTEGER NOT NULL REFERENCES nr_groups(id),
    start_ts        INTEGER NOT NULL,
    end_ts          INTEGER,          -- null = still open
    min_sum_ask     REAL,             -- deepest dislocation seen
    max_gap         REAL,             -- 1 - min_sum_ask
    group_volume    REAL,
    n_snapshots     INTEGER DEFAULT 1
);
"""

# ── Crypto schema ─────────────────────────────────────────────────────────────

_CRYPTO_DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS cr_markets (
    market_id       TEXT PRIMARY KEY,
    token_id        TEXT NOT NULL,
    question        TEXT NOT NULL,
    asset           TEXT,             -- BTC / ETH / SOL
    threshold       REAL,
    direction       TEXT,             -- above / below
    expiry_ts       INTEGER,
    parse_template  TEXT,             -- rise_to / dip_to / up_or_down / other
    parse_ok        INTEGER DEFAULT 0,
    first_seen_ts   INTEGER,
    resolved_ts     INTEGER,
    outcome         INTEGER           -- 1=YES won, 0=NO won, null=unresolved
);

-- One row per 30-min polling cycle per market
CREATE TABLE IF NOT EXISTS cr_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id       TEXT NOT NULL REFERENCES cr_markets(market_id),
    ts              INTEGER NOT NULL,
    polymarket_price REAL,
    model_price     REAL,
    gap             REAL,             -- model_price - polymarket_price
    tte_hours       REAL,
    spot_price      REAL,
    vol_estimate    REAL,             -- annualised realised vol used
    in_garbage_time INTEGER DEFAULT 0 -- 1 if tte < 2% of total duration
);

-- Resolved market outcomes joined back to last snapshot before resolution
CREATE TABLE IF NOT EXISTS cr_resolved (
    market_id       TEXT PRIMARY KEY REFERENCES cr_markets(market_id),
    resolved_ts     INTEGER,
    outcome         INTEGER,          -- 1=YES, 0=NO
    last_gap        REAL,             -- gap at last snapshot before resolution
    last_polymarket_price REAL,
    last_model_price REAL,
    was_directionally_correct INTEGER  -- 1 if gap sign predicted outcome
);
"""


def init_negrisk() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(str(NEGRISK_DB))
    conn.row_factory = sqlite3.Row
    conn.executescript(_NEGRISK_DDL)
    conn.commit()
    return conn


def init_crypto() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(str(CRYPTO_DB))
    conn.row_factory = sqlite3.Row
    conn.executescript(_CRYPTO_DDL)
    conn.commit()
    return conn


def conn_negrisk() -> sqlite3.Connection:
    c = sqlite3.connect(str(NEGRISK_DB))
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c


def conn_crypto() -> sqlite3.Connection:
    c = sqlite3.connect(str(CRYPTO_DB))
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c
