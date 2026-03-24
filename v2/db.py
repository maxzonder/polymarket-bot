"""
Initialize observation databases for v2.

obs_negrisk.db  — neg-risk group snapshots
obs_crypto.db   — crypto threshold gap snapshots
obs_sports.db   — sports external-anchor + internal-inconsistency snapshots
"""
from __future__ import annotations

import sqlite3
from .utils.paths import NEGRISK_DB, CRYPTO_DB, SPORTS_DB, ensure_dirs


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
    last_trade_price REAL,            -- last traded price from Gamma (no extra API call)
    market_volume   REAL
);

-- Resolved neg-risk groups: which leg (market) won
CREATE TABLE IF NOT EXISTS nr_resolved (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id        INTEGER NOT NULL REFERENCES nr_groups(id),
    resolved_ts     INTEGER NOT NULL,
    winner_market_id TEXT,            -- market whose YES token resolved to 1
    n_legs          INTEGER,
    had_dislocation INTEGER DEFAULT 0 -- 1 if group ever had sum_best_ask < threshold
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
    start_ts        INTEGER,          -- real market open time from Gamma startDate
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
    # Migrations for existing DBs
    leg_cols = {row[1] for row in conn.execute("PRAGMA table_info(nr_legs)")}
    if "last_trade_price" not in leg_cols:
        conn.execute("ALTER TABLE nr_legs ADD COLUMN last_trade_price REAL")
    conn.commit()
    return conn


def init_crypto() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(str(CRYPTO_DB))
    conn.row_factory = sqlite3.Row
    conn.executescript(_CRYPTO_DDL)
    # Migration: add start_ts if column doesn't exist yet (existing DBs)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(cr_markets)")}
    if "start_ts" not in cols:
        conn.execute("ALTER TABLE cr_markets ADD COLUMN start_ts INTEGER")
    conn.commit()
    return conn


# ── Sports schema ──────────────────────────────────────────────────────────────

_SPORTS_DDL = """
PRAGMA journal_mode=WAL;

-- Raw evidence for every match attempt (audit trail)
CREATE TABLE IF NOT EXISTS sp_match_attempts (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                    REAL NOT NULL,
    event_title           TEXT NOT NULL,
    parsed_sport          TEXT,
    parsed_home           TEXT,
    parsed_away           TEXT,
    normalized_home       TEXT,
    normalized_away       TEXT,
    candidate_external_id TEXT,
    candidate_home        TEXT,
    candidate_away        TEXT,
    candidate_commence_ts REAL,
    date_window_score     REAL,
    name_match_score      REAL,
    final_score           REAL,
    accepted              INTEGER DEFAULT 0
);

-- Accepted canonical events (score >= threshold)
CREATE TABLE IF NOT EXISTS sp_events (
    event_id       TEXT PRIMARY KEY,
    event_title    TEXT NOT NULL,
    sport          TEXT,
    team_home      TEXT,
    team_away      TEXT,
    game_ts        REAL,
    external_id    TEXT,
    match_score    REAL,
    created_ts     REAL NOT NULL
);

-- Per-market per-cycle snapshot
CREATE TABLE IF NOT EXISTS sp_snapshots (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id            TEXT NOT NULL,
    event_id             TEXT,
    ts                   REAL NOT NULL,
    market_type          TEXT,
    poly_price           REAL,
    external_implied     REAL,
    mode_a_gap           REAL,
    external_fetch_ts    REAL,
    external_quote_age_s REAL,
    tte_hours            REAL
);

-- Per-event per-cycle basket aggregate (Mode A and B kept separate)
CREATE TABLE IF NOT EXISTS sp_baskets (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id                 TEXT NOT NULL,
    ts                       REAL NOT NULL,
    n_markets                INTEGER,
    n_with_external          INTEGER DEFAULT 0,
    n_aligned_mode_a         INTEGER DEFAULT 0,
    avg_gap_mode_a           REAL,
    n_semantic_pairs_checked INTEGER DEFAULT 0,
    n_contradicting_pairs    INTEGER DEFAULT 0,
    mode_b_score             REAL
);

-- Post-resolution accuracy tracking
CREATE TABLE IF NOT EXISTS sp_resolved (
    market_id            TEXT PRIMARY KEY,
    resolved_ts          REAL,
    outcome              INTEGER,
    last_mode_a_gap      REAL,
    last_poly_price      REAL,
    last_external_implied REAL,
    mode_a_was_correct   INTEGER,
    last_mode_b_score    REAL,
    mode_b_direction     TEXT,
    mode_b_was_correct   INTEGER
);
"""


def init_sports() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(str(SPORTS_DB))
    conn.row_factory = sqlite3.Row
    conn.executescript(_SPORTS_DDL)
    conn.commit()
    return conn


def conn_sports() -> sqlite3.Connection:
    c = sqlite3.connect(str(SPORTS_DB))
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c


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
