from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from replay.tape_feed import TradeFileRef, discover_trade_files
from utils.logger import setup_logger
from utils.paths import DATA_DIR, DATABASE_DIR, DB_PATH, ensure_runtime_dirs

logger = setup_logger("historical_tape_migration")

DEFAULT_DB_PATH = DATA_DIR / "historical_tape.db"
SCHEMA_VERSION = "3"


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA temp_store=MEMORY;
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS source_files (
            id INTEGER PRIMARY KEY,
            market_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            rel_path TEXT NOT NULL UNIQUE,
            abs_path TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            file_mtime_ns INTEGER NOT NULL,
            trade_count INTEGER,
            min_ts INTEGER,
            max_ts INTEGER,
            imported_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_source_files_market_token
        ON source_files (market_id, token_id);

        CREATE TABLE IF NOT EXISTS tape (
            source_file_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            market_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            price REAL NOT NULL,
            size REAL NOT NULL,
            side TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (source_file_id, seq),
            FOREIGN KEY (source_file_id) REFERENCES source_files(id) ON DELETE CASCADE
        ) WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS idx_tape_ts ON tape (timestamp);
        CREATE INDEX IF NOT EXISTS idx_tape_token_ts ON tape (token_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_tape_market_ts ON tape (market_id, timestamp);
        """
    )
    conn.execute(
        "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (SCHEMA_VERSION,),
    )
    conn.commit()


def maybe_rebuild(conn: sqlite3.Connection, db_path: Path, rebuild: bool) -> None:
    if not rebuild:
        return
    logger.info(f"Rebuilding historical tape DB: {db_path}")
    conn.executescript(
        """
        DROP TABLE IF EXISTS tape;
        DROP TABLE IF EXISTS source_files;
        DROP TABLE IF EXISTS meta;
        VACUUM;
        """
    )
    conn.commit()
    ensure_schema(conn)


def load_trades(path: Path) -> list[dict]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw.sort(key=lambda row: int(row.get("timestamp") or 0))
    return raw


def upsert_source_file(
    conn: sqlite3.Connection,
    *,
    market_id: str,
    token_id: str,
    rel_path: str,
    abs_path: str,
    file_size: int,
    file_mtime_ns: int,
) -> int:
    existing = conn.execute(
        "SELECT id, file_size, file_mtime_ns FROM source_files WHERE rel_path=?",
        (rel_path,),
    ).fetchone()
    if existing is None:
        cur = conn.execute(
            """
            INSERT INTO source_files(
                market_id, token_id, rel_path, abs_path, file_size, file_mtime_ns
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (market_id, token_id, rel_path, abs_path, file_size, file_mtime_ns),
        )
        return int(cur.lastrowid)

    source_file_id = int(existing[0])
    conn.execute(
        """
        UPDATE source_files
        SET market_id=?, token_id=?, abs_path=?, file_size=?, file_mtime_ns=?
        WHERE id=?
        """,
        (market_id, token_id, abs_path, file_size, file_mtime_ns, source_file_id),
    )
    return source_file_id


def source_file_is_fresh(conn: sqlite3.Connection, source_file_id: int, file_size: int, file_mtime_ns: int) -> bool:
    row = conn.execute(
        "SELECT file_size, file_mtime_ns, trade_count, imported_at FROM source_files WHERE id=?",
        (source_file_id,),
    ).fetchone()
    if row is None:
        return False
    if int(row[0]) != int(file_size) or int(row[1]) != int(file_mtime_ns):
        return False
    if row[2] is None or row[3] is None:
        return False
    tape_count = conn.execute(
        "SELECT COUNT(*) FROM tape WHERE source_file_id=?",
        (source_file_id,),
    ).fetchone()[0]
    return int(tape_count) == int(row[2])


def import_file(conn: sqlite3.Connection, ref, *, database_dir: Path) -> tuple[int, int, int]:
    stat = ref.path.stat()
    rel_path = str(ref.path.relative_to(database_dir))
    source_file_id = upsert_source_file(
        conn,
        market_id=ref.market_id,
        token_id=ref.token_id,
        rel_path=rel_path,
        abs_path=str(ref.path),
        file_size=int(stat.st_size),
        file_mtime_ns=int(stat.st_mtime_ns),
    )

    if source_file_is_fresh(conn, source_file_id, int(stat.st_size), int(stat.st_mtime_ns)):
        meta = conn.execute(
            "SELECT trade_count, min_ts, max_ts FROM source_files WHERE id=?",
            (source_file_id,),
        ).fetchone()
        return int(meta[0] or 0), int(meta[1] or 0), int(meta[2] or 0)

    conn.execute("DELETE FROM tape WHERE source_file_id=?", (source_file_id,))
    raw_trades = load_trades(ref.path)

    rows = []
    min_ts = None
    max_ts = None
    for seq, trade in enumerate(raw_trades):
        ts = int(trade.get("timestamp") or 0)
        min_ts = ts if min_ts is None else min(min_ts, ts)
        max_ts = ts if max_ts is None else max(max_ts, ts)
        rows.append(
            (
                source_file_id,
                seq,
                ts,
                ref.market_id,
                ref.token_id,
                float(trade.get("price") or 0.0),
                float(trade.get("size") or 0.0),
                str(trade.get("side") or ""),
            )
        )

    conn.executemany(
        """
        INSERT INTO tape(
            source_file_id, seq, timestamp, market_id, token_id, price, size, side
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.execute(
        """
        UPDATE source_files
        SET trade_count=?, min_ts=?, max_ts=?, imported_at=?
        WHERE id=?
        """,
        (
            len(rows),
            min_ts,
            max_ts,
            datetime.now(timezone.utc).isoformat(),
            source_file_id,
        ),
    )
    return len(rows), int(min_ts or 0), int(max_ts or 0)


def load_dataset_universe(dataset_db_path: Path) -> dict[str, set[str]]:
    conn = sqlite3.connect(dataset_db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT DISTINCT CAST(t.market_id AS TEXT) AS market_id,
                        CAST(t.token_id AS TEXT) AS token_id
        FROM tokens t
        JOIN markets m ON m.id = t.market_id
        ORDER BY m.start_date ASC, t.market_id ASC, t.token_id ASC
        """
    ).fetchall()
    conn.close()

    market_tokens: dict[str, set[str]] = {}
    for row in rows:
        market_id = str(row["market_id"])
        token_id = str(row["token_id"])
        market_tokens.setdefault(market_id, set()).add(token_id)
    return market_tokens



def load_dataset_trade_refs(
    *,
    dataset_db_path: Path,
    database_dir: Path,
    limit_files: int | None = None,
) -> tuple[list[TradeFileRef], int, int]:
    market_tokens = load_dataset_universe(dataset_db_path)
    allowed_markets = set(market_tokens)

    refs: list[TradeFileRef] = []
    skipped_outside_universe = 0
    for ref in discover_trade_files(database_dir):
        token_ids = market_tokens.get(ref.market_id)
        if token_ids is None:
            skipped_outside_universe += 1
            continue
        if ref.token_id not in token_ids:
            skipped_outside_universe += 1
            continue
        refs.append(ref)
        if limit_files is not None and len(refs) >= limit_files:
            break

    return refs, len(allowed_markets), skipped_outside_universe



def build_historical_tape_db(
    *,
    db_path: Path,
    database_dir: Path,
    dataset_db_path: Path,
    rebuild: bool = False,
    limit_files: int | None = None,
    log_every_files: int = 100,
) -> None:
    ensure_runtime_dirs()
    logger.info(
        f"Historical tape migration starting | db={db_path} | database_dir={database_dir} | dataset_db={dataset_db_path}"
    )
    refs, market_count, skipped_outside_universe = load_dataset_trade_refs(
        dataset_db_path=dataset_db_path,
        database_dir=database_dir,
        limit_files=limit_files,
    )
    logger.info(
        "Selected %s trade files from parsed dataset universe | markets=%s | skipped_outside_universe=%s"
        % (len(refs), market_count, skipped_outside_universe)
    )

    conn = sqlite3.connect(db_path)
    try:
        maybe_rebuild(conn, db_path, rebuild)
        ensure_schema(conn)
        conn.execute("PRAGMA cache_size=-200000")
        conn.execute("PRAGMA mmap_size=268435456")

        start = time.monotonic()
        total_trades = 0
        last_max_ts = None

        for idx, ref in enumerate(refs, start=1):
            trades_in_file, _min_ts, max_ts = import_file(conn, ref, database_dir=database_dir)
            total_trades += trades_in_file
            if max_ts:
                last_max_ts = max_ts

            if idx % 25 == 0:
                conn.commit()

            if idx == 1 or idx % max(1, log_every_files) == 0:
                elapsed = max(0.001, time.monotonic() - start)
                files_per_s = idx / elapsed
                trades_per_s = total_trades / elapsed
                eta_s = (len(refs) - idx) / files_per_s if files_per_s > 0 else None
                latest_ts = (
                    datetime.fromtimestamp(last_max_ts, tz=timezone.utc).isoformat()
                    if last_max_ts
                    else None
                )
                logger.info(
                    "Progress %s/%s files | trades=%s | %.1f files/s | %.0f trades/s | eta=%ss | latest_trade_ts=%s"
                    % (
                        idx,
                        len(refs),
                        total_trades,
                        files_per_s,
                        trades_per_s,
                        int(eta_s) if eta_s is not None else -1,
                        latest_ts,
                    )
                )

        conn.commit()
        elapsed = max(0.001, time.monotonic() - start)
        total_files = conn.execute("SELECT COUNT(*) FROM source_files").fetchone()[0]
        tape_rows = conn.execute("SELECT COUNT(*) FROM tape").fetchone()[0]
        logger.info(
            "Historical tape migration finished | files=%s | tape_rows=%s | elapsed=%.1fs | %.1f files/s | %.0f trades/s"
            % (total_files, tape_rows, elapsed, total_files / elapsed, tape_rows / elapsed)
        )
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Build historical_tape.db from full-history JSON trade snapshots")
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Output SQLite DB path")
    ap.add_argument("--database-dir", default=str(DATABASE_DIR), help="Source database/ directory with *_trades JSON files")
    ap.add_argument("--dataset-db", default=str(DB_PATH), help="polymarket_dataset.db used as source of canonical market/token universe")
    ap.add_argument("--rebuild", action="store_true", help="Drop and rebuild DB from scratch")
    ap.add_argument("--limit-files", type=int, default=None, help="Debug: import only first N trade files")
    ap.add_argument("--log-every-files", type=int, default=100, help="Progress log cadence")
    args = ap.parse_args()

    build_historical_tape_db(
        db_path=Path(args.db).expanduser(),
        database_dir=Path(args.database_dir).expanduser(),
        dataset_db_path=Path(args.dataset_db).expanduser(),
        rebuild=args.rebuild,
        limit_files=args.limit_files,
        log_every_files=max(1, args.log_every_files),
    )


if __name__ == "__main__":
    main()
