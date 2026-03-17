"""
Build ChromaDB vector store from Polymarket markets — issue #12.

Embeds each market as:
    {question}. {event_title}. {event_description[:200]}. Tags: {tags}

Uses ChromaDB embedded mode (no infra) with the default all-MiniLM-L6-v2
ONNX model. Persists to {POLYMARKET_DATA_DIR}/chroma_db.

Re-run safe: deletes and recreates the collection on each run.

Usage:
    python3 scripts/build_chroma.py
    python3 scripts/build_chroma.py --limit 5000   # quick smoke-test
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.paths import DB_PATH, DATA_DIR
from utils.logger import setup_logger

logger = setup_logger("build_chroma")

CHROMA_PATH = DATA_DIR / "chroma_db"
COLLECTION_NAME = "markets"
BATCH_SIZE = 500


def build_doc(row: dict) -> str:
    question = row.get("question") or ""
    event_title = row.get("event_title") or ""
    event_desc = (row.get("event_description") or "")[:200]
    tags = row.get("tags") or ""

    parts = []
    if question:
        parts.append(question)
    if event_title and event_title != question:
        parts.append(event_title)
    if event_desc:
        parts.append(event_desc)
    if tags:
        parts.append(f"Tags: {tags}")
    return ". ".join(parts) if parts else question


def load_markets(conn: sqlite3.Connection, limit: int | None) -> list[dict]:
    """
    Load all markets with their best (highest) swan_score across all tokens/dates.
    Markets with no swan entry get swan_score=None.
    """
    rows = conn.execute(
        """
        SELECT
            m.id          AS market_id,
            m.question,
            m.event_title,
            m.event_description,
            m.tags,
            m.slug        AS market_slug,
            MAX(fm.swan_score)      AS swan_score,
            MIN(fm.entry_min_price) AS entry_price,
            MAX(ts.possible_x)      AS possible_x
        FROM markets m
        LEFT JOIN feature_mart fm ON m.id = fm.market_id
        LEFT JOIN token_swans  ts ON m.id = ts.market_id
        WHERE m.question IS NOT NULL
        GROUP BY m.id
        ORDER BY m.id
        """
    ).fetchall()
    if limit:
        rows = rows[:limit]
    return [dict(r) for r in rows]


def run(limit: int | None) -> None:
    import chromadb

    CHROMA_PATH.mkdir(parents=True, exist_ok=True)

    db_conn = sqlite3.connect(DB_PATH)
    db_conn.row_factory = sqlite3.Row
    logger.info("Loading markets from SQLite …")
    markets = load_markets(db_conn, limit)
    db_conn.close()
    logger.info(f"  {len(markets):,} markets loaded")

    client = chromadb.PersistentClient(path=str(CHROMA_PATH))

    # Delete existing collection so re-runs are idempotent
    try:
        client.delete_collection(COLLECTION_NAME)
        logger.info(f"Deleted existing '{COLLECTION_NAME}' collection")
    except Exception:
        pass

    col = client.create_collection(COLLECTION_NAME)
    logger.info(f"Created collection '{COLLECTION_NAME}' at {CHROMA_PATH}")

    t0 = time.monotonic()
    total = len(markets)

    for batch_start in range(0, total, BATCH_SIZE):
        batch = markets[batch_start : batch_start + BATCH_SIZE]

        ids = [r["market_id"] for r in batch]
        documents = [build_doc(r) for r in batch]
        metadatas = [
            {
                "market_id":   r["market_id"],
                "market_slug": r["market_slug"] or "",
                "swan_score":  int(r["swan_score"]) if r["swan_score"] is not None else -1,
                "entry_price": float(r["entry_price"]) if r["entry_price"] is not None else -1.0,
                "possible_x":  float(r["possible_x"]) if r["possible_x"] is not None else -1.0,
            }
            for r in batch
        ]

        col.add(ids=ids, documents=documents, metadatas=metadatas)

        done = batch_start + len(batch)
        if done % 10_000 < BATCH_SIZE or done == total:
            elapsed = time.monotonic() - t0
            rate = done / elapsed if elapsed > 0 else 0
            logger.info(f"  {done:>7,}/{total:,}  ({rate:.0f}/s)  {elapsed:.0f}s elapsed")

    elapsed = time.monotonic() - t0
    logger.info(
        f"Done in {elapsed:.0f}s — {total:,} markets embedded → {CHROMA_PATH}"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Build ChromaDB vector store (issue #12)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Max markets to embed (smoke-test)")
    args = ap.parse_args()
    run(limit=args.limit)


if __name__ == "__main__":
    main()
