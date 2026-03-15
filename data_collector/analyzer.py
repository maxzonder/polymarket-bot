"""
Polymarket Analyzer — Real Swan Detector

Цель:
    Находить не просто геометрические лебеди, а более реалистичные торговые события,
    где был реальный вход и где можно было выйти на заданных иксах.

Ключевые идеи:
1. Находим сырые swan-события zigzag'ом (локальный минимум -> локальный максимум).
2. Для каждого события считаем узкую зону входа вокруг минимума.
3. Выход считаем не по "верхней половине пика", а по конкретному target_x:
       target_exit_price = entry_min_price * target_exit_x
4. Лебедь считается реальным только если:
   - possible_x >= min_recovery
   - entry_volume_usdc >= min_entry_liquidity
   - duration_entry_to_target_minutes >= min_duration_minutes
   - exit_volume_usdc >= required_exit_liquidity

required_exit_liquidity по умолчанию = min_entry_liquidity * target_exit_x
То есть если минимальный реальный вход считаем $10, а target_x=5,
то для выхода нужно хотя бы $50 объёма на целевой цене и выше.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

from utils.logger import setup_logger

from utils.paths import DATABASE_DIR, DB_PATH, ensure_runtime_dirs

ensure_runtime_dirs()
logger = setup_logger("analyzer")

DEFAULT_MIN_ENTRY_LIQUIDITY = 10.0
DEFAULT_MIN_RECOVERY = 5.0
DEFAULT_TARGET_EXIT_X = 5.0
DEFAULT_MIN_DURATION_MINUTES = 10.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS token_swans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    date TEXT NOT NULL,

    min_recovery REAL NOT NULL,
    target_exit_x REAL NOT NULL,
    min_entry_liquidity_required REAL NOT NULL,
    min_exit_liquidity_required REAL NOT NULL,
    min_duration_minutes_required REAL NOT NULL,

    entry_min_price REAL NOT NULL,
    entry_threshold_price REAL NOT NULL,
    entry_volume_usdc REAL NOT NULL,
    entry_trade_count INTEGER NOT NULL,
    entry_ts_first INTEGER,
    entry_ts_last INTEGER,

    target_exit_price REAL NOT NULL,
    exit_max_price REAL NOT NULL,
    exit_volume_usdc REAL NOT NULL,
    exit_trade_count INTEGER NOT NULL,
    exit_ts_first INTEGER,
    exit_ts_last INTEGER,

    duration_entry_zone_seconds INTEGER NOT NULL,
    duration_exit_zone_seconds INTEGER NOT NULL,
    duration_entry_to_target_minutes REAL NOT NULL,
    duration_entry_to_peak_minutes REAL NOT NULL,

    possible_x REAL NOT NULL,

    FOREIGN KEY (token_id) REFERENCES tokens(token_id)
);
CREATE INDEX IF NOT EXISTS idx_token_swans_date ON token_swans(date);
CREATE INDEX IF NOT EXISTS idx_token_swans_market ON token_swans(market_id);
CREATE INDEX IF NOT EXISTS idx_token_swans_token ON token_swans(token_id);
CREATE INDEX IF NOT EXISTS idx_token_swans_x ON token_swans(possible_x);
"""


@dataclass(frozen=True)
class RawSwan:
    entry_min_idx: int
    exit_max_idx: int
    entry_min_price: float
    exit_max_price: float
    possible_x: float


def build_folder_index() -> dict[str, str]:
    index: dict[str, str] = {}
    for date_str in os.listdir(DATABASE_DIR):
        date_path = os.path.join(DATABASE_DIR, date_str)
        if not os.path.isdir(date_path):
            continue
        for entry in os.listdir(date_path):
            if entry.endswith(".json") and not entry.startswith("collector"):
                index[entry[:-5]] = date_str
    return index


def load_trades(date_str: str, market_id: str, token_id: str) -> list[dict]:
    path = os.path.join(DATABASE_DIR, date_str, f"{market_id}_trades", f"{token_id}.json")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        trades = json.load(f)
    return sorted(trades, key=lambda t: t["timestamp"])


def find_zigzag_swans(trades: list[dict], min_recovery: float) -> list[RawSwan]:
    if not trades:
        return []

    swans: list[RawSwan] = []
    direction = "seeking_min"
    extreme_idx = 0
    pending_min_idx: Optional[int] = None

    for idx, t in enumerate(trades):
        price = float(t["price"])

        if direction == "seeking_min":
            if price <= float(trades[extreme_idx]["price"]):
                extreme_idx = idx
            elif price >= float(trades[extreme_idx]["price"]) * min_recovery:
                pending_min_idx = extreme_idx
                direction = "seeking_max"
                extreme_idx = idx

        else:
            if price >= float(trades[extreme_idx]["price"]):
                extreme_idx = idx
            elif price <= float(trades[extreme_idx]["price"]) / min_recovery:
                entry_price = float(trades[pending_min_idx]["price"])
                exit_price = float(trades[extreme_idx]["price"])
                swans.append(
                    RawSwan(
                        entry_min_idx=pending_min_idx,
                        exit_max_idx=extreme_idx,
                        entry_min_price=entry_price,
                        exit_max_price=exit_price,
                        possible_x=exit_price / entry_price,
                    )
                )
                pending_min_idx = None
                direction = "seeking_min"
                extreme_idx = idx

    if direction == "seeking_max" and pending_min_idx is not None:
        entry_price = float(trades[pending_min_idx]["price"])
        exit_price = float(trades[extreme_idx]["price"])
        possible_x = exit_price / entry_price
        if possible_x >= min_recovery:
            swans.append(
                RawSwan(
                    entry_min_idx=pending_min_idx,
                    exit_max_idx=extreme_idx,
                    entry_min_price=entry_price,
                    exit_max_price=exit_price,
                    possible_x=possible_x,
                )
            )

    return swans


def _sum_usdc(trades: list[dict]) -> float:
    return sum(float(t["price"]) * float(t["size"]) for t in trades)


def _find_entry_zone(trades: list[dict], swan: RawSwan, min_recovery: float) -> tuple[int, int, float]:
    entry_threshold = swan.entry_min_price * min_recovery
    min_idx = swan.entry_min_idx
    max_idx = swan.exit_max_idx

    start = min_idx
    while start > 0 and start - 1 <= max_idx and float(trades[start - 1]["price"]) <= entry_threshold:
        start -= 1

    end = min_idx
    while end + 1 < len(trades) and end + 1 <= max_idx and float(trades[end + 1]["price"]) <= entry_threshold:
        end += 1

    return start, end, entry_threshold


def _find_target_hit_idx(trades: list[dict], swan: RawSwan, target_exit_price: float) -> Optional[int]:
    for idx in range(swan.entry_min_idx, swan.exit_max_idx + 1):
        if float(trades[idx]["price"]) >= target_exit_price:
            return idx
    return None


def compute_real_metrics(
    trades: list[dict],
    swan: RawSwan,
    min_recovery: float,
    target_exit_x: float,
) -> Optional[dict]:
    entry_start_idx, entry_end_idx, entry_threshold = _find_entry_zone(trades, swan, min_recovery)
    entry_trades = trades[entry_start_idx: entry_end_idx + 1]
    if not entry_trades:
        return None

    target_exit_price = swan.entry_min_price * target_exit_x
    target_hit_idx = _find_target_hit_idx(trades, swan, target_exit_price)
    if target_hit_idx is None:
        return None

    exit_trades = [
        t for t in trades[target_hit_idx: swan.exit_max_idx + 1]
        if float(t["price"]) >= target_exit_price
    ]
    if not exit_trades:
        return None

    entry_first = int(entry_trades[0]["timestamp"])
    entry_last = int(entry_trades[-1]["timestamp"])
    exit_first = int(exit_trades[0]["timestamp"])
    exit_last = int(exit_trades[-1]["timestamp"])
    peak_ts = int(trades[swan.exit_max_idx]["timestamp"])

    return {
        "entry_min_price": swan.entry_min_price,
        "entry_threshold_price": entry_threshold,
        "entry_volume_usdc": _sum_usdc(entry_trades),
        "entry_trade_count": len(entry_trades),
        "entry_ts_first": entry_first,
        "entry_ts_last": entry_last,
        "duration_entry_zone_seconds": max(0, entry_last - entry_first),
        "target_exit_price": target_exit_price,
        "exit_max_price": swan.exit_max_price,
        "exit_volume_usdc": _sum_usdc(exit_trades),
        "exit_trade_count": len(exit_trades),
        "exit_ts_first": exit_first,
        "exit_ts_last": exit_last,
        "duration_exit_zone_seconds": max(0, exit_last - exit_first),
        "duration_entry_to_target_minutes": max(0.0, (exit_first - entry_first) / 60.0),
        "duration_entry_to_peak_minutes": max(0.0, (peak_ts - entry_first) / 60.0),
        "possible_x": swan.possible_x,
    }


def run(
    recompute: bool = False,
    filter_date: str | None = None,
    filter_market_id: str | None = None,
    min_entry_liquidity: float = DEFAULT_MIN_ENTRY_LIQUIDITY,
    min_recovery: float = DEFAULT_MIN_RECOVERY,
    target_exit_x: float = DEFAULT_TARGET_EXIT_X,
    min_duration_minutes: float = DEFAULT_MIN_DURATION_MINUTES,
):
    required_exit_liquidity = min_entry_liquidity * target_exit_x

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    if recompute:
        conn.execute("DROP TABLE IF EXISTS token_swans")

    conn.executescript(SCHEMA)
    conn.commit()

    if recompute:
        clauses = []
        params = []
        if filter_date:
            clauses.append("date = ?")
            params.append(filter_date)
        if filter_market_id:
            clauses.append("market_id = ?")
            params.append(filter_market_id)
        if clauses:
            conn.execute(f"DELETE FROM token_swans WHERE {' AND '.join(clauses)}", params)
        else:
            conn.execute("DELETE FROM token_swans")
        conn.commit()
        logger.info("Recompute: cleared token_swans")

    logger.info("Building folder index...")
    folder_index = build_folder_index()

    token_query = "SELECT token_id, market_id FROM tokens"
    params = []
    where = []
    if filter_market_id:
        where.append("market_id = ?")
        params.append(filter_market_id)
    if filter_date:
        market_ids = [mid for mid, d in folder_index.items() if d == filter_date]
        if not market_ids:
            logger.error(f"No markets found for date {filter_date}")
            return
        placeholders = ",".join("?" * len(market_ids))
        where.append(f"market_id IN ({placeholders})")
        params.extend(market_ids)
    if where:
        token_query += " WHERE " + " AND ".join(where)

    tokens = conn.execute(token_query, params).fetchall()
    total = len(tokens)
    logger.info(
        f"Processing {total} tokens | min_entry_liq=${min_entry_liquidity}, "
        f"target_exit_x={target_exit_x}x, required_exit_liq=${required_exit_liquidity}, "
        f"min_recovery={min_recovery}x, min_duration={min_duration_minutes}m"
    )

    ok = no_trades = no_raw_swans = rejected = errors = 0
    real_swans_total = 0
    t0 = time.monotonic()
    PROGRESS_STEP = 1000
    COMMIT_STEP = 2000

    for i, (token_id, market_id) in enumerate(tokens, 1):
        date_str = folder_index.get(market_id)
        if not date_str:
            no_trades += 1
            continue

        trades = load_trades(date_str, market_id, token_id)
        if not trades:
            no_trades += 1
            continue

        try:
            raw_swans = find_zigzag_swans(trades, min_recovery=min_recovery)
        except Exception as e:
            logger.warning(f"{token_id}: zigzag error — {e}")
            errors += 1
            continue

        if not raw_swans:
            no_raw_swans += 1
            continue

        token_real_count = 0
        for swan in raw_swans:
            metrics = compute_real_metrics(trades, swan, min_recovery=min_recovery, target_exit_x=target_exit_x)
            if not metrics:
                rejected += 1
                continue
            if metrics["entry_volume_usdc"] < min_entry_liquidity:
                rejected += 1
                continue
            if metrics["exit_volume_usdc"] < required_exit_liquidity:
                rejected += 1
                continue
            full_swan_minutes = (metrics["exit_ts_last"] - metrics["entry_ts_first"]) / 60.0 if metrics["entry_ts_first"] is not None and metrics["exit_ts_last"] is not None else 0.0
            if full_swan_minutes < min_duration_minutes:
                rejected += 1
                continue

            conn.execute(
                """
                INSERT INTO token_swans (
                    token_id, market_id, date,
                    min_recovery, target_exit_x, min_entry_liquidity_required,
                    min_exit_liquidity_required, min_duration_minutes_required,
                    entry_min_price, entry_threshold_price, entry_volume_usdc,
                    entry_trade_count, entry_ts_first, entry_ts_last,
                    target_exit_price, exit_max_price, exit_volume_usdc,
                    exit_trade_count, exit_ts_first, exit_ts_last,
                    duration_entry_zone_seconds, duration_exit_zone_seconds,
                    duration_entry_to_target_minutes, duration_entry_to_peak_minutes,
                    possible_x
                ) VALUES (
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?
                )
                """,
                (
                    token_id, market_id, date_str,
                    min_recovery, target_exit_x, min_entry_liquidity,
                    required_exit_liquidity, min_duration_minutes,
                    metrics["entry_min_price"], metrics["entry_threshold_price"], metrics["entry_volume_usdc"],
                    metrics["entry_trade_count"], metrics["entry_ts_first"], metrics["entry_ts_last"],
                    metrics["target_exit_price"], metrics["exit_max_price"], metrics["exit_volume_usdc"],
                    metrics["exit_trade_count"], metrics["exit_ts_first"], metrics["exit_ts_last"],
                    metrics["duration_entry_zone_seconds"], metrics["duration_exit_zone_seconds"],
                    metrics["duration_entry_to_target_minutes"], metrics["duration_entry_to_peak_minutes"],
                    metrics["possible_x"],
                ),
            )
            token_real_count += 1
            real_swans_total += 1

        if token_real_count > 0:
            ok += 1

        if i % COMMIT_STEP == 0:
            conn.commit()
        if i % PROGRESS_STEP == 0:
            elapsed = time.monotonic() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = int((total - i) / rate) if rate > 0 else 0
            logger.info(
                f"{i}/{total} — {rate:.0f}/s ETA ~{eta}s | ok={ok} no_trades={no_trades} "
                f"no_raw_swans={no_raw_swans} rejected={rejected} real_swans_total={real_swans_total}"
            )

    conn.commit()
    elapsed = int(time.monotonic() - t0)
    logger.info(
        f"Done in {elapsed}s | ok={ok} no_trades={no_trades} no_raw_swans={no_raw_swans} "
        f"rejected={rejected} errors={errors} real_swans_total={real_swans_total}"
    )

    clauses = []
    params = []
    if filter_date:
        clauses.append("date = ?")
        params.append(filter_date)
    if filter_market_id:
        clauses.append("market_id = ?")
        params.append(filter_market_id)
    where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    stats = conn.execute(
        f"""
        SELECT
            COUNT(*) as total_swans,
            COUNT(DISTINCT token_id) as tokens_with_swans,
            COUNT(DISTINCT market_id) as markets_with_swans,
            ROUND(AVG(possible_x), 2) as avg_x,
            ROUND(MAX(possible_x), 2) as max_x,
            ROUND(AVG(entry_volume_usdc), 2) as avg_entry_liq,
            ROUND(AVG(exit_volume_usdc), 2) as avg_exit_liq,
            ROUND(AVG(duration_entry_to_target_minutes), 2) as avg_minutes_to_target
        FROM token_swans
        {where_sql}
        """,
        params,
    ).fetchone()

    logger.info(
        "Stats: total_swans={}, tokens={}, markets={}, avg_x={}, max_x={}, avg_entry_liq=${}, avg_exit_liq=${}, avg_minutes_to_target={}".format(
            stats[0], stats[1], stats[2], stats[3], stats[4], stats[5], stats[6], stats[7]
        )
    )
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Polymarket Analyzer — Real Swan Detector")
    ap.add_argument("--date", metavar="YYYY-MM-DD", help="Только один день")
    ap.add_argument("--market-id", help="Только один market_id")
    ap.add_argument("--recompute", action="store_true", help="Пересчитать заново")
    ap.add_argument("--min-entry-liquidity", type=float, default=DEFAULT_MIN_ENTRY_LIQUIDITY)
    ap.add_argument("--min-recovery", type=float, default=DEFAULT_MIN_RECOVERY)
    ap.add_argument("--target-exit-x", type=float, default=DEFAULT_TARGET_EXIT_X)
    ap.add_argument("--min-duration-minutes", type=float, default=DEFAULT_MIN_DURATION_MINUTES)
    args = ap.parse_args()

    run(
        recompute=args.recompute,
        filter_date=args.date,
        filter_market_id=args.market_id,
        min_entry_liquidity=args.min_entry_liquidity,
        min_recovery=args.min_recovery,
        target_exit_x=args.target_exit_x,
        min_duration_minutes=args.min_duration_minutes,
    )
