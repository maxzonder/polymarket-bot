"""
Neg-Risk Group Observer — Phase 1A.

Every 5 minutes:
  1. Fetch all open neg_risk=true markets from Gamma API
  2. Cluster into groups by event_slug
  3. For each group: fetch YES-token orderbook per leg from CLOB
  4. Compute sum_best_ask (executable cost to buy all YES outcomes)
  5. Persist snapshot + per-leg detail to obs_negrisk.db
  6. Track dislocation episodes (sum_best_ask < DISLOCATION_THRESHOLD)

Leg selection: always the YES token for each market (clobTokenIds index matching
"Yes" in outcomes[], fallback to index 0). This makes sum_best_ask a coherent,
resolution-consistent basket — not "cheapest token from each market".

No trading. Pure observation.
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

import requests

from ..utils.paths import NEGRISK_DB, ensure_dirs
from ..utils.logger import setup_logger
from ..db import init_negrisk, conn_negrisk

logger = setup_logger("negrisk_observer")

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"

DISLOCATION_THRESHOLD = 0.97   # sum_best_ask below this = dislocation
POLL_INTERVAL_SEC     = 300    # 5 minutes
PAGE_SIZE             = 500
MIN_GROUP_MARKETS     = 2      # skip singleton "groups"


@dataclass
class LegBook:
    market_id: str
    token_id: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    best_bid_size: Optional[float]
    best_ask_size: Optional[float]
    last_trade_price: Optional[float]  # from Gamma — no extra API call
    market_volume: float


def _fetch_neg_risk_markets() -> list[dict]:
    """Fetch all active neg_risk markets from Gamma API."""
    markets = []
    offset = 0
    while True:
        params = {
            "closed": "false",
            "active": "true",
            "neg_risk": "true",
            "include_tag": "true",
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        try:
            resp = requests.get(f"{GAMMA_BASE}/markets", params=params, timeout=30)
            resp.raise_for_status()
            page = resp.json()
        except Exception as e:
            logger.warning(f"Gamma fetch failed at offset={offset}: {e}")
            break

        if not page:
            break
        markets.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.1)

    logger.info(f"Fetched {len(markets)} neg_risk markets from Gamma")
    return markets


def _cluster_by_event(markets: list[dict]) -> dict[str, list[dict]]:
    """Group markets by event_slug. Falls back to event_title if slug missing."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for m in markets:
        events = m.get("events") or []
        slug = None
        title = None
        if events and isinstance(events[0], dict):
            slug  = events[0].get("slug")
            title = events[0].get("title")
        slug = slug or m.get("eventSlug") or title or m.get("id")
        groups[str(slug)].append(m)
    return dict(groups)


def _fetch_orderbook(token_id: str) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Returns (best_bid, best_ask, bid_size, ask_size) from CLOB."""
    try:
        resp = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=10)
        resp.raise_for_status()
        book = resp.json()
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        best_bid = float(bids[0]["price"]) if bids else None
        bid_size = float(bids[0]["size"])  if bids else None
        best_ask = float(asks[0]["price"]) if asks else None
        ask_size = float(asks[0]["size"])  if asks else None
        return best_bid, best_ask, bid_size, ask_size
    except Exception:
        return None, None, None, None


def _get_yes_token_id(m: dict) -> Optional[str]:
    """
    Return the YES-token ID for a market.

    clobTokenIds[] and outcomes[] are parallel arrays from Gamma.
    Find the index where outcome name is "Yes" (case-insensitive);
    fall back to index 0 if no explicit "Yes" label found.
    """
    token_ids_raw = m.get("clobTokenIds", "[]")
    if isinstance(token_ids_raw, str):
        try:
            token_ids_raw = json.loads(token_ids_raw)
        except Exception:
            token_ids_raw = []

    if not token_ids_raw:
        return None

    outcomes_raw = m.get("outcomes", "[]")
    if isinstance(outcomes_raw, str):
        try:
            outcomes_raw = json.loads(outcomes_raw)
        except Exception:
            outcomes_raw = []

    for i, outcome in enumerate(outcomes_raw):
        if str(outcome).lower() in ("yes", "true", "1") and i < len(token_ids_raw):
            return str(token_ids_raw[i])

    # Fallback: Polymarket convention is YES at index 0
    return str(token_ids_raw[0])


def _get_legs(markets_in_group: list[dict]) -> list[LegBook]:
    """
    For each market, fetch the YES-token orderbook.

    YES token represents the specific outcome this market covers in the group.
    In a neg-risk group exactly one YES resolves to $1, all others to $0,
    so sum(YES best_asks) is the coherent executable cost of the full basket.
    Picking cheapest token would mix YES/NO arbitrarily and break this invariant.
    """
    legs = []
    for m in markets_in_group:
        market_id = str(m.get("id", ""))
        volume = float(m.get("volumeNum") or m.get("volume") or 0)

        # last_trade_price already in Gamma response — no extra call
        try:
            ltp = float(m["lastTradePrice"]) if m.get("lastTradePrice") else None
        except (ValueError, TypeError):
            ltp = None

        token_id = _get_yes_token_id(m)
        if not token_id:
            continue

        bid, ask, bid_sz, ask_sz = _fetch_orderbook(token_id)
        time.sleep(0.05)  # gentle rate limit

        if ask is not None:
            legs.append(LegBook(
                market_id=market_id,
                token_id=token_id,
                best_bid=bid,
                best_ask=ask,
                best_bid_size=bid_sz,
                best_ask_size=ask_sz,
                last_trade_price=ltp,
                market_volume=volume,
            ))

    return legs


def _upsert_group(conn, event_slug: str, event_title: Optional[str], n_markets: int) -> int:
    now = int(time.time())
    conn.execute(
        """INSERT INTO nr_groups (event_slug, event_title, n_markets, first_seen_ts)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(event_slug) DO UPDATE SET
               n_markets=excluded.n_markets,
               event_title=COALESCE(excluded.event_title, event_title)""",
        (event_slug, event_title, n_markets, now),
    )
    row = conn.execute("SELECT id FROM nr_groups WHERE event_slug=?", (event_slug,)).fetchone()
    return row["id"]


def _save_snapshot(conn, group_id: int, legs: list[LegBook]) -> int:
    ts = int(time.time())
    asks = [l.best_ask for l in legs if l.best_ask is not None]
    bids = [l.best_bid for l in legs if l.best_bid is not None]

    sum_ask = sum(asks) if asks else None
    sum_mid = None
    if len(asks) == len(legs) and len(bids) == len(legs):
        sum_mid = sum((a + b) / 2 for a, b in zip(asks, bids))

    total_vol = sum(l.market_volume for l in legs)
    is_dis = int(sum_ask is not None and sum_ask < DISLOCATION_THRESHOLD)

    conn.execute(
        """INSERT INTO nr_snapshots
           (group_id, ts, n_legs, sum_best_ask, sum_mid, total_group_vol, is_dislocated)
           VALUES (?,?,?,?,?,?,?)""",
        (group_id, ts, len(legs), sum_ask, sum_mid, total_vol, is_dis),
    )
    snap_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    for leg in legs:
        conn.execute(
            """INSERT INTO nr_legs
               (snapshot_id, market_id, token_id, best_bid, best_ask,
                best_bid_size, best_ask_size, last_trade_price, market_volume)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (snap_id, leg.market_id, leg.token_id, leg.best_bid, leg.best_ask,
             leg.best_bid_size, leg.best_ask_size, leg.last_trade_price, leg.market_volume),
        )

    return snap_id


def _update_dislocations(conn, group_id: int, sum_ask: Optional[float], ts: int) -> None:
    """Open/close dislocation episode records.

    Episode lifecycle:
      - end_ts=NULL  → episode is open and ongoing
      - end_ts set   → episode closed (dislocation ended)

    During continuation we update aggregate stats only; end_ts stays NULL.
    end_ts is only written when the group exits the dislocation zone.
    """
    if sum_ask is None:
        return

    open_ep = conn.execute(
        "SELECT id FROM nr_dislocations WHERE group_id=? AND end_ts IS NULL",
        (group_id,),
    ).fetchone()

    if sum_ask < DISLOCATION_THRESHOLD:
        gap = 1.0 - sum_ask
        if open_ep:
            # Episode continues — update stats, keep end_ts=NULL
            conn.execute(
                """UPDATE nr_dislocations
                   SET min_sum_ask=MIN(min_sum_ask,?), max_gap=MAX(max_gap,?),
                       n_snapshots=n_snapshots+1
                   WHERE id=?""",
                (sum_ask, gap, open_ep["id"]),
            )
        else:
            # New episode starts
            group_vol = conn.execute(
                "SELECT total_group_vol FROM nr_snapshots WHERE group_id=? ORDER BY ts DESC LIMIT 1",
                (group_id,),
            ).fetchone()
            vol = group_vol["total_group_vol"] if group_vol else 0
            conn.execute(
                """INSERT INTO nr_dislocations
                   (group_id, start_ts, end_ts, min_sum_ask, max_gap, group_volume, n_snapshots)
                   VALUES (?,?,NULL,?,?,?,1)""",
                (group_id, ts, sum_ask, gap, vol),
            )
    else:
        if open_ep:
            # Dislocation ended — close the episode
            conn.execute(
                "UPDATE nr_dislocations SET end_ts=? WHERE id=?",
                (ts, open_ep["id"]),
            )


def _reconcile_resolved(conn) -> int:
    """
    For known groups: check Gamma if all markets are now closed.
    If so, determine winner (YES token = 1) and write to nr_resolved.
    Runs once per cycle — checks only groups not yet in nr_resolved.
    """
    unresolved = conn.execute(
        """SELECT g.id, g.event_slug, g.n_markets
           FROM nr_groups g
           WHERE NOT EXISTS (SELECT 1 FROM nr_resolved r WHERE r.group_id = g.id)
           AND g.first_seen_ts < ?""",
        (int(time.time()) - 3600,),  # only check groups seen > 1h ago
    ).fetchall()

    n_resolved = 0
    for group in unresolved:
        group_id = group["id"]
        slug = group["event_slug"]

        # Get distinct market_ids in this group from nr_legs
        market_ids = [
            r["market_id"] for r in conn.execute(
                """SELECT DISTINCT l.market_id FROM nr_legs l
                   JOIN nr_snapshots s ON s.id = l.snapshot_id
                   WHERE s.group_id = ?""",
                (group_id,),
            ).fetchall()
        ]
        if not market_ids:
            continue

        # Check Gamma for each market — all must be closed
        closed_markets = []
        winner_market_id = None
        all_closed = True

        for mid in market_ids[:20]:  # cap at 20 to avoid long loops
            try:
                resp = requests.get(
                    f"{GAMMA_BASE}/markets/{mid}", timeout=10
                )
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list):
                    data = data[0] if data else {}
            except Exception:
                all_closed = False
                break

            if not (data.get("closed") or not data.get("active", True)):
                all_closed = False
                break

            # Determine winner from outcomePrices
            op_raw = data.get("outcomePrices", "[]")
            if isinstance(op_raw, str):
                try:
                    op_raw = json.loads(op_raw)
                except Exception:
                    op_raw = []
            if op_raw:
                try:
                    yes_price = float(op_raw[0])
                    if yes_price >= 0.99:
                        winner_market_id = mid
                except (ValueError, TypeError):
                    pass

            closed_markets.append(mid)
            time.sleep(0.05)

        if not all_closed or len(closed_markets) < len(market_ids):
            continue

        # Check if group ever had a dislocation
        had_dis = conn.execute(
            "SELECT COUNT(*) n FROM nr_dislocations WHERE group_id=?",
            (group_id,),
        ).fetchone()["n"] > 0

        resolved_ts = int(time.time())
        conn.execute(
            """INSERT OR IGNORE INTO nr_resolved
               (group_id, resolved_ts, winner_market_id, n_legs, had_dislocation)
               VALUES (?,?,?,?,?)""",
            (group_id, resolved_ts, winner_market_id,
             len(closed_markets), 1 if had_dis else 0),
        )
        n_resolved += 1
        logger.info(
            f"RESOLVED group {slug[:40]} "
            f"winner={winner_market_id or 'unknown'} "
            f"had_dislocation={had_dis}"
        )

    return n_resolved


def run_once() -> dict:
    """Run one observation cycle. Returns summary dict."""
    init_negrisk()
    markets = _fetch_neg_risk_markets()
    if not markets:
        logger.warning("No neg_risk markets fetched")
        return {"groups": 0, "dislocations": 0}

    groups = _cluster_by_event(markets)
    event_titles: dict[str, Optional[str]] = {}
    for slug, ms in groups.items():
        events = ms[0].get("events") or []
        title = events[0].get("title") if events and isinstance(events[0], dict) else None
        event_titles[slug] = title

    conn = conn_negrisk()
    n_dis = 0
    n_groups = 0

    for slug, ms in groups.items():
        if len(ms) < MIN_GROUP_MARKETS:
            continue
        n_groups += 1
        title = event_titles.get(slug)

        legs = _get_legs(ms)
        if not legs:
            continue

        group_id = _upsert_group(conn, slug, title, len(ms))
        _save_snapshot(conn, group_id, legs)

        asks = [l.best_ask for l in legs if l.best_ask is not None]
        sum_ask = sum(asks) if len(asks) == len(legs) else None
        ts = int(time.time())
        _update_dislocations(conn, group_id, sum_ask, ts)

        if sum_ask is not None and sum_ask < DISLOCATION_THRESHOLD:
            n_dis += 1
            logger.info(
                f"DISLOCATION: {slug[:40]} sum_ask={sum_ask:.4f} "
                f"gap={1-sum_ask:.4f} legs={len(legs)}"
            )

    n_resolved = _reconcile_resolved(conn)
    conn.commit()
    conn.close()

    summary = {
        "groups": n_groups,
        "dislocations": n_dis,
        "resolved": n_resolved,
        "ts": int(time.time()),
    }
    logger.info(f"Cycle done: {n_groups} groups, {n_dis} dislocations, {n_resolved} resolved")
    return summary


def run_loop() -> None:
    """Run observation loop forever."""
    logger.info(f"Neg-Risk observer started | interval={POLL_INTERVAL_SEC}s | threshold={DISLOCATION_THRESHOLD}")
    while True:
        try:
            run_once()
        except Exception as e:
            logger.error(f"Observer cycle error: {e}", exc_info=True)
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run_loop()
