"""
Sports Observer — Path C.

Every 30 minutes:
  1. Fetch all open sports markets from Gamma API
  2. Group by event_title into baskets
  3. Layer 1 (canonicalization): parse event_title, attempt external match
  4. Layer 2 (market observation):
       Mode A — if event matched externally: compute gap = external_implied - poly_price
       Mode B — always: check semantic pair inconsistencies within basket
  5. Layer 3 (basket summary): aggregate Mode A gaps and Mode B score separately
  6. Reconcile resolved markets → sp_resolved + directional accuracy

Mode A requires ODDS_API_KEY in environment. If absent, only Mode B runs.
Mode B fires on any basket with ≥2 markets regardless of external matching.

No trading. Pure observation.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Optional

import requests

from ..utils.paths import SPORTS_DB, ensure_dirs
from ..utils.logger import setup_logger
from ..db import init_sports, conn_sports
from ..api.odds_client import fetch_all_sports, SPORT_KEYS
from .matcher import (
    match_event, persist_attempt, persist_event,
    SUPPORTED_SPORTS, make_event_id,
)

logger = setup_logger("sports_observer")

GAMMA_BASE        = "https://gamma-api.polymarket.com"
POLL_INTERVAL_SEC = 1800   # 30 minutes
PAGE_SIZE         = 500
MIN_BASKET_SIZE   = 2      # skip single-market events
MODE_A_GAP_THRESH = 0.03   # gap magnitude to count as "aligned"

# ── Semantic pair rules for Mode B ────────────────────────────────────────────
# Each rule: (type_a, type_b, direction)
# direction "same"    → P(type_a for team X) should correlate positively with P(type_b for team X)
#           "ge"      → P(type_a) >= P(type_b) semantically (violation if type_b > type_a + threshold)
#
# Market type classification is done by keyword matching on question text.

_MARKET_TYPE_RULES: list[tuple[str, str]] = [
    # (keyword, market_type)
    ("win",           "winner"),
    ("winner",        "winner"),
    ("beat",          "winner"),
    ("defeat",        "winner"),
    ("halftime",      "halftime"),
    ("half-time",     "halftime"),
    ("half time",     "halftime"),
    ("first quarter", "first_quarter"),
    ("first score",   "first_score"),
    ("first point",   "first_score"),
    ("first blood",   "first_score"),
    ("first kill",    "first_score"),
    ("map",           "map"),
    ("series",        "series"),
    ("game",          "game_outcome"),
]

# Semantic pair rules: (type_a, type_b, rule)
# "winner_ge_halftime": P(team wins) >= P(team leads at half) for same team
# "winner_correlates_firstscore": if winner prob diverges greatly from first_score, flag
_SEMANTIC_PAIRS: list[tuple[str, str]] = [
    ("winner",   "halftime"),
    ("winner",   "first_score"),
    ("series",   "map"),        # CS2: P(series win) ≤ P(map win) for same team
]

CONTRADICTION_THRESHOLD = 0.20  # flag if implied probabilities diverge by > 20%


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_date(raw: Optional[str]) -> Optional[float]:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _classify_market_type(question: str) -> str:
    q = question.lower()
    for keyword, mtype in _MARKET_TYPE_RULES:
        if keyword in q:
            return mtype
    return "other"


def _extract_team_direction(question: str, team_home: Optional[str], team_away: Optional[str]) -> Optional[str]:
    """
    Return 'home' or 'away' based on which team appears to be the YES side
    of the question. Used for Mode B pairing.
    """
    if not team_home or not team_away:
        return None
    q = question.lower()
    home_l = team_home.lower()
    away_l = team_away.lower()
    # Simple: whichever team name appears first in the question
    hi = q.find(home_l.split()[-1])  # use last word of team name (e.g. "Warriors")
    ai = q.find(away_l.split()[-1])
    if hi == -1 and ai == -1:
        return None
    if hi == -1:
        return "away"
    if ai == -1:
        return "home"
    return "home" if hi <= ai else "away"


def _get_yes_price(market: dict) -> Optional[float]:
    """Extract YES token mid price from outcomePrices."""
    outcome_prices_raw = market.get("outcomePrices", "[]")
    outcomes_raw = market.get("outcomes", "[]")
    if isinstance(outcome_prices_raw, str):
        try:
            outcome_prices_raw = json.loads(outcome_prices_raw)
        except Exception:
            outcome_prices_raw = []
    if isinstance(outcomes_raw, str):
        try:
            outcomes_raw = json.loads(outcomes_raw)
        except Exception:
            outcomes_raw = []
    # find YES index
    for i, o in enumerate(outcomes_raw):
        if str(o).lower() in ("yes", "true", "1") and i < len(outcome_prices_raw):
            try:
                return float(outcome_prices_raw[i])
            except (ValueError, TypeError):
                return None
    # fallback to index 0
    if outcome_prices_raw:
        try:
            return float(outcome_prices_raw[0])
        except (ValueError, TypeError):
            pass
    return None


def _fetch_sports_events() -> dict[str, list[dict]]:
    """
    Fetch active sports events from Gamma /events endpoint.
    Returns dict: event_title → list of market dicts.
    The events endpoint already groups sub-markets by event.
    """
    baskets: dict[str, list[dict]] = {}
    offset = 0
    while True:
        params = {
            "active": "true",
            "tag_slug": "sports",
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        try:
            resp = requests.get(f"{GAMMA_BASE}/events", params=params, timeout=30)
            resp.raise_for_status()
            page = resp.json()
        except Exception as e:
            logger.warning(f"Gamma events fetch failed at offset={offset}: {e}")
            break
        if not page:
            break
        for event in page:
            title = (event.get("title") or "").strip()
            markets = event.get("markets") or []
            if title and markets:
                baskets[title] = markets
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.1)

    n_markets = sum(len(v) for v in baskets.values())
    logger.info(f"Fetched {len(baskets)} sports events ({n_markets} markets) from Gamma")
    return baskets


# ── Mode B: internal inconsistency ───────────────────────────────────────────

def _compute_mode_b(
    basket: list[dict],
    team_home: Optional[str],
    team_away: Optional[str],
) -> tuple[int, int]:
    """
    Check semantic pair rules within the basket.
    Returns (n_pairs_checked, n_contradicting_pairs).
    """
    # Build index: market_type → list of (poly_price, team_direction)
    typed: dict[str, list[tuple[float, Optional[str]]]] = {}
    for m in basket:
        mtype = _classify_market_type(m.get("question", ""))
        price = _get_yes_price(m)
        if price is None:
            continue
        direction = _extract_team_direction(m.get("question", ""), team_home, team_away)
        typed.setdefault(mtype, []).append((price, direction))

    n_checked = 0
    n_contradicting = 0

    for type_a, type_b in _SEMANTIC_PAIRS:
        if type_a not in typed or type_b not in typed:
            continue

        prices_a = typed[type_a]
        prices_b = typed[type_b]

        # For each pair of (a, b) that refers to the same team
        for price_a, dir_a in prices_a:
            for price_b, dir_b in prices_b:
                # Only compare if we can confirm they refer to same team.
                # Skip if direction unknown for either — avoids cross-team noise.
                if dir_a is None or dir_b is None or dir_a != dir_b:
                    continue
                n_checked += 1

                # Rule: winner prob should be >= halftime/first_score prob for same team
                # Large violation = contradiction
                if type_a == "winner" and type_b in ("halftime", "first_score"):
                    if price_b > price_a + CONTRADICTION_THRESHOLD:
                        n_contradicting += 1

                # Rule: series win prob <= map win prob (bo3: win series is harder than win one map)
                elif type_a == "series" and type_b == "map":
                    if price_a > price_b + CONTRADICTION_THRESHOLD:
                        n_contradicting += 1

    return n_checked, n_contradicting


# ── Resolution reconciliation ─────────────────────────────────────────────────

def _reconcile_resolved(conn) -> int:
    """Check recently closed markets and record outcomes."""
    # Get markets we have snapshots for but no resolution yet
    rows = conn.execute(
        """SELECT DISTINCT s.market_id, s.event_id
           FROM sp_snapshots s
           LEFT JOIN sp_resolved r ON s.market_id = r.market_id
           WHERE r.market_id IS NULL
           ORDER BY s.ts DESC
           LIMIT 200"""
    ).fetchall()

    if not rows:
        return 0

    n = 0
    now = time.time()
    for row in rows:
        market_id = row["market_id"]
        try:
            resp = requests.get(
                f"{GAMMA_BASE}/markets/{market_id}",
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            continue

        if not (data.get("closed") or data.get("resolved")):
            continue

        # Get outcome
        outcome_prices_raw = data.get("outcomePrices", "[]")
        if isinstance(outcome_prices_raw, str):
            try:
                outcome_prices_raw = json.loads(outcome_prices_raw)
            except Exception:
                outcome_prices_raw = []
        if len(outcome_prices_raw) < 1:
            continue
        try:
            yes_price = float(outcome_prices_raw[0])
        except (ValueError, TypeError):
            continue
        outcome = 1 if yes_price >= 0.99 else 0

        # Get last snapshot before expiry
        end_ts = _parse_date(data.get("endDate") or data.get("endDateIso")) or now
        last_snap = conn.execute(
            """SELECT mode_a_gap, poly_price, external_implied
               FROM sp_snapshots
               WHERE market_id=? AND ts <= ?
               ORDER BY ts DESC LIMIT 1""",
            (market_id, end_ts),
        ).fetchone()
        if not last_snap:
            continue

        gap = last_snap["mode_a_gap"]
        poly = last_snap["poly_price"]
        ext = last_snap["external_implied"]

        mode_a_correct = None
        if gap is not None:
            if gap > 0:
                mode_a_correct = 1 if outcome == 1 else 0
            elif gap < 0:
                mode_a_correct = 1 if outcome == 0 else 0

        resolved_ts = _parse_date(
            data.get("resolvedAt") or data.get("resolutionTime")
        ) or now

        try:
            conn.execute(
                """INSERT OR REPLACE INTO sp_resolved
                   (market_id, resolved_ts, outcome,
                    last_mode_a_gap, last_poly_price, last_external_implied,
                    mode_a_was_correct)
                   VALUES (?,?,?,?,?,?,?)""",
                (market_id, resolved_ts, outcome, gap, poly, ext, mode_a_correct),
            )
            n += 1
        except Exception as e:
            logger.warning(f"Failed to write resolution for {market_id}: {e}")

        time.sleep(0.1)

    return n


# ── Main cycle ────────────────────────────────────────────────────────────────

def run_once() -> dict:
    """Run one observation cycle. Returns summary dict."""
    init_sports()
    conn = conn_sports()
    now_ts = time.time()

    # Fetch external odds (all supported sports in one shot)
    external_by_sport = fetch_all_sports()
    # Build flat list per external_id for matching
    all_external: list[dict] = []
    for games in external_by_sport.values():
        all_external.extend(games)
    mode_a_available = bool(all_external)
    if not mode_a_available:
        logger.info("No external odds fetched (ODDS_API_KEY absent or API error) — Mode B only")

    # Fetch open sports events (already grouped by event)
    baskets = _fetch_sports_events()
    if not baskets:
        logger.warning("No sports events fetched")
        conn.close()
        return {"markets": 0, "baskets": 0, "snapshots": 0, "reconciled": 0}
    total_markets = sum(len(v) for v in baskets.values())

    n_snapshots = 0
    n_matched   = 0
    n_baskets   = 0

    for event_title, basket_markets in baskets.items():
        if len(basket_markets) < MIN_BASKET_SIZE:
            continue

        # Use end_date of first market as proxy for game time
        end_ts = None
        for m in basket_markets:
            end_ts = _parse_date(m.get("endDate") or m.get("endDateIso"))
            if end_ts:
                break
        if not end_ts:
            continue

        # Layer 1: match to external
        match = match_event(event_title, end_ts, all_external)
        persist_attempt(conn, match, now_ts)
        if match.accepted:
            persist_event(conn, match, now_ts)
            n_matched += 1

        event_id = match.event_id
        team_home = match.external_home
        team_away = match.external_away

        # Layer 2: per-market snapshots
        basket_mode_a_gaps: list[float] = []
        n_with_external = 0

        for m in basket_markets:
            market_id = str(m.get("id", ""))
            if not market_id:
                continue

            poly_price = _get_yes_price(m)
            mtype = _classify_market_type(m.get("question", ""))
            tte_hours = (end_ts - now_ts) / 3600.0 if end_ts else None

            # Mode A: external anchor
            mode_a_gap = None
            external_implied = None
            external_fetch_ts = None
            quote_age = None

            if match.accepted and mtype == "winner":
                # Map to external implied: which team is the YES side?
                direction = _extract_team_direction(
                    m.get("question", ""), team_home, team_away
                )
                if direction == "home" and match.home_implied is not None:
                    external_implied = match.home_implied
                elif direction == "away" and match.away_implied is not None:
                    external_implied = match.away_implied

                if external_implied is not None and poly_price is not None:
                    mode_a_gap = external_implied - poly_price
                    basket_mode_a_gaps.append(mode_a_gap)
                    n_with_external += 1

                external_fetch_ts = match.fetch_ts
                if external_fetch_ts:
                    quote_age = now_ts - external_fetch_ts

            conn.execute(
                """INSERT INTO sp_snapshots
                   (market_id, event_id, ts, market_type, poly_price,
                    external_implied, mode_a_gap,
                    external_fetch_ts, external_quote_age_s, tte_hours)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    market_id, event_id, now_ts, mtype, poly_price,
                    external_implied, mode_a_gap,
                    external_fetch_ts, quote_age, tte_hours,
                ),
            )
            n_snapshots += 1

        # Layer 3: basket summary
        n_aligned_a = sum(1 for g in basket_mode_a_gaps if abs(g) > MODE_A_GAP_THRESH)
        avg_gap_a = (sum(basket_mode_a_gaps) / len(basket_mode_a_gaps)) if basket_mode_a_gaps else None

        # Mode B
        n_pairs, n_contra = _compute_mode_b(basket_markets, team_home, team_away)
        mode_b_score = (n_contra / n_pairs) if n_pairs > 0 else None

        conn.execute(
            """INSERT INTO sp_baskets
               (event_id, ts, n_markets,
                n_with_external, n_aligned_mode_a, avg_gap_mode_a,
                n_semantic_pairs_checked, n_contradicting_pairs, mode_b_score)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                event_id, now_ts, len(basket_markets),
                n_with_external, n_aligned_a, avg_gap_a,
                n_pairs, n_contra, mode_b_score,
            ),
        )
        n_baskets += 1

        # Log interesting baskets
        if n_aligned_a >= 2:
            logger.info(
                f"MODE-A ALERT {event_title!r} "
                f"aligned={n_aligned_a}/{n_with_external} avg_gap={avg_gap_a:+.3f}"
            )
        if mode_b_score is not None and mode_b_score >= 0.5 and n_pairs >= 2:
            logger.info(
                f"MODE-B ALERT {event_title!r} "
                f"contradictions={n_contra}/{n_pairs} score={mode_b_score:.2f}"
            )

    # Reconcile resolved
    n_reconciled = _reconcile_resolved(conn)

    conn.commit()
    conn.close()

    summary = {
        "markets":    total_markets,
        "baskets":    n_baskets,
        "snapshots":  n_snapshots,
        "matched":    n_matched,
        "reconciled": n_reconciled,
        "mode_a":     mode_a_available,
        "ts":         int(now_ts),
    }
    logger.info(
        f"Cycle done: {total_markets} markets, {n_baskets} baskets, "
        f"{n_snapshots} snapshots, matched={n_matched}, reconciled={n_reconciled}"
    )
    return summary


def run_loop() -> None:
    """Run observation loop forever."""
    logger.info(f"Sports observer started | interval={POLL_INTERVAL_SEC}s")
    while True:
        try:
            run_once()
        except Exception as e:
            logger.error(f"Observer cycle error: {e}", exc_info=True)
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run_loop()
