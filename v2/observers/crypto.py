"""
Crypto Threshold Observer — Phase 1B.

Every 30 minutes:
  1. Fetch all active crypto markets (BTC/ETH/SOL threshold questions) from Gamma API
  2. Parse question to extract: asset, threshold, direction (above/below), expiry
  3. Fetch current spot price from CoinGecko (free, no auth)
  4. Compute Black-Scholes-style model price using realised vol (30-day)
  5. Compute gap = model_price - polymarket_price
  6. Persist snapshot to obs_crypto.db
  7. Reconcile resolved markets → cr_resolved + was_directionally_correct

Parser note: question parsing uses a regex baseline intentionally for Phase 1.
Regex gives high precision (no false positives in observation data) at the cost
of recall (some valid markets won't be parsed). An LLM-based parser for broader
recall is planned for Phase 2.

Garbage time: computed from real market start_ts (startDate from Gamma),
NOT from first_seen_ts. Falls back to first_seen_ts only if startDate absent.

No trading. Pure observation.
"""
from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests

from ..utils.paths import CRYPTO_DB, ensure_dirs
from ..utils.logger import setup_logger
from ..db import init_crypto, conn_crypto

logger = setup_logger("crypto_observer")

GAMMA_BASE      = "https://gamma-api.polymarket.com"
COINGECKO_BASE  = "https://api.coingecko.com/api/v3"
CLOB_BASE       = "https://clob.polymarket.com"

POLL_INTERVAL_SEC    = 1800   # 30 minutes
PAGE_SIZE            = 500
GARBAGE_TIME_PCT     = 0.02   # ignore last 2% of market duration
RESOLUTION_GRACE_SEC = 3600   # wait 1h after expiry before checking resolved

# CoinGecko asset IDs
ASSET_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
}

# Regex baseline parser — Phase 1. High precision, lower recall by design.
_PATTERNS = [
    # "Will X be above/below $Y ..."  or  "Will ETH drop below $1,500 ..."
    re.compile(
        r"will\s+(btc|eth|sol|bitcoin|ethereum|solana)\s+(?:be\s+)?"
        r"(above|below|reach|hit|exceed|"
        r"drop\s+below|drop\s+to|fall\s+below|fall\s+to|"
        r"rise\s+to|rise\s+above|"
        r"trade\s+above|trade\s+below|"
        r"close\s+above|close\s+below)"
        r"\s+\$?([\d,]+(?:\.\d+)?)",
        re.IGNORECASE,
    ),
    # "Will BTC reach $X by ..."
    re.compile(
        r"(btc|eth|sol|bitcoin|ethereum|solana)\s+(?:above|below|over|under|reach|hit|exceed)\s+\$?([\d,]+(?:\.\d+)?)",
        re.IGNORECASE,
    ),
]

_ASSET_ALIASES = {
    "bitcoin": "BTC", "btc": "BTC",
    "ethereum": "ETH", "eth": "ETH",
    "solana": "SOL", "sol": "SOL",
}

_ABOVE_WORDS = {"above", "reach", "hit", "exceed", "rise to", "rise above", "trade above", "close above", "over"}
_BELOW_WORDS = {"below", "drop to", "drop below", "fall to", "fall below", "trade below", "close below", "under"}


@dataclass
class ParsedQuestion:
    asset: str       # BTC / ETH / SOL
    threshold: float
    direction: str   # above / below
    template: str    # rise_to / dip_to / other


def _parse_question(question: str) -> Optional[ParsedQuestion]:
    """Extract asset, threshold, direction from market question text."""
    q = question.strip()

    for pat in _PATTERNS:
        m = pat.search(q)
        if not m:
            continue
        groups = m.groups()
        if len(groups) == 3:
            asset_raw, direction_raw, price_raw = groups
        else:
            asset_raw, price_raw = groups[0], groups[1]
            direction_raw = "above"

        asset = _ASSET_ALIASES.get(asset_raw.lower())
        if not asset:
            continue

        try:
            threshold = float(price_raw.replace(",", ""))
        except ValueError:
            continue

        dir_lower = direction_raw.lower().strip()
        if any(w in dir_lower for w in _BELOW_WORDS):
            direction = "below"
        else:
            direction = "above"

        template = "rise_to" if direction == "above" else "dip_to"

        return ParsedQuestion(
            asset=asset,
            threshold=threshold,
            direction=direction,
            template=template,
        )

    return None


def _parse_date_field(raw: Optional[str]) -> Optional[int]:
    """Parse ISO date string to unix timestamp."""
    if not raw:
        return None
    try:
        ed = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ed)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def _fetch_crypto_markets() -> list[dict]:
    """Fetch active crypto markets from Gamma API."""
    markets = []
    offset = 0
    while True:
        params = {
            "closed": "false",
            "active": "true",
            "include_tag": "true",
            "tag_slug": "crypto",
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

    logger.info(f"Fetched {len(markets)} crypto markets from Gamma")
    return markets


def _get_spot_prices() -> dict[str, Optional[float]]:
    """Fetch current spot prices from CoinGecko."""
    ids = ",".join(ASSET_IDS.values())
    try:
        resp = requests.get(
            f"{COINGECKO_BASE}/simple/price",
            params={"ids": ids, "vs_currencies": "usd"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return {asset: data.get(cg_id, {}).get("usd") for asset, cg_id in ASSET_IDS.items()}
    except Exception as e:
        logger.warning(f"CoinGecko spot fetch failed: {e}")
        return {asset: None for asset in ASSET_IDS}


def _get_realised_vol(asset: str) -> Optional[float]:
    """30-day annualised realised vol from CoinGecko daily closes."""
    cg_id = ASSET_IDS.get(asset)
    if not cg_id:
        return None
    try:
        resp = requests.get(
            f"{COINGECKO_BASE}/coins/{cg_id}/market_chart",
            params={"vs_currency": "usd", "days": "30", "interval": "daily"},
            timeout=15,
        )
        resp.raise_for_status()
        prices = [p[1] for p in resp.json().get("prices", [])]
        if len(prices) < 5:
            return None
        log_returns = [math.log(prices[i] / prices[i - 1]) for i in range(1, len(prices))]
        n = len(log_returns)
        mean = sum(log_returns) / n
        variance = sum((r - mean) ** 2 for r in log_returns) / (n - 1)
        return math.sqrt(variance) * math.sqrt(365)
    except Exception as e:
        logger.warning(f"CoinGecko vol fetch failed for {asset}: {e}")
        return None


def _bs_binary_price(
    spot: float,
    strike: float,
    tte_years: float,
    vol: float,
    direction: str,
) -> Optional[float]:
    """
    Black-Scholes price for a binary (digital) option.
    direction='above' → P(S_T > K) = N(d2)
    direction='below' → P(S_T < K) = N(-d2)
    Zero-drift (risk-neutral) approximation.
    """
    if tte_years <= 0 or vol <= 0 or spot <= 0 or strike <= 0:
        return None
    try:
        d2 = math.log(spot / strike) / (vol * math.sqrt(tte_years))
        def norm_cdf(x: float) -> float:
            return 0.5 * (1 + math.erf(x / math.sqrt(2)))
        return norm_cdf(d2) if direction == "above" else norm_cdf(-d2)
    except Exception:
        return None


def _get_token_price(token_id: str) -> Optional[float]:
    """Get best_ask from CLOB for YES token."""
    try:
        resp = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=10)
        resp.raise_for_status()
        book = resp.json()
        asks = book.get("asks") or []
        if asks:
            return float(asks[0]["price"])
    except Exception:
        pass
    return None


def _upsert_market(conn, m: dict, parsed: Optional[ParsedQuestion], token_id: str) -> None:
    """Insert or update market record, storing real start_ts from Gamma."""
    now = int(time.time())
    end_ts   = _parse_date_field(m.get("endDate") or m.get("endDateIso"))
    start_ts = _parse_date_field(m.get("startDate") or m.get("startDateIso"))

    conn.execute(
        """INSERT INTO cr_markets
           (market_id, token_id, question, asset, threshold, direction,
            expiry_ts, start_ts, parse_template, parse_ok, first_seen_ts)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(market_id) DO UPDATE SET
               token_id=excluded.token_id,
               expiry_ts=COALESCE(excluded.expiry_ts, expiry_ts),
               start_ts=COALESCE(excluded.start_ts, start_ts),
               parse_ok=excluded.parse_ok
        """,
        (
            str(m["id"]),
            token_id,
            m.get("question", ""),
            parsed.asset if parsed else None,
            parsed.threshold if parsed else None,
            parsed.direction if parsed else None,
            end_ts,
            start_ts,
            parsed.template if parsed else "other",
            1 if parsed else 0,
            now,
        ),
    )


def _is_garbage_time(conn, market_id: str, end_ts: int, now_ts: float) -> bool:
    """
    Return True if we are in the last GARBAGE_TIME_PCT of market lifetime.

    Uses real market start_ts (startDate from Gamma) for total duration.
    Falls back to first_seen_ts only when startDate was absent.
    """
    row = conn.execute(
        "SELECT start_ts, first_seen_ts FROM cr_markets WHERE market_id=?",
        (market_id,),
    ).fetchone()
    if not row:
        return False

    market_start = row["start_ts"] or row["first_seen_ts"]
    if not market_start:
        return False

    total_sec     = end_ts - market_start
    remaining_sec = end_ts - now_ts
    if total_sec <= 0:
        return False
    return (remaining_sec / total_sec) < GARBAGE_TIME_PCT


def _save_snapshot(
    conn,
    market_id: str,
    poly_price: Optional[float],
    model_price: Optional[float],
    tte_hours: Optional[float],
    spot: Optional[float],
    vol: Optional[float],
    garbage_time: bool,
) -> None:
    gap = None
    if poly_price is not None and model_price is not None:
        gap = model_price - poly_price

    conn.execute(
        """INSERT INTO cr_snapshots
           (market_id, ts, polymarket_price, model_price, gap,
            tte_hours, spot_price, vol_estimate, in_garbage_time)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            market_id,
            int(time.time()),
            poly_price,
            model_price,
            gap,
            tte_hours,
            spot,
            vol,
            1 if garbage_time else 0,
        ),
    )


def _reconcile_resolved(conn) -> int:
    """
    Check expired markets in cr_markets and reconcile resolved outcomes.

    For each market past its expiry (+ grace period) with no resolved_ts:
      1. Hit Gamma API to confirm resolved and read outcome
      2. Find last snapshot before expiry
      3. Insert into cr_resolved with was_directionally_correct
      4. Update cr_markets.resolved_ts and outcome

    Returns number of markets reconciled.
    """
    now = int(time.time())
    cutoff = now - RESOLUTION_GRACE_SEC

    candidates = conn.execute(
        """SELECT market_id, token_id, asset, threshold, direction, expiry_ts
           FROM cr_markets
           WHERE expiry_ts IS NOT NULL
             AND expiry_ts < ?
             AND resolved_ts IS NULL
             AND parse_ok = 1""",
        (cutoff,),
    ).fetchall()

    n_reconciled = 0
    for row in candidates:
        market_id = row["market_id"]
        try:
            resp = requests.get(
                f"{GAMMA_BASE}/markets/{market_id}",
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                data = data[0] if data else {}
        except Exception as e:
            logger.debug(f"Gamma fetch for resolved {market_id}: {e}")
            continue

        # Must be closed/resolved
        if not (data.get("closed") or data.get("resolved")):
            continue

        # Determine YES/NO outcome from outcomePrices
        outcome_prices_raw = data.get("outcomePrices", "[]")
        if isinstance(outcome_prices_raw, str):
            try:
                outcome_prices_raw = json.loads(outcome_prices_raw)
            except Exception:
                outcome_prices_raw = []

        if len(outcome_prices_raw) < 2:
            continue

        try:
            yes_price = float(outcome_prices_raw[0])
        except (ValueError, TypeError):
            continue

        # YES wins if its final price is 1 (or >= 0.99 accounting for rounding)
        outcome = 1 if yes_price >= 0.99 else 0

        # Find last snapshot before expiry
        last_snap = conn.execute(
            """SELECT polymarket_price, model_price, gap
               FROM cr_snapshots
               WHERE market_id=? AND ts <= ?
               ORDER BY ts DESC LIMIT 1""",
            (market_id, row["expiry_ts"]),
        ).fetchone()

        if not last_snap:
            continue

        gap = last_snap["gap"]
        poly_price = last_snap["polymarket_price"]
        model_price = last_snap["model_price"]

        # Directionally correct: gap > 0 means model said price higher than poly,
        # i.e. model predicted YES more likely → correct if YES won (outcome=1)
        was_correct = None
        if gap is not None:
            if gap > 0:
                was_correct = 1 if outcome == 1 else 0
            elif gap < 0:
                was_correct = 1 if outcome == 0 else 0
            else:
                was_correct = None  # gap=0, indeterminate

        resolved_ts = _parse_date_field(
            data.get("resolvedAt") or data.get("resolutionTime")
        ) or now

        try:
            conn.execute(
                """INSERT OR REPLACE INTO cr_resolved
                   (market_id, resolved_ts, outcome, last_gap,
                    last_polymarket_price, last_model_price, was_directionally_correct)
                   VALUES (?,?,?,?,?,?,?)""",
                (market_id, resolved_ts, outcome, gap,
                 poly_price, model_price, was_correct),
            )
            conn.execute(
                "UPDATE cr_markets SET resolved_ts=?, outcome=? WHERE market_id=?",
                (resolved_ts, outcome, market_id),
            )
            n_reconciled += 1
            logger.info(
                f"RESOLVED {market_id} outcome={'YES' if outcome else 'NO'} "
                f"gap={gap:+.3f} correct={was_correct}"
            )
        except Exception as e:
            logger.warning(f"Failed to write resolution for {market_id}: {e}")

        time.sleep(0.1)

    return n_reconciled


def run_once() -> dict:
    """Run one observation cycle. Returns summary dict."""
    init_crypto()
    markets = _fetch_crypto_markets()
    if not markets:
        logger.warning("No crypto markets fetched")
        return {"markets": 0, "snapshots": 0, "parsed": 0, "reconciled": 0}

    spot_prices = _get_spot_prices()
    vols: dict[str, Optional[float]] = {}

    conn = conn_crypto()
    now_ts = time.time()
    n_snapshots = 0
    n_parsed = 0

    for m in markets:
        market_id = str(m.get("id", ""))
        if not market_id:
            continue

        question = m.get("question", "")
        parsed = _parse_question(question)

        # YES token = index 0
        token_ids_raw = m.get("clobTokenIds", "[]")
        if isinstance(token_ids_raw, str):
            try:
                token_ids_raw = json.loads(token_ids_raw)
            except Exception:
                token_ids_raw = []
        token_id = str(token_ids_raw[0]) if token_ids_raw else ""
        if not token_id:
            continue

        _upsert_market(conn, m, parsed, token_id)

        if not parsed:
            continue
        n_parsed += 1

        end_ts = _parse_date_field(m.get("endDate") or m.get("endDateIso"))
        if end_ts is None:
            continue
        tte_hours = (end_ts - now_ts) / 3600.0
        if tte_hours <= 0:
            continue

        garbage_time = _is_garbage_time(conn, market_id, end_ts, now_ts)

        spot = spot_prices.get(parsed.asset)
        if parsed.asset not in vols:
            vols[parsed.asset] = _get_realised_vol(parsed.asset)
            time.sleep(0.5)
        vol = vols[parsed.asset]

        poly_price = _get_token_price(token_id)
        time.sleep(0.05)

        model_price = None
        if spot and vol and tte_hours > 0:
            model_price = _bs_binary_price(
                spot=spot,
                strike=parsed.threshold,
                tte_years=tte_hours / 8760.0,
                vol=vol,
                direction=parsed.direction,
            )

        _save_snapshot(conn, market_id, poly_price, model_price, tte_hours, spot, vol, garbage_time)
        n_snapshots += 1

        if model_price is not None and poly_price is not None:
            gap = model_price - poly_price
            if abs(gap) > 0.05:
                logger.info(
                    f"GAP {parsed.asset} {parsed.direction} ${parsed.threshold:,.0f} "
                    f"poly={poly_price:.3f} model={model_price:.3f} gap={gap:+.3f} "
                    f"tte={tte_hours:.1f}h"
                )

    # Resolution reconciliation
    n_reconciled = _reconcile_resolved(conn)

    conn.commit()
    conn.close()

    summary = {
        "markets": len(markets),
        "parsed": n_parsed,
        "snapshots": n_snapshots,
        "reconciled": n_reconciled,
        "ts": int(time.time()),
    }
    logger.info(
        f"Cycle done: {len(markets)} markets, {n_parsed} parsed, "
        f"{n_snapshots} snapshots, {n_reconciled} resolved"
    )
    return summary


def run_loop() -> None:
    """Run observation loop forever."""
    logger.info(f"Crypto observer started | interval={POLL_INTERVAL_SEC}s")
    while True:
        try:
            run_once()
        except Exception as e:
            logger.error(f"Observer cycle error: {e}", exc_info=True)
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run_loop()
