"""
Crypto Threshold Observer — Phase 1B.

Every 30 minutes:
  1. Fetch all active crypto markets (BTC/ETH/SOL threshold questions) from Gamma API
  2. Parse question to extract: asset, threshold, direction (above/below), expiry
  3. Fetch current spot price from CoinGecko (free, no auth)
  4. Compute Black-Scholes-style model price using realised vol (30-day)
  5. Compute gap = model_price - polymarket_price
  6. Persist snapshot to obs_crypto.db
  7. Track resolved outcomes to validate model directional accuracy

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

POLL_INTERVAL_SEC = 1800   # 30 minutes
PAGE_SIZE         = 500
GARBAGE_TIME_PCT  = 0.02   # ignore last 2% of market duration

# CoinGecko asset IDs
ASSET_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
}

# Regex patterns to parse crypto threshold questions
# "Will BTC hit $100,000 by Dec 31?"  /  "Will ETH be above $3,000 on..."
_PATTERNS = [
    # "Will X be above/below $Y by/on ..."  or  "Will ETH drop below $1,500 ..."
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
            direction_raw = "above"   # default for "reach/hit"

        asset = _ASSET_ALIASES.get(asset_raw.lower())
        if not asset:
            continue

        try:
            threshold = float(price_raw.replace(",", ""))
        except ValueError:
            continue

        dir_lower = direction_raw.lower().strip()
        if any(w in dir_lower for w in _ABOVE_WORDS):
            direction = "above"
        elif any(w in dir_lower for w in _BELOW_WORDS):
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


def _fetch_crypto_markets() -> list[dict]:
    """Fetch active crypto markets from Gamma API (BTC/ETH/SOL threshold questions)."""
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
        result = {}
        for asset, cg_id in ASSET_IDS.items():
            result[asset] = data.get(cg_id, {}).get("usd")
        return result
    except Exception as e:
        logger.warning(f"CoinGecko spot fetch failed: {e}")
        return {asset: None for asset in ASSET_IDS}


def _get_realised_vol(asset: str) -> Optional[float]:
    """
    Fetch 30-day daily closes from CoinGecko and compute annualised realised vol.
    Returns annualised vol as a decimal (e.g. 0.65 = 65%).
    """
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
        daily_vol = math.sqrt(variance)
        return daily_vol * math.sqrt(365)
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
    Assumes zero drift (risk-neutral martingale approximation).
    """
    if tte_years <= 0 or vol <= 0 or spot <= 0 or strike <= 0:
        return None
    try:
        d2 = (math.log(spot / strike)) / (vol * math.sqrt(tte_years))
        # N(d2) via error function
        def norm_cdf(x: float) -> float:
            return 0.5 * (1 + math.erf(x / math.sqrt(2)))

        if direction == "above":
            return norm_cdf(d2)
        else:
            return norm_cdf(-d2)
    except Exception:
        return None


def _get_token_price(token_id: str) -> Optional[float]:
    """Get best_ask from CLOB for YES token."""
    try:
        resp = requests.get(
            f"{CLOB_BASE}/book",
            params={"token_id": token_id},
            timeout=10,
        )
        resp.raise_for_status()
        book = resp.json()
        asks = book.get("asks") or []
        if asks:
            return float(asks[0]["price"])
    except Exception:
        pass
    return None


def _parse_end_date(m: dict) -> Optional[int]:
    """Parse endDate or endDateIso to unix ts."""
    raw = m.get("endDate") or m.get("endDateIso")
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


def _upsert_market(conn, m: dict, parsed: Optional[ParsedQuestion], token_id: str) -> None:
    """Insert or update market record."""
    now = int(time.time())
    end_ts = _parse_end_date(m)
    conn.execute(
        """INSERT INTO cr_markets
           (market_id, token_id, question, asset, threshold, direction,
            expiry_ts, parse_template, parse_ok, first_seen_ts)
           VALUES (?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(market_id) DO UPDATE SET
               token_id=excluded.token_id,
               expiry_ts=COALESCE(excluded.expiry_ts, expiry_ts),
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
            parsed.template if parsed else "other",
            1 if parsed else 0,
            now,
        ),
    )


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


def run_once() -> dict:
    """Run one observation cycle. Returns summary dict."""
    init_crypto()
    markets = _fetch_crypto_markets()
    if not markets:
        logger.warning("No crypto markets fetched")
        return {"markets": 0, "snapshots": 0, "parsed": 0}

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

        # Pick YES token (index 0)
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

        # Expiry / TTE
        end_ts = _parse_end_date(m)
        if end_ts is None:
            continue
        tte_hours = (end_ts - now_ts) / 3600.0
        if tte_hours <= 0:
            continue

        # Garbage time check — skip last 2% of market duration
        # We need to know total duration. Approximate from first_seen_ts vs end_ts.
        row = conn.execute(
            "SELECT first_seen_ts FROM cr_markets WHERE market_id=?", (market_id,)
        ).fetchone()
        garbage_time = False
        if row and row["first_seen_ts"]:
            total_sec = end_ts - row["first_seen_ts"]
            remaining_sec = end_ts - now_ts
            if total_sec > 0 and remaining_sec / total_sec < GARBAGE_TIME_PCT:
                garbage_time = True

        # Spot + vol (cached per asset per cycle)
        spot = spot_prices.get(parsed.asset)
        if parsed.asset not in vols:
            vols[parsed.asset] = _get_realised_vol(parsed.asset)
            time.sleep(0.5)  # gentle rate limit
        vol = vols[parsed.asset]

        # Polymarket price
        poly_price = _get_token_price(token_id)
        time.sleep(0.05)

        # Model price
        model_price = None
        if spot and vol and tte_hours > 0:
            model_price = _bs_binary_price(
                spot=spot,
                strike=parsed.threshold,
                tte_years=tte_hours / 8760.0,
                vol=vol,
                direction=parsed.direction,
            )

        _save_snapshot(
            conn, market_id, poly_price, model_price,
            tte_hours, spot, vol, garbage_time,
        )
        n_snapshots += 1

        if model_price is not None and poly_price is not None:
            gap = model_price - poly_price
            if abs(gap) > 0.05:
                logger.info(
                    f"GAP {parsed.asset} {parsed.direction} ${parsed.threshold:,.0f} "
                    f"poly={poly_price:.3f} model={model_price:.3f} gap={gap:+.3f} "
                    f"tte={tte_hours:.1f}h"
                )

    conn.commit()
    conn.close()

    summary = {
        "markets": len(markets),
        "parsed": n_parsed,
        "snapshots": n_snapshots,
        "ts": int(time.time()),
    }
    logger.info(
        f"Cycle done: {len(markets)} markets, {n_parsed} parsed, {n_snapshots} snapshots"
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
