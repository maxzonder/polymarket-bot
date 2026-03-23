"""
v2 smoke test — validates API connectivity and DB init without running full loops.

Usage:
    cd /home/polybot/claude-polymarket
    python -m v2.smoke_test
"""
from __future__ import annotations

import sys
import time

print("=== v2 Smoke Test ===")
errors: list[str] = []


# ── 1. DB init ────────────────────────────────────────────────────────────────
print("\n[1] DB init...")
try:
    from v2.db import init_negrisk, init_crypto
    init_negrisk()
    init_crypto()
    print("    OK — obs_negrisk.db and obs_crypto.db initialised")
except Exception as e:
    errors.append(f"DB init: {e}")
    print(f"    FAIL: {e}")


# ── 2. Gamma API — neg-risk markets ──────────────────────────────────────────
print("\n[2] Gamma API — neg_risk markets (first page only)...")
try:
    import requests
    resp = requests.get(
        "https://gamma-api.polymarket.com/markets",
        params={"closed": "false", "active": "true", "neg_risk": "true", "limit": 5},
        timeout=15,
    )
    resp.raise_for_status()
    page = resp.json()
    print(f"    OK — got {len(page)} neg_risk markets")
    if page:
        m = page[0]
        slug = None
        evts = m.get("events") or []
        if evts and isinstance(evts[0], dict):
            slug = evts[0].get("slug")
        print(f"    Sample: id={m.get('id')} slug={slug or m.get('eventSlug', 'n/a')}")
except Exception as e:
    errors.append(f"Gamma neg_risk: {e}")
    print(f"    FAIL: {e}")


# ── 3. Gamma API — crypto markets ─────────────────────────────────────────────
print("\n[3] Gamma API — crypto markets (first page only)...")
try:
    resp = requests.get(
        "https://gamma-api.polymarket.com/markets",
        params={"closed": "false", "active": "true", "tag_slug": "crypto", "limit": 5},
        timeout=15,
    )
    resp.raise_for_status()
    page = resp.json()
    print(f"    OK — got {len(page)} crypto markets")
    if page:
        print(f"    Sample: {page[0].get('question', '')[:60]}")
except Exception as e:
    errors.append(f"Gamma crypto: {e}")
    print(f"    FAIL: {e}")


# ── 4. CLOB orderbook ─────────────────────────────────────────────────────────
print("\n[4] CLOB — sample orderbook...")
try:
    # Use a well-known long-lived token as a probe
    # Will Trump win the 2024 election YES token (should have history)
    PROBE_TOKEN = "21742633143463906290569050155826241533067272736897614950488156847949938836455"
    resp = requests.get(
        "https://clob.polymarket.com/book",
        params={"token_id": PROBE_TOKEN},
        timeout=10,
    )
    resp.raise_for_status()
    book = resp.json()
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    print(f"    OK — bids={len(bids)} asks={len(asks)}")
    if asks:
        print(f"    Best ask: {asks[0]['price']}")
except Exception as e:
    # CLOB may 404 for resolved markets — not a hard failure
    print(f"    NOTE: {e} (market may be resolved — not critical)")


# ── 5. CoinGecko spot ─────────────────────────────────────────────────────────
print("\n[5] CoinGecko — spot prices...")
try:
    resp = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={"ids": "bitcoin,ethereum,solana", "vs_currencies": "usd"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    btc = data.get("bitcoin", {}).get("usd")
    eth = data.get("ethereum", {}).get("usd")
    sol = data.get("solana", {}).get("usd")
    print(f"    OK — BTC=${btc:,.0f}  ETH=${eth:,.0f}  SOL=${sol:.2f}")
except Exception as e:
    errors.append(f"CoinGecko spot: {e}")
    print(f"    FAIL: {e}")


# ── 6. Question parser ────────────────────────────────────────────────────────
print("\n[6] Crypto question parser...")
try:
    from v2.observers.crypto import _parse_question
    tests = [
        ("Will BTC be above $100,000 by end of 2025?", "BTC", "above", 100000.0),
        ("Will ETH drop below $1,500 before June?",    "ETH", "below", 1500.0),
        ("Will SOL reach $500 in 2025?",               "SOL", "above", 500.0),
    ]
    for q, exp_asset, exp_dir, exp_thresh in tests:
        p = _parse_question(q)
        if p and p.asset == exp_asset and p.direction == exp_dir and abs(p.threshold - exp_thresh) < 1:
            print(f"    OK: {q[:50]}")
        else:
            got = f"{p}" if p else "None"
            errors.append(f"Parser: '{q[:50]}' → {got}")
            print(f"    FAIL: {q[:50]} → {got}")
except Exception as e:
    errors.append(f"Parser import: {e}")
    print(f"    FAIL: {e}")


# ── 7. Neg-risk clusterer ──────────────────────────────────────────────────────
print("\n[7] Neg-risk clusterer (live data)...")
try:
    from v2.observers.negrisk import _fetch_neg_risk_markets, _cluster_by_event
    markets = _fetch_neg_risk_markets()
    groups = _cluster_by_event(markets)
    multi = {k: v for k, v in groups.items() if len(v) >= 2}
    print(f"    OK — {len(markets)} markets → {len(groups)} slugs → {len(multi)} groups with ≥2 legs")
    if multi:
        slug, ms = next(iter(multi.items()))
        print(f"    Sample group: '{slug[:40]}' ({len(ms)} markets)")
except Exception as e:
    errors.append(f"NegRisk clusterer: {e}")
    print(f"    FAIL: {e}")


# ── Summary ────────────────────────────────────────────────────────────────────
print("\n" + "=" * 40)
if errors:
    print(f"FAILED ({len(errors)} error(s)):")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)
else:
    print("ALL CHECKS PASSED")
    sys.exit(0)
