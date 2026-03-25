"""
Stage 0 — CLOB Pricing Validation (v1.1)

Validates the assumption: NO_price ≈ 1 − YES_price.

For N live markets, fetches:
  - YES price from Gamma (best_ask or last_trade_price)
  - NO token CLOB orderbook (best_ask, best_bid, spread)

Compares synthetic NO price (1 − YES) vs real CLOB NO best_ask.
Reports absolute deviation, relative deviation, and distribution.

Key question: is the synthetic price good enough for a screener, or does the
real NO best_ask deviate significantly (> 0.01 absolute) at low price levels?

Usage:
    python scripts/validate_clob_pricing.py
    python scripts/validate_clob_pricing.py --n 200 --price-max 0.10
    python scripts/validate_clob_pricing.py --all-prices
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests

from api.clob_client import get_orderbook
from api.gamma_client import fetch_open_markets, PAGE_SIZE, GAMMA_BASE, DEFAULT_TIMEOUT
from utils.logger import setup_logger

logger = setup_logger("validate_clob_pricing")

SLEEP_CLOB = 0.15   # polite delay between CLOB requests


def _fetch_live_sample(n: int, price_max: float) -> list:
    """Fetch up to n open markets with YES price <= price_max."""
    logger.info(f"Fetching live markets (price_max={price_max}, target_n={n}) ...")

    now_ts = time.time()
    from api.gamma_client import _parse_market

    markets = []
    offset = 0
    pages_checked = 0

    # We want a random sample across different price levels, so we fetch broadly
    # and subsample. Use volume_min=50 to exclude trivially thin markets.
    while len(markets) < n * 5 and pages_checked < 30:
        params = {
            "closed": "false",
            "active": "true",
            "include_tag": "true",
            "volume_num_min": 50,
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        try:
            resp = requests.get(
                f"{GAMMA_BASE}/markets",
                params=params,
                timeout=DEFAULT_TIMEOUT,
            )
            resp.raise_for_status()
            page = resp.json()
        except Exception as e:
            logger.warning(f"Gamma page fetch failed: {e}")
            break

        if not page:
            break

        for raw in page:
            m = _parse_market(raw, now_ts)
            if m is None:
                continue
            if len(m.token_ids) < 2:
                continue  # need both YES and NO token_ids
            price = m.best_ask if m.best_ask is not None else m.last_trade_price
            if price is not None and 0 < price < price_max:
                markets.append(m)

        pages_checked += 1
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.05)

    logger.info(f"Collected {len(markets)} candidate markets, sampling {min(n, len(markets))}")
    if len(markets) > n:
        markets = random.sample(markets, n)
    return markets


def validate(n: int = 100, price_max: float = 0.10) -> None:
    markets = _fetch_live_sample(n, price_max)

    if not markets:
        logger.error("No live markets found — check network or Gamma API")
        return

    results = []
    errors = 0

    for i, mkt in enumerate(markets):
        yes_token_id = mkt.token_ids[0]
        no_token_id  = mkt.token_ids[1]

        yes_price = mkt.best_ask if mkt.best_ask is not None else mkt.last_trade_price
        if yes_price is None or yes_price <= 0 or yes_price >= 1:
            errors += 1
            continue

        synthetic_no = 1.0 - yes_price

        try:
            ob = get_orderbook(no_token_id)
            time.sleep(SLEEP_CLOB)
        except Exception as e:
            logger.debug(f"CLOB error for {no_token_id}: {e}")
            errors += 1
            continue

        real_no_ask = ob.best_ask
        real_no_bid = ob.best_bid
        spread      = ob.spread

        if real_no_ask is None:
            errors += 1
            continue

        abs_dev = abs(synthetic_no - real_no_ask)
        # relative deviation: how big is the error as fraction of the real price
        rel_dev = abs_dev / max(real_no_ask, 0.001)

        results.append({
            "market_id":     mkt.market_id,
            "question":      mkt.question[:60],
            "category":      mkt.category,
            "yes_price":     yes_price,
            "synthetic_no":  synthetic_no,
            "real_no_ask":   real_no_ask,
            "real_no_bid":   real_no_bid,
            "spread":        spread,
            "abs_dev":       abs_dev,
            "rel_dev":       rel_dev,
        })

        if (i + 1) % 20 == 0:
            logger.info(f"  Processed {i+1}/{len(markets)} ...")

    if not results:
        logger.error(f"No valid results (errors={errors}). Check CLOB API access.")
        return

    _print_report(results, errors)


def _print_report(results: list, errors: int) -> None:
    n = len(results)
    abs_devs = sorted(r["abs_dev"] for r in results)
    rel_devs = sorted(r["rel_dev"] for r in results)

    def _p(arr, pct):
        idx = int(len(arr) * pct)
        return arr[min(idx, len(arr) - 1)]

    mean_abs = sum(abs_devs) / n
    mean_rel = sum(rel_devs) / n

    print("\n" + "="*70)
    print("CLOB PRICING VALIDATION — synthetic NO vs real CLOB best_ask")
    print("="*70)
    print(f"  Samples: {n}  (errors/skipped: {errors})")
    print()
    print(f"  Absolute deviation |synthetic_NO - clob_best_ask|:")
    print(f"    mean  = {mean_abs:.5f}")
    print(f"    p25   = {_p(abs_devs, 0.25):.5f}")
    print(f"    p50   = {_p(abs_devs, 0.50):.5f}")
    print(f"    p75   = {_p(abs_devs, 0.75):.5f}")
    print(f"    p90   = {_p(abs_devs, 0.90):.5f}")
    print(f"    p95   = {_p(abs_devs, 0.95):.5f}")
    print(f"    max   = {max(abs_devs):.5f}")
    print()
    print(f"  Relative deviation (abs_dev / real_no_ask):")
    print(f"    mean  = {mean_rel:.3f}  ({mean_rel*100:.1f}%)")
    print(f"    p50   = {_p(rel_devs, 0.50):.3f}")
    print(f"    p90   = {_p(rel_devs, 0.90):.3f}")
    print(f"    p95   = {_p(rel_devs, 0.95):.3f}")

    # Cases where synthetic price is significantly wrong (> 0.01 absolute)
    large_devs = [r for r in results if r["abs_dev"] > 0.01]
    print()
    print(f"  Significant mispricing (abs_dev > 0.01): {len(large_devs)} / {n} "
          f"({100*len(large_devs)/n:.1f}%)")

    # Break down by YES price bucket
    print()
    print("  By YES price bucket:")
    print(f"  {'yes_bucket':<12} {'n':>5} {'mean_abs_dev':>14} {'mean_rel_dev':>14} {'large%':>8}")
    buckets = [
        ("<= 0.01", lambda r: r["yes_price"] <= 0.01),
        ("0.01–0.02", lambda r: 0.01 < r["yes_price"] <= 0.02),
        ("0.02–0.05", lambda r: 0.02 < r["yes_price"] <= 0.05),
        ("0.05–0.10", lambda r: 0.05 < r["yes_price"] <= 0.10),
        ("> 0.10",    lambda r: r["yes_price"] > 0.10),
    ]
    for label, fn in buckets:
        subset = [r for r in results if fn(r)]
        if not subset:
            continue
        m_abs = sum(r["abs_dev"] for r in subset) / len(subset)
        m_rel = sum(r["rel_dev"] for r in subset) / len(subset)
        large = sum(1 for r in subset if r["abs_dev"] > 0.01)
        print(f"  {label:<12} {len(subset):>5} {m_abs:>14.5f} {m_rel:>14.3f} "
              f"{100*large/len(subset):>7.1f}%")

    # Worst offenders
    if large_devs:
        print()
        print(f"  Top 5 worst mispriced markets:")
        worst = sorted(large_devs, key=lambda r: r["abs_dev"], reverse=True)[:5]
        for r in worst:
            sp = f"{r['spread']:.4f}" if r['spread'] is not None else "N/A"
            print(f"    abs_dev={r['abs_dev']:.4f} rel={r['rel_dev']:.2f} "
                  f"yes={r['yes_price']:.4f} syn_no={r['synthetic_no']:.4f} "
                  f"real_no={r['real_no_ask']:.4f} spread={sp} "
                  f"| {r['question']}")

    print()
    print("VERDICT:")
    if mean_abs < 0.005 and len(large_devs) / n < 0.05:
        print("  ✓ Synthetic pricing is ACCEPTABLE — deviation is small.")
        print("  The screener can use (1 - YES) for NO side without significant error.")
    elif mean_abs < 0.015:
        print("  ~ Synthetic pricing has MODERATE error.")
        print("  Consider using real CLOB prices for final candidate evaluation,")
        print("  but category-level screener pass can still use synthetic.")
    else:
        print("  ✗ Synthetic pricing is UNRELIABLE — significant deviation detected.")
        print("  The screener MUST use real CLOB orderbook for NO-side pricing.")
        print("  See issue #44: Problem 2.")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Validate synthetic NO pricing vs real CLOB best_ask"
    )
    ap.add_argument("--n", type=int, default=100,
                    help="Number of markets to sample (default: 100)")
    ap.add_argument("--price-max", type=float, default=0.10,
                    help="Max YES price for sampled markets (default: 0.10)")
    ap.add_argument("--all-prices", action="store_true",
                    help="Sample across all prices (set price_max=0.99)")
    ap.add_argument("--seed", type=int, default=42, help="Random seed")
    args = ap.parse_args()

    random.seed(args.seed)

    price_max = 0.99 if args.all_prices else args.price_max
    validate(n=args.n, price_max=price_max)


if __name__ == "__main__":
    main()
