"""Compatibility wrapper: data_collector.analyzer now delegates to analyzer_v3.

Legacy v2/price_history analyzer has been retired.
Use this module only as a stable entrypoint for older commands/scripts.
"""

import argparse
import os
import sys

if __package__:
    from .analyzer_v3 import run
else:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from data_collector.analyzer_v3 import run


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Compatibility wrapper for analyzer_v3")
    ap.add_argument("--recompute", action="store_true")
    ap.add_argument("--date", metavar="YYYY-MM-DD")
    ap.add_argument("--market-id")
    ap.add_argument("--min-entry-liquidity", type=float, default=10.0)
    ap.add_argument("--min-recovery", type=float, default=5.0)
    ap.add_argument("--target-exit-x", type=float, default=5.0)
    ap.add_argument("--min-duration-minutes", type=float, default=10.0)
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
