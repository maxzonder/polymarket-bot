from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from replay.offline_live import OfflineLiveRunner
from utils.paths import DATA_DIR


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Historical offline live runner using the real dry-run execution path"
    )
    ap.add_argument("--start", metavar="YYYY-MM-DD", required=True)
    ap.add_argument("--end", metavar="YYYY-MM-DD", required=True)
    ap.add_argument(
        "--mode",
        default="big_swan_mode",
        choices=["big_swan_mode", "balanced_mode", "fast_tp_mode", "small_swan_mode"],
    )
    ap.add_argument("--limit", type=int, default=None, help="Max market/token rows to load")
    ap.add_argument("--out", default=None, help="Output dir (default: data/replay_runs/offline_live_<timestamp>)")
    ap.add_argument("--summary", action="store_true", help="Suppress per-tick logs")
    ap.add_argument("--trade-cache-size", type=int, default=512)
    args = ap.parse_args()

    if args.out:
        output_dir = Path(args.out)
    else:
        import time

        output_dir = DATA_DIR / "replay_runs" / f"offline_live_{time.strftime('%Y%m%d_%H%M%S')}"

    runner = OfflineLiveRunner(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        mode=args.mode,
        output_dir=output_dir,
        limit=args.limit,
        summary_only=args.summary,
        trade_cache_size=args.trade_cache_size,
    )
    runner.run()


if __name__ == "__main__":
    main()
