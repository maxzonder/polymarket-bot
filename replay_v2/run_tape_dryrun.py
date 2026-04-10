from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tape_runner import TapeDrivenDryRunRunner
from utils.paths import DATA_DIR


def main() -> None:
    ap = argparse.ArgumentParser(description="Tape-driven offline dryrun runner")
    ap.add_argument("--start", metavar="YYYY-MM-DD", required=True)
    ap.add_argument("--end", metavar="YYYY-MM-DD", required=True)
    ap.add_argument(
        "--mode",
        default="big_swan_mode",
        choices=["big_swan_mode", "balanced_mode", "fast_tp_mode", "small_swan_mode"],
    )
    ap.add_argument("--limit-markets", type=int, default=None)
    ap.add_argument("--batch-seconds", type=int, default=300)
    ap.add_argument("--out", default=None)
    ap.add_argument("--tape-db", default=None, help="Path to historical_tape.db (default: auto-detect in data dir)")
    args = ap.parse_args()

    if args.out:
        output_dir = Path(args.out)
    else:
        import time
        output_dir = DATA_DIR / "replay_runs" / f"tape_dryrun_{time.strftime('%Y%m%d_%H%M%S')}"

    runner = TapeDrivenDryRunRunner(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        mode=args.mode,
        output_dir=output_dir,
        limit_markets=args.limit_markets,
        batch_seconds=args.batch_seconds,
        tape_db_path=Path(args.tape_db).expanduser() if args.tape_db else None,
    )
    result = runner.run()
    print(result)


if __name__ == "__main__":
    main()
