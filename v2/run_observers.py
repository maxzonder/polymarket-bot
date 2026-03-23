"""
v2 Observer scheduler — runs neg-risk and crypto observers in parallel threads.

Usage:
    cd /home/polybot/claude-polymarket
    python -m v2.run_observers

    # neg-risk only
    python -m v2.run_observers --only negrisk

    # crypto only
    python -m v2.run_observers --only crypto
"""
from __future__ import annotations

import argparse
import signal
import sys
import threading
import time

from .utils.logger import setup_logger

logger = setup_logger("run_observers")


def _run_negrisk() -> None:
    from .observers.negrisk import run_loop
    run_loop()


def _run_crypto() -> None:
    from .observers.crypto import run_loop
    run_loop()


def main(only: str | None = None) -> None:
    threads: list[threading.Thread] = []

    if only in (None, "negrisk"):
        t = threading.Thread(target=_run_negrisk, name="negrisk", daemon=True)
        threads.append(t)
        logger.info("Starting neg-risk observer thread")

    if only in (None, "crypto"):
        t = threading.Thread(target=_run_crypto, name="crypto", daemon=True)
        threads.append(t)
        logger.info("Starting crypto observer thread")

    if not threads:
        logger.error(f"Unknown --only value: {only!r}")
        sys.exit(1)

    for t in threads:
        t.start()

    # Graceful shutdown on SIGINT / SIGTERM
    def _handle_signal(sig, frame):
        logger.info(f"Signal {sig} received — shutting down")
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info(f"All observers running ({len(threads)} thread(s)). Press Ctrl+C to stop.")
    try:
        while True:
            alive = [t.name for t in threads if t.is_alive()]
            if not alive:
                logger.error("All observer threads died — exiting")
                sys.exit(1)
            time.sleep(30)
    except SystemExit:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="v2 observer scheduler")
    parser.add_argument(
        "--only",
        choices=["negrisk", "crypto"],
        default=None,
        help="Run only one observer (default: both)",
    )
    args = parser.parse_args()
    main(only=args.only)
