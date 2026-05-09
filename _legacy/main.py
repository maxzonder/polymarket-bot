"""
Polymarket Big Swan Bot — entry point.

Usage:
    python main.py                          # big_swan_mode, dry_run=True
    BOT_MODE=fast_tp_mode python main.py    # fast_tp_mode
    DRY_RUN=false python main.py            # live trading (requires POLY_PRIVATE_KEY)

Environment variables:
    BOT_MODE          — trading mode: big_swan_mode | balanced_mode | fast_tp_mode
    DRY_RUN           — true/false (default: true)
    POLY_PRIVATE_KEY  — Ethereum private key for CLOB auth (required for live)
    POLYMARKET_DATA_DIR — path to data directory (default: project root)
"""

import asyncio
import subprocess
import sys

from config import load_config, check_swan_buy_price_threshold
from bot.main_loop import BotRunner
from utils.logger import setup_logger, add_bot_log_file

add_bot_log_file("v1_1_paper.log")
logger = setup_logger("main")


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return "unknown"


def main() -> None:
    config = load_config()
    commit = _git_commit()

    logger.info("=" * 60)
    logger.info("Polymarket Big Swan Bot")
    logger.info(f"  mode:    {config.mode}")
    logger.info(f"  dry_run: {config.dry_run}")
    logger.info(f"  commit:  {commit}")
    if config.dry_run:
        logger.info("  [DRY RUN] No real orders will be placed")
    else:
        if not config.private_key:
            logger.error("POLY_PRIVATE_KEY is not set. Cannot run in live mode.")
            sys.exit(1)
        logger.info("  [LIVE] Real orders will be placed on Polymarket")
    logger.info("=" * 60)

    for alert in check_swan_buy_price_threshold(config.mode_config):
        logger.warning(alert)

    runner = BotRunner(config)
    asyncio.run(runner.run())
    logger.info(f"Bot stopped | commit={commit} mode={config.mode}")


if __name__ == "__main__":
    main()
