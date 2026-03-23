from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler

from .paths import LOGS_DIR, ensure_dirs

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Create logger with console + rotating file output to logs/{name}.log."""
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger

    ensure_dirs()
    logger.setLevel(level)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    log_file = LOGS_DIR / f"{name}.log"
    fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
