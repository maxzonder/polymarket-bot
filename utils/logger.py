import logging
import sys
from logging.handlers import RotatingFileHandler

from utils.paths import LOGS_DIR, ensure_runtime_dirs

ensure_runtime_dirs()

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def add_bot_log_file(filename: str, level: int = logging.INFO) -> None:
    """
    Attach a shared RotatingFileHandler to the root logger so that ALL module
    loggers (main_loop, order_manager, etc.) also write to one combined file.
    Call once from main.py before any other loggers are created.
    """
    root = logging.getLogger()
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
    log_file = LOGS_DIR / filename
    fh = RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.setLevel(level)
    root.addHandler(fh)
    root.setLevel(level)


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Создаёт логгер с выводом в консоль и ротируемый файл logs/{name}.log.
    Повторный вызов с тем же name вернёт уже настроенный логгер.
    """
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger

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
