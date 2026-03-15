from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
except ImportError:
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT
DATA_DIR = Path(os.environ.get("POLYMARKET_DATA_DIR", str(DEFAULT_DATA_DIR))).expanduser()
DATABASE_DIR = DATA_DIR / "database"
DB_PATH = DATA_DIR / "polymarket_dataset.db"
LOGS_DIR = DATA_DIR / "logs"


def ensure_runtime_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
