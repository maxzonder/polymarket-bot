from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env", override=False)
except ImportError:
    _env_file = Path(__file__).resolve().parent.parent.parent / ".env"
    if _env_file.exists():
        for _line in _env_file.read_text().splitlines():
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("POLYMARKET_DATA_DIR", str(PROJECT_ROOT))).expanduser()

# v2 has its own subdirectory — no collision with v1 DBs
V2_DIR = DATA_DIR / "v2"
LOGS_DIR = V2_DIR / "logs"

# Observation DBs
NEGRISK_DB = V2_DIR / "obs_negrisk.db"
CRYPTO_DB   = V2_DIR / "obs_crypto.db"

# Historical dataset (shared, read-only from v2)
DATASET_DB = DATA_DIR / "polymarket_dataset.db"


def ensure_dirs() -> None:
    V2_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
