"""Cancel the 5 resting swan orders from the live smoke test."""
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from short_horizon.venue_polymarket.execution_client import PolymarketExecutionClient

VENUE_ORDER_IDS = [
    "0x59add2a755558883accd7a109e731825bc94da1c06cffd7d5b1b297575dc5e6d",
    "0x3a7f5e08d9287645b401bdcc924e1d64fbd8ccefb1b6632709254b0f0e7125da",
    "0x626092c0c2cfd326a3001217a8375e7b38d540f82779a37cfbeb7f484c2404ae",
    "0x9f7d8bc222101e0e2ff7c30ec14bece40a2366fad4941cb0d9ef42c6b842e013",
    "0x8f5abbcbac1c73486682e6ada93733c596220b9c7fc09d231be96be0dea6e273",
]

client = PolymarketExecutionClient()
client.startup()

for oid in VENUE_ORDER_IDS:
    result = client.cancel_order(oid)
    status = "OK" if result.success else "FAIL"
    print(f"[{status}] {oid[:20]}... -> {result.status}")
