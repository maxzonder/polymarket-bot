"""V2 CLOB SDK spike — read-only surface check.

Run from a scratch venv that has only `py-clob-client-v2==1.0.0` installed:

    python3 -m venv /tmp/v2_spike_venv
    /tmp/v2_spike_venv/bin/pip install --quiet py-clob-client-v2==1.0.0
    /tmp/v2_spike_venv/bin/python scripts/spike_v2_clob_hello.py

The script:

- imports `py_clob_client_v2`
- constructs an OrderArgs (no signing, no network, no private key)
- prints the SDK surface differences vs the V1 SDK we currently use
- exits 0 on success

Goal: validate enough of the V2 SDK shape to plan M-3 / M-4 in #138 without
touching the production venv where probes are running.
"""

from __future__ import annotations

import dataclasses
import inspect
import sys


def main() -> int:
    try:
        import py_clob_client_v2 as v2
    except ImportError as exc:
        print(f"FAIL: py-clob-client-v2 not installed: {exc}")
        return 1

    print("=" * 60)
    print(f"py_clob_client_v2 version: {getattr(v2, '__version__', '<no __version__>')}")
    print("=" * 60)

    print("\n--- ClobClient.__init__ signature ---")
    print(inspect.signature(v2.ClobClient.__init__))

    print("\n--- OrderArgs (default V2 alias) fields ---")
    for f in dataclasses.fields(v2.OrderArgs):
        default = "<required>" if f.default is dataclasses.MISSING else repr(f.default)
        print(f"  {f.name}: {default}")

    print("\n--- OrderArgsV1 (legacy shape, still importable) ---")
    for f in dataclasses.fields(v2.OrderArgsV1):
        default = "<required>" if f.default is dataclasses.MISSING else repr(f.default)
        print(f"  {f.name}: {default}")

    print("\n--- OrderPayload (cancel_order argument) ---")
    for f in dataclasses.fields(v2.OrderPayload):
        default = "<required>" if f.default is dataclasses.MISSING else repr(f.default)
        print(f"  {f.name}: {default}")

    print("\n--- PartialCreateOrderOptions (replaces tick_size_ttl_ms etc) ---")
    for f in dataclasses.fields(v2.PartialCreateOrderOptions):
        default = "<required>" if f.default is dataclasses.MISSING else repr(f.default)
        print(f"  {f.name}: {default}")

    print("\n--- FeeDetails (returned by getClobMarketInfo) ---")
    for f in dataclasses.fields(v2.FeeDetails):
        default = "<required>" if f.default is dataclasses.MISSING else repr(f.default)
        print(f"  {f.name}: {default}")

    print("\n--- BuilderConfig (replaces HMAC headers) ---")
    for f in dataclasses.fields(v2.BuilderConfig):
        default = "<required>" if f.default is dataclasses.MISSING else repr(f.default)
        print(f"  {f.name}: {default}")

    print("\n--- ClobClient call surface (relevant subset) ---")
    relevant_methods = (
        "create_or_derive_api_key",
        "derive_api_key",
        "create_api_key",
        "create_and_post_order",
        "create_order",
        "cancel_order",
        "cancel_orders",
        "cancel_all",
        "get_order",
        "get_open_orders",
        "get_clob_market_info",
        "get_fee_rate_bps",
        "get_pre_migration_orders",
    )
    for name in relevant_methods:
        method = getattr(v2.ClobClient, name, None)
        if method is None:
            print(f"  {name}: <not found in V2 SDK>")
            continue
        try:
            sig = inspect.signature(method)
        except (TypeError, ValueError):
            sig = "(<unknown>)"
        print(f"  {name}{sig}")

    print("\n--- OrderArgs construction smoke (no signing, no network) ---")
    args = v2.OrderArgs(
        token_id="dummy_token_id",
        price=0.55,
        size=2.0,
        side=v2.Side.BUY,
    )
    print(f"  built: {args}")

    print("\n--- Cancel payload smoke ---")
    payload = v2.OrderPayload(orderID="dummy_order_id")
    print(f"  built: {payload}")

    print("\nOK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
