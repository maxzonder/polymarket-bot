from __future__ import annotations

from typing import Any

from ..core.events import FeeInfo


def fetch_fee_info(client: Any, condition_id: str) -> dict[str, FeeInfo]:
    """Query V2 `getClobMarketInfo` for a market and return per-token FeeInfo.

    `client` is a `py_clob_client_v2.client.ClobClient`. The V2 endpoint
    returns a payload with a `t` array (one entry per token in the market)
    and a single `fd` field (`{r, e}`) shared by all tokens. We pair the
    token IDs with the `fd` plus a per-token `base_fee_bps` resolved via
    `client.get_fee_rate_bps(token_id)`.

    Returns an empty dict if the response is empty or malformed; callers
    should fall back to the legacy `fee_rate_bps` path in that case.
    """
    raw = client.get_clob_market_info(condition_id)
    if not isinstance(raw, dict):
        return {}
    tokens = raw.get("t") or []
    fd = raw.get("fd") or {}
    rate = float(fd.get("r", 0.0) or 0.0)
    exponent = float(fd.get("e", 0.0) or 0.0)
    out: dict[str, FeeInfo] = {}
    for token in tokens:
        if not isinstance(token, dict):
            continue
        token_id = token.get("t")
        if not token_id:
            continue
        try:
            base_fee_bps = int(client.get_fee_rate_bps(str(token_id)) or 0)
        except Exception:
            base_fee_bps = 0
        out[str(token_id)] = FeeInfo(
            base_fee_bps=base_fee_bps,
            rate=rate,
            exponent=exponent,
            source="v2.clob_market_info",
        )
    return out


__all__ = ["fetch_fee_info"]
