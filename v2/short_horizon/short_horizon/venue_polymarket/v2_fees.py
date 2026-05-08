from __future__ import annotations

import time
from typing import Any, Callable

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


class V2FeeInfoCache:
    """TTL cache over `fetch_fee_info` keyed by condition_id.

    Avoids hammering the V2 CLOB `getClobMarketInfo` endpoint on every
    market refresh tick. Negative results (empty dict, exception) are
    cached for `negative_ttl_seconds` so a transient SDK 4xx doesn't
    cause repeated calls inside a refresh interval.
    """

    def __init__(
        self,
        client: Any,
        *,
        ttl_seconds: float = 60.0,
        negative_ttl_seconds: float = 10.0,
        fetcher: Callable[[Any, str], dict[str, FeeInfo]] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._client = client
        self._ttl_seconds = float(ttl_seconds)
        self._negative_ttl_seconds = float(negative_ttl_seconds)
        self._fetcher = fetcher or fetch_fee_info
        self._clock = clock or time.monotonic
        self._cache: dict[str, tuple[float, dict[str, FeeInfo]]] = {}

    def get(self, condition_id: str) -> dict[str, FeeInfo]:
        now = self._clock()
        cached = self._cache.get(condition_id)
        if cached is not None:
            expires_at, value = cached
            if now < expires_at:
                return value
        try:
            value = self._fetcher(self._client, condition_id)
        except Exception:
            value = {}
        ttl = self._ttl_seconds if value else self._negative_ttl_seconds
        self._cache[condition_id] = (now + ttl, value)
        return value

    def invalidate(self, condition_id: str | None = None) -> None:
        if condition_id is None:
            self._cache.clear()
        else:
            self._cache.pop(condition_id, None)


__all__ = ["fetch_fee_info", "V2FeeInfoCache"]
