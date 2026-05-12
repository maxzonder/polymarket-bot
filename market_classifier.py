"""Shared Polymarket market classification helpers.

The bot has several ingestion paths (historical parser, live collectors, paper/live
screener).  Category labels are strategy buckets, not necessarily Polymarket's
public taxonomy: finance/equities/commodities are intentionally mapped into the
`crypto`/market-movement bucket to preserve historical black-swan priors.
"""

from __future__ import annotations

import re
from typing import Any


_TAG_CATEGORY_MAP: dict[str, str] = {
    # crypto / market-movement bucket
    "crypto": "crypto", "bitcoin": "crypto", "ethereum": "crypto",
    "defi": "crypto", "nft": "crypto", "web3": "crypto",
    "stablecoins": "crypto", "altcoins": "crypto", "solana": "crypto",
    "finance": "crypto", "economy": "crypto", "business": "crypto",
    "stocks": "crypto", "stock-market": "crypto", "markets": "crypto",
    "fed": "crypto", "commodities": "crypto",
    # sports / esports
    "sports": "sports", "soccer": "sports", "football": "sports",
    "basketball": "sports", "nba": "sports", "nfl": "sports",
    "baseball": "sports", "mlb": "sports", "tennis": "sports",
    "golf": "sports", "mma": "sports", "boxing": "sports",
    "hockey": "sports", "nhl": "sports", "cricket": "sports",
    "olympics": "sports", "f1": "sports", "formula-1": "sports",
    "ufc": "sports", "wrestling": "sports", "rugby": "sports",
    "esports": "sports", "cs2": "sports", "csgo": "sports",
    "league-of-legends": "sports", "valorant": "sports",
    "dota": "sports", "dota-2": "sports", "overwatch": "sports", "gaming": "sports",
    # politics / geopolitics
    "politics": "politics", "elections": "politics",
    "us-politics": "politics", "congress": "politics",
    "senate": "politics", "trump": "politics", "president": "politics",
    "supreme-court": "politics",
    "geopolitics": "geopolitics", "world": "geopolitics",
    "international": "geopolitics", "nato": "geopolitics",
    "war": "geopolitics", "middle-east": "geopolitics",
    "russia": "geopolitics", "china": "geopolitics", "ukraine": "geopolitics",
    # weather
    "weather": "weather", "climate": "weather", "hurricane": "weather",
    # entertainment
    "entertainment": "entertainment", "pop-culture": "entertainment",
    "celebrity": "entertainment", "music": "entertainment",
    "movies": "entertainment", "tv": "entertainment",
    "awards": "entertainment", "oscars": "entertainment",
    "culture": "entertainment",
    # tech / health
    "tech": "tech", "technology": "tech", "ai": "tech",
    "science": "tech", "space": "tech",
    "health": "health", "covid-19": "health", "medicine": "health",
}

_CATEGORY_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("crypto", (
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "doge", "xrp", "gold", "silver",
        "oil", "nasdaq", "s&p", "spy", "qqq", "fed", "cpi", "rate cut", "treasury", "eur", "jpy", "rial",
        "bnb", "hyperliquid", "hype", "ethena", "ena", "chainlink", "link", "avax", "avalanche",
        "polygon", "matic", "arbitrum", "arb", "optimism", "op ", "sui ", "aptos", "apt ",
        "pepe", "shib", "floki", "wif ", "bonk", "meme", "airdrop", "nft",
        "litecoin", "ltc", "cardano", "ada", "polkadot", "dot ", "uniswap", "uni ",
        "near ", "injective", "inj ", "sei ", "mantle", "mnt ",
        "nikkei", "dow jones", "dji", "ftse", "dax", "hang seng", "hsi", "russell", "rut ",
        "nya ", "nyk ",
        "palantir", "pltr", "airbnb", "abnb", "rocket lab", "rklb", "opendoor",
        "coinbase", "coin ", "robinhood", "hood ", "rivian", "lucid", "rivn",
        "uranium", "natural gas", "copper", "platinum", "wheat", "corn ", "soybean",
        "up or down",
    )),
    ("sports", (
        "fc ", " vs ", "game ", "match ", "mlb", "nba", "nhl", "nfl", "uefa", "champions league",
        "world cup", "premier league", "serie a", "la liga", "bundesliga", "tennis", "f1", "formula 1",
        "ufc", "cricket", "afghanistan", "south africa", "win on ",
        "sailgp", "sailing", "golf", "pga", "masters", "nascar", "indycar", "mls", "wnba",
        "olympics", "wimbledon", "tour de france", "cycling", "swimming", "athletics",
        "boxing", "wrestling", "esports", "e-sports", "league of legends", "dota", "dota 2", "cs2",
        "chess", "fide", "world chess", "grand chess tour", "sinquefield cup", "candidates tournament",
        "darts", "pdc", "bdo darts",
        "motogp", "moto2", "moto3", "superbike", "wsbk",
        "pickleball", "ppa",
        "volleyball", "beach volleyball",
        "basketball", "eurobasket", "fiba", "ncaa", "march madness",
        "college football", "college basketball", "cfp", "heisman", "big 10", "big ten", "sec ", "acc ",
        "pac-12", "big 12", "carabao cup", "fa cup", "copa del rey", "dfb pokal",
        "transfer window", "sign with", "sign for",
        "valorant", "overwatch", "rocket league", "lck", "lpl", "cblol", "fncs", "fortnite championship",
        "hltv", "blast", "iem ", "esl pro", "pgl ",
        "rugby", "handball", "snooker", "badminton", "table tennis", "archery", "equestrian",
        "marathon", "triathlon", "ironman", "motocross",
        "atp ", "wta ",
        "horse racing", "ladbrokes", "jockey", "racecourse", "epsom", "kentucky derby",
        "melbourne cup", "ascot", "cheltenham",
        "ligue 1", "ligue 2", "eredivisie", "a-league", "j-league", "k-league",
        "super lig", "primeira liga",
    )),
    ("politics", (
        "trump", "biden", "election", "senate", "house", "republican", "democrat", "white house",
        "prime minister", "president", "governor", "mayor", "parliament", "minister",
        "elon musk", "doge ", "department of",
        "unemployment rate", "gdp", "inflation", "interest rate", "interest rates", "jobs report",
        "nonfarm payroll", "add jobs", "bps after",
        "zelenskyy", "zelensky", "maduro", "macron", "scholz", "modi ", "erdogan", "netanyahu",
        "sanders", "aoc ", "ocasio-cortez", "pelosi", "mcconnell", "schumer",
        "south korea", "yoon ",
        "tariff", "sanctions", "veto", "impeach", "resign", "cabinet",
        "supreme court", "federal reserve", "powell", "yellen",
        "bank of england", "european central bank", "ecb ", "bank of japan",
        "redistrict", "border encounter", "border crossing",
    )),
    ("geopolitics", (
        "iran", "iraq", "israel", "gaza", "hamas", "hezbollah", "ukraine", "russia", "china", "taiwan",
        "strike", "missile", "nuclear", "military", "ceasefire", "war", "attack",
        "venezuela", "north korea", "kim jong", "nato", "un security council",
        "coup", "invasion", "occupation", "sanction", "airstrike",
    )),
    ("weather", (
        "temperature", "°c", "°f", "highest temperature", "lowest temperature", "highest temp", "lowest temp",
        "rain", "rainfall", "precipitation", "snow", "storm", "hurricane",
        "tornado", "typhoon", "earthquake", "magnitude", "flood", "wildfire", "drought",
        "wind speed", "wind gust", "arctic sea ice", "sea ice extent", "tsa passengers",
    )),
    ("entertainment", (
        "oscar", "grammy", "emmy", "sag awards", "box office", "movie", "album", "netflix", "actor", "actress",
        "rotten tomatoes", "tomatometer", "spotify", "billboard",
        "casino", "poker", "hustler",
        "bafta", "golden globe", "tony award", "cma awards", "ama awards", "mtv awards",
        "dga award", "directors guild", "film independent", "spirit award",
        "goodreads", "book award", "literary award",
        "eurovision",
        "taylor swift", "beyonce", "drake ", "kanye", "rihanna",
        "mrbeast", "youtube views", "tiktok views",
        "season finale",
    )),
    ("tech", (
        "openai", "chatgpt", "apple", "google", "microsoft", "nvidia", "tesla", "meta", "amazon",
        "spacex", "starship", "falcon 9",
        "tiktok", "bytedance",
        "anthropic", "gemini", "grok", "deepseek", "claude",
        "alphabet", "waymo", "x.com",
        "ipo", "spac",
        "earnings call", "quarterly earnings",
    )),
    ("health", (
        "measles", "flu ", "influenza", "covid", "coronavirus", "pandemic", "epidemic",
        "vaccine", "vaccination", "cdc ", "who ", "fda approval", "drug approval",
        "hospitalization rate", "infection rate", "outbreak", "ebola", "mpox", "monkeypox",
        "cancer", "clinical trial",
    )),
]


def infer_market_category(raw: dict[str, Any], event0: dict[str, Any] | None = None) -> str | None:
    """Infer the internal strategy category for a Gamma market payload."""
    event0 = event0 if event0 is not None else _first_event(raw)

    explicit = _normalize_category(raw.get("category") or raw.get("categorySlug") or raw.get("category_slug"))
    if explicit:
        return explicit
    explicit = _normalize_category((event0 or {}).get("category") or (event0 or {}).get("categorySlug"))
    if explicit:
        return explicit

    tag_category = _category_from_tags(raw.get("tags") or []) or _category_from_tags((event0 or {}).get("tags") or [])
    if tag_category:
        return tag_category

    haystack = " ".join(
        str(part or "")
        for part in (
            raw.get("question"),
            raw.get("description"),
            raw.get("slug"),
            raw.get("title"),
            raw.get("groupItemTitle"),
            (event0 or {}).get("title"),
            (event0 or {}).get("slug"),
            (event0 or {}).get("description"),
        )
    ).lower()
    return infer_category_from_text(haystack)


def infer_category_from_text(text: str) -> str | None:
    haystack = str(text or "").lower()
    for category, keywords in _CATEGORY_KEYWORDS:
        if any(_keyword_matches(haystack, keyword) for keyword in keywords):
            return category
    return None


def _first_event(raw: dict[str, Any]) -> dict[str, Any] | None:
    events = raw.get("events") or []
    if events and isinstance(events[0], dict):
        return events[0]
    return None


def _normalize_category(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    key = value.strip().lower()
    return _TAG_CATEGORY_MAP.get(key) or key


def _category_from_tags(tags: list[Any]) -> str | None:
    for tag in tags:
        if not isinstance(tag, dict):
            continue
        for key in ("slug", "label", "name"):
            value = tag.get(key)
            if not isinstance(value, str):
                continue
            category = _normalize_category(value)
            if category:
                return category
    return None


def _keyword_matches(text: str, keyword: str) -> bool:
    keyword = keyword.lower()
    if not keyword.strip():
        return False
    if keyword.strip() != keyword or " " in keyword or "-" in keyword or not keyword.isalnum():
        return keyword in text
    return re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", text) is not None


__all__ = ["infer_category_from_text", "infer_market_category"]
