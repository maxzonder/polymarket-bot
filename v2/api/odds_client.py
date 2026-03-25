"""
The Odds API client — fetches sports odds for external-anchor signal.

Requires ODDS_API_KEY in environment (from .env or shell).
If key is absent, all calls return empty results; Mode B still works.

Free tier: 500 requests/month.
Usage: poll 2 sports every 30 min = ~2,880 requests/month → needs paid tier
  or: poll 2 sports every 4 hours = ~360 requests/month → fits free tier
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests

BASE_URL = "https://api.the-odds-api.com/v4"

# Sport keys → Polymarket sport label
SPORT_KEYS = {
    "basketball_nba": "nba",
    "esports_cs2":    "cs2",
}

# bookmakers in priority order (Pinnacle = sharpest lines)
BOOKMAKER_PRIORITY = ["pinnaclesports", "draftkings", "fanduel", "betmgm"]


def _api_key() -> Optional[str]:
    return os.environ.get("ODDS_API_KEY") or None


def decimal_to_implied(decimal_odds: float) -> float:
    """Convert decimal odds to raw implied probability (before vig removal)."""
    if decimal_odds <= 0:
        return 0.0
    return 1.0 / decimal_odds


def remove_vig(home_raw: float, away_raw: float) -> tuple[float, float]:
    """Normalize raw implied probabilities to remove bookmaker vig.

    Sum of raw implied probs > 1.0 (the vig margin). Dividing by the total
    gives true probabilities that sum to 1.0.
    """
    total = home_raw + away_raw
    if total <= 0:
        return 0.5, 0.5
    return home_raw / total, away_raw / total


def american_to_implied(american_odds: int) -> float:
    """Convert American odds to implied probability."""
    if american_odds > 0:
        return 100.0 / (american_odds + 100.0)
    else:
        return abs(american_odds) / (abs(american_odds) + 100.0)


def _best_bookmaker(bookmakers: list[dict]) -> Optional[dict]:
    """Pick the sharpest available bookmaker from a game's odds."""
    by_key = {b["key"]: b for b in bookmakers}
    for key in BOOKMAKER_PRIORITY:
        if key in by_key:
            return by_key[key]
    return bookmakers[0] if bookmakers else None


def fetch_odds(sport_key: str, fetch_ts: Optional[float] = None) -> list[dict]:
    """
    Fetch h2h odds for a sport. Returns list of game dicts:
      {
        "external_id": str,
        "sport_key": str,
        "sport_label": str,          # nba / cs2 / ...
        "commence_ts": float,        # Unix timestamp
        "home_team": str,
        "away_team": str,
        "home_implied": float,       # 0–1
        "away_implied": float,       # 0–1
        "bookmaker": str,
        "fetch_ts": float,
      }
    Returns [] if API key absent or request fails.
    """
    key = _api_key()
    if not key:
        return []

    if fetch_ts is None:
        fetch_ts = time.time()

    try:
        resp = requests.get(
            f"{BASE_URL}/sports/{sport_key}/odds",
            params={
                "apiKey": key,
                "regions": "us",
                "markets": "h2h",
                "oddsFormat": "decimal",
            },
            timeout=15,
        )
        resp.raise_for_status()
    except Exception:
        return []

    games = []
    for g in resp.json():
        commence_ts = None
        raw_ts = g.get("commence_time")
        if raw_ts:
            try:
                dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                commence_ts = dt.timestamp()
            except Exception:
                pass

        bm = _best_bookmaker(g.get("bookmakers", []))
        if not bm:
            continue

        # find h2h market
        h2h = next((m for m in bm.get("markets", []) if m["key"] == "h2h"), None)
        if not h2h:
            continue

        outcomes = {o["name"]: o["price"] for o in h2h.get("outcomes", [])}
        home = g.get("home_team", "")
        away = g.get("away_team", "")
        home_odds = outcomes.get(home)
        away_odds = outcomes.get(away)
        if not home_odds or not away_odds:
            continue

        home_raw = decimal_to_implied(home_odds)
        away_raw = decimal_to_implied(away_odds)
        home_implied, away_implied = remove_vig(home_raw, away_raw)

        games.append({
            "external_id":   g["id"],
            "sport_key":     sport_key,
            "sport_label":   SPORT_KEYS.get(sport_key, sport_key),
            "commence_ts":   commence_ts,
            "home_team":     home,
            "away_team":     away,
            "home_implied":  home_implied,
            "away_implied":  away_implied,
            "bookmaker":     bm["key"],
            "fetch_ts":      fetch_ts,
        })

    return games


def fetch_all_sports() -> dict[str, list[dict]]:
    """Fetch odds for all configured sport keys. Returns {sport_key: [games]}."""
    results = {}
    for sport_key in SPORT_KEYS:
        results[sport_key] = fetch_odds(sport_key)
        time.sleep(0.5)
    return results
