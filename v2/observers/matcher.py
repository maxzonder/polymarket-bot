"""
Sports event matcher — Layer 1 of the sports observer.

Parses Polymarket event_title → normalizes team names → matches to
external odds game. Stores full raw evidence in sp_match_attempts.

Match score: 0.0–1.0
  - 1.0  both teams match (after normalization) + date window ok
  - 0.6  one team matches + date window ok
  - 0.0  no team match

Only scores >= ACCEPT_THRESHOLD are used for Mode A signal.
All attempts (accepted or rejected) are persisted for audit.
"""
from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Optional

# ── Team name normalization lookup ─────────────────────────────────────────────
# Polymarket short names / aliases → canonical form used by The Odds API.
# Expand this table as sp_unmatched surfaces new names.

_TEAM_ALIASES: dict[str, str] = {
    # NBA
    "hawks":        "Atlanta Hawks",
    "celtics":      "Boston Celtics",
    "nets":         "Brooklyn Nets",
    "hornets":      "Charlotte Hornets",
    "bulls":        "Chicago Bulls",
    "cavaliers":    "Cleveland Cavaliers",
    "mavericks":    "Dallas Mavericks",
    "nuggets":      "Denver Nuggets",
    "pistons":      "Detroit Pistons",
    "warriors":     "Golden State Warriors",
    "rockets":      "Houston Rockets",
    "pacers":       "Indiana Pacers",
    "clippers":     "Los Angeles Clippers",
    "lakers":       "Los Angeles Lakers",
    "grizzlies":    "Memphis Grizzlies",
    "heat":         "Miami Heat",
    "bucks":        "Milwaukee Bucks",
    "timberwolves": "Minnesota Timberwolves",
    "pelicans":     "New Orleans Pelicans",
    "knicks":       "New York Knicks",
    "thunder":      "Oklahoma City Thunder",
    "magic":        "Orlando Magic",
    "76ers":        "Philadelphia 76ers",
    "philly":       "Philadelphia 76ers",
    "suns":         "Phoenix Suns",
    "blazers":      "Portland Trail Blazers",
    "trail blazers":"Portland Trail Blazers",
    "kings":        "Sacramento Kings",
    "spurs":        "San Antonio Spurs",
    "raptors":      "Toronto Raptors",
    "jazz":         "Utah Jazz",
    "wizards":      "Washington Wizards",
    # CS2 teams
    "navi":         "Natus Vincere",
    "natus vincere":"Natus Vincere",
    "vitality":     "Team Vitality",
    "faze":         "FaZe Clan",
    "faze clan":    "FaZe Clan",
    "g2":           "G2 Esports",
    "g2 esports":   "G2 Esports",
    "liquid":       "Team Liquid",
    "team liquid":  "Team Liquid",
    "heroic":       "Heroic",
    "nip":          "Ninjas in Pyjamas",
    "ninjas in pyjamas": "Ninjas in Pyjamas",
    "spirit":       "Team Spirit",
    "team spirit":  "Team Spirit",
    "astralis":     "Astralis",
    "complexity":   "Complexity Gaming",
    "ence":         "ENCE",
    "mouz":         "MOUZ",
    "mousesports":  "MOUZ",
    "apeks":        "Apeks",
    "eternal fire": "Eternal Fire",
    "imperial":     "Imperial Esports",
    "mibr":         "MIBR",
    "9z":           "9z Team",
    "pain":         "paiN Gaming",
    "cloud9":       "Cloud9",
    "c9":           "Cloud9",
    "virtus.pro":   "Virtus.pro",
    "vp":           "Virtus.pro",
}

# Sport/league prefix patterns in Polymarket event titles
_SPORT_PREFIXES: dict[str, str] = {
    "nba":    "nba",
    "nfl":    "nfl",
    "mlb":    "mlb",
    "nhl":    "nhl",
    "cs2":    "cs2",
    "csgo":   "cs2",
    "cs:go":  "cs2",
    "dota":   "dota2",
    "dota 2": "dota2",
    "lol":    "lol",
    "valorant": "valorant",
}

# Which sport keys we actually have external odds for
SUPPORTED_SPORTS = {"nba", "cs2"}

ACCEPT_THRESHOLD = 0.85
DATE_WINDOW_SEC  = 43200  # 12 hours each side


@dataclass
class ParsedEvent:
    raw_title:   str
    sport:       Optional[str]       # nba / cs2 / None
    home:        Optional[str]       # raw parsed
    away:        Optional[str]
    norm_home:   Optional[str]       # after lookup table
    norm_away:   Optional[str]


@dataclass
class MatchResult:
    event_id:             str        # hash(event_title + date)
    event_title:          str
    parsed:               ParsedEvent
    external_id:          Optional[str]
    external_home:        Optional[str]
    external_away:        Optional[str]
    candidate_commence_ts: Optional[float]
    date_window_score:    float
    name_match_score:     float
    final_score:          float
    accepted:             bool
    # implied probs from external (filled if accepted)
    home_implied:         Optional[float] = None
    away_implied:         Optional[float] = None
    bookmaker:            Optional[str]   = None
    fetch_ts:             Optional[float] = None


def _normalize(name: str) -> str:
    """Look up canonical team name; fall back to title-cased raw name."""
    key = name.lower().strip()
    return _TEAM_ALIASES.get(key, name.strip())


def _detect_sport(title: str) -> Optional[str]:
    """Detect sport from event_title prefix or keywords."""
    low = title.lower()
    for prefix, sport in _SPORT_PREFIXES.items():
        if low.startswith(prefix + ":") or low.startswith(prefix + " "):
            return sport
    return None


def parse_event_title(title: str) -> ParsedEvent:
    """
    Parse 'Team A vs Team B' or 'SPORT: Team A vs Team B'.
    Returns ParsedEvent with home/away (raw + normalized) and sport.
    """
    sport = _detect_sport(title)

    # Strip sport prefix if present
    clean = title
    if sport:
        idx = title.find(":")
        if idx != -1:
            clean = title[idx + 1:].strip()

    # Split on ' vs ' or ' vs. ' (case-insensitive)
    import re
    parts = re.split(r"\s+vs\.?\s+", clean, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) == 2:
        home_raw = parts[0].strip()
        away_raw = parts[1].strip()
        # Remove trailing parentheticals like "(Game 3)"
        home_raw = re.sub(r"\s*\(.*?\)\s*$", "", home_raw).strip()
        away_raw = re.sub(r"\s*\(.*?\)\s*$", "", away_raw).strip()
        norm_home = _normalize(home_raw)
        norm_away = _normalize(away_raw)
    else:
        home_raw = away_raw = norm_home = norm_away = None

    return ParsedEvent(
        raw_title=title,
        sport=sport,
        home=home_raw,
        away=away_raw,
        norm_home=norm_home,
        norm_away=norm_away,
    )


def _name_match_score(norm_home: str, norm_away: str, ext_home: str, ext_away: str) -> float:
    """
    Score how well parsed team names match an external game.
    Checks both orderings (home/away may be swapped between sources).
    Returns 1.0 (both match), 0.6 (one matches), 0.0 (none).
    """
    def same(a: str, b: str) -> bool:
        return a.lower().strip() == b.lower().strip()

    # Direct order
    direct = (same(norm_home, ext_home) and same(norm_away, ext_away))
    # Swapped order
    swapped = (same(norm_home, ext_away) and same(norm_away, ext_home))
    if direct or swapped:
        return 1.0

    # Partial: one team matches
    one_match = (
        same(norm_home, ext_home) or same(norm_home, ext_away) or
        same(norm_away, ext_home) or same(norm_away, ext_away)
    )
    return 0.6 if one_match else 0.0


def _date_score(poly_end_ts: float, commence_ts: Optional[float]) -> float:
    """Score how well the dates align. 1.0 within window, 0 outside."""
    if commence_ts is None:
        return 0.5  # unknown date — partial credit
    diff = abs(poly_end_ts - commence_ts)
    return 1.0 if diff <= DATE_WINDOW_SEC else 0.0


def make_event_id(event_title: str, game_ts: Optional[float]) -> str:
    """Stable hash key for a canonical event."""
    date_part = str(int(game_ts / 86400)) if game_ts else "unknown"
    raw = f"{event_title.lower().strip()}|{date_part}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def match_event(
    event_title: str,
    poly_end_ts: float,
    external_games: list[dict],
) -> MatchResult:
    """
    Match one Polymarket event_title to the best external game candidate.
    Returns a MatchResult with full evidence regardless of acceptance.
    """
    now = time.time()
    parsed = parse_event_title(event_title)

    best_score = 0.0
    best_game: Optional[dict] = None
    best_date_score = 0.0
    best_name_score = 0.0

    if parsed.norm_home and parsed.norm_away and external_games:
        for g in external_games:
            ds = _date_score(poly_end_ts, g.get("commence_ts"))
            ns = _name_match_score(
                parsed.norm_home, parsed.norm_away,
                g.get("home_team", ""), g.get("away_team", ""),
            )
            combined = ds * 0.3 + ns * 0.7
            if combined > best_score:
                best_score = combined
                best_game = g
                best_date_score = ds
                best_name_score = ns

    accepted = best_score >= ACCEPT_THRESHOLD and best_game is not None

    game_ts = best_game["commence_ts"] if best_game else None
    event_id = make_event_id(event_title, game_ts)

    return MatchResult(
        event_id=event_id,
        event_title=event_title,
        parsed=parsed,
        external_id=best_game["external_id"] if best_game else None,
        external_home=best_game["home_team"] if best_game else None,
        external_away=best_game["away_team"] if best_game else None,
        candidate_commence_ts=game_ts,
        date_window_score=best_date_score,
        name_match_score=best_name_score,
        final_score=best_score,
        accepted=accepted,
        home_implied=best_game["home_implied"] if accepted and best_game else None,
        away_implied=best_game["away_implied"] if accepted and best_game else None,
        bookmaker=best_game["bookmaker"] if accepted and best_game else None,
        fetch_ts=best_game["fetch_ts"] if best_game else None,
    )


def persist_attempt(conn, result: MatchResult, ts: float) -> None:
    """Write raw match evidence to sp_match_attempts."""
    p = result.parsed
    conn.execute(
        """INSERT INTO sp_match_attempts
           (ts, event_title, parsed_sport, parsed_home, parsed_away,
            normalized_home, normalized_away,
            candidate_external_id, candidate_home, candidate_away,
            candidate_commence_ts, date_window_score, name_match_score,
            final_score, accepted)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            ts, result.event_title, p.sport, p.home, p.away,
            p.norm_home, p.norm_away,
            result.external_id, result.external_home, result.external_away,
            result.candidate_commence_ts,
            result.date_window_score, result.name_match_score,
            result.final_score, int(result.accepted),
        ),
    )


def persist_event(conn, result: MatchResult, ts: float) -> None:
    """Upsert accepted event into sp_events."""
    if not result.accepted:
        return
    p = result.parsed
    conn.execute(
        """INSERT OR REPLACE INTO sp_events
           (event_id, event_title, sport, team_home, team_away,
            game_ts, external_id, match_score, created_ts)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            result.event_id, result.event_title, p.sport,
            result.external_home, result.external_away,
            result.candidate_commence_ts, result.external_id,
            result.final_score, ts,
        ),
    )
