"""
Tests for stake-multiplier helpers (issue #180 P1.2 + P1.3).
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_V2 = _REPO / "v2" / "short_horizon"
for p in (_REPO, _V2):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from short_horizon.strategies.swan_strategy_v1 import _phase_stake_multiplier
from types import SimpleNamespace

from v2.short_horizon.swan_live import _duration_stake_multiplier, _ws_pattern_retry_delay_seconds


# Mirrors BLACK_SWAN_MODE.duration_stake_multipliers
_DUR_TABLE = (
    (1.0, 0.30),
    (6.0, 0.70),
    (float("inf"), 1.00),
)

# Mirrors BLACK_SWAN_MODE.phase_stake_multipliers
_PHASE_TABLE = (
    (0.10, 0.50),
    (0.75, 1.30),
    (0.95, 1.30),
    (1.00, 0.60),
)


def test_duration_short_market_attenuated():
    assert _duration_stake_multiplier(0.5, _DUR_TABLE) == 0.30
    assert _duration_stake_multiplier(1.0, _DUR_TABLE) == 0.30


def test_duration_medium_market_partial():
    assert _duration_stake_multiplier(3.0, _DUR_TABLE) == 0.70
    assert _duration_stake_multiplier(6.0, _DUR_TABLE) == 0.70


def test_duration_long_market_full():
    assert _duration_stake_multiplier(24.0, _DUR_TABLE) == 1.00
    assert _duration_stake_multiplier(168.0, _DUR_TABLE) == 1.00


def test_duration_none_hours_no_scaling():
    assert _duration_stake_multiplier(None, _DUR_TABLE) == 1.0


def test_duration_empty_table_no_scaling():
    assert _duration_stake_multiplier(2.0, ()) == 1.0


def test_phase_opening_dampened():
    assert _phase_stake_multiplier(0.05, _PHASE_TABLE) == 0.50


def test_phase_middle_boosted():
    assert _phase_stake_multiplier(0.50, _PHASE_TABLE) == 1.30


def test_phase_late_boosted():
    assert _phase_stake_multiplier(0.85, _PHASE_TABLE) == 1.30


def test_phase_final_attenuated():
    assert _phase_stake_multiplier(0.98, _PHASE_TABLE) == 0.60


def test_phase_empty_table_no_scaling():
    assert _phase_stake_multiplier(0.5, ()) == 1.0


def test_phase_above_table_max_returns_unity():
    """Defensive: lifecycle > all table entries → no-op (table should bound to 1.0)."""
    short_table = ((0.5, 0.7),)
    assert _phase_stake_multiplier(0.9, short_table) == 1.0


def test_ws_pattern_retry_delay_uses_remaining_time():
    assert _ws_pattern_retry_delay_seconds(SimpleNamespace(hours_to_close=10 / 60)) == 30
    assert _ws_pattern_retry_delay_seconds(SimpleNamespace(hours_to_close=1.0)) == 90
    assert _ws_pattern_retry_delay_seconds(SimpleNamespace(hours_to_close=None, end_date_ts=1_600), now_s=1_000.0) == 30
    assert _ws_pattern_retry_delay_seconds(SimpleNamespace(hours_to_close=None, end_date_ts=None)) == 90


def test_ws_pattern_retry_delay_clamps_short_markets():
    assert _ws_pattern_retry_delay_seconds(SimpleNamespace(hours_to_close=1 / 60)) == 20
