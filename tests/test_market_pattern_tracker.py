from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from strategy import market_pattern_tracker as mpt


def test_filter_token_trades_prefers_asset_token_id():
    trades = [
        {"asset": "YES", "outcome": "Yes", "outcomeIndex": 0, "price": 0.50, "timestamp": 1000},
        {"asset": "NO", "outcome": "No", "outcomeIndex": 1, "price": 0.90, "timestamp": 1010},
        {"asset": "YES", "outcome": "Yes", "outcomeIndex": 0, "price": 0.04, "timestamp": 1100},
        {"asset": "NO", "outcome": "No", "outcomeIndex": 1, "price": 0.94, "timestamp": 1110},
        {"asset": "YES", "outcome": "Yes", "outcomeIndex": 0, "price": 0.04, "timestamp": 1200},
        {"asset": "NO", "outcome": "No", "outcomeIndex": 1, "price": 0.95, "timestamp": 1210},
    ]

    yes_trades = mpt._filter_token_trades(trades, token_id="YES", outcome_name="Yes", outcome_index=0)
    no_trades = mpt._filter_token_trades(trades, token_id="NO", outcome_name="No", outcome_index=1)

    assert [t["asset"] for t in yes_trades] == ["YES", "YES", "YES"]
    assert [t["asset"] for t in no_trades] == ["NO", "NO", "NO"]
    assert mpt._classify(yes_trades, end_date_ts=1300) == mpt.FLOOR_ACCUMULATION
    assert mpt._classify(no_trades, end_date_ts=1300) == mpt.NO_FLOOR_YET


def test_filter_token_trades_falls_back_to_outcome_fields():
    trades = [
        {"outcome": "Yes", "outcomeIndex": 0, "price": 0.04, "timestamp": 1000},
        {"outcome": "No", "outcomeIndex": 1, "price": 0.92, "timestamp": 1010},
        {"outcome": "Yes", "outcomeIndex": 0, "price": 0.05, "timestamp": 1100},
    ]

    assert len(mpt._filter_token_trades(trades, token_id="YES_TOKEN", outcome_name="Yes", outcome_index=0)) == 2
    assert len(mpt._filter_token_trades(trades, token_id="NO_TOKEN", outcome_name="No", outcome_index=1)) == 1


def test_tracker_classifies_and_scores_per_token_side(monkeypatch):
    trades = [
        {"asset": "YES", "outcome": "Yes", "outcomeIndex": 0, "price": 0.50, "timestamp": 1000},
        {"asset": "NO", "outcome": "No", "outcomeIndex": 1, "price": 0.90, "timestamp": 1010},
        {"asset": "YES", "outcome": "Yes", "outcomeIndex": 0, "price": 0.04, "timestamp": 1100},
        {"asset": "NO", "outcome": "No", "outcomeIndex": 1, "price": 0.94, "timestamp": 1110},
        {"asset": "YES", "outcome": "Yes", "outcomeIndex": 0, "price": 0.04, "timestamp": 1200},
        {"asset": "NO", "outcome": "No", "outcomeIndex": 1, "price": 0.95, "timestamp": 1210},
    ]
    calls = []

    def fake_fetch(condition_id: str):
        calls.append(condition_id)
        return trades

    monkeypatch.setattr(mpt, "_fetch_trades", fake_fetch)
    tracker = mpt.MarketPatternTracker()
    market = SimpleNamespace(
        market_id="m1",
        condition_id="c1",
        category="weather",
        end_date_ts=1300,
    )

    yes_mult = tracker.get_pattern_mult(market, 24.0, token_id="YES", outcome_name="Yes", outcome_index=0)
    no_mult = tracker.get_pattern_mult(market, 24.0, token_id="NO", outcome_name="No", outcome_index=1)

    assert yes_mult == 1.5
    assert no_mult == 0.0
    assert tracker.get_pattern_label("m1", "YES") == mpt.FLOOR_ACCUMULATION
    assert tracker.get_pattern_label("m1", "NO") == mpt.NO_FLOOR_YET
    assert calls == ["c1"]  # raw market trades fetched once; token labels cached separately
