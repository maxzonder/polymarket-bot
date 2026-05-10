"""
Tests for screener cluster (category × duration_bucket) multiplier
(issue #180 P2.2).
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from strategy.screener import _cluster_multiplier, _duration_bucket


def test_duration_bucket_boundaries():
    assert _duration_bucket(0.25) == "15m"
    assert _duration_bucket(0.5) == "15m"
    assert _duration_bucket(1.0) == "1h"
    assert _duration_bucket(2.0) == "1h"
    assert _duration_bucket(5.99) == "6h"
    assert _duration_bucket(24.0) == "1-7d"
    assert _duration_bucket(168.0) == "1-7d"
    assert _duration_bucket(200.0) == "long"


_TABLE = {
    ("crypto",  "15m"):  0.70,
    ("weather", "1-7d"): 1.30,
}


def test_cluster_known_pair_returns_value():
    assert _cluster_multiplier(category="crypto", hours=0.25, table=_TABLE) == 0.70
    assert _cluster_multiplier(category="weather", hours=72.0, table=_TABLE) == 1.30


def test_cluster_unknown_pair_returns_one():
    # crypto + 1h not in table → neutral
    assert _cluster_multiplier(category="crypto", hours=1.5, table=_TABLE) == 1.0
    # category not in table → neutral
    assert _cluster_multiplier(category="tech", hours=24.0, table=_TABLE) == 1.0


def test_cluster_none_category_returns_one():
    assert _cluster_multiplier(category=None, hours=24.0, table=_TABLE) == 1.0


def test_cluster_empty_table_returns_one():
    assert _cluster_multiplier(category="crypto", hours=0.25, table={}) == 1.0
