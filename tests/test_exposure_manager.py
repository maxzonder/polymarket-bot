"""Tests for ExposureManager — issue #71 finding 1."""

import pytest
from pathlib import Path

from execution.exposure_manager import ExposureManager


@pytest.fixture
def em(tmp_path):
    return ExposureManager(db_path=tmp_path / "test.db", max_per_market=2.0)


def test_can_add_first_fill(em):
    assert em.can_add("mkt1", "tok1", 1.0) is True


def test_can_add_within_cap(em):
    em.record_fill("mkt1", "tok1", 0.10, 5.0)  # stake = 0.50
    assert em.can_add("mkt1", "tok1", 1.0) is True  # 0.50 + 1.0 = 1.50 <= 2.0


def test_can_add_at_exact_cap(em):
    em.record_fill("mkt1", "tok1", 0.10, 10.0)  # stake = 1.0
    assert em.can_add("mkt1", "tok1", 1.0) is True  # 1.0 + 1.0 = 2.0 == 2.0


def test_can_add_exceeds_cap(em):
    em.record_fill("mkt1", "tok1", 0.10, 10.0)  # stake = 1.0
    assert em.can_add("mkt1", "tok1", 1.01) is False  # 1.0 + 1.01 > 2.0


def test_record_fill_accumulates(em):
    em.record_fill("mkt1", "tok1", 0.10, 5.0)   # stake = 0.50
    em.record_fill("mkt1", "tok1", 0.20, 10.0)  # stake = 2.00
    exp = em.get("mkt1", "tok1")
    assert exp is not None
    assert abs(exp.total_stake_usdc - 2.50) < 1e-9
    assert exp.fill_count == 2


def test_record_fill_avg_price(em):
    em.record_fill("mkt1", "tok1", 0.10, 10.0)  # stake 1.0, qty 10
    em.record_fill("mkt1", "tok1", 0.20, 10.0)  # stake 2.0, qty 10
    exp = em.get("mkt1", "tok1")
    # avg = (1.0 + 2.0) / (10 + 10) = 0.15
    assert abs(exp.avg_entry_price - 0.15) < 1e-9


def test_different_tokens_independent(em):
    em.record_fill("mkt1", "tok1", 0.10, 10.0)  # fills tok1 with 1.0
    assert em.can_add("mkt1", "tok2", 2.0) is True  # tok2 untouched


def test_current_stake_zero_before_fill(em):
    assert em.current_stake("mkt1", "tok1") == 0.0


def test_cache_consistent_with_db(tmp_path):
    db = tmp_path / "test.db"
    em1 = ExposureManager(db_path=db, max_per_market=2.0)
    em1.record_fill("mkt1", "tok1", 0.10, 5.0)

    # New instance reads from DB, not cache
    em2 = ExposureManager(db_path=db, max_per_market=2.0)
    assert abs(em2.current_stake("mkt1", "tok1") - 0.50) < 1e-9
