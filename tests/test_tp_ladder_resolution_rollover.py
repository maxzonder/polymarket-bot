"""Regression tests for binary-native progress TP ladders."""

from config import BIG_SWAN_MODE
from strategy.risk_manager import RiskManager, SizedPosition


def _make_position(entry_price: float, qty: float = 100.0) -> SizedPosition:
    return SizedPosition(
        token_id="tok1",
        entry_price=entry_price,
        stake_usdc=entry_price * qty,
        token_quantity=qty,
        tp_levels=list(BIG_SWAN_MODE.tp_levels),
        moonbag_fraction=BIG_SWAN_MODE.moonbag_fraction,
        rationale="test",
    )


def test_entry_005_uses_progress_targets():
    rm = RiskManager(BIG_SWAN_MODE)
    orders = rm.build_tp_orders(_make_position(0.05))

    by_label = {o.label: o for o in orders}
    assert "tp_p10" in by_label
    assert "tp_p50" in by_label
    assert "moonbag_resolution" in by_label

    assert abs(by_label["tp_p10"].sell_price - 0.145) < 1e-9
    assert abs(by_label["tp_p50"].sell_price - 0.525) < 1e-9
    assert abs(by_label["moonbag_resolution"].sell_quantity - 70.0) < 1e-9

    total_qty = sum(o.sell_quantity for o in orders)
    assert abs(total_qty - 100.0) < 1e-9


def test_entry_010_uses_progress_targets():
    rm = RiskManager(BIG_SWAN_MODE)
    orders = rm.build_tp_orders(_make_position(0.10))

    by_label = {o.label: o for o in orders}
    assert "tp_p10" in by_label
    assert "tp_p50" in by_label
    assert "moonbag_resolution" in by_label

    assert abs(by_label["tp_p10"].sell_price - 0.19) < 1e-9
    assert abs(by_label["tp_p50"].sell_price - 0.55) < 1e-9
    assert abs(by_label["moonbag_resolution"].sell_quantity - 70.0) < 1e-9

    total_qty = sum(o.sell_quantity for o in orders)
    assert abs(total_qty - 100.0) < 1e-9
