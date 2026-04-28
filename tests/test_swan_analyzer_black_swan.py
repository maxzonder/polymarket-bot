import sqlite3

from analyzer.swan_analyzer import analyze_token, init_db


def _trade(ts: int, price: float, size: float = 100.0) -> dict:
    return {"timestamp": ts, "price": price, "size": size}


def test_black_swan_requires_buyable_penny_floor_fast_reprice_and_winner():
    start = 1_000_000
    end = start + 24 * 3600
    trades = [
        _trade(start + 60, 0.40, 10),
        _trade(start + 600, 0.04, 100),
        _trade(start + 660, 0.035, 100),
        _trade(start + 900, 0.22, 80),
        _trade(start + 960, 0.25, 80),
        _trade(start + 1800, 0.70, 50),
    ]

    result = analyze_token(
        trades,
        is_winner=1,
        buy_price_threshold=0.20,
        min_buy_volume=1.0,
        min_sell_volume=30.0,
        min_real_x=5.0,
        market_start_ts=start,
        market_end_ts=end,
    )

    assert result is not None
    assert result["black_swan"] == 1
    assert result["black_swan_reason"] == "pass"
    assert result["buy_phase"] == "opening_10pct"
    assert result["buy_time_to_close_s"] == end - (start + 660)
    assert result["shock_price"] == 0.22
    assert result["shock_x"] > 6
    assert result["shock_volume"] >= 30
    assert result["shock_trade_count"] >= 2


def test_black_swan_flags_last_seconds_as_swan_but_not_black_swan():
    start = 2_000_000
    end = start + 3600
    trades = [
        _trade(end - 20, 0.04, 100),
        _trade(end - 15, 0.03, 100),
        _trade(end - 10, 0.22, 100),
        _trade(end - 5, 0.25, 100),
    ]

    result = analyze_token(
        trades,
        is_winner=1,
        buy_price_threshold=0.20,
        min_buy_volume=1.0,
        min_sell_volume=30.0,
        min_real_x=5.0,
        market_start_ts=start,
        market_end_ts=end,
    )

    assert result is not None
    assert result["max_traded_x"] > 30
    assert result["black_swan"] == 0
    assert result["buy_phase"] == "final_seconds"
    assert "not_last_seconds" in result["black_swan_reason"]


def test_black_swan_requires_resolution_winner():
    start = 3_000_000
    end = start + 24 * 3600
    trades = [
        _trade(start + 600, 0.04, 100),
        _trade(start + 660, 0.035, 100),
        _trade(start + 900, 0.22, 100),
        _trade(start + 960, 0.25, 100),
    ]

    result = analyze_token(
        trades,
        is_winner=0,
        buy_price_threshold=0.20,
        min_buy_volume=1.0,
        min_sell_volume=1.0,
        min_real_x=5.0,
        market_start_ts=start,
        market_end_ts=end,
    )

    assert result is not None
    assert result["black_swan"] == 0
    assert "winner" in result["black_swan_reason"]


def test_init_db_migrates_black_swan_columns():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE swans_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_id TEXT NOT NULL,
            market_id TEXT NOT NULL,
            date TEXT NOT NULL,
            buy_price_threshold REAL NOT NULL,
            min_buy_volume REAL NOT NULL,
            min_sell_volume REAL NOT NULL,
            min_real_x REAL NOT NULL,
            buy_min_price REAL NOT NULL,
            buy_volume REAL NOT NULL,
            buy_trade_count INTEGER NOT NULL,
            buy_ts_first INTEGER NOT NULL,
            buy_ts_last INTEGER NOT NULL,
            sell_volume REAL NOT NULL,
            max_price_in_history REAL NOT NULL,
            last_price_in_history REAL NOT NULL,
            is_winner INTEGER NOT NULL DEFAULT 0,
            max_traded_x REAL NOT NULL,
            payout_x REAL NOT NULL,
            UNIQUE(token_id, date)
        )
        """
    )

    init_db(conn)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(swans_v2)")}

    assert "black_swan" in columns
    assert "buy_phase" in columns
    assert "shock_x" in columns
