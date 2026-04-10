from __future__ import annotations

import sqlite3

from config import BotConfig


def load_all_markets(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
    config: BotConfig,
) -> list[dict]:
    """
    Load all markets closing within the simulation window, with their tokens.
    Applies static screener filters:
      - volume between config min/max
      - end_date within [start_ts, end_ts + buffer for long markets]
      - excludes markets that closed before start_ts
    """
    mc = config.mode_config
    close_buffer = int(mc.max_hours_to_close * 3600)

    rows = conn.execute(
        """
        SELECT
            m.id           AS market_id,
            m.question,
            m.category,
            m.volume,
            m.end_date,
            m.start_date,
            m.duration_hours,
            m.neg_risk,
            m.neg_risk_market_id,
            COALESCE(m.comment_count, 0) AS comment_count,
            t.token_id,
            t.outcome_name,
            t.is_winner
        FROM markets m
        JOIN tokens t ON m.id = t.market_id
        WHERE m.end_date  >= :start
          AND m.end_date  <= :end_buf
          AND m.volume    >= :vol_min
          AND m.volume    <= :vol_max
        ORDER BY m.end_date ASC
        """,
        {
            "start": start_ts,
            "end_buf": end_ts + close_buffer,
            "vol_min": config.min_volume_usdc,
            "vol_max": config.max_volume_usdc,
        },
    ).fetchall()

    return [dict(r) for r in rows]
