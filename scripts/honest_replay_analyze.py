"""Analyze an honest replay run directory.

Usage:
    python3 scripts/honest_replay_analyze.py --run-dir /path/to/replay_run
    python3 scripts/honest_replay_analyze.py --run-dir /path/to/replay_run --top 15
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _conn(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _print_section(title: str) -> None:
    print(f"\n== {title} ==")


def _print_rows(rows) -> None:
    for row in rows:
        print(dict(row))


def _one(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> dict:
    row = cur.execute(sql, params).fetchone()
    return dict(row) if row is not None else {}


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze an honest replay run directory")
    ap.add_argument("--run-dir", required=True, help="Replay run directory containing positions.db and paper_trades.db")
    ap.add_argument("--top", type=int, default=12, help="How many top winners/losers/markets to show")
    args = ap.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    positions_db = run_dir / "positions.db"
    paper_db = run_dir / "paper_trades.db"

    if not positions_db.exists():
        raise SystemExit(f"positions.db not found: {positions_db}")
    if not paper_db.exists():
        raise SystemExit(f"paper_trades.db not found: {paper_db}")

    con = _conn(positions_db)
    cur = con.cursor()
    con2 = _conn(paper_db)
    cur2 = con2.cursor()

    print(f"run_dir={run_dir}")
    print(f"positions_db={positions_db}")
    print(f"paper_db={paper_db}")
    overall = _one(cur, '''
        select
          count(*) as positions,
          round(sum(entry_size_usdc),6) as stake,
          round(sum(coalesce(realized_pnl,0)),6) as pnl,
          round(sum(coalesce(realized_pnl,0))/sum(entry_size_usdc),6) as roi_on_stake,
          round(avg(entry_size_usdc),6) as avg_stake,
          round(avg(entry_price),6) as avg_entry_price
        from positions
    ''')

    _print_section("winners / losers")
    _print_rows(cur.execute('''
        select
          is_winner,
          count(*) c,
          round(sum(entry_size_usdc),6) stake,
          round(sum(coalesce(realized_pnl,0)),6) pnl,
          round(avg(coalesce(realized_pnl,0)),6) avg_pnl
        from positions
        group by is_winner
        order by is_winner
    '''))

    _print_section("entry price breakdown")
    _print_rows(cur.execute('''
        select
          entry_price,
          count(*) c,
          sum(is_winner) winners,
          round(1.0*sum(is_winner)/count(*),6) win_rate,
          round(sum(entry_size_usdc),6) stake,
          round(sum(coalesce(realized_pnl,0)),6) pnl,
          round(sum(coalesce(realized_pnl,0))/sum(entry_size_usdc),6) roi_on_stake,
          round(avg(coalesce(realized_pnl,0)),6) avg_pnl
        from positions
        group by entry_price
        order by entry_price
    '''))

    _print_section("cash balance")
    _print_rows(cur.execute('select * from paper_balance'))

    rows = cur.execute('select ts, delta_usdc, note from paper_balance_events order by ts, id').fetchall()
    bal = 0.0
    min_bal = (10**18, None)
    max_bal = (-10**18, None)
    for r in rows:
        bal += r['delta_usdc']
        if bal < min_bal[0]:
            min_bal = (bal, dict(r))
        if bal > max_bal[0]:
            max_bal = (bal, dict(r))
    initial_balance = 0.0
    initial_row = cur.execute("""
        select delta_usdc, note
        from paper_balance_events
        where note like 'initial funding%' or note like 'initial balance%'
        order by id asc
        limit 1
    """).fetchone()
    if initial_row is not None:
        initial_balance = float(initial_row['delta_usdc'] or 0.0)
    open_cost = cur.execute("select coalesce(sum(entry_size_usdc), 0) from positions where status='open'").fetchone()[0] or 0.0
    final_equity = bal + float(open_cost)
    bankroll_return_pct = ((final_equity - initial_balance) / initial_balance * 100.0) if initial_balance > 0 else None

    overall['initial_balance'] = round(initial_balance, 6)
    overall['final_equity'] = round(final_equity, 6)
    overall['bankroll_return_pct'] = round(bankroll_return_pct, 6) if bankroll_return_pct is not None else None

    _print_section("overall")
    print(overall)

    print({
        'final_balance_from_events': round(bal, 6),
        'open_position_cost_basis': round(float(open_cost), 6),
        'final_equity': round(final_equity, 6),
        'bankroll_return_pct': round(bankroll_return_pct, 6) if bankroll_return_pct is not None else None,
        'min_balance': round(min_bal[0], 6),
        'min_at': min_bal[1],
        'max_balance': round(max_bal[0], 6),
        'max_at': max_bal[1],
    })

    _print_section("balance event classes")
    _print_rows(cur.execute('''
        select
          case
            when note like 'initial funding%' or note like 'initial balance%' then 'initial_funding'
            when note like 'entry fill %' then 'entry_fill'
            when note like 'tp fill %' then 'tp_fill'
            when note like 'resolution % winner=True' then 'resolution_win'
            when note like 'resolution % winner=False' then 'resolution_loss'
            else 'other'
          end as cls,
          count(*) c,
          round(sum(delta_usdc),6) delta,
          round(avg(delta_usdc),6) avg_delta
        from paper_balance_events
        group by cls
        order by c desc
    '''))

    _print_section("tp orders by label/status")
    _print_rows(cur.execute('''
        select label, status, count(*) c,
               round(sum(sell_quantity),6) qty,
               round(avg(sell_price),6) avg_price,
               round(sum(filled_quantity),6) filled_qty
        from tp_orders
        group by label, status
        order by label, status
    '''))

    _print_section("paper orders by side/status")
    _print_rows(cur2.execute('''
        select side, status, count(*) c,
               round(sum(size),6) size,
               round(sum(coalesce(filled_size,0)),6) filled
        from paper_orders
        group by side, status
        order by side, status
    '''))

    _print_section("paper pnl")
    print(_one(cur2, '''
        select count(*) c,
               round(sum(pnl_usdc),6) pnl,
               round(avg(pnl_usdc),6) avg_pnl,
               round(min(pnl_usdc),6) min_pnl,
               round(max(pnl_usdc),6) max_pnl
        from paper_pnl
    '''))

    _print_section("resting orders by status")
    _print_rows(cur.execute('''
        select status, count(*) c,
               round(sum(size),6) qty,
               round(sum(filled_quantity),6) filled
        from resting_orders
        group by status
        order by status
    '''))

    _print_section("cleanup consistency")
    print({
        'resting_live': cur.execute("select count(*) from resting_orders where status='live'").fetchone()[0],
        'resting_cancelled': cur.execute("select count(*) from resting_orders where status='cancelled'").fetchone()[0],
        'resting_matched': cur.execute("select count(*) from resting_orders where status='matched'").fetchone()[0],
        'paper_buy_live': cur2.execute("select count(*) from paper_orders where side='BUY' and status='live'").fetchone()[0],
        'paper_buy_cancelled': cur2.execute("select count(*) from paper_orders where side='BUY' and status='cancelled'").fetchone()[0],
        'paper_buy_matched': cur2.execute("select count(*) from paper_orders where side='BUY' and status='matched'").fetchone()[0],
    })

    _print_section("top winning markets")
    _print_rows(cur.execute(f'''
        select market_id, count(*) c, sum(is_winner) winners,
               round(sum(entry_size_usdc),6) stake,
               round(sum(realized_pnl),6) pnl
        from positions
        group by market_id
        having pnl > 0
        order by pnl desc
        limit {int(args.top)}
    '''))

    _print_section("top losing markets")
    _print_rows(cur.execute(f'''
        select market_id, count(*) c, sum(is_winner) winners,
               round(sum(entry_size_usdc),6) stake,
               round(sum(realized_pnl),6) pnl
        from positions
        group by market_id
        order by pnl asc
        limit {int(args.top)}
    '''))

    _print_section("top winners")
    _print_rows(cur.execute(f'''
        select position_id, market_id, token_id, outcome_name,
               entry_price, entry_size_usdc, realized_pnl, peak_x, is_winner
        from positions
        order by realized_pnl desc
        limit {int(args.top)}
    '''))

    _print_section("worst positions")
    _print_rows(cur.execute(f'''
        select position_id, market_id, token_id, outcome_name,
               entry_price, entry_size_usdc, realized_pnl, peak_x, is_winner
        from positions
        order by realized_pnl asc
        limit {int(args.top)}
    '''))

    con.close()
    con2.close()


if __name__ == "__main__":
    main()
