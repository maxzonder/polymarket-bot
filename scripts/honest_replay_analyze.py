"""Analyze an honest replay run directory or compare two runs.

Usage:
    python3 scripts/honest_replay_analyze.py --run-dir /path/to/replay_run
    python3 scripts/honest_replay_analyze.py --run-dir /path/to/replay_run --top 15
    python3 scripts/honest_replay_analyze.py --compare /path/to/run1 /path/to/run2
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


QUICK_ROWS = [
    ("universe", "universe", "int"),
    ("passed_screener", "passed_screener", "int"),
    ("entry_fills", "entry_fills", "int"),
    ("tp_fills", "tp_fills", "int"),
    ("fill_rate", "fill_rate_pct", "pct1"),
    ("positions", "positions", "int"),
    ("resolved", "resolved_positions", "int"),
    ("open", "open_positions", "int"),
    ("winners", "winners", "int"),
    ("losers", "losers", "int"),
    ("win_rate", "win_rate_pct", "pct1"),
    ("markets", "distinct_markets", "int"),
    ("tokens", "distinct_tokens", "int"),
    ("stake", "stake", "float4"),
    ("pnl", "pnl", "float4"),
    ("roi_on_stake", "roi_on_stake_pct", "pct1"),
    ("initial_balance", "initial_balance", "float4"),
    ("final_equity", "final_equity", "float4"),
    ("bankroll_return", "bankroll_return_pct", "pct1"),
    ("avg_stake", "avg_stake", "float4"),
    ("avg_entry", "avg_entry_price", "float6"),
    ("avg_peak_x", "avg_peak_x", "float4"),
    ("max_peak_x", "max_peak_x", "float4"),
]


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


def _parse_summary_file(path: Path) -> dict:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")

    def _m(pattern: str, cast=int):
        found = re.search(pattern, text, re.MULTILINE)
        if not found:
            return None
        value = found.group(1).replace(",", "")
        return cast(value)

    return {
        "universe": _m(r"Universe \(market/token pairs\)\s*:\s*([0-9,]+)", int),
        "passed_screener": _m(r"Passed screener\s*:\s*([0-9,]+)", int),
        "entry_fills": _m(r"Entry fills\s*:\s*([0-9,]+)", int),
        "tp_fills": _m(r"TP fills\s*:\s*([0-9,]+)", int),
        "positions_opened": _m(r"Positions opened\s*:\s*([0-9,]+)", int),
    }


def _fmt(value, kind: str) -> str:
    if value is None:
        return "n/a"
    if kind == "int":
        return str(int(value))
    if kind == "pct1":
        return f"{float(value):.1f}%"
    if kind == "float6":
        return f"{float(value):.6f}"
    if kind == "float4":
        return f"{float(value):.4f}"
    return str(value)


def _metric(label: str, value, desc: str) -> None:
    print(f"{label}: {value} ({desc})")


def _load_config_snapshot(run_dir: Path) -> dict:
    path = run_dir / "config_snapshot.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _collect_run_data(run_dir: Path) -> dict:
    run_dir = run_dir.expanduser().resolve()
    positions_db = run_dir / "positions.db"
    paper_db = run_dir / "paper_trades.db"
    summary_file = run_dir / "summary.txt"

    if not positions_db.exists():
        raise SystemExit(f"positions.db not found: {positions_db}")
    if not paper_db.exists():
        raise SystemExit(f"paper_trades.db not found: {paper_db}")

    con = _conn(positions_db)
    cur = con.cursor()
    con2 = _conn(paper_db)
    cur2 = con2.cursor()

    summary_metrics = _parse_summary_file(summary_file)

    overall = _one(cur, '''
        select
          count(*) as positions,
          round(sum(entry_size_usdc),6) as stake,
          round(sum(coalesce(realized_pnl,0)),6) as pnl,
          round(sum(coalesce(realized_pnl,0))/sum(entry_size_usdc),6) as roi_on_stake,
          round(avg(entry_size_usdc),6) as avg_stake,
          round(avg(entry_price),6) as avg_entry_price,
          round(avg(coalesce(peak_x, 0)),6) as avg_peak_x,
          round(max(coalesce(peak_x, 0)),6) as max_peak_x
        from positions
    ''')

    winners = cur.execute("select count(*) from positions where coalesce(realized_pnl,0) > 0").fetchone()[0]
    losers = cur.execute("select count(*) from positions where coalesce(realized_pnl,0) <= 0").fetchone()[0]
    open_positions = cur.execute("select count(*) from positions where status='open'").fetchone()[0]
    resolved_positions = cur.execute("select count(*) from positions where status='resolved'").fetchone()[0]
    distinct_markets = cur.execute("select count(distinct market_id) from positions").fetchone()[0]
    distinct_tokens = cur.execute("select count(distinct token_id) from positions").fetchone()[0]

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

    passed_screener = summary_metrics.get("passed_screener")
    if passed_screener is None:
        passed_screener = cur.execute("select count(distinct candidate_id) from resting_orders where candidate_id is not null").fetchone()[0]

    entry_fills = summary_metrics.get("entry_fills")
    if entry_fills is None:
        entry_fills = cur.execute("select count(*) from paper_balance_events where note like 'entry fill %'").fetchone()[0]

    tp_fills = summary_metrics.get("tp_fills")
    if tp_fills is None:
        tp_fills = cur.execute("select count(*) from paper_balance_events where note like 'tp fill %'").fetchone()[0]

    universe = summary_metrics.get("universe")
    fill_rate_pct = (entry_fills / passed_screener * 100.0) if passed_screener else None
    win_rate_pct = (winners / resolved_positions * 100.0) if resolved_positions else None

    metrics = {
        "universe": universe,
        "passed_screener": passed_screener,
        "entry_fills": entry_fills,
        "tp_fills": tp_fills,
        "fill_rate_pct": fill_rate_pct,
        "positions": overall.get("positions"),
        "resolved_positions": resolved_positions,
        "open_positions": open_positions,
        "winners": winners,
        "losers": losers,
        "win_rate_pct": win_rate_pct,
        "distinct_markets": distinct_markets,
        "distinct_tokens": distinct_tokens,
        "stake": overall.get("stake"),
        "pnl": overall.get("pnl"),
        "roi_on_stake_pct": (overall.get("roi_on_stake") * 100.0) if overall.get("roi_on_stake") is not None else None,
        "initial_balance": initial_balance,
        "final_equity": final_equity,
        "bankroll_return_pct": bankroll_return_pct,
        "avg_stake": overall.get("avg_stake"),
        "avg_entry_price": overall.get("avg_entry_price"),
        "avg_peak_x": overall.get("avg_peak_x"),
        "max_peak_x": overall.get("max_peak_x"),
    }

    data = {
        "run_dir": run_dir,
        "positions_db": positions_db,
        "paper_db": paper_db,
        "summary_file": summary_file if summary_file.exists() else None,
        "summary_metrics": summary_metrics,
        "overall": overall,
        "metrics": metrics,
        "balance": {
            "final_balance_from_events": round(bal, 6),
            "open_position_cost_basis": round(float(open_cost), 6),
            "final_equity": round(final_equity, 6),
            "bankroll_return_pct": round(bankroll_return_pct, 6) if bankroll_return_pct is not None else None,
            "min_balance": round(min_bal[0], 6),
            "min_at": min_bal[1],
            "max_balance": round(max_bal[0], 6),
            "max_at": max_bal[1],
        },
        "config_snapshot": _load_config_snapshot(run_dir),
        "con": con,
        "cur": cur,
        "con2": con2,
        "cur2": cur2,
    }
    return data


def _close_data(data: dict) -> None:
    data["con"].close()
    data["con2"].close()


def _print_ascii_table(headers: list[str], rows: list[list[str]], fenced: bool = False) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    pad = 2

    def line(sep: str = '-') -> str:
        return '+' + '+'.join(sep * (w + pad * 2) for w in widths) + '+'

    def render_row(cells: list[str]) -> str:
        return '|' + '|'.join((' ' * pad) + cells[i].ljust(widths[i]) + (' ' * pad) for i in range(len(cells))) + '|'

    lines = []
    lines.append(line('-'))
    lines.append(render_row(headers))
    lines.append(line('='))
    for row in rows:
        lines.append(render_row(row))
        lines.append(line('-'))

    if fenced:
        print('```text')
    print('\n'.join(lines))
    if fenced:
        print('```')


def _flatten(prefix: str, value, out: dict[str, object]) -> None:
    if isinstance(value, dict):
        for k in sorted(value):
            _flatten(f"{prefix}.{k}" if prefix else str(k), value[k], out)
    else:
        out[prefix] = value


def _compare_runs(run1: Path, run2: Path) -> None:
    data1 = _collect_run_data(run1)
    data2 = _collect_run_data(run2)
    try:
        print(f"run1={data1['run_dir']}")
        print(f"run2={data2['run_dir']}")
        print()

        _print_section("compare")
        print(f"run1_name: {data1['run_dir'].name}")
        print(f"run2_name: {data2['run_dir'].name}")
        print()
        for label, key, kind in QUICK_ROWS:
            v1 = _fmt(data1['metrics'].get(key), kind)
            v2 = _fmt(data2['metrics'].get(key), kind)
            print(f"{label}: {v1} | {v2}")

        _print_section("differing config values")
        snap1 = data1.get("config_snapshot") or {}
        snap2 = data2.get("config_snapshot") or {}
        if not snap1 or not snap2:
            print({
                "run1_has_snapshot": bool(snap1),
                "run2_has_snapshot": bool(snap2),
                "note": "config_snapshot.json missing in one or both runs",
            })
            return

        flat1: dict[str, object] = {}
        flat2: dict[str, object] = {}
        _flatten("bot_config", snap1.get("bot_config", {}), flat1)
        _flatten("mode_config", snap1.get("mode_config", {}), flat1)
        _flatten("bot_config", snap2.get("bot_config", {}), flat2)
        _flatten("mode_config", snap2.get("mode_config", {}), flat2)

        keys = sorted(set(flat1) | set(flat2))
        diff_rows = []
        def _compact(value):
            s = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            return s if len(s) <= 48 else s[:45] + "..."

        for key in keys:
            v1 = flat1.get(key)
            v2 = flat2.get(key)
            if v1 != v2:
                diff_rows.append([key, _compact(v1), _compact(v2)])

        if not diff_rows:
            print("no config differences")
        else:
            print(f"run1_name: {data1['run_dir'].name}")
            print(f"run2_name: {data2['run_dir'].name}")
            print()
            for key, v1, v2 in diff_rows:
                print(f"{key}: {v1} | {v2}")
    finally:
        _close_data(data1)
        _close_data(data2)


def _print_single_run(data: dict, top: int) -> None:
    cur = data["cur"]
    cur2 = data["cur2"]
    m = data["metrics"]

    print(f"run_dir={data['run_dir']}")
    print(f"positions_db={data['positions_db']}")
    print(f"paper_db={data['paper_db']}")
    print(f"summary_file={data['summary_file'] if data['summary_file'] else 'n/a'}")

    _print_section("quick summary")
    desc = {
        "universe": "кол-во просмотренных market/token pairs",
        "passed_screener": "сколько кандидатов прошли screener",
        "entry_fills": "сколько entry fills произошло",
        "tp_fills": "сколько TP fills произошло",
        "fill_rate_pct": "entry fills / passed screener",
        "positions": "сколько позиций открылось",
        "resolved_positions": "сколько позиций уже закрыто",
        "open_positions": "сколько позиций осталось открыто",
        "winners": "сколько позиций дало плюс",
        "losers": "сколько позиций дало ноль/минус",
        "win_rate_pct": "доля winners среди resolved",
        "distinct_markets": "уникальные рынки с позицией",
        "distinct_tokens": "уникальные токены с позицией",
        "stake": "суммарный вложенный stake",
        "pnl": "суммарный realized pnl",
        "roi_on_stake_pct": "pnl / total stake",
        "initial_balance": "стартовый paper balance",
        "final_equity": "cash плюс cost basis open positions",
        "bankroll_return_pct": "доходность на стартовый bankroll",
        "avg_stake": "средний stake на позицию",
        "avg_entry_price": "средняя цена входа",
        "avg_peak_x": "средний достигнутый peak_x",
        "max_peak_x": "максимальный peak_x в run",
    }
    kind_by_key = {key: kind for _, key, kind in QUICK_ROWS}
    for _, key, kind in QUICK_ROWS:
        _metric(key, _fmt(m.get(key), kind), desc[key])

    _print_section("overall")
    print({
        **data["overall"],
        **m,
    })

    _print_section("config snapshot")
    print(data.get("config_snapshot") or {"note": "config_snapshot.json not found"})

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
    print(data["balance"])

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
        limit {int(top)}
    '''))

    _print_section("top losing markets")
    _print_rows(cur.execute(f'''
        select market_id, count(*) c, sum(is_winner) winners,
               round(sum(entry_size_usdc),6) stake,
               round(sum(realized_pnl),6) pnl
        from positions
        group by market_id
        order by pnl asc
        limit {int(top)}
    '''))

    _print_section("top winners")
    _print_rows(cur.execute(f'''
        select position_id, market_id, token_id, outcome_name,
               entry_price, entry_size_usdc, realized_pnl, peak_x, is_winner
        from positions
        order by realized_pnl desc
        limit {int(top)}
    '''))

    _print_section("top losers")
    _print_rows(cur.execute(f'''
        select position_id, market_id, token_id, outcome_name,
               entry_price, entry_size_usdc, realized_pnl, peak_x, is_winner
        from positions
        order by realized_pnl asc
        limit {int(top)}
    '''))


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze an honest replay run directory")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-dir", help="Replay run directory containing positions.db and paper_trades.db")
    group.add_argument("--compare", nargs=2, metavar=("RUN1", "RUN2"), help="Compare two replay run directories")
    ap.add_argument("--top", type=int, default=12, help="How many top winners/losers/markets to show")
    args = ap.parse_args()

    if args.compare:
        _compare_runs(Path(args.compare[0]), Path(args.compare[1]))
        return

    data = _collect_run_data(Path(args.run_dir))
    try:
        _print_single_run(data, args.top)
    finally:
        _close_data(data)


if __name__ == "__main__":
    main()
