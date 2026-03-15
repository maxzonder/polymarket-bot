import sqlite3
from dataclasses import dataclass
from pathlib import Path

DB_PATH = Path('/home/polybot/polymarket-bot/polymarket_dataset.db')
TARGETS = (5, 10, 20, 30, 40, 50, 100, 200, 500, 1000)
STAKE = 0.01
TOP = 3

@dataclass(frozen=True)
class Swan:
    market_id: str
    token_id: str
    possible_x: float
    entry_volume_usdc: float
    exit_volume_usdc: float

@dataclass(frozen=True)
class Step:
    target_x: int
    weight: float

@dataclass
class SchemeResult:
    steps: tuple
    buyable_swans: int
    total_cost_usd: float
    total_revenue_usd: float
    total_profit_usd: float
    roi_pct: float
    per_step_fill_counts: tuple


def evaluate_scheme(swans, stake, steps):
    fill_counts = [0] * len(steps)
    total_revenue = 0.0
    for swan in swans:
        cumulative_required = 0.0
        for idx, step in enumerate(steps):
            tranche_gross = stake * step.weight * step.target_x
            required_after_fill = cumulative_required + tranche_gross
            if swan.possible_x >= step.target_x and swan.exit_volume_usdc >= required_after_fill:
                total_revenue += tranche_gross
                cumulative_required = required_after_fill
                fill_counts[idx] += 1
            else:
                break
    total_cost = len(swans) * stake
    total_profit = total_revenue - total_cost
    roi_pct = (total_profit / total_cost * 100.0) if total_cost else 0.0
    return SchemeResult(tuple(steps), len(swans), total_cost, total_revenue, total_profit, roi_pct, tuple(fill_counts))


def optimize_full_exit_single(swans, stake, targets):
    results = []
    for target in targets:
        steps = (Step(target_x=target, weight=1.0),)
        results.append(evaluate_scheme(swans, stake, steps))
    results.sort(key=lambda r: (r.total_profit_usd, r.total_revenue_usd, r.roi_pct), reverse=True)
    return results


def format_steps(steps):
    return ' | '.join(f"{int(round(step.weight * 100))}% @ {step.target_x}x" for step in steps)


conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row

dates = [r['date'] for r in conn.execute('SELECT DISTINCT date FROM token_swans_v3 ORDER BY date').fetchall()]
print(f'dates={dates}')
print(f'stake={STAKE}')
print(f'targets={list(TARGETS)}')
print()

all_swans = []
for date in dates:
    rows = conn.execute(
        '''
        SELECT market_id, token_id, possible_x, entry_volume_usdc, exit_volume_usdc
        FROM token_swans_v3
        WHERE date = ? AND entry_volume_usdc >= ?
        ''',
        (date, STAKE),
    ).fetchall()
    swans = [
        Swan(
            market_id=row['market_id'],
            token_id=row['token_id'],
            possible_x=float(row['possible_x']),
            entry_volume_usdc=float(row['entry_volume_usdc']),
            exit_volume_usdc=float(row['exit_volume_usdc']),
        )
        for row in rows
    ]
    all_swans.extend(swans)
    results = optimize_full_exit_single(swans, STAKE, TARGETS)
    best = results[0]
    print(
        f"{date}\tbuyable={len(swans)}\tbest={format_steps(best.steps)}\t"
        f"profit=${best.total_profit_usd:.2f}\trevenue=${best.total_revenue_usd:.2f}\troi={best.roi_pct:.2f}%\t"
        f"fills={list(best.per_step_fill_counts)}"
    )

print('\nALL_DATES_AGGREGATED')
results = optimize_full_exit_single(all_swans, STAKE, TARGETS)
best = results[0]
print(
    f"buyable={len(all_swans)}\tbest={format_steps(best.steps)}\t"
    f"profit=${best.total_profit_usd:.2f}\trevenue=${best.total_revenue_usd:.2f}\troi={best.roi_pct:.2f}%\t"
    f"fills={list(best.per_step_fill_counts)}"
)
print('TOP3')
for i, r in enumerate(results[:TOP], 1):
    print(f"{i}\t{format_steps(r.steps)}\tprofit=${r.total_profit_usd:.2f}\trevenue=${r.total_revenue_usd:.2f}\troi={r.roi_pct:.2f}%\tfills={list(r.per_step_fill_counts)}")

conn.close()
