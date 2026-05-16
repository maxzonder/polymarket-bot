# How to start black_swan paper bot

Правило: параметры стратегии и universe selector должны жить в `config.py`. CLI-флаги нужны только для выбора режима запуска, run/db paths и включения selector boundary.

## Правильный paper restart

На prod-сервере:

```bash
cd /home/polybot/claude-polymarket
set -a
source ./.env
set +a
export POLYMARKET_DATA_DIR=/home/polybot/.polybot
export POLYBOT_DATA_DIR=/home/polybot/.polybot

./.venv/bin/python -m v2.short_horizon.swan_live \
  --strategy black_swan \
  --execution-mode dry_run \
  --db-path /home/polybot/.polybot/swan_v2/black_swan_v1_paper_20260515T134005Z.sqlite3 \
  --run-id black_swan_v1_paper_20260515T134005Z \
  --apply-universe-selector \
  > /home/polybot/.polybot/logs/paper_bs3_20260515T134005Z.log 2>&1
```

Через tmux:

```bash
tmux new-session -d -s paper_bs3 "cd /home/polybot/claude-polymarket && set -a && source ./.env && set +a && export POLYMARKET_DATA_DIR=/home/polybot/.polybot && export POLYBOT_DATA_DIR=/home/polybot/.polybot && ./.venv/bin/python -m v2.short_horizon.swan_live --strategy black_swan --execution-mode dry_run --db-path /home/polybot/.polybot/swan_v2/black_swan_v1_paper_20260515T134005Z.sqlite3 --run-id black_swan_v1_paper_20260515T134005Z --apply-universe-selector > /home/polybot/.polybot/logs/paper_bs3_20260515T134005Z.log 2>&1"
```

## Какие значения должны прийти из config.py

Для `BLACK_SWAN_MODE`:

- `stake_per_level=1.0`
- `universe_selector_min_volume_usdc=100.0`
- `min_hours_to_close=0.4`
- `min_hours_to_close_fraction_of_duration=0.10`
- `max_hours_to_close=168.0`
- `min_total_duration_hours=50/60`
- `min_market_score=0.25`

Для universe selector без CLI overrides:

- `min_volume_usdc=100.0`
- `reject_random_walk=false` — legacy CLI/config knob, не должен быть pre-WS hard filter
- `max_markets=0`
- `max_tokens=0`
- `max_markets_per_category=0`

## Не добавлять в штатный запуск

Эти флаги перебивают config и меняют смысл эксперимента:

- `--stake-per-level 1.0`
  - уже задано в `BLACK_SWAN_MODE.stake_per_level`
- `--universe-selector-min-volume-usdc 1000`
  - старый validation override; штатно должно быть `100.0` из config
- `--universe-selector-reject-random-walk`
  - legacy validation flag; штатно random-walk/category/catalyst не должны быть pre-WS фильтрами или score penalty
- `--universe-selector-max-markets 200`
- `--universe-selector-max-tokens 400`
- `--universe-selector-max-markets-per-category 75`
  - старые validation caps; штатно caps выключены (`0`)

## Проверка после старта

Команда процесса не должна содержать лишних `--universe-selector-*`:

```bash
pgrep -af "swan_live --strategy black_swan"
```

В логе должны быть такие effective values:

```text
swan_live_stake_configured:
  configured_stake_per_level=1.0
  effective_stake_per_level=1.0

ws_universe_selector_configured:
  min_volume_usdc=100.0
  reject_random_walk=false
  max_markets=0
  max_tokens=0
  max_markets_per_category=0
```

Если эти значения другие — запуск перебит CLI-флагами или код/config не тот.
