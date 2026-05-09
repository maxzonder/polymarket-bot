# RUNBOOK — Операционный гайд

Операционный справочник: что где запускать, в каком порядке, с какими параметрами.
Архитектурное описание — в [BOT_ARCHITECTURE.md](./BOT_ARCHITECTURE.md) и [RUNTIME_FLOW.md](./RUNTIME_FLOW.md).

---

## Обзор ботов и стратегий

| Бот / стратегия | Точка входа | Universe | Статус |
|-----------------|-------------|----------|--------|
| Swan V1 live bot | `main.py` → `bot/main_loop.py` | `swans_v2` | ⚠️ Legacy, заменён V2 |
| Swan V2 live | `v2/short_horizon/swan_live.py` | `black_swan=1` | ✅ Production |
| 15m Touch V2 live | `v2/short_horizon/swan_live.py` (другая стратегия внутри) | touch-сигналы | ✅ Production |
| V1 Tape Replay | `scripts/run_tape_dryrun.py` | `historical_tape.db` | ✅ Backtest |
| V2 15m Replay | `v2/short_horizon/short_horizon/replay_runner.py` | captured responses | ✅ Backtest |

**Два независимых replay-раннера:** V1 (tape, `historical_tape.db`) используется для swan backtest;
V2 replay (captured responses) — для 15m touch. Объединять сейчас не нужно.

---

## 1. V1 Tape Replay — Swan backtest

Офлайн-симуляция на `historical_tape.db`. Основной инструмент валидации параметров swan.

### Запуск

```bash
python scripts/run_tape_dryrun.py \
  --start 2025-08-01 --end 2026-04-01 \
  --mode big_swan_mode \
  --out /path/to/output_dir
```

| Параметр | Описание | Default |
|----------|----------|---------|
| `--start` / `--end` | Диапазон дат (YYYY-MM-DD) | обязательны |
| `--mode` | `big_swan_mode` / `balanced_mode` / `fast_tp_mode` / `small_swan_mode` | `big_swan_mode` |
| `--limit-markets N` | Ограничить число рынков (быстрая проверка) | без ограничения |
| `--batch-seconds N` | Размер батча в секундах | 300 |
| `--tape-db PATH` | Путь к historical_tape.db | из `DATA_DIR` |
| `--out PATH` | Директория результатов | `DATA_DIR/replay_runs/tape_dryrun_TIMESTAMP` |

### Анализ результатов

```bash
python scripts/honest_replay_analyze.py --run-dir /path/to/output_dir
python scripts/honest_replay_analyze.py --run-dir /path/to/output_dir --top 20
python scripts/honest_replay_analyze.py --compare /path/run1 /path/run2
```

### Батч-тест матрицы параметров

```bash
python scripts/run_tape_matrix.py
```

Запускает сетку конфигов и генерирует HTML-отчёт.

### Предварительная оптимизация параметров (до tape replay)

```bash
python scripts/optimize_big_swan_params.py
```

Офлайн-оптимизатор по `swans_v2` путям. Не causal — только для первичного отбора параметров
перед честным replay.

### Данные

| Путь | Содержимое |
|------|-----------|
| `$POLYMARKET_DATA_DIR/historical_tape.db` | Сырые трейды |
| `$POLYMARKET_DATA_DIR/replay_runs/` | Результаты прогонов |

---

## 2. V2 Swan Live Bot

Production бот. V2 CTF Exchange + pUSD. Заменяет устаревший `main.py`.

### Dry-run (paper trading, без реальных ордеров)

```bash
python -m v2.short_horizon.swan_live
```

### Live (реальные ордера)

```bash
python -m v2.short_horizon.swan_live --execution-mode live
```

### Live с периодическим redeem

```bash
python -m v2.short_horizon.swan_live --execution-mode live --redeem-interval 3600
```

### В tmux (рекомендуется для длительных прогонов)

```bash
tmux new-session -d -s swan_v2 \
  "cd /home/polybot/claude-polymarket && python -m v2.short_horizon.swan_live"
```

### Экстренная отмена ордеров

```bash
python -m v2.short_horizon.cancel_swan_orders
```

### Конфиг

`v2/short_horizon/short_horizon/config.py` — `ShortHorizonConfig`, `RiskConfig`, `SpotDislocationConfig`.

---

## 3. V2 15m Touch Live Bot

Запускается тем же entrypoint, что и Swan V2, но с другой стратегией внутри.
Подробности — в `v2/short_horizon/short_horizon/strategies/short_horizon_15m_touch_v1.py`.

### V2 15m Replay (captured responses)

Использует сохранённые ответы venue, а не `historical_tape.db`.
CLI-скрипт пока отсутствует — точка входа: `v2/short_horizon/short_horizon/replay_runner.py`.

---

## 4. V2 Black Swan Strategy

Стратегия на основе строгого фильтра `black_swan=1` (issue #180).

Universe: `black_swan=1` из `polymarket_dataset.db` — токены, которые были доступны по копейкам
и реализовались в $1.

Аналитика по universe — в issue #180 (69% hit-rate, avg 66.7x, ~17 событий/день).

Стратегический файл: `v2/short_horizon/short_horizon/strategies/black_swan_strategy_v1.py`.
Screener (фильтрация кандидатов по `black_swan=1`) — добавляется отдельно к swan_live.py.

---

## 5. Paper Trading мониторинг (V1)

```bash
# Отчёт по live dry-run результатам V1
python scripts/paper_trading_report.py

# Баланс paper trading
python scripts/paper_balance.py status
python scripts/paper_balance.py topup 100
python scripts/paper_balance.py history

# Постмортем конкретного прогона
python scripts/postmortem_probe.py

# Уведомление о завершении probe
python scripts/notify_probe_completed.py
```

---

## 6. ML Pipeline (ежедневно, 04:00 UTC)

```bash
python -m pipeline.daily_pipeline
```

Шаги:
1. Скачать новые рынки (Gamma)
2. Запустить `swan_analyzer` на закрытых рынках → обновить `swans_v2`, `black_swan`
3. Перестроить `feature_mart_v1_1`
4. Материализовать `ml_outcomes` + `ml_rejected_outcomes`
5. Рекалибровать scorer thresholds

---

## 7. Разовые операции

```bash
# Backfill данных
python scripts/backfill_market_resolutions.py
python scripts/backfill_new_fields.py
python scripts/backfill_spot_features.py

# Построить исторический tape DB
python scripts/build_historical_tape_db.py

# Проверки
python scripts/check_filter_coverage.py
python scripts/validate_clob_pricing_v1_1.py
python scripts/execution_health.py

# Анализ
python scripts/analyze_token_side_bias.py
python scripts/analyze_maker_fees_and_adverse_selection.py
python scripts/measure_live_depth_and_survival.py
```

---

## Конфигурация

| Файл | Назначение |
|------|-----------|
| `config.py` | V1 режимы (big_swan_mode, balanced_mode, fast_tp_mode) и пороги — используется replay и pipeline |
| `v2/short_horizon/short_horizon/config.py` | V2 конфиг (ShortHorizonConfig, RiskConfig) |
| `utils/paths.py` | Пути к данным (`DATA_DIR`, `DB_PATH`) |
| `.env` | Приватные ключи и API-токены (не коммитить) |

---

## Базы данных

| Файл | Содержимое |
|------|-----------|
| `$POLYMARKET_DATA_DIR/polymarket_dataset.db` | markets, tokens, swans_v2, black_swan, feature_mart_v1_1, ml_outcomes |
| `$POLYMARKET_DATA_DIR/positions.db` | resting_orders, positions, screener_log (операционная) |
| `$POLYMARKET_DATA_DIR/historical_tape.db` | Сырые трейды для tape replay |
| `$POLYMARKET_DATA_DIR/replay_runs/` | Результаты replay прогонов |
