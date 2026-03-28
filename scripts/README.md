# scripts/

Справочник по всем скриптам. Запускаются вручную или через `daily_pipeline.py`.

---

## Ежедневный пайплайн

### `daily_pipeline.py`
Оркестратор всех ML-шагов. **Запускается автоматически ботом каждый день в 04:00 UTC**
через `_daily_pipeline_loop` в `bot/main_loop.py` — cron не нужен.
Порядок шагов: `analyzer` → `feature_mart_v1_1` → `feature_mart` → `ml_outcomes` → `rejected_outcomes` → `recalibrate`.
После успешного прогона бот автоматически рефрешит `MarketScorer`.

Ручной запуск (отладка / разовый прогон):
```bash
python scripts/daily_pipeline.py                             # все шаги
python scripts/daily_pipeline.py --step feature_mart_v1_1   # один шаг
```

---

## Сбор и анализ данных

### `../data_collector/data_collector_and_parsing.py`
Единый скрипт сбора и парсинга. Три шага в одном запуске:
1. Скачать рынки за дату (Gamma API → JSON-файлы)
2. Скачать трейды по каждому рынку (data-api.polymarket.com)
3. Распарсить JSON-файлы в таблицы `markets` + `tokens` в БД

```bash
python -m data_collector.data_collector_and_parsing --start 2026-03-27 --end 2026-03-27
python -m data_collector.data_collector_and_parsing --start 2026-03-27 --end 2026-03-27 --skip-trades
```

После: запустить `swan_analyzer.py` для построения `swans_v2`.

### `swan_analyzer.py`
Находит black swan события в закрытых рынках. Читает сырые трейды из JSON-файлов
в `database/`, строит таблицу `swans_v2` в `polymarket_dataset.db` с метриками entry/exit
и метками победителей. Обрабатывает все токены с trade-файлами (UPSERT по `token_id, date`).
Без `--recompute` таблица не очищается, но уже обработанные токены перезаписываются.

### `backfill_new_fields.py`
Одноразовый legacy-скрипт: дозаполняет поля `restricted` и `volume_1wk` в таблице `markets`
для исторических записей, читая сырые JSON-файлы.

### `report_analyzer_stats.py`
Статистика по таблице `token_swans`: покрытие дат, количество лебедей, распределение
x-мультипликаторов, топ категорий и рынков. Read-only, только для анализа.

---

## Feature Engineering

### `market_level_features_v1_1.py`  *(новый в v1.1)*
Строит `feature_mart_v1_1` в `polymarket_dataset.db` — market-level фичи для `MarketScorer`.
Одна строка на рынок (не на событие-лебедь). Позитивы: любой токен с `entry_min_price <= 0.02`.
После сборки автоматически запускает когортный анализ сепарабельности признаков.

```bash
python scripts/market_level_features_v1_1.py
python scripts/market_level_features_v1_1.py --recompute
```

### `build_feature_mart.py`  *(legacy v1)*
Строит token-level `feature_mart` в `polymarket_dataset.db`. Используется `EntryFillScorer`
и `ResolutionScorer` для знаменателей скоринга (общий счётчик рынков по категориям).
В v1.1 заменён `market_level_features_v1_1.py` для `MarketScorer`, но продолжает работать
параллельно для legacy-скореров.

---

## Лейблинг и ML

### `build_ml_outcomes.py`
Материализует таблицу `ml_outcomes` в `positions.db`. Линкует цепочку:
`screener_log` → `resting_orders` → `positions` → `tp_orders`.
Поля: `got_fill`, `is_winner`, `realized_pnl`, `realized_roi`, `time_to_fill_hours`,
`tp_5x_hit`, `tp_10x_hit`, `tp_20x_hit`, `tp_moonbag_hit`, `peak_price`, `peak_x`.
Безопасно перезапускать (UPSERT).

```bash
python scripts/build_ml_outcomes.py --summary
python scripts/build_ml_outcomes.py --rebuild   # полный пересчёт
```

### `build_rejected_outcomes.py`
Post-hoc лейблинг отвергнутых кандидатов. Джойнит `screener_log` (из `positions.db`)
с `token_swans` (из `polymarket_dataset.db`): случился ли swan event после того, как
скринер отверг рынок? Строит `ml_rejected_outcomes` в `polymarket_dataset.db`.

```bash
python scripts/build_rejected_outcomes.py --summary
```

### `recalibrate_scorers.py`
Читает `ml_outcomes` + `ml_rejected_outcomes`. Генерирует `recommended_config.json`
с рекомендуемыми изменениями порогов и category_weights. Срабатывает когда:
miss_rate > 5%, winner_rate < 5%, avg_tail_ev аномален.

---

## Бэктест и реплей

### `run_honest_replay.py`  *(рекомендуемый)*
Честный full-universe реплей без look-ahead bias. В отличие от `run_dry_run_replay.py`,
проходит по **всем** скачанным рынкам (не только по известным лебедям): применяет
статические фильтры скринера, расставляет resting биды на подходящие рынки, реплеит
трейды хронологически. Большинство бидов не заполняется — это честная цена стратегии.

```bash
python scripts/run_honest_replay.py --start 2025-12-01 --end 2026-02-28
python scripts/run_honest_replay.py --start 2025-12-01 --end 2026-02-28 --summary
```

### `run_dry_run_replay.py`
Упрощённый бэктест: реплеит только известные события из `swans_v2` через `OrderManager`.
Быстрее, но оптимистичнее — видит только рынки где лебедь уже случился.

```bash
python scripts/run_dry_run_replay.py --months dec jan feb --summary
python scripts/run_dry_run_replay.py --pessimistic          # delay=300s, fill_fraction=0.3
python scripts/run_dry_run_replay.py --activation-delay 300 --fill-fraction 0.3
```

### `replay_cohort_report.py`
Запускает реплей по когортам `swan_score` (high/mid/low) и сравнивает результаты.
Отчёт: candidates, fill_rate, winner_rate, stake, PnL, ROI по когорте.

### `entry_stack_comparison.py`
Анализ вклада отдельных ценовых уровней в ROI. Помогает решить — стоит ли оставлять
самый глубокий уровень (0.002) в стеке, или он только снижает ROI.

### `optimize_profit_take_strategy.py`
Оптимизация схем выхода (full_exit_single, ascending_ladder, fixed_tail_500).
Тестирует разные комбинации TP-целей и весов на данных `token_swans`. Legacy-исследование.

---

## Валидация

### `validate_clob_pricing_v1_1.py`  *(новый в v1.1)*
Валидирует синтетическую цену NO-токена (`1 - YES`) против реального CLOB `best_ask`.
Берёт N живых рынков из Gamma, сравнивает отклонения по ценовым бакетам.
Вывод: среднее абс. отклонение, худшие кейсы, вердикт пригодности для скринера.

```bash
python scripts/validate_clob_pricing_v1_1.py --samples 100
```

### `validate_dry_run.py`
Тестовый стенд: 5 сценариев dry-run (scanner entry, resting bids, partial fill,
TP/PnL учёт, проигрыш на resolution). Проверяет корректность схемы `positions.db`
и логику `OrderManager`.

### `validate_content_signal.py`
Исследование: проверяет семантический сигнал из ChromaDB (похожие прошлые рынки).
Тест монотонности Q1 vs Q4 по win_rate. Не интегрирован в бот.

---

## Мониторинг

### `dashboard.py`
Живой терминальный дашборд (curses). Показывает: баланс, позиции, resting/TP ордера,
screener funnel (последний час, включая `rejected_market_score`), order manager funnel,
exposure v1.1, активность. В wide-universe режиме (issue #57): 800–1000+ resting bids.

```bash
python scripts/dashboard.py
python scripts/dashboard.py --interval 5
```

### `paper_trading_report.py`
Текстовый отчёт по paper trading сессии. Читает `positions.db`, выводит:
funnel скринера, исходы ордеров (fill rate, TP hit rate, stale/cancelled),
сводку по позициям, сравнение с replay baseline.

### `paper_balance.py`
CLI управления paper балансом. Команды: `status`, `topup`, `history`.

```bash
python scripts/paper_balance.py status
python scripts/paper_balance.py topup --amount 50
python scripts/paper_balance.py history --limit 20
```

---

## Исследования (не интегрированы в бот)

### `build_chroma.py`
Строит ChromaDB векторное хранилище из таблицы `markets`. Используется
`validate_content_signal.py` для семантического поиска похожих рынков.
Модель: `all-MiniLM-L6-v2` (ONNX).
