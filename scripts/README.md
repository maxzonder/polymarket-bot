# scripts/

Справочник по всем скриптам. Запускаются вручную или через `daily_pipeline.py`.

---

## Ежедневный пайплайн

### `daily_pipeline.py`
Оркестратор всех ML-шагов. **Запускается автоматически ботом каждый день в 04:00 UTC**
через `_daily_pipeline_loop` в `bot/main_loop.py` — cron не нужен.
Порядок шагов: `analyzer` → `feature_mart_v1_1` → `ml_outcomes` → `feedback_penalties` → `rejected_outcomes` → `recalibrate`.
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

После: запустить `analyzer/swan_analyzer.py` для построения `swans_v2`.

### `backfill_new_fields.py`
Одноразовый legacy-скрипт: дозаполняет поля `restricted` и `volume_1wk` в таблице `markets`
для исторических записей, читая сырые JSON-файлы.

### `report_analyzer_stats.py`  *(moved to `_legacy/`)*
Статистика по таблице `token_swans`: покрытие дат, количество лебедей, распределение
x-мультипликаторов, топ категорий и рынков. Read-only, только для анализа.

---

## Feature Engineering

### `../analyzer/market_level_features_v1_1.py`  *(новый в v1.1)*
Строит `feature_mart_v1_1` в `polymarket_dataset.db` — market-level фичи для `MarketScorer`.
Одна строка на рынок (не на событие-лебедь). Позитивы: любой токен с `buy_min_price <= 0.20`.
Источник данных: `swans_v2` (история Aug 2025 – Mar 2026).
После сборки автоматически запускает когортный анализ сепарабельности признаков.

```bash
python analyzer/market_level_features_v1_1.py
python analyzer/market_level_features_v1_1.py --recompute
```

### `build_feature_mart.py`  *(moved to `_legacy/`)*
Строит token-level `feature_mart` в `polymarket_dataset.db`. Использовался `EntryFillScorer`
и `ResolutionScorer` для знаменателей скоринга (общий счётчик рынков по категориям).
Заменён `market_level_features_v1_1.py` в v1.1. Перенесён в `_legacy/`.

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

### `analyze_empty_candidates.py`
Вычисляет `feedback_penalties` по сегментам (category, vol_bucket) из `ml_outcomes`.
Пишет таблицу `feedback_penalties` в dataset DB — используется `MarketScorer` для штрафа скора.

Логика штрафов:
- `empty_rate > 95%` и `avg_roi < 0` → penalty = 0.50
- `filled_loser_rate > 80%` → penalty = 0.60
- иначе → penalty = 1.00

Требует 2+ недель paper trading. При пустом `ml_outcomes` завершается без ошибок.

```bash
python scripts/analyze_empty_candidates.py
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

### `run_dry_run_replay.py`  *(moved to `_legacy/`)*
Упрощённый бэктест: реплеит только известные события из `swans_v2` через `OrderManager`.
Быстрее, но оптимистичнее — видит только рынки где лебедь уже случился. Перенесён в `_legacy/`.

### `replay_cohort_report.py`  *(moved to `_legacy/`)*
Запускает реплей по когортам `swan_score` (high/mid/low) и сравнивает результаты.
Отчёт: candidates, fill_rate, winner_rate, stake, PnL, ROI по когорте. Перенесён в `_legacy/`.

### `entry_stack_comparison.py`  *(moved to `_legacy/`)*
Анализ вклада отдельных ценовых уровней в ROI. Помогает решить — стоит ли оставлять
самый глубокий уровень (0.002) в стеке, или он только снижает ROI. Перенесён в `_legacy/`.

### `optimize_profit_take_strategy.py`  *(moved to `_legacy/`)*
Оптимизация схем выхода (full_exit_single, ascending_ladder, fixed_tail_500).
Тестирует разные комбинации TP-целей и весов на данных `token_swans`. Перенесён в `_legacy/`.

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

### `validate_content_signal.py`  *(moved to `_legacy/`)*
Исследование: проверяет семантический сигнал из ChromaDB (похожие прошлые рынки).
Тест монотонности Q1 vs Q4 по win_rate. Не интегрирован в бот. Перенесён в `_legacy/`.

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

### `build_chroma.py`  *(moved to `_legacy/`)*
Строит ChromaDB векторное хранилище из таблицы `markets`. Использовалось
`validate_content_signal.py` для семантического поиска похожих рынков.
Модель: `all-MiniLM-L6-v2` (ONNX). Перенесён в `_legacy/`.
