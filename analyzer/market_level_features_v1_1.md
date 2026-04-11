# market_level_features_v1_1.py

Строит таблицу `feature_mart_v1_1` в `polymarket_dataset.db` — market-level фичи для `MarketScorer`.
Источник данных: `swans_v2` (актуальные события Aug 2025 – Mar 2026) + `markets` + `tokens`.

## Использование

```bash
python3 analyzer/market_level_features_v1_1.py
python3 analyzer/market_level_features_v1_1.py --recompute    # дропнуть и пересобрать
python3 analyzer/market_level_features_v1_1.py --analysis-only  # только cohort анализ
```

---

## Что такое фичи

**Фича (feature)** — числовое свойство рынка, которое помогает модели предсказать, есть ли в этом рынке "лебедь" (резкий движ цены до 20x+). Фичи — входные данные для `MarketScorer`. Чем лучше фичи, тем точнее скоринг.

## Алгоритм

1. **Взять все рынки** из `markets` с `volume >= 50 USDC` и `closed_time >= 2025-08-01`
   — Отсекаем мусорные рынки без объёма и старые данные.

2. **LEFT JOIN с `swans_v2`** по `token_id` — рынки с лебедями получают `was_swan=1`, остальные `was_swan=0` (настоящие негативы).
   — Это ключевое отличие от legacy: раньше были только позитивы, теперь есть обе стороны, и модель может учиться.

3. **Агрегировать лебединые фичи** по рынку: если у рынка несколько токенов с лебедями, CTE с `ROW_NUMBER()` выбирает **один лучший токен** — тот, у которого максимальный `max_traded_x`. Это исключает «франкенштейн»-строки на neg-risk рынках, где MIN/MAX агрегаты ранее брали поля от разных токенов.

4. **Вычислить производные фичи**:
   - `log_volume = log1p(volume_usdc)` — логарифм объёма, потому что распределение сильно скошено (несколько огромных рынков и тысячи маленьких). Логарифм выравнивает.
   - `niche_score_raw = volume / log(volume+1)` — индикатор "мейнстримности": у крупных рынков выше, у нишевых ниже.
   - `best_time_to_res_hours` — часов от первой сделки на дне до закрытия рынка. Показывает, насколько рано можно было войти.

5. **Проставить лейблы** (целевая переменная — то, что предсказываем):
   - `label_20x = 1` если рынок дал 20x+
   - `label_tail` — гранулярная разбивка: 0 = <5x, 1 = 5–20x, 2 = 20–50x, 3 = 50–100x, 4 = 100x+
   - `swan_is_winner` — 1 если лучший токен выиграл

6. **UPSERT в `feature_mart_v1_1`** по `market_id` — при повторном запуске обновляем существующие строки.

7. **Автоматически запустить cohort analysis** — распечатать сепарабельность признаков (см. раздел ниже).

### Ключевые константы

| Константа | Источник | Значение | Смысл |
|-----------|----------|----------|-------|
| `ENTRY_MAX` | `SWAN_ENTRY_MAX` из `config.py` | 0.20 | Макс. `buy_min_price` для события-лебедя (покрывает все уровни входа бота) |
| `MIN_VOLUME` | `BotConfig().min_volume_usdc` из `config.py` | 50.0 | Минимальный объём рынка (совпадает со screener gate) |
| `DATE_FROM` | `_find_oldest_data_date()` — сканирует `DATABASE_DIR` | авто | Начало окна истории — старейшая дата-папка из `database/` |

---

## Таблица `feature_mart_v1_1`

| Поле | Тип | Описание |
|------|-----|----------|
| `market_id` | TEXT PK | ID рынка |
| `date` | TEXT | Дата закрытия рынка |
| `category` | TEXT | Категория (crypto, politics, sports...) |
| `volume_usdc` | REAL | Объём торгов USDC |
| `log_volume` | REAL | log1p(volume_usdc) |
| `liquidity_usdc` | REAL | Ликвидность рынка |
| `log_liquidity` | REAL | log1p(liquidity_usdc) |
| `duration_hours` | REAL | Продолжительность жизни рынка |
| `comment_count` | INTEGER | Кол-во комментариев (прокси внимания) |
| `neg_risk` | INTEGER | 1 = мульти-исходный рынок (neg-risk group) |
| `token_count` | INTEGER | Кол-во токенов (обычно 2) |
| `niche_score_raw` | REAL | volume / log(volume+1) — выше = мейнстримный |
| `was_swan` | INTEGER | 1 = в этом рынке был лебедь (`buy_min_price <= ENTRY_MAX`) |
| `best_buy_min_price` | REAL | Мин. цена дна по всем токенам рынка |
| `best_max_traded_x` | REAL | Макс. икс по всем токенам рынка |
| `best_buy_volume` | REAL | Объём в зоне дна лучшего токена |
| `best_buy_trade_count` | INTEGER | Кол-во сделок в зоне дна |
| `best_floor_duration_s` | REAL | Продолжительность зоны дна в секундах |
| `best_time_to_res_hours` | REAL | Часов от первой сделки на дне до закрытия рынка |
| `swan_is_winner` | INTEGER | 1 если лучший по иксу токен выиграл |
| `label_20x` | INTEGER | 1 если `best_max_traded_x >= 20` |
| `label_tail` | INTEGER | 0:<5x, 1:5-20x, 2:20-50x, 3:50-100x, 4:100x+ |
| `neg_risk_group_id` | TEXT | ID neg-risk группы (`neg_risk_market_id` из markets) |
| `group_token_count` | INTEGER | Кол-во токенов в neg-risk группе |
| `group_max_price` | REAL | Максимальная цена токена в группе (фаворит) |
| `group_underdog_count` | INTEGER | Кол-во токенов с `buy_min_price <= ENTRY_MAX` в группе |
| `group_underdog_winner` | INTEGER | 1 если хотя бы один underdog в группе выиграл |

Уникальный ключ: `market_id`. UPSERT обновляет swan-поля и лейблы.

**Миграция:** при первом запуске автоматически применяется `ALTER TABLE markets ADD COLUMN neg_risk_market_id TEXT` (если колонки ещё нет) и аналогичные ALTER TABLE для новых полей `feature_mart_v1_1`.

---

## Отличия от `feature_mart` (legacy)

| | `feature_mart` (legacy) | `feature_mart_v1_1` (актуальный) |
|---|---|---|
| Единица анализа | Токен-событие | Рынок |
| Источник позитивов | `token_swans` (Dec–Feb 2026) | `swans_v2` (Aug 2025 – Mar 2026) |
| Негативы | Нет | Да (`was_swan=0`) |
| Знает победителя | Нет | Да (`swan_is_winner`) |
| Порог входа | 0.02 | 0.20 |
| Строк в таблице | ~88k | ~358k |

---

## Cohort analysis (Шаг 7)

Запускается автоматически после `build()`. Цель: понять, какие фичи реально **разделяют** хороших лебедей (20x+) от плохих и от нелебедей. Это помогает назначить веса в `market_score`.

### Блок 1 — Общая статистика

Baseline — базовая частота лебедей по всей выборке. Например: `swan_rate = 8.4%`, `good_swan_rate = 1.2%`. Всё остальное сравнивается с этим baseline.

### Блок 2 — Разбивка по объёму (volume buckets)

Самый сильный предиктор. Показывает, как меняется доля лебедей с ростом объёма:

```
bucket    swan%  good%   avg_x
<1k       0.2%   0.0%    N/A
1k-10k    3%     0.3%    ...
>1M       48.8%  12%     ...
```

Вывод: чем крупнее рынок, тем выше шанс лебедя. `log_volume` — ключевая фича.

### Блок 3 — Разбивка по категориям

Смотрим, в каких категориях лебеди встречаются чаще. Если crypto даёт 2x против baseline — `category` полезна как фича.

### Блок 4 — Разбивка по длительности рынка

Гипотеза: длинные рынки (>720h) дают лебедей с бо́льшим `avg_x`, потому что у цены больше времени дойти до дна и отскочить.

### Блок 5 — neg_risk флаг

`neg_risk=1` — мульти-исходный рынок (много токенов в одной группе). Проверяем, есть ли там лифт по сравнению с обычными рынками.

### Блок 6 — Топ-10 комбо category × volume

Самый практичный блок — таблица исторических базовых ставок:

```
category   vol       good%  avg_x
crypto     >1M       15%    89x
politics   100k-1M   8%     34x
```

Это основа для `analogy_score` — исторический prior для данного типа рынка.

### Итог — рекомендуемые веса для market_score

```
market_score = 0.4667·liquidity_score  # объём/ликвидность
             + 0.2000·time_score       # длительность рынка
             + 0.2000·analogy_score    # исторический prior (category × vol)
             + 0.1333·context_score    # neg_risk флаг
```

`niche` из runtime score удалён. `niche_score_raw` в mart остаётся только как research/debug колонка.

Критерий качества: top-20% кандидатов по `market_score` должны давать `good_swan_rate >= 2x baseline` и `avg_x >= 20`.

---

## Где используется

| Компонент | Как |
|-----------|-----|
| `strategy/scorer.py` | `MarketScorer` читает `feature_mart_v1_1` при `refresh()` |
| `scripts/daily_pipeline.py` | Шаг 2 ежедневного пайплайна (`--step feature_mart_v1_1`) |

---

## Текущее состояние

После `--recompute` с `swans_v2` (Mar 31 2026):
- **384,356 рынков** (все закрытые с Aug 2025)
- swans = 31,319 (8.1%) — was_swan=1
- good swans (≥20x) = 4,345 (1.1%)
- winners среди лебедей = 24,302 (6.3%)
- neg-risk рынки: avg_x = 16.7 vs 14.2 для бинарных
