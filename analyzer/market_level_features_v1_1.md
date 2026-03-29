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

## Алгоритм

1. **Взять все рынки** из `markets` с `volume >= 50 USDC` и `closed_time >= 2025-08-01`
2. **LEFT JOIN с `swans_v2`** по `token_id` — рынки с лебедями получают `was_swan=1`, остальные `was_swan=0` (настоящие негативы)
3. **Агрегировать лебединые фичи** по рынку: берём лучший токен (с минимальной `buy_min_price`)
4. **Вычислить производные фичи**: `log_volume`, `niche_score_raw`, `best_time_to_res_hours`
5. **Проставить лейблы**: `label_20x`, `label_tail` (0–4), `swan_is_winner`
6. **UPSERT в `feature_mart_v1_1`** по `market_id`
7. **Автоматически запустить cohort analysis** — распечатать сепарабельность признаков

### Ключевые константы

| Константа | Значение | Смысл |
|-----------|----------|-------|
| `ENTRY_MAX` | 0.20 | Макс. `buy_min_price` для события-лебедя (покрывает все уровни входа бота) |
| `MIN_VOLUME` | 50.0 | Минимальный объём рынка (совпадает со screener gate) |
| `DATE_FROM` | `"2025-08-01"` | Начало окна истории (весь период swans_v2) |

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

Уникальный ключ: `market_id`. UPSERT обновляет swan-поля и лейблы.

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

## Cohort analysis

Запускается автоматически после `build()`. Показывает:
- Распределение лебедей и хороших лебедей (≥20x) по категориям
- Сепарабельность по объёму: top-10% по volume vs базовый уровень
- Разбивка по длительности рынка, neg_risk
- Топ-10 комбо (category × volume_bucket) по доле хороших лебедей
- Рекомендуемые начальные веса для `market_score`

---

## Где используется

| Компонент | Как |
|-----------|-----|
| `strategy/scorer.py` | `MarketScorer` читает `feature_mart_v1_1` при `refresh()` |
| `scripts/daily_pipeline.py` | Шаг 2 ежедневного пайплайна (`--step feature_mart_v1_1`) |

---

## Текущее состояние

После `--recompute` с `swans_v2` (Mar 29 2026):
- **358,120 рынков** (все закрытые с Aug 2025)
- swans = 30,030 (8.4%) — was_swan=1
- good swans (≥20x) = 4,164 (1.2%)
- winners среди лебедей = 23,321 (77.7%)
