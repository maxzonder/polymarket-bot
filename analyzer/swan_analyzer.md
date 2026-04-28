# swan_analyzer.py

Детектор swan-событий v2. Читает сырые трейды из `database/`, пишет в `swans_v2` в `polymarket_dataset.db`.

Важное разделение:
- базовый `swan` = токен дал достаточный исторический икс от executable low-zone;
- строгий `black_swan=1` = winner-токен можно было купить за копейки, затем рынок резко переоценил исход, и токен зарезолвился в $1.

## Использование

```bash
# Полный пересчёт всей истории
python3 analyzer/swan_analyzer.py --recompute --date-from 2025-08-01 --date-to 2026-03-28

# Диапазон (UPSERT по token_id + date) — так вызывает daily_pipeline
python3 analyzer/swan_analyzer.py --date-from 2026-03-19 --date-to 2026-03-28

# Один день
python3 analyzer/swan_analyzer.py --date 2026-03-28
```

## Основные параметры swan layer

| Флаг | По умолчанию | Источник | Описание |
|------|-------------|----------|----------|
| `--buy-price-threshold` | `SWAN_BUY_PRICE_THRESHOLD` | `config.py` — `max(entry_price_levels)` по всем режимам (обычно 0.20) | Верхняя граница low-zone |
| `--min-buy-volume` | `SWAN_MIN_BUY_VOLUME` | `config.py` | Мин. объём сделок в low-zone (можно войти) |
| `--min-sell-volume` | `SWAN_MIN_SELL_VOLUME` | `config.py` | Мин. объём сделок на выходе для non-winner |
| `--min-real-x` | `SWAN_MIN_REAL_X` | `config.py` | Минимальный итоговый икс для записи в `swans_v2` |
| `--recompute` | — | — | Дропнуть и пересоздать `swans_v2`, затем заполнить |

## Параметры strict black_swan layer

| Флаг | По умолчанию | Описание |
|------|-------------|----------|
| `--black-swan-buy-price-max` | `0.05` | Winner должен быть покупаемым в low-zone не дороже этой цены |
| `--black-swan-min-shock-x` | `5.0` | Минимальный repricing multiple от `buy_min_price` до shock |
| `--black-swan-breakout-price` | `0.20` | Абсолютный порог shock; целевой shock price = `max(breakout_price, buy_min_price × min_shock_x)` |
| `--black-swan-max-shock-delay-s` | `21600` | Shock должен прийти не позже 6h после low-zone |
| `--black-swan-shock-window-s` | `900` | Окно подтверждения shock-volume после первого shock print |
| `--black-swan-min-shock-volume` | `30.0` | Мин. USDC объём по shock-price или выше внутри окна подтверждения |
| `--black-swan-min-buy-time-to-close-s` | `300` | Не ставить `black_swan=1`, если купить можно было только ближе чем за 5m до close/resolution |

---

## Алгоритм на один токен

1. **Загрузить trades** из `database/{date}/{market_id}_trades/{token_id}.json`, отсортировать по timestamp.
   Объём сделки в USDC = `price × size`.

2. **Найти все contiguous low-zones**, где `price <= buy_price_threshold`.
   Зоны сортируются по минимальной цене: сначала самый глубокий floor.

3. **Выбрать первую executable low-zone**:
   - `buy_volume >= min_buy_volume`;
   - micro-print без ликвидности не блокирует поиск следующей, более реальной зоны.

4. **Записать buy analytics**:
   - `buy_min_price`;
   - `buy_volume`;
   - `buy_trade_count`;
   - `buy_ts_first`, `buy_ts_last`;
   - `buy_window_s`;
   - `buy_time_to_close_s`;
   - `buy_time_from_start_s`;
   - `buy_position_pct`;
   - `buy_phase`.

5. **Классифицировать фазу покупки** (`buy_phase`):
   - `final_seconds` — <=30s до close;
   - `final_minute` — <=60s;
   - `final_5m` — <=5m;
   - `final_15m` — <=15m;
   - `final_hour` — <=1h;
   - `final_6h` — <=6h;
   - `opening_10pct` — первые 10% жизни рынка;
   - `late` — последние 25% жизни рынка;
   - `middle` — всё между ними;
   - `unknown` — нет `start_date`/`end_date`.

6. **Проверить выход / итоговый икс**:
   - winner: `max_traded_x = 1.0 / buy_min_price`, `sell_volume=0`, потому что выход — payout $1;
   - non-winner: `max_traded_x = max_price_after_floor / buy_min_price`, и нужен `sell_volume >= min_sell_volume` на уровне `buy_min_price × min_real_x` или выше.

7. **Найти первый regime shock после low-zone**.
   Shock price threshold:
   ```text
   shock_target = max(black_swan_breakout_price,
                      buy_min_price × black_swan_min_shock_x)
   ```

   Пишутся:
   - `shock_ts`;
   - `shock_price`;
   - `shock_time_to_close_s`;
   - `shock_delay_s` — сколько секунд от конца buy-zone до shock;
   - `shock_x` — `shock_price / buy_min_price`;
   - `shock_delta_logit` — скачок в logit-space;
   - `shock_volume` — USDC объём по shock-target или выше внутри confirmation window;
   - `shock_trade_count`;
   - `shock_phase`.

8. **Поставить strict label `black_swan`**.
   `black_swan=1`, только если выполнено всё:
   - токен winner (`is_winner=1`);
   - `buy_min_price <= black_swan_buy_price_max`;
   - low-zone executable: достаточно объёма и не единичный мусорный print;
   - buy-zone не только в последние секунды/минуты (`buy_time_to_close_s >= min_buy_time_to_close_s`);
   - найден shock;
   - shock пришёл достаточно быстро (`shock_delay_s <= max_shock_delay_s`);
   - shock подтверждён объёмом и минимум двумя trades внутри confirmation window.

   Если label не поставлен, причина пишется в `black_swan_reason`, например:
   - `failed:winner`;
   - `failed:penny_buy`;
   - `failed:not_last_seconds`;
   - `failed:shock_found`;
   - `failed:sharp_reprice`;
   - `failed:shock_confirmed`.

9. **Записать UPSERT в `swans_v2`** по ключу `(token_id, date)`.
   Старые таблицы без новых колонок мигрируются через `ALTER TABLE` при запуске.

---

## Таблица `swans_v2`: основные поля

| Поле | Тип | Описание |
|------|-----|----------|
| `token_id` | TEXT | ID токена |
| `market_id` | TEXT | ID рынка |
| `date` | TEXT | Дата папки коллектора (YYYY-MM-DD) |
| `buy_min_price` | REAL | Минимальная цена выбранной executable low-zone |
| `buy_volume` | REAL | Объём сделок в low-zone (USDC) |
| `buy_trade_count` | INTEGER | Количество сделок в low-zone |
| `buy_ts_first` | INTEGER | Первый timestamp low-zone |
| `buy_ts_last` | INTEGER | Последний timestamp low-zone |
| `sell_volume` | REAL | Объём выхода для non-winner; 0 для winner payout |
| `max_price_in_history` | REAL | Максимальная цена за историю токена |
| `last_price_in_history` | REAL | Последняя цена в истории |
| `is_winner` | INTEGER | 1 если токен выиграл |
| `max_traded_x` | REAL | Итоговый икс с учётом payout $1 для winner |
| `payout_x` | REAL | `1/buy_min_price` для winner, иначе = `max_traded_x` |

## Таблица `swans_v2`: black_swan analytics

| Поле | Тип | Описание |
|------|-----|----------|
| `black_swan` | INTEGER | 1 если это strict black swan: penny buy → sharp repricing → winner payout |
| `black_swan_reason` | TEXT | `pass` или список failed checks |
| `black_swan_score` | REAL | Мягкий rank-score для near-misses, не label |
| `buy_time_to_close_s` | INTEGER | Секунд от конца buy-zone до close/resolution |
| `buy_time_from_start_s` | INTEGER | Секунд от старта рынка до конца buy-zone |
| `buy_position_pct` | REAL | Позиция buy-zone внутри жизни рынка: 0=start, 1=close |
| `buy_phase` | TEXT | `final_seconds`, `final_5m`, `middle`, `late`, etc. |
| `buy_window_s` | INTEGER | Длительность executable low-zone |
| `shock_ts` | INTEGER | Первый timestamp резкой переоценки |
| `shock_price` | REAL | Цена первого shock print |
| `shock_time_to_close_s` | INTEGER | Секунд от shock до close/resolution |
| `shock_delay_s` | INTEGER | Секунд от buy-zone до shock |
| `shock_x` | REAL | `shock_price / buy_min_price` |
| `shock_delta_logit` | REAL | Скачок цены в logit-space |
| `shock_volume` | REAL | Объём подтверждения shock внутри window |
| `shock_trade_count` | INTEGER | Кол-во подтверждающих shock trades |
| `shock_phase` | TEXT | Фаза рынка на момент shock |

---

## Где используется

| Компонент | Как |
|-----------|-----|
| `analyzer/market_level_features_v1_1.py` | Источник позитивов для `feature_mart_v1_1`; может дополнительно фильтровать/анализировать `black_swan=1` |
| `scripts/build_rejected_outcomes.py` | Ground truth для пропущенных возможностей |
| `scripts/daily_pipeline.py` | `--date-from {start} --date-to {end}` для новых дат |
| research scripts | Сегментация по `buy_phase`, `shock_delay_s`, `black_swan_score`, near-misses |

---

## Интерпретация

`swans_v2` больше не надо читать как “всё это настоящие чёрные лебеди”.

Правильная семантика:
- `max_traded_x` / `payout_x` — потенциальный исторический икс;
- `black_swan=1` — строгий subset, где была реальная смена позы рынка;
- `buy_phase` показывает, где была возможность купить: последние секунды, минуты, часы, middle, late;
- `shock_*` показывает момент, скорость и подтверждение переоценки.

Так слой сохраняет полезность старого x-анализатора, но добавляет отдельный аналитический ответ на вопрос: “где рынок реально резко поменял мнение, а winner можно было купить за копейки заранее?”.
