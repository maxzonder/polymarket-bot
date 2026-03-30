# Схема базы данных

*Связано с: [DATA_COLLECTION.md](./DATA_COLLECTION.md)*

Основная SQLite база: **`polymarket_dataset.db`** (в `$POLYMARKET_DATA_DIR`).
Операционная база: **`positions.db`** (в `$POLYMARKET_DATA_DIR`).

Сырые сделки (raw trades) в SQLite не хранятся — они лежат в JSON-файлах:
`$POLYMARKET_DATA_DIR/database/YYYY-MM-DD/{market_id}_trades/{token_id}.json`

---

## `polymarket_dataset.db`

### Таблица: `markets`
Метаданные закрытых рынков. Заполняется `data_collector_and_parsing.py`. UPSERT по `id`.

| Поле | Тип | Описание |
|------|-----|---------|
| `id` | TEXT PK | ID рынка из Gamma API |
| `question` | TEXT | Название рынка |
| `description` | TEXT | Полное описание |
| `category` | TEXT | Категория (best-effort: из API → тегов → ключевых слов) |
| `slug` | TEXT | URL slug рынка |
| `event_title` | TEXT | Название события (`events[0].title`) |
| `event_slug` | TEXT | Slug события |
| `event_description` | TEXT | Описание события |
| `tags` | TEXT | JSON-массив тегов (`events[0].tags`) |
| `ticker` | TEXT | Тикер события |
| `resolution_source` | TEXT | Источник разрешения |
| `start_date` | INTEGER | Unix timestamp начала рынка |
| `end_date` | INTEGER | Unix timestamp плановой даты закрытия |
| `closed_time` | INTEGER | Unix timestamp фактического закрытия |
| `duration_hours` | REAL | `(closed_time - start_date) / 3600` |
| `volume` | REAL | Объём торгов (USDC) |
| `liquidity` | REAL | Ликвидность |
| `comment_count` | INTEGER | Количество комментариев (`events[0].commentCount`) |
| `fees_enabled` | INTEGER | 1 если есть торговая комиссия |
| `neg_risk` | INTEGER | 1 если negative risk market |
| `group_item_title` | TEXT | Название в группе (`groupItemTitle`) |
| `cyom` | INTEGER | 1 если custom outcome market |
| `restricted` | INTEGER | 1 если доступ ограничен |
| `volume_1wk` | REAL | Объём за последнюю неделю |

---

### Таблица: `tokens`
Токены рынков (YES/NO или мультивариантные). UPSERT по `token_id`.

| Поле | Тип | Описание |
|------|-----|---------|
| `token_id` | TEXT PK | ID токена (CLOB) |
| `market_id` | TEXT FK | → `markets.id` |
| `outcome_name` | TEXT | Название исхода (`Yes`, `No`, название команды и т.д.) |
| `is_winner` | INTEGER | 1 если `outcomePrices[i] >= 0.99` (токен выиграл) |

---

### Таблица: `swans_v2`
**Основная аналитическая таблица.** Результаты работы `analyzer/swan_analyzer.py`.
Одна строка = одно swan-событие (один токен за один торговый день).
Уникальный ключ: `(token_id, date)`. UPSERT обновляет `max_traded_x`, `is_winner`, `payout_x`, `sell_volume`.

| Поле | Тип | Описание |
|------|-----|---------|
| `token_id` | TEXT | FK → `tokens.token_id` |
| `market_id` | TEXT | FK → `markets.id` |
| `date` | TEXT | Дата папки коллектора (YYYY-MM-DD) |
| `buy_price_threshold` | REAL | Порог входа при анализе |
| `min_buy_volume` | REAL | Мин. ликвидность входа при анализе |
| `min_sell_volume` | REAL | Мин. ликвидность выхода при анализе |
| `min_real_x` | REAL | Мин. max_traded_x при анализе |
| `buy_min_price` | REAL | Глобальный минимум цены в зоне дна |
| `buy_volume` | REAL | Объём сделок в зоне дна (USDC) |
| `buy_trade_count` | INTEGER | Количество сделок в зоне дна |
| `buy_ts_first` | INTEGER | Первый timestamp зоны дна |
| `buy_ts_last` | INTEGER | Последний timestamp зоны дна |
| `sell_volume` | REAL | Объём сделок выше target (`>= buy_min_price × min_real_x`); 0 для победителей |
| `max_price_in_history` | REAL | Максимальная цена за всю историю токена |
| `last_price_in_history` | REAL | Последняя цена в истории |
| `is_winner` | INTEGER | 1 если рынок разрешился в пользу токена |
| `max_traded_x` | REAL | Итоговый икс: `1/buy_min_price` для winner, `max_price / buy_min_price` для остальных |
| `payout_x` | REAL | `1/buy_min_price` для winner; = `max_traded_x` для остальных |

**Архивная таблица:** `token_swans` — legacy zigzag-анализатор (88k строк). Не используется ни одним активным скриптом. Хранится в БД как архив.

---

### Таблица: `feature_mart_v1_1`
Рыночный feature store для `MarketScorer`. Строится `analyzer/market_level_features_v1_1.py --recompute`.
Одна строка = один рынок. UPSERT по `market_id`.

| Поле | Тип | Описание |
|------|-----|---------|
| `market_id` | TEXT PK | FK → `markets.id` |
| `date` | TEXT | Дата закрытия рынка |
| `volume` | REAL | Объём торгов |
| `duration_hours` | REAL | Длительность рынка |
| `neg_risk` | INTEGER | 0/1 |
| `category` | TEXT | Категория |
| `is_winner` | INTEGER | 1 если хотя бы один токен выиграл |
| `has_swan` | INTEGER | 1 если хотя бы один токен имеет swan event с `buy_min_price <= ENTRY_MAX (0.20)` |
| `best_buy_min_price` | REAL | Минимальная цена дна среди всех токенов рынка |
| `best_max_traded_x` | REAL | Максимальный итоговый икс по рынку |
| `best_buy_volume` | REAL | Объём на дне у лучшего swan |
| `best_buy_trade_count` | INTEGER | Количество сделок на дне у лучшего swan |
| `best_floor_duration_s` | REAL | Длительность зоны дна (секунды) |
| `best_time_to_res_h` | REAL | Время от дна до закрытия рынка (часы) |
| `best_max_traded_x_excl_winner` | REAL | Максимальный икс без учёта выплаты $1 |
| `n_swans` | INTEGER | Количество swan events на рынке |
| `log_volume` | REAL | `log1p(volume)` |
| `log_duration_h` | REAL | `log1p(duration_hours)` |

---

### Таблица: `ml_rejected_outcomes`
Пост-фактум метки для отклонённых рынков — была ли пропущена возможность.
Строится `scripts/build_rejected_outcomes.py`.

| Поле | Тип | Описание |
|------|-----|---------|
| `market_id` | TEXT PK | FK → `markets.id` |
| `had_swan_event` | INTEGER | 1 если на рынке был swan event |
| `was_missed_opportunity` | INTEGER | 1 если `buy_min_price <= 0.20` и `max_traded_x >= 5` |
| ... | | Дополнительные поля из markets + swans_v2 |

---

## `positions.db`

Операционная база бота. Создаётся и обновляется в рантайме.

### Таблица: `resting_orders`
Рестинг BUY биды, выставленные ботом.

| Поле | Тип | Описание |
|------|-----|---------|
| `id` | INTEGER PK | |
| `token_id` | TEXT | ID токена |
| `market_id` | TEXT | ID рынка |
| `price` | REAL | Цена бида |
| `quantity` | REAL | Количество токенов |
| `stake_usdc` | REAL | Размер ставки (USDC) |
| `status` | TEXT | `live` / `matched` / `cancelled` / `expired` |
| `clob_order_id` | TEXT | ID ордера в CLOB API |
| `created_at` | INTEGER | Timestamp создания |
| `filled_at` | INTEGER | Timestamp исполнения (NULL пока не исполнен) |

### Таблица: `positions`
Открытые позиции (исполненные BUY ордера).

| Поле | Тип | Описание |
|------|-----|---------|
| `id` | INTEGER PK | |
| `token_id` | TEXT | ID токена |
| `market_id` | TEXT | ID рынка |
| `entry_price` | REAL | Цена входа |
| `quantity` | REAL | Количество токенов |
| `stake_usdc` | REAL | Потраченный USDC |
| `status` | TEXT | `open` / `resolved` |
| `entry_at` | INTEGER | Timestamp входа |
| `resolved_at` | INTEGER | Timestamp разрешения рынка |

### Таблица: `tp_orders`
TP SELL ордера + moonbag записи.

### Таблица: `ml_outcomes`
ML-метки по реальным сделкам бота. Строится `scripts/build_ml_outcomes.py`.

| Поле | Тип | Описание |
|------|-----|---------|
| `id` | TEXT PK | candidate_id + уровень входа |
| `got_fill` | INTEGER | 1 если бид был исполнен |
| `is_winner` | INTEGER | 1 если рынок выиграл |
| `realized_pnl` | REAL | Реализованный PnL (USDC) |
| ... | | Фичи рынка для обучения модели |

### Таблица: `screener_log`
Лог всех кандидатов, которых скринер рассмотрел. Основа для `ml_outcomes`.

---

## Вспомогательные файлы

**`$POLYMARKET_DATA_DIR/database/collector_state.db`** — операционный журнал сборщика.

| Поле | Описание |
|------|---------|
| `date` + `market_id` | PK |
| `downloaded_at` | Timestamp скачивания market JSON |
| `trades_downloaded_at` | Timestamp скачивания trades |
| `parsed_at` | Timestamp парсинга в SQLite |
| `error` | Текст ошибки (если была) |

**`$POLYMARKET_DATA_DIR/recommended_config.json`** — рекомендованные изменения порогов скоринга, генерируется `scripts/recalibrate_scorers.py`.
