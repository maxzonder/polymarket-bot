 *Входит в состав документации [ROADMAP.md](./ROADMAP.md) (Этап 1).*
*Связано с: [DATA_COLLECTION.md](./DATA_COLLECTION.md)*

# Схема базы данных для Data Collector

Архитектура разделена на **две независимые базы данных** (SQLite), чтобы разделить тяжелые сырые данные и оптимизированную витрину данных для бэктестинга.

---

## 1. База сырых данных: `polymarket_raw.db`
Хранит полные дампы данных из API "как есть". Используется как бэкап и источник для пересчета агрегаций.

### Таблица: `raw_markets`
Полные данные по закрытым рынкам.
| Поле | Тип | Описание |
| --- | --- | --- |
| `id` | TEXT PRIMARY KEY | ID рынка (UUID/хеш). |
| `question` | TEXT | Название рынка. |
| `description` | TEXT | Полное, нефильтрованное описание рынка (Resolution Rules). |
| `category` | TEXT | Категория рынка. |
| `resolution_source` | TEXT | Источник разрешения. |
| `start_date` | DATETIME | Дата начала события. |
| `end_date` | DATETIME | Ожидаемая дата окончания события. |
| `closed_time` | DATETIME | Фактическое время закрытия рынка. |
| `volume` | REAL | Общий объем торгов (USD). |
| `liquidity` | REAL | Ликвидность на момент закрытия. |
| `comment_count` | INTEGER | Количество комментариев (индикатор "хайпа" толпы из объекта Event). |
| `outcomes` | TEXT | JSON массив возможных исходов. |
| `outcome_prices` | TEXT | JSON массив финальных цен закрытия. |
| `clob_token_ids` | TEXT | JSON массив идентификаторов токенов. |

### Таблица: `raw_price_history`
Полная история цен для выигрышных токенов (сырые ответы эндпоинта `/prices-history`).
| Поле | Тип | Описание |
| --- | --- | --- |
| `id` | INTEGER PRIMARY KEY AUTOINCREMENT | Суррогатный ключ. |
| `market_id` | TEXT | Внешний ключ, связь с `raw_markets.id`. |
| `token_id` | TEXT | ID токена (исхода). |
| `timestamp` | DATETIME | Время фиксации цены. |
| `price` | REAL | Зафиксированная цена (0.00 - 1.00). |

---

## 2. Аналитическая база: `polymarket_analytics.db`
Оптимизированная база (Data Mart). Содержит только агрегаты и выжимки для дешевого бэктестинга.

### Таблица: `markets_summary`
| Поле | Тип | Описание |
| --- | --- | --- |
| `id` | TEXT PRIMARY KEY | ID рынка. |
| `question` | TEXT | Название рынка. |
| `category` | TEXT | Категория рынка (Киберспорт, Крипта, Политика и т.д.). |
| `duration_hours` | REAL | Разница между `closed_time` и `start_date`. |
| `hours_to_close_at_scrape` | REAL | Сколько часов оставалось до закрытия в момент сбора данных (для фильтра <= 48h). |
| `volume` | REAL | Объем торгов (USD). |
| `comment_count` | INTEGER | Уровень "хайпа" вокруг рынка. |
| `winning_outcome` | TEXT | Текстовое название выигравшего исхода. |
| `min_price_observed` | REAL | Минимальная цена любого токена в последние 48h (включая проигравшие). |
| `fees_enabled` | BOOLEAN | Есть ли торговая комиссия на рынке (`feesEnabled` из API). Влияет на EV-расчёт. |
| `is_black_swan` | BOOLEAN | **Целевой флаг (1/0):** Равен 1, если `min_price_observed` <= $0.03. |
| `had_tp_spike` | BOOLEAN | **TP-флаг (1/0):** Равен 1, если цена поднималась от <= $0.03 до >= $0.15 (5x) в любой момент до закрытия. Ключевой критерий для валидации стратегии. |
| `max_spike_multiplier` | REAL | Максимальный достигнутый множитель от дна (напр. 12.5 = цена выросла в 12.5x от минимума). Используется Profit Module для расчёта EV. |

### Таблица: `swan_metrics` (Анализ "Чёрного лебедя")
Заполняется **только для рынков** где `is_black_swan = 1`. Содержит глубокую аналитику паттернов падения и отскоков цены.

| Поле | Тип | Описание |
| --- | --- | --- |
| `market_id` | TEXT PRIMARY KEY | ID рынка. |
| `time_in_swan_zone_mins` | INTEGER | Сколько минут цена победившего токена находилась <= $0.05. (Помогает отсеять миллисекундные "прострелы", которые невозможно купить). |
| `hours_to_close_from_bottom` | REAL | За сколько часов до закрытия рынка цена упала на дно. (Определяет "горячие часы" для работы Screener-а). |
| `crash_velocity_per_hour` | REAL | Скорость падения цены (Delta Price / Hours). Отличает медленное угасание от внезапного шока (красной карточки, скандала). |
| `tp_opportunities_count` | INTEGER | Сколько раз токен "отскакивал" от $0.05 до $0.15+ (Take Profit) до конца рынка. (Оценивает жизнеспособность стратегии TP на высокой волатильности). |

### Таблица: `category_ev_stats` (EV-статистика для Profit Module)
Хранит накопленную статистику по каждому параметру скоринга. Profit Module читает эту таблицу для корректировки весов.

| Поле | Тип | Описание |
| --- | --- | --- |
| `param_key` | TEXT PRIMARY KEY | Ключ параметра (напр. `category:esports`, `price_range:0.001-0.005`, `liquidity:500-2000`). |
| `sum_payout` | REAL | Суммарный USDC-возврат с победных сделок по этому параметру. |
| `sum_loss` | REAL | Суммарный потраченный stake на проигрышные сделки. |
| `ev_score` | REAL | `sum_payout - sum_loss`. Главная метрика: > 0 = прибыльный параметр. |
| `trade_count` | INTEGER | Количество сделок. Веса не обновляются при `trade_count < 20` (недостаточно данных). |
| `current_weight` | REAL | Текущий вес параметра в скоринге (default = 1.0). |
| `last_updated` | DATETIME | Дата последнего обновления Profit Module. |

---

## Архитектура Data Pipeline

1. **Сбор (Scraping):** Скрипт скачивает закрытые рынки из Gamma API и складывает их "как есть" в `polymarket_raw.db` (`raw_markets`). Обязательно вытягивает `commentCount` из родительского объекта `Event`.
2. **Сбор истории (Price Fetching):** Для каждого рынка определяется победитель, его история цен скачивается в `raw_price_history`.
3. **ETL-процесс (Extract, Transform, Load):** 
   - Высчитывается `duration_hours` (если > 120 часов, рынок игнорируется).
   - В `raw_price_history` ищется минимальная цена, устанавливаются флаги `is_black_swan` и `min_price_observed` в `markets_summary`.
   - **Только если `is_black_swan == 1`**:
     - Рассчитываются продвинутые метрики (`time_in_swan_zone_mins`, `hours_to_close_from_bottom`, `crash_velocity_per_hour`, `tp_opportunities_count`) и пишутся в таблицу `swan_metrics`.

*NLP-поля и ChromaDB перенесены в [POSTPONED_IDEAS.md](./POSTPONED_IDEAS.md).*
