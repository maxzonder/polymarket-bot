*Входит в состав документации [ROADMAP.md](./ROADMAP.md) (Этап 1).*
*Связано с: [DATA_COLLECTION.md](./DATA_COLLECTION.md)*

# Схема базы данных для Data Collector

Один файл: **`polymarket_dataset.db`** (SQLite).

Разделение на два файла (`raw.db` + `analytics.db`) убрано — для MVP объём данных (~50MB/месяц) не требует такой сложности. Всё живёт в одной БД.

Флаги `is_black_swan` и `had_tp_spike` **не хранятся** — пороги входа и TP являются параметрами бэктеста, не константами. Вычисляются в Python при анализе.

---

## Таблица: `markets`
Метаданные рынков. Основа для скоринга и фильтрации.

| Поле | Тип | Описание |
|------|-----|---------|
| `id` | TEXT PRIMARY KEY | ID рынка из Gamma API |
| `question` | TEXT | Название рынка |
| `description` | TEXT | Полное описание и правила разрешения |
| `category` | TEXT | Категория (sports, crypto, politics и т.д.) |
| `resolution_source` | TEXT | Источник разрешения |
| `start_date` | INTEGER | Unix timestamp начала рынка (из `events[0].startDate`) |
| `end_date` | INTEGER | Unix timestamp ожидаемого закрытия |
| `closed_time` | INTEGER | Unix timestamp фактического закрытия |
| `duration_hours` | REAL | `(closed_time - start_date) / 3600` — вычисляется при ETL |
| `volume` | REAL | Объём торгов (USDC) |
| `liquidity` | REAL | Ликвидность на момент закрытия |
| `comment_count` | INTEGER | Количество комментариев (из `events[0].commentCount`) |
| `fees_enabled` | INTEGER | 1 если есть торговая комиссия (влияет на EV) |

---

## Таблица: `tokens`
Нормализованные токены рынка. Каждый бинарный рынок даёт 2 строки (YES и NO).

| Поле | Тип | Описание |
|------|-----|---------|
| `token_id` | TEXT PRIMARY KEY | ID токена из `clobTokenIds` (используется в `/prices-history`) |
| `market_id` | TEXT | FK → `markets.id` |
| `outcome_name` | TEXT | Название исхода (`Yes`, `No`, или кастомное) |
| `is_winner` | INTEGER | 1 если этот токен выиграл рынок (resolved at $1.00) |

**Индекс:** `CREATE INDEX idx_tokens_market ON tokens(market_id)`

---

## Таблица: `price_history`
Почасовой временной ряд цен по каждому токену. Сырые данные из CLOB API `/prices-history?fidelity=60`.

| Поле | Тип | Описание |
|------|-----|---------|
| `id` | INTEGER PRIMARY KEY AUTOINCREMENT | Суррогатный ключ |
| `token_id` | TEXT | FK → `tokens.token_id` |
| `ts` | INTEGER | Unix timestamp точки (часовой тик) |
| `price` | REAL | Цена токена (0.00 – 1.00) |

**Индексы:**
```sql
CREATE INDEX idx_ph_token ON price_history(token_id);
CREATE INDEX idx_ph_token_ts ON price_history(token_id, ts);
```

> Одна запись = одна часовая свеча. При месяце данных (~1500 рынков, ~3000 токенов, средний рынок 7 дней = 168 точек) ≈ 500,000 строк. Размер ~30MB.

---

## Таблица: `token_analytics`
Pre-computed статистика на токен. Заполняется ETL-скриптом после загрузки `price_history`. Ускоряет перебор порогов при бэктестинге — не нужно каждый раз сканировать весь временной ряд.

| Поле | Тип | Описание |
|------|-----|---------|
| `token_id` | TEXT PRIMARY KEY | FK → `tokens.token_id` |
| `market_id` | TEXT | FK → `markets.id` (для удобства JOIN) |
| `min_price` | REAL | Абсолютный минимум цены за всё время жизни рынка |
| `min_price_ts` | INTEGER | Unix timestamp момента минимума |
| `hours_to_close_at_min` | REAL | `(closed_time - min_price_ts) / 3600` — сколько часов до закрытия было в момент дна |
| `max_price_after_min` | REAL | Максимум цены **после** момента минимума (до закрытия) |
| `max_spike_multiplier` | REAL | `max_price_after_min / min_price` — максимально возможный ROI от дна |
| `hours_at_bottom` | INTEGER | Количество часовых свечей подряд где цена была <= `min_price × 1.5`. Отсекает одиночные часовые "прострелы" вниз — реальный лебедь обычно лежит внизу несколько часов |

---

## Data Pipeline (этапы заполнения)

**Шаг 1 — Scraping:**
```
GET /markets?closed=true → INSERT INTO markets
Для каждого рынка: parse clobTokenIds + outcomes → INSERT INTO tokens
```

**Шаг 2 — Price Fetching:**
```
Для каждого token_id:
  GET /prices-history?market={token_id}&interval=max&fidelity=60
  → INSERT INTO price_history
```

**Шаг 3 — ETL (Analytics):**
```python
Для каждого token_id:
  history = SELECT ts, price FROM price_history WHERE token_id = ? ORDER BY ts
  
  min_price = min(history.price)
  min_price_ts = history[argmin].ts
  
  after_min = history[history.ts > min_price_ts]
  max_price_after_min = max(after_min.price) if after_min else min_price
  max_spike_multiplier = max_price_after_min / min_price
  
  # Считаем часы подряд у дна (threshold = min_price × 1.5)
  bottom_threshold = min_price * 1.5
  hours_at_bottom = count_consecutive_candles_below(history, bottom_threshold, starting_at=min_price_ts)
  
  hours_to_close = (markets.closed_time - min_price_ts) / 3600
  
  → INSERT INTO token_analytics
```

**Шаг 4 — Backtesting (параметрический перебор):**
```python
# Пороги — параметры, не константы в БД
for entry_threshold in [0.01, 0.02, 0.03, 0.05]:
    for tp_multiplier in [3, 5, 10, 20]:
        for min_hours_at_bottom in [1, 2, 3]:
            swans = query("""
                SELECT ta.*, m.category, m.duration_hours, m.volume
                FROM token_analytics ta
                JOIN markets m ON ta.market_id = m.id
                WHERE ta.min_price <= ?
                  AND ta.hours_at_bottom >= ?
            """, entry_threshold, min_hours_at_bottom)
            
            swans["had_tp"] = swans["max_spike_multiplier"] >= tp_multiplier
            ev = swans["had_tp"].mean() * tp_multiplier - 1
            
            # → сохранить результаты для нахождения оптимума
```

---

*NLP-поля и ChromaDB перенесены в [POSTPONED_IDEAS.md](./POSTPONED_IDEAS.md).*
