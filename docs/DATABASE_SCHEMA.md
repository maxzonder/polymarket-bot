# Схема базы данных

*Связано с: [DATA_COLLECTION.md](./DATA_COLLECTION.md), [ANALYZER.md](./ANALYZER.md)*

Основная SQLite база: **`polymarket_dataset.db`** (находится в `$POLYMARKET_DATA_DIR`).

Сырые сделки (raw trades) в SQLite не хранятся из-за огромного объема. Они лежат в виде JSON-файлов в папке `database/YYYY-MM-DD/{market_id}_trades/{token_id}.json`.

В SQLite хранятся метаданные рынков, токенов и результаты работы Анализатора (найденные лебеди).

---

## Таблица: `markets`
Метаданные рынков.

| Поле | Тип | Описание |
|------|-----|---------|
| `id` | TEXT PRIMARY KEY | ID рынка из Gamma API |
| `question` | TEXT | Название рынка |
| `description` | TEXT | Полное описание |
| `category` | TEXT | Категория (best-effort extraction из тегов и текста) |
| `tags` | TEXT | JSON-массив тегов рынка |
| `start_date` | INTEGER | Unix timestamp начала |
| `end_date` | INTEGER | Unix timestamp закрытия |
| `volume_usdc` | REAL | Объём торгов |
| `liquidity_usdc` | REAL | Ликвидность |
| `comment_count` | INTEGER | Количество комментариев |
| `fees_enabled` | INTEGER | 1 если есть торговая комиссия |

---

## Таблица: `tokens`
Каждый бинарный рынок даёт 2 строки (YES и NO), многовариантный — больше.

| Поле | Тип | Описание |
|------|-----|---------|
| `token_id` | TEXT PRIMARY KEY | ID токена |
| `market_id` | TEXT | FK → `markets.id` |
| `outcome_name` | TEXT | Название исхода (`Yes`, `No`, и т.д.) |
| `is_winner` | INTEGER | 1 если токен выиграл (resolved at $1.00) |

---

## Таблица: `token_swans`
Результаты работы `analyzer.py`. Содержит только "реальные" лебеди, прошедшие фильтры ликвидности и времени.

| Поле | Тип | Описание |
|------|-----|---------|
| `id` | INTEGER PK | Суррогатный ключ |
| `token_id` | TEXT | FK → `tokens.token_id` |
| `market_id` | TEXT | FK → `markets.id` |
| `date` | TEXT | Дата закрытия рынка (YYYY-MM-DD) |
| `min_recovery` | REAL | Параметр анализатора (минимальный X) |
| `target_exit_x` | REAL | Параметр анализатора (целевой тейк) |
| `entry_min_price` | REAL | Минимальная цена на дне |
| `entry_volume_usdc` | REAL | Реальный проторгованный объем в зоне дна |
| `exit_max_price` | REAL | Пиковая цена после дна |
| `exit_volume_usdc` | REAL | Реальный объем, проторгованный выше целевого тейка |
| `duration_entry_to_target_minutes` | REAL | Время от дна до достижения тейка (в минутах) |
| `possible_x` | REAL | Реальный X (exit_max / entry_min) |

---

## Дополнительная база: `collector_state.db`
Операционный журнал сборщика (лежит в `$POLYMARKET_DATA_DIR/database/`).

| Поле | Тип | Описание |
|------|-----|---------|
| `date` | TEXT | Дата (YYYY-MM-DD) |
| `market_id` | TEXT | ID рынка |
| `downloaded_at` | TEXT | Timestamp скачивания Market JSON |
| `prices_downloaded_at` | TEXT | Timestamp скачивания Raw Trades |
| `parsed_at` | TEXT | Timestamp парсинга в SQLite |
| `error` | TEXT | Текст ошибки, если была |