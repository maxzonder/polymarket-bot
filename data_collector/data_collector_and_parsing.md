# data_collector_and_parsing.py

Единый скрипт сбора и парсинга данных Polymarket. Три последовательных шага за один запуск.

## Использование

```bash
python -m data_collector.data_collector_and_parsing --start 2026-03-27 --end 2026-03-27
python -m data_collector.data_collector_and_parsing --start 2025-08-01 --end 2026-03-16
python -m data_collector.data_collector_and_parsing --start 2026-03-27 --end 2026-03-27 --skip-trades
python -m data_collector.data_collector_and_parsing --start 2026-03-27 --end 2026-03-27 --skip-markets --skip-trades
```

Флаги:
- `--skip-markets` — пропустить шаг 1 (не качать JSON рынков)
- `--skip-trades` — пропустить шаг 2 (не качать трейды)
- `--skip-parse` — пропустить шаг 3 (не парсить в БД)

Логи: `/home/polybot/.polybot/logs/data_collector_and_parsing.log`

После запуска: `python scripts/swan_analyzer.py --date-from ... --date-to ...` для построения `swans_v2`.

---

## Шаг 1 — Скачивание рынков

**API:** `https://gamma-api.polymarket.com/markets`

Для каждой даты в диапазоне запрашивает все закрытые рынки с `end_date == day`.

Параметры запроса:
- `closed=true`
- `include_tag=true` — чтобы в JSON были теги/категории
- `volume_num_min=50` — фильтр по минимальному объёму
- Пагинация по 100 рынков, sleep 50ms между страницами

**Результат:** JSON-файлы в `database/{yyyy-mm-dd}/{market_id}.json`

**Пропуск:** если файл уже существует на диске — рынок не перекачивается.

---

## Шаг 2 — Скачивание трейдов

**API:** `https://data-api.polymarket.com/trades`

Для каждого рынка из шага 1 скачивает историю трейдов по `conditionId`.

Параметры:
- `filterType=CASH`, `filterAmount=10` — только сделки от $10
- Пагинация по 1000 трейдов, max offset 3000
- Sleep 100ms между рынками

Из каждого трейда сохраняет: `side`, `size`, `price`, `timestamp`. Разбивает по токенам (`asset`).

**Результат:** JSON-файлы в `database/{yyyy-mm-dd}/{market_id}_trades/{token_id}.json`

Каждый файл — массив трейдов для одного токена за всё время жизни рынка.

**Пропуск:** если папка `{market_id}_trades/` существует и непустая — рынок не перекачивается.

---

## Шаг 3 — Парсинг в БД

Читает JSON-файлы из `database/` и пишет в `polymarket_dataset.db`.

### Таблица `markets`

| Поле | Тип | Источник в JSON |
|------|-----|-----------------|
| `id` | TEXT PK | `id` |
| `question` | TEXT | `question` |
| `description` | TEXT | `description` |
| `category` | TEXT | см. ниже |
| `slug` | TEXT | `slug` |
| `event_title` | TEXT | `events[0].title` |
| `event_slug` | TEXT | `events[0].slug` |
| `event_description` | TEXT | `events[0].description` |
| `tags` | TEXT (JSON) | `events[0].tags` |
| `ticker` | TEXT | `events[0].ticker` |
| `resolution_source` | TEXT | `resolutionSource` |
| `start_date` | INTEGER (unix) | `startDate` |
| `end_date` | INTEGER (unix) | `endDate` |
| `closed_time` | INTEGER (unix) | `closedTime` |
| `duration_hours` | REAL | `(closed_time - start_date) / 3600` |
| `volume` | REAL | `volumeNum` или `volume` |
| `liquidity` | REAL | `liquidity` |
| `comment_count` | INTEGER | `events[0].commentCount` |
| `fees_enabled` | INTEGER (0/1) | `feesEnabled` |
| `neg_risk` | INTEGER (0/1) | `negRisk` |
| `group_item_title` | TEXT | `groupItemTitle` |
| `cyom` | INTEGER (0/1) | `cyom` |
| `restricted` | INTEGER (0/1) | `restricted` |
| `volume_1wk` | REAL | `volume1wk` или `volume1wkClob` |

Запись: UPSERT по `id`. При конфликте обновляет все поля, кроме `category` — для неё берётся `COALESCE(new, existing)` (сохраняет старое если новое NULL).

### Определение категории

Приоритет:
1. Поле `category` в корне JSON
2. Поле `category` в `events[0]`
3. Первый тег из `events[0].tags` (label/slug/name)
4. Эвристика по ключевым словам в question/description/slug

Ключевые слова по категориям: `crypto`, `sports`, `politics`, `geopolitics`, `weather`, `entertainment`, `tech`.

### Таблица `tokens`

| Поле | Тип | Источник |
|------|-----|----------|
| `token_id` | TEXT PK | `clobTokenIds[i]` |
| `market_id` | TEXT FK | `id` |
| `outcome_name` | TEXT | `outcomes[i]` |
| `is_winner` | INTEGER (0/1) | `outcomePrices[i] >= 0.99` |

Запись: UPSERT по `token_id`.

**Пропуск:** если `market_id` уже есть в таблице `markets` — рынок не перепарсивается.
