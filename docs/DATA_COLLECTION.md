*Входит в состав документации [ROADMAP.md](./ROADMAP.md).*
*Связано с: [DATABASE_SCHEMA.md](./DATABASE_SCHEMA.md), [API_LIMITS.md](./API_LIMITS.md)*

# Сбор исторических данных

Текущая архитектура исторического пайплайна опирается на два сырьевых слоя:
- **market metadata** из Gamma API `/markets`
- **raw trades** из `data-api.polymarket.com/trades`

`prices-history` и любые derived-слои на его основе исключены из runtime-пайплайна.

## Ключевой принцип

**Единственный источник правды для цены, ликвидности и swan-событий — сырые `trades`.**

Почему:
- `prices-history` сглаживает движение и стирает внутричасовые spikes;
- для нашей стратегии важны реальные сделки, а не агрегированные свечи;
- ликвидность нужно измерять по фактически исполненным трейдам в зоне входа и выхода.

---

## Текущий pipeline

Все три шага объединены в единый скрипт `data_collector/data_collector_and_parsing.py`.
Ежедневно запускается автоматически через `scripts/daily_pipeline.py` (шаг `ingest`).

### 1. Download markets
Источник: `GET /markets` (Gamma API)

- Скачивает закрытые рынки по диапазону дат (`end_date_min`/`end_date_max`)
- `include_tag=true` — сохраняет tags и category metadata
- Мягкий фильтр: `volume_num_min=50`
- Сохраняет: `$POLYMARKET_DATA_DIR/database/YYYY-MM-DD/{market_id}.json`
- Идемпотентный: файл уже существует → пропускается

### 2. Download raw trades
Источник: `data-api.polymarket.com/trades`

- Для каждого рынка скачивает историю сделок по `conditionId`
- Фильтр: `filterType=CASH`, `filterAmount=10`
- Разбивает по токенам: `{market_id}_trades/{token_id}.json`
- Идемпотентный: папка `{market_id}_trades/` непустая → пропускается

Почему `CASH >= 10`: без фильтра история забивается микросделками и ботошумом за лимит пагинации (max offset 3000). Фильтры по времени (`startTs`/`endTs`) работают ненадёжно.

### 3. Parse into SQLite
Читает JSON из `database/`, нормализует в `polymarket_dataset.db`:
- **`markets`** — метаданные рынков, UPSERT по `id`
- **`tokens`** — YES/NO токены + `is_winner`, UPSERT по `token_id`

Категория: из явных полей API → тегов → best-effort inference по keywords в question/description/slug.

Поддерживаемые категории и их веса в скоринге (`config.py: BotConfig.category_weights`):

| Категория      | Вес  | Что туда попадает |
|----------------|------|-------------------|
| `geopolitics`  | 1.5  | Иран, Израиль, Украина, Россия, Китай, Тайвань, НАТО, Венесуэла, Северная Корея, военные события |
| `politics`     | 1.5  | Выборы, Трамп, Байден, ЕЦБ, Банк Англии, Fed/Powell, тарифы, санкции, ВВП, безработица |
| `crypto`       | 1.0  | BTC/ETH/SOL и 50+ токенов, биржевые индексы, акции, сырьё (нефть, уран, пшеница) |
| `weather`      | 1.0  | Температура, осадки, ураганы, землетрясения, наводнения, TSA-пассажиры |
| `health`       | 1.0  | Грипп, корь, COVID, CDC, FDA, вакцины, вспышки инфекций, клинические испытания |
| `esports`      | 1.0  | (legacy — теперь входит в `sports`) |
| `sports`       | 0.8  | НБА/НФЛ/НХЛ, футбол (все лиги), теннис/АТП/WTA, шахматы/FIDE, дартс/PDC, MotoGP, пиклбол/PPA, конный спорт, Ladbrokes и др. |
| `tech`         | 0.6  | OpenAI, Apple, Google, SpaceX, TikTok, Anthropic/Gemini/Grok, IPO, earnings calls |
| `entertainment`| 0.5  | Оскар, Грэмми, Эмми, BAFTA, Золотой глобус, Goodreads, Eurovision, Taylor Swift, MrBeast |
| *(null)*       | 1.0  | Рынки без категории — нейтральный вес (`.get(None, 1.0)`) |

Кодовое покрытие: `data_collector_and_parsing.py → _CATEGORY_KEYWORDS`, `_infer_category()`.
Приоритет уровней: явный API-field → event category → event tags → keyword matching → None.

### 4. Swan Analyzer (`analyzer/swan_analyzer.py`)
Читает raw trades с диска → строит таблицу `swans_v2`.

- Находит глобальный минимум цены токена (зону дна)
- Проверяет `buy_min_price < buy_price_threshold` (0.20)
- Считает `max_traded_x`: `1/buy_min_price` для победителей, `max_price/buy_min_price` для остальных
- UPSERT по `(token_id, date)`

Запускается через `daily_pipeline.py` с `--date-from`/`--date-to` для новых дат.

### 5. Feature Mart (`analyzer/market_level_features_v1_1.py`)
Читает `swans_v2 + markets + tokens` → строит `feature_mart_v1_1`.

Одна строка = один рынок. Агрегирует swan-события на уровень рынка.
Используется `MarketScorer` для принятия решений о входе.
Запускается с `--recompute` (полный пересчёт из всего `swans_v2`).

---

## Что хранится где

### Raw files
- `$POLYMARKET_DATA_DIR/database/YYYY-MM-DD/{market_id}.json`
  - сырой market object
- `$POLYMARKET_DATA_DIR/database/YYYY-MM-DD/{market_id}_trades/{token_id}.json`
  - сырые сделки по токену

### SQLite (`polymarket_dataset.db`)
Актуальные рабочие таблицы:
- `markets`
- `tokens`
- `swans_v2`
- `feature_mart_v1_1`
- `ml_rejected_outcomes`

Архивные (не обновляются):
- `token_swans` — legacy zigzag-анализатор, сохранён как архив
- `feature_mart` — legacy token-level mart

Legacy-слои убраны из runtime:
- `price_history`
- `token_analytics`

### Pipeline state
Операционное состояние хранится в `$POLYMARKET_DATA_DIR/database/collector_state.db`.

Текущие статусы:
- `downloaded_at`
- `trades_downloaded_at`
- `parsed_at`
- `error`

---

## Почему `prices-history` исключён

Ранее использовался CLOB `/prices-history`, но он оказался непригоден для стратегии swan-детекции.

Проблемы:
- сглаживает экстремумы;
- скрывает реальные внутридневные spikes;
- создаёт ложное представление о минимумах и максимумах;
- не даёт корректной оценки ликвидности на входе и выходе.

Вывод:
- `prices-history` не участвует в текущем пайплайне вообще.

---

## Практические правила эксплуатации

- Пайплайн должен быть идемпотентным: любой этап можно перезапустить.
- Долгие процессы на сервере запускать через `tmux`.
- Collector markets пишет в один лог:
  - `$POLYMARKET_DATA_DIR/logs/collector.log`
- Любые эвристики сначала проверяются на сырых данных, а потом попадают в архитектуру.
- Для анализа swans нельзя подменять raw trades агрегированными данными.

---

## Краткая формула проекта

Не искать "самый низкий print".

А искать:
- реальный локальный минимум;
- подтверждённую ликвидность на входе;
- реализуемый выход на target-x;
- и повторяемые паттерны с положительным EV.
