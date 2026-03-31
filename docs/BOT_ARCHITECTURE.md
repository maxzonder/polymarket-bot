# Polymarket Big Swan Bot — Архитектура и документация

*Дата написания: 2026-03-16*
*Связано с: [ROADMAP.md](./ROADMAP.md), [TRADING_LOGIC.md](./TRADING_LOGIC.md)*

---

## Что это

Торговый бот для Polymarket, заточенный на поиск редких событий с правым хвостом **50x–1000x** («чёрные лебеди»).

Стратегия основана на структурной особенности бинарных рынков: когда исход кажется маловероятным, цена упирается в пол ($0.001–$0.02). Downside уже почти исчерпан, upside — потенциально кратный. Большинство позиций сгорит, но редкие победители с лихвой окупают серию потерь.

---

## Три торговых режима

Режим задаётся переменной окружения `BOT_MODE`.

| Параметр | `fast_tp_mode` | `balanced_mode` | `big_swan_mode` |
|---|---|---|---|
| **Цель** | Частые 5–10x отскоки | Баланс EV и правого хвоста | Охота за 50x–1000x |
| **Вход** | Сканер (рынок уже на дне) | Сканер + рестинг биды | ТОЛЬКО рестинг биды |
| **Уровни входа** | $0.005–$0.05 | $0.002–$0.05 | $0.001–$0.01 |
| **Выход** | 70% @ 5x, 30% @ 10x | 35%@5x + 25%@10x + 20%@20x | 20%@5x + 20%@20x |
| **Moonbag** | 0% | 20% | **60% до resolution** |
| **Оптимизация** | EV суммарный | EV суммарный | `tail_ev` |
| **Ставка** | $0.05 | $0.05 | **$0.01 микростейк** |
| **Max позиций** | 30 | 20 | 100 |

**Ключевое** в `big_swan_mode`:
- Бот **не ждёт**, пока рынок упадёт. Он заранее расставляет рестинг лимитки на экстремальных уровнях ($0.001, $0.002, $0.005) и паника приходит к нашим ценам сама.
- 60% позиции держится до разрешения рынка (Polymarket платит $1.00 за токен победителю). Именно это ловит хвосты 100x–1000x.
- Нет трейлинг стопа — на тонком рынке Polymarket он создаёт ложное чувство защиты.

---

## Структура проекта

```
polymarket-bot/
│
├── main.py                     # Точка входа
├── config.py                   # Конфигурация трёх режимов
├── requirements.txt
│
├── api/
│   ├── gamma_client.py         # Gamma API — сканирование открытых рынков
│   ├── clob_client.py          # CLOB API — стакан, ордера, dry-run
│   └── data_api.py             # Polymarket Data API — последние трейды, timestamp активности
│
├── strategy/
│   ├── scorer.py               # Два независимых скора: fill + resolution
│   ├── screener.py             # Фильтрация и ранжирование кандидатов
│   └── risk_manager.py         # Сайзинг позиции, TP лестница, moonbag
│
├── execution/
│   ├── order_manager.py        # Размещение / отмена ордеров, positions.db
│   └── position_monitor.py     # Мониторинг fills и resolution (REST polling)
│
├── bot/
│   └── main_loop.py            # asyncio: три параллельных цикла
│
├── data_collector/             # Пайплайн сбора данных
│   ├── data_collector_and_parsing.py  # Скачивает рынки + трейды + парсит в SQLite
│   └── state_db.py             # Журнал состояния пайплайна
│
├── analyzer/                   # Оффлайн аналитика
│   ├── swan_analyzer.py        # Строит swans_v2 (resolution-aware)
│   └── market_level_features_v1_1.py  # Строит feature_mart_v1_1 для MarketScorer
│
├── scripts/
│   ├── daily_pipeline.py       # Оркестратор: ingest→analyzer→feature_mart→ml→feedback→recalibrate
│   ├── build_ml_outcomes.py    # ML-метки по реальным сделкам бота
│   ├── build_rejected_outcomes.py  # ML-метки по отклонённым рынкам
│   ├── analyze_empty_candidates.py  # Feedback penalties: штрафы по (category, vol_bucket)
│   └── recalibrate_scorers.py  # Пересчёт порогов → recommended_config.json
│
├── _legacy/                    # Устаревшие скрипты (не используются)
│
├── utils/
│   ├── logger.py               # RotatingFileHandler логгер
│   └── paths.py                # Пути (DATA_DIR, DB_PATH и т.д.)
│
└── docs/
    ├── BOT_ARCHITECTURE.md     # Этот файл
    ├── ROADMAP.md
    ├── TRADING_LOGIC.md
    ├── DATABASE_SCHEMA.md
    └── API_REFERENCE.md
```

---

## База данных

### Основная БД: `polymarket_dataset.db`

Путь: `$POLYMARKET_DATA_DIR/polymarket_dataset.db` (по умолчанию — корень проекта).

| Таблица | Что хранит | Откуда берётся |
|---|---|---|
| `markets` | Метаданные рынков | `data_collector_and_parsing.py` ← Gamma API |
| `tokens` | YES/NO токены, `is_winner` | `data_collector_and_parsing.py` ← Gamma API |
| `swans_v2` | Resolution-aware лебеди | `analyzer/swan_analyzer.py` ← raw trades |
| `feature_mart_v1_1` | Market-level фичи для MarketScorer | `analyzer/market_level_features_v1_1.py` ← swans_v2 |
| `ml_rejected_outcomes` | Метки пропущенных возможностей | `pipeline/build_rejected_outcomes.py` |
| `token_swans` | **Архив** zigzag-анализатора (legacy) | не обновляется |

### Операционные БД (создаются ботом в рантайме)

**`positions.db`** — активные позиции и ордера:
- `resting_orders` — рестинг BUY биды (live / matched / cancelled)
- `positions` — открытые позиции (open / resolved)
- `tp_orders` — TP SELL ордера + moonbag записи

**`paper_trades.db`** — виртуальные ордера в dry-run режиме

### Raw trades

Не в SQLite — в JSON файлах:
```
$POLYMARKET_DATA_DIR/database/YYYY-MM-DD/{market_id}_trades/{token_id}.json
```

Формат: `[{"side":"BUY","size":72,"price":0.002,"timestamp":1760468941}, ...]`

---

## Ключевые концепции

### 1. Два независимых скора (никогда не смешивать)

```python
# strategy/scorer.py

EntryFillScore   → P(рынок дойдёт до нашего рестинг бида)
ResolutionScore  → tail_ev после входа (E[real_x | real_x>=20x] * P(real_x>=20x))
```

**Почему не один скор:** рынок может часто падать в дно (высокий fill score) но почти никогда не выигрывать (низкий resolution score). Это bounce-only рынок — ему место в `fast_tp_mode`, но не в `big_swan_mode`. Объединение этих сигналов убивает различие.

Оба скора вычисляются из `swans_v2` сгруппированной по категории рынка.

### 2. Метрика оптимизации: `tail_ev`, не `win_rate`

```
tail_ev = E[real_x | real_x >= 20x]  ×  P(real_x >= 20x)
```

Win rate намеренно игнорируется. Большинство позиций сгорит — это нормально. Редкие 100x+ победители перекрывают всё.

### 3. Partial TP + moonbag в big_swan_mode

```
Вход: $0.002 × 5 токенов = $0.01 stake
  → 1 токен SELL @ $0.010  (5x, TP ордер)   ← возврат капитала
  → 1 токен SELL @ $0.040  (20x, TP ордер)  ← частичная фиксация
  → 3 токена          → hold до resolution  ← moonbag
                                               если выиграет → $3.00 = 300x
```

Moonbag **не продаётся на рынке** — Polymarket сам выплачивает $1.00 за токен при resolution.

### 4. real_x vs resolution_x

- `real_x` — фактический икс от max рыночной цены: `max_price / entry_min_price`
- `resolution_x` — если рынок выиграл: `1.0 / entry_min_price`

Пример: купили @ $0.002. Рыночный max = $0.03 (15x). Но рынок закрылся YES → `resolution_x = 500x`.

---

## Главный цикл (main_loop.py)

Три параллельных asyncio задачи:

```
┌─────────────────────────────────────────────────────────────┐
│                    BotRunner.run()                          │
│                                                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────┐  │
│  │  Screener Loop   │  │  Monitor Loop    │  │ Cleanup  │  │
│  │  каждые 5 мин    │  │  каждые 90 сек   │  │ каждый   │  │
│  │                  │  │                  │  │ час      │  │
│  │ Gamma API scan   │  │ Check fills      │  │          │  │
│  │ Score candidates │  │ Simulate fills   │  │ Cancel   │  │
│  │ Place bids       │  │ (dry-run)        │  │ stale    │  │
│  │                  │  │ Check resolution │  │ bids     │  │
│  │                  │  │ Log PnL stats    │  │ Refresh  │  │
│  │                  │  │                  │  │ scorers  │  │
│  └──────────────────┘  └──────────────────┘  └──────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Поток данных

```
Gamma API (открытые рынки)
      │
      ▼
  Screener.scan()
  ├─ Hard filters: volume, hours_to_close, price <= entry_price_max
  ├─ EntryFillScorer  (из swans_v2 по категории)
  └─ ResolutionScorer (из swans_v2 по категории)
      │
      ▼
  EntryCandidate list (отсортирован по total_score)
      │
      ▼
  OrderManager.process_candidate()
  ├─ Проверяет дубликаты (уже есть бид на этом уровне?)
  ├─ RiskManager.size_position() → stake_usdc, token_quantity
  └─ ClobClient.place_limit_order(BUY, GTC)
      │
  [рынок падает, бид исполняется]
      │
      ▼
  PositionMonitor.check_all()  (каждые 90 сек)
  └─ on_entry_filled()
      ├─ Создаёт запись в positions
      └─ Размещает TP SELL ордера (через RiskManager.build_tp_orders())
          │
      [рынок растёт, TP исполняется]   [рынок разрешается → $1 или $0]
          │                                      │
          ▼                                      ▼
      on_tp_filled()                    on_market_resolved()
      (частичная фиксация)              (moonbag PnL)
```

---

## Запуск

### Зависимости

```bash
pip install -r requirements.txt
# Включает: requests, python-dotenv, py-clob-client
```

### Переменные окружения (`.env` в корне проекта)

```bash
# Режим и dry-run
BOT_MODE=big_swan_mode        # big_swan_mode | balanced_mode | fast_tp_mode
DRY_RUN=true                  # true = симуляция, false = реальная торговля

# Путь к данным (если не в корне проекта)
POLYMARKET_DATA_DIR=/home/polybot/.polybot

# Ключи для реальной торговли (не нужны для dry-run)
POLY_PRIVATE_KEY=0x...
```

### Команды запуска

```bash
# Dry-run (безопасно, ничего не тратит)
python3 main.py

# Конкретный режим
BOT_MODE=balanced_mode python3 main.py

# Реальная торговля
DRY_RUN=false python3 main.py
```

### Пайплайн сбора данных (первоначальный запуск)

```bash
# 1. Скачать рынки + трейды + распарсить в SQLite
python3 data_collector/data_collector_and_parsing.py --start 2025-08-01 --end 2026-03-28

# 2. Построить swans_v2
python3 analyzer/swan_analyzer.py --recompute --date-from 2025-08-01 --date-to 2026-03-28

# 3. Построить feature_mart_v1_1 (для MarketScorer)
python3 analyzer/market_level_features_v1_1.py --recompute
```

После первоначального запуска ежедневный пайплайн автоматизирован:
```bash
# Каждый день (04:00 UTC): ingest → analyzer → feature_mart_v1_1 → ml → recalibrate
python3 pipeline/daily_pipeline.py
```

---

## Описание модулей

### `config.py`

Содержит три инстанса `ModeConfig` (`FAST_TP_MODE`, `BALANCED_MODE`, `BIG_SWAN_MODE`) и класс `BotConfig` с runtime настройками. Импортируется всеми модулями.

Ключевые параметры `ModeConfig`:
- `entry_price_levels` — уровни для рестинг бидов
- `tp_levels` — список `TPLevel(x=5.0, fraction=0.20)` — TP лестница
- `moonbag_fraction` — доля позиции, которая держится до resolution
- `use_resting_bids` / `scanner_entry` — стратегия входа
- `optimize_metric` — `"tail_ev"` | `"ev_total"` | `"roi_pct"`

### `api/gamma_client.py`

Публичный клиент Gamma API. Функция `fetch_open_markets(price_max, volume_min, volume_max)` возвращает список `MarketInfo` — лёгких объектов с ценой, объёмом, token_ids и т.д.

`MarketInfo` содержит поле `neg_risk_group_id: Optional[str]` — заполняется из `negRiskMarketID` или `negRiskRequestID`.

**Важно:** комментарии рынка (`comment_count`) живут в `market["events"][0]["commentCount"]`, не на верхнем уровне.

### `api/data_api.py`

Клиент Polymarket Data API (`https://data-api.polymarket.com`).

- `get_last_trade_ts(condition_id) → Optional[int]` — timestamp последней сделки по рынку
- `get_recent_trades(condition_id, limit=50) → list[dict]` — последние N сделок

Используется `screener.py` для фильтрации мёртвых рынков.

### `api/clob_client.py`

Два режима:

**dry_run=True:** все ордера записываются в `paper_trades.db`. Симуляция fills происходит в `PositionMonitor` путём сравнения цен стакана с ценами ордеров.

**dry_run=False:** использует `py-clob-client` для подписанных ордеров на Polymarket. Требует `POLY_PRIVATE_KEY`.

Публичный метод `get_orderbook(token_id)` работает в обоих режимах (нужен для симуляции fills).

### `strategy/scorer.py`

Два отдельных класса, каждый со своим кэшем:

**`EntryFillScorer`** — вычисляет `P(fill)` для категории:
- Считает, в скольких рынках категории был хотя бы один swans_v2 event
- Делит на общее количество рынков категории
- Взвешивает по среднему объёму на дне

**`ResolutionScorer`** — вычисляет tail EV для категории:
- `p_winner` = доля is_winner=1 среди swans_v2
- `p_20x`, `p_50x`, `p_100x` = доли с real_x >= N
- `tail_ev` = `E[real_x | real_x>=20x] * P(real_x>=20x)`
- Итоговый `score` = `tail_ev/50 * 0.7 + p_winner * 0.3`

Оба класса кэшируют результаты в памяти. Метод `.refresh()` перечитывает из БД (вызывается в cleanup loop раз в час).

### `strategy/market_scorer.py`

`MarketScorer` читает `feature_mart_v1_1` при инициализации и применяет два дополнительных штрафа в `_score_raw()`:

- **`loser_penalty`** — вычисляется из `loser_rate` по (category, vol_bucket): rate≤0.70 → 1.0, rate=0.85 → 0.70, rate=1.0 → 0.40 (floor 0.3). Загружается методом `_load_analogy_table()` из `was_swan` + `swan_is_winner` в `feature_mart_v1_1`.
- **`feedback_penalty`** — читается из таблицы `feedback_penalties` в dataset DB (генерирует `analyze_empty_candidates.py`). Default = 1.0 для незнакомых сегментов.

`total_score *= loser_penalty * feedback_penalty`

### `strategy/screener.py`

Hard фильтры (до скоринга, дёшево):
- Рынок активен
- `hours_to_close` в допустимом диапазоне
- Есть token_ids
- `entry_fill_score >= min_entry_fill_score` (из режима)
- `resolution_score >= min_resolution_score` (из режима)
- **Dead market filter:** `get_last_trade_ts()` — рынок отклоняется если >48ч без сделок (логируется как `rejected_dead_market`; порог задаётся `BotConfig.dead_market_hours`)

Для каждого токена (YES / NO) отдельно вычисляется `current_price`:
- YES = `market.best_ask`
- NO ≈ `1.0 - best_ask`

Отбираются только токены с `price <= entry_price_max` и `price > 0`.

`total_score` = взвешенная сумма `(ef_score, res_score, duration_score, liquidity_score, category_weight)` — веса зависят от `optimize_metric` режима.

**Neg-risk группировка:** `scan()` разделяет рынки на два потока:
- Бинарные рынки (neg_risk_group_id=None) → `_evaluate_market()` как прежде
- Neg-risk группы → `_evaluate_neg_risk_group(group_id, markets)`:
  - Определяет фаворита (токен с наибольшей YES-ценой)
  - `group_boost`: фав≥0.80 → 1.5, ≥0.60 → 1.2, ≥0.40 → 1.0, иначе 0.7
  - Если фаворит упал ≥20% за 24ч → `group_boost *= 1.5` (структурный катализатор)
  - Фильтрует underdog-токены (price ≤ entry_price_max), топ N по объёму
  - Каждый underdog оценивается через `_evaluate_market(group_boost=...)`

### `strategy/risk_manager.py`

Чистые функции без side effects.

**`size_position(token_id, entry_price, resolution_score, open_positions)`:**
- Базовая ставка = `mode_config.stake_usdc`
- В `big_swan_mode` масштабируется на `0.5 + res_score * 0.5`
- Жёсткий cap = `balance * max_capital_deployed_pct / max_open_positions`
- Отклоняет позицию если `entry_price < 0.0005` без исторических данных (защита от артефактов пустого стакана)

**`build_tp_orders(position)`:**
- Для каждого TPLevel создаёт `TPOrder(sell_price=entry*x, sell_quantity=total_qty*fraction)`
- Moonbag (label=`moonbag_resolution`) записывается как виртуальный TP @ $1.00 — реального ордера нет

### `execution/order_manager.py`

Хранит состояние в `positions.db` (SQLite, WAL mode).

**`process_candidate(candidate)`:**
1. Берёт активные цены рестинг бидов для этого token_id
2. Для каждого уровня из `suggested_entry_levels`: пропускает если уже есть активный бид
3. Вызывает `risk.size_position()`
4. `clob.place_limit_order(BUY, GTC)`
5. Сохраняет в `resting_orders`

**`on_entry_filled(order_id, ...)`:**
1. Создаёт запись в `positions`
2. Вызывает `risk.build_tp_orders()`
3. Размещает TP SELL ордера (кроме moonbag — только запись в таблицу)

**`cancel_stale_orders()`:**
- Отменяет биды старше `resting_order_ttl` (24ч по умолчанию)
- Также проверяет через Gamma API, живы ли ещё рынки

**Таблица `resting_orders`** содержит поле `neg_risk_group_id TEXT` (индекс `idx_resting_group`). Кластерный cap считается по группам neg-risk; бинарные рынки (neg_risk_group_id=NULL) кластерный cap не применяют.

**Spread gate:** после проверки thin-book применяется адаптивный spread gate — логируется как `spread_gate`. Максимальный spread: 30% для mid≥0.05, 80% для low-price токенов.

### `execution/position_monitor.py`

REST polling каждые 90 секунд. Без WebSocket (позиции живут часами, 90с задержка несущественна).

**Dry-run режим:**
- Для каждого живого BUY ордера: запрашивает `get_orderbook(token_id)`, если `best_ask <= bid_price` → симулирует fill
- Для живых SELL TP: если `best_bid >= sell_price` → симулирует fill

**Live режим:**
- `clob.get_all_orders()` → ищет `status=matched` → вызывает `on_entry_filled` / `on_tp_filled`
- Для resolution: проверяет `outcomePrices` из Gamma API

### `analyzer/market_level_features_v1_1.py`

Оффлайн скрипт. Запускать после `swan_analyzer.py`.

Строит таблицу `feature_mart_v1_1` — агрегирует `swans_v2 + markets + tokens` на уровне рынка.
Одна строка = один рынок. Источник позитивов: рынки с `buy_min_price <= ENTRY_MAX (0.20)`.

Ключевые поля:
- `has_swan`, `n_swans` — наличие и количество swan events
- `best_buy_min_price`, `best_max_traded_x` — лучшая сделка на рынке
- `best_floor_duration_s`, `best_time_to_res_h` — временные характеристики
- `log_volume`, `log_duration_h` — log-трансформации

Используется `MarketScorer` для обучения и инференса.

---

## Защита от ошибок

### Антимусорная фильтрация рынков

- `entry_price < 0.0005` без исторических данных → позиция отклоняется (артефакты пустого стакана)
- `max_volume_usdc = 50,000` — не смотрим на крупные мейнстримные рынки (хуже edge)
- Дубликаты бидов: max один активный бид на (token_id, price_level)

### Защита от переобучения (важно при обучении скоринга)

- `scorer_min_samples = 5` — не вычисляем скор по категории с < 5 примерами
- Fallback на глобальный средний скор с penalty (× 0.6–0.7) для незнакомых категорий
- **Survivorship bias:** `swans_v2` содержит только рынки, по которым были собраны raw trades. Рынки без трейдов (объём < 50 USDC) не попадают в датасет.
- **Lookahead:** `is_winner` в `swans_v2` — это финальный результат рынка. При обучении модели проверять, что все фичи были доступны на момент входа.

---

## FAQ

**Q: Почему нет WebSocket?**
A: Позиции живут часами. Узнать о fill через 90 секунд вместо мгновенно — некритично. WebSocket требует реконнект-логику, heartbeat, out-of-order обработку. REST проще и надёжнее для этого use case.

**Q: Почему не trailing stop?**
A: На тонком рынке Polymarket синтетический trailing stop не гарантирует исполнения. Цена может идти 0.001 → 0.05 → 0.01 → 1.0. Стоп на -50% от пика вышибет позицию при первом откате и лишит 20x–1000x хвоста.

**Q: Как работает dry-run?**
A: Бот делает настоящие запросы к Gamma API (читает рынки) и CLOB API (читает стаканы). Но вместо подачи ордеров пишет в `paper_trades.db`. Fills симулируются: если `best_ask <= bid_price` — считаем, что BUY исполнился.

**Q: Что такое `neg_risk` в markets?**
A: Negative risk market — группа рынков, где сумма цен токенов не может превышать 1. Особый случай ценообразования, учитывается в `feature_mart` как отдельная фича.

**Q: Почему `max_volume_usdc = 50,000`?**
A: Рынки с объёмом $50k+ хорошо информационно эффективны (много участников, маркет-мейкеры). Edge там минимален. Наш edge — в нишевых рынках $500–$5,000, где ценообразование хуже.

---

## Следующие шаги (не реализованы)

1. **ML скоринг** — заменить SQL-эвристику в `scorer.py` на CatBoost/LightGBM модель, обученную на `feature_mart`. Оффлайн-обучение, онлайн-инференс.

2. **Profit feedback loop** — после каждой resolved позиции обновлять `category_weights` в конфиге по реальному EV (задокументировано в `TRADING_LOGIC.md` раздел 4).

3. **Cohort mining** — автоматическое обнаружение повторяющихся паттернов (Elon-рынки, crypto price triggers и т.д.) через кластеризацию `question` текстов.

4. **WebSocket** — для более быстрого обнаружения fills. Приоритет низкий — see FAQ.

5. **Telegram alerts** — задокументировано в `LOGGING_AND_ALERTS.md`.
