# Config Flow — как конфиг читается по всей системе

## Краткая карта

```
config.py
├── BotConfig  — runtime (из .env)
│   ├── mode → ModeConfig  — стратегия
│   ├── screener_interval, monitor_interval, resting_cleanup_interval
│   ├── min_volume_usdc, max_volume_usdc, dead_market_hours
│   └── category_weights
└── ModeConfig  — иммутабельный объект стратегии
    ├── entry_price_levels, entry_price_max
    ├── use_resting_bids, scanner_entry
    ├── tp_levels, moonbag_fraction
    ├── stake_usdc, stake_tiers, market_score_tiers
    ├── max_open_positions, max_resting_markets, max_resting_per_cluster
    ├── max_exposure_per_market
    ├── min_hours_to_close, max_hours_to_close
    ├── min_market_score
    └── scoring_weights, prefer_long_duration
```

---

## 1. Сбор данных (`data_collector_and_parsing.py`)

**Конфиг не читается напрямую.** Сборщик работает независимо — скачивает все рынки с Polymarket по заданным датам.

Единственная косвенная связь: `SWAN_BUY_PRICE_THRESHOLD` не используется здесь, но определяет что именно считается "значимой покупкой" на этапе анализа.

```
python data_collector/data_collector_and_parsing.py --start 2026-01-01 --end 2026-01-31
```

Результат: `database/YYYY-MM-DD/{market_id}_trades/{token_id}.json` + строки в `polymarket_dataset.db` (таблицы `markets`, `tokens`).

---

## 2. Swan Analyzer (`analyzer/swan_analyzer.py`)

**Читает из `config.py`:**

| Константа | Значение | Откуда |
|---|---|---|
| `SWAN_BUY_PRICE_THRESHOLD` | `max(entry_price_levels)` по всем режимам | auto, сейчас = 0.50 |
| `SWAN_ENTRY_MAX` | = выше | alias |
| `SWAN_MIN_BUY_VOLUME` | 1.0 USDC | константа |
| `SWAN_MIN_SELL_VOLUME` | 30.0 USDC | константа |
| `SWAN_MIN_REAL_X` | 5.0× | константа |

`SWAN_BUY_PRICE_THRESHOLD` — это потолок цены покупки для определения "swan event". Если `entry_price_levels` в любом режиме поднять выше текущего максимума, порог автоматически поднимется и аналайзер начнёт собирать данные с новых уровней.

```
python analyzer/swan_analyzer.py
# --buy-price-threshold аргумент дефолтится на SWAN_BUY_PRICE_THRESHOLD
```

Результат: `swans_v2` в `polymarket_dataset.db`.

---

## 3. Feature Mart (`analyzer/market_level_features_v1_1.py`)

**Читает из `config.py`:**

| Что | Поле | Использование |
|---|---|---|
| `SWAN_ENTRY_MAX` | = `SWAN_BUY_PRICE_THRESHOLD` | Граница "swan event" в SQL WHERE |
| `BotConfig().min_volume_usdc` | `min_volume_usdc` (дефолт 50) | Фильтр рынков при построении feature_mart_v1_1 |

`BotConfig()` создаётся с дефолтными значениями (не из .env). Это нормально — `min_volume_usdc` нужен только чтобы отсечь мусорные рынки из обучающей выборки, а не для live-торговли.

```
python analyzer/market_level_features_v1_1.py --recompute
```

Результат: `feature_mart_v1_1` в `polymarket_dataset.db` — market-level фичи (swan_rate, win_rate, avg_x по (category, vol_bucket)).

---

## 4. ML Outcomes (`pipeline/build_ml_outcomes.py`)

**Конфиг не читается.** Работает с `positions.db` напрямую — берёт принятых кандидатов и проставляет лейблы (got_fill, is_winner, realized_roi).

---

## 5. Feedback Penalties (`pipeline/analyze_empty_candidates.py`)

**Конфиг не читается.** Читает `ml_outcomes` из `positions.db`, пишет `feedback_penalties` в `polymarket_dataset.db`.

---

## 6. Recalibrate (`pipeline/recalibrate_scorers.py`)

**Читает из `config.py`:**

```python
from config import BIG_SWAN_MODE, BotConfig
cfg = BotConfig()
```

Использует `BIG_SWAN_MODE.entry_price_levels` и `SWAN_BUY_PRICE_THRESHOLD` как ориентиры при анализе порогов. Результат → `recommended_config.json`.

---

## 7. Бот (`main.py` → `bot/main_loop.py`)

**Главная точка входа.** Читает весь конфиг:

```python
config = load_config()  # читает .env → BotConfig
```

Переменные окружения:
```
BOT_MODE=big_swan_mode   # какой ModeConfig активен
DRY_RUN=true             # paper trading или live
POLY_PRIVATE_KEY=...     # нужен только при DRY_RUN=false
```

### Что из конфига куда идёт

```
BotConfig
│
├── .mode → ModeConfig mc
│   ├── MarketScorer (strategy/market_scorer.py)
│   │     min_score ← mc.min_market_score
│   │     (данные из feature_mart_v1_1 в SQLite)
│   │
│   ├── Screener (strategy/screener.py)
│   │     volume range    ← config.min_volume_usdc / max_volume_usdc
│   │     dead_market     ← config.dead_market_hours
│   │     price gate      ← mc.entry_price_max
│   │     entry levels    ← mc.entry_price_levels (только < current_price)
│   │     neg-risk boost  ← mc.max_resting_per_cluster (cap underdogs)
│   │     score gate      ← mc.min_market_score
│   │     category weight ← config.category_weights[market.category]
│   │
│   ├── RiskManager (strategy/risk_manager.py)
│   │     sizing priority:
│   │       1. mc.market_score_tiers (если задан) → stake по score
│   │       2. mc.stake_tiers (legacy)             → stake по цене
│   │       3. mc.stake_usdc                       → fallback
│   │     reject if market_score < mc.min_market_score
│   │
│   └── OrderManager (execution/order_manager.py)
│         tp_levels       ← mc.tp_levels
│         moonbag_fraction← mc.moonbag_fraction
│         max_exposure    ← mc.max_exposure_per_market
│
├── .screener_interval  → screener loop sleep (300s)
├── .monitor_interval   → position monitor sleep (90s)
└── .resting_cleanup_interval → cleanup loop sleep (3600s)
```

---

## 8. Daily Pipeline (`pipeline/daily_pipeline.py`)

Оркестратор — запускает шаги 1–6 через subprocess. **Сам конфиг не читает** (каждый шаг читает его самостоятельно при старте).

Порядок шагов:
```
ingest → analyzer → feature_mart_v1_1 → ml_outcomes → rejected_outcomes → feedback_penalties → recalibrate
```

---

## Как конфиг влияет на весь цикл

```
Изменение в config.py
│
├── entry_price_levels ──→ SWAN_BUY_PRICE_THRESHOLD (auto) ──→ swan_analyzer потолок сбора
│                      └─→ Screener: какие уровни выставляются
│                      └─→ RiskManager: stake_tiers матчинг по цене
│
├── min_volume_usdc ────→ Screener: фильтр кандидатов
│                     └─→ feature_mart_v1_1: фильтр обучающей выборки
│
├── market_score_tiers ─→ RiskManager: sizing по quality score
│                      └─→ MarketScorer.min_score: gate на вход
│
├── category_weights ───→ Screener._compute_total_score: EV multiplier
│
└── dead_market_hours ──→ Screener: фильтр мёртвых рынков
                       └─→ replay runners: тот же фильтр offline
```

---

## Точки рассинхронизации (известные)

| Ситуация | Эффект |
|---|---|
| Добавить новый режим с `entry_price_levels` > 0.50 | `SWAN_BUY_PRICE_THRESHOLD` поднимется, `swans_v2` начнёт собирать новые уровни — но исторических данных по ним ещё нет |
| Поменять `min_volume_usdc` | Screener применит новое значение сразу; `feature_mart_v1_1` применит при следующем `--recompute` |
| Поменять `category_weights` | Вступает в силу при следующем цикле screener, без перезапуска бота |
| Поменять `market_score_tiers` | Вступает в силу при перезапуске бота (MarketScorer инициализируется при старте BotRunner) |
