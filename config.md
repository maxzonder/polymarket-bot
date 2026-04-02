# config.py

## Обзор

`config.py` определяет все параметры торговой стратегии. Два класса конфигурации:

- **`ModeConfig`** — неизменяемая конфигурация стратегии для одного режима
- **`BotConfig`** — runtime-конфигурация из `.env` / переменных окружения

Активный режим выбирается через переменную `BOT_MODE` (по умолчанию: `big_swan_mode`).

---

## Торговые режимы

### `big_swan_mode` ← активный

Resting bids на глубоком флоре, удержание до резолюции.

| Параметр | Значение |
|---|---|
| `entry_price_levels` | `(0.001, 0.005, 0.01)` — resting bids на трёх уровнях |
| `entry_price_max` | `0.20` — рынки выше пропускаются |
| `use_resting_bids` | `True` |
| `scanner_entry` | `False` — только resting, не гоняться за дипами |
| `tp_levels` | 10× → 10%, 20× → 10% |
| `moonbag_fraction` | `0.80` — 80% удерживается до резолюции |
| `min_hours_to_close` | `0.25` (15 мин) |
| `max_hours_to_close` | `24.0` |
| `optimize_metric` | `tail_ev` |
| `max_open_positions` | `500` |
| `max_resting_markets` | `5000` |
| `max_resting_per_cluster` | `10` |
| `max_capital_deployed_pct` | `0.99` |
| `max_exposure_per_market` | `$2.00` (на один токен) |
| `min_market_score` | `0.25` — рынки ниже отклоняются |

**Ставки (market_score_tiers):**

| market_score | ставка за уровень |
|---|---|
| ≥ 0.60 | $0.50 |
| ≥ 0.40 | $0.25 |
| ≥ 0.25 | $0.10 |
| < 0.25 | reject |

---

### `small_swan_mode`

Resting bids на умеренных дипах, ставка зависит от глубины входа.

| Параметр | Значение |
|---|---|
| `entry_price_levels` | `(0.05, 0.10, 0.15, 0.20)` |
| `entry_price_max` | `0.50` |
| `use_resting_bids` | `True` |
| `scanner_entry` | `False` |
| `tp_levels` | 2× → 30%, 5× → 30% |
| `moonbag_fraction` | `0.40` |
| `min_hours_to_close` | `1.0` |
| `max_hours_to_close` | `120.0` |
| `optimize_metric` | `ev_total` |
| `max_open_positions` | `100` |
| `max_resting_per_cluster` | `1` |

**Ставки (stake_tiers по цене входа):**

| цена входа | ставка |
|---|---|
| ≤ 0.05 | $0.20 |
| ≤ 0.10 | $0.10 |
| ≤ 0.15 | $0.05 |
| ≤ 0.20 | $0.05 |

---

### `balanced_mode`

Resting bids + scanner, умеренный moonbag.

| Параметр | Значение |
|---|---|
| `entry_price_levels` | `(0.002, 0.005, 0.01, 0.02, 0.05)` |
| `entry_price_max` | `0.10` |
| `use_resting_bids` | `True` |
| `scanner_entry` | `True` |
| `tp_levels` | 5× → 35%, 10× → 25%, 20× → 20% |
| `moonbag_fraction` | `0.20` |
| `stake_usdc` | `$0.05` (flat) |
| `max_open_positions` | `20` |

---

### `fast_tp_mode`

Scanner-вход, быстрый выход, нет moonbag.

| Параметр | Значение |
|---|---|
| `entry_price_levels` | `(0.005, 0.01, 0.02, 0.03, 0.05)` |
| `entry_price_max` | `0.05` |
| `use_resting_bids` | `False` |
| `scanner_entry` | `True` |
| `tp_levels` | 5× → 70%, 10× → 30% |
| `moonbag_fraction` | `0.0` |
| `stake_usdc` | `$0.05` (flat) |
| `max_open_positions` | `30` |

---

## Поля ModeConfig (справочник)

### Вход

| Поле | Тип | Описание |
|---|---|---|
| `entry_price_levels` | `tuple[float, ...]` | Ценовые уровни для resting-заявок. Screener выставляет только уровни **ниже** текущей цены рынка. |
| `entry_price_max` | `float` | Максимальная текущая цена для скрининга. |
| `use_resting_bids` | `bool` | Выставлять ли ордера заранее, до падения цены к уровню. |
| `scanner_entry` | `bool` | Входить ли, если scanner видит цену уже в зоне входа. |

### Выход

| Поле | Тип | Описание |
|---|---|---|
| `tp_levels` | `tuple[TPLevel, ...]` | Лестница тейк-профита. `TPLevel(x, fraction)` продаёт `fraction` позиции при достижении `entry_price * x`. |
| `moonbag_fraction` | `float` | Доля позиции, удерживаемая до резолюции. |

### Фильтры scoring

| Поле | Тип | Описание |
|---|---|---|
| `min_entry_fill_score` | `float` | Мин. P(рынок когда-либо достигнет зоны входа). |
| `min_resolution_score` | `float` | Мин. resolution score для принятия кандидата. |
| `min_real_x_historical` | `float` | Мин. наблюдаемый множитель цены в исторических данных. |
| `min_market_score` | `float` | Мин. композитный `market_score`. `0.0` = отключено. |

### Размер позиции

| Поле | Тип | Описание |
|---|---|---|
| `stake_usdc` | `float` | Fallback-стейк. Используется если ни один тир не совпал. |
| `max_open_positions` | `int` | Жёсткий лимит одновременно открытых позиций. |
| `max_resting_markets` | `int` | Макс. число рынков с живыми resting-заявками. |
| `max_resting_per_cluster` | `int` | Макс. рынков на neg-risk группу. |
| `max_capital_deployed_pct` | `float` | Макс. доля баланса в открытых позициях. |
| `max_exposure_per_market` | `float` | Макс. суммарный USDC по всем филам на одном токене. `0.0` = отключено. |

### Стейк-тиры

Два взаимоисключающих расписания — `market_score_tiers` имеет приоритет:

#### `market_score_tiers` (по score, v1.1) — используется в `big_swan_mode`

```python
# формат: ((min_score_threshold, stake_usdc), ...)
market_score_tiers: tuple[tuple[float, float], ...] = ()
```

Для каждого фила: найти наивысший порог ≤ score → использовать его стейк. Полностью заменяет `stake_tiers`.

#### `stake_tiers` (по цене, legacy) — используется в `small_swan_mode`

```python
# формат: ((max_entry_price, stake_usdc), ...)
stake_tiers: tuple[tuple[float, float], ...] = ()
```

Для каждого фила: найти первый тир где `fill_price <= max_entry_price` → использовать его стейк. Идея: глубже флор = больше стейк.

### Временное окно

| Поле | Тип | Описание |
|---|---|---|
| `min_hours_to_close` | `float` | Отклонять рынки, закрывающиеся раньше этого. |
| `max_hours_to_close` | `float` | Отклонять рынки, закрывающиеся позже этого. |
| `hours_to_close_null_default` | `float` | Если Gamma вернул `None` для дедлайна — считать это количество часов. По умолчанию: 48h. |

---

## Поля BotConfig (Runtime)

Загружается из окружения / `.env`. Запуск: `BOT_MODE=big_swan_mode DRY_RUN=false python main.py`.

| Поле | Env var | По умолчанию | Описание |
|---|---|---|---|
| `mode` | `BOT_MODE` | `big_swan_mode` | Активный торговый режим |
| `dry_run` | `DRY_RUN` | `true` | Бумажная торговля |
| `private_key` | `POLY_PRIVATE_KEY` | — | Приватный ключ CLOB-кошелька |
| `api_key` | `POLY_API_KEY` | — | API-ключ Polymarket |
| `api_secret` | `POLY_API_SECRET` | — | API-секрет Polymarket |
| `api_passphrase` | `POLY_PASSPHRASE` | — | Passphrase Polymarket |
| `screener_interval` | — | 300с | Интервал запуска screener |
| `monitor_interval` | — | 90с | Интервал проверки открытых позиций |
| `resting_cleanup_interval` | — | 3600с | Интервал очистки устаревших resting-заявок |
| `min_volume_usdc` | — | 50 | Мин. суммарный объём рынка для скрининга |
| `max_volume_usdc` | — | 300 000 | Макс. суммарный объём рынка для скрининга |
| `dead_market_hours` | — | 48h | Отклонять рынки без трейдов за это время |
| `scorer_entry_price_max` | — | 0.02 | Использовать только строки `swans_v2` с ценой ≤ этого значения для scoring |
| `scorer_min_samples` | — | 5 | Мин. число сэмплов для надёжного resolution score |

### `category_weights`

Множитель EV на категорию, применяется к market score. Нормализован к crypto = 1.0:

| Категория | Вес |
|---|---|
| geopolitics | 1.5 |
| politics | 1.5 |
| crypto | 1.0 |
| weather | 1.0 |
| health | 1.0 |
| esports | 1.0 |
| sports | 0.8 |
| tech | 0.6 |
| entertainment | 0.5 |

---

## Глобальные константы

| Константа | Значение | Описание |
|---|---|---|
| `SWAN_BUY_PRICE_THRESHOLD` | `0.20` | Фиксированная константа — потолок цены при сборе данных в swan_analyzer. История первична: бот не должен ставить выше этого уровня. |
| `SWAN_MIN_BUY_VOLUME` | 1.0 USDC | Мин. объём на флоре (проверка ликвидности) |
| `SWAN_MIN_SELL_VOLUME` | 30.0 USDC | Мин. объём на выходе (качество фила) |
| `SWAN_MIN_REAL_X` | 5.0× | Мин. множитель для учёта события как настоящего swan |
