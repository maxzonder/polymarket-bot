# config.py

## Обзор

`config.py` описывает:
- структуры `TPLevel`, `ModeConfig`, `BotConfig`
- предустановленные режимы `fast_tp_mode`, `balanced_mode`, `big_swan_mode`, `small_swan_mode`
- словарь `MODES`
- глобальные swan-константы
- runtime-валидацию активного режима

Активный режим выбирается через `BOT_MODE`. По умолчанию используется `big_swan_mode`.

`.env` загружается из корня репозитория. Если `python-dotenv` не установлен, файл всё равно парсится вручную.

## TPLevel

`TPLevel` описывает одну ступень тейк-профита:
- `progress` — прогресс от цены входа до `$1.00`, диапазон `0.0–1.0`
- `fraction` — доля позиции, продаваемая на этой ступени

Пример:
- `TPLevel(progress=0.50, fraction=0.20)` значит продать 20% позиции на середине пути от цены входа к `$1.00`.

## ModeConfig

`ModeConfig` это полный профиль торгового режима.

### Поля входа

- `name`
  - имя режима
- `entry_price_levels`
  - tuple ценовых уровней входа
  - это потолки допустимой цены покупки
  - если рынок уже торгуется дешевле уровня, бот может купить по лучшей текущей цене
- `entry_price_max`
  - максимальная текущая цена рынка, при которой рынок вообще рассматривается
- `use_resting_bids`
  - выставлять ли resting bids заранее
- `scanner_entry`
  - разрешён ли вход, если scanner уже видит рынок в зоне входа

### Поля выхода

- `tp_levels`
  - tuple ступеней `TPLevel`
  - сумма всех `fraction` должна быть не больше `1 - moonbag_fraction`
- `moonbag_fraction`
  - доля позиции, удерживаемая до резолюции

### Поля сайзинга и риска

- `stake_usdc`
  - fallback stake, если ни один tier не сработал
- `stake_per_level`
  - explicit stake на один entry level для live/paper swan runner
  - `0.0` значит fallback на `stake_usdc`
  - для `black_swan_mode` активное значение — `$1.00` на каждый level
- `max_open_positions`
  - максимум одновременно открытых позиций
- `max_resting_markets`
  - максимум рынков с живыми resting bids
- `max_resting_per_cluster`
  - максимум рынков на один neg-risk cluster
- `stake_tiers`
  - legacy price-based sizing
  - формат: `(max_entry_price, stake_usdc)`
  - используется, если `market_score_tiers` пустой
- `min_market_score`
  - нижний порог композитного `market_score`
  - `0.0` отключает фильтр
- `market_score_tiers`
  - quality-based sizing по `market_score`
  - формат: `(min_market_score_threshold, stake_usdc)`
  - имеет приоритет над `stake_tiers`
- `max_exposure_per_market`
  - потолок суммарного USDC на один `(market_id, token_id)`
  - `0.0` отключает cap
- `universe_selector_min_volume_usdc`
  - минимальный Gamma volume для pre-subscription WS selector
  - для `black_swan_mode` active default — `$100`

### Поля временного окна

- `min_hours_to_close`
  - рынок слишком близок к закрытию, если меньше этого значения
- `max_hours_to_close`
  - рынок слишком дальний, если больше этого значения
- `hours_to_close_null_default`
  - fallback, если Gamma вернул `None` для дедлайна

### Поля ranking/scoring

- `scoring_weights`
  - веса компонент итогового screener score
  - допустимые компоненты: `market_score`, `liq`, `duration`, `category`
  - пустой tuple включает fallback scoring formula
- `prefer_long_duration`
  - `True` значит предпочитать длинные рынки
  - `False` значит предпочитать короткие рынки

## Предустановленные режимы

### `fast_tp_mode`

Быстрый scanner-driven режим без moonbag.

- вход
  - `entry_price_levels=(0.005, 0.01, 0.02, 0.03, 0.05)`
  - `entry_price_max=0.05`
  - `use_resting_bids=False`
  - `scanner_entry=True`
- выход
  - `tp_levels=`
    - `progress=0.50, fraction=0.70`
    - `progress=0.80, fraction=0.30`
  - `moonbag_fraction=0.0`
- риск и размер
  - `stake_usdc=0.05`
  - `max_open_positions=30`
  - `max_resting_markets=0`
  - `max_resting_per_cluster=0`
  - `stake_tiers=()`
  - `min_market_score=0.0`
  - `market_score_tiers=()`
  - `max_exposure_per_market=0.0`
- время и ranking
  - `min_hours_to_close=1.0`
  - `max_hours_to_close=48.0`
  - `hours_to_close_null_default=48.0`
  - `scoring_weights=(('market_score', 0.40), ('duration', 0.35), ('liq', 0.15), ('category', 0.10))`
  - `prefer_long_duration=False`

### `balanced_mode`

Смешанный режим: resting bids плюс scanner, частичный TP, небольшой moonbag.

- вход
  - `entry_price_levels=(0.002, 0.005, 0.01, 0.02, 0.05)`
  - `entry_price_max=0.10`
  - `use_resting_bids=True`
  - `scanner_entry=True`
- выход
  - `tp_levels=`
    - `progress=0.25, fraction=0.35`
    - `progress=0.50, fraction=0.25`
    - `progress=0.75, fraction=0.20`
  - `moonbag_fraction=0.20`
- риск и размер
  - `stake_usdc=0.05`
  - `max_open_positions=20`
  - `max_resting_markets=1000`
  - `max_resting_per_cluster=3`
  - `stake_tiers=()`
  - `min_market_score=0.0`
  - `market_score_tiers=()`
  - `max_exposure_per_market=0.0`
- время и ranking
  - `min_hours_to_close=1.0`
  - `max_hours_to_close=120.0`
  - `hours_to_close_null_default=48.0`
  - `scoring_weights=(('market_score', 0.50), ('duration', 0.25), ('liq', 0.15), ('category', 0.10))`
  - `prefer_long_duration=False`

### `big_swan_mode`

Широкий tail-risk режим. Только resting bids, большой moonbag, score-based sizing.

Derived-константы:
- `_BIG_SWAN_BUDGET = 2.0`
- `_BIG_SWAN_LEVELS = (0.01, 0.10, 0.15)`
- `_bsm_s = _BIG_SWAN_BUDGET / len(_BIG_SWAN_LEVELS)`

- вход
  - `entry_price_levels=(0.01, 0.10, 0.15)`
  - `entry_price_max=0.20`
  - `use_resting_bids=True`
  - `scanner_entry=False`
- выход
  - `tp_levels=`
    - `progress=0.10, fraction=0.10`
    - `progress=0.50, fraction=0.20`
  - `moonbag_fraction=0.70`
- риск и размер
  - `stake_usdc=0.05`
  - `max_open_positions=500`
  - `max_resting_markets=5000`
  - `max_resting_per_cluster=10`
  - `stake_tiers=()`
  - `min_market_score=0.25`
  - `market_score_tiers=`
    - `score >= 0.60 -> _bsm_s`
    - `score >= 0.40 -> _bsm_s * 0.50`
    - `score >= 0.25 -> _bsm_s * 0.25`
  - `max_exposure_per_market=2.0`
- время и ranking
  - `min_hours_to_close=0.25`
  - `max_hours_to_close=168.0`
  - `hours_to_close_null_default=48.0`
  - `scoring_weights=(('market_score', 0.70), ('liq', 0.20), ('category', 0.10))`
  - `prefer_long_duration=True`

### `small_swan_mode`

Режим для умеренных dip/recovery-сценариев с price-based sizing.

- вход
  - `entry_price_levels=(0.05, 0.10, 0.15, 0.20)`
  - `entry_price_max=0.50`
  - `use_resting_bids=True`
  - `scanner_entry=False`
- выход
  - `tp_levels=`
    - `progress=0.20, fraction=0.30`
    - `progress=0.50, fraction=0.30`
  - `moonbag_fraction=0.40`
- риск и размер
  - `stake_usdc=0.10`
  - `max_open_positions=100`
  - `max_resting_markets=1000`
  - `max_resting_per_cluster=1`
  - `stake_tiers=`
    - `fill_price <= 0.05 -> 0.20`
    - `fill_price <= 0.10 -> 0.10`
    - `fill_price <= 0.15 -> 0.05`
    - `fill_price <= 0.20 -> 0.05`
  - `min_market_score=0.0`
  - `market_score_tiers=()`
  - `max_exposure_per_market=0.0`
- время и ranking
  - `min_hours_to_close=1.0`
  - `max_hours_to_close=48.0`
  - `hours_to_close_null_default=48.0`
  - `scoring_weights=(('market_score', 0.50), ('duration', 0.25), ('liq', 0.15), ('category', 0.10))`
  - `prefer_long_duration=False`

### `black_swan_mode`

Строгий low-zone режим для black_swan paper/live runner.

- вход
  - `entry_price_levels=(0.005, 0.01, 0.02, 0.03, 0.05)`
  - `entry_price_max=0.05`
  - `use_resting_bids=True`
  - `scanner_entry=False`
- размер
  - `stake_per_level=1.0`
  - это human-facing настройка: `$1.00` на каждый entry level
  - execution/order translator сам округляет shares и добавляет venue min-notional buffer `$0.01`, поэтому фактический venue order обычно около `$1.01`
  - отдельный ручной `5.5 USDC` cap на рынок не поддерживается как operator config; размер ladder задаётся `entry_price_levels + stake_per_level`
- universe selector
  - `universe_selector_min_volume_usdc=100.0`
  - CLI `--universe-selector-min-volume-usdc` остаётся runtime override

## MODES

`MODES` это registry режимов:
- `fast_tp_mode -> FAST_TP_MODE`
- `balanced_mode -> BALANCED_MODE`
- `big_swan_mode -> BIG_SWAN_MODE`
- `small_swan_mode -> SMALL_SWAN_MODE`
- `black_swan_mode -> BLACK_SWAN_MODE`

`BotConfig.mode_config` всегда берёт активный режим из этого словаря.

## Swan-константы и защитные проверки

### Глобальные константы

- `SWAN_BUY_PRICE_THRESHOLD = 0.20`
  - потолок цены, до которой исторически собирались `swans_v2`
- `SWAN_ENTRY_MAX = SWAN_BUY_PRICE_THRESHOLD`
  - единый buy ceiling для swan-анализа
- `SWAN_MIN_BUY_VOLUME = 1.0`
  - минимальный объём на флоре
- `SWAN_MIN_SELL_VOLUME = 30.0`
  - минимальный объём на выходе
- `SWAN_MIN_REAL_X = 5.0`
  - минимальный множитель для настоящего swan-движения

### `check_swan_buy_price_threshold(mode_config)`

Проверяет, что `entry_price_levels` активного режима не выходят выше `SWAN_BUY_PRICE_THRESHOLD`.

Если выходят, код не падает, а поднимает warning с инструкцией:
- пересобрать swan-историю с новым `--buy-price-threshold`
- затем поднять `SWAN_BUY_PRICE_THRESHOLD`

## BotConfig

`BotConfig` это runtime-конфигурация процесса.

### Поля, читаемые из env

- `mode`
  - env: `BOT_MODE`
  - default: `big_swan_mode`
- `dry_run`
  - env: `DRY_RUN`
  - default: `true`
  - значения `false`, `0`, `no` отключают dry-run
- `paper_initial_balance_usdc`
  - env: `PAPER_INITIAL_BALANCE_USDC`
  - default: `100.0`
- `private_key`
  - env: `POLY_PRIVATE_KEY`
  - default: `""`

### Поля runtime с кодовыми дефолтами

- `screener_interval = 300`
  - интервал screener, секунды
- `monitor_interval = 90`
  - интервал мониторинга позиций, секунды
- `resting_cleanup_interval = 3600`
  - интервал housekeeping по resting orders, секунды
- `min_volume_usdc = 50.0`
  - минимальный объём рынка для screener
- `max_volume_usdc = 300_000.0`
  - максимальный объём рынка для screener
- `dead_market_hours = 48.0`
  - рынок считается мёртвым, если не было трейдов столько часов
- `category_weights`
  - веса по категориям:
    - `geopolitics: 1.5`
    - `politics: 1.5`
    - `crypto: 1.0`
    - `weather: 1.0`
    - `health: 1.0`
    - `esports: 1.0`
    - `sports: 0.8`
    - `tech: 0.6`
    - `entertainment: 0.5`

## `BotConfig.mode_config`

Property `mode_config` возвращает `MODES[self.mode]`.

## `BotConfig.validate()`

Валидация делает две вещи:
- проверяет, что `mode` существует в `MODES`
- проверяет, что сумма `tp_levels` и `moonbag_fraction` не превышает `1.0`
- дополнительно запускает `check_swan_buy_price_threshold()` и поднимает warning, если режим пытается ставить выше исторического потолка

## `load_config()`

`load_config()` создаёт `BotConfig`, читает env, вызывает `validate()` и возвращает готовый runtime-config.
