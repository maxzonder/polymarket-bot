# Big Swan Mode — архитектурное ТЗ для Claude Opus

Ты — сильный **staff/principal-level quant + product architect + python engineer**.

Нужен не поверхностный обзор, а **жёсткая архитектурная критика** и проектирование рабочей системы.

---

## Контекст

Я строю бота для **Polymarket**, который должен искать и торговать не просто мелкие отскоки, а большие **«чёрные лебеди»** — редкие сделки с правым хвостом **50x–1000x**.

Меня интересует не "как сделать аккуратный low-price scalper", а **как спроектировать бота, который действительно умеет искать большие хвосты**.

---

## Что уже известно и что нужно считать фактами

1. **Нельзя опираться на агрегированные price/candle endpoints** вроде `prices-history`.
   Они сглаживают внутрисвечные паники и скрывают реальные spikes.
   **Источник истины — только raw trades.**

2. На бинарных рынках YES/NO ключевые события часто вызывают **зеркальную реакцию** в двух токенах.

3. Для выигравшего токена реальная выплата на resolution = **$1.00 за токен**.
   Поэтому нельзя оценивать только market spike; нужно учитывать:
   - `resolution_x = 1.0 / entry_price`

4. Обычная логика **«купили дёшево — продали всё на 5x»** убивает правый хвост распределения.
   Для настоящих big swans важны редкие **hold-to-resolution** победители.

5. Polling уже упавшего рынка — не единственный способ входа.
   Самые жирные хвосты могут лучше ловиться **заранее расставленными resting limit orders**.

6. Стратегию нельзя оптимизировать по **win rate**.
   Нужно оптимизировать по **expected value** и по вкладу **правого хвоста**.

7. Тонкий рынок Polymarket плохо сочетается с красивыми теоретическими `trailing stop` идеями.
   На практике synthetic stop может дать **ложное чувство защиты**.

8. Есть смысл разделять:
   - вероятность того, что рынок вообще ударится в нашу зону входа
   - вероятность того, что после входа имеет смысл держать до resolution

9. Цель: обучить бота искать именно **большие лебеди**, а не просто частые **3x–5x bounce-сделки**.

---

## Дополнительный контекст: база для обучения уже существует

Важно: не проектируй систему так, будто датасета ещё нет.
У нас уже есть рабочая **historical training base**, собранная по Polymarket.

### Что уже есть физически

#### 1. Основная SQLite база
- `polymarket_dataset.db`

#### 2. Raw trades хранятся не в SQLite, а в файловой структуре
- `$POLYMARKET_DATA_DIR/database/YYYY-MM-DD/{market_id}.json`
- `$POLYMARKET_DATA_DIR/database/YYYY-MM-DD/{market_id}_trades/{token_id}.json`

#### 3. Формат raw trades файла
Каждый файл — это JSON-массив трейдов одного токена.
Формат элемента:

```json
{"side":"BUY","size":72,"price":0.999,"timestamp":1760468941}
```

### Актуальный размер базы
- `markets`: **107054** строк
- `tokens`: **214108** строк
- `token_swans`: **8415** строк
- `swans_v2`: **1263** строк

---

## Текущие таблицы и поля

### 1. `markets`
- `id`
- `question`
- `description`
- `category`
- `resolution_source`
- `start_date`
- `end_date`
- `closed_time`
- `duration_hours`
- `volume`
- `liquidity`
- `comment_count`
- `fees_enabled`
- `neg_risk`
- `group_item_title`
- `cyom`

### 2. `tokens`
- `token_id`
- `market_id`
- `outcome_name`
- `is_winner`

### 3. `token_swans`
Это результат `zigzag/analyzer`-подхода.

Поля:
- `id`
- `token_id`
- `market_id`
- `date`
- `min_recovery`
- `target_exit_x`
- `min_entry_liquidity_required`
- `min_exit_liquidity_required`
- `min_duration_minutes_required`
- `entry_min_price`
- `entry_threshold_price`
- `entry_volume_usdc`
- `entry_trade_count`
- `entry_ts_first`
- `entry_ts_last`
- `target_exit_price`
- `exit_max_price`
- `exit_volume_usdc`
- `exit_trade_count`
- `exit_ts_first`
- `exit_ts_last`
- `duration_entry_zone_seconds`
- `duration_exit_zone_seconds`
- `duration_entry_to_target_minutes`
- `duration_entry_to_peak_minutes`
- `possible_x`

### 4. `swans_v2`
Это более новый `real-x / resolution-aware` слой.

Поля:
- `id`
- `token_id`
- `market_id`
- `date`
- `entry_threshold`
- `min_entry_liquidity`
- `min_exit_liquidity`
- `target_exit_x`
- `min_real_x`
- `entry_min_price`
- `entry_volume_usdc`
- `entry_trade_count`
- `entry_ts_first`
- `entry_ts_last`
- `target_exit_price`
- `exit_volume_usdc`
- `max_price_in_history`
- `last_price_in_history`
- `is_winner`
- `real_x`
- `resolution_x`

---

## Критически важно

- **raw trades уже есть** и являются source of truth
- в SQLite уже есть готовые аналитические слои: `token_swans`, `swans_v2`
- **не предлагай архитектуру, которая игнорирует существующую БД и начинает всё с нуля**
- проектируй решение как **эволюцию уже имеющегося пайплайна**

---

## Твоя задача

Сделай глубокий дизайн **big-swan trading architecture** для Polymarket-бота.

Мне нужен ответ **НЕ** в стиле "можно попробовать X/Y/Z", а в стиле:
- где текущая логика концептуально слаба
- как должна выглядеть правильная архитектура
- какие сущности, фичи, лейблы, метрики и режимы нужны
- что реально можно внедрить поэтапно

---

## Что нужно разобрать

### A. Жёсткая критика текущей идеи
Покритикуй наивную архитектуру:
- скринер ищет уже дешёвые токены
- бот покупает по low price
- бот продаёт всё на `5x / 10x`

Объясни, почему такая схема может быть нормальной для **bounce trading**, но плохой для поиска **50x–1000x**.

### B. Разделение на два независимых prediction problems
Предложи архитектуру, где есть минимум два score:

1. **Entry Fill Score**
   - вероятность, что рынок вообще дойдёт до наших resting bid уровней

2. **Resolution / Tail Score**
   - вероятность, что после входа этот рынок даст большой правый хвост:
     - `20x+`
     - `50x+`
     - `100x+`
     - либо `hold-to-resolution winner`

Опиши, почему объединять это в один score — ошибка.

### C. Big Swan Mode
Спроектируй отдельный режим бота:
- `fast_tp_mode`
- `balanced_mode`
- `big_swan_mode`

Опиши:
- в чём разница их целей
- в чём разница `execution policy`
- в чём разница `exit logic`
- когда какой режим использовать

### D. Execution logic
Предложи execution architecture для big swans:
- когда использовать **resting limit orders**
- на каких уровнях имеет смысл ставить bids
- как не плодить тысячи мусорных ордеров
- как обновлять / снимать stale orders
- как сочетать `pre-positioned bids` и `scanner-based entries`
- как защититься от illiquid garbage markets

### E. Exit logic
Предложи правильную схему выхода для big swans.

Мне нужна не банальная фраза **"используй partial take profit"**, а чёткая логика:
- сколько позиции продавать на `5x–10x`
- сколько на `20x–50x`
- сколько держать до `resolution`
- при каких условиях полный выход оправдан
- в каких случаях `moonbag` обязателен
- почему `trailing stop` в Polymarket может быть плохой идеей

### F. Features / labels / dataset design
Опиши, какие фичи собирать для обучения `big_swan_mode`.
Раздели фичи на:

1. `market metadata features`
2. `raw-trade-derived features`
3. `orderbook/liquidity features`
4. `temporal features`
5. `category/pattern features`

Отдельно предложи labels:
- `tp_5x_hit`
- `tp_10x_hit`
- `tp_20x_hit`
- `is_winner`
- `real_x`
- `resolution_x`
- `tail_bucket`
- `floor_duration_seconds`
- `entry_volume_usdc`
- `time_to_resolution_hours`

И предложи, какие labels лучше для:
- classification
- regression
- ranking

### G. Objective functions / metrics
Сформулируй, по каким метрикам реально оптимизировать big-swan bot.
Не ограничивайся `win rate` и `average PnL`.

Мне нужны метрики уровня:
- `EV_entry_to_tp`
- `EV_entry_to_resolution`
- `tail_ev`
- `P(real_x >= 20x)`
- `P(real_x >= 50x)`
- `P(real_x >= 100x)`
- `capital_lock_days`
- `fill_efficiency`
- `false_fill_rate`
- `opportunity_cost`

Объясни, какие из них:
- primary
- secondary
- guardrail

### H. Learning architecture
Предложи, как **practically** обучать такую систему:
- `rule-based baseline`
- `scorecard / weighted heuristic`
- `gradient boosting / ranking model`
- `bandit / exploration layer`
- `feedback loop from realized PnL`

Мне важен **реалистичный roadmap**, а не "сразу обучим end-to-end AI".

### I. Cohorts and pattern mining
Предложи, как искать recurring positive cohorts:
- Elon tweet markets
- event-driven crypto triggers
- niche sports matchups
- thin regional politics
- other repeatable patterns

Опиши, как обнаруживать когорты **автоматически**, а не только руками.

### J. Anti-overfitting / anti-self-deception
Опиши, как не обмануть себя:
- survivorship bias
- lookahead bias
- leakage from resolution
- overfitting to a few huge winners
- confusing tradable spikes with paper spikes
- stale liquidity assumptions
- using market metadata that was unavailable at entry time

Мне нужен отдельный раздел:
**"где именно этот проект вероятнее всего сам себя обманет"**.

### K. Existing DB → training pipeline
С учётом уже существующей базы оцени:
- насколько текущий формат БД уже пригоден для обучения `big_swan_mode`
- каких полей / таблиц / derived features не хватает
- как превратить существующие `markets + tokens + raw trades + token_swans + swans_v2` в нормальный training dataset
- нужна ли отдельная **denormalized training table / feature mart**
- какие признаки вычислять offline из raw trades, а какие хранить прямо в SQLite

---

## Требования к формату ответа

В ответе дай:

1. `Executive summary`
2. `Critique of naive design`
3. `Target architecture`
4. `Modes: fast_tp / balanced / big_swan`
5. `Feature schema`
6. `Labels and objectives`
7. `Execution policy`
8. `Exit policy`
9. `Evaluation framework`
10. `Step-by-step implementation roadmap`
11. `Top 10 failure modes`
12. `Final recommendation: what to build first`

---

## Критические требования к содержанию

- Не пиши воду
- Не уходи в абстрактный ML-академизм
- Пиши как архитектор системы, которую реально будут кодить
- Если видишь внутреннее противоречие в стратегии — прямо скажи
- Если считаешь, что часть идей нужно выбросить — выброси
- Если считаешь, что big swan bot должен быть гибридом
  `resting bids + scorer + cohort miner`, так и скажи
- Предлагай решения в терминах реальной инженерии, а не красивых лозунгов

---

## Тон ответа

Жёсткий, умный, прагматичный.

Без лести.
Без воды.
Без ухода в `it depends`, если можно занять сильную позицию.
