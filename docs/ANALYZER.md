# Analyzer v3 — Real Swan Detector

*Связано с: [DATA_COLLECTION.md](./DATA_COLLECTION.md), [DATABASE_SCHEMA.md](./DATABASE_SCHEMA.md), [ROADMAP.md](./ROADMAP.md)*

## Зачем нужен Analyzer v3

`analyzer` хорошо находит **геометрические лебеди**:
- локальный минимум,
- затем значимый рост,
- `possible_x >= 5`.

Но этого недостаточно для торговли. Геометрический лебедь не обязательно означает, что:
- внизу была реальная ликвидность для входа,
- на целевых иксах была реальная ликвидность для выхода,
- цена держалась достаточно долго, чтобы это был не мгновенный прострел.

`analyzer` решает именно эту задачу:
он ищет **реальные торговые лебеди**, а не просто красивые формы на графике.

---

## Что считается "реальным лебедем"

Событие попадает в `token_swans`, только если одновременно выполняются все условия:

1. **Базовый лебедь найден zigzag-алгоритмом**
   - локальный минимум → локальный максимум;
   - `possible_x >= min_recovery`.

2. **Есть реальная ликвидность на входе**
   - `entry_volume_usdc >= min_entry_liquidity`

3. **Есть реальная ликвидность на выходе под целевой тейк**
   - задаётся `target_exit_x`
   - считается `target_exit_price = entry_min_price * target_exit_x`
   - выходная ликвидность берётся только по сделкам на `target_exit_price` и выше
   - нужно, чтобы:
     - `exit_volume_usdc >= min_entry_liquidity * target_exit_x`

4. **Лебедь жил достаточно долго**
   - `duration_entry_to_target_minutes >= min_duration_minutes`

Идея простая:
если минимальный рабочий вход считаем `$10`, а хотим выйти на `5x`,
то на выходе должно быть хотя бы `$50` реального объёма на целевой цене и выше.

---

## Входные параметры

### `--min-entry-liquidity`
Минимальная ликвидность на входе в USDC.

Пример:
- `10`

Смысл:
- если в зоне входа наторговалось меньше `$10`, событие считаем мусором.

### `--min-recovery`
Минимальная кратность роста, чтобы raw zigzag-событие вообще считалось лебедем.

Пример:
- `5`

Смысл:
- всё, что меньше `5x`, считаем шумом.

### `--target-exit-x`
Целевой тейк, под который проверяется выходная ликвидность.

Пример:
- `5`

Смысл:
- анализатор не проверяет "была ли ликвидность где-то наверху вообще";
- он проверяет: можно ли было **выйти на конкретном target-x**.

### `--min-duration-minutes`
Минимальное время от начала входной зоны до первого достижения target-x.

Пример:
- `10`

Смысл:
- отсечь мгновенные прострелы, где цена формально дала `5x`, но у стратегии не было реалистичного окна на вход/выход.

---

## Главные отличия от Analyzer v2

### 1. Узкая входная зона вместо широкого исторического окна

В `v2` входная ликвидность считалась слишком широко: туда могли попадать старые дешёвые сделки до конкретного лебедя.

В `v3` входная зона строится локально вокруг минимума:
- находим `entry_min_price`
- строим порог:
  - `entry_threshold_price = entry_min_price * min_recovery`
- идём **назад от минимума**, пока цена не выше порога
- идём **вперёд от минимума**, пока цена не выше порога

Это даёт **contiguous low-zone** вокруг дна.

Плюсы:
- не подтягиваются старые нерелевантные сделки;
- `entry_volume_usdc` становится ближе к реальной ликвидности возле дна;
- меньше завышения торгуемости.

### 2. Выход считается по target-x, а не по "верхней половине пика"

В `v2` выходная ликвидность считалась по условию вроде:
- `price >= exit_max_price * 0.5`

Это полезный прокси, но он не отвечает на реальный вопрос:
- можно ли было продать на нужном тейке?

В `v3` делается иначе:
- считаем `target_exit_price = entry_min_price * target_exit_x`
- ищем **первый момент**, когда цена реально достигла target
- берём только сделки:
  - от первого достижения target,
  - до локального максимума этого лебедя,
  - и только с `price >= target_exit_price`

Так `exit_volume_usdc` становится привязанным к конкретному торговому сценарию.

### 3. Встроенный фильтр по длительности

В `v3` событие отбрасывается, если target был достигнут слишком быстро:
- `duration_entry_to_target_minutes < min_duration_minutes`

Это помогает отсеивать:
- ультракороткие вспышки;
- ситуации, где цена формально дошла до target, но окно для исполнения было нереалистично узким.

---

## Алгоритм работы

### Шаг 1. Найти raw swans zigzag-алгоритмом
Для каждого токена:
- trades сортируются по `timestamp`
- ищутся пары:
  - локальный минимум
  - следующий локальный максимум
- если `exit_max_price / entry_min_price >= min_recovery`, это raw swan

### Шаг 2. Построить локальную входную зону
Для каждого raw swan:
- определяется `entry_min_price`
- считается порог входной зоны:
  - `entry_threshold_price = entry_min_price * min_recovery`
- вокруг минимума строится непрерывный диапазон сделок, где цена не выше этого порога

Из него считаются:
- `entry_volume_usdc`
- `entry_trade_count`
- `entry_ts_first`
- `entry_ts_last`
- `duration_entry_zone_seconds`

### Шаг 3. Проверить достижение target-x
- `target_exit_price = entry_min_price * target_exit_x`
- ищется первый trade, где `price >= target_exit_price`
- если такого нет — raw swan отбрасывается

### Шаг 4. Построить выходную зону
Берутся сделки:
- от первого достижения `target_exit_price`
- до локального максимума этого лебедя
- только с `price >= target_exit_price`

Из них считаются:
- `exit_volume_usdc`
- `exit_trade_count`
- `exit_ts_first`
- `exit_ts_last`
- `duration_exit_zone_seconds`
- `duration_entry_to_target_minutes`
- `duration_entry_to_peak_minutes`

### Шаг 5. Применить фильтры реалистичности
Лебедь сохраняется только если:
- `entry_volume_usdc >= min_entry_liquidity`
- `exit_volume_usdc >= min_entry_liquidity * target_exit_x`
- `duration_entry_to_target_minutes >= min_duration_minutes`

---

## Что хранится в `token_swans`

### Идентификация
- `token_id`
- `market_id`
- `date`

### Параметры анализа
- `min_recovery`
- `target_exit_x`
- `min_entry_liquidity_required`
- `min_exit_liquidity_required`
- `min_duration_minutes_required`

### Вход
- `entry_min_price`
- `entry_threshold_price`
- `entry_volume_usdc`
- `entry_trade_count`
- `entry_ts_first`
- `entry_ts_last`

### Выход
- `target_exit_price`
- `exit_max_price`
- `exit_volume_usdc`
- `exit_trade_count`
- `exit_ts_first`
- `exit_ts_last`

### Время
- `duration_entry_zone_seconds`
- `duration_exit_zone_seconds`
- `duration_entry_to_target_minutes`
- `duration_entry_to_peak_minutes`

### Итог
- `possible_x`

---

## Пример запуска

### Один день
```bash
cd ~/polymarket-bot
.venv/bin/python -m data_collector.analyzer \
  --date 2026-02-28 \
  --recompute \
  --min-entry-liquidity 10 \
  --min-recovery 5 \
  --target-exit-x 5 \
  --min-duration-minutes 10
```

### Один рынок
```bash
cd ~/polymarket-bot
.venv/bin/python -m data_collector.analyzer \
  --market-id 1403334 \
  --recompute \
  --min-entry-liquidity 10 \
  --min-recovery 5 \
  --target-exit-x 5 \
  --min-duration-minutes 10
```

### Вся база
```bash
cd ~/polymarket-bot
.venv/bin/python -m data_collector.analyzer \
  --recompute \
  --min-entry-liquidity 10 \
  --min-recovery 5 \
  --target-exit-x 5 \
  --min-duration-minutes 10
```

---

## Что считать текущими рабочими настройками

Базовый реалистичный режим:
- `min_entry_liquidity = 10`
- `min_recovery = 5`
- `target_exit_x = 5`
- `min_duration_minutes = 10`

Интерпретация:
- лебедь должен давать хотя бы `5x`
- внизу должно быть хотя бы `$10` реального объёма
- на цене `5x` и выше должно быть хотя бы `$50` реального объёма
- от входной зоны до target должно пройти хотя бы `10 минут`

---

## Ограничения модели

`analyzer` всё ещё не полноценный симулятор стакана.

Он работает по:
- **реально исполненным сделкам** (`trades`)

Но он не знает:
- полный order book в каждый момент времени;
- какое место занимал бы наш лимитный ордер в очереди;
- можно ли было забрать лучший принт полностью, частично или вообще нет.

Поэтому `v3` — это:
- **trade-based realism filter**
- а не идеальный microstructure simulator.

Тем не менее он заметно реалистичнее `v2`, потому что:
- не раздувает входную зону историческим мусором;
- считает выход по конкретному target-x;
- требует реального окна времени;
- применяет жёсткие пороги ликвидности.

---

## Практический смысл

`legacy analyzer` отвечал на вопрос:
- **где вообще были лебеди?**

`analyzer` отвечает на вопрос:
- **где были лебеди, в которые реально можно было войти и из которых реально можно было выйти на заданном тейке?**

Это уже слой, который ближе не к исследованию формы рынка, а к построению рабочей торговой стратегии.
