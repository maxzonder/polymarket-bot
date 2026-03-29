# swan_analyzer.py

Детектор событий «чёрного лебедя» v2. Читает сырые трейды из `database/`, пишет в таблицу `swans_v2` в `polymarket_dataset.db`.

## Использование

```bash
# Полный пересчёт всей истории
python3 analyzer/swan_analyzer.py --recompute --date-from 2025-08-01 --date-to 2026-03-18

# Один день (инкрементальный запуск из daily_pipeline)
python3 analyzer/swan_analyzer.py --date 2026-03-18

# Диапазон без очистки (UPSERT по token_id + date)
python3 analyzer/swan_analyzer.py --date-from 2026-03-01 --date-to 2026-03-18
```

## Параметры

| Флаг | По умолчанию | Описание |
|------|-------------|----------|
| `--buy-price-threshold` | `SWAN_BUY_PRICE_THRESHOLD` из `config.py` (сейчас 0.20) | Порог цены дна — токен должен торговаться ниже этого значения |
| `--min-buy-volume` | 1.0 | Мин. объём сделок в зоне дна (можно войти) |
| `--min-sell-volume` | 30.0 | Мин. объём сделок на выходе >= buy_min_price * min_real_x (можно продать) |
| `--min-real-x` | 5.0 | Минимальный реальный икс для записи в таблицу |
| `--recompute` | — | Дропнуть и пересоздать `swans_v2`, затем заполнить |

---

## Алгоритм (на один токен)

1. **Загрузить трейды** из `database/{date}/{market_id}_trades/{token_id}.json`, отсортировать по timestamp.
   Каждый трейд: `{price, size, side, timestamp}`. Объём в USDC = `price × size`.

2. **Найти глобальный минимум цены** — самую низкую цену за всю историю токена среди всех трейдов.

3. **Проверить порог**: если `buy_min_price >= buy_price_threshold` (сейчас 0.20) → не лебедь, пропустить.
   Смысл: нас интересуют только токены, которые в какой-то момент торговались очень дёшево.

4. **Построить зону дна** (buy zone): от трейда с глобальным минимумом расширяем в обе стороны по времени,
   захватывая все соседние трейды пока их цена < `buy_price_threshold`.
   Это непрерывный временной отрезок, когда токен был "на полу".
   Записываем: `buy_ts_first`, `buy_ts_last`, `buy_volume` (сумма USDC в зоне), `buy_trade_count`.

5. **Проверить ликвидность покупки**: если `buy_volume < min_buy_volume` (1.0 USDC) → пропустить.
   Смысл: убедиться что на уровне дна реально были сделки, а не единичная аномалия.

6. **Проверить ликвидность продажи** (только для не-победителей):
   - берём все трейды *после* зоны дна с ценой >= `buy_min_price × min_real_x` (целевой уровень выхода)
   - суммируем объём → `sell_volume`
   - если `sell_volume < min_sell_volume` (5.0 USDC) → пропустить
   - для победителей (`is_winner=1`) этот шаг пропускается: Polymarket выплачивает $1 за каждый токен
     при резолюции через смарт-контракт, контрагент не нужен.

7. **Рассчитать max_traded_x** — фактический максимальный икс относительно цены входа:
   - победитель: `max_traded_x = 1.0 / buy_min_price` (гарантированная выплата $1)
   - не победитель: `max_traded_x = max_price_after_floor / buy_min_price` (максимум цены после зоны дна)

   Пример: купил на дне по 0.01 → max_traded_x = 100x для победителя, или цена поднялась до 0.50 → max_traded_x = 50x для не-победителя.

8. **Проверить минимальный икс**: если `max_traded_x < min_real_x` (5.0) → пропустить.
   Отсекаем незначительные отскоки.

9. **Записать UPSERT в `swans_v2`** по ключу `(token_id, date)`.
   При повторном прогоне того же диапазона обновляет `max_traded_x`, `is_winner`, `payout_x`, `sell_volume`.

### Ключевые отличия от старого `analyzer.py` (zigzag)

| | `token_swans` (старый) | `swans_v2` (новый) |
|---|---|---|
| Событий на токен | Много (каждый зигзаг) | Одно (глобальный минимум) |
| Фильтр по времени | Есть (`min_duration_minutes`) | Нет |
| Знает победителя | Нет (`possible_x` — оценочный) | Да (`is_winner` из DB) |
| Учёт выплаты $1 | Нет | Да (`max_traded_x = 1/buy_min_price` для winner) |
| Записей в таблице | ~88k (Dec–Mar) | ~4k (Aug–Mar) |

---

## Таблица `swans_v2`

| Поле | Тип | Описание |
|------|-----|----------|
| `token_id` | TEXT | ID токена |
| `market_id` | TEXT | ID рынка |
| `date` | TEXT | Дата папки коллектора (YYYY-MM-DD) |
| `buy_price_threshold` | REAL | Порог входа использованный при анализе |
| `min_buy_volume` | REAL | Мин. ликвидность входа при анализе |
| `min_sell_volume` | REAL | Мин. ликвидность выхода при анализе |
| `min_real_x` | REAL | Мин. max_traded_x при анализе |
| `buy_min_price` | REAL | Глобальный минимум цены в зоне дна |
| `buy_volume` | REAL | Объём сделок в зоне дна (USDC) |
| `buy_trade_count` | INTEGER | Количество сделок в зоне дна |
| `buy_ts_first` | INTEGER | Первый timestamp зоны дна |
| `buy_ts_last` | INTEGER | Последний timestamp зоны дна |
| `sell_volume` | REAL | Объём сделок на выходе >= buy_min_price * min_real_x (0 для победителей) |
| `max_price_in_history` | REAL | Максимальная цена за всю историю токена |
| `last_price_in_history` | REAL | Последняя цена в истории |
| `is_winner` | INTEGER | 1 если токен выиграл (outcomePrices >= 0.99) |
| `max_traded_x` | REAL | Итоговый иkс с учётом выплаты $1 |
| `payout_x` | REAL | `1/buy_min_price` для winner, иначе = `max_traded_x` |

Уникальный ключ: `(token_id, date)`. UPSERT обновляет `max_traded_x`, `is_winner`, `payout_x`, `sell_volume`.

---

## Где используется

| Компонент | Как |
|-----------|-----|
| `strategy/scorer.py` | `EntryFillScorer` и `ResolutionScorer` (fallback, основной — `token_swans`) |
| `scripts/daily_pipeline.py` | Инкрементальный запуск без `--recompute` |

---

## Глобальный порог — SWAN_BUY_PRICE_THRESHOLD

Порог `--buy-price-threshold` вынесен в `config.py` как:

```python
SWAN_BUY_PRICE_THRESHOLD = max(max(m.entry_price_levels) for m in MODES.values())
```

Автоматически равен максимальному уровню входа среди всех режимов бота. При добавлении нового режима с более высокими уровнями — порог обновится автоматически.

`check_swan_buy_price_threshold(threshold)` — возвращает список предупреждений если порог не покрывает уровни какого-либо режима. Вызывается при старте `swan_analyzer.py`.

---

## Текущее состояние

После запуска `--recompute --date-from 2025-08-01 --date-to 2026-03-18` с `threshold=0.20` (Mar 29 2026):
- **33,092 событий** в `swans_v2` (было 3,943 при threshold=0.02)
- avg max_traded_x = 14.44x, max = 1000x
- winners = 24,503 (74%)
- avg buy_volume = $495.46
