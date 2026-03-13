*Входит в состав документации [ROADMAP.md](./ROADMAP.md).*

# Best Practices & Референсы (Готовые решения)

Для ускорения разработки и повышения стабильности кода мы не "изобретаем велосипеды", а используем проверенные решения и официальные библиотеки. В этом файле собираются архитектурные референсы.

## 1. Официальный AI-фреймворк (Polymarket/agents)
**Репозиторий:** `https://github.com/Polymarket/agents`
Официальный фреймворк от разработчиков Polymarket для создания AI-агентов.
- **Что берем:**
  - **Pydantic-модели (`Objects.py`):** Готовые схемы валидации данных для ответов от API Polymarket. Позволяет избежать ошибок типизации и `KeyError`.
  - **`GammaMarketClient` и `Polymarket` классы** — хорошая база для `gamma_client.py` и `clob_client.py`.
- **Что НЕ берём сейчас:** ChromaDB, Langchain, RAG — перенесены в [POSTPONED_IDEAS.md](./POSTPONED_IDEAS.md).
- **Важное наблюдение:** В официальном репо `maintain_positions()` — пустой stub (`pass`). Мониторинг открытых позиций официально не реализован. Мы реализуем его сами через REST polling (см. п. 7 ниже).

## 2. Официальный CLOB SDK (py-clob-client)
**Репозиторий:** `https://github.com/Polymarket/py-clob-client`
Официальный Python-клиент для биржевого стакана.
- **Что берем:** Абсолютно всё, что касается исполнения ордеров (Execution).
- **Почему:** Он *автоматически* скачивает актуальные Taker Fee, подставляет их в подписанный ордер (`feeRateBps`), считает хеши (EIP-712) и подписывает транзакции приватным ключом.
- **Gas:** Polymarket использует gasless relayer на Polygon — пользователь газ не платит. py-clob-client работает с этим прозрачно.
- **Fee-free рынки:** Большинство рынков (Киберспорт, Политика) имеют `feesEnabled: false`. Крипто-рынки с марта 2026 имеют комиссию, но она ~0% при покупке на экстремально низких ценах (< $0.03). Подробности: [TRADING_LOGIC.md](./TRADING_LOGIC.md) раздел 0.

## 3. Логирование и Алерты (Python Logging)
- **Решение:** Встроенный модуль `logging` + `logging.handlers.RotatingFileHandler`.
- **Практика:** Обертка написана в `utils/logger.py`. Использование жёсткой ротации логов (5 МБ, 3 бэкапа) и обязательная передача `__name__` в каждый лог для идентификации модуля (см. [LOGGING_AND_ALERTS.md](./LOGGING_AND_ALERTS.md)).

### Логирование для Data Collector

Коллектор пишет итоговый отчёт за каждый обработанный день. Минимум в логах:

```python
import logging
from logging.handlers import RotatingFileHandler

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler("logs/collector.log", maxBytes=5_000_000, backupCount=3)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(handler)
    return logger
```

**Что логировать в сборщике (на уровне дня):**
```
[INFO] collector.markets: 2026-02-15 — запрошено 340 рынков, скачано 340, пропущено (уже есть) 0
[INFO] collector.prices:  2026-02-15 — запрошено 680 токенов, скачано 678, ошибок 2
[WARNING] collector.prices: token_id=abc123 — HTTP 429, retried 3x, skipped
[INFO] collector.summary: 2026-02-15 — DONE. Markets: 340, Price files: 678, Errors: 2, Time: 4m 12s
```

**Что логировать в парсере (на уровне дня):**
```
[INFO] parser: 2026-02-15 — начало парсинга, рынков в очереди: 340
[INFO] parser: 2026-02-15 — записано в markets: 340, в raw_price_history: 127,440 точек
[WARNING] parser: market_id=xyz — поле clobTokenIds пустое, пропущен
[INFO] parser: 2026-02-15 — DONE. Time: 1m 03s
```

**Уровни логирования:**
- `DEBUG` — детали каждого запроса (только при отладке, по флагу)
- `INFO` — прогресс и итоги за день
- `WARNING` — пропущенные записи, временные ошибки, retry
- `ERROR` — критические сбои (не смогли подключиться к БД и т.п.)

## 4. Отслеживание состояния сборщика (collector_state.db)

Отдельная SQLite-база `collector_state.db` — операционный журнал сборщика. **Не смешивать с `polymarket_raw.db`** (аналитические данные).

**Схема:**
```sql
CREATE TABLE IF NOT EXISTS collection_state (
    date                    TEXT NOT NULL,  -- yyyy-mm-dd (end_date рынка)
    market_id               TEXT NOT NULL,  -- condition_id из Gamma API
    downloaded_at           TEXT,           -- ISO timestamp или NULL
    prices_downloaded_at    TEXT,           -- ISO timestamp или NULL
    parsed_at               TEXT,           -- NULL = ещё не парсили
    error                   TEXT,           -- последняя ошибка (если была)
    PRIMARY KEY (date, market_id)
);
```

**Логика коллектора:**
- Перед скачиванием рынка: `SELECT downloaded_at WHERE date=? AND market_id=?` — если не NULL, пропустить
- После скачивания: `INSERT OR REPLACE ... SET downloaded_at = datetime('now')`
- Парсер берёт только: `WHERE downloaded_at IS NOT NULL AND parsed_at IS NULL`
- После парсинга: `UPDATE SET parsed_at = datetime('now')`

**Команда "докачать месяц"** автоматически пропустит уже скачанное и добавит только новое.

## 5. Асинхронность и Очереди (asyncio)
- **Решение:** Встроенный пакет `asyncio`.
- **Практика:** Для связи модулей (Скринер -> Трейдер) строго используется `asyncio.Queue`. Это гарантирует, что медленные операции (например, HTTP-запросы Трейдера) не будут блокировать цикл Скринера.

## 5. Базы Данных (SQLite)
- **Решение:** Встроенный модуль `sqlite3` + паттерн Data Lake / Data Mart.
- **Практика:** Для локального бэктеста и хранения состояний (Watchlist, открытые ордера, история профита) используется SQLite. База легко переносится одним файлом и не требует поднятия Docker-контейнеров с PostgreSQL на ранних этапах.

## 6. Что мы ИГНОРИРУЕМ из `Polymarket/agents` (И почему)
В официальном репозитории есть модули коннекторов к новостям (`news.py` / NewsAPI) и поисковикам (`search.py` / Tavily). 
**Мы их не используем.**
- **Причина:** Их архитектура рассчитана на "Фундаментальный анализ" (агент гуглит новости, чтобы предсказать исход и торговать вероятностями). 
- **Наша стратегия:** Мы торгуем математику и неэффективность ценообразования. Парсинг новостей добавляет сложность и сжигает бюджет на API-ключи. Наш Скринер реагирует только на цифры из REST API.

## 7. Мониторинг открытых позиций (REST polling)
**Решение: REST polling через `py-clob-client`, раз в 1-2 минуты.**

**Почему не WebSocket:**
- WebSocket требует логику реконнекта, heartbeat, обработку out-of-order сообщений.
- При разрыве соединения всё равно нужен fallback на REST → два механизма вместо одного.
- Итого: WebSocket сложнее, не проще.

**Почему REST достаточно:**
- Наши позиции живут часами. Узнать об исполнении TP через 60 секунд вместо мгновенно — некритично.
- `py-clob-client` уже реализует `get_orders()` из коробки — один вызов, никакого состояния.

**Практика:**
```python
# Position Monitor — раз в 1-2 минуты
open_orders = client.get_orders()
for order in open_orders:
    if order["status"] == "MATCHED":
        record_profit(order)
    elif market_is_resolved(order["market"]):
        record_loss_or_win(order)
```

WebSocket остаётся опциональным улучшением на будущее — только после того как MVP стабильно работает на REST.
