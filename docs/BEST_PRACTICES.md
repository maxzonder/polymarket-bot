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

**Collector log examples (day level):**
```
[INFO] collector: 2026-02-15: start
[INFO] collector: 2026-02-15: DONE | total: 340 | new: 340 | skipped: 0 | prices ok: 338 | price errors: 2
[WARNING] collector: 2026-02-15 | abc123 | token=xyz: price fetch error — HTTP 429
[WARNING] collector: 2026-02-15 | abc123: clobTokenIds empty, skipping prices
```

**Parser log examples (day level):**
```
[INFO] parser: 2026-02-15: start, markets in queue: 340
[INFO] parser: 2026-02-15: DONE | markets: 340 | price points: 127440 | time: 1m 03s
[WARNING] parser: 2026-02-15 | market_id=xyz: clobTokenIds empty, skipped
```

**Log levels:**
- `DEBUG` — per-request details (enabled via flag for troubleshooting only)
- `INFO` — day-level start/done summaries
- `WARNING` — skipped records, transient errors, retries
- `ERROR` — critical failures (DB connection lost, etc.)

**Правило имени лога:**
- Для всех ручных и фоновых запусков коллектора использовать один и тот же файл: `logs/collector.log`.
- Не плодить отдельные файлы вида `collector_*.log` под даты, диапазоны или разовые прогоны.
- Если нужен контекст конкретного запуска, он должен различаться именем `tmux`-сессии или сообщением в самом логе, а не именем лог-файла.

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

## 7. Рабочий процесс разработки (Development Workflow)

### Структура окружения
- **Локалка** `/home/agent/projects/polymarket-bot` — разработка, коммиты, пуш
- **GitHub** `git@github.com:maxzonder/polymarket-bot.git` — source of truth для кода
- **Сервер** `/home/polybot/polymarket-bot` — только runtime, данные в `/home/polybot/.polybot`

### Разделение кода и данных
- **Код** — в git-репозитории
- **Данные** (database, sqlite, logs) — **вне репо**, в `POLYMARKET_DATA_DIR=/home/polybot/.polybot`
- Переменная читается из `.env` через `python-dotenv` (см. `utils/paths.py`)

### Стандартный деплой кода на сервер
```bash
# 1. Коммит локально
git add -A && git commit -m "..."

# 2. Пуш в GitHub
GIT_SSH_COMMAND='ssh -i /home/agent/.openclaw/lebedevaemk_github_priv.key -o StrictHostKeyChecking=no' \
  git push origin main

# 3. Пулл на сервере
ssh polybot@135.181.134.96 'cd ~/polymarket-bot && \
  GIT_SSH_COMMAND="ssh -i /home/polybot/lebedevaemk_github_priv.key -o StrictHostKeyChecking=no" \
  git pull --ff-only'
```

### Синхронизация ТОЛЬКО кода (без затирания данных)
- **Никогда не использовать `rsync --delete` в папке с данными.**
- Для деплоя кода — только `git pull` на сервере.
- Если нужна ручная синхронизация файлов — `scp` или `rsync` **без** `--delete`.

### Запуск пайплайна на сервере (через tmux)

**Важно:** всегда используй `bash -l -c '...'` чтобы загрузить `.bash_profile` и получить `POLYMARKET_DATA_DIR`.

```bash
# Ручной запуск инgest за диапазон (первоначальная загрузка)
tmux new-session -d -s ingest \
  "bash -l -c 'python3 data_collector/data_collector_and_parsing.py --start 2025-08-01 --end 2026-03-28'"

# Swan analyzer (после ingest)
tmux new-session -d -s analyzer \
  "bash -l -c 'python3 analyzer/swan_analyzer.py --recompute --date-from 2025-08-01 --date-to 2026-03-28'"

# Feature mart (после analyzer)
tmux new-session -d -s feature_mart \
  "bash -l -c 'python3 analyzer/market_level_features_v1_1.py --recompute'"

# Ежедневный пайплайн (автоматизирует всё выше + ml + recalibrate)
tmux new-session -d -s daily_pipeline \
  "bash -l -c 'python3 scripts/daily_pipeline.py'"
```

### Правило логов
- Все collector-запуски пишут в `logs/collector.log` — один файл, без суффиксов дат.
- Не создавать `collector_2026-02-01.log`, `collector_feb.log` и т.д.

## 8. Мониторинг открытых позиций (REST polling)
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
