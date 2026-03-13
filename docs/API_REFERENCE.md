*Входит в состав документации [ROADMAP.md](./ROADMAP.md).*

# Polymarket API Reference (рабочая выжимка)

Полная документация: https://docs.polymarket.com/api-reference/introduction

---

## Три API

| API | Base URL | Назначение |
|-----|----------|-----------|
| **Gamma API** | `https://gamma-api.polymarket.com` | Рынки, события, теги, поиск. Основной источник данных. Публичный, без auth. |
| **CLOB API** | `https://clob.polymarket.com` | Стакан, цены, история цен, ордера. Публичные + authenticated эндпоинты. |
| **Data API** | `https://data-api.polymarket.com` | Позиции пользователей, трейды, лидерборд. Публичный. |

---

## 1. Gamma API — Список рынков

### `GET /markets`
**Используется:** Data Collector (скачать закрытые рынки), Screener (найти активные рынки под критерии).

**Ключевые параметры:**
- `closed=true` — только закрытые рынки (для бэктеста)
- `closed=false` или без параметра — активные (для live-бота)
- `end_date_min` / `end_date_max` — фильтр по дате закрытия (ISO 8601)
- `limit` — макс. кол-во записей в ответе
- `offset` — для пагинации
- `order` — поле для сортировки (например, `end_date`)
- `ascending=true/false`
- `liquidity_num_min` / `liquidity_num_max` — фильтр по ликвидности (USDC)
- `volume_num_min` / `volume_num_max` — фильтр по объёму

**Пример запроса (закрытые рынки за февраль 2026):**
```
GET https://gamma-api.polymarket.com/markets?closed=true&end_date_min=2026-02-01T00:00:00Z&end_date_max=2026-03-01T00:00:00Z&limit=100&offset=0
```

**Ключевые поля ответа (`Market` объект):**
- `id` — ID рынка (строка)
- `question` — название рынка
- `description` — полное описание и правила разрешения
- `category` — категория (sports, crypto, politics и т.д.)
- `endDate` — дата закрытия (ISO 8601)
- `closedTime` — фактическое время закрытия
- `startDate` — дата открытия (внутри `events[]`)
- `clobTokenIds` — JSON-массив ID токенов (YES/NO) — нужны для `/prices-history`
- `outcomePrices` — JSON-массив финальных цен (`["1", "0"]` или `["0", "1"]`)
- `outcomes` — JSON-массив названий исходов (`["Yes", "No"]`)
- `volume` / `volumeNum` — объём торгов (USDC)
- `liquidity` / `liquidityNum` — текущая ликвидность
- `bestAsk` / `bestBid` — лучшие цены стакана (для live Screener)
- `lastTradePrice` — цена последней сделки
- `feesEnabled` — есть ли комиссия на этом рынке
- `active` / `closed` / `archived` — статусы рынка
- `events[].commentCount` — количество комментариев (из родительского Event)

> ⚠️ `commentCount` живёт в родительском объекте `Event`, а не в `Market`. Брать из `market.events[0].commentCount`.

---

## 2. CLOB API — История цен

### `GET /prices-history`
**Используется:** Data Collector (скачать историю цен токенов для анализа).

**Параметры:**
- `market` (required) — ID токена (`asset_id` / `clob_token_id`)
- `startTs` — начальный timestamp (unix, опционально)
- `endTs` — конечный timestamp (unix, опционально)
- `interval` — интервал: `max`, `all`, `1m`, `1w`, `1d`, `6h`, `1h`
- `fidelity` — точность в минутах (default: 1 минута). Для почасовых данных — `60`.

**Пример (почасовые данные токена за всё время):**
```
GET https://clob.polymarket.com/prices-history?market=<token_id>&interval=max&fidelity=60
```

**Ответ:**
```json
{
  "history": [
    {"t": 1706745600, "p": 0.03},
    {"t": 1706749200, "p": 0.07},
    ...
  ]
}
```
- `t` — unix timestamp
- `p` — цена (0.00 – 1.00)

> ⚠️ `market` параметр принимает `token_id` (из `clobTokenIds`), не `market_id`. Для бинарного рынка у нас два токена (YES и NO) — скачиваем оба.

---

## 3. CLOB API — Стакан ордеров

### `GET /book`
**Используется:** Order Manager (проверка глубины стакана перед покупкой).

**Параметры:**
- `token_id` (required) — ID токена

**Пример:**
```
GET https://clob.polymarket.com/book?token_id=<token_id>
```

**Ключевые поля ответа:**
- `bids` — массив `{price, size}` (покупатели)
- `asks` — массив `{price, size}` (продавцы)
- `best_bid` / `best_ask` — лучшие цены
- `spread` — разница best_ask - best_bid

---

## 4. CLOB API — Создание ордера

### `POST /order`
**Используется:** Order Manager (покупка + выставление TP). Требует authentication.

Создание и подписание ордера — через `py-clob-client` (не вручную). Клиент сам считает EIP-712 подпись.

**Типы ордеров:**
- `GTC` — Good Till Cancelled (наш основной тип: лимитный BUY и TP SELL)
- `FOK` — Fill Or Kill
- `GTD` — Good Till Date

**Через py-clob-client:**
```python
from py_clob_client.clob_types import OrderArgs
from py_order_utils.builders import OrderBuilder

order = client.create_and_post_order(OrderArgs(
    price=0.02,         # цена входа
    size=50,            # размер позиции в USDC
    side="BUY",
    token_id="<token_id>"
))
```

---

## 5. CLOB API — Открытые ордера (Position Monitor)

### `GET /orders`
**Используется:** Position Monitor (проверка статуса TP-ордеров, раз в 1-2 мин). Требует authentication.

**Параметры:**
- `market` — фильтр по condition ID
- `asset_id` — фильтр по token ID
- `next_cursor` — пагинация

**Статусы ордера (`status`):**
- `ORDER_STATUS_LIVE` — ордер активен, ещё не исполнен
- `ORDER_STATUS_MATCHED` — **TP исполнен**, позиция закрыта с прибылью
- `ORDER_STATUS_CANCELED_MARKET_RESOLVED` — рынок закрылся, ордер отменён (позиция разрешена по итогу рынка)
- `ORDER_STATUS_CANCELED` — ордер отменён вручную
- `ORDER_STATUS_INVALID` — невалидный ордер

**Пример через py-clob-client:**
```python
open_orders = client.get_orders()  # все открытые ордера
# или
tp_order = client.get_order(order_id)  # конкретный ордер по ID
```

---

## 6. Rate Limits (важно для Data Collector)

| Эндпоинт | Лимит |
|----------|-------|
| `GET /markets` (Gamma) | 300 req / 10s |
| `GET /events` (Gamma) | 500 req / 10s |
| `GET /prices-history` (CLOB) | 1,000 req / 10s |
| `GET /book` (CLOB) | 1,500 req / 10s |
| `POST /order` (CLOB) | 3,500 req / 10s (burst) |
| `GET /orders` (CLOB) | 900 req / 10s |

> Лимиты через Cloudflare — при превышении запросы **throttle** (задержка), а не reject. Для Data Collector: sleep 0.1–0.3s между запросами к `/prices-history` достаточно. Для Screener (раз в 5-10 мин): лимиты некритичны.

---

## Аутентификация CLOB API

Нужна только для торговых эндпоинтов (`POST /order`, `GET /orders`).

**Заголовки:**
- `POLY_API_KEY`
- `POLY_ADDRESS` — Ethereum адрес
- `POLY_SIGNATURE` — HMAC подпись запроса
- `POLY_PASSPHRASE`
- `POLY_TIMESTAMP` — unix timestamp запроса

**На практике:** используем `py-clob-client`, который управляет auth автоматически:
```python
client = ClobClient(host, key=PRIVATE_KEY, chain_id=POLYGON)
client.set_api_creds(client.create_or_derive_api_creds())
```

---

## Полезные ссылки

- Полная документация: https://docs.polymarket.com
- py-clob-client: https://github.com/Polymarket/py-clob-client
- Polymarket agents (референсный код): https://github.com/Polymarket/agents
- OpenAPI specs: https://docs.polymarket.com/api-spec/gamma-openapi.yaml
