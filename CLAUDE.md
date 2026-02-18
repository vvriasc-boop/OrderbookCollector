# OrderbookCollector

Telegram-бот для мониторинга BTC ордербука Binance в реальном времени. Собирает данные по лимитным ордерам, крупным сделкам и ликвидациям через WebSocket, хранит в SQLite, отправляет алерты в Telegram Forum Topics.

## Структура проекта

```
OrderbookCollector/
├── main.py              ← запуск, init, graceful shutdown, ensure_forum_topics
├── config.py            ← .env, константы, пороги, http_session
├── .env                 ← API ключи (не коммитить)
├── requirements.txt
├── README.md
├── CLAUDE.md            ← этот файл
├── database/
│   ├── __init__.py
│   └── db.py            ← SQLite схема, insert/query через run_in_executor
├── services/
│   ├── __init__.py
│   ├── ws_manager.py    ← WebSocket подключения, reconnect, watchdog, system alerts
│   ├── orderbook.py     ← ордербук в памяти, диффы, wall detection, WallInfo
│   ├── trades.py        ← агрегация сделок, CVD, крупные сделки
│   ├── liquidations.py  ← фильтрация и хранение ликвидаций
│   ├── alerts.py        ← AlertManager, ConfirmedWallChecker, SpoofTracker
│   ├── snapshots.py     ← REST снапшоты, periodic refresh, snapshot_recovery_loop, confirmed_wall_check_loop
│   └── digests.py       ← периодические дайджесты 15/30/60 мин
├── handlers/
│   ├── __init__.py
│   ├── commands.py      ← /start /status /walls /trades /liq /cvd /depth /stats /notify /topics /help
│   ├── callbacks.py     ← роутер inline-кнопок
│   └── keyboards.py     ← InlineKeyboardMarkup
└── utils/
    ├── __init__.py
    └── helpers.py        ← форматирование, split_text, fmt_time_msk
```

## Запуск

```bash
# Заполнить .env (TELEGRAM_BOT_TOKEN, ADMIN_USER_ID, FORUM_GROUP_ID)
pip install -r requirements.txt
python main.py
```

## Systemd (автозапуск)

```bash
# Сервис: /etc/systemd/system/orderbook-collector.service
sudo systemctl start orderbook-collector
sudo systemctl stop orderbook-collector
sudo systemctl restart orderbook-collector
sudo systemctl status orderbook-collector
journalctl -u orderbook-collector -f   # логи
```

## .env параметры

| Параметр | Обязательный | Описание |
|----------|:---:|----------|
| TELEGRAM_BOT_TOKEN | да | Токен Telegram-бота от @BotFather |
| ADMIN_USER_ID | да | Telegram user ID для алертов |
| FORUM_GROUP_ID | да | ID супергруппы с Forum Topics (отрицательное число) |
| PROXY_URL | нет | HTTP/SOCKS5 прокси для Binance |
| WALL_THRESHOLD_USD | нет | Порог "стены" (дефолт: 500000) |
| LARGE_TRADE_THRESHOLD_USD | нет | Порог крупной сделки (дефолт: 100000) |

## Telegram Forum Topics

Алерты маршрутизируются в 20 отдельных топиков. Топики создаются автоматически при первом запуске через `ensure_forum_topics()`, thread_id сохраняются в таблицу `forum_topics`.

### Маршрутизация алертов

| Топик | Что попадает | Механизм |
|-------|-------------|----------|
| `walls_futures_bid` | Стены Futures BID (new/gone) | topic_key override |
| `walls_futures_ask` | Стены Futures ASK (new/gone) | topic_key override |
| `walls_spot_bid` | Стены Spot BID (new/gone) | topic_key override |
| `walls_spot_ask` | Стены Spot ASK (new/gone) | topic_key override |
| `trades_futures_buy` | Сделки Futures BUY | topic_key override |
| `trades_futures_sell` | Сделки Futures SELL | topic_key override |
| `trades_spot_buy` | Сделки Spot BUY | topic_key override |
| `trades_spot_sell` | Сделки Spot SELL | topic_key override |
| `confirmed_walls_futures` | Подтв. стены Futures | topic_key override |
| `confirmed_walls_spot` | Подтв. стены Spot | topic_key override |
| `mega_events` | Мега-сделки + мега-ликвидации | ALERT_TO_TOPIC static |
| `liquidations` | Ликвидации | ALERT_TO_TOPIC static |
| `cvd` | CVD алерты | ALERT_TO_TOPIC static |
| `imbalance` | Дисбаланс алерты | ALERT_TO_TOPIC static |
| `digests` | Дайджесты (legacy) | ALERT_TO_TOPIC static |
| `digest_15m` | Дайджест каждые 15 мин | topic_key override |
| `digest_30m` | Дайджест каждые 30 мин | topic_key override |
| `digest_60m` | Дайджест каждые 60 мин | topic_key override |
| `system` | Системные + WS disconnect/recover | ALERT_TO_TOPIC static |

**Два механизма маршрутизации:**
- `ALERT_TO_TOPIC` dict — статическое сопоставление alert_type → topic_key (для типов с одним топиком)
- `topic_key` override через `_enqueue(topic_key=...)` — динамическое, формируется из market+side (для стен, сделок, подтв. стен)

## Детекция стен (Wall Detection)

### Пороги

| Порог | Значение | Назначение |
|-------|---------|------------|
| WALL_THRESHOLD_USD | $500K | Порог обнаружения стены в OrderBook |
| WALL_ALERT_USD | $2M | Порог алерта новой стены |
| WALL_CANCEL_ALERT_USD | $1M | Порог алерта снятой стены |
| CONFIRMED_WALL_THRESHOLD_USD | $5M | Порог подтверждённой стены |
| CONFIRMED_WALL_MAX_DISTANCE_PCT | 2% | Макс. расстояние от mid для подтверждения |
| CONFIRMED_WALL_DELAY_SEC | 60 сек | Минимальное время жизни для подтверждения |

### Подтверждённые стены (ConfirmedWallChecker)

Стена >= $5M, в пределах ±2% от mid, простоявшая >= 60 сек → алерт "подтверждённая стена". Проверка каждые 10 сек в `confirmed_wall_check_loop`. При снятии подтверждённой стены — отдельный алерт.

### Спуфинг-детекция (SpoofTracker)

`SpoofTracker` в `alerts.py` считает сколько раз стена появлялась на одном уровне `market:side:price_str` за последний час. Если count >= 2 — в алерте показывается предупреждение о возможном спуфинге.

### Алерты стен содержат

- Объём и цена
- Расстояние до mid price (% выше/ниже)
- Время жизни стены (для снятых)
- Причина снятия (отменена/исполнена/частично)
- Предупреждение о спуфинге (если count >= 2)

## Таблицы БД

- `orderbook_walls` — жизненный цикл крупных ордеров (price как TEXT!)
- `large_trades` — крупные сделки $100K+
- `liquidations` — все BTC ликвидации
- `trade_aggregates_1m` — 1-минутные агрегаты сделок (volume, delta, CVD, VWAP)
- `ob_snapshots_1m` — снапшоты глубины ордербука каждую минуту
- `alerts_log` — лог отправленных алертов
- `notification_settings` — настройки уведомлений (вкл/выкл по типам)
- `forum_topics` — topic_key → thread_id маппинг

## WebSocket: combined stream формат

Binance combined stream оборачивает события:
```json
{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate",...}}
```
Роутинг: `msg["stream"]` определяет тип, `msg["data"]` содержит событие.

## Ключевые решения

- **str-ключи в dict**: цены хранятся как строки от Binance ("97500.00"). float-ключи ненадёжны из-за floating point.
- **asyncio.Lock**: OrderBook использует Lock для защиты от гонок при await. Public методы берут lock, private — нет (вызываются изнутри).
- **run_in_executor**: SQLite через ThreadPoolExecutor. `check_same_thread=False` обязателен.
- **State recovery**: при старте загружаются active walls из БД и CVD из trade_aggregates_1m.
- **Exponential backoff**: WS reconnect 5→10→20→...→300 сек. Сброс при первом сообщении.
- **Pruning**: раз в минуту удаляются уровни дальше 50% от mid_price (memory management).
- **Batching alerts**: если >3 алертов одного типа за 0.3 сек — объединяются в одно сообщение. Группировка по (alert_type, topic_key).
- **OB sync protection**: при periodic REST refresh — `invalidate()` → буферизация WS → snapshot → обработка буфера. Плюс `snapshot_recovery_loop` (5 сек) для авто-восстановления при любом gap.
- **Нет времени в алертах**: московское время убрано — Telegram показывает timestamp нативно.

## Периодические дайджесты (services/digests.py)

Каждые 15, 30 и 60 минут (выровнены по часам) публикуется отчёт в отдельный Forum Topic (`digest_15m`, `digest_30m`, `digest_60m`). Содержимое:

- **Цена BTC**: начало/конец периода + % изменения (futures only, из `ob_snapshots_1m.mid_price`)
- **Крупные сделки**: кол-во + объём по market×side + дельта BUY-SELL (из `large_trades`)
- **Стакан (стены)**: кол-во + объём по market×side + дельта BID-ASK (из `orderbook_walls`)
- **Фреши по глубине**: стены ≥60 сек жизни, разбивка по ±1%/±2%/±5% от mid + дельта (из `orderbook_walls`)
- **CVD**: дельта за период по market (из `trade_aggregates_1m.delta_usd`)
- **Дисбаланс**: текущий BID/ASK% + кол-во алертов-аномалий (из `ob_snapshots_1m.imbalance_1pct` + `alerts_log`)

### Пороги дайджеста (разные для spot/futures)

| Метрика | Spot | Futures |
|---------|------|---------|
| Крупные сделки | ≥$100K | ≥$500K |
| Стены / Фреши | ≥$500K | ≥$2M |

`digest_loop` — единый цикл, проверяет границы 15/30/60 мин каждые 30 сек. Отправка через `alert_manager.send_digest(text, topic_key)`.

## Разница Futures vs Spot diff-логики

### Spot
1. Отбросить если `u <= lastUpdateId`
2. Первый: `U <= lastUpdateId+1 AND u >= lastUpdateId+1`
3. Далее: `U == lastU + 1`

### Futures
1. Отбросить если `u <= lastUpdateId`
2. Первый: `U <= lastUpdateId AND u >= lastUpdateId`
3. Далее: `pu == lastU` (поле `pu` — previous final update ID)

## Команды разработки

```bash
# Проверка синтаксиса
python3 -m py_compile main.py
python3 -m py_compile config.py
python3 -m py_compile database/db.py
python3 -m py_compile services/orderbook.py
python3 -m py_compile services/alerts.py
python3 -m py_compile services/snapshots.py
python3 -m py_compile services/digests.py
python3 -m py_compile services/ws_manager.py

# Запуск (ручной)
python main.py

# Управление (systemd)
sudo systemctl restart orderbook-collector
```

## Critical Rules

- **get_wall_info() ПЕРЕД unregister_wall()**: при обработке снятия стены в `_process_wall_event`, данные о стене (detected_at для возраста) нужно получить ДО вызова `unregister_wall()`, иначе данные потеряны.
- **list() при итерации dict с await**: `for key, val in self.pending.items()` с `await` внутри цикла — RuntimeError. Использовать `list(self.pending.items())`.
- **distance_pct — знаковая величина**: не использовать `abs()` при сохранении distance_pct. Знак нужен для отображения "ниже"/"выше". Фильтры используют `abs()` явно.
- **invalidate() ПЕРЕД periodic REST refresh**: в `periodic_rest_refresh` обязательно `await ob.invalidate()` перед `fetch_rest_snapshot()`. Без этого WS-события не буферизуются → gap → OB недоступен до следующего refresh (~1 час).
- **CancelledError + self._running в WS таске**: при `task.cancel()` от watchdog, CancelledError не должен re-raise'иться если `self._running=True`. Иначе таск умирает навсегда, флаг connected не сбрасывается, watchdog бесконечно cancel'ит мёртвый таск.

## Lessons Learned

### Async
**[2026-02-18]** RuntimeError: dictionary changed size during iteration

- **Симптом:** Крэш в `check_confirmations` при итерации `self.pending.items()`
- **Причина:** `await` внутри цикла отдаёт управление, другой корутин модифицирует dict
- **Решение:** `for key, pw in list(self.pending.items()):`
- **При повторении:** Любой `for ... in dict.items()` с `await` внутри — обернуть в `list()`

### WebSocket
**[2026-02-18]** Spot OB sync loss after hourly REST refresh

- **Симптом:** Spot OB "not ready" на ~59 мин из каждого часа после periodic REST refresh
- **Причина:** `apply_snapshot()` вызывался пока OB в `ready=True` → WS не буферизовались → после замены `last_update_id` следующий WS diff имеет `U >> expected` → gap → `ready=False`
- **Решение:** `invalidate()` перед `fetch_rest_snapshot()` + `snapshot_recovery_loop` (5 сек)
- **При повторении:** При любой замене состояния в реальном времени — сначала остановить приём событий (буферизация), затем заменить состояние, затем обработать буфер

### WebSocket Watchdog
**[2026-02-18]** Watchdog `forcing reconnect` не переподключал WS

- **Симптом:** Лог спамил "no data for X sec, forcing reconnect" каждые 10 сек, но реального переподключения не было (~70 мин даунтайма)
- **Причина:** `_silence_watchdog` делал `task.cancel()` → `CancelledError` в `_run_connection` → `raise` (re-raise) → таск умирал. Флаг `connected` оставался `True` → watchdog повторял cancel на мёртвый таск
- **Решение:** В `except CancelledError:` проверять `self._running`. Если `True` — это watchdog, не умирать, сбросить backoff delay, продолжить reconnect loop. Если `False` — это `stop()`, re-raise
- **При повторении:** Любой asyncio watchdog, который cancel'ит таски — проверить что таск выживает после cancel и корректно обновляет свои флаги

### Config
**[2026-02-18]** FORUM_GROUP_ID: "The chat is not a forum"

- **Симптом:** Ошибка при создании Forum Topics
- **Причина:** При включении Topics группа мигрирует в супергруппу с новым ID
- **Решение:** Использовать новый ID супергруппы (начинается с -100)
- **При повторении:** После включения Topics проверить новый chat_id через Telegram API
