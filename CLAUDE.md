# OrderbookCollector

Telegram-бот для мониторинга BTC ордербука Binance в реальном времени. Собирает данные по лимитным ордерам, крупным сделкам и ликвидациям через WebSocket, хранит в SQLite, отправляет алерты.

## Структура проекта

```
OrderbookCollector/
├── main.py              ← запуск, init, graceful shutdown
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
│   ├── ws_manager.py    ← WebSocket подключения, reconnect, proxy
│   ├── orderbook.py     ← ордербук в памяти, диффы, wall detection
│   ├── trades.py        ← агрегация сделок, CVD, крупные сделки
│   ├── liquidations.py  ← фильтрация и хранение ликвидаций
│   ├── alerts.py        ← алерты, cooldown, batching, Telegram
│   └── snapshots.py     ← REST снапшоты, периодические задачи
├── handlers/
│   ├── __init__.py
│   ├── commands.py      ← /start, /status, /walls, /trades, /liq, /cvd, /depth, /stats, /notify
│   ├── callbacks.py     ← роутер inline-кнопок
│   └── keyboards.py     ← InlineKeyboardMarkup
└── utils/
    ├── __init__.py
    └── helpers.py        ← форматирование, split_text
```

## Запуск

```bash
# Заполнить .env (TELEGRAM_BOT_TOKEN, ADMIN_USER_ID, опционально PROXY_URL)
pip install -r requirements.txt
python main.py
```

## Перезапуск

```bash
# Graceful (отправляет SIGTERM)
kill -SIGTERM $(pgrep -f "python main.py")
# Или Ctrl+C (SIGINT)
```

## .env параметры

| Параметр | Обязательный | Описание |
|----------|:---:|----------|
| TELEGRAM_BOT_TOKEN | да | Токен Telegram-бота от @BotFather |
| ADMIN_USER_ID | да | Telegram user ID для алертов |
| PROXY_URL | нет | HTTP/SOCKS5 прокси для Binance |
| WALL_THRESHOLD_USD | нет | Порог "стены" (дефолт: 500000) |
| LARGE_TRADE_THRESHOLD_USD | нет | Порог крупной сделки (дефолт: 100000) |

## Таблицы БД

- `orderbook_walls` — жизненный цикл крупных ордеров (price как TEXT!)
- `large_trades` — крупные сделки $100K+
- `liquidations` — все BTC ликвидации
- `trade_aggregates_1m` — 1-минутные агрегаты сделок (volume, delta, CVD, VWAP)
- `ob_snapshots_1m` — снапшоты глубины ордербука каждую минуту
- `alerts_log` — лог отправленных алертов
- `notification_settings` — настройки уведомлений (вкл/выкл по типам)

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
- **Batching alerts**: если >3 алертов одного типа за 0.3 сек — объединяются в одно сообщение.

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

# Запуск
python main.py

# Перезапуск
kill -SIGTERM $(pgrep -f "python main.py") && sleep 2 && python main.py
```

## Lessons Learned

(пополнять по мере разработки)
