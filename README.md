# OrderbookCollector

Telegram-бот для мониторинга BTC ордербука, крупных сделок и ликвидаций на Binance в реальном времени. Алерты в Telegram Forum Topics с разделением по рынку и стороне.

## Возможности

- Мониторинг ордербука Futures и Spot через WebSocket
- Детекция крупных ордеров ($500K+) с отслеживанием жизненного цикла
- Подтверждённые стены ($5M+, ±2%, 60 сек) с отдельными топиками
- Детекция спуфинга (повторные появления стен на одном уровне)
- Фиксация крупных сделок ($100K+) с агрегацией в 1-минутные бакеты
- Отслеживание ликвидаций BTC
- CVD (Cumulative Volume Delta)
- 15 Forum Topics с разделением по рынку и стороне (BID/ASK, BUY/SELL)
- Статистика по таймфреймам (30m/1h/4h/24h/48h/all)
- Настраиваемые уведомления

## Установка

```bash
pip install -r requirements.txt
```

## Настройка

Заполните `.env`:
```
TELEGRAM_BOT_TOKEN=your_token
ADMIN_USER_ID=your_user_id
FORUM_GROUP_ID=-100xxxxxxxxxx
PROXY_URL=socks5://...  # опционально
```

## Запуск

```bash
python main.py

# Или через systemd:
sudo systemctl start orderbook-collector
```

## Команды бота

| Команда | Описание |
|---------|----------|
| /start | Главное меню |
| /status | Статус подключений |
| /walls | Активные крупные ордера |
| /trades | Последние крупные сделки |
| /liq | Ликвидации |
| /cvd | Cumulative Volume Delta |
| /depth | Глубина ордербука |
| /stats | Статистика по периодам |
| /notify | Настройки уведомлений |
| /topics | Forum Topics и их thread_id |
| /help | Список команд |
