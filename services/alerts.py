import asyncio
import time
import logging

from telegram import Bot
from telegram.constants import ParseMode

import config
from database import db as database
from services.orderbook import WallEvent
from services.trades import LargeTradeEvent
from services.liquidations import LiqEvent
from utils.helpers import format_usd, format_price, format_duration, format_pct, format_timestamp, delta_arrow, split_text

logger = logging.getLogger("orderbook_collector")


class AlertManager:
    """Manages alert cooldowns, formatting, batching and sending to Telegram."""

    def __init__(self, bot: Bot, admin_id: int, cooldown_sec: int):
        self.bot = bot
        self.admin_id = admin_id
        self.cooldown_sec = cooldown_sec
        self.queue: asyncio.Queue = asyncio.Queue()
        self.last_alerts: dict[str, float] = {}  # cooldown_key -> timestamp
        self._send_task: asyncio.Task | None = None

    def start(self):
        self._send_task = asyncio.create_task(self._send_loop())

    async def stop(self):
        if self._send_task:
            self._send_task.cancel()
            try:
                await self._send_task
            except asyncio.CancelledError:
                pass

    async def process_wall_event(self, event: WallEvent):
        """New or gone wall."""
        if event.event_type == "new":
            alert_type = "wall_new"
            cooldown_key = f"wall_new:{event.market}:{event.side}"
            if not await self._should_send(alert_type, cooldown_key):
                return
            price_f = event.price_float
            now = time.time()
            text = (
                f"\U0001f9f1 НОВАЯ СТЕНА — {event.market.title()} {event.side.upper()}\n"
                f"\U0001f4b0 {format_usd(event.new_size_usd)} @ {format_price(price_f)}\n"
                f"\U0001f552 {format_timestamp(now)} UTC"
            )
            await self._enqueue(alert_type, text)

        elif event.event_type in ("cancelled", "filled", "partial"):
            alert_type = "wall_gone"
            cooldown_key = f"wall_gone:{event.market}:{event.side}"
            if not await self._should_send(alert_type, cooldown_key):
                return

            reason_emoji = {
                "cancelled": "отменена",
                "filled": "исполнена (цена коснулась)",
                "partial": "частично исполнена",
            }
            now = time.time()
            text = (
                f"\U0001f4a5 СТЕНА СНЯТА — {event.market.title()} {event.side.upper()}\n"
                f"\U0001f4b0 {format_usd(event.old_size_usd)} @ {format_price(event.price_float)}\n"
                f"\U0001f4ca Причина: {reason_emoji.get(event.event_type, event.event_type)}\n"
                f"\U0001f552 {format_timestamp(now)} UTC"
            )
            await self._enqueue(alert_type, text)

    async def process_large_trade(self, event: LargeTradeEvent):
        """Large trade alert."""
        if event.quantity_usd >= config.MEGA_TRADE_ALERT_USD:
            alert_type = "mega_trade"
        else:
            alert_type = "large_trade"

        cooldown_key = f"{alert_type}:{event.market}:{event.side}"
        if not await self._should_send(alert_type, cooldown_key):
            return

        arrow = "\U0001f534" if event.side == "sell" else "\U0001f7e2"
        emoji = "\U0001f6a8" if alert_type == "mega_trade" else "\U0001f40b"
        text = (
            f"{emoji} {'МЕГА-СДЕЛКА' if alert_type == 'mega_trade' else 'КРУПНАЯ СДЕЛКА'}"
            f" — {event.market.title()}\n"
            f"{arrow} {event.side.upper()} {format_usd(event.quantity_usd)}"
            f" @ {format_price(event.price)}\n"
            f"\U0001f552 {format_timestamp(event.timestamp)} UTC"
        )
        await self._enqueue(alert_type, text)

    async def process_liquidation(self, event: LiqEvent):
        """Large liquidation alert."""
        alert_type = "liquidation"
        cooldown_key = f"liquidation:futures:{event.side}"
        if not await self._should_send(alert_type, cooldown_key):
            return

        arrow = "\U0001f534" if event.side == "long" else "\U0001f7e2"
        text = (
            f"\U0001f480 ЛИКВИДАЦИЯ — Futures\n"
            f"{arrow} {event.side.upper()} {format_usd(event.quantity_usd)}"
            f" @ {format_price(event.price)}\n"
            f"\U0001f552 {format_timestamp(event.timestamp)} UTC"
        )
        await self._enqueue(alert_type, text)

    async def process_cvd_spike(self, delta_5m: float, market: str):
        """CVD spike alert."""
        alert_type = "cvd_spike"
        direction = "buy" if delta_5m > 0 else "sell"
        cooldown_key = f"cvd_spike:{market}:{direction}"
        if not await self._should_send(alert_type, cooldown_key):
            return

        arrow = delta_arrow(delta_5m)
        sign = "+" if delta_5m > 0 else ""
        buyer_seller = "покупатели" if delta_5m > 0 else "продавцы"
        now = time.time()
        text = (
            f"\U0001f4ca CVD ВСПЛЕСК — {market.title()}\n"
            f"{arrow} {sign}{format_usd(delta_5m)} за 5 мин ({buyer_seller})\n"
            f"\U0001f552 {format_timestamp(now)} UTC"
        )
        await self._enqueue(alert_type, text)

    async def process_imbalance(self, imb: float, market: str):
        """Strong orderbook imbalance alert."""
        alert_type = "imbalance"
        direction = "bid" if imb > 0 else "ask"
        cooldown_key = f"imbalance:{market}:{direction}"
        if not await self._should_send(alert_type, cooldown_key):
            return

        bid_pct = int((1 + imb) / 2 * 100)
        ask_pct = 100 - bid_pct
        arrow = delta_arrow(imb)
        dominant = "BID перевес" if imb > 0 else "ASK перевес"
        now = time.time()
        text = (
            f"\u2696\ufe0f ДИСБАЛАНС — {market.title()}\n"
            f"{arrow} {dominant} {bid_pct}% / {ask_pct}% (\u00b11%)\n"
            f"\U0001f552 {format_timestamp(now)} UTC"
        )
        await self._enqueue(alert_type, text)

    async def process_confirmed_wall(self, wall_data: dict):
        """Wall $5M+ within ±2% that has been standing for >= 1 minute."""
        alert_type = "confirmed_wall"
        cooldown_key = f"confirmed_wall:{wall_data['market']}:{wall_data['side']}"
        if not await self._should_send(alert_type, cooldown_key):
            return

        side_label = "BID (поддержка)" if wall_data["side"] == "bid" else "ASK (сопротивление)"
        dist = wall_data["distance_pct"]
        dist_dir = "ниже" if dist < 0 else "выше"
        text = (
            f"\U0001f3f0 ПОДТВЕРЖДЁННАЯ СТЕНА — {wall_data['market'].title()} {side_label}\n"
            f"\U0001f4b0 {format_usd(wall_data['size_usd'])} @ {format_price(wall_data['price'])}\n"
            f"\U0001f4cf Расстояние: {format_pct(abs(dist))} {dist_dir}\n"
            f"\u23f1 Стоит: {format_duration(wall_data['age_sec'])}\n"
            f"\U0001f552 {format_timestamp(wall_data['detected_at'])} UTC"
        )
        await self._enqueue(alert_type, text)

    async def send_admin_message(self, text: str):
        """Send direct message to admin (not through queue)."""
        try:
            for chunk in split_text(text):
                await self.bot.send_message(chat_id=self.admin_id, text=chunk)
                await asyncio.sleep(config.TELEGRAM_DELAY_SEC)
        except Exception as e:
            logger.error("Failed to send admin message: %s", e)

    async def _should_send(self, alert_type: str, cooldown_key: str) -> bool:
        """Check notification settings and cooldown."""
        if not await self._is_enabled(alert_type):
            return False
        if not self._check_cooldown(alert_type, cooldown_key):
            return False
        return True

    async def _is_enabled(self, alert_type: str) -> bool:
        row = await database.get_notification_setting(alert_type)
        if row:
            return bool(row["enabled"])
        return True  # default: enabled

    def _check_cooldown(self, alert_type: str, key: str) -> bool:
        now = time.time()
        last = self.last_alerts.get(key, 0)
        if now - last < self.cooldown_sec:
            return False
        self.last_alerts[key] = now
        return True

    async def _enqueue(self, alert_type: str, text: str):
        await database.insert_alert_log(alert_type, text)
        await self.queue.put({"type": alert_type, "text": text, "time": time.time()})

    async def _send_loop(self):
        """Send alerts from queue with batching and delay."""
        while True:
            try:
                msg = await self.queue.get()

                # Wait briefly for potential batch
                await asyncio.sleep(config.ALERT_BATCH_WAIT_SEC)

                # Collect additional messages
                batch = [msg]
                while not self.queue.empty():
                    try:
                        extra = self.queue.get_nowait()
                        batch.append(extra)
                    except asyncio.QueueEmpty:
                        break

                # Group by type
                groups: dict[str, list] = {}
                for m in batch:
                    groups.setdefault(m["type"], []).append(m)

                for alert_type, msgs in groups.items():
                    if len(msgs) > config.ALERT_BATCH_THRESHOLD:
                        # Batch into one message
                        header = f"\u26a1\ufe0f {len(msgs)} событий ({alert_type}):\n\n"
                        combined = header + "\n---\n".join(m["text"] for m in msgs[:10])
                        if len(msgs) > 10:
                            combined += f"\n\n...и ещё {len(msgs) - 10}"
                        await self._send_telegram(combined)
                    else:
                        for m in msgs:
                            await self._send_telegram(m["text"])
                            if len(msgs) > 1:
                                await asyncio.sleep(config.TELEGRAM_DELAY_SEC)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Alert send_loop error: %s", e)
                await asyncio.sleep(1)

    async def _send_telegram(self, text: str):
        try:
            for chunk in split_text(text):
                await self.bot.send_message(chat_id=self.admin_id, text=chunk)
                await asyncio.sleep(config.TELEGRAM_DELAY_SEC)
        except Exception as e:
            logger.error("Telegram send error: %s", e)
