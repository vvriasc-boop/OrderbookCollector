import asyncio
import time
import logging
from dataclasses import dataclass

from telegram import Bot

import config
from database import db as database
from services.orderbook import WallEvent
from services.trades import LargeTradeEvent
from services.liquidations import LiqEvent
from utils.helpers import (
    format_usd, format_price, format_duration, format_pct,
    fmt_time_msk, delta_arrow, split_text,
)

logger = logging.getLogger("orderbook_collector")

# --- Alert type -> topic key mapping ---
ALERT_TO_TOPIC = {
    "wall_new":             "walls",
    "wall_gone":            "walls",
    "confirmed_wall":       "confirmed_walls",
    "confirmed_wall_gone":  "confirmed_walls",
    "large_trade":          "large_trades",
    "mega_trade":           "mega_events",
    "liquidation":          "liquidations",
    "mega_liq":             "mega_events",
    "cvd_spike":            "cvd_imbalance",
    "imbalance":            "cvd_imbalance",
    "system":               "system",
    "digest":               "digests",
}


class AlertManager:
    """Manages alert cooldowns, formatting, batching and sending to Telegram topics."""

    def __init__(self, bot: Bot, admin_id: int, cooldown_sec: int):
        self.bot = bot
        self.admin_id = admin_id
        self.cooldown_sec = cooldown_sec
        self.queue: asyncio.Queue = asyncio.Queue()
        self.last_alerts: dict[str, float] = {}
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

    # --- Alert processors ---

    async def process_wall_event(self, event: WallEvent):
        """New or gone wall."""
        if event.event_type == "new":
            alert_type = "wall_new"
            cooldown_key = f"wall_new:{event.market}:{event.side}"
            if not await self._should_send(alert_type, cooldown_key):
                return
            price_f = event.price_float
            text = (
                f"\U0001f9f1 НОВАЯ СТЕНА — {event.market.title()} {event.side.upper()}\n"
                f"\U0001f4b0 {format_usd(event.new_size_usd)} @ {format_price(price_f)}\n"
                f"\U0001f552 {fmt_time_msk()}"
            )
            await self._enqueue(alert_type, text)

        elif event.event_type in ("cancelled", "filled", "partial"):
            alert_type = "wall_gone"
            cooldown_key = f"wall_gone:{event.market}:{event.side}"
            if not await self._should_send(alert_type, cooldown_key):
                return
            reason_map = {
                "cancelled": "отменена",
                "filled": "исполнена (цена коснулась)",
                "partial": "частично исполнена",
            }
            text = (
                f"\U0001f4a5 СТЕНА СНЯТА — {event.market.title()} {event.side.upper()}\n"
                f"\U0001f4b0 {format_usd(event.old_size_usd)} @ {format_price(event.price_float)}\n"
                f"\U0001f4ca Причина: {reason_map.get(event.event_type, event.event_type)}\n"
                f"\U0001f552 {fmt_time_msk()}"
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
        label = "МЕГА-СДЕЛКА" if alert_type == "mega_trade" else "КРУПНАЯ СДЕЛКА"
        text = (
            f"{emoji} {label} — {event.market.title()}\n"
            f"{arrow} {event.side.upper()} {format_usd(event.quantity_usd)}"
            f" @ {format_price(event.price)}\n"
            f"\U0001f552 {fmt_time_msk(event.timestamp)}"
        )
        await self._enqueue(alert_type, text)

    async def process_liquidation(self, event: LiqEvent):
        """Liquidation alert. >=1M -> mega_events topic, else liquidations topic."""
        if event.quantity_usd >= config.LIQ_ALERT_USD:
            alert_type = "mega_liq"
        else:
            alert_type = "liquidation"

        cooldown_key = f"{alert_type}:futures:{event.side}"
        if not await self._should_send(alert_type, cooldown_key):
            return

        arrow = "\U0001f534" if event.side == "long" else "\U0001f7e2"
        text = (
            f"\U0001f480 ЛИКВИДАЦИЯ — Futures\n"
            f"{arrow} {event.side.upper()} {format_usd(event.quantity_usd)}"
            f" @ {format_price(event.price)}\n"
            f"\U0001f552 {fmt_time_msk(event.timestamp)}"
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
        text = (
            f"\U0001f4ca CVD ВСПЛЕСК — {market.title()}\n"
            f"{arrow} {sign}{format_usd(delta_5m)} за 5 мин ({buyer_seller})\n"
            f"\U0001f552 {fmt_time_msk()}"
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
        text = (
            f"\u2696\ufe0f ДИСБАЛАНС — {market.title()}\n"
            f"{arrow} {dominant} {bid_pct}% / {ask_pct}% (\u00b11%)\n"
            f"\U0001f552 {fmt_time_msk()}"
        )
        await self._enqueue(alert_type, text)

    async def process_confirmed_wall(self, wall_data: dict):
        """Confirmed wall: $5M+, ±2%, stood >= 1 min."""
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
            f"\u23f1 Стоит уже: {format_duration(wall_data['age_sec'])}\n"
            f"\U0001f552 Обнаружена: {fmt_time_msk(wall_data['detected_at'])}\n"
            f"\U0001f552 Подтверждена: {fmt_time_msk()}"
        )
        await self._enqueue(alert_type, text)

    async def process_confirmed_wall_gone(self, wall_data: dict, reason: str):
        """Confirmed wall removed/filled."""
        alert_type = "confirmed_wall_gone"
        cooldown_key = f"confirmed_wall_gone:{wall_data['market']}:{wall_data['side']}"
        if not await self._should_send(alert_type, cooldown_key):
            return

        side_label = "BID" if wall_data["side"] == "bid" else "ASK"
        reason_map = {
            "cancelled": "отменена (цена далеко)",
            "filled": "исполнена (цена коснулась)",
            "partial": "частично исполнена",
        }
        age = time.time() - wall_data["detected_at"]
        text = (
            f"\U0001f3f0\u274c СТЕНА СНЯТА — {wall_data['market'].title()} {side_label}\n"
            f"\U0001f4b0 {format_usd(wall_data['size_usd'])} @ {format_price(wall_data['price'])}\n"
            f"\u23f1 Жила: {format_duration(age)}\n"
            f"\U0001f4ca Причина: {reason_map.get(reason, reason)}\n"
            f"\U0001f552 {fmt_time_msk()}"
        )
        await self._enqueue(alert_type, text)

    async def send_system_message(self, text: str):
        """Send message to system topic."""
        text += f"\n\U0001f552 {fmt_time_msk()}"
        await self._enqueue("system", text)

    # --- Internal ---

    async def _should_send(self, alert_type: str, cooldown_key: str) -> bool:
        if not await self._is_enabled(alert_type):
            return False
        if not self._check_cooldown(alert_type, cooldown_key):
            return False
        return True

    async def _is_enabled(self, alert_type: str) -> bool:
        row = await database.get_notification_setting(alert_type)
        if row:
            return bool(row["enabled"])
        return True

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
                await asyncio.sleep(config.ALERT_BATCH_WAIT_SEC)

                batch = [msg]
                while not self.queue.empty():
                    try:
                        batch.append(self.queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                # Group by type
                groups: dict[str, list] = {}
                for m in batch:
                    groups.setdefault(m["type"], []).append(m)

                for alert_type, msgs in groups.items():
                    if len(msgs) > config.ALERT_BATCH_THRESHOLD:
                        header = f"\u26a1\ufe0f {len(msgs)} событий ({alert_type}):\n\n"
                        combined = header + "\n---\n".join(m["text"] for m in msgs[:10])
                        if len(msgs) > 10:
                            combined += f"\n\n...и ещё {len(msgs) - 10}"
                        await self._send_to_topic(alert_type, combined)
                    else:
                        for m in msgs:
                            await self._send_to_topic(alert_type, m["text"])
                            if len(msgs) > 1:
                                await asyncio.sleep(config.TELEGRAM_DELAY_SEC)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Alert send_loop error: %s", e)
                await asyncio.sleep(1)

    async def _send_to_topic(self, alert_type: str, text: str):
        """Send message to the appropriate forum topic."""
        topic_key = ALERT_TO_TOPIC.get(alert_type, "system")
        thread_id = config.TOPIC_IDS.get(topic_key)

        chat_id = config.FORUM_GROUP_ID
        if not chat_id:
            # Fallback to admin DM if no forum group configured
            chat_id = self.admin_id
            thread_id = None

        try:
            for chunk in split_text(text):
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                    message_thread_id=thread_id,
                )
                await asyncio.sleep(config.TELEGRAM_DELAY_SEC)
        except Exception as e:
            logger.error("Telegram send error (topic=%s): %s", topic_key, e)


# --- Confirmed Wall Checker ---

@dataclass
class PendingWall:
    price_str: str
    side: str
    market: str
    size_usd: float
    detected_at: float
    distance_pct: float


class ConfirmedWallChecker:
    """Checks walls for confirmation (stood for 1 minute, $5M+, within ±2%)."""

    def __init__(self):
        self.pending: dict[str, PendingWall] = {}  # key -> PendingWall
        self.already_confirmed: set[str] = set()
        self.confirmed_data: dict[str, PendingWall] = {}  # for gone notifications

    def on_wall_detected(self, event: WallEvent, mid_price: float):
        """Call when a wall >= $5M appears within 2%."""
        if event.new_size_usd < config.CONFIRMED_WALL_THRESHOLD_USD:
            return
        if mid_price <= 0:
            return
        distance_pct = (event.price_float - mid_price) / mid_price * 100
        if abs(distance_pct) > config.CONFIRMED_WALL_MAX_DISTANCE_PCT:
            return

        key = f"{event.market}:{event.side}:{event.price_str}"
        if key in self.already_confirmed:
            return

        self.pending[key] = PendingWall(
            price_str=event.price_str,
            side=event.side,
            market=event.market,
            size_usd=event.new_size_usd,
            detected_at=time.time(),
            distance_pct=distance_pct,
        )

    def on_wall_gone(self, event: WallEvent) -> PendingWall | None:
        """Call when a wall disappears. Returns PendingWall if it was confirmed (for gone alert)."""
        key = f"{event.market}:{event.side}:{event.price_str}"
        self.pending.pop(key, None)

        if key in self.already_confirmed:
            self.already_confirmed.discard(key)
            return self.confirmed_data.pop(key, None)
        return None

    async def check_confirmations(self, orderbooks: dict) -> list[PendingWall]:
        """Check pending walls for confirmation. Call every ~10 seconds.

        orderbooks: {"futures": OrderBook, "spot": OrderBook}
        Returns list of newly confirmed walls.
        """
        now = time.time()
        confirmed = []
        to_remove = []

        for key, pw in list(self.pending.items()):
            if now - pw.detected_at < config.CONFIRMED_WALL_DELAY_SEC:
                continue

            ob = orderbooks.get(pw.market)
            if not ob:
                to_remove.append(key)
                continue

            wall_state = await ob.check_wall_exists(pw.price_str)
            if not wall_state:
                to_remove.append(key)
                continue

            if wall_state["size_usd"] < config.CONFIRMED_WALL_THRESHOLD_USD:
                to_remove.append(key)
                continue

            if abs(wall_state["distance_pct"]) > config.CONFIRMED_WALL_MAX_DISTANCE_PCT:
                to_remove.append(key)
                continue

            # Confirmed
            pw.size_usd = wall_state["size_usd"]
            pw.distance_pct = wall_state["distance_pct"]
            confirmed.append(pw)
            self.already_confirmed.add(key)
            self.confirmed_data[key] = pw
            to_remove.append(key)

        for key in to_remove:
            self.pending.pop(key, None)

        return confirmed
