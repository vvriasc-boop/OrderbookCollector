import logging

from telegram import Update
from telegram.ext import ContextTypes

from database import db as database
from handlers import commands
from handlers.keyboards import main_menu_keyboard, notify_keyboard, back_keyboard

logger = logging.getLogger("orderbook_collector")


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Route all callback queries."""
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "cmd_back":
        text = (
            "\U0001f4e1 OrderbookCollector\n\n"
            "Мониторинг BTC ордербука, сделок и ликвидаций."
        )
        await query.edit_message_text(text, reply_markup=main_menu_keyboard())

    elif data == "cmd_walls":
        await commands.cmd_walls(update, context)

    elif data == "cmd_trades":
        await commands.cmd_trades(update, context)

    elif data == "cmd_liq":
        await commands.cmd_liq(update, context)

    elif data == "cmd_cvd":
        await commands.cmd_cvd(update, context)

    elif data == "cmd_depth":
        await commands.cmd_depth(update, context)

    elif data == "cmd_stats":
        await commands.cmd_stats(update, context)

    elif data == "cmd_status":
        await commands.cmd_status(update, context)

    elif data == "cmd_notify":
        await commands.cmd_notify(update, context)

    elif data == "cmd_help":
        await commands.cmd_help(update, context)

    elif data.startswith("stats_period:"):
        period_key = data.split(":", 1)[1]
        text = await commands.build_stats_text(period_key, context)
        await query.edit_message_text(text, reply_markup=back_keyboard())

    elif data.startswith("notify_toggle:"):
        alert_type = data.split(":", 1)[1]
        new_state = await database.toggle_notification(alert_type)
        # Refresh notify view
        rows = await database.get_all_notification_settings()
        settings = {r["alert_type"]: bool(r["enabled"]) for r in rows}

        labels = {
            "wall_new": ("\U0001f9f1 Новые стены", "$2M+"),
            "wall_gone": ("\U0001f4a5 Стены сняты", "$1M+"),
            "large_trade": ("\U0001f40b Крупные сделки", "$500K+"),
            "mega_trade": ("\U0001f6a8 Мега-сделки", "$2M+"),
            "liquidation": ("\U0001f480 Ликвидации", "$1M+"),
            "cvd_spike": ("\U0001f4ca CVD спайки", "$5M/5m"),
            "imbalance": ("\u2696\ufe0f Дисбаланс", ">40%"),
            "confirmed_wall": ("\U0001f3f0 Стена $5M+", "\u00b12%, 1мин"),
        }
        text = "\U0001f514 Настройки уведомлений\n\n"
        for at, (label, threshold) in labels.items():
            enabled = settings.get(at, True)
            icon = "\u2705 Вкл" if enabled else "\u274c Выкл"
            text += f"{label} ({threshold})     [{icon}]\n"
        text += "\nНажмите для переключения:"
        await query.edit_message_text(text, reply_markup=notify_keyboard(settings))

    elif data == "notify_all_on":
        await database.set_all_notifications(True)
        rows = await database.get_all_notification_settings()
        settings = {r["alert_type"]: bool(r["enabled"]) for r in rows}
        await commands.cmd_notify(update, context)

    elif data == "notify_all_off":
        await database.set_all_notifications(False)
        rows = await database.get_all_notification_settings()
        settings = {r["alert_type"]: bool(r["enabled"]) for r in rows}
        await commands.cmd_notify(update, context)

    else:
        logger.warning("Unknown callback: %s", data)
