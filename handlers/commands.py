import time
import logging

from telegram import Update
from telegram.ext import ContextTypes

import config
from database import db as database
from handlers.keyboards import main_menu_keyboard, stats_period_keyboard, notify_keyboard, back_keyboard
from utils.helpers import (
    format_usd, format_price, format_duration, format_pct,
    delta_arrow, imbalance_bar, split_text,
)

logger = logging.getLogger("orderbook_collector")


def get_app_context(context: ContextTypes.DEFAULT_TYPE) -> dict:
    """Get shared application context (ob, ws_manager, etc.)."""
    return context.bot_data


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "\U0001f4e1 OrderbookCollector\n\n"
        "Мониторинг BTC ордербука, сделок и ликвидаций."
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ctx = get_app_context(context)
    ws = ctx.get("ws_manager")
    ob_f = ctx.get("ob_futures")
    ob_s = ctx.get("ob_spot")

    ws_status = ws.get_status() if ws else {}
    f_icon = "\u2705" if ws_status.get("futures_connected") else "\u274c"
    s_icon = "\u2705" if ws_status.get("spot_connected") else "\u274c"
    f_up = format_duration(ws_status.get("futures_uptime", 0))
    s_up = format_duration(ws_status.get("spot_uptime", 0))

    ob_f_st = await ob_f.get_status() if ob_f else {}
    ob_s_st = await ob_s.get_status() if ob_s else {}

    # Walls summary
    walls_f = await ob_f.get_walls_list() if ob_f else []
    walls_s = await ob_s.get_walls_list() if ob_s else []
    bid_walls_f = [w for w in walls_f if w["side"] == "bid"]
    ask_walls_f = [w for w in walls_f if w["side"] == "ask"]

    db_size = await database.get_db_size()

    text = (
        f"\U0001f4e1 OrderbookCollector — Status\n\n"
        f"WebSocket:\n"
        f"  Futures: {f_icon} {'Connected' if ws_status.get('futures_connected') else 'Disconnected'}"
        f" (uptime: {f_up})\n"
        f"  Spot: {s_icon} {'Connected' if ws_status.get('spot_connected') else 'Disconnected'}"
        f" (uptime: {s_up})\n\n"
        f"Ордербук:\n"
        f"  Futures: {ob_f_st.get('bid_levels', 0):,} bid | {ob_f_st.get('ask_levels', 0):,} ask\n"
        f"  Spot: {ob_s_st.get('bid_levels', 0):,} bid | {ob_s_st.get('ask_levels', 0):,} ask\n\n"
        f"Активные стены:\n"
        f"  \U0001f7e2 {len(bid_walls_f)} bid walls"
    )
    if bid_walls_f:
        text += " (" + " + ".join(format_usd(w["size_usd"]) for w in sorted(bid_walls_f, key=lambda x: -x["size_usd"])[:3]) + ")"
    text += f"\n  \U0001f534 {len(ask_walls_f)} ask walls"
    if ask_walls_f:
        text += " (" + " + ".join(format_usd(w["size_usd"]) for w in sorted(ask_walls_f, key=lambda x: -x["size_usd"])[:3]) + ")"
    text += f"\n\nБД: {db_size:.1f} MB"

    msg = update.message or update.callback_query.message
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=back_keyboard())
    else:
        await msg.reply_text(text, reply_markup=back_keyboard())


async def cmd_walls(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ctx = get_app_context(context)
    ob_f = ctx.get("ob_futures")
    ob_s = ctx.get("ob_spot")

    text = ""

    for ob, label in [(ob_f, "Futures"), (ob_s, "Spot")]:
        if not ob:
            continue
        walls = await ob.get_walls_list()
        if not walls:
            text += f"\U0001f9f1 Активные стены ({label}): нет\n\n"
            continue

        bids = sorted([w for w in walls if w["side"] == "bid"], key=lambda x: -x["size_usd"])
        asks = sorted([w for w in walls if w["side"] == "ask"], key=lambda x: -x["size_usd"])

        text += f"\U0001f9f1 Активные стены ({label}):\n\n"
        if bids:
            text += "BID (поддержка):\n"
            for i, w in enumerate(bids, 1):
                text += (
                    f"  {i}. {format_usd(w['size_usd'])} @ {format_price(w['price'])}"
                    f" ({format_pct(abs(w['distance_pct']))} ниже)"
                    f" — {format_duration(w['age_sec'])}\n"
                )
        if asks:
            text += "\nASK (сопротивление):\n"
            for i, w in enumerate(asks, 1):
                text += (
                    f"  {i}. {format_usd(w['size_usd'])} @ {format_price(w['price'])}"
                    f" ({format_pct(abs(w['distance_pct']))} выше)"
                    f" — {format_duration(w['age_sec'])}\n"
                )
        text += "\n"

    if not text:
        text = "Нет данных об ордербуке."

    msg = update.message or update.callback_query.message
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=back_keyboard())
    else:
        for chunk in split_text(text):
            await msg.reply_text(chunk, reply_markup=back_keyboard())


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = await database.fetchall(
        "SELECT * FROM large_trades ORDER BY timestamp DESC LIMIT 10"
    )

    if not rows:
        text = "\U0001f40b Крупные сделки: нет данных"
    else:
        text = "\U0001f40b Последние крупные сделки:\n\n"
        for r in rows:
            arrow = "\U0001f534" if r["side"] == "sell" else "\U0001f7e2"
            text += (
                f"{arrow} {r['side'].upper()} {format_usd(r['quantity_usd'])}"
                f" @ {format_price(r['price'])} ({r['market']})\n"
            )

    msg = update.message or update.callback_query.message
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=back_keyboard())
    else:
        await msg.reply_text(text, reply_markup=back_keyboard())


async def cmd_liq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = await database.fetchall(
        "SELECT * FROM liquidations ORDER BY timestamp DESC LIMIT 10"
    )

    if not rows:
        text = "\U0001f480 Ликвидации: нет данных"
    else:
        text = "\U0001f480 Последние ликвидации:\n\n"
        for r in rows:
            arrow = "\U0001f534" if r["side"] == "long" else "\U0001f7e2"
            text += (
                f"{arrow} {r['side'].upper()} {format_usd(r['quantity_usd'])}"
                f" @ {format_price(r['price'])}\n"
            )

    msg = update.message or update.callback_query.message
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=back_keyboard())
    else:
        await msg.reply_text(text, reply_markup=back_keyboard())


async def cmd_cvd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ctx = get_app_context(context)
    trade_f = ctx.get("trade_agg_futures")
    trade_s = ctx.get("trade_agg_spot")

    text = "\U0001f4ca CVD (Cumulative Volume Delta)\n\n"

    for agg, label in [(trade_f, "Futures"), (trade_s, "Spot")]:
        if not agg:
            continue
        stats = await agg.get_cvd_stats()
        text += f"{label}:\n"
        text += f"  Сегодня: {'+' if stats['cvd_today'] >= 0 else ''}{format_usd(stats['cvd_today'])} {delta_arrow(stats['cvd_today'])}\n"
        text += f"  Последний час: {'+' if stats['cvd_1h'] >= 0 else ''}{format_usd(stats['cvd_1h'])} {delta_arrow(stats['cvd_1h'])}\n"
        text += f"  Последние 5 мин: {'+' if stats['cvd_5m'] >= 0 else ''}{format_usd(stats['cvd_5m'])} {delta_arrow(stats['cvd_5m'])}\n\n"

    msg = update.message or update.callback_query.message
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=back_keyboard())
    else:
        await msg.reply_text(text, reply_markup=back_keyboard())


async def cmd_depth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ctx = get_app_context(context)
    ob_f = ctx.get("ob_futures")
    ob_s = ctx.get("ob_spot")

    text = ""
    for ob, label in [(ob_f, "Futures"), (ob_s, "Spot")]:
        if not ob:
            continue
        data = await ob.get_depth_display()
        if data["mid"] <= 0:
            text += f"\U0001f4ca Глубина ордербука ({label}): нет данных\n\n"
            continue

        text += (
            f"\U0001f4ca Глубина ордербука ({label})\n\n"
            f"Mid price: {format_price(data['mid'])}\n"
            f"Spread: {format_pct(data['spread'])}\n\n"
            f"{'':>8}BID{'':>8}ASK\n"
        )
        for r in data["ranges"]:
            bid_s = format_usd(r["bid_usd"])
            ask_s = format_usd(r["ask_usd"])
            ib = imbalance_bar(r["bid_pct"])
            text += f"{r['label']:>5}: {bid_s:>8}  | {ask_s:>8}  ({r['bid_pct']:.0f}/{r['ask_pct']:.0f})"
            if ib:
                text += f" {ib}"
            text += "\n"
        text += "\n"

    if not text:
        text = "Нет данных об ордербуке."

    msg = update.message or update.callback_query.message
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=back_keyboard())
    else:
        await msg.reply_text(text, reply_markup=back_keyboard())


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "\U0001f4c8 Выберите период статистики:"
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=stats_period_keyboard())
    else:
        await update.message.reply_text(text, reply_markup=stats_period_keyboard())


async def build_stats_text(period_key: str, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Build statistics text for a given period."""
    periods = {
        "30m": (30 * 60, "30 минут"),
        "1h": (3600, "1 час"),
        "4h": (4 * 3600, "4 часа"),
        "24h": (24 * 3600, "24 часа"),
        "48h": (48 * 3600, "48 часов"),
        "all": (0, "всё время"),
    }

    period_sec, period_label = periods.get(period_key, (3600, "1 час"))
    cutoff = time.time() - period_sec if period_sec > 0 else 0

    # Trades
    trade_rows = await database.fetchall(
        "SELECT side, COUNT(*) as cnt, SUM(quantity_usd) as total, MAX(quantity_usd) as mx "
        "FROM large_trades WHERE timestamp >= ? GROUP BY side",
        (cutoff,),
    )
    trade_stats = {"buy": (0, 0, 0), "sell": (0, 0, 0)}
    for r in trade_rows:
        trade_stats[r["side"]] = (r["cnt"], r["total"] or 0, r["mx"] or 0)

    total_trades = trade_stats["buy"][0] + trade_stats["sell"][0]
    total_vol = trade_stats["buy"][1] + trade_stats["sell"][1]
    avg_trade = total_vol / total_trades if total_trades > 0 else 0

    # Liquidations
    liq_rows = await database.fetchall(
        "SELECT side, COUNT(*) as cnt, SUM(quantity_usd) as total, MAX(quantity_usd) as mx "
        "FROM liquidations WHERE timestamp >= ? GROUP BY side",
        (cutoff,),
    )
    liq_stats = {"long": (0, 0, 0), "short": (0, 0, 0)}
    for r in liq_rows:
        liq_stats[r["side"]] = (r["cnt"], r["total"] or 0, r["mx"] or 0)

    total_liq = liq_stats["long"][0] + liq_stats["short"][0]

    # Walls
    walls_appeared = await database.fetchone(
        "SELECT COUNT(*) as c FROM orderbook_walls WHERE detected_at >= ?", (cutoff,)
    )
    walls_gone = await database.fetchone(
        "SELECT COUNT(*) as c FROM orderbook_walls WHERE ended_at >= ? AND ended_at IS NOT NULL",
        (cutoff,),
    )
    wall_reasons = await database.fetchall(
        "SELECT end_reason, COUNT(*) as c FROM orderbook_walls "
        "WHERE ended_at >= ? AND end_reason IS NOT NULL GROUP BY end_reason",
        (cutoff,),
    )
    avg_lifetime = await database.fetchone(
        "SELECT AVG(lifetime_sec) as a FROM orderbook_walls "
        "WHERE ended_at >= ? AND lifetime_sec IS NOT NULL",
        (cutoff,),
    )
    active_walls = await database.fetchone(
        "SELECT COUNT(*) as c FROM orderbook_walls WHERE status = 'active'"
    )
    active_bid = await database.fetchone(
        "SELECT COUNT(*) as c FROM orderbook_walls WHERE status = 'active' AND side = 'bid'"
    )
    active_ask = await database.fetchone(
        "SELECT COUNT(*) as c FROM orderbook_walls WHERE status = 'active' AND side = 'ask'"
    )

    filled = sum(r["c"] for r in wall_reasons if r["end_reason"] == "filled")
    cancelled = sum(r["c"] for r in wall_reasons if r["end_reason"] == "cancelled")
    total_ended = filled + cancelled
    filled_pct = (filled / total_ended * 100) if total_ended > 0 else 0
    cancelled_pct = (cancelled / total_ended * 100) if total_ended > 0 else 0

    # Volume
    vol_futures = await database.fetchone(
        "SELECT SUM(buy_volume_usd) as bv, SUM(sell_volume_usd) as sv, SUM(delta_usd) as d "
        "FROM trade_aggregates_1m WHERE timestamp >= ? AND market = 'futures'",
        (cutoff,),
    )
    vol_spot = await database.fetchone(
        "SELECT SUM(buy_volume_usd) as bv, SUM(sell_volume_usd) as sv, SUM(delta_usd) as d "
        "FROM trade_aggregates_1m WHERE timestamp >= ? AND market = 'spot'",
        (cutoff,),
    )

    # Alerts count
    alerts_count = await database.fetchone(
        "SELECT COUNT(*) as c FROM alerts_log WHERE timestamp >= ?", (cutoff,)
    )

    # Build text
    text = f"\U0001f4ca Статистика за {period_label}\n\n"

    text += (
        f"\U0001f40b Сделки:\n"
        f"  Всего крупных: {total_trades}\n"
        f"  BUY: {trade_stats['buy'][0]} ({format_usd(trade_stats['buy'][1])})"
        f" | SELL: {trade_stats['sell'][0]} ({format_usd(trade_stats['sell'][1])})\n"
    )
    # Max trade
    max_side = "BUY" if trade_stats["buy"][2] >= trade_stats["sell"][2] else "SELL"
    max_val = max(trade_stats["buy"][2], trade_stats["sell"][2])
    if max_val > 0:
        text += f"  Макс: {max_side} {format_usd(max_val)}\n"
    if avg_trade > 0:
        text += f"  Средняя крупная: {format_usd(avg_trade)}\n"
    text += "\n"

    text += (
        f"\U0001f480 Ликвидации:\n"
        f"  Всего: {total_liq}\n"
        f"  LONG: {liq_stats['long'][0]} ({format_usd(liq_stats['long'][1])})"
        f" | SHORT: {liq_stats['short'][0]} ({format_usd(liq_stats['short'][1])})\n"
    )
    max_liq_side = "LONG" if liq_stats["long"][2] >= liq_stats["short"][2] else "SHORT"
    max_liq_val = max(liq_stats["long"][2], liq_stats["short"][2])
    if max_liq_val > 0:
        text += f"  Макс: {max_liq_side} {format_usd(max_liq_val)}\n"
    text += "\n"

    text += (
        f"\U0001f9f1 Стены:\n"
        f"  Появилось: {walls_appeared['c'] if walls_appeared else 0}"
        f" | Исчезло: {walls_gone['c'] if walls_gone else 0}\n"
        f"  Сейчас активных: {active_walls['c'] if active_walls else 0}"
        f" ({active_bid['c'] if active_bid else 0} bid / {active_ask['c'] if active_ask else 0} ask)\n"
    )
    if avg_lifetime and avg_lifetime["a"]:
        text += f"  Средняя жизнь: {format_duration(avg_lifetime['a'])}\n"
    if total_ended > 0:
        text += f"  Исполнено: {filled} ({filled_pct:.0f}%) | Отменено: {cancelled} ({cancelled_pct:.0f}%)\n"
    text += "\n"

    # Volume
    for row, label in [(vol_futures, "Futures"), (vol_spot, "Spot")]:
        if row and row["bv"]:
            bv = row["bv"] or 0
            sv = row["sv"] or 0
            d = row["d"] or 0
            text += (
                f"\U0001f4ca Объём ({label}):\n"
                f"  BUY: {format_usd(bv)} | SELL: {format_usd(sv)}\n"
                f"  CVD: {'+' if d >= 0 else ''}{format_usd(d)} {delta_arrow(d)}\n\n"
            )

    text += f"\U0001f514 Алертов отправлено: {alerts_count['c'] if alerts_count else 0}"

    return text


async def cmd_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, reply_markup=notify_keyboard(settings)
        )
    else:
        await update.message.reply_text(text, reply_markup=notify_keyboard(settings))


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "\u2753 Справка — OrderbookCollector\n\n"
        "Бот в реальном времени собирает данные ордербука BTC с Binance.\n\n"
        "Команды:\n"
        "/start — Главное меню\n"
        "/status — Статус подключений\n"
        "/walls — Текущие крупные ордера ($500K+)\n"
        "/trades — Последние крупные сделки\n"
        "/liq — Ликвидации\n"
        "/cvd — Cumulative Volume Delta\n"
        "/depth — Глубина ордербука\n"
        "/stats — Статистика по периодам\n"
        "/notify — Настройки уведомлений\n\n"
        "Алерты:\n"
        "  \U0001f9f1 Стены $2M+\n"
        "  \U0001f40b Сделки $500K+\n"
        "  \U0001f480 Ликвидации $1M+\n"
        "  \U0001f4ca CVD спайки $5M/5мин\n"
        "  \u2696\ufe0f Дисбаланс >40%"
    )
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=back_keyboard())
    else:
        await update.message.reply_text(text, reply_markup=back_keyboard())
