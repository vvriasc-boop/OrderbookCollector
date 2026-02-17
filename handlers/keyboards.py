from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f9f1 Стены", callback_data="cmd_walls"),
            InlineKeyboardButton("\U0001f40b Сделки", callback_data="cmd_trades"),
        ],
        [
            InlineKeyboardButton("\U0001f480 Ликвидации", callback_data="cmd_liq"),
            InlineKeyboardButton("\U0001f4ca CVD", callback_data="cmd_cvd"),
        ],
        [
            InlineKeyboardButton("\U0001f4cf Глубина", callback_data="cmd_depth"),
            InlineKeyboardButton("\U0001f4c8 Статистика", callback_data="cmd_stats"),
        ],
        [
            InlineKeyboardButton("\U0001f514 Уведомления", callback_data="cmd_notify"),
            InlineKeyboardButton("\u2699\ufe0f Статус", callback_data="cmd_status"),
        ],
        [
            InlineKeyboardButton("\u2753 Помощь", callback_data="cmd_help"),
        ],
    ])


def stats_period_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("30 мин", callback_data="stats_period:30m"),
            InlineKeyboardButton("1 час", callback_data="stats_period:1h"),
            InlineKeyboardButton("4 часа", callback_data="stats_period:4h"),
        ],
        [
            InlineKeyboardButton("24 часа", callback_data="stats_period:24h"),
            InlineKeyboardButton("48 часов", callback_data="stats_period:48h"),
            InlineKeyboardButton("Всё время", callback_data="stats_period:all"),
        ],
        [
            InlineKeyboardButton("\u2b05\ufe0f Назад", callback_data="cmd_back"),
        ],
    ])


def notify_keyboard(settings: dict) -> InlineKeyboardMarkup:
    """Build notification toggle keyboard.

    settings: {alert_type: bool} mapping
    """
    labels = {
        "wall_new": "\U0001f9f1 Стены+",
        "wall_gone": "\U0001f4a5 Стены-",
        "large_trade": "\U0001f40b Сделки",
        "mega_trade": "\U0001f6a8 Мега",
        "liquidation": "\U0001f480 Ликвид",
        "mega_liq": "\U0001f480 МегаЛик",
        "cvd_spike": "\U0001f4ca CVD",
        "imbalance": "\u2696\ufe0f Баланс",
        "confirmed_wall": "\U0001f3f0 $5M+",
        "confirmed_wall_gone": "\U0001f3f0\u274c Снята",
    }

    buttons = []
    row = []
    for alert_type, label in labels.items():
        enabled = settings.get(alert_type, True)
        icon = "\u2705" if enabled else "\u274c"
        row.append(InlineKeyboardButton(
            f"{icon} {label}",
            callback_data=f"notify_toggle:{alert_type}",
        ))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([
        InlineKeyboardButton("\u2705 Вкл все", callback_data="notify_all_on"),
        InlineKeyboardButton("\u274c Выкл все", callback_data="notify_all_off"),
    ])
    buttons.append([
        InlineKeyboardButton("\u2b05\ufe0f Назад", callback_data="cmd_back"),
    ])

    return InlineKeyboardMarkup(buttons)


def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\u2b05\ufe0f Назад", callback_data="cmd_back")],
    ])
