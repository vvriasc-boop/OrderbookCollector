import time
from datetime import datetime, timezone


def format_usd(value: float) -> str:
    """Format USD value with appropriate suffix."""
    abs_val = abs(value)
    if abs_val >= 1_000_000:
        return f"${abs_val / 1_000_000:.1f}M"
    if abs_val >= 1_000:
        return f"${abs_val / 1_000:.0f}K"
    return f"${abs_val:.0f}"


def format_usd_precise(value: float) -> str:
    """Format USD with commas."""
    return f"${value:,.0f}"


def format_btc(value: float) -> str:
    """Format BTC quantity."""
    return f"{value:.3f} BTC"


def format_price(value: float) -> str:
    """Format price with commas."""
    return f"${value:,.2f}"


def format_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 60:
        return f"{int(seconds)} сек"
    if seconds < 3600:
        mins = int(seconds / 60)
        return f"{mins} мин"
    hours = int(seconds / 3600)
    mins = int((seconds % 3600) / 60)
    if hours >= 24:
        days = int(hours / 24)
        hours = hours % 24
        return f"{days}d {hours}h {mins}m"
    return f"{hours}h {mins}m"


def format_pct(value: float) -> str:
    """Format percentage."""
    return f"{value:.1f}%"


def format_timestamp(ts: float) -> str:
    """Format unix timestamp to HH:MM:SS."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%H:%M:%S")


def delta_arrow(value: float) -> str:
    """Return colored arrow for positive/negative delta."""
    if value > 0:
        return "\U0001f7e2"  # green circle
    if value < 0:
        return "\U0001f534"  # red circle
    return "\u26aa"  # white circle


def imbalance_bar(bid_pct: float) -> str:
    """Return imbalance indicator."""
    if bid_pct > 55:
        return "\U0001f7e2 BID"
    if bid_pct < 45:
        return "\U0001f534 ASK"
    return ""


def split_text(text: str, max_length: int = 4096) -> list[str]:
    """Split text into chunks for Telegram (max 4096 chars per message)."""
    if len(text) <= max_length:
        return [text]
    chunks = []
    while text:
        if len(text) <= max_length:
            chunks.append(text)
            break
        split_pos = text.rfind("\n", 0, max_length)
        if split_pos == -1:
            split_pos = max_length
        chunks.append(text[:split_pos])
        text = text[split_pos:].lstrip("\n")
    return chunks


def get_midnight_utc() -> float:
    """Get today's midnight UTC as unix timestamp."""
    now = datetime.now(timezone.utc)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight.timestamp()


def current_minute_ts() -> int:
    """Get current minute start as unix timestamp."""
    return int(time.time()) // 60 * 60
