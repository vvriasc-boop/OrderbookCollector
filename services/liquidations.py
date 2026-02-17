import logging
from dataclasses import dataclass

import config
from database import db as database

logger = logging.getLogger("orderbook_collector")


@dataclass
class LiqEvent:
    side: str  # 'long' or 'short'
    price: float
    quantity_btc: float
    quantity_usd: float
    order_type: str
    timestamp: float


async def on_liquidation(event: dict) -> LiqEvent | None:
    """Process forceOrder event. Save if BTCUSDT, return LiqEvent if large."""
    o = event.get("o", {})
    symbol = o.get("s", "")
    if symbol != "BTCUSDT":
        return None

    side_raw = o.get("S", "")  # SELL = long liquidation, BUY = short liquidation
    side = "long" if side_raw == "SELL" else "short"
    price = float(o.get("p", "0"))
    qty = float(o.get("q", "0"))
    usd = price * qty
    order_type = o.get("o", "MARKET")
    ts = o.get("T", 0) / 1000.0

    await database.insert_liquidation({
        "timestamp": ts,
        "side": side,
        "price": price,
        "quantity_btc": qty,
        "quantity_usd": usd,
        "order_type": order_type,
    })

    if usd >= config.LIQ_ALERT_USD:
        return LiqEvent(
            side=side,
            price=price,
            quantity_btc=qty,
            quantity_usd=usd,
            order_type=order_type,
            timestamp=ts,
        )

    return None
