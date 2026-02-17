import time
import logging
from dataclasses import dataclass

import config
from database import db as database
from utils.helpers import current_minute_ts, get_midnight_utc

logger = logging.getLogger("orderbook_collector")


@dataclass
class LargeTradeEvent:
    market: str
    side: str
    price: float
    quantity_btc: float
    quantity_usd: float
    is_maker_buy: bool
    timestamp: float


class TradeBucket:
    """In-memory accumulator for 1-minute trade aggregates."""

    def __init__(self):
        self.buy_volume_usd: float = 0.0
        self.sell_volume_usd: float = 0.0
        self.buy_count: int = 0
        self.sell_count: int = 0
        self.max_trade_usd: float = 0.0
        self.total_price_volume: float = 0.0  # for VWAP
        self.total_volume: float = 0.0  # for VWAP

    def add(self, side: str, price: float, qty_btc: float, usd: float):
        if side == "buy":
            self.buy_volume_usd += usd
            self.buy_count += 1
        else:
            self.sell_volume_usd += usd
            self.sell_count += 1
        if usd > self.max_trade_usd:
            self.max_trade_usd = usd
        self.total_price_volume += price * qty_btc
        self.total_volume += qty_btc

    @property
    def delta(self) -> float:
        return self.buy_volume_usd - self.sell_volume_usd

    @property
    def vwap(self) -> float:
        if self.total_volume > 0:
            return self.total_price_volume / self.total_volume
        return 0.0

    def reset(self):
        self.buy_volume_usd = 0.0
        self.sell_volume_usd = 0.0
        self.buy_count = 0
        self.sell_count = 0
        self.max_trade_usd = 0.0
        self.total_price_volume = 0.0
        self.total_volume = 0.0


class TradeAggregator:
    """Aggregates trades into 1-minute buckets."""

    def __init__(self, market: str):
        self.market = market
        self.current_minute: int = current_minute_ts()
        self.bucket = TradeBucket()
        self.cvd_today: float = 0.0

    async def on_trade(self, event: dict) -> LargeTradeEvent | None:
        """Process aggTrade event. Returns LargeTradeEvent if trade is large enough for alert."""
        price = float(event["p"])
        qty = float(event["q"])
        usd = price * qty
        is_maker_buy = event["m"]  # m=true -> buyer is maker -> sell aggressor
        side = "sell" if is_maker_buy else "buy"
        ts = event["T"] / 1000.0

        # Check minute boundary
        minute_ts = int(ts) // 60 * 60
        if minute_ts > self.current_minute:
            await self.flush_bucket()
            self.current_minute = minute_ts

        # Add to bucket
        self.bucket.add(side, price, qty, usd)

        result = None

        # Large trade -> DB
        if usd >= config.LARGE_TRADE_THRESHOLD_USD:
            await database.insert_large_trade({
                "timestamp": ts,
                "market": self.market,
                "side": side,
                "price": price,
                "quantity_btc": qty,
                "quantity_usd": usd,
                "is_maker_buy": 1 if is_maker_buy else 0,
            })

            # Alert-worthy?
            if usd >= config.LARGE_TRADE_ALERT_USD:
                result = LargeTradeEvent(
                    market=self.market,
                    side=side,
                    price=price,
                    quantity_btc=qty,
                    quantity_usd=usd,
                    is_maker_buy=is_maker_buy,
                    timestamp=ts,
                )

        return result

    async def flush_bucket(self):
        """Flush current bucket to DB."""
        if self.bucket.buy_count == 0 and self.bucket.sell_count == 0:
            return

        delta = self.bucket.delta
        self.cvd_today += delta

        data = {
            "timestamp": self.current_minute,
            "market": self.market,
            "buy_volume_usd": self.bucket.buy_volume_usd,
            "sell_volume_usd": self.bucket.sell_volume_usd,
            "buy_count": self.bucket.buy_count,
            "sell_count": self.bucket.sell_count,
            "delta_usd": delta,
            "cvd_usd": self.cvd_today,
            "max_trade_usd": self.bucket.max_trade_usd,
            "vwap": self.bucket.vwap,
        }
        await database.insert_trade_aggregate(data)
        self.bucket.reset()

    def reset_cvd(self):
        """Reset CVD at midnight UTC."""
        self.cvd_today = 0.0
        logger.info("%s: CVD reset to 0", self.market)

    async def recover_cvd(self):
        """Recover CVD from DB on restart."""
        midnight = get_midnight_utc()
        row = await database.fetchone(
            "SELECT SUM(delta_usd) as total FROM trade_aggregates_1m WHERE timestamp >= ? AND market = ?",
            (int(midnight), self.market),
        )
        if row and row["total"]:
            self.cvd_today = row["total"]
            logger.info("%s: CVD recovered = %.0f", self.market, self.cvd_today)

    async def get_cvd_stats(self) -> dict:
        """Get CVD stats for display."""
        now = int(time.time())
        hour_ago = now - 3600
        five_min_ago = now - 300

        row_hour = await database.fetchone(
            "SELECT SUM(delta_usd) as d FROM trade_aggregates_1m WHERE timestamp >= ? AND market = ?",
            (hour_ago, self.market),
        )
        row_5m = await database.fetchone(
            "SELECT SUM(delta_usd) as d FROM trade_aggregates_1m WHERE timestamp >= ? AND market = ?",
            (five_min_ago, self.market),
        )

        return {
            "cvd_today": self.cvd_today,
            "cvd_1h": row_hour["d"] if row_hour and row_hour["d"] else 0.0,
            "cvd_5m": row_5m["d"] if row_5m and row_5m["d"] else 0.0,
        }
