import asyncio
import time
import logging
from dataclasses import dataclass

import config
from database import db as database

logger = logging.getLogger("orderbook_collector")


@dataclass
class WallInfo:
    wall_id: int
    side: str
    price_str: str
    size_btc: float
    size_usd: float
    peak_size_usd: float
    detected_at: float


@dataclass
class WallEvent:
    event_type: str  # 'new', 'cancelled', 'filled', 'partial', 'updated'
    market: str
    side: str
    price_str: str
    old_size_usd: float
    new_size_usd: float
    wall_id: int | None = None

    @property
    def price_float(self) -> float:
        return float(self.price_str)


class OrderBook:
    """Orderbook for one market (spot or futures).

    Uses asyncio.Lock for all public read/write operations.
    Private sync methods (_mid_price, _spread, etc.) are called only
    from within already-locked public methods -- never call them directly.
    """

    def __init__(self, market: str, wall_threshold_usd: float, is_futures: bool):
        self.market = market
        self.is_futures = is_futures
        self.wall_threshold_usd = wall_threshold_usd
        self.bids: dict[str, float] = {}  # price_str -> qty_btc
        self.asks: dict[str, float] = {}  # price_str -> qty_btc
        self.last_update_id: int = 0
        self.buffer: list = []
        self.ready: bool = False
        self.tracked_walls: dict[str, WallInfo] = {}  # price_str -> WallInfo
        self.lock = asyncio.Lock()
        self._last_trade_prices: list[float] = []  # recent trade prices for fill detection

    def record_trade_price(self, price: float):
        """Record recent trade price (for wall fill detection). No lock needed -- append is atomic."""
        self._last_trade_prices.append(price)
        if len(self._last_trade_prices) > 200:
            self._last_trade_prices = self._last_trade_prices[-100:]

    async def apply_snapshot(self, snapshot: dict):
        """Apply REST snapshot and process buffered events."""
        async with self.lock:
            self.bids.clear()
            self.asks.clear()
            for price_str, qty_str in snapshot["bids"]:
                qty = float(qty_str)
                if qty > 0:
                    self.bids[price_str] = qty
            for price_str, qty_str in snapshot["asks"]:
                qty = float(qty_str)
                if qty > 0:
                    self.asks[price_str] = qty
            self.last_update_id = snapshot["lastUpdateId"]
            self.ready = True
            logger.info(
                "%s snapshot applied: %d bids, %d asks, lastUpdateId=%d",
                self.market, len(self.bids), len(self.asks), self.last_update_id,
            )
            # Process buffered events
            applied = 0
            for evt in self.buffer:
                if self._should_apply_buffered(evt):
                    self._apply_diff_levels(evt)
                    applied += 1
            self.buffer.clear()
            if applied:
                logger.info("%s: applied %d buffered events", self.market, applied)

    def _should_apply_buffered(self, event: dict) -> bool:
        """Check if a buffered event should be applied after snapshot. NO LOCK."""
        u = event["u"]
        U = event["U"]

        if u <= self.last_update_id:
            return False

        if self.is_futures:
            # Futures: first event U <= lastUpdateId AND u >= lastUpdateId
            if U <= self.last_update_id and u >= self.last_update_id:
                self.last_update_id = u
                return True
            pu = event.get("pu", -1)
            if pu == self.last_update_id:
                self.last_update_id = u
                return True
        else:
            # Spot: first event U <= lastUpdateId+1 AND u >= lastUpdateId+1
            target = self.last_update_id + 1
            if U <= target and u >= target:
                self.last_update_id = u
                return True
            if U == self.last_update_id + 1:
                self.last_update_id = u
                return True
        return False

    async def apply_diff(self, event: dict) -> list[WallEvent]:
        """Apply depth diff event. Returns list of wall events."""
        async with self.lock:
            if not self.ready:
                self.buffer.append(event)
                return []

            u = event["u"]
            U = event["U"]

            if u <= self.last_update_id:
                return []

            # Continuity check
            if self.is_futures:
                pu = event.get("pu", -1)
                if self.last_update_id > 0 and pu != self.last_update_id:
                    # First valid event after snapshot
                    if not (U <= self.last_update_id and u >= self.last_update_id):
                        logger.warning(
                            "%s: gap detected pu=%d != lastU=%d, need re-snapshot",
                            self.market, pu, self.last_update_id,
                        )
                        self.ready = False
                        self.buffer.clear()
                        return []
            else:
                expected = self.last_update_id + 1
                if U != expected:
                    if not (U <= expected and u >= expected):
                        logger.warning(
                            "%s: gap detected U=%d != expected=%d, need re-snapshot",
                            self.market, U, expected,
                        )
                        self.ready = False
                        self.buffer.clear()
                        return []

            self.last_update_id = u

            wall_events = self._apply_diff_levels(event)
            return wall_events

    def _apply_diff_levels(self, event: dict) -> list[WallEvent]:
        """Apply bid/ask level updates and detect wall changes. NO LOCK."""
        wall_events = []
        mid = self._mid_price()
        if mid <= 0:
            mid = 97000.0  # fallback

        # Process bids
        for price_str, qty_str in event.get("b", []):
            qty = float(qty_str)
            old_qty = self.bids.get(price_str, 0.0)
            if qty == 0:
                self.bids.pop(price_str, None)
            else:
                self.bids[price_str] = qty

            price_f = float(price_str)
            old_usd = old_qty * price_f
            new_usd = qty * price_f
            we = self._check_wall_change(price_str, "bid", old_usd, new_usd, qty, mid)
            if we:
                wall_events.append(we)

        # Process asks
        for price_str, qty_str in event.get("a", []):
            qty = float(qty_str)
            old_qty = self.asks.get(price_str, 0.0)
            if qty == 0:
                self.asks.pop(price_str, None)
            else:
                self.asks[price_str] = qty

            price_f = float(price_str)
            old_usd = old_qty * price_f
            new_usd = qty * price_f
            we = self._check_wall_change(price_str, "ask", old_usd, new_usd, qty, mid)
            if we:
                wall_events.append(we)

        return wall_events

    def _check_wall_change(self, price_str: str, side: str,
                           old_usd: float, new_usd: float,
                           new_qty: float, mid: float) -> WallEvent | None:
        """Check if a wall appeared, disappeared or changed. NO LOCK."""
        was_wall = price_str in self.tracked_walls
        is_wall = new_usd >= self.wall_threshold_usd

        if not was_wall and is_wall:
            # New wall
            return WallEvent(
                event_type="new",
                market=self.market,
                side=side,
                price_str=price_str,
                old_size_usd=0,
                new_size_usd=new_usd,
            )
        elif was_wall and not is_wall:
            # Wall gone
            wall = self.tracked_walls[price_str]
            price_f = float(price_str)
            distance = abs(price_f - mid) / mid if mid > 0 else 1.0
            # Determine reason
            if new_qty == 0 and distance > 0.005:
                reason = "cancelled"
            elif new_qty == 0 and distance <= 0.001:
                reason = "filled"
            elif new_qty == 0:
                # Check recent trades
                if any(abs(tp - price_f) / price_f < 0.001 for tp in self._last_trade_prices[-50:]):
                    reason = "filled"
                else:
                    reason = "cancelled"
            else:
                reason = "partial"

            return WallEvent(
                event_type=reason,
                market=self.market,
                side=side,
                price_str=price_str,
                old_size_usd=wall.size_usd,
                new_size_usd=new_usd,
                wall_id=wall.wall_id,
            )
        elif was_wall and is_wall:
            # Wall updated -- update peak
            wall = self.tracked_walls[price_str]
            wall.size_usd = new_usd
            wall.size_btc = new_qty
            if new_usd > wall.peak_size_usd:
                wall.peak_size_usd = new_usd
            return None

        return None

    async def register_wall(self, price_str: str, side: str, size_btc: float,
                            size_usd: float, wall_id: int, detected_at: float):
        """Register a wall in tracked_walls. Called after DB insert."""
        async with self.lock:
            self.tracked_walls[price_str] = WallInfo(
                wall_id=wall_id,
                side=side,
                price_str=price_str,
                size_btc=size_btc,
                size_usd=size_usd,
                peak_size_usd=size_usd,
                detected_at=detected_at,
            )

    async def unregister_wall(self, price_str: str):
        """Remove wall from tracked_walls. Called after DB status update."""
        async with self.lock:
            self.tracked_walls.pop(price_str, None)

    async def get_snapshot_metrics(self) -> dict | None:
        """Compute aggregated metrics for ob_snapshots_1m."""
        async with self.lock:
            mid = self._mid_price()
            if mid <= 0:
                return None
            spread = self._spread()

            ranges = [0.001, 0.005, 0.01, 0.02, 0.05]
            range_labels = ["01pct", "05pct", "1pct", "2pct", "5pct"]
            bid_depths = {}
            ask_depths = {}
            imbalances = {}

            for rng, label in zip(ranges, range_labels):
                low = mid * (1 - rng)
                high = mid * (1 + rng)

                bid_sum = sum(
                    float(p) * q for p, q in self.bids.items()
                    if low <= float(p) <= mid
                )
                ask_sum = sum(
                    float(p) * q for p, q in self.asks.items()
                    if mid <= float(p) <= high
                )

                bid_depths[f"bid_depth_{label}"] = bid_sum
                ask_depths[f"ask_depth_{label}"] = ask_sum

                total = bid_sum + ask_sum
                if total > 0:
                    imbalances[f"imbalance_{label}"] = (bid_sum - ask_sum) / total
                else:
                    imbalances[f"imbalance_{label}"] = 0.0

            wall_bid = sum(1 for w in self.tracked_walls.values() if w.side == "bid")
            wall_ask = sum(1 for w in self.tracked_walls.values() if w.side == "ask")

            result = {
                "mid_price": mid,
                "spread_pct": spread,
                "wall_count_bid": wall_bid,
                "wall_count_ask": wall_ask,
            }
            result.update(bid_depths)
            result.update(ask_depths)
            result.update(imbalances)
            return result

    async def get_status(self) -> dict:
        """Status info for Telegram commands."""
        async with self.lock:
            return {
                "mid": self._mid_price(),
                "spread": self._spread(),
                "bid_levels": len(self.bids),
                "ask_levels": len(self.asks),
                "walls_bid": sum(1 for w in self.tracked_walls.values() if w.side == "bid"),
                "walls_ask": sum(1 for w in self.tracked_walls.values() if w.side == "ask"),
                "ready": self.ready,
                "last_update_id": self.last_update_id,
            }

    async def get_walls_list(self) -> list[dict]:
        """Get list of active walls for display."""
        async with self.lock:
            mid = self._mid_price()
            walls = []
            for w in self.tracked_walls.values():
                price_f = float(w.price_str)
                distance = ((price_f - mid) / mid * 100) if mid > 0 else 0
                walls.append({
                    "side": w.side,
                    "price": price_f,
                    "price_str": w.price_str,
                    "size_usd": w.size_usd,
                    "peak_usd": w.peak_size_usd,
                    "distance_pct": distance,
                    "age_sec": time.time() - w.detected_at,
                })
            return walls

    async def check_wall_exists(self, price_str: str) -> dict | None:
        """Check if a wall still exists and return its current data. For confirmed wall checker."""
        async with self.lock:
            w = self.tracked_walls.get(price_str)
            if not w:
                return None
            mid = self._mid_price()
            price_f = float(price_str)
            return {
                "size_usd": w.size_usd,
                "size_btc": w.size_btc,
                "mid_price": mid,
                "distance_pct": (price_f - mid) / mid * 100 if mid > 0 else 999,
            }

    async def get_depth_display(self) -> dict:
        """Get depth data for /depth command."""
        async with self.lock:
            mid = self._mid_price()
            if mid <= 0:
                return {"mid": 0, "spread": 0, "ranges": []}
            spread = self._spread()
            ranges_pct = [0.001, 0.005, 0.01, 0.02, 0.05]
            range_labels = ["\u00b10.1%", "\u00b10.5%", "\u00b11.0%", "\u00b12.0%", "\u00b15.0%"]
            ranges = []
            for rng, label in zip(ranges_pct, range_labels):
                low = mid * (1 - rng)
                high = mid * (1 + rng)
                bid_sum = sum(float(p) * q for p, q in self.bids.items() if low <= float(p) <= mid)
                ask_sum = sum(float(p) * q for p, q in self.asks.items() if mid <= float(p) <= high)
                total = bid_sum + ask_sum
                bid_pct = (bid_sum / total * 100) if total > 0 else 50
                ask_pct = 100 - bid_pct
                ranges.append({
                    "label": label,
                    "bid_usd": bid_sum,
                    "ask_usd": ask_sum,
                    "bid_pct": bid_pct,
                    "ask_pct": ask_pct,
                })
            return {"mid": mid, "spread": spread, "ranges": ranges}

    async def prune_distant_levels(self):
        """Remove levels further than 50% from mid_price."""
        async with self.lock:
            mid = self._mid_price()
            if mid <= 0:
                return
            low_f = mid * (1 - config.OB_PRUNE_DISTANCE_PCT)
            high_f = mid * (1 + config.OB_PRUNE_DISTANCE_PCT)
            before_bids = len(self.bids)
            before_asks = len(self.asks)
            self.bids = {p: q for p, q in self.bids.items() if low_f <= float(p) <= high_f}
            self.asks = {p: q for p, q in self.asks.items() if low_f <= float(p) <= high_f}
            pruned = (before_bids - len(self.bids)) + (before_asks - len(self.asks))
            if pruned > 0:
                logger.debug("%s: pruned %d distant levels", self.market, pruned)

    async def invalidate(self):
        """Mark orderbook as not ready (needs re-snapshot)."""
        async with self.lock:
            self.ready = False
            self.buffer.clear()
            logger.warning("%s: orderbook invalidated, needs re-snapshot", self.market)

    async def is_ready(self) -> bool:
        async with self.lock:
            return self.ready

    # --- Private methods (NO LOCK, called from within locked context) ---

    def _mid_price(self) -> float:
        if not self.bids or not self.asks:
            return 0.0
        best_bid = max(float(p) for p in self.bids)
        best_ask = min(float(p) for p in self.asks)
        return (best_bid + best_ask) / 2

    def _spread(self) -> float:
        if not self.bids or not self.asks:
            return 0.0
        best_bid = max(float(p) for p in self.bids)
        best_ask = min(float(p) for p in self.asks)
        mid = (best_bid + best_ask) / 2
        if mid <= 0:
            return 0.0
        return (best_ask - best_bid) / mid * 100
