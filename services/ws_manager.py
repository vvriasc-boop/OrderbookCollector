import asyncio
import time
import logging
from typing import Callable, Awaitable

import aiohttp
import ujson

import config
from services.snapshots import fetch_rest_snapshot

logger = logging.getLogger("orderbook_collector")


class WSManager:
    """Manages 2 WebSocket connections (Futures + Spot) with auto-reconnect."""

    def __init__(
        self,
        on_depth: Callable,
        on_trade: Callable,
        on_liquidation: Callable,
        on_snapshot_needed: Callable,
    ):
        self.on_depth = on_depth
        self.on_trade = on_trade
        self.on_liquidation = on_liquidation
        self.on_snapshot_needed = on_snapshot_needed
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self.futures_connected = False
        self.spot_connected = False
        self.futures_uptime_start: float = 0
        self.spot_uptime_start: float = 0
        self.last_message_time: dict[str, float] = {"futures": 0, "spot": 0}

    async def start(self):
        """Start both connections in parallel."""
        self._running = True
        self._tasks = [
            asyncio.create_task(
                self._run_connection(config.FUTURES_WS_URL, "futures"),
                name="ws-futures",
            ),
            asyncio.create_task(
                self._run_connection(config.SPOT_WS_URL, "spot"),
                name="ws-spot",
            ),
            asyncio.create_task(self._silence_watchdog(), name="ws-watchdog"),
        ]

    async def stop(self):
        """Graceful shutdown."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _run_connection(self, url: str, market: str):
        """Single WebSocket with auto-reconnect and exponential backoff."""
        delay = config.WS_RECONNECT_DELAY_SEC
        first_message_received = False

        while self._running:
            try:
                logger.info("%s: connecting to WebSocket...", market)
                ws = await config.http_session.ws_connect(
                    url,
                    heartbeat=config.WS_PING_INTERVAL_SEC,
                    receive_timeout=config.WS_SILENCE_TIMEOUT_SEC + 10,
                )

                if market == "futures":
                    self.futures_connected = True
                    self.futures_uptime_start = time.time()
                else:
                    self.spot_connected = True
                    self.spot_uptime_start = time.time()

                logger.info("%s: WebSocket connected", market)
                first_message_received = False

                # Request snapshot after connection
                await self.on_snapshot_needed(market)

                async for msg in ws:
                    if not self._running:
                        break

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self.last_message_time[market] = time.time()

                        if not first_message_received:
                            first_message_received = True
                            delay = config.WS_RECONNECT_DELAY_SEC  # reset backoff

                        try:
                            raw = ujson.loads(msg.data)
                            stream_name = raw.get("stream", "")
                            event_data = raw.get("data", {})

                            if "depth" in stream_name:
                                await self.on_depth(event_data, market)
                            elif "aggTrade" in stream_name:
                                await self.on_trade(event_data, market)
                            elif "forceOrder" in stream_name:
                                await self.on_liquidation(event_data)
                        except Exception as e:
                            logger.error("%s: message processing error: %s", market, e)

                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error("%s: WS error: %s", market, ws.exception())
                        break
                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                        logger.warning("%s: WS closed", market)
                        break

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("%s: WS connection error: %s", market, e)

            # Mark disconnected
            if market == "futures":
                self.futures_connected = False
            else:
                self.spot_connected = False

            if not self._running:
                break

            logger.info("%s: reconnecting in %d sec...", market, delay)
            await asyncio.sleep(delay)
            # Exponential backoff
            delay = min(delay * 2, config.WS_RECONNECT_MAX_DELAY_SEC)

    async def _silence_watchdog(self):
        """Monitor for silence on WebSocket connections."""
        while self._running:
            try:
                await asyncio.sleep(10)
                now = time.time()

                for market in ["futures", "spot"]:
                    last = self.last_message_time.get(market, 0)
                    if last > 0 and (now - last) > config.WS_SILENCE_TIMEOUT_SEC:
                        connected = (
                            self.futures_connected if market == "futures"
                            else self.spot_connected
                        )
                        if connected:
                            logger.warning(
                                "%s: no data for %.0f sec, forcing reconnect",
                                market, now - last,
                            )
                            # Cancel the connection task to trigger reconnect
                            for task in self._tasks:
                                if task.get_name() == f"ws-{market}":
                                    task.cancel()
                                    break

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Silence watchdog error: %s", e)

    def get_status(self) -> dict:
        """Get connection status."""
        now = time.time()
        return {
            "futures_connected": self.futures_connected,
            "spot_connected": self.spot_connected,
            "futures_uptime": now - self.futures_uptime_start if self.futures_connected else 0,
            "spot_uptime": now - self.spot_uptime_start if self.spot_connected else 0,
        }
