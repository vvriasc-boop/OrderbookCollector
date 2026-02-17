import asyncio
import signal
import time
import logging
from logging.handlers import RotatingFileHandler

from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler

import config
from database import db as database
from services.orderbook import OrderBook, WallEvent, WallInfo
from services.trades import TradeAggregator
from services.liquidations import on_liquidation
from services.alerts import AlertManager
from services.ws_manager import WSManager
from services.snapshots import (
    fetch_rest_snapshot,
    periodic_snapshot_loop,
    periodic_rest_refresh,
    periodic_archive_cleanup,
)
from handlers.commands import (
    cmd_start, cmd_status, cmd_walls, cmd_trades,
    cmd_liq, cmd_cvd, cmd_depth, cmd_stats, cmd_notify, cmd_help,
)
from handlers.callbacks import callback_router

# --- Logging ---
logger = logging.getLogger("orderbook_collector")
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler("bot.log", maxBytes=10 * 1024 * 1024, backupCount=5)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())


async def main():
    logger.info("=== START ===")

    # 0. Validate required config
    if not config.TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN is not set in .env")
        return
    if not config.ADMIN_USER_ID:
        logger.error("ADMIN_USER_ID is not set in .env")
        return

    # 1. Init DB
    db = database.init_database("data.db")
    logger.info("Database initialized")

    # 2. Init HTTP session
    proxy = config.PROXY_URL or None
    await config.init_http(proxy)
    logger.info("HTTP session initialized (proxy=%s)", "yes" if proxy else "no")

    # 3. Build Telegram application
    try:
        app = ApplicationBuilder().token(config.TELEGRAM_BOT_TOKEN).build()
    except Exception as e:
        logger.error("Failed to build Telegram app: %s", e)
        await config.close_http()
        database.close_database()
        return

    # 4. Init AlertManager
    alert_manager = AlertManager(app.bot, config.ADMIN_USER_ID, config.ALERT_COOLDOWN_SEC)
    alert_manager.start()

    # 5. Init OrderBooks
    ob_futures = OrderBook("futures", config.WALL_THRESHOLD_USD, is_futures=True)
    ob_spot = OrderBook("spot", config.WALL_THRESHOLD_USD, is_futures=False)

    # 6. State recovery: load active walls from DB
    active_walls = await database.get_active_walls()
    for w in active_walls:
        ob = ob_futures if w["market"] == "futures" else ob_spot
        await ob.register_wall(
            price_str=w["price"],
            side=w["side"],
            size_btc=w["size_btc"],
            size_usd=w["size_usd"],
            wall_id=w["id"],
            detected_at=w["detected_at"],
        )
    logger.info("Recovered %d active walls from DB", len(active_walls))

    # 7. Init TradeAggregators
    trade_agg_futures = TradeAggregator("futures")
    trade_agg_spot = TradeAggregator("spot")
    await trade_agg_futures.recover_cvd()
    await trade_agg_spot.recover_cvd()

    # --- WS event callbacks ---

    async def handle_depth(event: dict, market: str):
        ob = ob_futures if market == "futures" else ob_spot
        wall_events = await ob.apply_diff(event)
        for we in wall_events:
            await _process_wall_event(we, ob)

    async def handle_trade(event: dict, market: str):
        agg = trade_agg_futures if market == "futures" else trade_agg_spot
        ob = ob_futures if market == "futures" else ob_spot

        # Record trade price for wall fill detection
        try:
            ob.record_trade_price(float(event["p"]))
        except (ValueError, KeyError):
            pass

        result = await agg.on_trade(event)
        if result:
            await alert_manager.process_large_trade(result)

    async def handle_liquidation(event: dict):
        result = await on_liquidation(event)
        if result:
            await alert_manager.process_liquidation(result)

    async def handle_snapshot_needed(market: str):
        """Called when WS connects/reconnects — fetch REST snapshot."""
        ob = ob_futures if market == "futures" else ob_spot
        snap = await fetch_rest_snapshot(market)
        if snap:
            await ob.apply_snapshot(snap)
            logger.info("%s: REST snapshot applied on connect", market)
        else:
            logger.error("%s: failed to get REST snapshot on connect", market)

    async def _process_wall_event(we: WallEvent, ob: OrderBook):
        """Process a wall event: update DB, tracking, send alert."""
        if we.event_type == "new":
            mid = (await ob.get_status())["mid"]
            price_f = we.price_float
            distance = ((price_f - mid) / mid * 100) if mid > 0 else 0
            wall_id = await database.insert_wall({
                "detected_at": time.time(),
                "market": we.market,
                "side": we.side,
                "price": we.price_str,
                "size_btc": we.new_size_usd / price_f if price_f > 0 else 0,
                "size_usd": we.new_size_usd,
                "price_at_detection": mid,
                "distance_pct": distance,
            })
            await ob.register_wall(
                price_str=we.price_str,
                side=we.side,
                size_btc=we.new_size_usd / price_f if price_f > 0 else 0,
                size_usd=we.new_size_usd,
                wall_id=wall_id,
                detected_at=time.time(),
            )
            # Alert only for big walls
            if we.new_size_usd >= config.WALL_ALERT_USD:
                await alert_manager.process_wall_event(we)

        elif we.event_type in ("cancelled", "filled", "partial"):
            if we.wall_id:
                mid = (await ob.get_status())["mid"]
                await database.update_wall_status(
                    we.wall_id, we.event_type, we.event_type, mid,
                )
            await ob.unregister_wall(we.price_str)

            # Alert only for significant walls
            if we.old_size_usd >= config.WALL_CANCEL_ALERT_USD:
                await alert_manager.process_wall_event(we)

    # 8. Init WSManager
    ws_manager = WSManager(
        on_depth=handle_depth,
        on_trade=handle_trade,
        on_liquidation=handle_liquidation,
        on_snapshot_needed=handle_snapshot_needed,
    )

    # Store shared context for Telegram handlers
    app.bot_data["ob_futures"] = ob_futures
    app.bot_data["ob_spot"] = ob_spot
    app.bot_data["ws_manager"] = ws_manager
    app.bot_data["alert_manager"] = alert_manager
    app.bot_data["trade_agg_futures"] = trade_agg_futures
    app.bot_data["trade_agg_spot"] = trade_agg_spot

    # Register Telegram handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("walls", cmd_walls))
    app.add_handler(CommandHandler("trades", cmd_trades))
    app.add_handler(CommandHandler("liq", cmd_liq))
    app.add_handler(CommandHandler("cvd", cmd_cvd))
    app.add_handler(CommandHandler("depth", cmd_depth))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("notify", cmd_notify))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CallbackQueryHandler(callback_router))

    # 9. Start WebSocket (will buffer events until snapshot)
    await ws_manager.start()
    logger.info("WebSocket connections started")

    # 10. Start periodic tasks
    periodic_tasks = [
        asyncio.create_task(
            periodic_snapshot_loop(ob_futures, ob_spot, alert_manager,
                                   trade_agg_futures, trade_agg_spot),
            name="snapshot-loop",
        ),
        asyncio.create_task(periodic_rest_refresh(ob_futures, ob_spot), name="rest-refresh"),
        asyncio.create_task(periodic_archive_cleanup(), name="archive-cleanup"),
        asyncio.create_task(_healthcheck_loop(ws_manager, ob_futures, ob_spot, alert_manager),
                           name="healthcheck"),
    ]

    # 11. Start Telegram bot
    try:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("Telegram bot started")
    except Exception as e:
        logger.error("Failed to start Telegram bot: %s", e)
        await ws_manager.stop()
        for t in periodic_tasks:
            t.cancel()
        await asyncio.gather(*periodic_tasks, return_exceptions=True)
        await alert_manager.stop()
        await config.close_http()
        database.close_database()
        return

    # 12. Wait for shutdown signal
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, stop.set)

    await stop.wait()
    logger.info("Shutdown signal received")

    # --- Shutdown ---
    # 1. Stop WS
    await ws_manager.stop()
    logger.info("WebSocket stopped")

    # 2. Flush trade buckets
    await trade_agg_futures.flush_bucket()
    await trade_agg_spot.flush_bucket()
    logger.info("Trade buckets flushed")

    # 3. Mark active walls as unknown
    await database.mark_walls_unknown()
    logger.info("Active walls marked as unknown")

    # 4. Cancel periodic tasks
    for t in periodic_tasks:
        t.cancel()
    await asyncio.gather(*periodic_tasks, return_exceptions=True)

    # 5. Stop alerts
    await alert_manager.stop()

    # 6. Stop Telegram
    await app.updater.stop()
    await app.stop()
    await app.shutdown()

    # 7. Close HTTP session
    await config.close_http()

    # 8. Close DB
    database.close_database()

    logger.info("=== STOP ===")


async def _healthcheck_loop(ws_manager, ob_futures, ob_spot, alert_manager):
    """Every 5 min: check WS alive, OB synchronized, DB accessible."""
    consecutive_failures = 0

    while True:
        try:
            await asyncio.sleep(300)
            issues = []
            now = time.time()

            # WS alive?
            ws_status = ws_manager.get_status()
            if not ws_status["futures_connected"]:
                issues.append("Futures WS disconnected")
            if not ws_status["spot_connected"]:
                issues.append("Spot WS disconnected")

            # OB synchronized?
            for ob, label in [(ob_futures, "Futures"), (ob_spot, "Spot")]:
                st = await ob.get_status()
                if not st["ready"]:
                    issues.append(f"{label} OB not ready")
                elif st["bid_levels"] < 100:
                    issues.append(f"{label} OB: only {st['bid_levels']} bid levels")

            # DB accessible?
            try:
                await database.fetchone("SELECT 1")
            except Exception as e:
                issues.append(f"DB error: {e}")

            if issues:
                consecutive_failures += 1
                msg = "\u26a0\ufe0f Healthcheck issues:\n" + "\n".join(f"  - {i}" for i in issues)
                logger.warning(msg)
                if consecutive_failures >= 3:
                    await alert_manager.send_admin_message(msg)
                    consecutive_failures = 0
            else:
                consecutive_failures = 0

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Healthcheck error: %s", e)


if __name__ == "__main__":
    asyncio.run(main())
