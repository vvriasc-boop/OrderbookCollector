import asyncio
import time
import logging
from datetime import datetime, timezone

import config
from database import db as database
from utils.helpers import current_minute_ts, get_midnight_utc

logger = logging.getLogger("orderbook_collector")


async def fetch_rest_snapshot(market: str) -> dict | None:
    """Fetch REST orderbook snapshot. Uses config.http_session."""
    if market == "futures":
        url = config.FUTURES_REST_DEPTH
    else:
        url = config.SPOT_REST_DEPTH

    for attempt in range(3):
        try:
            async with config.http_session.get(url) as resp:
                if resp.status == 200:
                    import ujson
                    data = await resp.text()
                    return ujson.loads(data)
                else:
                    logger.warning(
                        "REST snapshot %s: HTTP %d (attempt %d)",
                        market, resp.status, attempt + 1,
                    )
        except Exception as e:
            logger.error("REST snapshot %s error (attempt %d): %s", market, attempt + 1, e)

        delay = 2 ** (attempt + 1)
        await asyncio.sleep(delay)

    logger.error("REST snapshot %s: all 3 attempts failed", market)
    return None


async def periodic_snapshot_loop(ob_futures, ob_spot, alert_manager,
                                 trade_agg_futures, trade_agg_spot):
    """Every 60s: prune levels, save OB metrics, check imbalance/CVD."""
    _cvd_reset_done_today = False

    while True:
        try:
            await asyncio.sleep(config.SNAPSHOT_INTERVAL_SEC)

            now_ts = current_minute_ts()

            # Prune distant levels
            await ob_futures.prune_distant_levels()
            await ob_spot.prune_distant_levels()

            # Save OB snapshots
            for ob in [ob_futures, ob_spot]:
                if not await ob.is_ready():
                    continue
                metrics = await ob.get_snapshot_metrics()
                if metrics:
                    metrics["timestamp"] = now_ts
                    metrics["market"] = ob.market
                    await database.insert_ob_snapshot(metrics)

                    # Check imbalance
                    imb_1pct = metrics.get("imbalance_1pct", 0)
                    if abs(imb_1pct) > config.IMBALANCE_ALERT_THRESHOLD:
                        await alert_manager.process_imbalance(imb_1pct, ob.market)

            # Confirmed wall check ($5M+, ±2%, stood >= 1 min)
            for ob in [ob_futures, ob_spot]:
                confirmed = await ob.get_walls_for_confirmed_alert()
                for cw in confirmed:
                    await alert_manager.process_confirmed_wall(cw)

            # CVD spike check (last 5 minutes)
            five_min_ago = now_ts - 300
            for market, agg in [("futures", trade_agg_futures), ("spot", trade_agg_spot)]:
                row = await database.fetchone(
                    "SELECT SUM(delta_usd) as d FROM trade_aggregates_1m "
                    "WHERE timestamp >= ? AND market = ?",
                    (five_min_ago, market),
                )
                if row and row["d"] and abs(row["d"]) > config.CVD_SPIKE_THRESHOLD_USD:
                    await alert_manager.process_cvd_spike(row["d"], market)

            # CVD midnight reset
            now_utc = datetime.now(timezone.utc)
            if now_utc.hour == config.CVD_RESET_HOUR_UTC and not _cvd_reset_done_today:
                trade_agg_futures.reset_cvd()
                trade_agg_spot.reset_cvd()
                _cvd_reset_done_today = True
                logger.info("CVD reset at midnight UTC")
            elif now_utc.hour != config.CVD_RESET_HOUR_UTC:
                _cvd_reset_done_today = False

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("periodic_snapshot_loop error: %s", e)
            await asyncio.sleep(5)


async def periodic_rest_refresh(ob_futures, ob_spot):
    """Hourly REST snapshot refresh (drift protection)."""
    while True:
        try:
            await asyncio.sleep(config.REST_SNAPSHOT_INTERVAL_SEC)

            for ob, market in [(ob_futures, "futures"), (ob_spot, "spot")]:
                snap = await fetch_rest_snapshot(market)
                if snap:
                    await ob.apply_snapshot(snap)
                    logger.info("%s: periodic REST refresh done", market)
                await asyncio.sleep(1)  # rate limit

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("periodic_rest_refresh error: %s", e)
            await asyncio.sleep(60)


async def periodic_archive_cleanup():
    """Daily at 04:00 UTC: delete old data, periodic VACUUM."""
    last_vacuum = time.time()

    while True:
        try:
            await asyncio.sleep(60)
            now_utc = datetime.now(timezone.utc)
            if now_utc.hour != 4 or now_utc.minute != 0:
                continue

            cutoff = time.time() - config.ARCHIVE_AFTER_DAYS * 86400
            tables = [
                ("large_trades", "timestamp"),
                ("liquidations", "timestamp"),
                ("trade_aggregates_1m", "timestamp"),
                ("ob_snapshots_1m", "timestamp"),
                ("alerts_log", "timestamp"),
            ]

            for table, col in tables:
                result = await database.execute(
                    f"DELETE FROM {table} WHERE {col} < ?", (cutoff,)
                )
                logger.info("Archive cleanup: %s done", table)

            # Walls: only delete ended walls
            await database.execute(
                "DELETE FROM orderbook_walls WHERE ended_at IS NOT NULL AND ended_at < ?",
                (cutoff,),
            )
            logger.info("Archive cleanup: orderbook_walls done")

            # VACUUM periodically
            if time.time() - last_vacuum > config.DB_VACUUM_INTERVAL_DAYS * 86400:
                await database.execute("VACUUM")
                last_vacuum = time.time()
                logger.info("Database VACUUM completed")

            # Wait to avoid re-triggering in same minute
            await asyncio.sleep(60)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("periodic_archive_cleanup error: %s", e)
            await asyncio.sleep(300)
