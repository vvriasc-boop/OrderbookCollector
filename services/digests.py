import asyncio
import time
import logging

from database import db as database
from utils.helpers import format_usd, format_price, format_pct

logger = logging.getLogger("orderbook_collector")

# Intervals in minutes
DIGEST_INTERVALS = [15, 30, 60]


def _plural_signals(n: int) -> str:
    """Russian pluralization for 'сигнал'."""
    if 11 <= n % 100 <= 19:
        return f"{n} сигналов"
    mod10 = n % 10
    if mod10 == 1:
        return f"{n} сигнал"
    if 2 <= mod10 <= 4:
        return f"{n} сигнала"
    return f"{n} сигналов"


def _plural_alerts(n: int) -> str:
    """Russian pluralization for 'алерт'."""
    if 11 <= n % 100 <= 19:
        return f"{n} алертов"
    mod10 = n % 10
    if mod10 == 1:
        return f"{n} алерт"
    if 2 <= mod10 <= 4:
        return f"{n} алерта"
    return f"{n} алертов"


def format_digest(interval_min: int, trades_rows: list, cvd_rows: list,
                  price_data: dict, imbalance_data: dict, depth_data: dict,
                  imbalance_alert_cnt: int) -> str:
    """Format digest text from DB query results.

    trades_rows: list of Row with (market, side, cnt, total_usd)
    cvd_rows: list of Row with (market, delta)
    price_data: {market: {"start": float, "end": float}}
    imbalance_data: {market: float}  (imbalance_1pct, -1..+1)
    depth_data: {market: {"bid": float, "ask": float}}
    imbalance_alert_cnt: int
    """
    lines = [f"\U0001f4ca \u0414\u0430\u0439\u0434\u0436\u0435\u0441\u0442 {interval_min} \u043c\u0438\u043d\n"]

    # --- Price change ---
    if price_data:
        lines.append("\U0001f4b0 \u0426\u0435\u043d\u0430 BTC:")
        for market in ["futures", "spot"]:
            pd = price_data.get(market)
            if not pd or not pd["start"] or not pd["end"]:
                continue
            start_p = pd["start"]
            end_p = pd["end"]
            change_pct = (end_p - start_p) / start_p * 100 if start_p > 0 else 0
            sign = "+" if change_pct >= 0 else ""
            lines.append(
                f"  {market.title()}: {format_price(start_p)} \u2192 {format_price(end_p)}"
                f" ({sign}{change_pct:.2f}%)"
            )
        lines.append("")

    # --- Trades ---
    if trades_rows:
        lines.append("\U0001f40b \u041a\u0440\u0443\u043f\u043d\u044b\u0435 \u0441\u0434\u0435\u043b\u043a\u0438:")
        total_cnt = 0
        total_usd = 0.0
        for row in trades_rows:
            market = row["market"].title()
            side = row["side"].upper()
            cnt = row["cnt"]
            vol = row["total_usd"]
            lines.append(f"  {market} {side}: {_plural_signals(cnt)}, {format_usd(vol)}")
            total_cnt += cnt
            total_usd += vol
        lines.append(f"  \u0418\u0442\u043e\u0433\u043e: {_plural_signals(total_cnt)}, {format_usd(total_usd)}")
    else:
        lines.append("\U0001f40b \u041a\u0440\u0443\u043f\u043d\u044b\u0435 \u0441\u0434\u0435\u043b\u043a\u0438: \u043d\u0435\u0442")

    lines.append("")

    # --- CVD ---
    if cvd_rows:
        lines.append("\U0001f4c8 CVD (\u0434\u0435\u043b\u044c\u0442\u0430 \u0437\u0430 \u043f\u0435\u0440\u0438\u043e\u0434):")
        for row in cvd_rows:
            market = row["market"].title()
            delta = row["delta"]
            sign = "+" if delta >= 0 else ""
            label = "\u043f\u043e\u043a\u0443\u043f\u0430\u0442\u0435\u043b\u0438" if delta >= 0 else "\u043f\u0440\u043e\u0434\u0430\u0432\u0446\u044b"
            lines.append(f"  {market}: {sign}{format_usd(delta)} ({label})")
    else:
        lines.append("\U0001f4c8 CVD: \u043d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445")

    lines.append("")

    # --- Imbalance ---
    lines.append("\u2696\ufe0f \u0414\u0438\u0441\u0431\u0430\u043b\u0430\u043d\u0441 (\u00b11%):")
    if imbalance_data:
        for market in ["futures", "spot"]:
            imb = imbalance_data.get(market)
            if imb is None:
                continue
            bid_pct = int((1 + imb) / 2 * 100)
            ask_pct = 100 - bid_pct
            if imb > 0.05:
                label = "\u043f\u0435\u0440\u0435\u0432\u0435\u0441 \u043f\u043e\u043a\u0443\u043f\u0430\u0442\u0435\u043b\u0435\u0439"
            elif imb < -0.05:
                label = "\u043f\u0435\u0440\u0435\u0432\u0435\u0441 \u043f\u0440\u043e\u0434\u0430\u0432\u0446\u043e\u0432"
            else:
                label = "\u0440\u0430\u0432\u043d\u043e\u0432\u0435\u0441\u0438\u0435"
            lines.append(f"  {market.title()}: BID {bid_pct}% / ASK {ask_pct}% ({label})")
    else:
        lines.append("  \u043d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445")

    if imbalance_alert_cnt > 0:
        lines.append(f"  \u26a0\ufe0f \u0410\u043d\u043e\u043c\u0430\u043b\u0438\u0438: {_plural_alerts(imbalance_alert_cnt)}")

    lines.append("")

    # --- Depth delta (bid vs ask) ---
    lines.append("\U0001f4ca \u0413\u043b\u0443\u0431\u0438\u043d\u0430 (\u00b11%):")
    if depth_data:
        for market in ["futures", "spot"]:
            dd = depth_data.get(market)
            if not dd:
                continue
            bid_d = dd["bid"]
            ask_d = dd["ask"]
            delta = bid_d - ask_d
            sign = "+" if delta >= 0 else ""
            lines.append(
                f"  {market.title()}: BID {format_usd(bid_d)} / ASK {format_usd(ask_d)}"
                f" (\u0394 {sign}{format_usd(delta)})"
            )
    else:
        lines.append("  \u043d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445")

    return "\n".join(lines)


async def _build_digest(interval_min: int, cutoff_ts: float) -> str:
    """Query DB and build digest text for one interval."""
    # --- Trades ---
    trades_rows = await database.fetchall(
        "SELECT market, side, COUNT(*) as cnt, SUM(quantity_usd) as total_usd "
        "FROM large_trades WHERE timestamp >= ? "
        "GROUP BY market, side ORDER BY market, side",
        (cutoff_ts,),
    )

    # --- CVD ---
    cvd_rows = await database.fetchall(
        "SELECT market, SUM(delta_usd) as delta "
        "FROM trade_aggregates_1m WHERE timestamp >= ? "
        "GROUP BY market ORDER BY market",
        (cutoff_ts,),
    )

    # --- Price at start of period (earliest snapshot per market after cutoff) ---
    price_start_rows = await database.fetchall(
        "SELECT market, mid_price FROM ob_snapshots_1m "
        "WHERE (market, timestamp) IN ("
        "  SELECT market, MIN(timestamp) FROM ob_snapshots_1m "
        "  WHERE timestamp >= ? GROUP BY market"
        ")",
        (cutoff_ts,),
    )

    # --- Latest snapshot per market (current price, imbalance, depth) ---
    latest_rows = await database.fetchall(
        "SELECT market, mid_price, imbalance_1pct, bid_depth_1pct, ask_depth_1pct "
        "FROM ob_snapshots_1m "
        "WHERE (market, timestamp) IN ("
        "  SELECT market, MAX(timestamp) FROM ob_snapshots_1m GROUP BY market"
        ")",
    )

    # --- Imbalance alerts in period ---
    imb_alert_row = await database.fetchone(
        "SELECT COUNT(*) as cnt FROM alerts_log "
        "WHERE alert_type = 'imbalance' AND timestamp >= ?",
        (cutoff_ts,),
    )
    imbalance_alert_cnt = imb_alert_row["cnt"] if imb_alert_row else 0

    # Build price_data
    price_data: dict = {}
    for row in price_start_rows:
        price_data[row["market"]] = {"start": row["mid_price"], "end": None}
    for row in latest_rows:
        m = row["market"]
        if m not in price_data:
            price_data[m] = {"start": row["mid_price"], "end": row["mid_price"]}
        else:
            price_data[m]["end"] = row["mid_price"]

    # Build imbalance_data and depth_data
    imbalance_data: dict = {}
    depth_data: dict = {}
    for row in latest_rows:
        m = row["market"]
        imbalance_data[m] = row["imbalance_1pct"]
        depth_data[m] = {
            "bid": row["bid_depth_1pct"] or 0,
            "ask": row["ask_depth_1pct"] or 0,
        }

    return format_digest(
        interval_min, trades_rows, cvd_rows,
        price_data, imbalance_data, depth_data, imbalance_alert_cnt,
    )


async def digest_loop(alert_manager):
    """Periodic digest reports every 15, 30, and 60 minutes (aligned to clock)."""
    now = time.time()
    last_run = {}
    for mins in DIGEST_INTERVALS:
        secs = mins * 60
        last_run[mins] = (int(now) // secs) * secs

    while True:
        try:
            await asyncio.sleep(30)
            now = time.time()

            for mins in DIGEST_INTERVALS:
                secs = mins * 60
                boundary = (int(now) // secs) * secs

                if boundary <= last_run[mins]:
                    continue

                # Time to report
                cutoff_ts = boundary - secs
                text = await _build_digest(mins, cutoff_ts)
                topic_key = f"digest_{mins}m"
                await alert_manager.send_digest(text, topic_key)
                last_run[mins] = boundary
                logger.info("Digest %dm sent to %s", mins, topic_key)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("digest_loop error: %s", e)
            await asyncio.sleep(5)
