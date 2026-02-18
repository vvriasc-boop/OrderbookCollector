import asyncio
import time
import logging

from database import db as database
from utils.helpers import format_usd, format_price

logger = logging.getLogger("orderbook_collector")

# Intervals in minutes
DIGEST_INTERVALS = [15, 30, 60]

# Distance bands for fresh walls breakdown
DEPTH_BANDS = [
    (1, "±1%", 0, 1),
    (2, "±2%", 1, 2),
    (5, "±5%", 2, 5),
]


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


def _delta_line(buy_usd: float, sell_usd: float) -> str:
    """Format delta line: Δ = BUY - SELL."""
    delta = buy_usd - sell_usd
    sign = "+" if delta >= 0 else ""
    label = "покупатели" if delta >= 0 else "продавцы"
    return f"  Δ {sign}{format_usd(delta)} ({label})"


def format_digest(interval_min: int, trades_rows: list, walls_rows: list,
                  cvd_rows: list, price_data: dict, imbalance_data: dict,
                  imbalance_alert_cnt: int, fresh_walls_rows: list) -> str:
    """Format digest text from DB query results."""
    lines = [f"📊 Дайджест {interval_min} мин\n"]

    # --- Price change (futures only) ---
    pd = price_data.get("futures")
    if pd and pd["start"] and pd["end"]:
        start_p = pd["start"]
        end_p = pd["end"]
        change_pct = (end_p - start_p) / start_p * 100 if start_p > 0 else 0
        sign = "+" if change_pct >= 0 else ""
        lines.append(
            f"💰 Цена BTC: {format_price(start_p)} → {format_price(end_p)}"
            f" ({sign}{change_pct:.2f}%)"
        )
        lines.append("")

    # --- Trades ---
    if trades_rows:
        lines.append("🐋 Крупные сделки:")
        total_cnt = 0
        total_usd = 0.0
        market_sides: dict[str, dict[str, float]] = {}
        for row in trades_rows:
            market = row["market"]
            side = row["side"]
            cnt = row["cnt"]
            vol = row["total_usd"]
            lines.append(f"  {market.title()} {side.upper()}: {_plural_signals(cnt)}, {format_usd(vol)}")
            total_cnt += cnt
            total_usd += vol
            market_sides.setdefault(market, {"buy": 0.0, "sell": 0.0})
            market_sides[market][side] = vol
        lines.append(f"  Итого: {_plural_signals(total_cnt)}, {format_usd(total_usd)}")
        for market in sorted(market_sides):
            ms = market_sides[market]
            lines.append(f"  {market.title()} {_delta_line(ms.get('buy', 0), ms.get('sell', 0)).strip()}")
    else:
        lines.append("🐋 Крупные сделки: нет")

    lines.append("")

    # --- Walls (orderbook) ---
    if walls_rows:
        lines.append("🧱 Стакан (стены):")
        total_cnt = 0
        total_usd = 0.0
        market_sides_w: dict[str, dict[str, float]] = {}
        for row in walls_rows:
            market = row["market"]
            side = row["side"]
            cnt = row["cnt"]
            vol = row["total_usd"]
            lines.append(f"  {market.title()} {side.upper()}: {_plural_signals(cnt)}, {format_usd(vol)}")
            total_cnt += cnt
            total_usd += vol
            market_sides_w.setdefault(market, {"bid": 0.0, "ask": 0.0})
            market_sides_w[market][side] = vol
        lines.append(f"  Итого: {_plural_signals(total_cnt)}, {format_usd(total_usd)}")
        for market in sorted(market_sides_w):
            ms = market_sides_w[market]
            lines.append(f"  {market.title()} {_delta_line(ms.get('bid', 0), ms.get('ask', 0)).strip()}")
    else:
        lines.append("🧱 Стакан (стены): нет")

    lines.append("")

    # --- Fresh walls by depth band (±1%, ±2%, ±5%) ---
    # Rows have: depth_band, market, side, cnt, total_usd
    if fresh_walls_rows:
        lines.append("📏 Фреши по глубине (≥60 сек):")
        # Group by band
        bands: dict[str, list] = {}
        for row in fresh_walls_rows:
            band = row["depth_band"]
            bands.setdefault(band, []).append(row)

        for _order, label, _lo, _hi in DEPTH_BANDS:
            band_key = str(_order)
            band_rows = bands.get(band_key, [])
            if not band_rows:
                lines.append(f"  {label}: нет")
                continue
            lines.append(f"  {label}:")
            market_sides_f: dict[str, dict[str, float]] = {}
            for row in band_rows:
                market = row["market"]
                side = row["side"]
                cnt = row["cnt"]
                vol = row["total_usd"]
                lines.append(f"    {market.title()} {side.upper()}: {_plural_signals(cnt)}, {format_usd(vol)}")
                market_sides_f.setdefault(market, {"bid": 0.0, "ask": 0.0})
                market_sides_f[market][side] = vol
            for market in sorted(market_sides_f):
                ms = market_sides_f[market]
                lines.append(f"    {market.title()} {_delta_line(ms.get('bid', 0), ms.get('ask', 0)).strip()}")
    else:
        lines.append("📏 Фреши по глубине (≥60 сек): нет")

    lines.append("")

    # --- CVD ---
    if cvd_rows:
        lines.append("📈 CVD (дельта за период):")
        for row in cvd_rows:
            market = row["market"].title()
            delta = row["delta"]
            sign = "+" if delta >= 0 else ""
            label = "покупатели" if delta >= 0 else "продавцы"
            lines.append(f"  {market}: {sign}{format_usd(delta)} ({label})")
    else:
        lines.append("📈 CVD: нет данных")

    lines.append("")

    # --- Imbalance ---
    lines.append("⚖️ Дисбаланс (±1%):")
    if imbalance_data:
        for market in ["futures", "spot"]:
            imb = imbalance_data.get(market)
            if imb is None:
                continue
            bid_pct = int((1 + imb) / 2 * 100)
            ask_pct = 100 - bid_pct
            if imb > 0.05:
                label = "перевес покупателей"
            elif imb < -0.05:
                label = "перевес продавцов"
            else:
                label = "равновесие"
            lines.append(f"  {market.title()}: BID {bid_pct}% / ASK {ask_pct}% ({label})")
    else:
        lines.append("  нет данных")

    if imbalance_alert_cnt > 0:
        lines.append(f"  ⚠️ Аномалии: {_plural_alerts(imbalance_alert_cnt)}")

    return "\n".join(lines)


async def _build_digest(interval_min: int, cutoff_ts: float) -> str:
    """Query DB and build digest text for one interval."""
    now = time.time()

    # --- Trades (futures >= $500K, spot >= $100K) ---
    trades_rows = await database.fetchall(
        "SELECT market, side, COUNT(*) as cnt, SUM(quantity_usd) as total_usd "
        "FROM large_trades WHERE timestamp >= ? "
        "  AND quantity_usd >= CASE WHEN market = 'futures' THEN 500000 ELSE 100000 END "
        "GROUP BY market, side ORDER BY market, side",
        (cutoff_ts,),
    )

    # --- Walls (futures >= $2M, spot >= $500K) ---
    walls_rows = await database.fetchall(
        "SELECT market, side, COUNT(*) as cnt, SUM(size_usd) as total_usd "
        "FROM orderbook_walls WHERE detected_at >= ? "
        "  AND size_usd >= CASE WHEN market = 'futures' THEN 2000000 ELSE 500000 END "
        "GROUP BY market, side ORDER BY market, side",
        (cutoff_ts,),
    )

    # --- Fresh walls by depth band (stood >= 60 sec, within ±5%) ---
    # Futures >= $2M, spot >= $500K
    fresh_walls_rows = await database.fetchall(
        "SELECT "
        "  CASE "
        "    WHEN abs(distance_pct) <= 1 THEN '1' "
        "    WHEN abs(distance_pct) <= 2 THEN '2' "
        "    ELSE '5' "
        "  END as depth_band, "
        "  market, side, COUNT(*) as cnt, SUM(size_usd) as total_usd "
        "FROM orderbook_walls "
        "WHERE detected_at >= ? "
        "  AND abs(distance_pct) <= 5 "
        "  AND size_usd >= CASE WHEN market = 'futures' THEN 2000000 ELSE 500000 END "
        "  AND ("
        "    (status = 'active' AND (? - detected_at) >= 60) "
        "    OR (status != 'active' AND COALESCE(lifetime_sec, 0) >= 60)"
        "  ) "
        "GROUP BY depth_band, market, side "
        "ORDER BY depth_band, market, side",
        (cutoff_ts, now),
    )

    # --- CVD ---
    cvd_rows = await database.fetchall(
        "SELECT market, SUM(delta_usd) as delta "
        "FROM trade_aggregates_1m WHERE timestamp >= ? "
        "GROUP BY market ORDER BY market",
        (cutoff_ts,),
    )

    # --- Price at start of period (futures only) ---
    price_start_row = await database.fetchone(
        "SELECT mid_price FROM ob_snapshots_1m "
        "WHERE market = 'futures' AND timestamp >= ? "
        "ORDER BY timestamp ASC LIMIT 1",
        (cutoff_ts,),
    )

    # --- Latest futures snapshot (current price) ---
    latest_row = await database.fetchone(
        "SELECT mid_price FROM ob_snapshots_1m "
        "WHERE market = 'futures' "
        "ORDER BY timestamp DESC LIMIT 1",
    )

    # --- Latest snapshot per market (imbalance) ---
    latest_imb_rows = await database.fetchall(
        "SELECT market, imbalance_1pct "
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

    # Build price_data (futures only)
    price_data: dict = {}
    start_p = price_start_row["mid_price"] if price_start_row else None
    end_p = latest_row["mid_price"] if latest_row else None
    if start_p and end_p:
        price_data["futures"] = {"start": start_p, "end": end_p}

    # Build imbalance_data
    imbalance_data: dict = {}
    for row in latest_imb_rows:
        imbalance_data[row["market"]] = row["imbalance_1pct"]

    return format_digest(
        interval_min, trades_rows, walls_rows, cvd_rows,
        price_data, imbalance_data, imbalance_alert_cnt, fresh_walls_rows,
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
