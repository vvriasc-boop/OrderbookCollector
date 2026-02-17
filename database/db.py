import sqlite3
import asyncio
import time
import logging

logger = logging.getLogger("orderbook_collector")

_db: sqlite3.Connection | None = None


def init_database(db_path: str = "data.db") -> sqlite3.Connection:
    global _db
    db = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.row_factory = sqlite3.Row
    _create_tables(db)
    _db = db
    return db


def get_db() -> sqlite3.Connection:
    assert _db is not None, "Database not initialized"
    return _db


def _create_tables(db: sqlite3.Connection):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS orderbook_walls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            detected_at REAL NOT NULL,
            market TEXT NOT NULL,
            side TEXT NOT NULL,
            price TEXT NOT NULL,
            size_btc REAL NOT NULL,
            size_usd REAL NOT NULL,
            peak_size_usd REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            ended_at REAL,
            lifetime_sec REAL,
            end_reason TEXT,
            price_at_detection REAL,
            price_at_end REAL,
            distance_pct REAL,
            updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_walls_status ON orderbook_walls(status);
        CREATE INDEX IF NOT EXISTS idx_walls_detected ON orderbook_walls(detected_at);
        CREATE INDEX IF NOT EXISTS idx_walls_side_price ON orderbook_walls(side, price);

        CREATE TABLE IF NOT EXISTS large_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            market TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            quantity_btc REAL NOT NULL,
            quantity_usd REAL NOT NULL,
            is_maker_buy INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_lt_timestamp ON large_trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_lt_side ON large_trades(side, timestamp);

        CREATE TABLE IF NOT EXISTS liquidations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            quantity_btc REAL NOT NULL,
            quantity_usd REAL NOT NULL,
            order_type TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_liq_timestamp ON liquidations(timestamp);
        CREATE INDEX IF NOT EXISTS idx_liq_side ON liquidations(side, timestamp);

        CREATE TABLE IF NOT EXISTS trade_aggregates_1m (
            timestamp INTEGER NOT NULL,
            market TEXT NOT NULL,
            buy_volume_usd REAL NOT NULL DEFAULT 0,
            sell_volume_usd REAL NOT NULL DEFAULT 0,
            buy_count INTEGER NOT NULL DEFAULT 0,
            sell_count INTEGER NOT NULL DEFAULT 0,
            delta_usd REAL NOT NULL DEFAULT 0,
            cvd_usd REAL NOT NULL DEFAULT 0,
            max_trade_usd REAL NOT NULL DEFAULT 0,
            vwap REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (timestamp, market)
        );

        CREATE TABLE IF NOT EXISTS ob_snapshots_1m (
            timestamp INTEGER NOT NULL,
            market TEXT NOT NULL,
            mid_price REAL NOT NULL,
            spread_pct REAL NOT NULL,
            bid_depth_01pct REAL,
            bid_depth_05pct REAL,
            bid_depth_1pct REAL,
            bid_depth_2pct REAL,
            bid_depth_5pct REAL,
            ask_depth_01pct REAL,
            ask_depth_05pct REAL,
            ask_depth_1pct REAL,
            ask_depth_2pct REAL,
            ask_depth_5pct REAL,
            imbalance_01pct REAL,
            imbalance_05pct REAL,
            imbalance_1pct REAL,
            imbalance_2pct REAL,
            imbalance_5pct REAL,
            wall_count_bid INTEGER DEFAULT 0,
            wall_count_ask INTEGER DEFAULT 0,
            PRIMARY KEY (timestamp, market)
        );

        CREATE TABLE IF NOT EXISTS alerts_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            alert_type TEXT NOT NULL,
            description TEXT NOT NULL,
            data_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts_log(alert_type, timestamp);

        CREATE TABLE IF NOT EXISTS notification_settings (
            alert_type TEXT PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 1,
            threshold_usd REAL,
            updated_at REAL NOT NULL
        );

        INSERT OR IGNORE INTO notification_settings (alert_type, enabled, threshold_usd, updated_at) VALUES
            ('wall_new',        1, NULL, 0),
            ('wall_gone',       1, NULL, 0),
            ('large_trade',     1, NULL, 0),
            ('mega_trade',      1, NULL, 0),
            ('liquidation',     1, NULL, 0),
            ('cvd_spike',       1, NULL, 0),
            ('imbalance',       1, NULL, 0),
            ('confirmed_wall',  1, NULL, 0);
    """)


def _sync_execute(query: str, params: tuple = ()) -> list:
    db = get_db()
    cursor = db.execute(query, params)
    return cursor.fetchall()


def _sync_executemany(query: str, params_list: list):
    db = get_db()
    db.executemany(query, params_list)


def _sync_fetchone(query: str, params: tuple = ()):
    db = get_db()
    cursor = db.execute(query, params)
    return cursor.fetchone()


def _sync_fetchall(query: str, params: tuple = ()) -> list:
    db = get_db()
    cursor = db.execute(query, params)
    return cursor.fetchall()


async def execute(query: str, params: tuple = ()) -> list:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_execute, query, params)


async def executemany(query: str, params_list: list):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _sync_executemany, query, params_list)


async def fetchone(query: str, params: tuple = ()):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_fetchone, query, params)


async def fetchall(query: str, params: tuple = ()) -> list:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_fetchall, query, params)


# --- Specific insert helpers ---

async def insert_wall(wall_data: dict) -> int:
    row = await execute(
        """INSERT INTO orderbook_walls
           (detected_at, market, side, price, size_btc, size_usd, peak_size_usd,
            status, price_at_detection, distance_pct, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)""",
        (
            wall_data["detected_at"], wall_data["market"], wall_data["side"],
            wall_data["price"], wall_data["size_btc"], wall_data["size_usd"],
            wall_data["size_usd"], wall_data["price_at_detection"],
            wall_data["distance_pct"], time.time(),
        ),
    )
    last_id = await fetchone("SELECT last_insert_rowid()")
    return last_id[0] if last_id else 0


async def update_wall_status(wall_id: int, status: str, end_reason: str | None,
                             price_at_end: float | None):
    now = time.time()
    await execute(
        """UPDATE orderbook_walls
           SET status=?, ended_at=?, end_reason=?,
               lifetime_sec = ? - detected_at,
               price_at_end=?, updated_at=?
           WHERE id=?""",
        (status, now, end_reason, now, price_at_end, now, wall_id),
    )


async def update_wall_peak(wall_id: int, new_peak: float):
    await execute(
        "UPDATE orderbook_walls SET peak_size_usd=?, updated_at=? WHERE id=?",
        (new_peak, time.time(), wall_id),
    )


async def mark_walls_unknown():
    now = time.time()
    await execute(
        """UPDATE orderbook_walls SET status='unknown', ended_at=?,
           lifetime_sec = ? - detected_at, updated_at=?
           WHERE status='active'""",
        (now, now, now),
    )


async def insert_large_trade(data: dict):
    await execute(
        """INSERT INTO large_trades
           (timestamp, market, side, price, quantity_btc, quantity_usd, is_maker_buy)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            data["timestamp"], data["market"], data["side"],
            data["price"], data["quantity_btc"], data["quantity_usd"],
            data["is_maker_buy"],
        ),
    )


async def insert_liquidation(data: dict):
    await execute(
        """INSERT INTO liquidations
           (timestamp, side, price, quantity_btc, quantity_usd, order_type)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            data["timestamp"], data["side"], data["price"],
            data["quantity_btc"], data["quantity_usd"], data["order_type"],
        ),
    )


async def insert_trade_aggregate(data: dict):
    await execute(
        """INSERT OR REPLACE INTO trade_aggregates_1m
           (timestamp, market, buy_volume_usd, sell_volume_usd, buy_count, sell_count,
            delta_usd, cvd_usd, max_trade_usd, vwap)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            data["timestamp"], data["market"],
            data["buy_volume_usd"], data["sell_volume_usd"],
            data["buy_count"], data["sell_count"],
            data["delta_usd"], data["cvd_usd"],
            data["max_trade_usd"], data["vwap"],
        ),
    )


async def insert_ob_snapshot(data: dict):
    await execute(
        """INSERT OR REPLACE INTO ob_snapshots_1m
           (timestamp, market, mid_price, spread_pct,
            bid_depth_01pct, bid_depth_05pct, bid_depth_1pct, bid_depth_2pct, bid_depth_5pct,
            ask_depth_01pct, ask_depth_05pct, ask_depth_1pct, ask_depth_2pct, ask_depth_5pct,
            imbalance_01pct, imbalance_05pct, imbalance_1pct, imbalance_2pct, imbalance_5pct,
            wall_count_bid, wall_count_ask)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            data["timestamp"], data["market"], data["mid_price"], data["spread_pct"],
            data["bid_depth_01pct"], data["bid_depth_05pct"],
            data["bid_depth_1pct"], data["bid_depth_2pct"], data["bid_depth_5pct"],
            data["ask_depth_01pct"], data["ask_depth_05pct"],
            data["ask_depth_1pct"], data["ask_depth_2pct"], data["ask_depth_5pct"],
            data["imbalance_01pct"], data["imbalance_05pct"],
            data["imbalance_1pct"], data["imbalance_2pct"], data["imbalance_5pct"],
            data["wall_count_bid"], data["wall_count_ask"],
        ),
    )


async def insert_alert_log(alert_type: str, description: str, data_json: str | None = None):
    await execute(
        "INSERT INTO alerts_log (timestamp, alert_type, description, data_json) VALUES (?, ?, ?, ?)",
        (time.time(), alert_type, description, data_json),
    )


async def get_notification_setting(alert_type: str):
    return await fetchone(
        "SELECT enabled, threshold_usd FROM notification_settings WHERE alert_type = ?",
        (alert_type,),
    )


async def toggle_notification(alert_type: str) -> bool:
    row = await fetchone(
        "SELECT enabled FROM notification_settings WHERE alert_type = ?",
        (alert_type,),
    )
    if not row:
        return True
    new_val = 0 if row["enabled"] else 1
    await execute(
        "UPDATE notification_settings SET enabled = ?, updated_at = ? WHERE alert_type = ?",
        (new_val, time.time(), alert_type),
    )
    return bool(new_val)


async def set_all_notifications(enabled: bool):
    val = 1 if enabled else 0
    await execute(
        "UPDATE notification_settings SET enabled = ?, updated_at = ?",
        (val, time.time()),
    )


async def get_all_notification_settings() -> list:
    return await fetchall("SELECT alert_type, enabled, threshold_usd FROM notification_settings")


async def get_active_walls() -> list:
    return await fetchall("SELECT * FROM orderbook_walls WHERE status = 'active'")


async def get_db_size() -> float:
    import os
    try:
        return os.path.getsize("data.db") / (1024 * 1024)
    except OSError:
        return 0.0


def close_database():
    global _db
    if _db:
        _db.close()
        _db = None
