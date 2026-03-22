import aiosqlite
import asyncio
import os
from datetime import datetime
import json
import logging
import time
import aiohttp
from encryption import EncryptionManager, DecryptionKeyMismatch

logger = logging.getLogger(__name__)
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.abspath(os.path.join(_PROJECT_ROOT, "users.db"))

# Serialize DB access so limit_engine, deposit_listener, and UI handlers don't deadlock (SQLite single-writer)
# All DB access uses "async with aiosqlite.connect(DB_PATH, timeout=30.0)" (or under _db_lock where needed). Connection is
# always released on exit or exception, so Swap failed / snipe failure paths do not hold a connection open.
# WAL mode is enabled at startup via ensure_wal_mode() so the Limit Engine and Bot UI can read/write concurrently.
# Multi-process writes: Bot UI and Limit Engine run in separate processes; use retry on "database is locked".
_db_lock = asyncio.Lock()
_DB_WRITE_TIMEOUT = 20.0
_DB_WRITE_RETRY_ATTEMPTS = 5
_DB_WRITE_RETRY_DELAY_BASE = 0.5


def _safe_json_loads(val, default):
    """Parse JSON from DB; return default if None, empty string, or invalid."""
    if val is None or (isinstance(val, str) and not val.strip()):
        return default
    try:
        return json.loads(val)
    except (TypeError, json.JSONDecodeError):
        return default


async def ensure_wal_mode() -> None:
    """Enable SQLite WAL (Write-Ahead Logging) so multiple processes (Bot UI + Limit Engine) can access the DB without blocking.
    Call once at startup from main.py and limit_engine.py. Idempotent and fast."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.commit()
        logger.debug("SQLite WAL mode enabled for %s", DB_PATH)
    except Exception as e:
        logger.warning("ensure_wal_mode failed (DB may still work with default journal): %s", e)


# Initialize encryption manager
encryption_mgr = EncryptionManager()

# In-memory cache for decrypted private keys (reduces 200ms decrypt to <1ms on cache hit)
# TTL 60 seconds; invalidated on update_user when private_key changes
_decrypted_key_cache = {}  # user_id -> (private_key_str, expiry_timestamp)
_DECRYPT_CACHE_TTL = 60.0

# In-memory cache for constructed Keypair (avoids ~140ms Keypair.from_bytes per trade)
# Same TTL and invalidation as decrypt cache
_keypair_cache = {}  # user_id -> (Keypair, expiry_timestamp)

# RAM copy of global_config fee row (id=1): avoids SQLite read on every swap quote/build path.
# Invalidated on set_global_fee_config and on pending_admin_fees_lamports mutations.
_GLOBAL_FEE_CONFIG_CACHE: dict | None = None


def _invalidate_global_fee_config_cache() -> None:
    global _GLOBAL_FEE_CONFIG_CACHE
    _GLOBAL_FEE_CONFIG_CACHE = None


def _invalidate_decrypt_cache(user_id):
    _decrypted_key_cache.pop(str(user_id), None)
    _keypair_cache.pop(str(user_id), None)

#create a error decorator
def error_decorator(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.exception("error_decorator: %s", e)
    return wrapper


# -----------------------------------------------------------------------------
# Schema: users table (core)
# -----------------------------------------------------------------------------
SCHEMA = '''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL UNIQUE,
        wallet_address TEXT,
        private_key TEXT,
        encrypted_private_key BLOB,
        key_salt BLOB,
        trades TEXT DEFAULT '{}',
        slippage INTEGER DEFAULT 10,
        monitor_wallet TEXT DEFAULT '{}',
        withdraw_wallet TEXT DEFAULT NULL,
        priority_mode TEXT DEFAULT 'Standard',
        mev_mode TEXT DEFAULT 'Low',
        mev_enabled INTEGER DEFAULT 0,
        mev_level TEXT DEFAULT 'Low',
        turbo_mode INTEGER DEFAULT 1,
        jito_tip_amount REAL DEFAULT 0.0005,
        jito_tip_low REAL DEFAULT 0.0005,
        jito_tip_mid REAL DEFAULT 0.001,
        jito_tip_pro REAL DEFAULT 0.005,
        slippage_turbo REAL DEFAULT 1.0,
        slippage_mev REAL DEFAULT 1.0,
        slippage_sniper REAL DEFAULT 20.0,
        slippage_bonding REAL DEFAULT 15.0,
        prio_fee_buy REAL DEFAULT 0.002,
        prio_fee_sell REAL DEFAULT 0.002,
        prio_fee_snipe REAL DEFAULT 0.035,
        prio_fee_bonding REAL DEFAULT 0.005,
        auto_buy_amount REAL DEFAULT 0.0,
        auto_sell_enabled INTEGER DEFAULT 0,
        auto_sell_tp_pct REAL DEFAULT 20.0,
        auto_sell_sl_pct REAL DEFAULT -50.0,
        auto_sell_amount_pct REAL DEFAULT 100.0,
        auto_sell_trailing INTEGER DEFAULT 0,
        referred_by INTEGER,
        referral_count INTEGER DEFAULT 0,
        referral_commissions_sol REAL DEFAULT 0.0,
        last_access TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
'''

# -----------------------------------------------------------------------------
# Copy targets: schema and table ensure (wallets to copy)
# -----------------------------------------------------------------------------
COPY_TARGETS_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS copy_targets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        target_wallet TEXT NOT NULL,
        buy_amount_sol REAL NOT NULL,
        is_active INTEGER DEFAULT 1,
        UNIQUE(user_id, target_wallet)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_copy_targets_user_id ON copy_targets(user_id)",
]


async def _ensure_copy_targets_table(db):
    """Create copy_targets table and index if they do not exist (migration for existing DBs)."""
    for stmt in COPY_TARGETS_SCHEMA:
        await db.execute(stmt)
    await db.commit()


async def ensure_copy_targets_ready():
    """Ensure copy_targets table exists (call at startup or before first use)."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_copy_targets_table(db)


# -----------------------------------------------------------------------------
# DCA orders: schema and table ensure (recurring buys at fixed intervals)
# -----------------------------------------------------------------------------
DCA_ORDERS_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS dca_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token_mint TEXT NOT NULL,
        amount_sol REAL NOT NULL,
        interval_hours REAL NOT NULL,
        next_run_time INTEGER NOT NULL,
        is_active INTEGER DEFAULT 1
    )""",
    "CREATE INDEX IF NOT EXISTS idx_dca_orders_user_id ON dca_orders(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_dca_orders_next_run ON dca_orders(next_run_time)",
]


async def _ensure_dca_orders_table(db):
    """Create dca_orders table and indexes if they do not exist (migration for existing DBs)."""
    for stmt in DCA_ORDERS_SCHEMA:
        await db.execute(stmt)
    await db.commit()


async def ensure_dca_orders_ready():
    """Ensure dca_orders table exists (call at startup or before first use)."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_dca_orders_table(db)


# Global bot config (single row: fee settings)
# This table is THE source of truth for the Admin Fee Control Center (menus.py / handlers/admin.py).
# get_global_fee_config() reads from here; set_global_fee_config() updates from the UI.
# Schema (base + migrations): id, fees_enabled, fee_destination_wallet, buy_fee_percent, sell_fee_percent,
# benchmark_custom_ca, benchmark_pump_ca, benchmark_route_hint, jito_enabled, jito_tip_lamports, bot_paused,
# last_benchmark_report; plus infra columns
# rpc_mainnet_url, jupiter_api_url, helius_api_key, priority_fee_provider (see get_global_infra_config / set_global_infra_config).
GLOBAL_CONFIG_TABLE = """CREATE TABLE IF NOT EXISTS global_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    fees_enabled INTEGER DEFAULT 0,
    fee_destination_wallet TEXT DEFAULT '',
    buy_fee_percent REAL DEFAULT 0.0,
    sell_fee_percent REAL DEFAULT 0.0,
    benchmark_custom_ca TEXT DEFAULT '',
    benchmark_pump_ca TEXT DEFAULT '',
    benchmark_route_hint TEXT DEFAULT '',
    jito_enabled INTEGER DEFAULT 1,
    jito_tip_lamports INTEGER DEFAULT 500000,
    bot_paused INTEGER DEFAULT 0,
    last_benchmark_report TEXT DEFAULT ''
)"""


# -----------------------------------------------------------------------------
# Global fee config: admin fee destination and buy/sell fee percent
# -----------------------------------------------------------------------------
async def _ensure_global_config_table(db):
    await db.execute(GLOBAL_CONFIG_TABLE)
    # Backwards-compatible schema migration: add missing columns if global_config
    # was created before new fields (e.g. benchmark_custom_ca, jito_enabled, bot_paused) were introduced.
    cursor = await db.execute("PRAGMA table_info(global_config)")
    cols = {row[1] for row in await cursor.fetchall()}
    await cursor.close()
    if "benchmark_custom_ca" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN benchmark_custom_ca TEXT DEFAULT ''")
    if "benchmark_pump_ca" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN benchmark_pump_ca TEXT DEFAULT ''")
    if "benchmark_route_hint" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN benchmark_route_hint TEXT DEFAULT ''")
    if "jito_enabled" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN jito_enabled INTEGER DEFAULT 1")
    if "jito_tip_lamports" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN jito_tip_lamports INTEGER DEFAULT 500000")
    if "bot_paused" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN bot_paused INTEGER DEFAULT 0")
    if "last_benchmark_report" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN last_benchmark_report TEXT DEFAULT ''")
    await db.execute(
        "INSERT OR IGNORE INTO global_config (id, fees_enabled, fee_destination_wallet, buy_fee_percent, sell_fee_percent, benchmark_custom_ca, benchmark_pump_ca, benchmark_route_hint, jito_enabled, jito_tip_lamports, bot_paused, last_benchmark_report) "
        "VALUES (1, 0, '', 0.0, 0.0, '', '', '', 1, 500000, 0, '')"
    )
    # Infrastructure: RPC, Jupiter, Helius, priority-fee provider (DB-backed config)
    if "rpc_mainnet_url" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN rpc_mainnet_url TEXT DEFAULT ''")
    if "jupiter_api_url" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN jupiter_api_url TEXT DEFAULT ''")
    if "helius_api_key" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN helius_api_key TEXT DEFAULT ''")
    if "priority_fee_provider" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN priority_fee_provider TEXT DEFAULT 'static'")
    # TG Sniper: enabled, amount SOL, comma-separated groups, user_id for wallet, per-sniper TP/SL
    if "snipe_tg_enabled" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN snipe_tg_enabled INTEGER DEFAULT 0")
    if "snipe_tg_amount" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN snipe_tg_amount REAL DEFAULT 0.01")
    if "snipe_tg_groups" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN snipe_tg_groups TEXT DEFAULT ''")
    if "snipe_tg_user_id" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN snipe_tg_user_id INTEGER DEFAULT 0")
    if "snipe_tp_pct" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN snipe_tp_pct REAL DEFAULT 100.0")
    if "snipe_sl_pct" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN snipe_sl_pct REAL DEFAULT -50.0")
    if "snipe_expiry_sec" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN snipe_expiry_sec REAL DEFAULT 120.0")
    if "snipe_tg_execution_mode" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN snipe_tg_execution_mode TEXT DEFAULT 'standard'")
    if "pending_admin_fees_lamports" not in cols:
        await db.execute("ALTER TABLE global_config ADD COLUMN pending_admin_fees_lamports INTEGER DEFAULT 0")
    # Seed infra from config/.env when DB values are empty (do not overwrite existing).
    # Local import to avoid circular dependency at module import time.
    try:
        from config import MAINNET_RPC_URL as _MAINNET_RPC_URL, JUPITER_API_URL as _JUP_URL
        import os
        _rpc_env = (_MAINNET_RPC_URL or "").strip()
        _jup_env = (_JUP_URL or os.getenv("JUPITER_API_URL", "https://public.jupiterapi.com") or "https://public.jupiterapi.com").strip()
        _helius_env = (os.getenv("HELIUS_API_KEY", "") or "").strip()
    except Exception:
        import os
        _rpc_env = ""
        _jup_env = os.getenv("JUPITER_API_URL", "https://public.jupiterapi.com") or "https://public.jupiterapi.com"
        _helius_env = os.getenv("HELIUS_API_KEY", "") or ""
    _pf_env = "static"
    await db.execute(
        """UPDATE global_config SET
           rpc_mainnet_url = CASE WHEN rpc_mainnet_url IS NULL OR TRIM(COALESCE(rpc_mainnet_url,'')) = '' THEN ? ELSE rpc_mainnet_url END,
           jupiter_api_url = CASE WHEN jupiter_api_url IS NULL OR TRIM(COALESCE(jupiter_api_url,'')) = '' THEN ? ELSE jupiter_api_url END,
           helius_api_key = CASE WHEN helius_api_key IS NULL OR TRIM(COALESCE(helius_api_key,'')) = '' THEN ? ELSE helius_api_key END,
           priority_fee_provider = CASE WHEN priority_fee_provider IS NULL OR TRIM(COALESCE(priority_fee_provider,'')) = '' THEN ? ELSE priority_fee_provider END
           WHERE id = 1""",
        (_rpc_env, _jup_env, _helius_env, _pf_env),
    )
    await db.commit()


async def _ensure_tg_groups_table(db):
    """Ensure tg_groups table exists and has at least (chat_id, name, created_at)."""
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS tg_groups (
            chat_id TEXT PRIMARY KEY,
            name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    # Backwards compatibility: add name column if an older schema created tg_groups without it.
    cur = await db.execute("PRAGMA table_info(tg_groups)")
    cols = {row[1] for row in await cur.fetchall()}
    await cur.close()
    if "name" not in cols:
        await db.execute("ALTER TABLE tg_groups ADD COLUMN name TEXT")
    await db.commit()


_DEFAULT_GLOBAL_FEE_CONFIG = {
    "fees_enabled": False,
    "fee_destination_wallet": "",
    "buy_fee_percent": 0.0,
    "sell_fee_percent": 0.0,
    "benchmark_custom_ca": "",
    "benchmark_pump_ca": "",
    "benchmark_route_hint": "",
    "jito_enabled": 1,
    "jito_tip_lamports": 500000,
    "bot_paused": 0,
    "pending_admin_fees_lamports": 0,
}


async def get_global_fee_config() -> dict:
    """Return {fees_enabled, fee_destination_wallet, buy_fee_percent, sell_fee_percent, benchmark_custom_ca,
    benchmark_pump_ca, benchmark_route_hint, jito_enabled, jito_tip_lamports, bot_paused, pending_admin_fees_lamports}.
    Reads FROM global_config WHERE id = 1 (single row; same table the Admin UI updates via set_global_fee_config).
    Uses an in-RAM cache after first successful DB read; returns a shallow copy so callers cannot mutate the cache.
    Uses defaults if no row."""
    global _GLOBAL_FEE_CONFIG_CACHE
    if _GLOBAL_FEE_CONFIG_CACHE is not None:
        return dict(_GLOBAL_FEE_CONFIG_CACHE)
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_global_config_table(db)
            db.row_factory = aiosqlite.Row
            # Log only on real DB read (not on cache hit); DEBUG to avoid noisy startup/journal.
            logger.debug("get_global_fee_config reading from DB (global_config id=1): %s", DB_PATH)
            cursor = await db.execute(
                "SELECT fees_enabled, fee_destination_wallet, buy_fee_percent, sell_fee_percent, benchmark_custom_ca, benchmark_pump_ca, benchmark_route_hint, jito_enabled, jito_tip_lamports, bot_paused, pending_admin_fees_lamports FROM global_config WHERE id = 1"
            )
            row = await cursor.fetchone()
            if row:
                out = {
                    "fees_enabled": bool(row["fees_enabled"]),
                    "fee_destination_wallet": (row["fee_destination_wallet"] or "").strip(),
                    "buy_fee_percent": float(row["buy_fee_percent"] or 0.0),
                    "sell_fee_percent": float(row["sell_fee_percent"] or 0.0),
                    "benchmark_custom_ca": (row["benchmark_custom_ca"] or "").strip() if "benchmark_custom_ca" in row.keys() else "",
                    "benchmark_pump_ca": (row["benchmark_pump_ca"] or "").strip() if "benchmark_pump_ca" in row.keys() else "",
                    "benchmark_route_hint": (row["benchmark_route_hint"] or "").strip() if "benchmark_route_hint" in row.keys() else "",
                    "jito_enabled": int(row["jito_enabled"] if "jito_enabled" in row.keys() else 1),
                    "jito_tip_lamports": int(row["jito_tip_lamports"] if "jito_tip_lamports" in row.keys() else 500000),
                    "bot_paused": int(row["bot_paused"] if "bot_paused" in row.keys() else 0),
                    "pending_admin_fees_lamports": int(row["pending_admin_fees_lamports"] if "pending_admin_fees_lamports" in row.keys() else 0),
                }
                try:
                    from shared import set_jito_settings, set_bot_paused
                    set_jito_settings(bool(out["jito_enabled"]), int(out["jito_tip_lamports"]))
                    set_bot_paused(bool(out["bot_paused"]))
                except Exception:
                    pass
                logger.debug(
                    "[DEBUG] get_global_fee_config (same table as UI): fees_enabled=%s, dest=%s, buy_fee_percent=%s, sell_fee_percent=%s",
                    out["fees_enabled"], out["fee_destination_wallet"] or "(empty)", out["buy_fee_percent"], out["sell_fee_percent"],
                )
                _GLOBAL_FEE_CONFIG_CACHE = dict(out)
                return dict(_GLOBAL_FEE_CONFIG_CACHE)
    except Exception as e:
        logger.warning("get_global_fee_config failed: %s", e)
    return dict(_DEFAULT_GLOBAL_FEE_CONFIG)


def get_global_fee_config_snapshot() -> dict:
    """RAM-only read of global fee row (no await, no SQLite).

    Same keys/shape as get_global_fee_config(). Use on hot swap paths after startup warm
    (main.py calls get_global_fee_config once). If cache is cold, returns defaults until
    the async loader runs.
    """
    global _GLOBAL_FEE_CONFIG_CACHE
    if _GLOBAL_FEE_CONFIG_CACHE is not None:
        return dict(_GLOBAL_FEE_CONFIG_CACHE)
    return dict(_DEFAULT_GLOBAL_FEE_CONFIG)


async def set_global_fee_config(
    *,
    fees_enabled: bool | None = None,
    fee_destination_wallet: str | None = None,
    buy_fee_percent: float | None = None,
    sell_fee_percent: float | None = None,
    benchmark_custom_ca: str | None = None,
    benchmark_pump_ca: str | None = None,
    benchmark_route_hint: str | None = None,
    jito_enabled: bool | None = None,
    jito_tip_lamports: int | None = None,
    bot_paused: bool | None = None,
    pending_admin_fees_lamports: int | None = None,
) -> bool:
    """Update global fee settings. Only provided keys are updated. Returns True on success."""
    try:
        updates = []
        values = []
        if fees_enabled is not None:
            updates.append("fees_enabled = ?")
            values.append(1 if fees_enabled else 0)
        if fee_destination_wallet is not None:
            updates.append("fee_destination_wallet = ?")
            values.append((fee_destination_wallet or "").strip())
        if buy_fee_percent is not None:
            updates.append("buy_fee_percent = ?")
            values.append(float(buy_fee_percent))
        if sell_fee_percent is not None:
            updates.append("sell_fee_percent = ?")
            values.append(float(sell_fee_percent))
        if benchmark_custom_ca is not None:
            updates.append("benchmark_custom_ca = ?")
            values.append((benchmark_custom_ca or "").strip())
        if benchmark_pump_ca is not None:
            updates.append("benchmark_pump_ca = ?")
            values.append((benchmark_pump_ca or "").strip())
        if benchmark_route_hint is not None:
            updates.append("benchmark_route_hint = ?")
            values.append((benchmark_route_hint or "").strip())
        if jito_enabled is not None:
            updates.append("jito_enabled = ?")
            values.append(1 if jito_enabled else 0)
        if jito_tip_lamports is not None:
            updates.append("jito_tip_lamports = ?")
            values.append(int(jito_tip_lamports))
        if bot_paused is not None:
            updates.append("bot_paused = ?")
            values.append(1 if bot_paused else 0)
        if pending_admin_fees_lamports is not None:
            updates.append("pending_admin_fees_lamports = ?")
            values.append(int(pending_admin_fees_lamports))
        if not updates:
            return True
        values.append(1)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_global_config_table(db)
            await db.execute(
                f"UPDATE global_config SET {', '.join(updates)} WHERE id = ?",
                values,
            )
            await db.commit()
        _invalidate_global_fee_config_cache()
        # Keep in-memory runtime cache in sync for Jito and kill-switch flags.
        try:
            from shared import set_jito_settings, set_bot_paused
            if jito_enabled is not None or jito_tip_lamports is not None:
                cfg = await get_global_fee_config()
                set_jito_settings(bool(cfg.get("jito_enabled", 1)), int(cfg.get("jito_tip_lamports", 500000)))
            if bot_paused is not None:
                set_bot_paused(bool(bot_paused))
        except Exception:
            pass
        return True
    except Exception as e:
        logger.warning("set_global_fee_config failed: %s", e)
        return False


ADMIN_FEE_FLUSH_THRESHOLD_LAMPORTS = 5_000_000  # 0.005 SOL


async def accumulate_or_flush_admin_fee(new_fee_lamports: int) -> tuple[bool, int]:
    """Atomically add *new_fee_lamports* to the pending admin fee accumulator.

    Returns ``(should_send, total_lamports)``.
    * If the accumulated total (pending + new) >= threshold: resets accumulator to 0
      and returns ``(True, total)`` so the caller can send the batch.
    * Otherwise: saves the new total and returns ``(False, 0)``.
    """
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_global_config_table(db)
            cursor = await db.execute(
                "SELECT pending_admin_fees_lamports FROM global_config WHERE id = 1"
            )
            row = await cursor.fetchone()
            pending = int(row[0] or 0) if row else 0
            total = pending + new_fee_lamports
            if total >= ADMIN_FEE_FLUSH_THRESHOLD_LAMPORTS:
                await db.execute(
                    "UPDATE global_config SET pending_admin_fees_lamports = 0 WHERE id = 1"
                )
                await db.commit()
                _invalidate_global_fee_config_cache()
                logger.info(
                    "[REVENUE] Fee accumulator flushing: %s lamports (pending=%s + new=%s, threshold=%s)",
                    total, pending, new_fee_lamports, ADMIN_FEE_FLUSH_THRESHOLD_LAMPORTS,
                )
                return True, total
            await db.execute(
                "UPDATE global_config SET pending_admin_fees_lamports = ? WHERE id = 1",
                (total,),
            )
            await db.commit()
            _invalidate_global_fee_config_cache()
            logger.info(
                "[REVENUE] Fee accumulated: %s lamports (pending=%s + new=%s, threshold=%s — not yet reached)",
                total, pending, new_fee_lamports, ADMIN_FEE_FLUSH_THRESHOLD_LAMPORTS,
            )
            return False, 0
    except Exception as e:
        logger.warning("accumulate_or_flush_admin_fee failed: %s", e)
        return False, 0


async def flush_pending_admin_fees() -> int:
    """Force-flush: return the full pending amount and reset to 0. Returns lamports (0 if nothing pending)."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_global_config_table(db)
            cursor = await db.execute(
                "SELECT pending_admin_fees_lamports FROM global_config WHERE id = 1"
            )
            row = await cursor.fetchone()
            pending = int(row[0] or 0) if row else 0
            if pending > 0:
                await db.execute(
                    "UPDATE global_config SET pending_admin_fees_lamports = 0 WHERE id = 1"
                )
                await db.commit()
                _invalidate_global_fee_config_cache()
                logger.info("[REVENUE] Manual flush: %s lamports", pending)
            return pending
    except Exception as e:
        logger.warning("flush_pending_admin_fees failed: %s", e)
        return 0


# -----------------------------------------------------------------------------
# Global infrastructure config: RPC URL, Jupiter URL, Helius API key, priority-fee provider
# -----------------------------------------------------------------------------
async def get_global_infra_config() -> dict:
    """Return {rpc_mainnet_url, jupiter_api_url, helius_api_key, priority_fee_provider} from global_config id=1.
    Strips strings; defaults to '' or 'static'. On exception returns defaults."""
    defaults = {"rpc_mainnet_url": "", "jupiter_api_url": "", "helius_api_key": "", "priority_fee_provider": "static"}
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_global_config_table(db)
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT rpc_mainnet_url, jupiter_api_url, helius_api_key, priority_fee_provider FROM global_config WHERE id = 1"
            )
            row = await cursor.fetchone()
            if row:
                provider = (row["priority_fee_provider"] or "").strip().lower()
                if provider not in ("static", "quicknode", "helius"):
                    provider = "static"
                return {
                    "rpc_mainnet_url": (row["rpc_mainnet_url"] or "").strip(),
                    "jupiter_api_url": (row["jupiter_api_url"] or "").strip().rstrip("/"),
                    "helius_api_key": (row["helius_api_key"] or "").strip(),
                    "priority_fee_provider": provider,
                }
    except Exception as e:
        logger.warning("get_global_infra_config failed: %s", e)
    return defaults


async def set_global_infra_config(
    *,
    rpc_mainnet_url: str | None = None,
    jupiter_api_url: str | None = None,
    helius_api_key: str | None = None,
    priority_fee_provider: str | None = None,
) -> bool:
    """Update global infrastructure settings. Only provided keys are updated. Returns True on success."""
    try:
        updates = []
        values = []
        if rpc_mainnet_url is not None:
            updates.append("rpc_mainnet_url = ?")
            values.append((rpc_mainnet_url or "").strip())
        if jupiter_api_url is not None:
            updates.append("jupiter_api_url = ?")
            values.append((jupiter_api_url or "").strip().rstrip("/"))
        if helius_api_key is not None:
            updates.append("helius_api_key = ?")
            values.append((helius_api_key or "").strip())
        if priority_fee_provider is not None:
            p = (priority_fee_provider or "").strip().lower()
            if p not in ("static", "quicknode", "helius"):
                p = "static"
            updates.append("priority_fee_provider = ?")
            values.append(p)
        if not updates:
            return True
        values.append(1)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_global_config_table(db)
            await db.execute(
                f"UPDATE global_config SET {', '.join(updates)} WHERE id = ?",
                values,
            )
            await db.commit()
        return True
    except Exception as e:
        logger.warning("set_global_infra_config failed: %s", e)
        return False


# -----------------------------------------------------------------------------
# TG Sniper config: enabled, amount SOL, comma-separated group usernames/links, user_id for wallet
# -----------------------------------------------------------------------------
async def get_global_snipe_tg_config() -> dict:
    """Return TG Sniper config from global_config id=1.

    Shape:
        {
            snipe_tg_enabled: bool,
            snipe_tg_amount: float,
            snipe_tg_groups: str (comma-separated),
            snipe_tg_user_id: int,
            snipe_tp_pct: float,
            snipe_sl_pct: float,
        }
    """
    defaults = {
        "snipe_tg_enabled": False,
        "snipe_tg_amount": 0.01,
        "snipe_tg_groups": "",
        "snipe_tg_user_id": 0,
        "snipe_tp_pct": 100.0,
        "snipe_sl_pct": -50.0,
        "snipe_expiry_sec": 120.0,
        "snipe_tg_execution_mode": "standard",
    }
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_global_config_table(db)
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT snipe_tg_enabled, snipe_tg_amount, snipe_tg_groups, snipe_tg_user_id, snipe_tp_pct, snipe_sl_pct, snipe_expiry_sec, snipe_tg_execution_mode FROM global_config WHERE id = 1"
            )
            row = await cursor.fetchone()
            if row:
                keys = row.keys()
                groups = (row["snipe_tg_groups"] or "").strip() if "snipe_tg_groups" in keys else ""
                # Use defaults when columns are missing or NULL to remain backwards compatible.
                enabled = bool(row["snipe_tg_enabled"]) if "snipe_tg_enabled" in keys and row["snipe_tg_enabled"] is not None else defaults["snipe_tg_enabled"]
                amount = float(row["snipe_tg_amount"]) if "snipe_tg_amount" in keys and row["snipe_tg_amount"] is not None else defaults["snipe_tg_amount"]
                user_id = int(row["snipe_tg_user_id"]) if "snipe_tg_user_id" in keys and row["snipe_tg_user_id"] is not None else defaults["snipe_tg_user_id"]
                tp_pct = float(row["snipe_tp_pct"]) if "snipe_tp_pct" in keys and row["snipe_tp_pct"] is not None else defaults["snipe_tp_pct"]
                sl_pct = float(row["snipe_sl_pct"]) if "snipe_sl_pct" in keys and row["snipe_sl_pct"] is not None else defaults["snipe_sl_pct"]
                expiry_sec = float(row["snipe_expiry_sec"]) if "snipe_expiry_sec" in keys and row["snipe_expiry_sec"] is not None else defaults["snipe_expiry_sec"]
                execution_mode = (row["snipe_tg_execution_mode"] or "standard").strip().lower() if "snipe_tg_execution_mode" in keys and row["snipe_tg_execution_mode"] else defaults["snipe_tg_execution_mode"]
                if execution_mode not in ("standard", "jito"):
                    execution_mode = defaults["snipe_tg_execution_mode"]
                return {
                    "snipe_tg_enabled": enabled,
                    "snipe_tg_amount": amount,
                    "snipe_tg_groups": groups,
                    "snipe_tg_user_id": user_id,
                    "snipe_tp_pct": tp_pct,
                    "snipe_sl_pct": sl_pct,
                    "snipe_expiry_sec": expiry_sec,
                    "snipe_tg_execution_mode": execution_mode,
                }
    except Exception as e:
        logger.warning("get_global_snipe_tg_config failed: %s", e)
    return defaults


async def set_global_snipe_tg_config(
    *,
    snipe_tg_enabled: bool | None = None,
    snipe_tg_amount: float | None = None,
    snipe_tg_groups: str | None = None,
    snipe_tg_user_id: int | None = None,
    snipe_tp_pct: float | None = None,
    snipe_sl_pct: float | None = None,
    snipe_expiry_sec: float | None = None,
    snipe_tg_execution_mode: str | None = None,
) -> bool:
    """Update TG sniper settings. Only provided keys are updated. Returns True on success."""
    try:
        updates = []
        values = []
        if snipe_tg_enabled is not None:
            updates.append("snipe_tg_enabled = ?")
            values.append(1 if snipe_tg_enabled else 0)
        if snipe_tg_amount is not None:
            updates.append("snipe_tg_amount = ?")
            values.append(float(snipe_tg_amount))
        if snipe_tg_groups is not None:
            updates.append("snipe_tg_groups = ?")
            values.append((snipe_tg_groups or "").strip())
        if snipe_tg_user_id is not None:
            updates.append("snipe_tg_user_id = ?")
            values.append(int(snipe_tg_user_id))
        if snipe_tp_pct is not None:
            updates.append("snipe_tp_pct = ?")
            values.append(float(snipe_tp_pct))
        if snipe_sl_pct is not None:
            updates.append("snipe_sl_pct = ?")
            values.append(float(snipe_sl_pct))
        if snipe_expiry_sec is not None:
            updates.append("snipe_expiry_sec = ?")
            values.append(float(snipe_expiry_sec))
        if snipe_tg_execution_mode is not None:
            mode = (snipe_tg_execution_mode or "standard").strip().lower()
            if mode not in ("standard", "jito"):
                mode = "standard"
            updates.append("snipe_tg_execution_mode = ?")
            values.append(mode)
        if not updates:
            return True
        values.append(1)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_global_config_table(db)
            await db.execute(
                f"UPDATE global_config SET {', '.join(updates)} WHERE id = ?",
                values,
            )
            await db.commit()
        return True
    except Exception as e:
        logger.warning("set_global_snipe_tg_config failed: %s", e)
        return False


@error_decorator
async def add_tg_group(chat_id: str, name: str | None = None) -> None:
    """Insert or update a Telegram group record (chat_id -> name)."""
    chat_id_str = str(chat_id).strip()
    name_val = (name or "").strip() or None
    if not chat_id_str:
        return
    async with _db_lock:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_tg_groups_table(db)
            await db.execute(
                """
                INSERT INTO tg_groups (chat_id, name)
                VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    name = COALESCE(excluded.name, tg_groups.name)
                """,
                (chat_id_str, name_val),
            )
            await db.commit()


@error_decorator
async def get_tg_groups_map() -> dict[str, str]:
    """Return a mapping of chat_id -> name (may be empty strings when unknown)."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_tg_groups_table(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT chat_id, COALESCE(name, '') AS name FROM tg_groups")
        rows = await cursor.fetchall()
        await cursor.close()
        return {str(row["chat_id"]): (row["name"] or "") for row in rows}


# -----------------------------------------------------------------------------
# Benchmark persistence: last benchmark report (HTML) stored in global_config
# -----------------------------------------------------------------------------
async def update_last_benchmark(report_text: str) -> bool:
    """Persist the LAST BENCHMARK REPORT block (HTML) in global_config.id=1."""
    try:
        txt = report_text or ""
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_global_config_table(db)
            await db.execute(
                "UPDATE global_config SET last_benchmark_report = ? WHERE id = 1",
                (txt,),
            )
            await db.commit()
        return True
    except Exception as e:
        logger.warning("update_last_benchmark failed: %s", e)
        return False


async def get_last_benchmark() -> str | None:
    """Return the stored LAST BENCHMARK REPORT block (HTML) from global_config, or None."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_global_config_table(db)
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT last_benchmark_report FROM global_config WHERE id = 1"
            )
            row = await cursor.fetchone()
            if row:
                value = row["last_benchmark_report"]
                if isinstance(value, str):
                    value = value.strip()
                else:
                    value = ""
                return value or None
    except Exception as e:
        logger.warning("get_last_benchmark failed: %s", e)
    return None


# -----------------------------------------------------------------------------
# DCA orders: CRUD (add, list, remove, get due, update next run)
# -----------------------------------------------------------------------------
async def add_dca_order(user_id: int, token_mint: str, amount_sol: float, interval_hours: float) -> bool:
    """Insert a new DCA order. next_run_time = now + interval_hours. Returns True on success."""
    try:
        uid = int(user_id)
        mint = (token_mint or "").strip()
        if not mint:
            return False
        amt = float(amount_sol)
        interval = float(interval_hours)
        if amt <= 0 or interval <= 0:
            return False
        next_run_time = int(time.time()) + int(interval * 3600)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_dca_orders_table(db)
            await db.execute(
                """INSERT INTO dca_orders (user_id, token_mint, amount_sol, interval_hours, next_run_time, is_active)
                   VALUES (?, ?, ?, ?, ?, 1)""",
                (uid, mint, amt, interval, next_run_time),
            )
            await db.commit()
        return True
    except Exception as e:
        logger.warning("add_dca_order failed: user_id=%s token_mint=%s error=%s", user_id, token_mint, e)
        return False


async def get_active_dca_orders(user_id: int) -> list[dict]:
    """Return list of active DCA orders for user. Each: {id, token_mint, amount_sol, interval_hours, next_run_time}."""
    try:
        uid = int(user_id)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_dca_orders_table(db)
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT id, token_mint, amount_sol, interval_hours, next_run_time
                   FROM dca_orders WHERE user_id = ? AND is_active = 1 ORDER BY next_run_time""",
                (uid,),
            )
            rows = await cursor.fetchall()
            return [
                {
                    "id": int(r["id"]),
                    "token_mint": r["token_mint"],
                    "amount_sol": float(r["amount_sol"]),
                    "interval_hours": float(r["interval_hours"]),
                    "next_run_time": int(r["next_run_time"]),
                }
                for r in rows
            ]
    except Exception as e:
        logger.warning("get_active_dca_orders failed: user_id=%s error=%s", user_id, e)
        return []


async def remove_dca_order(order_id: int, user_id: int) -> bool:
    """Deactivate a DCA order (set is_active = 0) for the given user. Returns True if a row was updated."""
    try:
        oid = int(order_id)
        uid = int(user_id)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_dca_orders_table(db)
            cursor = await db.execute(
                "UPDATE dca_orders SET is_active = 0 WHERE id = ? AND user_id = ?",
                (oid, uid),
            )
            await db.commit()
            return cursor.rowcount > 0
    except Exception as e:
        logger.warning("remove_dca_order failed: order_id=%s user_id=%s error=%s", order_id, user_id, e)
        return False


async def get_due_dca_orders(current_timestamp: int) -> list[dict]:
    """Return DCA orders that are due to run (is_active = 1 and next_run_time <= current_timestamp)."""
    try:
        ts = int(current_timestamp)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_dca_orders_table(db)
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT id, user_id, token_mint, amount_sol, interval_hours
                   FROM dca_orders WHERE is_active = 1 AND next_run_time <= ?""",
                (ts,),
            )
            rows = await cursor.fetchall()
            return [
                {
                    "id": int(r["id"]),
                    "user_id": int(r["user_id"]),
                    "token_mint": r["token_mint"],
                    "amount_sol": float(r["amount_sol"]),
                    "interval_hours": float(r["interval_hours"]),
                }
                for r in rows
            ]
    except Exception as e:
        logger.warning("get_due_dca_orders failed: %s", e)
        return []


async def update_dca_next_run(order_id: int, new_next_run_time: int) -> bool:
    """Update next_run_time for a DCA order. Returns True if a row was updated."""
    try:
        oid = int(order_id)
        ts = int(new_next_run_time)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_dca_orders_table(db)
            cursor = await db.execute(
                "UPDATE dca_orders SET next_run_time = ? WHERE id = ?",
                (ts, oid),
            )
            await db.commit()
            return cursor.rowcount > 0
    except Exception as e:
        logger.warning("update_dca_next_run failed: order_id=%s error=%s", order_id, e)
        return False


# -----------------------------------------------------------------------------
# Copy targets: CRUD (add, list, remove, toggle; get all active targets)
# -----------------------------------------------------------------------------
async def add_copy_target(user_id: int, wallet: str, amount: float) -> bool:
    """Add or update a copy target. Uses INSERT with ON CONFLICT to handle UNIQUE(user_id, target_wallet)."""
    try:
        uid = int(user_id)
        w = (wallet or "").strip()
        if not w:
            return False
        amt = float(amount)
        if amt <= 0:
            return False
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_copy_targets_table(db)
            await db.execute(
                """INSERT INTO copy_targets (user_id, target_wallet, buy_amount_sol, is_active)
                   VALUES (?, ?, ?, 1)
                   ON CONFLICT(user_id, target_wallet) DO UPDATE SET
                       buy_amount_sol = excluded.buy_amount_sol,
                       is_active = 1""",
                (uid, w, amt),
            )
            await db.commit()
        return True
    except Exception as e:
        logger.warning("add_copy_target failed: user_id=%s wallet=%s error=%s", user_id, wallet, e)
        return False


async def get_all_unique_active_targets() -> list[str]:
    """Return distinct target_wallet addresses that are active (is_active = 1). For WSS listener."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_copy_targets_table(db)
            cursor = await db.execute(
                "SELECT DISTINCT target_wallet FROM copy_targets WHERE is_active = 1"
            )
            rows = await cursor.fetchall()
            return [row[0] for row in rows if row[0]]
    except Exception as e:
        logger.warning("get_all_unique_active_targets failed: %s", e)
        return []


async def get_users_copying_target(target_wallet: str) -> list[dict]:
    """Return list of {user_id, buy_amount_sol} for users who are actively copying this target wallet."""
    try:
        w = (target_wallet or "").strip()
        if not w:
            return []
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_copy_targets_table(db)
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT user_id, buy_amount_sol FROM copy_targets WHERE target_wallet = ? AND is_active = 1",
                (w,),
            )
            rows = await cursor.fetchall()
            return [{"user_id": int(r["user_id"]), "buy_amount_sol": float(r["buy_amount_sol"])} for r in rows]
    except Exception as e:
        logger.warning("get_users_copying_target failed: target_wallet=%s error=%s", target_wallet, e)
        return []


async def get_copy_targets(user_id: int) -> list[dict]:
    """Return list of copy targets for user. Each item: {target_wallet, buy_amount_sol, is_active}."""
    try:
        uid = int(user_id)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_copy_targets_table(db)
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT target_wallet, buy_amount_sol, is_active FROM copy_targets WHERE user_id = ? ORDER BY id",
                (uid,),
            )
            rows = await cursor.fetchall()
            return [{"target_wallet": r["target_wallet"], "buy_amount_sol": r["buy_amount_sol"], "is_active": bool(r["is_active"])} for r in rows]
    except Exception as e:
        logger.warning("get_copy_targets failed: user_id=%s error=%s", user_id, e)
        return []


async def remove_copy_target(user_id: int, wallet: str) -> bool:
    """Remove a copy target for the user."""
    try:
        uid = int(user_id)
        w = (wallet or "").strip()
        if not w:
            return False
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_copy_targets_table(db)
            cursor = await db.execute(
                "DELETE FROM copy_targets WHERE user_id = ? AND target_wallet = ?",
                (uid, w),
            )
            await db.commit()
            return cursor.rowcount > 0
    except Exception as e:
        logger.warning("remove_copy_target failed: user_id=%s wallet=%s error=%s", user_id, wallet, e)
        return False


async def toggle_copy_target(user_id: int, wallet: str) -> bool:
    """Flip is_active for the given user and target wallet. Returns True if a row was updated."""
    try:
        uid = int(user_id)
        w = (wallet or "").strip()
        if not w:
            return False
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_copy_targets_table(db)
            cursor = await db.execute(
                "UPDATE copy_targets SET is_active = 1 - is_active WHERE user_id = ? AND target_wallet = ?",
                (uid, w),
            )
            await db.commit()
            return cursor.rowcount > 0
    except Exception as e:
        logger.warning("toggle_copy_target failed: user_id=%s wallet=%s error=%s", user_id, wallet, e)
        return False


# -----------------------------------------------------------------------------
# DB init: create users table and run additive migrations (no data drop)
# -----------------------------------------------------------------------------
@error_decorator
async def create_db_and_table():
    """Create users table if missing; then run additive migrations only. NEVER drop or overwrite user data."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        try:
            cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
            table_exists = await cursor.fetchone() is not None

            if not table_exists:
                logger.info("Creating users table...")
                await db.execute(SCHEMA)
                await db.commit()

            # Additive migrations only: add missing columns. Existing data is preserved.
            await _ensure_referral_columns(db)
            await _ensure_settings_columns(db)

            # Migrate any plaintext private keys to encrypted format (no table drop)
            try:
                await encrypt_all_plaintext_keys()
            except Exception as e:
                logger.warning("encrypt_all_plaintext_keys during init: %s", e)

            logger.info("Database initialization completed (additive migrations only).")
            return True
        except Exception as e:
            logger.error("Error in create_db_and_table: %s", e)
            raise

# -----------------------------------------------------------------------------
# Referral, safe mode, onboarding (referral columns migration)
# -----------------------------------------------------------------------------
async def _ensure_referral_columns(db):
    """Add referral columns if they don't exist (safe for existing DBs)."""
    cursor = await db.execute("PRAGMA table_info(users)")
    existing = {row[1] for row in await cursor.fetchall()}
    if "referred_by" not in existing:
        await db.execute("ALTER TABLE users ADD COLUMN referred_by INTEGER")
    if "referral_count" not in existing:
        await db.execute("ALTER TABLE users ADD COLUMN referral_count INTEGER DEFAULT 0")
    if "referral_commissions_sol" not in existing:
        await db.execute("ALTER TABLE users ADD COLUMN referral_commissions_sol REAL DEFAULT 0.0")
    if "cashback_earned_sol" not in existing:
        await db.execute("ALTER TABLE users ADD COLUMN cashback_earned_sol REAL DEFAULT 0.0")
    if "total_withdrawn_sol" not in existing:
        await db.execute("ALTER TABLE users ADD COLUMN total_withdrawn_sol REAL DEFAULT 0.0")
    if "wallets" not in existing:
        await db.execute("ALTER TABLE users ADD COLUMN wallets TEXT DEFAULT '[]'")
    if "default_wallet_name" not in existing:
        await db.execute("ALTER TABLE users ADD COLUMN default_wallet_name TEXT DEFAULT 'W1'")
    if "safe_mode" not in existing:
        await db.execute("ALTER TABLE users ADD COLUMN safe_mode INTEGER DEFAULT 1")
    await db.commit()


async def get_safe_mode(user_id) -> bool:
    """Return True if safe_mode is enabled for this user (default True)."""
    try:
        uid = int(user_id)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_referral_columns(db)
            cursor = await db.execute("SELECT safe_mode FROM users WHERE user_id = ?", (uid,))
            row = await cursor.fetchone()
            if row is None:
                cursor = await db.execute("SELECT safe_mode FROM users WHERE user_id = ?", (str(uid),))
                row = await cursor.fetchone()
            return bool(row[0]) if row and row[0] is not None else True
    except Exception as e:
        logger.warning("get_safe_mode failed: %s", e)
        return True


async def toggle_safe_mode(user_id) -> bool:
    """Toggle safe_mode for a user. Returns the NEW value after toggle."""
    try:
        uid = int(user_id)
        current = await get_safe_mode(uid)
        new_val = 0 if current else 1
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_referral_columns(db)
            await db.execute("UPDATE users SET safe_mode = ? WHERE user_id = ?", (new_val, uid))
            if db.total_changes == 0:
                await db.execute("UPDATE users SET safe_mode = ? WHERE user_id = ?", (new_val, str(uid)))
            await db.commit()
        return bool(new_val)
    except Exception as e:
        logger.warning("toggle_safe_mode failed: %s", e)
        return True


async def delete_test_users(user_ids: list) -> int:
    """Remove users by id (for referral test cleanup). Returns number of rows deleted."""
    if not user_ids:
        return 0
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            placeholders = ",".join("?" * len(user_ids))
            cursor = await db.execute(
                f"DELETE FROM users WHERE user_id IN ({placeholders})",
                [int(uid) for uid in user_ids],
            )
            n = cursor.rowcount
            await db.commit()
            return n
    except Exception as e:
        logger.warning("delete_test_users failed: %s", e)
        return 0


async def increment_referral_count(referrer_id) -> bool:
    """Increment referral_count for the referrer. Returns True if updated. Call _ensure_referral_columns before first use."""
    try:
        rid = int(referrer_id)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_referral_columns(db)
            cursor = await db.execute(
                "UPDATE users SET referral_count = COALESCE(referral_count, 0) + 1 WHERE user_id = ?",
                (rid,),
            )
            if cursor.rowcount == 0:
                cursor = await db.execute(
                    "UPDATE users SET referral_count = COALESCE(referral_count, 0) + 1 WHERE user_id = ?",
                    (str(rid),),
                )
            await db.commit()
            return cursor.rowcount > 0
    except Exception as e:
        logger.warning("increment_referral_count failed: %s", e)
        return False


async def add_referral_commission(referrer_id, amount_sol) -> bool:
    """Add amount_sol to referrer's referral_commissions_sol. Safe for missing column; DB errors are logged, not raised."""
    try:
        rid = int(referrer_id)
        amt = float(amount_sol)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_referral_columns(db)
            await db.execute(
                "UPDATE users SET referral_commissions_sol = COALESCE(referral_commissions_sol, 0) + ? WHERE user_id = ?",
                (amt, rid),
            )
            await db.commit()
        return True
    except Exception as e:
        logger.warning("add_referral_commission failed: referrer_id=%s amount=%s error=%s", referrer_id, amount_sol, e)
        return False


async def add_cashback_earned(user_id, amount_sol) -> bool:
    """Add 10% fee rebate to user's cashback_earned_sol. Safe for missing column."""
    try:
        uid = int(user_id)
        amt = float(amount_sol)
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_referral_columns(db)
            await db.execute(
                "UPDATE users SET cashback_earned_sol = COALESCE(cashback_earned_sol, 0) + ? WHERE user_id = ?",
                (amt, uid),
            )
            await db.commit()
        return True
    except Exception as e:
        logger.warning("add_cashback_earned failed: user_id=%s amount=%s error=%s", user_id, amount_sol, e)
        return False


async def get_onboarding_stats() -> tuple[int, int]:
    """Return (total_users, new_users_last_24h). Counts all rows in users (everyone who clicked /start)."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total = (await cursor.fetchone())[0]
            await cursor.close()
            # created_at is TIMESTAMP; SQLite: last 24 hours
            cursor = await db.execute(
                "SELECT COUNT(*) FROM users WHERE created_at >= datetime('now', '-1 day')"
            )
            new_24h = (await cursor.fetchone())[0]
            await cursor.close()
            return int(total), int(new_24h)
    except Exception as e:
        logger.warning("get_onboarding_stats failed: %s", e)
        return 0, 0


async def get_user_wallet_stats() -> dict:
    """Return total users, users with wallet set (active), and pending setup (no wallet).
    'NOT_SET' is the default wallet_address before a wallet is generated or imported."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total = (await cursor.fetchone())[0]
            await cursor.close()
            cursor = await db.execute(
                "SELECT COUNT(*) FROM users WHERE wallet_address IS NOT NULL AND wallet_address != '' AND wallet_address != 'NOT_SET'"
            )
            active = (await cursor.fetchone())[0]
            await cursor.close()
            total, active = int(total), int(active)
            return {"total": total, "active": active, "pending": total - active}
    except Exception as e:
        logger.error("Error fetching user wallet stats: %s", e)
        return {"total": 0, "active": 0, "pending": 0}


# -----------------------------------------------------------------------------
# Users (core): insert, get_user, get_keypair, update_user
# -----------------------------------------------------------------------------
@error_decorator
async def insert_user(user_id, wallet_address, private_key, trades, slippage, monitor_wallet, referred_by=None):
    uid = int(user_id)
    logger.info("insert_user: user_id=%s wallet=%s referred_by=%s", uid, wallet_address, referred_by)
    try:
        if not wallet_address or not wallet_address.strip():
            raise ValueError("Wallet address cannot be empty")
        
        if isinstance(trades, str):
            trades = json.loads(trades)
        if not isinstance(trades, dict):
            trades = {}
        
        if isinstance(monitor_wallet, str):
            monitor_wallet = json.loads(monitor_wallet)
        if not isinstance(monitor_wallet, dict):
            monitor_wallet = {}
        
        encrypted_key, salt = encryption_mgr.encrypt_private_key(private_key, uid)
        
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await db.execute(SCHEMA)
            await _ensure_referral_columns(db)
            
            if referred_by is not None:
                insert_data = (
                    uid,
                    wallet_address.strip(),
                    encrypted_key,
                    salt,
                    json.dumps(trades),
                    slippage,
                    json.dumps(monitor_wallet),
                    int(referred_by),
                )
                await db.execute('''
                    INSERT INTO users (
                        user_id, wallet_address, encrypted_private_key, key_salt,
                        trades, slippage, monitor_wallet, wallets, default_wallet_name,
                        referred_by, last_access, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, '[]', 'W1', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ''', insert_data)
            else:
                insert_data = (
                    uid,
                    wallet_address.strip(),
                    encrypted_key,
                    salt,
                    json.dumps(trades),
                    slippage,
                    json.dumps(monitor_wallet)
                )
                await db.execute('''
                    INSERT INTO users (
                        user_id, wallet_address, encrypted_private_key, key_salt,
                        trades, slippage, monitor_wallet, wallets, default_wallet_name,
                        last_access, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, '[]', 'W1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ''', insert_data)
            
            await db.commit()
            logger.info("insert_user: user_id=%s SUCCESS", uid)
                
    except Exception as e:
        logger.error("insert_user: user_id=%s FAILED: %s", uid, e)
        raise
    
    encryption_mgr.securely_wipe(private_key)


async def get_user(user_id):
    uid = int(user_id)
    async with _db_lock:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row

            # First check if last_access column exists
            cursor = await db.execute("PRAGMA table_info(users)")
            columns = await cursor.fetchall()
            column_names = {col[1] for col in columns}

            cursor = await db.execute(
                'SELECT * FROM users WHERE CAST(user_id AS TEXT) = CAST(? AS TEXT)',
                (user_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()

            if row is None:
                logger.warning("get_user: NO row for user_id=%s (CAST TEXT match)", uid)
                return None

            if row is not None:
                user_data = dict(row)
                # Ensure wallet_address is present (SELECT * includes it; log if missing after migration)
                if 'wallet_address' not in user_data:
                    logger.warning(
                        "get_user: user_id=%s row missing 'wallet_address' (keys=%s); check schema",
                        uid, list(user_data.keys()),
                    )
                # The actual stored value (may be int or str depending on insert path)
                stored_uid = user_data.get('user_id', uid)
                
                # Removed last_access UPDATE here. 
                # get_user() is called hundreds of times per minute. Doing a WRITE (UPDATE) 
                # on every read causes intense SQLite lock contention between the Bot UI 
                # and Limit Engine processes, causing snipes and UI refreshes to freeze.

                # If we need the decrypted private key, use cache or decrypt
                if 'encrypted_private_key' in user_data and user_data['encrypted_private_key']:
                    cache_key = str(uid)
                    now = time.monotonic()
                    cached = _decrypted_key_cache.get(cache_key)
                    if cached is not None and cached[1] > now:
                        user_data['private_key'] = cached[0]
                    else:
                        try:
                            decrypted_key = encryption_mgr.decrypt_private_key(
                                user_data['encrypted_private_key'],
                                user_data['key_salt'],
                                str(uid)
                            )
                            user_data['private_key'] = decrypted_key
                            _decrypted_key_cache[cache_key] = (decrypted_key, now + _DECRYPT_CACHE_TTL)
                        except DecryptionKeyMismatch:
                            logger.error(
                                "\n" + "=" * 70 +
                                "\nCRITICAL: Your encryption.key is different from the one"
                                "\n   used to create this wallet.  You must either:"
                                "\n   1) Restore the ORIGINAL secure_storage/encryption.key, OR"
                                "\n   2) Re-import your wallet private key (/wallet > Set Wallet)."
                                "\n" + "=" * 70
                            )
                            user_data['private_key'] = None
                        except Exception as e:
                            logger.error("Error decrypting private key: %s", e)
                            user_data['private_key'] = None
                    del user_data['encrypted_private_key']
                    del user_data['key_salt']
                wallet = user_data.get('wallet_address', 'MISSING')
                has_key = bool(user_data.get('private_key'))
                # Silenced to avoid flooding limit_engine/UI logs; only warnings/errors are visible
                # logger.debug("get_user: user_id=%s wallet=%s has_key=%s", uid, wallet, has_key)
                return user_data
            return None


async def get_users_batch(user_ids: list[int]) -> dict[int, dict]:
    """Fetch multiple users in one lock-acquiring DB round-trip. Returns dict user_id -> user_data (with decrypted private_key).
    Used by the limit engine to avoid N serial get_user calls per tick. Skips last_access update to keep critical section short."""
    if not user_ids:
        return {}
    uids = list({int(uid) for uid in user_ids})
    async with _db_lock:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row
            placeholders = ",".join("?" * len(uids))
            params = [str(uid) for uid in uids]
            cursor = await db.execute(
                f"SELECT * FROM users WHERE user_id IN ({placeholders})",
                params,
            )
            rows = await cursor.fetchall()
            await cursor.close()
            cursor = await db.execute("PRAGMA table_info(users)")
            columns = await cursor.fetchall()
            column_names = {col[1] for col in columns}
            await cursor.close()
            out = {}
            for row in rows:
                user_data = dict(row)
                uid = int(user_data.get("user_id", 0))
                if "encrypted_private_key" in user_data and user_data.get("encrypted_private_key"):
                    cache_key = str(uid)
                    now = time.monotonic()
                    cached = _decrypted_key_cache.get(cache_key)
                    if cached is not None and cached[1] > now:
                        user_data["private_key"] = cached[0]
                    else:
                        try:
                            decrypted_key = encryption_mgr.decrypt_private_key(
                                user_data["encrypted_private_key"],
                                user_data.get("key_salt"),
                                str(uid),
                            )
                            user_data["private_key"] = decrypted_key
                            _decrypted_key_cache[cache_key] = (decrypted_key, now + _DECRYPT_CACHE_TTL)
                        except DecryptionKeyMismatch:
                            logger.error("get_users_batch: encryption key mismatch for user_id=%s", uid)
                            user_data["private_key"] = None
                        except Exception as e:
                            logger.error("Error decrypting private key (batch) for user %s: %s", uid, e)
                            user_data["private_key"] = None
                    del user_data["encrypted_private_key"]
                    if "key_salt" in user_data:
                        del user_data["key_salt"]
                out[uid] = user_data
            return out


async def get_keypair(user_id):
    """Return a cached or newly built Keypair for the user. Invalidated when private_key is updated."""
    import base58
    from solders.keypair import Keypair
    uid = str(user_id)
    now = time.monotonic()
    cached = _keypair_cache.get(uid)
    if cached is not None and cached[1] > now:
        return cached[0]
    user_data = await get_user(user_id=user_id)
    if not user_data or not user_data.get('private_key'):
        return None
    pk = user_data['private_key']
    try:
        decoded = base58.b58decode(pk)
        kp = Keypair.from_bytes(decoded)
        _keypair_cache[uid] = (kp, now + _DECRYPT_CACHE_TTL)
        return kp
    except Exception as e:
        logger.error("Error building keypair for user %s: %s", user_id, e)
        return None


@error_decorator
async def update_user(user_id, wallet_address=None, private_key=None, trades=None, slippage=None, monitor_wallet=None, withdraw_wallet=None, last_deposit_address=None):
    uid = int(user_id)
    logger.info("update_user: user_id=%s wallet_change=%s key_change=%s", uid, wallet_address is not None, private_key is not None)
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        updates = {}
        
        # Validate wallet address if provided
        if wallet_address is not None:
            if not wallet_address or not wallet_address.strip():
                raise ValueError("Wallet address cannot be empty")
            updates['wallet_address'] = wallet_address.strip()
        
        # Handle private key encryption
        if private_key is not None:
            _invalidate_decrypt_cache(uid)
            encrypted_key, salt = encryption_mgr.encrypt_private_key(private_key, uid)
            updates['encrypted_private_key'] = encrypted_key
            updates['key_salt'] = salt
            encryption_mgr.securely_wipe(private_key)
        
        # Ensure proper JSON format for trades
        if trades is not None:
            if isinstance(trades, str):
                try:
                    trades = json.loads(trades)
                except Exception:
                    trades = {}
            if not isinstance(trades, dict):
                trades = {}
            updates['trades'] = json.dumps(trades)
        
        # Ensure proper JSON format for monitor_wallet
        if monitor_wallet is not None:
            if isinstance(monitor_wallet, str):
                try:
                    monitor_wallet = json.loads(monitor_wallet)
                except Exception:
                    monitor_wallet = {}
            if not isinstance(monitor_wallet, dict):
                monitor_wallet = {}
            updates['monitor_wallet'] = json.dumps(monitor_wallet)
        if slippage is not None:
            updates['slippage'] = slippage
            
        if withdraw_wallet is not None:
            updates['withdraw_wallet'] = withdraw_wallet
        if last_deposit_address is not None:
            updates['last_deposit_address'] = last_deposit_address

        # Construct the SQL query dynamically
        if updates:
            update_clause = ', '.join(f"{key} = ?" for key in updates)
            # Try integer first; if 0 rows affected, try string (insert_user stores str(user_id))
            sql_query = f"UPDATE users SET {update_clause} WHERE user_id = ?"
            values = list(updates.values())
            cursor = await db.execute(sql_query, values + [uid])
            if cursor.rowcount == 0:
                cursor = await db.execute(sql_query, values + [str(uid)])
            await db.commit()
            logger.info("update_user: user_id=%s rows_affected=%s SUCCESS", uid, cursor.rowcount)
        else:
            logger.warning("update_user: user_id=%s no updates specified", uid)


# ---------------------------------------------------------------------------
# Trade-mode settings (MEV / Turbo / Jito tip)
# ---------------------------------------------------------------------------

_TRADE_SETTING_COLS = (
    "priority_mode",
    "mev_mode",
    "mev_enabled",
    "mev_level",
    "turbo_mode",
    "jito_tip_amount",
    "jito_tip_low",
    "jito_tip_mid",
    "jito_tip_pro",
)


# -----------------------------------------------------------------------------
# Trade settings: priority_mode, Jito tips, slippage presets (per user)
# -----------------------------------------------------------------------------
async def _ensure_trade_setting_columns(db):
    """Add columns if they don't exist yet (safe for existing DBs)."""
    cursor = await db.execute("PRAGMA table_info(users)")
    existing = {row[1] for row in await cursor.fetchall()}
    migrations = {
        "priority_mode": "ALTER TABLE users ADD COLUMN priority_mode TEXT DEFAULT 'Standard'",
        "mev_mode": "ALTER TABLE users ADD COLUMN mev_mode TEXT DEFAULT 'Low'",
        "mev_enabled": "ALTER TABLE users ADD COLUMN mev_enabled INTEGER DEFAULT 0",
        "mev_level": "ALTER TABLE users ADD COLUMN mev_level TEXT DEFAULT 'Low'",
        "turbo_mode": "ALTER TABLE users ADD COLUMN turbo_mode INTEGER DEFAULT 1",
        "jito_tip_amount": "ALTER TABLE users ADD COLUMN jito_tip_amount REAL DEFAULT 0.0005",
        "jito_tip_low": "ALTER TABLE users ADD COLUMN jito_tip_low REAL DEFAULT 0.0005",
        "jito_tip_mid": "ALTER TABLE users ADD COLUMN jito_tip_mid REAL DEFAULT 0.001",
        "jito_tip_pro": "ALTER TABLE users ADD COLUMN jito_tip_pro REAL DEFAULT 0.005",
    }
    for col, ddl in migrations.items():
        if col not in existing:
            await db.execute(ddl)
    # Backfill priority_mode for existing rows (derive from mev_enabled/mev_level)
    if "priority_mode" in migrations and ("priority_mode" not in existing):
        await db.execute(
            """
            UPDATE users SET priority_mode = CASE
                WHEN COALESCE(mev_enabled, 0) = 0 THEN 'Standard'
                WHEN LOWER(TRIM(COALESCE(mev_level, 'Low'))) IN ('low', 'mid') THEN 'Fast'
                ELSE 'Turbo'
            END WHERE priority_mode IS NULL OR TRIM(priority_mode) = ''
            """
        )
    # Backfill for existing rows (SQLite adds NULL for prior rows).
    # Preserve legacy behavior: if MEV was OFF -> mev_mode=Off; otherwise Low/High based on mev_level.
    if "mev_mode" in migrations and ("mev_mode" not in existing):
        await db.execute(
            """
            UPDATE users
               SET mev_mode =
                 CASE
                   WHEN COALESCE(mev_enabled, 0) = 0 THEN 'Off'
                   WHEN TRIM(COALESCE(mev_level, 'Low')) = 'Low' THEN 'Low'
                   ELSE 'High'
                 END
             WHERE mev_mode IS NULL OR TRIM(mev_mode) = ''
            """
        )
    await db.commit()


def _default_trade_settings() -> dict:
    """Default trade settings when user has no row."""
    return {
        "priority_mode": "Standard",
        "mev_mode": "Off",
        "mev_enabled": False,
        "mev_level": "Low",
        "turbo_mode": True,
        # Effective Jito tip for Standard/Fast/Turbo modes
        "jito_tip_amount": 0.0005,
        "jito_tip_low": 0.0005,
        "jito_tip_mid": 0.001,
        "jito_tip_pro": 0.005,
    }


async def get_user_trade_settings(user_id) -> dict:
    """Return trade settings including 3-level MEV tip presets."""
    uid = int(user_id)
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_trade_setting_columns(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT priority_mode, mev_mode, mev_enabled, mev_level, turbo_mode, jito_tip_amount, jito_tip_low, jito_tip_mid, jito_tip_pro FROM users WHERE user_id = ?",
            (uid,),
        )
        row = await cursor.fetchone()
        if row is None:
            cursor = await db.execute(
                "SELECT priority_mode, mev_mode, mev_enabled, mev_level, turbo_mode, jito_tip_amount, jito_tip_low, jito_tip_mid, jito_tip_pro FROM users WHERE user_id = ?",
                (str(uid),),
            )
            row = await cursor.fetchone()
        if row is None:
            return _default_trade_settings()

        # Priority Mode (Standard / Fast / Turbo) is the single source of truth for execution
        raw_priority = (row["priority_mode"] or "").strip()
        if raw_priority in ("Standard", "Fast", "Turbo"):
            # Map execution mode -> effective Jito tip (SOL)
            tip_by_priority = {"Standard": 0.0005, "Fast": 0.001, "Turbo": 0.0025}
            effective_tip = tip_by_priority[raw_priority]
            if raw_priority == "Standard":
                effective_mev_enabled = False
                effective_mev_level = "Low"
                effective_turbo = True
            else:
                effective_mev_enabled = True
                effective_mev_level = "Low" if raw_priority == "Fast" else "Pro"
                effective_turbo = False
            return {
                "priority_mode": raw_priority,
                "mev_mode": "Off" if raw_priority == "Standard" else ("Low" if raw_priority == "Fast" else "High"),
                "mev_enabled": effective_mev_enabled,
                "mev_level": effective_mev_level,
                "turbo_mode": effective_turbo,
                "jito_tip_amount": effective_tip,
                "jito_tip_low": float(row["jito_tip_low"] or 0.0005),
                "jito_tip_mid": float(row["jito_tip_mid"] or 0.001),
                "jito_tip_pro": float(row["jito_tip_pro"] or 0.005),
            }

        raw_level = (row["mev_level"] or "Low").strip()
        if raw_level == "Basic":
            normalized_level = "Low"
        elif raw_level == "Enhanced":
            normalized_level = "Pro"
        elif raw_level in ("Low", "Mid", "Pro"):
            normalized_level = raw_level
        else:
            normalized_level = "Low"

        raw_mode = (row["mev_mode"] or "").strip()
        if raw_mode in ("Off", "Low", "High"):
            normalized_mode = raw_mode
        else:
            # Derive mode from legacy fields for backward compatibility
            if not bool(row["mev_enabled"]):
                normalized_mode = "Off"
            elif normalized_level == "Low":
                normalized_mode = "Low"
            else:
                normalized_mode = "High"

        # Make returned legacy flags consistent with mev_mode (single source of truth for UI+engine).
        if normalized_mode == "Off":
            effective_mev_enabled = False
            effective_mev_level = normalized_level
        elif normalized_mode == "Low":
            effective_mev_enabled = True
            effective_mev_level = "Low"
        else:  # High
            effective_mev_enabled = True
            effective_mev_level = "Pro"

        # Preserve mutual exclusivity (if MEV is on, Turbo is effectively off)
        effective_turbo = bool(row["turbo_mode"]) and not effective_mev_enabled
        # Derive priority_mode from legacy mev_mode for backward compat
        derived_priority = "Standard" if normalized_mode == "Off" else ("Fast" if normalized_mode == "Low" else "Turbo")

        return {
            "priority_mode": derived_priority,
            "mev_mode": normalized_mode,
            "mev_enabled": effective_mev_enabled,
            "mev_level": effective_mev_level,
            "turbo_mode": effective_turbo,
            "jito_tip_amount": float(row["jito_tip_amount"] or 0.0005),
            "jito_tip_low": float(row["jito_tip_low"] or 0.0005),
            "jito_tip_mid": float(row["jito_tip_mid"] or 0.001),
            "jito_tip_pro": float(row["jito_tip_pro"] or 0.005),
        }


async def update_user_trade_settings(user_id, **kwargs) -> bool:
    """Update one or more of: mev_mode, mev_enabled, mev_level, turbo_mode, jito_tip_amount."""
    uid = int(user_id)
    allowed = set(_TRADE_SETTING_COLS)
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return False
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_trade_setting_columns(db)
        set_clause = ", ".join(f"{col} = ?" for col in updates)
        values = list(updates.values()) + [uid]
        await db.execute(f"UPDATE users SET {set_clause} WHERE user_id = ?", values)
        if db.total_changes == 0:
            values[-1] = str(uid)
            await db.execute(f"UPDATE users SET {set_clause} WHERE user_id = ?", values)
        await db.commit()
    return True


# ---------------------------------------------------------------------------
# Pro settings: single source of truth for every UI setting (persists across restarts)
# ---------------------------------------------------------------------------
# (col_name, sql_type, default_value) — used for migration and get/update. Never drop table.

_SETTINGS_SCHEMA = (
    ("slippage_turbo", "REAL", 1.0),
    ("slippage_mev", "REAL", 1.0),           # UI: Anti-MEV Slippage
    ("slippage_sniper", "REAL", 20.0),       # UI: Snipe Slippage
    ("slippage_bonding", "REAL", 15.0),
    ("prio_fee_buy", "REAL", 0.002),
    ("prio_fee_sell", "REAL", 0.002),
    ("prio_fee_snipe", "REAL", 0.035),
    ("prio_fee_bonding", "REAL", 0.005),
    ("auto_buy_amount", "REAL", 0.0),
    ("auto_buy_saved_amount", "REAL", 0.0),
    ("buy_amount_1", "REAL", 0.1),
    ("buy_amount_2", "REAL", 0.25),
    ("buy_amount_3", "REAL", 0.5),
    ("buy_amount_4", "REAL", 1.0),
    ("sell_percent_1", "INTEGER", 25),
    ("sell_percent_2", "INTEGER", 50),
    ("sell_percent_3", "INTEGER", 100),
    ("auto_sell_enabled", "INTEGER", 0),
    ("auto_sell_tp_pct", "REAL", 20.0),
    ("auto_sell_sl_pct", "REAL", -50.0),
    ("auto_sell_amount_pct", "REAL", 100.0),
    ("auto_sell_trailing", "INTEGER", 0),
    ("confirm_trades_enabled", "INTEGER", 0),
    ("risk_alert_enabled", "INTEGER", 1),
    ("auto_buy_enabled", "INTEGER", 0),       # UI: CA Auto Buy
    ("anti_mev_level", "TEXT", "Basic"),     # UI: Anti-MEV mode
    ("default_buy_amount", "REAL", 0.1),
    # Global Trading Shield (applies to every new trade when auto_arm_shield is True)
    ("global_tp_pct", "REAL", 100.0),
    ("global_sl_pct", "REAL", -20.0),
    ("auto_arm_shield", "INTEGER", 1),        # 1 = arm shield on every buy
    ("rug_protection", "INTEGER", 0),         # 1 = 50% slippage on Trading Shield Stop-Loss
    # Social metadata filter for automated snipes (TG sniper / auto-buys only; manual buys are never blocked).
    ("social_filter_enabled", "INTEGER", 1),
    # TG Sniper execution mode: "safe" (default) or "degen"
    ("sniper_mode", "TEXT", "safe"),
)

_SETTINGS_COLS = tuple(c[0] for c in _SETTINGS_SCHEMA)
_SETTINGS_DEFAULTS = {c[0]: c[2] for c in _SETTINGS_SCHEMA}


# -----------------------------------------------------------------------------
# User settings (pro): slippage, auto_buy, safe_mode, etc. (grid/settings UI)
# -----------------------------------------------------------------------------
def _settings_sql_default(col: str, sql_type: str, default) -> str:
    """Return SQL default literal for ALTER TABLE."""
    if sql_type == "TEXT":
        if default is None:
            return "DEFAULT NULL"
        esc = str(default).replace("'", "''")
        return f"DEFAULT '{esc}'"
    if sql_type == "INTEGER":
        return f"DEFAULT {int(default)}"
    return f"DEFAULT {float(default)}"


async def _ensure_settings_columns(db):
    """Add every settings column if missing. Uses PRAGMA table_info; only ALTER ADD. Never drop or overwrite data."""
    cursor = await db.execute("PRAGMA table_info(users)")
    existing = {row[1] for row in await cursor.fetchall()}
    for col, sql_type, default in _SETTINGS_SCHEMA:
        if col not in existing:
            default_sql = _settings_sql_default(col, sql_type, default)
            ddl = f"ALTER TABLE users ADD COLUMN {col} {sql_type} {default_sql}"
            try:
                await db.execute(ddl)
                logger.info("Migration: added column %s to users", col)
            except Exception as e:
                logger.warning("Migration: could not add %s: %s", col, e)
    await db.commit()


# Casting for get_user_settings (derived from _SETTINGS_SCHEMA)
_SETTINGS_INT_COLS = {c[0] for c in _SETTINGS_SCHEMA if c[1] == "INTEGER"}
_SETTINGS_STR_COLS = {c[0] for c in _SETTINGS_SCHEMA if c[1] == "TEXT"}


async def get_user_settings(user_id) -> dict:
    """Return all pro-settings from DB. Uses defaults only when user has no row or column is NULL (no in-memory overrides)."""
    uid = int(user_id)
    cols = ", ".join(_SETTINGS_COLS)
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_settings_columns(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(f"SELECT {cols} FROM users WHERE user_id = ?", (uid,))
        row = await cursor.fetchone()
        if row is None:
            cursor = await db.execute(f"SELECT {cols} FROM users WHERE user_id = ?", (str(uid),))
            row = await cursor.fetchone()
        if row is None:
            return dict(_SETTINGS_DEFAULTS)
        result = {}
        for col in _SETTINGS_COLS:
            val = row[col]
            if val is None:
                result[col] = _SETTINGS_DEFAULTS[col]
            elif col in _SETTINGS_INT_COLS:
                result[col] = int(val)
            elif col in _SETTINGS_STR_COLS:
                result[col] = str(val).strip() if val else _SETTINGS_DEFAULTS[col]
            else:
                result[col] = float(val)
        return result


async def reset_auto_buy_on_startup() -> int:
    """Set Auto Buy to OFF (0) for all users. Call once on bot startup. Returns number of rows updated."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_settings_columns(db)
            cursor = await db.execute("UPDATE users SET auto_buy_amount = 0.0 WHERE auto_buy_amount != 0 OR auto_buy_amount IS NULL")
            n = cursor.rowcount
            await db.commit()
            if n and n > 0:
                logger.info("STARTUP: Auto Buy set to OFF for all users (rows=%s)", n)
            return n or 0
    except Exception as e:
        logger.warning("STARTUP: reset_auto_buy_on_startup failed: %s", e)
        return 0


async def update_user_settings(user_id, **kwargs) -> bool:
    """Update one or more pro-settings columns."""
    uid = int(user_id)
    allowed = set(_SETTINGS_COLS)
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return False
    for key, value in updates.items():
        logger.info("DB UPDATE: User %s setting %s set to %s", uid, key, value)
    async with _db_lock:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_settings_columns(db)
            set_clause = ", ".join(f"{col} = ?" for col in updates)
            values = list(updates.values()) + [uid]
            await db.execute(f"UPDATE users SET {set_clause} WHERE user_id = ?", values)
            if db.total_changes == 0:
                values[-1] = str(uid)
                await db.execute(f"UPDATE users SET {set_clause} WHERE user_id = ?", values)
            await db.commit()
    return True


async def get_global_shield_settings(user_id) -> dict:
    """Return global Trading Shield settings: global_tp_pct, global_sl_pct, auto_arm_shield."""
    settings = await get_user_settings(user_id)
    return {
        "global_tp_pct": float(settings.get("global_tp_pct", 100.0)),
        "global_sl_pct": float(settings.get("global_sl_pct", -20.0)),
        "auto_arm_shield": bool(settings.get("auto_arm_shield", 1)),
    }


async def get_user_social_filter(user_id) -> bool:
    """Return True if the Social Metadata Filter is enabled for this user (default: True)."""
    settings = await get_user_settings(user_id)
    return bool(settings.get("social_filter_enabled", 1))


async def toggle_user_social_filter(user_id) -> bool:
    """Flip social_filter_enabled for this user and return the new boolean value."""
    settings = await get_user_settings(user_id)
    current = bool(settings.get("social_filter_enabled", 1))
    new_val = 0 if current else 1
    await update_user_settings(user_id, social_filter_enabled=new_val)
    return bool(new_val)


async def update_global_shield(user_id, tp: float | None = None, sl: float | None = None, enabled: bool | None = None) -> bool:
    """Update global Trading Shield. Pass tp (global_tp_pct), sl (global_sl_pct), enabled (auto_arm_shield)."""
    updates = {}
    if tp is not None:
        updates["global_tp_pct"] = float(tp)
    if sl is not None:
        updates["global_sl_pct"] = float(sl)
    if enabled is not None:
        updates["auto_arm_shield"] = 1 if enabled else 0
    if not updates:
        return False
    return await update_user_settings(user_id, **updates)


async def get_user_ids_with_auto_arm_shield() -> list:
    """Return user_id list for users who have auto_arm_shield enabled (1 or NULL). For limit engine global fallback."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_settings_columns(db)
            cursor = await db.execute(
                "SELECT user_id FROM users WHERE COALESCE(auto_arm_shield, 1) = 1"
            )
            rows = await cursor.fetchall()
            return [int(r[0]) for r in rows] if rows else []
    except Exception as e:
        logger.warning("get_user_ids_with_auto_arm_shield failed: %s", e)
        return []


# -----------------------------------------------------------------------------
# Wallets (multi-wallet): add_wallet, get_user_wallets, set_default, unbind, rename
# -----------------------------------------------------------------------------
@error_decorator
async def add_wallet(user_id, wallet_address, private_key, name=""):
    """
    Add a wallet to a user's additional wallets list.
    This function only adds to the wallets JSON array, it doesn't modify the main wallet fields.
    """
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            logger.info("Adding additional wallet for user %s", user_id)
            
            # Get current user data including wallets and main wallet
            cursor = await db.execute(
                'SELECT wallet_address, wallets FROM users WHERE user_id = ?', 
                (str(user_id),)
            )
            row = await cursor.fetchone()
            
            if row is None:
                logger.warning("No user found with ID %s", user_id)
                return False
            
            main_wallet_address = row[0]
            
            # Don't add if it matches the main wallet
            if wallet_address == main_wallet_address:
                logger.info("This wallet is already set as the main wallet")
                return False
            
            # Parse existing wallets
            try:
                current_wallets = row[1] if row[1] else '[]'
                logger.debug("Current wallets: %s", current_wallets)
                wallets = json.loads(current_wallets)
                
                # Check if wallet already exists in additional wallets
                if any(w.get('address') == wallet_address for w in wallets):
                    logger.info("This wallet is already in the additional wallets list")
                    return False
                # Dual-wallet: max 1 additional (W2)
                if len(wallets) >= 1:
                    logger.warning("Max 2 wallets (W1, W2) reached. Unbind one first.")
                    return False
                
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("Error parsing wallets JSON: %s", e)
                wallets = []
            
            # Encrypt private key before storage
            encrypted_key, salt = encryption_mgr.encrypt_private_key(private_key, user_id)
            
            # Add new wallet to additional wallets with encrypted key (W2)
            new_wallet = {
                "name": (name or "W2").strip() or "W2",
                "address": wallet_address,
                "encrypted_private_key": encrypted_key,
                "key_salt": salt,
                "type": "devnet",
                "date_added": datetime.now().isoformat()
            }
            wallets.append(new_wallet)
            
            # Securely wipe the private key from memory
            encryption_mgr.securely_wipe(private_key)
            
            # Convert to JSON and update database
            wallets_json = json.dumps(wallets)
            
            # Update only the wallets column
            await db.execute(
                'UPDATE users SET wallets = ? WHERE user_id = ?', 
                (wallets_json, str(user_id))
            )
            await db.commit()
            
            return True
    except Exception as e:
        logger.error("Error in add_wallet: %s", e)
        return False


async def get_user_wallets(user_id) -> list[dict]:
    """
    Return list of wallets for user. Each: {address, name, is_default}.
    Main wallet (users.wallet_address) first, then entries from users.wallets (max 1 for W2).
    """
    uid = int(user_id)
    out = []
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_referral_columns(db)
        cursor = await db.execute(
            "SELECT wallet_address, default_wallet_name, wallets FROM users WHERE user_id = ?",
            (uid,),
        )
        row = await cursor.fetchone()
        if row is None:
            cursor = await db.execute(
                "SELECT wallet_address, default_wallet_name, wallets FROM users WHERE user_id = ?",
                (str(uid),),
            )
            row = await cursor.fetchone()
        if row is None:
            return []
        main_addr = (row[0] or "").strip()
        default_name = (row[1] or "W1").strip() or "W1"
        wallets_json = row[2] if row[2] else "[]"
        if main_addr and main_addr != "NOT_SET":
            out.append({"address": main_addr, "name": default_name, "is_default": True})
        try:
            extra = json.loads(wallets_json)
            if isinstance(extra, list):
                for w in extra[:1]:  # max 1 for dual-wallet (W2)
                    addr = (w.get("address") or "").strip()
                    if addr and addr != main_addr:
                        out.append({
                            "address": addr,
                            "name": (w.get("name") or "W2").strip() or "W2",
                            "is_default": False,
                        })
        except (TypeError, json.JSONDecodeError):
            pass
    return out


async def set_default_wallet(user_id, address: str) -> bool:
    """
    Set the active trading wallet. If address is the second wallet, swap it with main.
    Returns True if updated.
    """
    uid = int(user_id)
    addr = (address or "").strip()
    if not addr:
        return False
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_referral_columns(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT wallet_address, encrypted_private_key, key_salt, default_wallet_name, wallets FROM users WHERE user_id = ?",
            (uid,),
        )
        row = await cursor.fetchone()
        if row is None:
            cursor = await db.execute(
                "SELECT wallet_address, encrypted_private_key, key_salt, default_wallet_name, wallets FROM users WHERE user_id = ?",
                (str(uid),),
            )
            row = await cursor.fetchone()
        if row is None:
            return False
        main_addr = (row["wallet_address"] or "").strip()
        if main_addr == addr:
            return True  # already default
        try:
            extra = json.loads(row["wallets"] or "[]")
        except (TypeError, json.JSONDecodeError):
            extra = []
        other = None
        for i, w in enumerate(extra):
            if (w.get("address") or "").strip() == addr:
                other = (i, w)
                break
        if not other:
            return False
        _, w2 = other
        # Swap: current main -> wallets[0], w2 -> main
        new_main_addr = (w2.get("address") or "").strip()
        new_main_enc = w2.get("encrypted_private_key")
        new_main_salt = w2.get("key_salt")
        new_main_name = (w2.get("name") or "W2").strip() or "W2"
        old_main_name = (row["default_wallet_name"] or "W1").strip() or "W1"
        new_extra = [{
            "address": main_addr,
            "encrypted_private_key": row["encrypted_private_key"],
            "key_salt": row["key_salt"],
            "name": old_main_name,
            "type": "imported",
            "date_added": (w2.get("date_added") or datetime.now().isoformat()),
        }]
        await db.execute(
            """UPDATE users SET
               wallet_address = ?, encrypted_private_key = ?, key_salt = ?,
               default_wallet_name = ?, wallets = ?
               WHERE user_id = ?""",
            (new_main_addr, new_main_enc, new_main_salt, new_main_name, json.dumps(new_extra), uid),
        )
        if db.total_changes == 0:
            await db.execute(
                """UPDATE users SET
                   wallet_address = ?, encrypted_private_key = ?, key_salt = ?,
                   default_wallet_name = ?, wallets = ?
                   WHERE user_id = ?""",
                (new_main_addr, new_main_enc, new_main_salt, new_main_name, json.dumps(new_extra), str(uid)),
            )
        await db.commit()
        _invalidate_decrypt_cache(uid)
        return True


async def unbind_wallet(user_id, address: str) -> bool:
    """
    Remove a wallet. Never allow removing the last one.
    If address is main: promote the second wallet to main (if any). Otherwise remove from list.
    Returns True if updated.
    """
    uid = int(user_id)
    addr = (address or "").strip()
    if not addr:
        return False
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_referral_columns(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT wallet_address, encrypted_private_key, key_salt, default_wallet_name, wallets FROM users WHERE user_id = ?",
            (uid,),
        )
        row = await cursor.fetchone()
        if row is None:
            cursor = await db.execute(
                "SELECT wallet_address, encrypted_private_key, key_salt, default_wallet_name, wallets FROM users WHERE user_id = ?",
                (str(uid),),
            )
            row = await cursor.fetchone()
        if row is None:
            return False
        main_addr = (row["wallet_address"] or "").strip()
        try:
            extra = json.loads(row["wallets"] or "[]")
        except (TypeError, json.JSONDecodeError):
            extra = []
        if addr == main_addr:
            if not extra:
                return False  # cannot unbind last wallet
            w2 = extra[0]
            new_main_addr = (w2.get("address") or "").strip()
            new_main_enc = w2.get("encrypted_private_key")
            new_main_salt = w2.get("key_salt")
            new_main_name = (w2.get("name") or "W2").strip() or "W2"
            await db.execute(
                """UPDATE users SET wallet_address = ?, encrypted_private_key = ?, key_salt = ?,
                   default_wallet_name = ?, wallets = ? WHERE user_id = ?""",
                (new_main_addr, new_main_enc, new_main_salt, new_main_name, json.dumps([]), uid),
            )
            if db.total_changes == 0:
                await db.execute(
                    """UPDATE users SET wallet_address = ?, encrypted_private_key = ?, key_salt = ?,
                       default_wallet_name = ?, wallets = ? WHERE user_id = ?""",
                    (new_main_addr, new_main_enc, new_main_salt, new_main_name, json.dumps([]), str(uid)),
                )
        else:
            # Remove from list
            new_extra = [x for x in extra if (x.get("address") or "").strip() != addr]
            if len(new_extra) == len(extra):
                return False
            await db.execute(
                "UPDATE users SET wallets = ? WHERE user_id = ?",
                (json.dumps(new_extra), uid),
            )
            if db.total_changes == 0:
                await db.execute(
                    "UPDATE users SET wallets = ? WHERE user_id = ?",
                    (json.dumps(new_extra), str(uid)),
                )
        await db.commit()
        _invalidate_decrypt_cache(uid)
        return True


async def get_private_key_for_wallet(user_id, address: str) -> str | None:
    """
    Return decrypted private key for the given wallet address (main or W2).
    Returns None if not found or decryption fails. Used for Export Private Key.
    """
    uid = int(user_id)
    addr = (address or "").strip()
    if not addr:
        return None
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_referral_columns(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT wallet_address, encrypted_private_key, key_salt, wallets FROM users WHERE user_id = ?",
            (uid,),
        )
        row = await cursor.fetchone()
        if row is None:
            cursor = await db.execute(
                "SELECT wallet_address, encrypted_private_key, key_salt, wallets FROM users WHERE user_id = ?",
                (str(uid),),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        main_addr = (row["wallet_address"] or "").strip()
        if main_addr == addr:
            enc = row["encrypted_private_key"]
            salt = row["key_salt"]
            if not enc:
                return None
            try:
                return encryption_mgr.decrypt_private_key(enc, salt, str(uid))
            except Exception:
                return None
        try:
            extra = json.loads(row["wallets"] or "[]")
        except (TypeError, json.JSONDecodeError):
            return None
        for w in extra:
            if (w.get("address") or "").strip() == addr:
                enc = w.get("encrypted_private_key")
                salt = w.get("key_salt")
                if not enc:
                    return None
                try:
                    return encryption_mgr.decrypt_private_key(enc, salt, str(uid))
                except Exception:
                    return None
        return None


async def rename_wallet(user_id, address: str, new_name: str) -> bool:
    """Update the display name for a wallet. Returns True if updated."""
    uid = int(user_id)
    addr = (address or "").strip()
    name = (new_name or "").strip() or "W1"
    if not addr:
        return False
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_referral_columns(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT wallet_address, default_wallet_name, wallets FROM users WHERE user_id = ?",
            (uid,),
        )
        row = await cursor.fetchone()
        if row is None:
            cursor = await db.execute(
                "SELECT wallet_address, default_wallet_name, wallets FROM users WHERE user_id = ?",
                (str(uid),),
            )
            row = await cursor.fetchone()
        if row is None:
            return False
        main_addr = (row["wallet_address"] or "").strip()
        if addr == main_addr:
            await db.execute(
                "UPDATE users SET default_wallet_name = ? WHERE user_id = ?",
                (name, uid),
            )
            if db.total_changes == 0:
                await db.execute(
                    "UPDATE users SET default_wallet_name = ? WHERE user_id = ?",
                    (name, str(uid)),
                )
        else:
            try:
                extra = json.loads(row["wallets"] or "[]")
            except (TypeError, json.JSONDecodeError):
                extra = []
            for w in extra:
                if (w.get("address") or "").strip() == addr:
                    w["name"] = name
                    break
            await db.execute(
                "UPDATE users SET wallets = ? WHERE user_id = ?",
                (json.dumps(extra), uid),
            )
            if db.total_changes == 0:
                await db.execute(
                    "UPDATE users SET wallets = ? WHERE user_id = ?",
                    (json.dumps(extra), str(uid)),
                )
        await db.commit()
        return True


# -----------------------------------------------------------------------------
# Users (bulk): get_all_users, get_all_users_with_wallets
# -----------------------------------------------------------------------------
@error_decorator
async def get_all_users():
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM users')
        rows = await cursor.fetchall()
        await cursor.close()
        return {row['user_id']: dict(row) for row in rows}


@error_decorator
async def get_all_users_with_wallets() -> list:
    """Return [(user_id, wallet_address), ...] for users with configured wallets."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT user_id, wallet_address FROM users WHERE wallet_address IS NOT NULL AND wallet_address != '' AND wallet_address != 'NOT_SET'"
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [(r["user_id"], r["wallet_address"]) for r in rows]


# -----------------------------------------------------------------------------
# Withdraw / deposit: withdraw_wallet, last_deposit_address, set_last_deposit_address
# -----------------------------------------------------------------------------
async def _ensure_withdraw_columns(db):
    """Add last_deposit_address if missing (safe for existing DBs)."""
    cursor = await db.execute("PRAGMA table_info(users)")
    existing = {row[1] for row in await cursor.fetchall()}
    if "last_deposit_address" not in existing:
        await db.execute("ALTER TABLE users ADD COLUMN last_deposit_address TEXT DEFAULT NULL")
    await db.commit()


@error_decorator
async def get_withdraw_wallet(user_id):
    """Get the user's manually set withdraw wallet address (withdraw_address)."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_withdraw_columns(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT withdraw_wallet FROM users WHERE user_id = ?', (user_id,))
        row = await cursor.fetchone()
        await cursor.close()
        if row and row['withdraw_wallet']:
            return row['withdraw_wallet']
        return None


@error_decorator
async def get_last_deposit_address(user_id):
    """Get the user's last deposit address (auto-updated on incoming SOL)."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_withdraw_columns(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT last_deposit_address FROM users WHERE user_id = ?', (user_id,))
        row = await cursor.fetchone()
        await cursor.close()
        if row and row['last_deposit_address']:
            return row['last_deposit_address']
        return None


@error_decorator
async def get_withdraw_destination(user_id, user_wallet_address: str | None = None) -> tuple:
    """
    Returns (address or None, source).
    a) Use withdraw_address (manual) if set.
    b) Otherwise recommend last_deposit_address only if it is a valid external address
       (not None, not empty, and not equal to the user's own wallet).
    """
    manual = await get_withdraw_wallet(user_id)
    if manual and str(manual).strip():
        return (manual.strip(), "manual")
    auto = await get_last_deposit_address(user_id)
    if auto and str(auto).strip():
        # Do not recommend if it equals the user's own wallet (invalid external)
        if user_wallet_address and str(auto).strip().lower() == str(user_wallet_address).strip().lower():
            return (None, None)
        return (auto.strip(), "recommended")
    return (None, None)


@error_decorator
async def set_last_deposit_address(user_id, address: str):
    """Update last_deposit_address when user receives SOL deposit."""
    return await update_last_deposit_address(user_id, address)


@error_decorator
async def update_last_deposit_address(user_id, address: str):
    """Update last_deposit_address with the external sender address (called by deposit listener)."""
    try:
        if not address or not str(address).strip():
            return False
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_withdraw_columns(db)
            cursor = await db.execute(
                "UPDATE users SET last_deposit_address = ? WHERE user_id = ?",
                (address.strip(), user_id),
            )
            if cursor.rowcount == 0:
                await db.execute(
                    "UPDATE users SET last_deposit_address = ? WHERE user_id = ?",
                    (address.strip(), str(user_id)),
                )
            await db.commit()
        return True
    except Exception as e:
        logger.warning("update_last_deposit_address error: %s", e)
        return False


@error_decorator
async def set_withdraw_wallet(user_id, wallet_address):
    """Set the user's withdraw wallet address"""
    try:
        await update_user(user_id, withdraw_wallet=wallet_address)
        return True
    except Exception as e:
        logger.error("Error setting withdraw wallet: %s", e)
        return False


# -----------------------------------------------------------------------------
# Trades: add_trade_record, record_automated_buy, get_token_pnl_data, get_latest_transaction, etc.
# -----------------------------------------------------------------------------
@error_decorator
async def add_trade_record(user_id, token_address, trade_type, sol_amount, token_amount, price_per_token, tx_signature, fee_usd: float = 0.0,
                           fee_sol: float | None = None, proceeds_are_net: bool = False,
                           pump_race_ms: float | None = None, pump_race_method: str | None = None):
    """Add a trade record for PnL tracking. Keeps a permanent history per user and token (CA).

    Records are appended only; never deleted. PnL calculation aggregates ALL trades for this
    user and token_address (contract address), not just "active" or recent ones.

    fee_usd: Jito tips + priority/Stealth fees in USD. Added to cost for buys, subtracted from proceeds for sells.
    pump_race_ms / pump_race_method: optional Racing Fallback metrics for database logging.
    """
    try:
        user_data = await get_user(user_id)
        if not user_data:
            return False

        # Parse existing trades
        trades_data = json.loads(user_data.get('trades', '{}'))

        # Initialize token data if not exists
        if token_address not in trades_data:
            trades_data[token_address] = {
                'first_buy_time': None,
                'total_bought_sol': 0.0,
                'total_sold_sol': 0.0,
                'total_bought_tokens': 0.0,
                'total_sold_tokens': 0.0,
                'trades': []
            }

        token_data = trades_data[token_address]

        # Fetch current SOL price from Binance (same source as assets.fetch_sol_price)
        sol_price_usd = None
        try:
            async with aiohttp.ClientSession() as session:
                url = "https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        data = await response.json()
                        price = float(data.get("price", 0))
                        if price > 0:
                            sol_price_usd = price
        except Exception as e:
            logger.warning("Error fetching SOL price for trade record: %s", e)
        if not sol_price_usd:
            sol_price_usd = 150.0  # fallback

        trade_record = {
            'timestamp': time.time(),
            'type': trade_type,  # 'buy' or 'sell'
            'sol_amount': float(sol_amount),
            'token_amount': float(token_amount),
            'price_per_token': float(price_per_token),
            'sol_price_usd': float(sol_price_usd),
            'tx_signature': tx_signature,
            'fee_usd': float(fee_usd) if fee_usd else 0.0,
            'fee_sol': float(fee_sol) if fee_sol is not None else 0.0,
            'proceeds_are_net': bool(proceeds_are_net) if trade_type == 'sell' else False,
        }
        if pump_race_ms is not None:
            trade_record['pump_race_ms'] = float(pump_race_ms)
        if pump_race_method:
            trade_record['pump_race_method'] = str(pump_race_method)

        token_data['trades'].append(trade_record)

        fee_sol_val = float(fee_sol) if fee_sol is not None else 0.0
        if trade_type == 'buy':
            invested = float(sol_amount) + fee_sol_val
            if float(sol_amount) < fee_sol_val and float(sol_amount) > 0:
                logger.warning(
                    "[ACCOUNTING WARN] BUY sol_amount (%.8f) < fee_sol (%.8f) for user=%s token=%s — "
                    "principal may be missing from cost basis!",
                    float(sol_amount), fee_sol_val, user_id, token_address[:8],
                )
            logger.info(
                "[ACCOUNTING] BUY recorded: user=%s token=%s sol_amount=%.8f + fee_sol=%.8f = invested=%.8f",
                user_id, token_address[:8], float(sol_amount), fee_sol_val, invested,
            )
            token_data['total_bought_sol'] += invested
            token_data['total_bought_tokens'] += float(token_amount)
            if token_data['first_buy_time'] is None:
                token_data['first_buy_time'] = time.time()
        elif trade_type == 'sell':
            received_sol = float(sol_amount) if proceeds_are_net else max(0.0, float(sol_amount) - fee_sol_val)
            token_data['total_sold_sol'] += received_sol
            token_data['total_sold_tokens'] += float(token_amount)
        
        # Update user trades data
        await update_user(user_id, trades=json.dumps(trades_data))
        return True
        
    except Exception as e:
        logger.error("Error adding trade record: %s", e)
        return False


async def record_automated_buy(
    user_id,
    token_mint: str,
    sol_amount: float,
    tx_signature: str,
    fee_usd: float = 0.0,
    price_usd_override: float | None = None,
    fee_sol: float | None = None,
) -> bool:
    """Record a buy for PnL when executed by DCA, Copy Trade, or Limit engine.
    Fetches SOL and token price to compute token_amount_est; use price_usd_override (e.g. filled price) when available.
    fee_sol: actual on-chain fee paid (SOL). If None, a conservative estimate (0.002 SOL) is used."""
    try:
        from pump_utils import (
            get_cached_sol_usd_price,
            get_real_mc,
            get_cached_pump_token_total_supply_raw,
            pump_fdv_to_price_usd,
        )
        from assets import fetch_token_metadata_and_price

        sol_price_usd = await get_cached_sol_usd_price()
        if price_usd_override is not None and price_usd_override > 0:
            price_usd = float(price_usd_override)
        else:
            price_usd = 0.0
            # Pump.fun: unified on-chain MC source of truth.
            if token_mint and str(token_mint).strip().lower().endswith("pump"):
                real_mc = await get_real_mc(token_mint, sol_price_usd)
                if real_mc and float(real_mc) > 0:
                    price_usd = pump_fdv_to_price_usd(
                        float(real_mc), get_cached_pump_token_total_supply_raw(token_mint)
                    )

            # Non-pump or on-chain curve not available -> fallback to metadata price.
            if price_usd <= 0:
                meta = await fetch_token_metadata_and_price(token_mint, sol_price_usd)
                price_usd = float(meta.get("price_usd") or 0)

        actual_fee_sol = float(fee_sol) if fee_sol is not None and fee_sol > 0 else 0.002
        fee_usd_calc = actual_fee_sol * sol_price_usd if sol_price_usd else float(fee_usd)
        token_amount_est = (sol_amount * sol_price_usd / price_usd) if price_usd else 0.0
        logger.info(
            "[ACCOUNTING] AUTO-BUY recorded: user=%s token=%s sol_amount=%.8f fee_sol=%.8f",
            user_id, token_mint[:8] if token_mint else "", sol_amount, actual_fee_sol,
        )
        return await add_trade_record(
            user_id,
            token_mint,
            "buy",
            sol_amount,
            token_amount_est,
            price_usd,
            tx_signature,
            fee_usd=fee_usd_calc,
            fee_sol=actual_fee_sol,
        )
    except Exception as e:
        logger.warning("record_automated_buy failed: user_id=%s mint=%s error=%s", user_id, token_mint[:8] if token_mint else "", e)
        return False


@error_decorator
async def get_token_pnl_data(user_id, token_address, user_data=None):
    """Get PnL data for a specific token. Pass user_data when already loaded to avoid repeated get_user (e.g. from fetch_user_assets)."""
    try:
        if user_data is None:
            user_data = await get_user(user_id)
        if not user_data:
            return None

        trades_data = user_data.get("trades")
        if isinstance(trades_data, str):
            trades_data = json.loads(trades_data or "{}")
        elif not isinstance(trades_data, dict):
            trades_data = {}

        if token_address not in trades_data:
            return None

        return trades_data[token_address]

    except Exception as e:
        logger.error("Error getting token PnL data: %s", e)
        return None


def _flatten_user_trades(user_id, user_data) -> list[dict]:
    """Flatten all trades from users.trades JSON into a list of { token_mint, type, sol_amount, timestamp, signature }. Sorted by timestamp descending."""
    out = []
    try:
        raw = user_data.get("trades") if user_data else None
        if isinstance(raw, str):
            trades_data = json.loads(raw or "{}")
        else:
            trades_data = raw if isinstance(raw, dict) else {}
        for token_mint, token_data in (trades_data or {}).items():
            if not isinstance(token_data, dict):
                continue
            for t in (token_data.get("trades") or []):
                if not isinstance(t, dict):
                    continue
                ts = t.get("timestamp")
                if ts is None:
                    continue
                out.append({
                    "token_mint": token_mint or "",
                    "type": (t.get("type") or "buy").lower(),
                    "sol_amount": float(t.get("sol_amount") or 0),
                    "timestamp": float(ts),
                    "signature": (t.get("tx_signature") or "").strip(),
                })
    except (json.JSONDecodeError, TypeError, KeyError) as e:
        logger.debug("_flatten_user_trades failed: %s", e)
    out.sort(key=lambda x: x["timestamp"], reverse=True)
    return out


async def get_latest_transaction(user_id: int) -> dict | None:
    """Retrieve the single most recent trade from the user's trades JSON. Returns dict with token_mint, type, sol_amount, timestamp, signature, or None."""
    try:
        user_data = await get_user(user_id)
        flat = _flatten_user_trades(user_id, user_data)
        return flat[0] if flat else None
    except Exception as e:
        logger.warning("get_latest_transaction failed: user_id=%s error=%s", user_id, e)
        return None


async def get_last_x_transactions(user_id: int, limit: int = 10) -> list[dict]:
    """Retrieve the last N trades from the user's trades JSON. Sorted by timestamp descending. Each item: token_mint, type, sol_amount, timestamp, signature."""
    try:
        user_data = await get_user(user_id)
        flat = _flatten_user_trades(user_id, user_data)
        return flat[: max(1, int(limit))]
    except Exception as e:
        logger.warning("get_last_x_transactions failed: user_id=%s error=%s", user_id, e)
        return []


async def get_user_performance_stats(user_id: int) -> dict:
    """Aggregate performance from users.trades JSON: total realized PnL (SOL), win rate, last 5 closed trades.
    A closed trade = per-token position with at least one sell; realized_pnl_sol = total_sold_sol - total_bought_sol.
    Uses single get_user read and in-memory aggregation (no long DB lock)."""
    try:
        user_data = await get_user(user_id)
        if not user_data:
            return {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "total_realized_sol": 0.0,
                "total_realized_pnl_sol": 0.0,
                "win_rate": 0.0,
                "last_5": [],
            }
        raw = user_data.get("trades")
        if isinstance(raw, str):
            trades_data = json.loads(raw or "{}")
        else:
            trades_data = raw if isinstance(raw, dict) else {}
        closed = []
        for token_mint, token_data in (trades_data or {}).items():
            if not isinstance(token_data, dict):
                continue
            total_bought = float(token_data.get("total_bought_sol") or 0)
            total_sold = float(token_data.get("total_sold_sol") or 0)
            if total_sold <= 0:
                continue
            pnl_sol = total_sold - total_bought
            pnl_pct = (pnl_sol / total_bought * 100.0) if total_bought > 0 else 0.0
            last_sell_ts = 0.0
            for t in (token_data.get("trades") or []):
                if isinstance(t, dict) and (t.get("type") or "").lower() == "sell":
                    ts = t.get("timestamp")
                    if ts is not None:
                        last_sell_ts = max(last_sell_ts, float(ts))
            closed.append({
                "token_mint": token_mint or "",
                "pnl_sol": pnl_sol,
                "pnl_pct": pnl_pct,
                "last_sell_ts": last_sell_ts,
            })
        closed.sort(key=lambda x: x["last_sell_ts"], reverse=True)
        last_5 = [{"token_mint": c["token_mint"], "pnl_sol": c["pnl_sol"], "pnl_pct": c["pnl_pct"]} for c in closed[:5]]
        total_realized_pnl_sol = sum(c["pnl_sol"] for c in closed)
        total_closed = len(closed)
        wins = sum(1 for c in closed if c["pnl_sol"] > 0)
        losses = sum(1 for c in closed if c["pnl_sol"] <= 0)
        win_rate = (wins / total_closed * 100.0) if total_closed else 0.0
        return {
            "total_trades": total_closed,
            "winning_trades": wins,
            "losing_trades": losses,
            "total_realized_sol": total_realized_pnl_sol,
            "total_realized_pnl_sol": total_realized_pnl_sol,
            "win_rate": round(win_rate, 1),
            "last_5": last_5,
        }
    except Exception as e:
        logger.warning("get_user_performance_stats failed: user_id=%s error=%s", user_id, e)
        return {
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "total_realized_sol": 0.0,
            "total_realized_pnl_sol": 0.0,
            "win_rate": 0.0,
            "last_5": [],
        }


@error_decorator
async def get_hiper_buyer_count(token_mint: str) -> int:
    """Count distinct users who have at least one buy (total_bought_sol > 0) for this token. For UI social proof."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT user_id, trades FROM users WHERE trades IS NOT NULL AND trades LIKE ?",
                (f"%{token_mint}%",),
            )
            rows = await cursor.fetchall()
            await cursor.close()
        count = 0
        for row in rows:
            try:
                trades_data = json.loads(row["trades"] or "{}")
                token_data = trades_data.get(token_mint)
                if token_data and float(token_data.get("total_bought_sol") or 0) > 0:
                    count += 1
            except (json.JSONDecodeError, TypeError, KeyError):
                continue
        return count
    except Exception as e:
        logger.debug("get_hiper_buyer_count failed: %s", e)
        return 0


@error_decorator
async def set_manual_entry_price(user_id: int, token_address: str, price_usd: float) -> bool:
    """Set a manual average entry price for a token (e.g. bought outside the bot). Takes precedence over calculated avg from trades."""
    try:
        if not price_usd or price_usd <= 0:
            return False
        user_data = await get_user(user_id)
        if not user_data:
            return False
        trades_data = json.loads(user_data.get('trades', '{}'))
        if token_address not in trades_data:
            trades_data[token_address] = {
                'first_buy_time': None,
                'total_bought_sol': 0.0,
                'total_sold_sol': 0.0,
                'total_bought_tokens': 0.0,
                'total_sold_tokens': 0.0,
                'trades': [],
            }
        trades_data[token_address]['manual_avg_price'] = float(price_usd)
        await update_user(user_id, trades=json.dumps(trades_data))
        return True
    except Exception as e:
        logger.error("Error setting manual entry price: %s", e)
        return False


@error_decorator
async def reset_token_position_tracking(user_id: int, token_address: str) -> bool:
    """Clear per-position overrides after a full close so the next buy starts a fresh cycle."""
    try:
        user_data = await get_user(user_id)
        if not user_data:
            return False
        trades_data = json.loads(user_data.get('trades', '{}'))
        token_data = trades_data.get(token_address)
        if not isinstance(token_data, dict):
            return False
        token_data.pop('manual_avg_price', None)
        token_data['first_buy_time'] = None
        await update_user(user_id, trades=json.dumps(trades_data))
        return True
    except Exception as e:
        logger.error("Error resetting token position tracking: %s", e)
        return False


# -----------------------------------------------------------------------------
# Limit orders (table is created automatically on first use)
# -----------------------------------------------------------------------------

_LIMIT_ORDERS_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS limit_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token_address TEXT NOT NULL,
        token_symbol TEXT,
        order_type TEXT NOT NULL,
        trigger_price_usd REAL NOT NULL,
        amount_lamports INTEGER NOT NULL,
        expiry_timestamp INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_relative_order INTEGER DEFAULT 0,
        reference_price_usd REAL,
        target_percentage REAL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_limit_orders_status ON limit_orders(status)",
    "CREATE INDEX IF NOT EXISTS idx_limit_orders_user_id ON limit_orders(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_limit_orders_expiry ON limit_orders(expiry_timestamp)",
]


async def _ensure_limit_orders_relative_columns(db):
    """Add Price Change Order columns if missing (migration for existing DBs)."""
    cursor = await db.execute("PRAGMA table_info(limit_orders)")
    cols = {row[1] for row in await cursor.fetchall()}
    if "is_relative_order" not in cols:
        await db.execute("ALTER TABLE limit_orders ADD COLUMN is_relative_order INTEGER DEFAULT 0")
    if "reference_price_usd" not in cols:
        await db.execute("ALTER TABLE limit_orders ADD COLUMN reference_price_usd REAL")
    if "target_percentage" not in cols:
        await db.execute("ALTER TABLE limit_orders ADD COLUMN target_percentage REAL")
    await db.commit()


async def _ensure_limit_orders_table(db):
    """Create limit_orders table and indexes if they do not exist."""
    for stmt in _LIMIT_ORDERS_SCHEMA:
        await db.execute(stmt)
    await db.commit()
    await _ensure_limit_orders_relative_columns(db)


async def ensure_limit_orders_ready():
    """Ensure limit_orders table exists. Call before launching bot suite."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_limit_orders_table(db)


async def ensure_withdraw_columns_ready():
    """Ensure withdraw-related columns (last_deposit_address) exist."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_withdraw_columns(db)


async def ensure_encryption_columns():
    """Add encrypted_private_key and key_salt to users table if missing (fixes old migrations)."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        cursor = await db.execute("PRAGMA table_info(users)")
        cols = {row[1] for row in await cursor.fetchall()}
        if "encrypted_private_key" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN encrypted_private_key BLOB")
        if "key_salt" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN key_salt BLOB")
        await db.commit()


@error_decorator
async def get_pending_limit_orders():
    """Fetch all pending limit orders that have not expired."""
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_limit_orders_table(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT id, user_id, token_address, token_symbol, order_type, trigger_price_usd,
                      amount_lamports, expiry_timestamp, status, created_at,
                      is_relative_order, reference_price_usd, target_percentage
               FROM limit_orders
               WHERE status = 'pending' AND expiry_timestamp > ?
               ORDER BY created_at ASC""",
            (now,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


@error_decorator
async def insert_limit_order(
    user_id: int,
    token_address: str,
    token_symbol: str,
    order_type: str,
    trigger_price_usd: float,
    amount_lamports: int,
    expiry_timestamp: int,
    is_relative_order: bool = False,
    reference_price_usd: float = None,
    target_percentage: float = None,
):
    """Insert a limit order. Returns order id or None. Price Change orders set is_relative_order=True, reference_price_usd, target_percentage."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_limit_orders_table(db)
        rel = 1 if is_relative_order else 0
        cursor = await db.execute(
            """INSERT INTO limit_orders
               (user_id, token_address, token_symbol, order_type, trigger_price_usd,
                amount_lamports, expiry_timestamp, status, is_relative_order, reference_price_usd, target_percentage)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)""",
            (
                user_id,
                token_address,
                token_symbol or "",
                order_type,
                trigger_price_usd,
                amount_lamports,
                expiry_timestamp,
                rel,
                reference_price_usd,
                target_percentage,
            ),
        )
        await db.commit()
        return cursor.lastrowid


@error_decorator
async def update_limit_order_status(order_id: int, status: str) -> bool:
    """Set order to terminal state: 'filled' or 'cancelled' removes the row (purge-on-completion)."""
    if status not in ("pending", "filled", "cancelled"):
        return False
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        if status in ("filled", "cancelled"):
            cursor = await db.execute("DELETE FROM limit_orders WHERE id = ?", (order_id,))
        else:
            cursor = await db.execute(
                "UPDATE limit_orders SET status = ? WHERE id = ?",
                (status, order_id),
            )
        await db.commit()
        return cursor.rowcount >= 1


@error_decorator
async def update_limit_order_trigger_price(order_id: int, user_id: int, trigger_price_usd: float) -> bool:
    """Update an existing pending order's trigger price and clear relative-order metadata."""
    try:
        order_id_int = int(order_id)
        user_id_int = int(user_id)
        trigger_price = float(trigger_price_usd)
    except (TypeError, ValueError):
        return False
    if trigger_price <= 0:
        return False
    async with _db_lock:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_limit_orders_table(db)
            cursor = await db.execute(
                """UPDATE limit_orders
                   SET trigger_price_usd = ?,
                       is_relative_order = 0,
                       reference_price_usd = NULL,
                       target_percentage = NULL
                   WHERE id = ? AND user_id = ? AND status = 'pending'""",
                (trigger_price, order_id_int, user_id_int),
            )
            await db.commit()
            return cursor.rowcount >= 1


@error_decorator
async def clear_all_inactive_orders(user_id: int) -> int:
    """Remove all limit orders for this user that are not pending (filled/cancelled). Returns number deleted."""
    uid = int(user_id)
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_limit_orders_table(db)
        cursor = await db.execute(
            "DELETE FROM limit_orders WHERE user_id = ? AND status IN ('filled', 'cancelled')",
            (uid,),
        )
        await db.commit()
        return cursor.rowcount


@error_decorator
async def purge_filled_cancelled_limit_orders_and_vacuum() -> int:
    """One-time / startup cleanup: delete all filled and cancelled limit orders, then VACUUM to reclaim space. Returns number deleted."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_limit_orders_table(db)
        cursor = await db.execute(
            "DELETE FROM limit_orders WHERE status IN ('filled', 'cancelled')"
        )
        deleted = cursor.rowcount
        await db.commit()
        if deleted:
            logger.info("Purged %s filled/cancelled limit order(s)", deleted)
        await db.execute("VACUUM")
        await db.commit()
    return deleted


# -----------------------------------------------------------------------------
# Auto Sell configs (multi-level TP + SL)
# -----------------------------------------------------------------------------

_AUTO_SELL_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS auto_sell_configs (
        user_id INTEGER NOT NULL,
        token_mint TEXT NOT NULL,
        is_enabled INTEGER NOT NULL DEFAULT 1,
        tp_levels TEXT NOT NULL DEFAULT '[]',
        sl_config TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, token_mint)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_auto_sell_enabled ON auto_sell_configs(is_enabled)",
]


async def _ensure_auto_sell_table(db):
    for stmt in _AUTO_SELL_SCHEMA:
        await db.execute(stmt)
    await db.commit()
    # Add reference_price_usd for "price at activation" (TP/SL from activation, not cost basis)
    cur = await db.execute("PRAGMA table_info(auto_sell_configs)")
    cols = [row[1] for row in await cur.fetchall()]
    if "reference_price_usd" not in cols:
        await db.execute("ALTER TABLE auto_sell_configs ADD COLUMN reference_price_usd REAL")
        await db.commit()
    if "high_watermark_usd" not in cols:
        await db.execute("ALTER TABLE auto_sell_configs ADD COLUMN high_watermark_usd REAL")
        await db.commit()
    if "expires_at" not in cols:
        await db.execute("ALTER TABLE auto_sell_configs ADD COLUMN expires_at REAL")
        await db.commit()


async def ensure_auto_sell_configs_ready():
    """Create auto_sell_configs table if it does not exist."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_auto_sell_table(db)


@error_decorator
async def get_auto_sell_config(user_id: int, token_mint: str):
    """Get Auto Sell config for user/token. Returns dict or None."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_auto_sell_table(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT user_id, token_mint, is_enabled, tp_levels, sl_config, last_updated, reference_price_usd, high_watermark_usd, expires_at FROM auto_sell_configs WHERE user_id = ? AND token_mint = ?",
            (user_id, token_mint),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        r = dict(row)
        r["tp_levels"] = _safe_json_loads(r.get("tp_levels"), [])
        r["sl_config"] = _safe_json_loads(r.get("sl_config"), None)
        return r


@error_decorator
async def upsert_auto_sell_config(user_id: int, token_mint: str, is_enabled: bool = True, tp_levels: list = None, sl_config: dict = None, reference_price_usd: float = None, high_watermark_usd: float = None, expires_at: float | None = None):
    """Insert or update Auto Sell config. tp_levels: [{"pct": 50.0, "sell_pct": 25.0, "triggered": False}, ...]. sl_config: {"pct": -30.0, "sell_pct": 100.0, "triggered": False}. reference_price_usd: price at activation for TP/SL (optional). high_watermark_usd: trailing peak (optional). expires_at: Unix timestamp for timed exit (optional)."""
    now = datetime.utcnow().isoformat()
    tp_json = json.dumps(tp_levels if tp_levels is not None else [])
    sl_json = json.dumps(sl_config) if sl_config is not None else None
    last_err = None
    for attempt in range(_DB_WRITE_RETRY_ATTEMPTS):
        try:
            async with aiosqlite.connect(DB_PATH, timeout=_DB_WRITE_TIMEOUT) as db:
                await _ensure_auto_sell_table(db)
                await db.execute("BEGIN IMMEDIATE")
                await db.execute(
                    """INSERT INTO auto_sell_configs (user_id, token_mint, is_enabled, tp_levels, sl_config, last_updated, reference_price_usd, high_watermark_usd, expires_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(user_id, token_mint) DO UPDATE SET
                         is_enabled = excluded.is_enabled,
                         tp_levels = excluded.tp_levels,
                         sl_config = excluded.sl_config,
                         last_updated = excluded.last_updated,
                         reference_price_usd = excluded.reference_price_usd,
                         high_watermark_usd = excluded.high_watermark_usd,
                         expires_at = excluded.expires_at""",
                    (user_id, token_mint, 1 if is_enabled else 0, tp_json, sl_json, now, reference_price_usd, high_watermark_usd, expires_at),
                )
                await db.commit()
            return True
        except Exception as e:
            last_err = e
            if "locked" in str(e).lower() or "database is locked" in str(e).lower():
                await asyncio.sleep(_DB_WRITE_RETRY_DELAY_BASE * (attempt + 1))
                continue
            raise
    if last_err is not None:
        logger.warning("upsert_auto_sell_config failed after %s retries: %s", _DB_WRITE_RETRY_ATTEMPTS, last_err)
        raise last_err
    return True


@error_decorator
async def update_auto_sell_triggered(user_id: int, token_mint: str, tp_levels: list = None, sl_config: dict = None, is_enabled: bool = None):
    """Update only tp_levels and/or sl_config (e.g. after triggering). Pass full lists/dict with triggered=True. Retries on database locked."""
    last_err = None
    for attempt in range(_DB_WRITE_RETRY_ATTEMPTS):
        try:
            async with aiosqlite.connect(DB_PATH, timeout=_DB_WRITE_TIMEOUT) as db:
                await _ensure_auto_sell_table(db)
                await db.execute("BEGIN IMMEDIATE")
                cur = await db.execute(
                    "SELECT tp_levels, sl_config, is_enabled FROM auto_sell_configs WHERE user_id = ? AND token_mint = ?",
                    (user_id, token_mint),
                )
                row = await cur.fetchone()
                if not row:
                    await db.rollback()
                    return False
                tp_json = json.dumps(tp_levels) if tp_levels is not None else row[0]
                sl_json = json.dumps(sl_config) if sl_config is not None else row[1]
                enabled = (1 if is_enabled else 0) if is_enabled is not None else row[2]
                now = datetime.utcnow().isoformat()
                await db.execute(
                    "UPDATE auto_sell_configs SET tp_levels = ?, sl_config = ?, is_enabled = ?, last_updated = ? WHERE user_id = ? AND token_mint = ?",
                    (tp_json, sl_json, enabled, now, user_id, token_mint),
                )
                await db.commit()
                if is_enabled is not None:
                    logger.info("DB UPDATE: Trading Shield (auto_sell) user_id=%s token=%s is_enabled=%s", user_id, token_mint[:8], bool(is_enabled))
            return True
        except Exception as e:
            last_err = e
            if "locked" in str(e).lower() or "database is locked" in str(e).lower():
                await asyncio.sleep(_DB_WRITE_RETRY_DELAY_BASE * (attempt + 1))
                continue
            raise
    if last_err is not None:
        logger.warning("update_auto_sell_triggered failed after %s retries: %s", _DB_WRITE_RETRY_ATTEMPTS, last_err)
        raise last_err
    return True


@error_decorator
async def update_auto_sell_watermark(user_id: int, token_mint: str, new_watermark_usd: float):
    """Update only high_watermark_usd for a config (used by limit engine when price makes new high). Retries on database locked."""
    now = datetime.utcnow().isoformat()
    last_err = None
    for attempt in range(_DB_WRITE_RETRY_ATTEMPTS):
        try:
            async with aiosqlite.connect(DB_PATH, timeout=_DB_WRITE_TIMEOUT) as db:
                await _ensure_auto_sell_table(db)
                await db.execute("BEGIN IMMEDIATE")
                await db.execute(
                    "UPDATE auto_sell_configs SET high_watermark_usd = ?, last_updated = ? WHERE user_id = ? AND token_mint = ?",
                    (new_watermark_usd, now, user_id, token_mint),
                )
                await db.commit()
            return True
        except Exception as e:
            last_err = e
            if "locked" in str(e).lower() or "database is locked" in str(e).lower():
                await asyncio.sleep(_DB_WRITE_RETRY_DELAY_BASE * (attempt + 1))
                continue
            raise
    if last_err is not None:
        logger.warning("update_auto_sell_watermark failed after %s retries: %s", _DB_WRITE_RETRY_ATTEMPTS, last_err)
        raise last_err
    return True


@error_decorator
async def get_all_active_auto_sell_configs():
    """Fetch all configs where is_enabled = 1. For limit engine."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_auto_sell_table(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT user_id, token_mint, is_enabled, tp_levels, sl_config, last_updated, reference_price_usd, high_watermark_usd, expires_at FROM auto_sell_configs WHERE is_enabled = 1"
        )
        rows = await cursor.fetchall()
        out = []
        for row in rows:
            r = dict(row)
            r["tp_levels"] = _safe_json_loads(r.get("tp_levels"), [])
            r["sl_config"] = _safe_json_loads(r.get("sl_config"), None)
            out.append(r)
        return out


async def get_user_pending_limit_orders(user_id):
    """Fetch pending limit orders for a specific user (not expired)."""
    user_id_int = int(user_id)
    now = int(time.time())
    async with _db_lock:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_limit_orders_table(db)
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT id, user_id, token_address, token_symbol, order_type, trigger_price_usd,
                          amount_lamports, expiry_timestamp, status, created_at,
                          is_relative_order, reference_price_usd, target_percentage
                   FROM limit_orders
                   WHERE user_id = ? AND status = 'pending' AND expiry_timestamp > ?
                   ORDER BY created_at ASC""",
                (user_id_int, now),
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_user_active_auto_sell_configs(user_id):
    """Fetch active Auto Sell configs for a specific user."""
    user_id_int = int(user_id)
    async with _db_lock:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_auto_sell_table(db)
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT user_id, token_mint, is_enabled, tp_levels, sl_config, last_updated, reference_price_usd, high_watermark_usd, expires_at FROM auto_sell_configs WHERE user_id = ? AND is_enabled = 1",
                (user_id_int,),
            )
            rows = await cursor.fetchall()
            out = []
            for row in rows:
                r = dict(row)
                r["tp_levels"] = _safe_json_loads(r.get("tp_levels"), [])
                r["sl_config"] = _safe_json_loads(r.get("sl_config"), None)
                out.append(r)
            return out


# -----------------------------------------------------------------------------
# Post-trade reconciliation: limit orders + trading shield
# -----------------------------------------------------------------------------

def _reset_trigger_flags(tp_levels: list | None, sl_config: dict | None) -> tuple[list, dict | None]:
    """Return copies of tp_levels/sl_config with triggered flags cleared."""
    try:
        new_tp = []
        for lvl in (tp_levels or []):
            if isinstance(lvl, dict):
                d = dict(lvl)
                if "triggered" in d:
                    d["triggered"] = False
                new_tp.append(d)
        new_sl = None
        if isinstance(sl_config, dict):
            new_sl = dict(sl_config)
            if "triggered" in new_sl:
                new_sl["triggered"] = False
        return new_tp, new_sl
    except Exception:
        return (tp_levels or []), sl_config


def _avg_entry_price_usd_from_pnl_data(token_data: dict | None) -> float:
    """Compute avg entry price for the current open cycle only (fees included)."""
    if not token_data:
        return 0.0
    try:
        manual = token_data.get("manual_avg_price")
        if manual is not None and float(manual) > 0:
            return float(manual)
    except Exception:
        pass
    trades = token_data.get("trades") or []
    if not trades:
        return 0.0
    total_usd = 0.0
    total_tokens = 0.0
    balance = 0.0
    for t in sorted(trades, key=lambda x: float(x.get("timestamp", 0) or 0)):
        try:
            token_amount = float(t.get("token_amount", 0) or 0)
            t_type = (t.get("type") or "").strip().lower()
            if t_type == "buy":
                if balance <= 0:
                    total_usd = 0.0
                    total_tokens = 0.0
                price_per_token = float(t.get("price_per_token", 0) or 0)
                sol_amount = float(t.get("sol_amount", 0) or 0)
                sol_price = float(t.get("sol_price_usd", 0) or 0)
                fee_usd = float(t.get("fee_usd", 0) or 0)
                if token_amount <= 0:
                    continue
                if price_per_token > 0:
                    base = price_per_token * token_amount
                elif sol_price > 0:
                    base = sol_amount * sol_price
                else:
                    base = sol_amount * 150.0
                total_usd += base + fee_usd
                total_tokens += token_amount
                balance += token_amount
            elif t_type == "sell":
                before = balance
                if before > 0:
                    sold_ratio = min(1.0, token_amount / before) if before > 0 else 0.0
                    total_usd -= total_usd * sold_ratio
                balance -= token_amount
                if balance <= 0:
                    balance = 0.0
                    total_usd = 0.0
                    total_tokens = 0.0
        except Exception:
            continue
    if total_tokens <= 0:
        return 0.0
    return total_usd / total_tokens


@error_decorator
async def delete_user_token_pending_limit_orders(user_id: int, token_address: str) -> int:
    """Delete all pending limit orders for a user+token (best-effort cleanup when position closes)."""
    uid = int(user_id)
    mint = (token_address or "").strip()
    if not mint:
        return 0
    async with _db_lock:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_limit_orders_table(db)
            cur = await db.execute(
                "DELETE FROM limit_orders WHERE user_id = ? AND token_address = ? AND status = 'pending'",
                (uid, mint),
            )
            await db.commit()
            deleted = int(cur.rowcount or 0)
            if deleted:
                logger.info("LIMIT ORDERS: Deleted %s pending order(s) for user_id=%s token=%s", deleted, uid, mint[:8])
            return deleted


@error_decorator
async def update_relative_sell_orders_reference(user_id: int, token_address: str, new_reference_price_usd: float) -> int:
    """When user re-buys and avg entry changes, update relative SELL orders that were based on entry price."""
    uid = int(user_id)
    mint = (token_address or "").strip()
    try:
        ref = float(new_reference_price_usd or 0.0)
    except Exception:
        ref = 0.0
    if not mint or ref <= 0:
        return 0
    async with _db_lock:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await _ensure_limit_orders_table(db)
            cur = await db.execute(
                """
                UPDATE limit_orders
                SET reference_price_usd = ?,
                    trigger_price_usd = (? * (1.0 + (target_percentage / 100.0)))
                WHERE user_id = ?
                  AND token_address = ?
                  AND status = 'pending'
                  AND is_relative_order = 1
                  AND order_type != 'buy_limit'
                  AND target_percentage IS NOT NULL
                """,
                (ref, ref, uid, mint),
            )
            await db.commit()
            updated = int(cur.rowcount or 0)
            if updated:
                logger.info("LIMIT ORDERS: Updated %s relative SELL order(s) ref for user_id=%s token=%s", updated, uid, mint[:8])
            return updated


@error_decorator
async def refresh_trading_shield_after_buy(user_id: int, token_mint: str, force_enable: bool | None = None) -> bool:
    """Update shield entry reference after a buy, optionally forcing the shield back on."""
    uid = int(user_id)
    mint = (token_mint or "").strip()
    if not mint:
        return False
    cfg = await get_auto_sell_config(uid, mint)
    if not cfg:
        return False
    token_data = await get_token_pnl_data(uid, mint)
    avg_entry = _avg_entry_price_usd_from_pnl_data(token_data)
    if avg_entry <= 0:
        return False
    await upsert_auto_sell_config(
        uid,
        mint,
        is_enabled=bool(cfg.get("is_enabled")) if force_enable is None else bool(force_enable),
        tp_levels=cfg.get("tp_levels") or [],
        sl_config=cfg.get("sl_config"),
        reference_price_usd=avg_entry,
        high_watermark_usd=avg_entry,
        expires_at=cfg.get("expires_at"),
    )
    await update_relative_sell_orders_reference(uid, mint, avg_entry)
    logger.info("TRADING SHIELD: Refreshed after buy for user_id=%s token=%s (avg_entry=$%.8f)", uid, mint[:8], avg_entry)
    return True


@error_decorator
async def cleanup_automation_on_position_closed(user_id: int, token_mint: str) -> dict:
    """When a position is fully closed (0 balance), disable shield and remove pending limit orders for this token."""
    uid = int(user_id)
    mint = (token_mint or "").strip()
    out = {"limit_orders_deleted": 0, "shield_disabled": False, "position_tracking_reset": False}
    if not mint:
        return out
    try:
        # Disable shield (do not delete row; keeps history / user settings)
        await update_auto_sell_triggered(uid, mint, is_enabled=False)
        out["shield_disabled"] = True
    except Exception:
        out["shield_disabled"] = False
    try:
        out["limit_orders_deleted"] = await delete_user_token_pending_limit_orders(uid, mint)
    except Exception:
        out["limit_orders_deleted"] = 0
    try:
        out["position_tracking_reset"] = bool(await reset_token_position_tracking(uid, mint))
    except Exception:
        out["position_tracking_reset"] = False
    return out


# -----------------------------------------------------------------------------
# User deletion and one-time migrations (encrypt_all_plaintext_keys, fix_invalid_data)
# -----------------------------------------------------------------------------
@error_decorator
async def delete_user(user_id):
    """Delete a user and their data from the database"""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
        await db.commit()

@error_decorator
async def encrypt_all_plaintext_keys():
    """One-time migration function to encrypt any plaintext private keys"""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        
        # First check table structure
        cursor = await db.execute("PRAGMA table_info(users)")
        columns = {col[1]: col for col in await cursor.fetchall()}
        
        # Check if we need to do migration
        has_private_key = 'private_key' in columns
        has_encrypted = 'encrypted_private_key' in columns and 'key_salt' in columns
        
        if not has_encrypted:
            logger.info("New encryption columns not found, skipping migration")
            return
        
        if not has_private_key:
            logger.info("No plaintext private keys found, migration not needed")
            return
        
        logger.info("Starting encryption migration...")
        cursor = await db.execute('SELECT user_id, private_key, wallets FROM users WHERE private_key IS NOT NULL')
        rows = await cursor.fetchall()
        
        for row in rows:
            try:
                user_id = row['user_id']
                private_key = row['private_key']
                wallets_str = row['wallets']
                
                if not private_key:
                    continue
                
                # Encrypt main private key
                encrypted_key, salt = encryption_mgr.encrypt_private_key(private_key, user_id)
                await db.execute('''
                    UPDATE users 
                    SET encrypted_private_key = ?,
                        key_salt = ?,
                        private_key = NULL
                    WHERE user_id = ?
                ''', (encrypted_key, salt, user_id))
                
                # Handle additional wallets if they exist
                if wallets_str:
                    try:
                        wallets = json.loads(wallets_str)
                        if isinstance(wallets, list):
                            modified = False
                            for wallet in wallets:
                                if isinstance(wallet, dict) and 'private_key' in wallet:
                                    encrypted_key, salt = encryption_mgr.encrypt_private_key(wallet['private_key'], user_id)
                                    wallet['encrypted_private_key'] = encrypted_key
                                    wallet['key_salt'] = salt
                                    del wallet['private_key']
                                    modified = True
                            
                            if modified:
                                await db.execute(
                                    'UPDATE users SET wallets = ? WHERE user_id = ?',
                                    (json.dumps(wallets), user_id)
                                )
                    except json.JSONDecodeError:
                        logger.warning("Invalid wallets JSON for user %s", user_id)
                
            except Exception as e:
                logger.error("Error processing user %s: %s", row['user_id'], e)
                continue
        
        await db.commit()
        logger.info("Encryption migration completed")

@error_decorator
async def fix_invalid_data():
    """Fix any invalid data in the database"""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM users')
        rows = await cursor.fetchall()
        
        for record in rows:
            row = dict(record)
            updates = {}
            modified = False
            
            # Fix empty wallet addresses
            if not row['wallet_address'] or not row['wallet_address'].strip():
                logger.warning("Empty wallet address for user %s", row['user_id'])
                continue  # Skip this record as we can't guess the correct wallet address
            
            # Fix trades JSON
            try:
                trades = row['trades']
                if isinstance(trades, str):
                    trades = json.loads(trades)
                if not isinstance(trades, dict):
                    updates['trades'] = json.dumps({})
                    modified = True
            except (json.JSONDecodeError, TypeError):
                updates['trades'] = json.dumps({})
                modified = True
            
            # Fix monitor_wallet JSON
            try:
                monitor = row['monitor_wallet']
                if isinstance(monitor, str):
                    monitor = json.loads(monitor)
                if not isinstance(monitor, dict):
                    updates['monitor_wallet'] = json.dumps({})
                    modified = True
            except (json.JSONDecodeError, TypeError):
                updates['monitor_wallet'] = json.dumps({})
                modified = True
            
            # Fix wallets JSON (only if column exists; current schema may not have it)
            if 'wallets' in row:
                try:
                    wallets = row['wallets']
                    if isinstance(wallets, str):
                        wallets = json.loads(wallets)
                    if not isinstance(wallets, list):
                        updates['wallets'] = json.dumps([])
                        modified = True
                except (json.JSONDecodeError, TypeError):
                    updates['wallets'] = json.dumps([])
                    modified = True
            
            # Apply updates if needed
            if modified:
                set_clause = ', '.join(f"{k} = ?" for k in updates.keys())
                values = list(updates.values()) + [row['user_id']]
                await db.execute(
                    f"UPDATE users SET {set_clause} WHERE user_id = ?",
                    values
                )
        
        await db.commit()

# -----------------------------------------------------------------------------
# Per-token trade configs (Migration Sell, Dev Sell, Snipe Migration)
# -----------------------------------------------------------------------------

_TOKEN_TRADE_CONFIG_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS token_trade_configs (
        user_id INTEGER NOT NULL,
        token_mint TEXT NOT NULL,
        migration_sell_active INTEGER NOT NULL DEFAULT 0,
        dev_sell_active INTEGER NOT NULL DEFAULT 0,
        dev_sell_trigger_pct INTEGER NOT NULL DEFAULT 0,
        snipe_migration_active INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (user_id, token_mint)
    )""",
]


async def _ensure_token_trade_config_table(db):
    for stmt in _TOKEN_TRADE_CONFIG_SCHEMA:
        await db.execute(stmt)
    # Add dev_balance_snapshot if missing (for dev-sell initial snapshot)
    try:
        cursor = await db.execute("PRAGMA table_info(token_trade_configs)")
        cols = [row[1] for row in await cursor.fetchall()]
        if "dev_balance_snapshot" not in cols:
            await db.execute("ALTER TABLE token_trade_configs ADD COLUMN dev_balance_snapshot REAL")
    except Exception:
        pass
    await db.commit()


async def ensure_token_trade_configs_ready():
    """Create token_trade_configs table if it does not exist."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_token_trade_config_table(db)


@error_decorator
async def get_token_trade_config(user_id: int, token_mint: str) -> dict:
    """Get per-token trade toggles (migration sell, dev sell, snipe migration). Returns dict (never None)."""
    defaults = {
        "migration_sell_active": False,
        "dev_sell_active": False,
        "dev_sell_trigger_pct": 0,
        "snipe_migration_active": False,
    }
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_token_trade_config_table(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT migration_sell_active, dev_sell_active, dev_sell_trigger_pct, snipe_migration_active, dev_balance_snapshot "
            "FROM token_trade_configs WHERE user_id = ? AND token_mint = ?",
            (user_id, token_mint),
        )
        row = await cursor.fetchone()
        if not row:
            return defaults
        out = {
            "migration_sell_active": bool(row["migration_sell_active"]),
            "dev_sell_active": bool(row["dev_sell_active"]),
            "dev_sell_trigger_pct": int(row["dev_sell_trigger_pct"]),
            "snipe_migration_active": bool(row["snipe_migration_active"]),
        }
        if "dev_balance_snapshot" in row.keys() and row["dev_balance_snapshot"] is not None:
            out["dev_balance_snapshot"] = float(row["dev_balance_snapshot"])
        return out


@error_decorator
async def update_token_trade_config(user_id: int, token_mint: str, **kwargs) -> bool:
    """Upsert one or more per-token trade toggle fields. Accepted kwargs:
    migration_sell_active (bool), dev_sell_active (bool),
    dev_sell_trigger_pct (int), snipe_migration_active (bool).
    """
    allowed = {"migration_sell_active", "dev_sell_active", "dev_sell_trigger_pct", "snipe_migration_active", "dev_balance_snapshot"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return False
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_token_trade_config_table(db)
        # Ensure row exists
        await db.execute(
            "INSERT OR IGNORE INTO token_trade_configs (user_id, token_mint) VALUES (?, ?)",
            (user_id, token_mint),
        )
        set_parts = ", ".join(f"{k} = ?" for k in updates)
        vals = []
        for v in updates.values():
            if isinstance(v, bool):
                vals.append(int(v))
            elif isinstance(v, (int, float)):
                vals.append(v)
            else:
                vals.append(v)
        vals.extend([user_id, token_mint])
        await db.execute(
            f"UPDATE token_trade_configs SET {set_parts} WHERE user_id = ? AND token_mint = ?",
            vals,
        )
        await db.commit()
    return True


@error_decorator
async def get_user_token_mints_from_config(user_id: int) -> set[str]:
    """Return set of token_mint values in token_trade_configs for this user. Used for rent recovery (keep ATAs that are still in config)."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_token_trade_config_table(db)
        cursor = await db.execute(
            "SELECT DISTINCT token_mint FROM token_trade_configs WHERE user_id = ?",
            (int(user_id),),
        )
        rows = await cursor.fetchall()
    return {str(r[0]) for r in rows if r and r[0]}


@error_decorator
async def get_active_token_trade_configs() -> list[dict]:
    """Return all token_trade_configs rows where at least one of migration_sell_active, dev_sell_active, or snipe_migration_active is 1.
    Used by the pump watcher to monitor bonding curve tokens.
    """
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_token_trade_config_table(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT user_id, token_mint, migration_sell_active, dev_sell_active, dev_sell_trigger_pct, snipe_migration_active, dev_balance_snapshot
               FROM token_trade_configs
               WHERE migration_sell_active = 1 OR dev_sell_active = 1 OR snipe_migration_active = 1"""
        )
        rows = await cursor.fetchall()
    out = []
    for row in rows:
        r = dict(row)
        out.append({
            "user_id": int(r["user_id"]),
            "token_mint": str(r["token_mint"]),
            "migration_sell_active": bool(r["migration_sell_active"]),
            "dev_sell_active": bool(r["dev_sell_active"]),
            "dev_sell_trigger_pct": int(r["dev_sell_trigger_pct"]),
            "snipe_migration_active": bool(r["snipe_migration_active"]),
            "dev_balance_snapshot": float(r["dev_balance_snapshot"]) if r.get("dev_balance_snapshot") is not None else None,
        })
    return out


# -----------------------------------------------------------------------------
# Snipe targets: token_mint + amount_sol; bot auto-buys when Raydium LP is added
# -----------------------------------------------------------------------------

SNIPE_TARGETS_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS snipe_targets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token_mint TEXT NOT NULL,
        amount_sol REAL NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_snipe_targets_user_id ON snipe_targets(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_snipe_targets_status ON snipe_targets(status)",
]


async def _ensure_snipe_targets_table(db):
    for stmt in SNIPE_TARGETS_SCHEMA:
        await db.execute(stmt)
    await db.commit()


async def ensure_snipe_targets_ready():
    """Ensure snipe_targets table exists (call at startup)."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_snipe_targets_table(db)


async def add_snipe_target(user_id: int, token_mint: str, amount_sol: float) -> int | None:
    """Add a snipe target. Returns id or None on failure. Ignores duplicate (user_id, token_mint) pending."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_snipe_targets_table(db)
        cursor = await db.execute(
            "SELECT id FROM snipe_targets WHERE user_id = ? AND token_mint = ? AND status = 'pending'",
            (user_id, token_mint),
        )
        if await cursor.fetchone():
            return None
        await db.execute(
            "INSERT INTO snipe_targets (user_id, token_mint, amount_sol, status) VALUES (?, ?, ?, 'pending')",
            (user_id, token_mint, max(0.001, float(amount_sol))),
        )
        await db.commit()
        cursor = await db.execute("SELECT last_insert_rowid()")
        row = await cursor.fetchone()
        return int(row[0]) if row else None


async def list_snipe_targets(user_id: int) -> list[dict]:
    """Return pending snipe targets for user (id, token_mint, amount_sol, created_at)."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_snipe_targets_table(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, token_mint, amount_sol, created_at FROM snipe_targets WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC",
            (user_id,),
        )
        rows = await cursor.fetchall()
    return [{"id": r["id"], "token_mint": r["token_mint"], "amount_sol": r["amount_sol"], "created_at": r["created_at"]} for r in rows]


async def get_pending_snipe_targets() -> list[dict]:
    """Return all pending snipe targets (id, user_id, token_mint, amount_sol). Used by snipe watcher."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await _ensure_snipe_targets_table(db)
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, user_id, token_mint, amount_sol FROM snipe_targets WHERE status = 'pending'"
        )
        rows = await cursor.fetchall()
    return [{"id": r["id"], "user_id": int(r["user_id"]), "token_mint": r["token_mint"], "amount_sol": float(r["amount_sol"])} for r in rows]


async def set_snipe_target_done(snipe_id: int, status: str = "done") -> None:
    """Mark snipe target as done (or cancelled)."""
    if status not in ("done", "cancelled"):
        status = "done"
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute(
            "UPDATE snipe_targets SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (status, snipe_id),
        )
        await db.commit()


async def cancel_snipe_target(user_id: int, snipe_id: int) -> bool:
    """Cancel a pending snipe target if it belongs to user. Returns True if updated."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        cursor = await db.execute(
            "UPDATE snipe_targets SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP WHERE id = ? AND user_id = ? AND status = 'pending'",
            (snipe_id, user_id),
        )
        await db.commit()
        return cursor.rowcount > 0


if __name__ == '__main__':
    async def main():
        try:
            logger.info("Starting database initialization...")
            await create_db_and_table()
            logger.info("Database initialization completed")
            
            logger.info("Checking for and fixing invalid data...")
            await fix_invalid_data()
            logger.info("Data validation completed")
        except Exception as e:
            logger.error("Error during database operations: %s", e)
            raise
    
    asyncio.run(main())
    # Encrypt any existing plaintext keys
    asyncio.run(encrypt_all_plaintext_keys())


