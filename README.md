# HiperSnipe — Solana High-Frequency Trading Bot
### Technical Case Study & QA Engineering Portfolio

---

## Overview

HiperSnipe is a production Solana DeFi trading bot built entirely in Python, designed for millisecond-class order execution on the Solana mainnet. The system executes **limit orders, take-profit / stop-loss automations, Telegram-triggered sniping, and timed exits** — all running concurrently under a single async event loop.

The project's most valuable engineering is not in the happy-path code; it is in the **seven distinct categories of bugs I discovered, diagnosed, and fixed** in a running production system without downtime. This document focuses on those QA challenges.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│  Telegram Bot (aiogram 3.x)                                    │
│   ├── /assets  /limits  /snipe  /settings  inline keyboards    │
│   └── FSM state machines per user conversation                 │
├────────────────────────────────────────────────────────────────┤
│  Limit / Shield Engine   (limit_engine.py)                     │
│   ├── WebSocket oracle: Helius accountSubscribe (WSS)          │
│   ├── Periodic price poll: 10-second HTTP fallback             │
│   ├── asyncio.Lock per (user_id, mint) — race guard            │
│   └── Per-mint oracle registry + 5-min resolution cache        │
├────────────────────────────────────────────────────────────────┤
│  Swap Router   (swap_engine/router.py)                         │
│   ├── Jupiter v6 Aggregator (graduated tokens)                 │
│   ├── Pump.fun bonding-curve native API                        │
│   ├── Raydium CPMM/CLMM fallback                               │
│   └── PumpPortal Lightning (sniping path)                      │
├────────────────────────────────────────────────────────────────┤
│  Database   (db_handler_aio.py / aiosqlite)                    │
│   ├── Forward-only schema migrations on every startup          │
│   ├── Async read/write with connection-pool timeout            │
│   └── PnL accounting, trade history, per-user settings        │
└────────────────────────────────────────────────────────────────┘
```

**Technology Stack**

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Async runtime | `asyncio`, `aiohttp`, `aiosqlite` |
| Telegram | `aiogram` 3.x with FSM |
| Blockchain I/O | `solders`, `solana-py`, custom RPC wrappers |
| Real-time data | Helius WebSocket `accountSubscribe` |
| Price oracles | Jupiter v6, DexScreener, Birdeye, on-chain reserves |
| Execution | Jito MEV bundles, standard RPC, PumpPortal Lightning |

---

## QA & Complex Problem Solving

This section documents the most technically demanding issues I encountered and resolved in production.

---

### 1. Race Condition: Duplicate Trade Executions

**Symptom**  
After a bot reboot, a single "Timed Exit" limit order fired **twice in the same second**, producing two on-chain sell transactions for the same position and a double notification to the user.

**Root Cause**  
The limit engine is event-driven: Helius WebSocket notifications are routed to `_evaluate_for_mint_event()`, which spawns `asyncio.create_task(_process_auto_sell_config(...))` for each active shield config. Two price events arriving within the same event-loop tick shared the *same empty `processed_this_tick` set* — both tasks passed the `key_te not in processed_this_tick` guard simultaneously, before either had a chance to add the key.

```python
# Before fix — both tasks see an empty set, both proceed
processed_this_tick = set()          # re-created per _evaluate_for_mint_event call
asyncio.create_task(_process_auto_sell_config(cfg, prices, bot, processed_this_tick, ...))
```

**Fix Applied**  
Introduced a per-`(user_id, mint)` `asyncio.Lock` stored in `_shield_eval_locks`. Every call to `_process_auto_sell_config` for a given position is now serialised through this lock:

```python
_shield_eval_locks: dict[tuple[int, str], asyncio.Lock] = {}

def _get_shield_eval_lock(user_id: int, token_mint: str) -> asyncio.Lock:
    key = (int(user_id), str(token_mint).strip())
    lock = _shield_eval_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _shield_eval_locks[key] = lock
    return lock

async def _run_shield_eval(c=cfg, u=uid, ...):
    async with _get_shield_eval_lock(u, mint):
        await _process_auto_sell_config(c, prices, bot, pts, ...)

asyncio.create_task(_run_shield_eval())
```

**Outcome**  
Zero duplicate executions across all tested scenarios: simultaneous WSS events, rapid refreshes, and reboot-triggered expired timers.

---

### 2. Data Precision: Market Cap Calculation Drift

**Symptom**  
The bot's displayed market cap for bonding-curve tokens was 1–3% lower than Pump.fun's native interface and DexScreener, causing incorrect TP/SL percentage triggers.

**Root Cause**  
The Python price-fetch path computed market cap as floating-point:

```python
price_usd = virtual_sol_reserves / virtual_token_reserves  # float division
market_cap = price_usd * token_total_supply                 # accumulated FP error
```

The Pump.fun Rust smart contract uses **integer division on `u64` lamports**, avoiding all floating-point drift:

```rust
// Pump.fun on-chain: BondingCurveAccount::get_market_cap_sol
mc_lamports = (token_total_supply * virtual_sol_reserves) / virtual_token_reserves
```

**Fix Applied**  
Rewrote the MC function to exactly replicate the Rust integer arithmetic:

```python
def pump_curve_market_cap_lamports(
    virtual_token_reserves: int,
    virtual_sol_reserves: int,
    token_total_supply_raw: int,
) -> int:
    """Exact port of Pump.fun BondingCurveAccount::get_market_cap_sol (u64 integer math)."""
    tts = int(token_total_supply_raw)
    vs  = int(virtual_sol_reserves)
    vt  = int(virtual_token_reserves)
    if vt <= 0 or tts <= 0 or vs <= 0:
        return 0
    return (tts * vs) // vt   # integer floor-div — matches on-chain exactly
```

**Outcome**  
MC values now match Pump.fun's native chart to the lamport, eliminating false TP/SL triggers.

---

### 3. Market Parity Bug: "Graduation Trap" — Wrong Supply for FDV

**Symptom**  
After a Pump.fun token graduates to Raydium, the bot reported a market cap ~20–25% lower than DexScreener, DexTools, and the Pump.fun native chart.

**Root Cause**  
The bot called Solana's `getTokenSupply` RPC to find the total supply, then computed `MC = price × circulating_supply`. During graduation, Pump.fun **burns approximately 793 million of the original 1 billion tokens** (the bonding-curve virtual reserve allocation), leaving only ~207 M in circulation. DexScreener and all major aggregators display **FDV = price × 1,000,000,000** (the original fixed total supply), not circulating supply.

```python
# Before — produces ~20% lower MC than any external aggregator
total_supply = await rpc.getTokenSupply(mint)     # returns ~207M post-graduation
market_cap   = price * total_supply               # wrong for graduated tokens
```

**Fix Applied**  
For any mint ending in `pump` (the authoritative Pump.fun mint suffix), hardcode 1 B as the supply basis:

```python
def _supply_for_mc_calc(token_mint: str, rpc_supply: float) -> float:
    """
    Pump.fun tokens always show FDV = price × 1_000_000_000.
    Graduated tokens have a lower on-chain circulating supply because
    ~793M tokens are burned on migration; using that value produces a
    ~20% lower MC than every external aggregator displays.
    """
    if str(token_mint or "").lower().endswith("pump"):
        return 1_000_000_000.0
    return rpc_supply if rpc_supply > 0 else 1_000_000_000.0
```

Applied consistently across limit order trigger-price calculation, market cap display, and Trading Shield range display.

**Outcome**  
MC values for graduated Pump.fun tokens now match DexScreener, DexTools, and Pump.fun native exactly — critical for user trust and accurate TP targets.

---

### 4. System Reliability: Silent WSS Starvation on Low-Volume Pairs

**Symptom**  
Pending limit orders for low-volume graduated Pump.fun tokens never triggered, even when the price had clearly crossed the target. The bot showed `WSS Health: 0 active subs, 0 events/sec` for 12+ consecutive minutes.

**Root Cause — two compounding issues:**

**4a. Oracle Resolution Failure (0 active subs)**  
The oracle resolver (`_resolve_oracle_for_mint`) queries DexScreener for the Raydium `pairAddress` to subscribe to. The reconcile loop runs every 10 seconds — rapid enough to cause rate limiting or transient timeouts. On failure, the resolver silently returned `oracle_type = "unknown"`, leaving `_account_meta_registry` empty and subscribing nothing. There was no retry, no cache, and no warning.

**4b. Architectural Gap (0 events/sec)**  
The fallback HTTP price poll (`_run_fallback_poll_loop`) only ran when `_ws_connected = False`. When WSS was connected but the Raydium pool was idle (no trades → no `accountNotification`), the fallback never triggered. The system was entirely dependent on on-chain activity that simply wasn't happening.

**Fix Applied**

*Part 1 — Oracle Cache + Retry + Jupiter Fallback:*

```python
_oracle_resolution_cache: dict[str, dict] = {}
_ORACLE_CACHE_TTL = 300.0  # 5 minutes — pool addresses are stable

async def _fetch_primary_amm_account(session, mint: str) -> str | None:
    # 1. Try DexScreener twice (1s between attempts)
    for attempt in range(2):
        try:
            async with session.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pair = select_best_dexscreener_pair(data.get("pairs") or [], mint)
                    addr = str((pair or {}).get("pairAddress") or "").strip()
                    if len(addr) >= 32:
                        return addr
        except Exception:
            pass
        if attempt == 0:
            await asyncio.sleep(1.0)

    # 2. Jupiter ammKey fallback — route a dummy 0.001 SOL quote
    #    and extract the on-chain AMM pool account from the route plan
    try:
        async with session.get(
            "https://quote-api.jup.ag/v6/quote",
            params={"inputMint": SOL_MINT, "outputMint": mint, "amount": "1000000"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            if resp.status == 200:
                quote = await resp.json()
                for plan in (quote.get("routePlan") or []):
                    amm_key = str((plan.get("swapInfo") or {}).get("ammKey") or "").strip()
                    if len(amm_key) >= 32:
                        return amm_key
    except Exception:
        pass
    return None
```

*Part 2 — Independent 10-second periodic poll (always runs):*

```python
_LIMIT_POLL_INTERVAL = 10.0  # seconds

async def _run_periodic_limit_poll(bot, session) -> None:
    """Safety-net: evaluate all pending orders every 10s regardless of WSS state.
    Covers: WSS connected but pool idle, oracle resolution failure."""
    while not _engine_shutting_down:
        await asyncio.sleep(_LIMIT_POLL_INTERVAL)
        all_mints = list(set(_pending_limits.keys()) | set(_active_shields_by_token.keys()))
        if all_mints:
            prices = await fetch_prices_batch(session, all_mints)
            for mint, price in (prices or {}).items():
                if price and float(price) > 0:
                    await _evaluate_for_mint_event(bot, session, mint, float(price))
```

**Outcome**  
- Worst-case fill delay reduced from ~30 seconds to ~10 seconds  
- 0-subscription scenarios emit a clear `[ORACLE] WARNING` log  
- Pool addresses cached for 5 minutes: DexScreener/Jupiter called once per token on startup, not every 10-second reconcile  
- Limit orders now trigger reliably on tokens with zero on-chain activity between checks

---

### 5. Forward-Only Database Migrations (Zero-Downtime Schema Evolution)

**Design Challenge**  
The bot runs continuously in production with a live SQLite database. Feature additions require new columns (e.g., `priority_mode`, `slippage_turbo`, `snipe_tg_execution_mode`, `expires_at`). Dropping and recreating the database is not acceptable.

**Solution**  
All schema changes run as idempotent migrations on every startup via `PRAGMA table_info` inspection:

```python
async def _ensure_trade_setting_columns(db) -> None:
    """Forward-only schema migrations — safe to run on every startup."""
    cursor = await db.execute("PRAGMA table_info(users)")
    cols = {row[1] async for row in cursor}

    migrations = {
        "priority_mode":  "ALTER TABLE users ADD COLUMN priority_mode TEXT DEFAULT 'Turbo'",
        "turbo_mode":     "ALTER TABLE users ADD COLUMN turbo_mode INTEGER DEFAULT 1",
        "slippage_turbo": "ALTER TABLE users ADD COLUMN slippage_turbo REAL DEFAULT 1.0",
        "expires_at":     "ALTER TABLE users ADD COLUMN expires_at REAL DEFAULT NULL",
    }
    for col, stmt in migrations.items():
        if col not in cols:
            await db.execute(stmt)
```

**Properties**  
- `ALTER TABLE ... ADD COLUMN` is a no-op if the column already exists (guarded by the `not in cols` check)  
- All migrations are additive — no destructive `DROP` statements ever  
- New installs and migrated production DBs converge to identical schemas automatically

---

### 6. Execution Mode Architecture: Single Source of Truth

**Problem**  
Three independent boolean flags (`mev_enabled`, `turbo_mode`, `mev_level`) could be set to contradictory states (MEV on + Turbo on simultaneously). UI, DB, and swap engine each interpreted the combination differently, causing inconsistent priority fee and Jito tip calculations.

**Solution**  
Collapsed all execution flags to a single `priority_mode` enum (`"Standard"` | `"Fast"` | `"Turbo"`) as the sole source of truth. All legacy flags are **derived** from it at read time:

```python
PRIORITY_MODES = ("Standard", "Fast", "Turbo")
# Standard: 0.0005 SOL Jito tip, MEV off, Turbo off
# Fast:     0.0010 SOL Jito tip, MEV on (Low)
# Turbo:    0.0025 SOL Jito tip, MEV on (Pro)

tip_by_priority = {"Standard": 0.0005, "Fast": 0.001, "Turbo": 0.0025}

if raw_priority == "Standard":
    effective_mev_enabled = False
    effective_turbo       = True
    effective_tip         = tip_by_priority["Standard"]
```

Mutual exclusivity is enforced once, at `get_user_trade_settings()`, and all callers receive a clean, consistent dict.

---

## Showcase Code

| File | Purpose |
|---|---|
| `showcase_code/handlers/trading_token_ui.py` | Telegram UI state management, inline keyboard construction, FSM callbacks |
| `showcase_code/db_handler_aio.py` | Async SQLite operations, schema migrations, PnL accounting |

> **Note:** All RPC endpoints, API keys, and wallet addresses have been replaced with `YOUR_RPC_URL_HERE` / `YOUR_WALLET_ADDRESS_HERE` placeholders. Core swap execution and proprietary routing logic are not included.

---

## Metrics (Production)

| Metric | Value |
|---|---|
| Swap execution latency (Jito bundle) | < 400 ms typical |
| Limit order trigger latency | < 10 s (periodic poll) / < 1 s (WSS active) |
| Supported DEXes | Pump.fun bonding curve, Raydium CPMM/CLMM, Jupiter aggregator |
| Concurrent user limit orders | Unlimited (per-mint asyncio.Lock) |
| Database schema migrations | Zero-downtime, fully automated |
| Uptime (systemd auto-restart) | 99.9%+ |

---

## Author

Built and maintained as a solo engineering project.  
Available for QA Engineering, Frontend Engineering, and Python Backend roles.

---

*This repository contains only safe, non-proprietary showcase code. All secrets have been scrubbed.*
