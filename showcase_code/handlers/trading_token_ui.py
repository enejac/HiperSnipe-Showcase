"""Token trade interface: build/show/refresh UI, trade_token, select_token, show_hidden, add_token."""

import asyncio
import logging
import time

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import BufferedInputFile, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions, Message

from shared import Form, DebugClickMiddleware, get_keyboard, safe_edit_text, next_priority_mode
from db_handler_aio import (
    get_user,
    get_token_pnl_data,
    get_user_trade_settings,
    update_user_trade_settings,
    get_auto_sell_config,
    update_global_shield,
    get_user_settings,
    get_hiper_buyer_count,
    update_user_settings,
    update_auto_sell_triggered,
    get_token_trade_config,
    update_token_trade_config,
)
from menus import (
    make_wallet_menu_keyboard,
    make_token_trade_keyboard,
    make_zero_typing_shield_keyboard,
    make_hybrid_limit_management_rows,
    merge_trade_keyboard_with_hybrid_rows,
)
from assets import fetch_user_assets, check_token_security, check_dex_paid, fetch_single_token_data, fetch_token_security_details, get_cached_rugcheck_report
from utils import format_market_cap, set_route, shorten_address

logger = logging.getLogger(__name__)

router = Router()
router.callback_query.middleware(DebugClickMiddleware())

# -----------------------------------------------------------------------------
# Token trade interface (build / show)
# -----------------------------------------------------------------------------


async def _build_token_trade_interface(user_id: int, token_mint: str, *, fast: bool = False) -> tuple[str, InlineKeyboardMarkup, str, float, float] | None:
    """Build trade interface text + keyboard. Returns (text, keyboard, symbol, token_qty, balance_usd) or None on error.

    fast=True: uses fetch_single_token_data (1 token only, parallel IO) instead of fetch_user_assets (all tokens).
    """
    import datetime as _dt
    _build_start_utc = _dt.datetime.now(_dt.timezone.utc)

    if fast:
        fast_data = await fetch_single_token_data(user_id, token_mint)
        if not fast_data:
            return None
        token_info = fast_data["token_info"]
        sol_balance = fast_data["sol_balance"]
        _ = fast_data["wallet_address"]
    else:
        user_data = await get_user(user_id=user_id)
        if not user_data or user_data.get('wallet_address') == "NOT_SET":
            return None
        asset_info = await fetch_user_assets(user_id)
        token_info = None
        for t in asset_info.get('holdings', []):
            if t['mint'] == token_mint:
                token_info = t
                break
        if not token_info:
            # Single source of truth: fetch_single_token_data gets price + balance + PnL in one go
            single_data = await fetch_single_token_data(user_id, token_mint)
            if single_data:
                token_info = single_data["token_info"]
                sol_balance = single_data["sol_balance"]
                _ = single_data.get("wallet_address", "N/A")
            else:
                from assets import fetch_token_metadata_and_price, fetch_sol_price
                from pnl_tracker import calculate_pnl
                sol_price = await fetch_sol_price()
                meta = await fetch_token_metadata_and_price(token_mint, sol_price)
                current_price = float(meta.get('price_usd') or 0)
                pnl_data = await get_token_pnl_data(user_id, token_mint)
                pnl_result = None
                if pnl_data:
                    pnl_result = calculate_pnl(
                        pnl_data, current_price or 0.0, 0.0, current_sol_price=sol_price,
                        symbol=meta.get('symbol') or 'UNKNOWN',
                    )
                token_info = {
                    'symbol': meta.get('symbol') or 'UNKNOWN',
                    'name': meta.get('name') or 'Unknown Token',
                    'balance': 0,
                    'sol_value': 0,
                    'usd_value': 0,
                    'price_usd': current_price,
                    'price_in_sol': meta.get('price_in_sol', 0),
                    'market_cap': meta.get('market_cap', 0),
                    'is_pumpfun_bonding': bool(meta.get('is_pumpfun_bonding', False)),
                    'pnl_percent': pnl_result.get('pnl_percentage') if pnl_result else None,
                    'pnl_usd': pnl_result.get('pnl_usd') if pnl_result else None,
                    'pnl_sol': pnl_result.get('realized_pnl_sol') if pnl_result else None,
                    'realized_pnl_sol': pnl_result.get('realized_pnl_sol') if pnl_result else None,
                    'unrealized_pnl_sol': pnl_result.get('unrealized_pnl_sol') if pnl_result else None,
                    'avg_entry_price': pnl_result.get('avg_buy_price') if pnl_result else None,
                    'liquidity_usd': meta.get('liquidity_usd'),
                    'liquidity_sol': meta.get('liquidity_sol'),
                    'dexscreener_url': meta.get('dexscreener_url'),
                    'twitter_url': meta.get('twitter_url'),
                    'telegram_url': meta.get('telegram_url'),
                    'solscan_url': meta.get('solscan_url'),
                    'pair_created_at': meta.get('pair_created_at'),
                    'volume_h24': meta.get('volume_h24'),
                    'volume_m5': meta.get('volume_m5'),
                }
                sol_balance = asset_info.get('sol_balance', 0)
                _ = asset_info.get('wallet_address', 'N/A')
        else:
            # token_info was found in holdings
            sol_balance = asset_info.get('sol_balance', 0)
            _ = asset_info.get('wallet_address', 'N/A')

    symbol = token_info['symbol']
    token_name = (token_info.get('name') or symbol or 'Unknown Token').strip()
    if symbol == 'UNKNOWN' or token_name == 'Unknown Token':
        token_name = shorten_address(token_mint, head=6) if token_mint else "Unknown"
    # Full CA for tap-to-copy (exact address, no truncation)
    full_token_address = token_mint or ""
    token_qty = token_info.get('balance', 0)
    balance_usd = token_info.get('usd_value', 0)
    is_pumpfun_bonding = bool(token_info.get("is_pumpfun_bonding", False))
    is_pump_mint = (token_mint or "").lower().endswith("pump")
    is_pump_token = is_pumpfun_bonding or is_pump_mint
    # For sniped tokens (e.g. Token-2022 without "pump" suffix): if metadata says not bonding, still check curve so manual sell uses Pump route
    curve_check_task = None
    if not is_pump_token and token_mint and len(token_mint) >= 43:
        from pump_utils import fetch_pump_fun_price_direct
        curve_check_task = fetch_pump_fun_price_direct(token_mint)
    # Pre-route cache: so Buy/Sell can skip platform re-detection and Jupiter quote for Pump.fun
    platform = "pumpfun" if is_pump_token else "jupiter"
    set_route(token_mint, platform)
    # Parallel: security, DEX paid, trade settings, auto-sell, user settings, HiperSnipe buyer count, token trade config, optional curve check
    security_task = check_token_security(token_mint)
    security_details_task = fetch_token_security_details(token_mint)
    dex_paid_task = check_dex_paid(token_mint)
    ts_task = get_user_trade_settings(user_id)
    as_task = get_auto_sell_config(user_id, token_mint)
    us_task = get_user_settings(user_id)
    hiper_buyers_task = get_hiper_buyer_count(token_mint)
    ttc_task = get_token_trade_config(user_id, token_mint)
    gather_tasks = [
        security_task, security_details_task, dex_paid_task, ts_task, as_task, us_task, hiper_buyers_task, ttc_task
    ]
    if curve_check_task is not None:
        gather_tasks.append(curve_check_task)
    gathered = await asyncio.gather(*gather_tasks)
    security_result = gathered[0]
    sec_details = gathered[1]
    dex_paid = gathered[2]
    ts = gathered[3]
    auto_sell_cfg = gathered[4]
    _ab_settings = gathered[5]
    hiper_buyers = gathered[6]
    _ttc = gathered[7]
    if curve_check_task is not None and len(gathered) > 8:
        curve_data = gathered[8]
        if curve_data and (curve_data.get("price_sol") or 0) > 0:
            is_pump_token = True
            is_pumpfun_bonding = True
            platform = "pumpfun"
            set_route(token_mint, "pumpfun")
    security_status = security_result.get("status", "Warning") if isinstance(security_result, dict) else (security_result or "Warning")
    rug_check_failed = security_result.get("rug_check_failed", False) if isinstance(security_result, dict) else False
    # When fast=True, prefer token_info.dex_paid from fetch_single_token_data (already fetched in parallel there)
    if fast and "dex_paid" in token_info:
        dex_paid = token_info.get("dex_paid")  # True / False / None (None = UNKNOWN)
    _ttc = _ttc or {}

    # Security section: 🟢/🔴 icons. Pump.fun bonding tokens: mint/freeze are typically fixed/safe.
    def _sec_icon(v, pump_safe_default=False):
        if v is True:
            return "🟢"
        if v is False:
            return "🔴"
        return "🟢" if pump_safe_default else "⚪"
    _pump_safe = is_pumpfun_bonding or is_pump_mint
    ren = _sec_icon(sec_details.get("renounced"), pump_safe_default=_pump_safe)
    lp = _sec_icon(sec_details.get("lp_burnt"), pump_safe_default=False)  # LP not applicable on bonding
    fr = _sec_icon(sec_details.get("freeze_revoked"), pump_safe_default=_pump_safe)
    security_line = (
        f"🟢 <b>Security:</b> [Mint Auth Disabled] {ren} | [LP Burned] {lp} | [Freeze Revoked] {fr}"
    )
    if security_status == "Danger":
        security_line = "🚫 [DANGER] " + security_line
    elif security_status == "Warning":
        security_line = "⚠️ [High Risk (RugCheck Flagged)] " + security_line
    if rug_check_failed:
        security_line = "🚨 [RugCheck failed] " + security_line

    # Unified on-chain MC/price (bonding curve) — only while still on-curve.
    # Graduated ...pump tokens must keep DexScreener/Jupiter MC (matches dexscreener.com).
    if is_pumpfun_bonding and token_mint:
        try:
            from pump_utils import (
                get_cached_sol_usd_price,
                get_real_mc,
                get_cached_pump_token_total_supply_raw,
                pump_fdv_to_price_usd,
            )

            sol_usd_price = await get_cached_sol_usd_price()
            real_mc = await get_real_mc(token_mint, sol_usd_price)
            if real_mc and float(real_mc) > 0:
                token_info["market_cap"] = float(real_mc)
                token_info["price_usd"] = pump_fdv_to_price_usd(
                    float(real_mc), get_cached_pump_token_total_supply_raw(token_mint)
                )
                token_info["price_in_sol"] = token_info["price_usd"] / float(sol_usd_price) if sol_usd_price else 0
                logger.debug("[MC UI] Unified on-chain MC for %s: mc=$%.6f price=$%.12f", token_mint[:8], float(real_mc), token_info["price_usd"])
        except Exception as mc_exc:
            logger.debug("[MC UI] Unified on-chain MC failed for %s: %s", (token_mint or "")[:8], mc_exc)

    price_usd = token_info.get('price_usd') or 0
    if price_usd and price_usd < 1.0:
        price_copy = f"{price_usd:.6f}"
    elif price_usd and price_usd >= 1.0:
        price_copy = f"{price_usd:.2f}"
    else:
        price_copy = "Fetching..."
    mc_display = format_market_cap(token_info.get('market_cap'))
    pnl_val = token_info.get('pnl_percent')
    pnl_usd_val = token_info.get('pnl_usd')
    pnl_sol_val = token_info.get('pnl_sol')  # same as realized_pnl_sol
    realized_sol = token_info.get('realized_pnl_sol')
    unrealized_sol = token_info.get('unrealized_pnl_sol')
    if realized_sol is None:
        realized_sol = pnl_sol_val
    avg_entry = token_info.get('avg_entry_price')
    effective_zero_balance = (token_qty is not None and float(token_qty or 0) < 0.000001)
    position_closed = bool(effective_zero_balance)
    # UI race guard: immediately after a successful buy, RPCs may still return 0 balance for a short window.
    # Do NOT display CLOSED/-100% during that indexing window.
    syncing_after_buy = False
    try:
        last_ts = token_info.get("last_trade_ts")
        last_type = (token_info.get("last_trade_type") or "").strip().lower()
        if last_ts and last_type == "buy":
            # timestamps are stored in seconds
            if (time.time() - float(last_ts)) <= 20.0 and position_closed:
                syncing_after_buy = True
                position_closed = False
    except Exception:
        syncing_after_buy = False
    # Market Cap + Liquidity % of MC (use liquidity_usd from API when available)
    liq_sol = token_info.get("liquidity_sol")
    liq_usd = token_info.get("liquidity_usd")
    mc_val = token_info.get("market_cap") or 0
    liq_pct = None
    if mc_val and liq_usd is not None:
        try:
            lu = float(liq_usd)
            mv = float(mc_val)
            if mv > 0 and lu > 0:
                liq_pct = (lu / mv) * 100
        except (TypeError, ValueError):
            pass
    # Bonding curve tokens: show "Bonding Curve" or "0 SOL (Pump)" instead of Liq: N/A
    if is_pumpfun_bonding or is_pump_mint:
        if liq_sol is None or (liq_sol is not None and float(liq_sol) == 0):
            liq_sol_str = "Bonding Curve"
        else:
            liq_sol_str = f"<b>{float(liq_sol):.0f} SOL</b>"
    else:
        liq_sol_str = f"<b>{float(liq_sol):.0f} SOL</b>" if liq_sol is not None else "N/A"
    liq_pct_str = f"(<b>{liq_pct:.2f}%</b>)" if liq_pct is not None else ""

    if dex_paid is True:
        dex_line = "DEX: ✅ PAID"
        dex_display = "✅ <b>DEX:</b> PAID"
    elif dex_paid is False:
        dex_line = "DEX: ❌ NOT PAID"
        dex_display = "❌ <b>DEX:</b> NOT PAID"
    else:
        dex_line = "DEX: ❓ UNKNOWN"
        dex_display = "❓ <b>DEX:</b> UNKNOWN"

    # Pro metrics: token age, dev sold, volume (5m/1h)
    def _format_age(ts_ms_or_s):
        if ts_ms_or_s is None:
            return None
        try:
            t = float(ts_ms_or_s)
            if t > 1e12:
                t = t / 1000.0  # ms -> s
            age_sec = max(0, int(time.time() - t))
            if age_sec < 60:
                return f"{age_sec}s ago"
            if age_sec < 3600:
                return f"{age_sec // 60} mins ago"
            if age_sec < 86400:
                return f"{age_sec // 3600}h ago"
            return f"{age_sec // 86400}d ago"
        except (TypeError, ValueError):
            return None
    token_age_str = _format_age(token_info.get("pair_created_at"))
    dev_sold = sec_details.get("dev_sold")
    vol_5m = token_info.get("volume_m5")
    vol_1h = token_info.get("volume_h24")
    def _fmt_vol(v):
        if v is None or (isinstance(v, (int, float)) and v <= 0):
            return "--"
        try:
            v = float(v)
            if v >= 1_000_000:
                return f"${v/1e6:.2f}M"
            if v >= 1_000:
                return f"${v/1e3:.2f}K"
            return f"${v:.2f}"
        except (TypeError, ValueError):
            return "--"
    dex_paid_status = dex_line
    risk_line = ""
    if security_status == "Danger":
        risk_line = "🚫 [DANGER]"
    elif security_status == "Warning":
        risk_line = "⚠️ [High Risk (RugCheck Flagged)]"
    dev_sold_status = "Yes ✅" if dev_sold is True else "No ❌" if dev_sold is False else "N/A"
    platform_short = "Pump\u200b.fun" if is_pump_token else "Raydium"
    price_display = "Fetching..." if price_copy == "Fetching..." else (f"${price_copy}" if price_copy else "--")
    age_display = token_age_str if token_age_str else "--"
    vol_5m_str = _fmt_vol(vol_5m)
    vol_24h_str = _fmt_vol(vol_1h)
    avg_entry_display = "--"
    if not position_closed and avg_entry is not None and float(avg_entry or 0) > 0:
        try:
            # Display Entry MC = avg_entry_price * total_supply.
            # Use token_info supply first; fall back to 1B (standard Pump.fun supply).
            _ts = token_info.get("total_supply") or token_info.get("supply")
            _supply = float(_ts) if _ts is not None and float(_ts) > 0 else 1_000_000_000.0
            _entry_mc = float(avg_entry) * _supply
            avg_entry_display = format_market_cap(_entry_mc) if _entry_mc > 0 else "--"
        except Exception:
            avg_entry_display = "--"
    # Lifetime PnL: Realized = net profit/loss from closed portions (0 until first sell). Unrealized = open position.
    pnl_indicator = "🟢" if (pnl_val is not None and float(pnl_val) > 0) else "🔴" if (pnl_val is not None and float(pnl_val) < 0) else "⚪"
    def _fmt_sol(v: float) -> str:
        return f"{v:.6f}".rstrip("0").rstrip(".") or "0"
    r_sol = float(realized_sol) if realized_sol is not None else 0.0
    realized_str = f"+{_fmt_sol(r_sol)} SOL" if r_sol >= 0 else f"{_fmt_sol(r_sol)} SOL"
    if position_closed and not syncing_after_buy:
        if pnl_val is not None and float(pnl_val) != 0:
            pnl_float = max(float(pnl_val), -100.0)
            pnl_pct_str = f"<b>+{pnl_float:.1f}%</b>" if pnl_float > 0 else f"{pnl_float:.1f}%"
        else:
            pnl_pct_str = "--"
        if r_sol != 0:
            lifetime_pnl_display = f"Realized: {realized_str}"
            if pnl_pct_str != "--":
                lifetime_pnl_display += f" ({pnl_pct_str})"
        else:
            lifetime_pnl_display = ""
    elif pnl_val is not None:
        pnl_float = max(float(pnl_val), -100.0)
        if pnl_float > 0:
            pnl_pct_str = f"<b>+{pnl_float:.1f}%</b>"
        else:
            pnl_pct_str = f"+{pnl_float:.1f}%" if pnl_float >= 0 else f"{pnl_float:.1f}%"
        pnl_usd_str = ""
        if pnl_usd_val is not None and float(pnl_usd_val) != 0:
            pnl_usd_str = f"+${float(pnl_usd_val):.2f}" if float(pnl_usd_val) >= 0 else f"-${abs(float(pnl_usd_val)):.2f}"
        if unrealized_sol is not None:
            u_sol = float(unrealized_sol)
            unrealized_str = f"+{_fmt_sol(u_sol)} SOL" if u_sol >= 0 else f"{_fmt_sol(u_sol)} SOL"
            lifetime_pnl_display = f"Realized: {realized_str} | Unrealized: {unrealized_str}"
        else:
            lifetime_pnl_display = f"Realized: {realized_str}"
        if pnl_pct_str != "--":
            lifetime_pnl_display += f" ({pnl_pct_str})"
        elif pnl_usd_str:
            lifetime_pnl_display += f" ({pnl_usd_str})"
    else:
        pnl_pct_str = "--"
        lifetime_pnl_display = ""
    status_line = "🔴 CLOSED" if position_closed else ("🟢 OPEN (⏳ Syncing...)" if syncing_after_buy else "🟢 OPEN")
    position_usd_str = f"${balance_usd:.2f}" if not syncing_after_buy else "--"
    buyers_display = hiper_buyers if hiper_buyers is not None else "N/A"

    if syncing_after_buy:
        qty_str = "⏳ Fetching..."
    else:
        if token_qty >= 1_000_000:
            qty_str = f"{token_qty:,.0f}"
        elif token_qty >= 1:
            qty_str = f"{token_qty:,.2f}"
        else:
            qty_str = f"{token_qty:.4f}"

    priority_mode = (ts.get("priority_mode") or "Standard").strip()
    if priority_mode not in ("Standard", "Fast", "Turbo"):
        priority_mode = "Standard"
    _now = _build_start_utc
    _ts = _now.strftime("%H:%M:%S")
    s = _ab_settings or {}
    fee_buy = float(s.get("prio_fee_buy") or 0.002)
    slip_turbo = float(s.get("slippage_turbo") or 1.0)
    slip_bonding = float(s.get("slippage_bonding", 15.0) or 15.0)
    if is_pump_token:
        if slip_bonding < 25.0:
            slip_display = "Slippage: 25% 💊 (Pump Optimized)"
        else:
            slip_display = f"Slippage: {slip_bonding}% 💊 (Pump Optimized)"
    else:
        slip_display = f"Slippage: {slip_turbo}%"
    auto_sell_active = bool(auto_sell_cfg and auto_sell_cfg.get("is_enabled"))
    # Single source of truth: only auto_sell_configs for this token (no global fallback)
    if auto_sell_active:
        shield_tag = "🟢 ON"
    else:
        shield_tag = "🔴 OFF"

    # Semantic layout: header → token identity → market overview → security → position → trade settings → footer
    liq_display = f"{liq_sol_str} {liq_pct_str}".strip() if liq_pct_str else liq_sol_str

    # Visual indent for sub-lines (non-breaking spaces so Telegram keeps the padding).
    indent = "\u00a0\u00a0\u00a0"

    # RugCheck: use pre-fetched cache (populated when user pasted CA)
    rugcheck = get_cached_rugcheck_report(token_mint)
    rugcheck_header = ""
    rugcheck_safe_tag = ""
    rugcheck_bullet = ""
    if rugcheck:
        status_rc = (rugcheck.get("status") or "").strip()
        if status_rc in ("Danger", "Scam"):
            risk_detail = (rugcheck.get("risk_details") or "").strip() or "Bundled Supply / Malicious Code"
            rugcheck_header = (
                "🚨 ⚠️ <b>RUGCHECK DANGER: "
                + risk_detail
                + " ⚠️ 🚨</b>\n\n"
            )
            rugcheck_bullet = f"{indent}🚨 <b>RugCheck:</b> DANGER"
        elif rugcheck.get("is_safe") or status_rc == "Good":
            rugcheck_safe_tag = "\n🛡️ <b>RugCheck: SAFE</b>"
            rugcheck_bullet = f"{indent}🚨 <b>RugCheck:</b> SAFE"

    # Show Avg Entry only when user still holds a position (not CLOSED and balance > 0).
    has_position = (not position_closed) and (token_qty is not None and float(token_qty or 0) >= 0.000001)

    text = (
        rugcheck_header
        + "📊 <b>Market Dashboard</b>\n"
        f"💎 <b>Balance:</b> {sol_balance:.4f} SOL\n\n"
        f"📈 <b>{token_name}</b> (${symbol})\n"
        f"<code>{full_token_address}</code>\n\n"
        "🌍 <b>Market Overview</b>\n"
        f"{indent}💲 <b>Price:</b> {price_display} | ⏳ <b>Age:</b> {age_display}\n"
        f"{indent}💎 <b>MC:</b> {mc_display} | 💧 <b>Liq:</b> {liq_display}\n"
        f"{indent}📊 <b>Vol (5m/24h):</b> {vol_5m_str} / {vol_24h_str}\n"
        f"📍 <b>Platform:</b> {platform_short}\n\n"
        "🛡️ <b>Security &amp; Trust</b>\n"
        + (f"{risk_line}\n" if risk_line else "")
        + f"{indent}👨‍💻 <b>Dev Sold:</b> {dev_sold_status}\n"
        + f"{indent}🔒 <b>Auth:</b> {ren} | <b>LP:</b> {lp} | <b>Freeze:</b> {fr}{rugcheck_safe_tag}\n"
        + (f"{rugcheck_bullet}\n" if rugcheck_bullet else "")
        + f"{indent}{dex_display}\n\n"
        "💰 <b>Your Position</b>\n"
        f"{indent}📌 <b>Status:</b> {status_line}\n"
        f"{indent}💼 <b>Holding:</b> {qty_str} ({position_usd_str})\n"
        + (f"{indent}🎯 <b>Avg Entry MC:</b> {avg_entry_display}\n" if has_position else "")
        + (f"{indent}📈 <b>PnL:</b> {pnl_indicator} {lifetime_pnl_display}\n" if lifetime_pnl_display else "")
        + "\n"
        "⚙️ <b>Trade Settings</b>\n"
        f"{indent}⚡ <b>Fee:</b> {fee_buy} | 📉 <b>{slip_display}</b>\n"
        f"{indent}🛡️ <b>Trading Shield:</b> {shield_tag} | 🚀 <b>Exec:</b> {priority_mode}\n"
        f"{indent}👥 <b>HiperSnipe Buyers:</b> {buyers_display}\n\n"
        f"🔄 <i>Updated: {_ts} UTC</i>"
    )
    # Resolve active slippage for toggle highlight (token-type aware presets)
    if is_pump_token:
        slippage_presets = (15, 25, 50)
        slip_raw = slip_bonding
    else:
        slippage_presets = (3, 15, 35)
        slip_raw = float(s.get("slippage_turbo") or s.get("slippage_mev") or s.get("slippage", 15) or 15)
    active_slippage = min(slippage_presets, key=lambda x: abs(x - slip_raw))

    social_links = {}
    for key, url_key in (
        ("twitter_url", "twitter_url"),
        ("telegram_url", "telegram_url"),
        ("dexscreener_url", "dexscreener_url"),
        ("solscan_url", "solscan_url"),
    ):
        val = token_info.get(url_key)
        if val and str(val).startswith("http"):
            social_links[key] = val
    if not social_links.get("solscan_url"):
        social_links["solscan_url"] = f"https://solscan.io/token/{token_mint}"
    if not social_links.get("dexscreener_url"):
        social_links["dexscreener_url"] = f"https://dexscreener.com/solana/{token_mint}"

    # Log Trading Shield values from DB for debugging (TP/SL must come from auto_sell_configs)
    if auto_sell_cfg:
        _tl = (auto_sell_cfg.get("tp_levels") or [])
        _tp = _tl[0].get("pct") if _tl else None
        _sc = auto_sell_cfg.get("sl_config") or {}
        _sl = _sc.get("pct") if _sc else None
        logger.info("UI REFRESH: Rendering Trading Shield for %s with TP:%s SL:%s", token_mint[:8], _tp, _sl)
    keyboard = make_token_trade_keyboard(
        token_mint, symbol, priority_mode=priority_mode,
        auto_sell_config=auto_sell_cfg, auto_buy_active=float(s.get("auto_buy_amount", 0) or 0) > 0,
        buy_amount_1=float(s.get("buy_amount_1", 0.1) or 0.1),
        buy_amount_2=float(s.get("buy_amount_2", 0.25) or 0.25),
        buy_amount_3=float(s.get("buy_amount_3", 0.5) or 0.5),
        buy_amount_4=float(s.get("buy_amount_4", 1.0) or 1.0),
        sell_percent_1=int(s.get("sell_percent_1", 25) or 25),
        sell_percent_2=int(s.get("sell_percent_2", 50) or 50),
        sell_percent_3=int(s.get("sell_percent_3", 100) or 100),
        active_slippage=active_slippage,
        slippage_presets=slippage_presets,
        social_links=social_links,
        is_bonding_curve_token=is_pump_token,
    )
    return (text, keyboard, symbol, token_qty, balance_usd)


def _set_trade_status_line(text: str, status: str) -> str:
    """Replace the 📌 Status line in trade/hybrid UI text (same rules as trading_buy_sell)."""
    marker = "📌 <b>Status:</b>"
    idx = text.find(marker)
    if idx == -1:
        return text
    line_start = text.rfind("\n", 0, idx)
    if line_start == -1:
        line_start = 0
    else:
        line_start += 1
    line_end = text.find("\n", idx)
    if line_end == -1:
        line_end = len(text)
    new_line = f"{marker} {status}"
    return text[:line_start] + new_line + text[line_end:]


async def build_hybrid_trade_limits_ui(
    user_id: int,
    token_mint: str,
    *,
    fast: bool = True,
    live_price_usd: float | None = None,
) -> tuple[str, InlineKeyboardMarkup, str, float, float] | None:
    """Token trade card + active limits for this mint + merged keyboard (trading rows, then limit rows)."""
    from handlers.trading_limits import build_active_limits_section_for_token
    from assets import fetch_sol_price

    result = await _build_token_trade_interface(user_id, token_mint, fast=fast)
    if not result:
        return None
    text, keyboard, symbol, token_qty, balance_usd = result
    try:
        sol_price = float(await fetch_sol_price() or 0)
    except Exception:
        sol_price = 0.0
    section_lines, orders = await build_active_limits_section_for_token(
        user_id, token_mint, sol_price, live_price_usd=live_price_usd
    )
    full_text = f"{text}\n\n---\n<b>📌 Active limits (this token)</b>\n{section_lines}"
    hybrid_rows = make_hybrid_limit_management_rows(token_mint, orders)
    merged_kb = merge_trade_keyboard_with_hybrid_rows(keyboard, hybrid_rows)
    return (full_text, merged_kb, symbol, token_qty, balance_usd)


async def refresh_hybrid_ui_by_ids(
    bot,
    user_id: int,
    token_mint: str,
    chat_id: int,
    message_id: int,
    *,
    status_override: str | None = None,
) -> bool:
    """Edit message by id to hybrid card; register for WSS refresh."""
    from limit_engine import register_hybrid_ui_message, get_cached_limit_price_for_mint

    token_mint = (token_mint or "").strip()
    if not token_mint:
        return False
    try:
        live = get_cached_limit_price_for_mint(token_mint)
        result = await asyncio.wait_for(
            build_hybrid_trade_limits_ui(user_id, token_mint, fast=True, live_price_usd=live),
            timeout=15.0,
        )
        if not result:
            return False
        text, keyboard, *_ = result
        if status_override:
            text = _set_trade_status_line(text, status_override)
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=keyboard,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        register_hybrid_ui_message(user_id, token_mint, chat_id, message_id)
        return True
    except Exception as e:
        logger.warning("refresh_hybrid_ui_by_ids failed: %s", e)
        return False


async def refresh_hybrid_ui(
    bot,
    user_id: int,
    token_mint: str,
    message: Message,
    *,
    status_override: str | None = None,
) -> bool:
    """Edit an existing Telegram message to the hybrid trade+limits view."""
    return await refresh_hybrid_ui_by_ids(
        bot,
        user_id,
        token_mint,
        message.chat.id,
        message.message_id,
        status_override=status_override,
    )


async def send_token_trade_ui(
    bot,
    user_id: int,
    token_mint: str,
    *,
    timeout: float = 8.0,
    message_to_edit: Message | None = None,
    edit_chat_id: int | None = None,
    edit_message_id: int | None = None,
) -> tuple[bool, Message | None]:
    """Build and send (or edit) the token trade UI for the given token. Used after CA snipe instead of limits dashboard.

    If message_to_edit is provided, that message is edited to the trade UI.
    Else if edit_chat_id and edit_message_id are set, that message is edited by id.
    Otherwise sends a new message. Returns (success, message or None).
    """
    try:
        result = await asyncio.wait_for(_build_token_trade_interface(user_id, token_mint, fast=True), timeout=timeout)
        if not result:
            return False, None
        text, keyboard, _symbol, _qty, _balance = result
        opts = {"parse_mode": "HTML", "link_preview_options": LinkPreviewOptions(is_disabled=True)}
        if message_to_edit is not None:
            await message_to_edit.edit_text(text, reply_markup=keyboard, **opts)
            return True, message_to_edit
        if edit_chat_id is not None and edit_message_id is not None:
            await bot.edit_message_text(
                chat_id=edit_chat_id,
                message_id=edit_message_id,
                text=text,
                reply_markup=keyboard,
                **opts,
            )
            return True, None
        msg = await bot.send_message(user_id, text, reply_markup=keyboard, **opts)
        return True, msg
    except asyncio.TimeoutError:
        logger.warning("send_token_trade_ui timed out for user_id=%s token=%s", user_id, token_mint[:8])
        return False, None
    except Exception as e:
        logger.warning("send_token_trade_ui failed for user_id=%s token=%s: %s", user_id, token_mint[:8], e)
        return False, None


async def _show_token_trade_interface(callback: CallbackQuery, state: FSMContext, token_mint: str) -> None:
    """Render the token trade UI for the given token_mint. Used by trade_token_, refresh_token_."""
    try:
        user_id = callback.from_user.id
        logger.info(f"TRADE TOKEN: User {user_id} wants to trade {token_mint[:8]}...")
        # Auto-jump to bottom: delete old message (best-effort) so new UI appears at bottom of chat.
        chat_id = callback.message.chat.id
        data = await state.get_data()
        sub_msg = data.get("submenu_msg_id")
        if sub_msg:
            try:
                await callback.bot.delete_message(chat_id, sub_msg)
            except Exception:
                pass
            await state.update_data(submenu_msg_id=None)
        trade_msg = data.get("trade_ui_msg_id")
        # Old message to remove: the one we would have edited (trade UI or the message user clicked).
        if trade_msg and sub_msg and callback.message.message_id == sub_msg:
            old_msg_id = trade_msg
        else:
            old_msg_id = callback.message.message_id
        try:
            await callback.bot.delete_message(chat_id, old_msg_id)
        except Exception:
            pass

        # Instant loading state while we build the full dashboard.
        loading_msg = await callback.bot.send_message(
            chat_id,
            "⏳ <b>Analyzing token data...</b>",
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )

        # Use fast single-token path so the trade UI opens quickly and benefits
        # from RAM caches in fetch_single_token_data (DB + price + PnL in one shot).
        result = await _build_token_trade_interface(user_id, token_mint, fast=True)
        if not result:
            await loading_msg.edit_text(
                "⚠️ Please set up your wallet first to trade.",
                reply_markup=get_keyboard(make_wallet_menu_keyboard),
                link_preview_options=LinkPreviewOptions(is_disabled=True),
            )
            return
        text, keyboard, symbol, *_ = result
        await loading_msg.edit_text(
            text,
            reply_markup=keyboard,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        await state.update_data(
            token_contract=token_mint,
            limit_token_symbol=symbol,
            trade_ui_msg_id=loading_msg.message_id,
        )
        try:
            from limit_engine import register_trade_ui_message
            register_trade_ui_message(user_id, token_mint, chat_id, loading_msg.message_id)
        except Exception:
            pass
        await state.set_state(Form.swap_menu)
        await callback.answer()
    except Exception as e:
        logger.exception(f"Error in trade token: {e}")
        try:
            await callback.answer("Error loading trading interface", show_alert=True)
        except Exception:
            pass


async def _refresh_token_view_after_setting_change(
    callback: CallbackQuery, state: FSMContext, token_mint: str
) -> None:
    """Refresh registered trade/hybrid card when possible; avoid editing wrong message (e.g. ⚙️ Settings).

    Bonding toggles (msell/dsell/smig/dtrig) live on Settings — refresh that panel too.
    """
    from limit_engine import get_registered_trade_ui_message, is_hybrid_ui_active

    uid = int(callback.from_user.id)
    tm = (token_mint or "").strip()
    data = (callback.data or "").strip()
    is_bonding_toggle = bool(
        tm
        and (
            data.startswith("msell_")
            or data.startswith("dsell_")
            or data.startswith("smig_")
            or data.startswith("dtrig_")
        )
    )

    meta = get_registered_trade_ui_message(uid, tm) if tm else None
    if tm and meta:
        try:
            if is_hybrid_ui_active(uid, tm):
                await refresh_hybrid_ui_by_ids(
                    callback.bot, uid, tm, int(meta["chat_id"]), int(meta["message_id"])
                )
            else:
                await send_token_trade_ui(
                    callback.bot,
                    uid,
                    tm,
                    edit_chat_id=int(meta["chat_id"]),
                    edit_message_id=int(meta["message_id"]),
                    timeout=12.0,
                )
        except Exception:
            logger.warning("_refresh_token_view_after_setting_change: registered UI refresh failed", exc_info=True)
            if not is_bonding_toggle:
                await _show_token_trade_interface(callback, state, token_mint)
    elif not is_bonding_toggle:
        try:
            if tm and is_hybrid_ui_active(uid, tm):
                if await refresh_hybrid_ui(callback.bot, uid, tm, callback.message):
                    return
        except Exception:
            pass
        await _show_token_trade_interface(callback, state, token_mint)

    if is_bonding_toggle and tm:
        try:
            from handlers.settings import refresh_pro_settings_dashboard_in_place

            await refresh_pro_settings_dashboard_in_place(callback, uid, tm)
        except Exception:
            logger.debug("refresh_pro_settings_dashboard_in_place skipped", exc_info=True)


_refresh_cooldown: dict[int, float] = {}


@router.callback_query(F.data.startswith("trade_token_"))
async def trade_token_callback(callback: CallbackQuery, state: FSMContext):
    """Handle token trading interface - matches screenshot layout"""
    token_mint = callback.data.replace("trade_token_", "")
    await _show_token_trade_interface(callback, state, token_mint)


@router.callback_query(F.data.startswith("disable_shield_"))
async def disable_shield_callback(callback: CallbackQuery, state: FSMContext):
    """Disable Trading Shield for this token; refresh menu to show 'Activate Shield'."""
    logger.info("CALLBACK RECEIVED: %s from %s", callback.data, callback.from_user.id)
    token_mint = callback.data.replace("disable_shield_", "", 1)
    user_id = callback.from_user.id
    try:
        await update_auto_sell_triggered(user_id, token_mint, is_enabled=False)
        await update_global_shield(user_id, enabled=False)
        try:
            from limit_engine import schedule_wss_registry_rebuild

            await schedule_wss_registry_rebuild()
        except Exception:
            pass
        await callback.answer("🛡️ Trading Shield Deactivated.", show_alert=False)

        # Refetch from DB to confirm and drive UI from persisted state
        config = await get_auto_sell_config(user_id, token_mint)
        tp, sl, amt = 100, -20, 100
        if config:
            tp_levels = config.get("tp_levels") or []
            if tp_levels:
                tp = tp_levels[0].get("pct", 100)
                amt = tp_levels[0].get("sell_pct", 100)
            sl_cfg = config.get("sl_config") or {}
            if sl_cfg:
                sl = sl_cfg.get("pct", -20)
        selections = {"tp": tp, "sl": sl, "amt": amt}
        shield_active = bool(config.get("is_enabled")) if config else False
        tp_str = f"+{int(tp)}%" if (tp or 0) >= 0 else f"{int(tp)}%"
        sl_str = f"{int(sl)}%" if (sl or 0) >= 0 else f"{int(sl)}%"
        levels_line = f"<b>Current: TP {tp_str} / Trailing SL {sl_str}</b>\n\n"
        peak_line = ""
        ref_usd = config.get("reference_price_usd") or 0
        hwm_usd = config.get("high_watermark_usd")
        if hwm_usd is not None and ref_usd and float(hwm_usd) > float(ref_usd) * 1.05:
            peak_line = f"Peak Tracked: ${float(hwm_usd):.5g}\n\n"
        kb = make_zero_typing_shield_keyboard(token_mint, selections, shield_active=shield_active)
        text = (
            "🛡️ <b>Trading Shield Configuration</b>\n\n"
            + levels_line
            + peak_line
            + ("Trading Shield is ON — click 'Disable Shield' to turn off." if shield_active else "Select your desired Take Profit and Stop Loss levels below. Click 'Activate Shield' to enable.")
        )
        await safe_edit_text(callback.message, text, reply_markup=kb, callback=callback)
    except Exception as e:
        logger.exception("disable_shield: %s", e)
        try:
            await callback.answer("Error: Update failed", show_alert=True)
        except Exception:
            pass


@router.callback_query(F.data.startswith("refresh_token_"))
async def refresh_token_callback(callback: CallbackQuery, state: FSMContext):
    """Refresh trade UI in-place — single edit, buttons never disappear. Handles API/DB timeouts."""
    user_id = callback.from_user.id
    token_mint = (callback.data or "").replace("refresh_token_", "").strip()
    
    logger.info("UI REFRESH: Clicked by user_id=%s token=%s...", user_id, token_mint[:8])

    now = time.monotonic()
    if now - _refresh_cooldown.get(user_id, 0) < 1.0:
        logger.debug("UI REFRESH: Cooldown active, returning")
        try:
            await callback.answer()
        except Exception:
            pass
        return
    _refresh_cooldown[user_id] = now

    if not token_mint:
        logger.warning("UI REFRESH: Invalid token mint")
        try:
            await callback.answer("Invalid token", show_alert=True)
        except Exception:
            pass
        return

    # NOTE: "ghost mint" cooldown is for background batch price fetching.
    # Manual user refresh should ALWAYS run (at worst it will show partial data).
    try:
        from limit_engine import is_ghost_mint
        if is_ghost_mint(token_mint):
            try:
                await callback.answer("🔄 Refreshing (token on cooldown)...", show_alert=False)
            except Exception:
                pass
    except Exception:
        pass

    try:
        await callback.answer("🔄 Refreshing...", show_alert=False)
    except Exception:
        pass

    _REFRESH_TIMEOUT_S = 12.0
    try:
        from assets import invalidate_token_market_caches

        invalidate_token_market_caches(token_mint)

        from limit_engine import (
            get_cached_limit_price_for_mint,
            is_hybrid_ui_active,
            register_hybrid_ui_message,
            register_trade_ui_message,
        )

        live = get_cached_limit_price_for_mint(token_mint)
        if is_hybrid_ui_active(user_id, token_mint):
            logger.debug("UI REFRESH: hybrid rebuild for %s", token_mint[:8])
            result = await asyncio.wait_for(
                build_hybrid_trade_limits_ui(
                    user_id, token_mint, fast=True, live_price_usd=live
                ),
                timeout=_REFRESH_TIMEOUT_S,
            )
            reg = register_hybrid_ui_message
        else:
            logger.debug("UI REFRESH: trade-only rebuild for %s", token_mint[:8])
            result = await asyncio.wait_for(
                _build_token_trade_interface(user_id, token_mint, fast=True),
                timeout=_REFRESH_TIMEOUT_S,
            )
            reg = register_trade_ui_message
        if not result:
            logger.warning("UI REFRESH: rebuild returned None")
            try:
                await callback.answer("⚠️ Could not fetch data", show_alert=True)
            except Exception:
                pass
            return
        text, keyboard, symbol, *_ = result

        await safe_edit_text(
            callback.message, text,
            reply_markup=keyboard,
            parse_mode="HTML",
            callback=callback,
        )

        await state.update_data(
            token_contract=token_mint,
            limit_token_symbol=symbol,
        )
        try:
            reg(user_id, token_mint, callback.message.chat.id, callback.message.message_id)
        except Exception:
            pass

    except asyncio.TimeoutError:
        logger.warning("REFRESH: timeout after %.0fs for %s", _REFRESH_TIMEOUT_S, token_mint[:8])
        import datetime as _dt
        _ts = _dt.datetime.now(_dt.timezone.utc).strftime("%H:%M:%S")
        fallback_text = (
            "📊 <b>Market Dashboard</b>\n\n"
            f"🪙 <b>Token:</b> <code>{token_mint}</code>\n\n"
            "💰 <b>Price:</b> $N/A (API Busy)\n"
            "📈 <b>MC:</b> $N/A\n\n"
            f"🔄 <i>Updated: {_ts} UTC</i>"
        )
        fallback_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh_token_{token_mint}")],
            [InlineKeyboardButton(text="🔄 Trade More", callback_data=f"trade_token_{token_mint}")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="main_menu")],
        ])
        try:
            await safe_edit_text(
                callback.message, fallback_text,
                reply_markup=fallback_kb,
                parse_mode="HTML",
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                callback=callback,
            )
        except Exception as edit_err:
            logger.warning("REFRESH: fallback edit failed: %s", edit_err)
        try:
            await callback.answer("⏱ Data request timed out. Try again.", show_alert=False)
        except Exception:
            pass

    except Exception as e:
        logger.exception("REFRESH: error for %s: %s", token_mint[:8], e)
        import datetime as _dt
        _ts = _dt.datetime.now(_dt.timezone.utc).strftime("%H:%M:%S")
        fallback_text = (
            "📊 <b>Market Dashboard</b>\n\n"
            f"🪙 <b>Token:</b> <code>{token_mint}</code>\n\n"
            "⚠️ Refresh failed. Try again.\n\n"
            f"🔄 <i>Updated: {_ts} UTC</i>"
        )
        fallback_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh_token_{token_mint}")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="main_menu")],
        ])
        try:
            await safe_edit_text(
                callback.message, fallback_text,
                reply_markup=fallback_kb,
                parse_mode="HTML",
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                callback=callback,
            )
        except Exception:
            pass
        try:
            await callback.answer("Error during refresh", show_alert=False)
        except Exception:
            pass


_SLIPPAGE_CYCLE = (3, 15, 35)
_PUMP_SLIPPAGE_CYCLE = (15, 25, 50)


@router.callback_query(F.data.startswith("cycle_slippage_"))
async def cycle_slippage_callback(callback: CallbackQuery, state: FSMContext):
    """Cycle slippage presets and refresh UI.

    Normal tokens: 3% -> 15% -> 35% -> 3% (updates slippage_turbo & slippage_mev).
    Pump.fun bonding tokens: 15% -> 25% -> 50% -> 15% (updates slippage_bonding).
    """
    try:
        data = callback.data or ""
        user_id = callback.from_user.id
        settings = await get_user_settings(user_id)

        # Pump.fun-specific cycle: prefix cycle_slippage_p_
        if data.startswith("cycle_slippage_p_"):
            token_mint = data.replace("cycle_slippage_p_", "", 1)
            current = float(settings.get("slippage_bonding", 15) or 15)
            cycle = _PUMP_SLIPPAGE_CYCLE
            idx = 0
            for i, pct in enumerate(cycle):
                if abs(current - pct) < 1:
                    idx = (i + 1) % len(cycle)
                    break
            new_val = cycle[idx]
            await update_user_settings(callback.from_user.id, slippage_bonding=new_val)
        else:
            # Default cycle for non-pump tokens
            token_mint = data.replace("cycle_slippage_", "", 1)
            current = float(settings.get("slippage_turbo") or settings.get("slippage_mev") or 15)
            cycle = _SLIPPAGE_CYCLE
            idx = 0
            for i, pct in enumerate(cycle):
                if abs(current - pct) < 1:
                    idx = (i + 1) % len(cycle)
                    break
            new_val = cycle[idx]
            await update_user_settings(user_id, slippage_turbo=new_val, slippage_mev=new_val)

        await callback.answer(f"Slippage: {new_val}%")
        await _refresh_token_view_after_setting_change(callback, state, token_mint)
    except Exception as e:
        logger.exception("cycle_slippage: %s", e)
        await callback.answer("Failed", show_alert=True)


_DEV_TRIGGER_CYCLE = (0, 10, 25, 50, 100)


@router.callback_query(F.data.startswith("msell_"))
async def toggle_migration_sell_callback(callback: CallbackQuery, state: FSMContext):
    """Toggle migration_sell_active for this user+token and refresh the trade UI."""
    token_mint = callback.data.replace("msell_", "", 1)
    user_id = callback.from_user.id
    try:
        cfg = await get_token_trade_config(user_id, token_mint) or {}
        new_val = not bool(cfg.get("migration_sell_active"))
        await update_token_trade_config(user_id, token_mint, migration_sell_active=new_val)
        await callback.answer(f"Migration Sell: {'ON' if new_val else 'OFF'}")
        await _refresh_token_view_after_setting_change(callback, state, token_mint)
    except Exception as e:
        logger.exception("toggle_migration_sell: %s", e)
        try:
            await callback.answer("⚠️ Failed to save setting.", show_alert=True)
        except Exception:
            pass


@router.callback_query(F.data.startswith("dsell_"))
async def toggle_dev_sell_callback(callback: CallbackQuery, state: FSMContext):
    """Toggle dev_sell_active for this user+token and refresh the trade UI."""
    token_mint = callback.data.replace("dsell_", "", 1)
    user_id = callback.from_user.id
    try:
        cfg = await get_token_trade_config(user_id, token_mint) or {}
        new_val = not bool(cfg.get("dev_sell_active"))
        await update_token_trade_config(user_id, token_mint, dev_sell_active=new_val)
        await callback.answer(f"Dev Sell: {'ON' if new_val else 'OFF'}")
        await _refresh_token_view_after_setting_change(callback, state, token_mint)
    except Exception as e:
        logger.exception("toggle_dev_sell: %s", e)
        try:
            await callback.answer("⚠️ Failed to save setting.", show_alert=True)
        except Exception:
            pass


@router.callback_query(F.data.startswith("smig_"))
async def snipe_migration_callback(callback: CallbackQuery, state: FSMContext):
    """Toggle snipe_migration_active for this user+token and refresh the trade UI."""
    token_mint = callback.data.replace("smig_", "", 1)
    user_id = callback.from_user.id
    try:
        cfg = await get_token_trade_config(user_id, token_mint) or {}
        new_val = not bool(cfg.get("snipe_migration_active"))
        await update_token_trade_config(user_id, token_mint, snipe_migration_active=new_val)
        await callback.answer(f"Snipe Migration: {'ON' if new_val else 'OFF'}")
        await _refresh_token_view_after_setting_change(callback, state, token_mint)
    except Exception as e:
        logger.exception("snipe_migration: %s", e)
        try:
            await callback.answer("⚠️ Failed to save setting.", show_alert=True)
        except Exception:
            pass


@router.callback_query(F.data.startswith("dtrig_"))
async def cycle_dev_trigger_callback(callback: CallbackQuery, state: FSMContext):
    """Cycle dev_sell_trigger_pct through 0 -> 10 -> 25 -> 50 -> 100 -> 0."""
    token_mint = callback.data.replace("dtrig_", "", 1)
    user_id = callback.from_user.id
    try:
        cfg = await get_token_trade_config(user_id, token_mint) or {}
        current = int(cfg.get("dev_sell_trigger_pct", 0))
        idx = 0
        for i, val in enumerate(_DEV_TRIGGER_CYCLE):
            if val == current:
                idx = (i + 1) % len(_DEV_TRIGGER_CYCLE)
                break
        new_val = _DEV_TRIGGER_CYCLE[idx]
        await update_token_trade_config(user_id, token_mint, dev_sell_trigger_pct=new_val)
        await callback.answer(f"Dev Trigger: >= {new_val}%")
        await _refresh_token_view_after_setting_change(callback, state, token_mint)
    except Exception as e:
        logger.exception("cycle_dev_trigger: %s", e)
        try:
            await callback.answer("⚠️ Failed to save setting.", show_alert=True)
        except Exception:
            pass


def _generate_pnl_chart_image(
    pnl_percent: float,
    symbol: str,
    entry_price: float | None,
    current_price: float | None,
    bot_username: str = "HiperSnipeBot",
    token_icon_url: str | None = None,
    token_icon_bytes: bytes | None = None,
    sol_price: float | None = None,
) -> bytes:
    """Generate PnL marketing card. MUST call utils.image_generator.generate_pnl_card only (no local ImageDraw).
    Passes pnl_percent, symbol, entry_price, current_price, bot_username, subtitle, token_icon_url, token_icon_bytes, sol_price. Returns PNG bytes."""
    from utils.image_generator import generate_pnl_card

    symbol_safe = (symbol or "TOKEN").strip().upper()
    subtitle = f"{symbol_safe} | Solscan Verified"
    return generate_pnl_card(
        pnl_percent=pnl_percent,
        symbol=symbol_safe,
        entry_price=entry_price,
        current_price=current_price,
        bot_username=bot_username,
        subtitle=subtitle,
        token_icon_url=token_icon_url,
        token_icon_bytes=token_icon_bytes,
        sol_price=sol_price,
    )


@router.callback_query(F.data.startswith("pnl_chart_"))
async def pnl_chart_callback(callback: CallbackQuery, state: FSMContext):
    """Generate PnL share image with referral link and social share buttons (incl. Add to Story)."""
    from urllib.parse import quote_plus
    from aiogram.exceptions import TelegramBadRequest

    try:
        token_mint = callback.data.replace("pnl_chart_", "")
        user_id = callback.from_user.id
        # Answer quickly to avoid Telegram's 10s query timeout; ignore if query is already too old/invalid.
        try:
            await callback.answer("Generating chart...")
        except TelegramBadRequest as te:
            logger.debug("pnl_chart: initial callback.answer skipped (too old/invalid): %s", te)

        from assets import fetch_single_token_data
        from config import BOT_USERNAME, WEBSITE_URL

        bot_name = BOT_USERNAME or "HiperSnipeBot"
        reflink = f"https://t.me/{bot_name}?start=ref_{user_id}"

        fast_data = await fetch_single_token_data(user_id, token_mint)
        if not fast_data:
            await callback.answer("Could not load token data", show_alert=True)
            return
        token_info = fast_data["token_info"]
        symbol = (token_info.get("symbol") or "TOKEN").strip().upper()
        pnl_val = token_info.get("pnl_percent")
        entry = token_info.get("avg_entry_price")
        balance = float(token_info.get("balance") or 0)
        exit_price = token_info.get("exit_price_usd")
        _position_closed = (balance < 0.000001)
        if _position_closed and exit_price is not None and float(exit_price) > 0:
            current_f = float(exit_price)
            price_label = "Exit"
        elif _position_closed:
            current_f = None
            price_label = "Exit"
        else:
            current_f = float(token_info.get("price_usd") or 0) if token_info.get("price_usd") is not None else None
            price_label = "Now"
        if pnl_val is None:
            pnl_val = 0.0
        else:
            pnl_val = max(float(pnl_val), -100.0)
        pnl_str = f"{pnl_val:+.2f}"
        entry_f = float(entry) if entry is not None else None

        token_icon_bytes = None
        logo_uri = token_info.get("logo_uri")
        if logo_uri and isinstance(logo_uri, str) and logo_uri.strip().startswith(("http://", "https://")):
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.get(logo_uri, timeout=aiohttp.ClientTimeout(total=2)) as r:
                        if r.status == 200:
                            token_icon_bytes = await r.read()
            except Exception:
                pass

        png_bytes = _generate_pnl_chart_image(
            pnl_percent=pnl_val,
            symbol=symbol,
            entry_price=entry_f,
            current_price=current_f,
            bot_username=bot_name,
            token_icon_url=logo_uri,
            token_icon_bytes=token_icon_bytes,
            sol_price=fast_data.get("sol_price"),
        )

        pnl_value = float(pnl_val)
        if pnl_value > 0:
            share_line = f"I just printed {pnl_str}% on ${symbol} with @HiperSnipe! 🦅🚀"
        elif pnl_value == 0:
            share_line = f"Broke even at {pnl_str}% on ${symbol} with @HiperSnipe! 🦅⚖️"
        else:
            share_line = f"Capital protected! 🛡️ Cut losses at {pnl_str}% on ${symbol} with @HiperSnipe! 🦅"

        tweet_text = (
            f"{share_line}\n\n"
            f"Use my link for 10% cashback: {reflink}\n\n"
            f"Follow us: @HiperSnipe | {WEBSITE_URL} #Solana #HiperSnipe #Memecoins"
        )
        if pnl_value > 0:
            telegram_text = (
                f"Check out my ${symbol} trade on @HiperSnipe! {pnl_str}% ROI 📈 Use my link for 10% cashback!"
            )
        elif pnl_value == 0:
            telegram_text = (
                f"Broke even on ${symbol} with @HiperSnipe! ⚖️ Use my link for 10% cashback!"
            )
        else:
            telegram_text = (
                f"Capital protected on ${symbol} with @HiperSnipe! 🛡️ Cut losses at {pnl_str}%. Use my link for 10% cashback!"
            )
        twitter_url = "https://twitter.com/intent/tweet?text=" + quote_plus(tweet_text)
        telegram_share_url = "https://t.me/share/url?url=" + quote_plus(reflink) + "&text=" + quote_plus(telegram_text)
        facebook_url = "https://www.facebook.com/sharer/sharer.php?u=" + quote_plus(reflink)

        caption_lines = [
            f"📊 <b>PnL: {pnl_str}%</b> | ${symbol}",
        ]
        if entry_f is not None and current_f is not None:
            caption_lines.append(f"Entry: ${entry_f:.6f} → {price_label}: ${current_f:.6f}")
        caption_lines.append("")
        caption_lines.append("⚡ <b>Trade with the fastest bot on Solana!</b>")
        caption_lines.append(f"🔗 {reflink}")
        caption = "\n".join(caption_lines)

        share_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🐦 X (Twitter)", url=twitter_url),
                InlineKeyboardButton(text="✈️ Telegram", url=telegram_share_url),
            ],
            [
                InlineKeyboardButton(text="🔵 Facebook", url=facebook_url),
                InlineKeyboardButton(text="📱 Copy for IG/TikTok", callback_data=f"pnl_copy_bio_{token_mint}"),
            ],
            [InlineKeyboardButton(text="📱 Add to Story (Telegram)", callback_data=f"pnl_story_{token_mint}")],
        ])
        photo_file = BufferedInputFile(
            file=png_bytes,
            filename=f"pnl_{(token_mint or '')[:8]}_{int(time.time())}.png",
        )
        await callback.message.answer_photo(
            photo=photo_file,
            caption=caption,
            parse_mode="HTML",
            reply_markup=share_kb,
        )
    except Exception as e:
        logger.exception("pnl_chart: %s", e)
        # Best-effort error feedback; ignore 'query is too old/invalid' here as well.
        try:
            await callback.answer("Failed to generate chart", show_alert=True)
        except TelegramBadRequest:
            pass
        except Exception:
            pass


@router.callback_query(F.data.startswith("pnl_story_"))
async def pnl_story_callback(callback: CallbackQuery, state: FSMContext):
    """Show instructions to add the PnL image to Telegram Story (no new message)."""
    try:
        await callback.answer(
            "💡 To add this PnL to your Story:\n\n1. Tap the image above 👆\n2. Click the 'Share' arrow ↗️\n3. Select 'My Story'.",
            show_alert=True,
        )
    except Exception as e:
        logger.exception("pnl_story: %s", e)
        await callback.answer("Something went wrong", show_alert=True)


@router.callback_query(F.data.startswith("pnl_copy_bio_"))
async def pnl_copy_bio_callback(callback: CallbackQuery, state: FSMContext):
    """Send dynamic IG/TikTok caption with actual PnL % and token symbol; message auto-deletes in 30s."""
    from config import BOT_USERNAME, WEBSITE_URL

    async def delete_later(msg, delay=30):
        await asyncio.sleep(delay)
        try:
            await msg.delete()
        except Exception:
            pass

    try:
        user_id = callback.from_user.id
        token_mint = callback.data.replace("pnl_copy_bio_", "", 1)
        if not token_mint:
            await callback.answer("Could not determine token", show_alert=True)
            return
        bot_name = BOT_USERNAME or "HiperSnipeBot"
        reflink = f"https://t.me/{bot_name}?start=ref_{user_id}"

        from assets import fetch_single_token_data
        fast_data = await fetch_single_token_data(user_id, token_mint)
        if not fast_data:
            await callback.answer("Could not load token data", show_alert=True)
            return
        token_info = fast_data["token_info"]
        symbol = (token_info.get("symbol") or "TOKEN").strip().upper()
        pnl_val = token_info.get("pnl_percent")
        if pnl_val is None:
            pnl_percent_str = "0%"
            pnl_value = 0.0
        else:
            pnl_val = max(float(pnl_val), -100.0)
            pnl_percent_str = f"{pnl_val:+.1f}%"
            pnl_value = float(pnl_val)

        if pnl_value > 0:
            share_line = f"🚀 I just hit {pnl_percent_str} on ${symbol} with @HiperSnipe! 🔥 "
        elif pnl_value == 0:
            share_line = f"⚖️ Broke even at {pnl_percent_str} on ${symbol} with @HiperSnipe! "
        else:
            share_line = f"🛡️ Capital protected! Cut losses at {pnl_percent_str} on ${symbol} with @HiperSnipe! "

        dynamic_text = (
            share_line
            + f"Sniping 100x gems on Solana. Join the hunt: {reflink} | {WEBSITE_URL}"
        )
        sent_msg = await callback.message.answer(
            text=f"<code>{dynamic_text}</code>",
            parse_mode="HTML",
        )
        await callback.answer("Text generated! Tap to copy.")
        asyncio.create_task(delete_later(sent_msg))
    except Exception as e:
        logger.exception("pnl_copy_bio: %s", e)
        await callback.answer("Failed to send text", show_alert=True)


@router.callback_query(F.data == "add_token")
async def add_token_callback(callback: CallbackQuery, state: FSMContext):
    """Handle add token button"""
    await safe_edit_text(callback.message,
        "🔍 Add Token\n\n"
        "Paste a Solana token contract address to add it to your watchlist:\n\n"
        "💡 **Tip:** You can also just paste any token address directly in chat!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="swap_menu")]
        ]),
        callback=callback
    )
    await state.set_state(Form.waiting_for_token_address)


# -----------------------------------------------------------------------------
# Priority mode and auto-buy toggles (from former trading_priority)
# -----------------------------------------------------------------------------

@router.callback_query(F.data == "cycle_priority")
async def cycle_priority_callback(callback: CallbackQuery, state: FSMContext):
    """Cycle execution mode: Standard -> Fast -> Turbo -> Standard.

    Updates DB then refreshes the FULL dashboard (text + keyboard) in-place
    so the execution line in the message body stays in sync with the button.
    """
    try:
        user_id = callback.from_user.id
        ts = await get_user_trade_settings(user_id)
        current = (ts.get("priority_mode") or "Standard").strip()
        next_mode = next_priority_mode(current)

        await update_user_trade_settings(user_id, priority_mode=next_mode)

        data = await state.get_data()
        token_mint = data.get("token_contract") or data.get("token_address")

        await callback.answer(f"Mode set to {next_mode}")

        if token_mint:
            await _refresh_token_view_after_setting_change(callback, state, token_mint)
    except Exception as e:
        logger.exception("cycle_priority error: %s", e)
        await callback.answer("Error changing execution mode", show_alert=True)


@router.callback_query(F.data == "toggle_auto_buy")
async def toggle_auto_buy_callback(callback: CallbackQuery, state: FSMContext):
    """Quick toggle: flip auto-buy on/off from the trading dashboard."""
    logger.info("CALLBACK RECEIVED: %s from %s", callback.data, callback.from_user.id)
    user_id = callback.from_user.id
    try:
        settings = await get_user_settings(user_id)
        current = float((settings or {}).get("auto_buy_amount", 0) or 0)
        if current > 0:
            await update_user_settings(user_id, auto_buy_amount=0.0, auto_buy_saved_amount=current)
            new_val = 0.0
            label = "OFF"
        else:
            saved = float((settings or {}).get("auto_buy_saved_amount", 0) or 0)
            new_val = saved if saved > 0 else 0.1
            await update_user_settings(user_id, auto_buy_amount=new_val)
            label = f"{new_val} SOL"
        await callback.answer(f"Auto Buy: {label}")

        data = await state.get_data()
        token_mint = data.get("token_contract")
        if token_mint:
            await _refresh_token_view_after_setting_change(callback, state, token_mint)
    except Exception as e:
        logger.exception("toggle_auto_buy error: %s", e)
        await callback.answer("Error: Update failed", show_alert=True)
