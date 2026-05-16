"""
Quant Trading Engine Main Process (Background Daemon).

Cash-account, long-only options vol-arbitrage with:
- Native market-hours gate
- 1% SettledCash position sizing
- TP/SL/Fundamental exit on every tick
- Dollar-Delta hedging (SPY → QQQ → IWM → fallback)
- Gamma circuit breaker (5% of NLV)
- 09:30 EST market-open priority routine
- 16:05 EST end-of-day Telegram summary
- 15-min TP limit re-peg
- Disconnect → sys.exit(1) so Docker's restart policy reboots the container
  on IBKR's daily 11:59 PM restart.
"""

import asyncio
import json
import logging
import logging.handlers
import os
import signal
import socket
import sys
import time
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Dict, List, Optional, Set

# --- 1. BOOTSTRAP PATHS (Must be at the very top) ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from ib_insync import IB, Stock, util

# Patch asyncio for nested loops (Streamlit, etc.)
util.patchAsyncio()

# Local Imports
from src.strategy import DynamicVolatilityEngine, VolatilityArbitrage
from src.ibkr_execution import IBKRExecutionEngine
from src.ibkr_streamer import run_ibkr_streamer_background
from src.portfolio import Portfolio
from src.streamer import OptionTick
from src.pricer import GreeksResult
from src.trade_tracker import TradeTracker
from src.risk_manager import RiskManager, GAMMA_LIMIT_PCT

# ============================================================
#                      LOGGING SETUP
# ============================================================
DATA_DIR = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

LOCAL_TZ_OFFSET_HOURS = int(os.getenv("LOCAL_TZ_OFFSET_HOURS", "8"))
LOCAL_TZ = timezone(timedelta(hours=LOCAL_TZ_OFFSET_HOURS))


class LocalTZFormatter(logging.Formatter):
    """Print log timestamps in the user's local timezone (UTC+8 by default)."""
    def converter(self, ts):  # type: ignore[override]
        # datetime.fromtimestamp(ts, LOCAL_TZ) returns aware, but logging expects struct_time
        return datetime.fromtimestamp(ts, LOCAL_TZ).timetuple()

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, LOCAL_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S %z")


def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fmt = LocalTZFormatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)

    # Rotating file handler — 10 MB per file, keep 5 backups
    fh = logging.handlers.RotatingFileHandler(
        os.path.join(DATA_DIR, "app.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)


setup_logging()
logger = logging.getLogger(__name__)

# ============================================================
#                     SHUTDOWN HANDLING
# ============================================================
IB_INSTANCES: List[IB] = []


def register_ib(ib: IB):
    if ib not in IB_INSTANCES:
        IB_INSTANCES.append(ib)


def _shutdown(signum, _frame):
    logger.info(f"Signal {signum} received — disconnecting {len(IB_INSTANCES)} IBKR session(s) cleanly")
    for ib in IB_INSTANCES:
        try:
            if ib.isConnected():
                ib.disconnect()
        except Exception:
            pass
    sys.exit(0)


signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT, _shutdown)


# ============================================================
#                    GATEWAY READINESS PROBE
# ============================================================
async def wait_for_gateway(host: str, port: int, timeout: int = 120) -> bool:
    logger.info(f"⏳ Waiting for IB Gateway at {host}:{port} to open...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                logger.info(f"✅ IB Gateway port {port} is now OPEN.")
                await asyncio.sleep(5)  # let the gateway finish internal init
                return True
        except (socket.timeout, ConnectionRefusedError):
            await asyncio.sleep(3)
    return False


# ============================================================
#                      MAIN ORCHESTRATION
# ============================================================
async def main():
    logger.info("🚀 Starting Master Trading Brain...")

    # Brain shares ib-gateway's network namespace → connect as 127.0.0.1
    gw_host = os.getenv('GW_HOST', '127.0.0.1')
    gw_port = int(os.getenv('GW_PORT', '4002'))

    if not await wait_for_gateway(gw_host, gw_port):
        logger.error("❌ IB Gateway failed to start in time. Brain exiting.")
        return

    # 1. Portfolio + tracker
    portfolio = Portfolio()
    tracker = TradeTracker(DATA_DIR, local_tz_offset_hours=LOCAL_TZ_OFFSET_HOURS)

    # 2. Strategy (one-shot RV fetch via temporary clientId=400 connection)
    rv_engine = DynamicVolatilityEngine(host=gw_host, port=gw_port, on_connect=register_ib)
    watchlist = ["SPY", "QQQ", "NVDA", "TSLA"]
    strategy = VolatilityArbitrage(rv_engine=rv_engine, symbols=watchlist)
    logger.info("📈 Initializing Strategy (Calculating RV)...")
    await strategy.initialize_async()
    logger.info("✅ Strategy Ready.")

    # 3. Persistent executor connection (clientId=402) — single IB shared by
    #    execution, risk management and the disconnect-watchdog
    exec_ib = IB()
    try:
        await exec_ib.connectAsync(gw_host, gw_port, clientId=402, timeout=20)
    except Exception as e:
        logger.error(f"❌ Executor IB connect failed: {e}")
        return
    register_ib(exec_ib)
    logger.info(f"🔌 Executor connected (clientId=402, accounts={exec_ib.managedAccounts()})")

    # Watchdog: on disconnect, exit so Docker restarts us. IBKR's daily restart
    # at 11:59 PM is the main reason we exit; transient drops within a session
    # are rare and the cleaner answer is a full container reboot anyway.
    def _on_exec_disconnected():
        logger.warning("⚡ executor IB disconnectedEvent — exiting for Docker restart")
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)
    exec_ib.disconnectedEvent += _on_exec_disconnected

    telegram_token = os.getenv("TELEGRAM_TOKEN")
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
    executor = IBKRExecutionEngine(
        ib=exec_ib, tracker=tracker,
        telegram_token=telegram_token, telegram_chat_id=telegram_chat_id,
    )
    risk = RiskManager(exec_ib)

    # 4. Streamer task (clientId=401) — long-lived background task with backoff retry
    update_queue: asyncio.Queue = asyncio.Queue()
    CONTROL_FILE = os.path.join(DATA_DIR, 'control.json')

    async def streamer_manager():
        delay = 1
        while True:
            try:
                logger.info("📡 Starting Market Data Streamer...")
                await run_ibkr_streamer_background(
                    watchlist, update_queue,
                    host=gw_host, port=gw_port,
                    client_id=401,
                    on_connect=register_ib,
                )
                logger.warning("Streamer returned without exception — restarting after 5s")
                await asyncio.sleep(5)
                delay = 1
            except Exception as e:
                logger.error(f"💥 Streamer disconnected: {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    asyncio.create_task(streamer_manager())

    # 5. Background periodic tasks
    asyncio.create_task(market_open_routine(executor, risk, portfolio, tracker))
    asyncio.create_task(eod_summary_routine(executor, tracker))
    asyncio.create_task(repeg_loop(executor, portfolio, tracker))

    # 6. Core tick processing loop
    logger.info("🧠 Brain is Active. Monitoring Market...")
    todays_ticker_set: Set[str] = set()   # for EOD summary
    last_day_key = _today_local_key()
    gamma_breach_active = False

    while True:
        try:
            # Roll over EOD ticker set at local midnight
            d_key = _today_local_key()
            if d_key != last_day_key:
                todays_ticker_set.clear()
                last_day_key = d_key

            # Control file
            is_running = True
            if os.path.exists(CONTROL_FILE):
                try:
                    with open(CONTROL_FILE, 'r') as f:
                        c = json.load(f)
                        if c.get("status") == "stopped":
                            is_running = False
                except Exception:
                    pass

            # Pull a tick
            try:
                raw_data = await asyncio.wait_for(update_queue.get(), timeout=2.0)
            except asyncio.TimeoutError:
                raw_data = None

            tick: Optional[OptionTick] = None
            if is_running and raw_data and raw_data.get("type") == "tick_update":
                tick = _build_tick(raw_data["data"])
                if tick is not None:
                    portfolio.update_position(tick)
                    await _process_tick(
                        tick, strategy, executor, risk, portfolio, tracker,
                        todays_ticker_set,
                    )

            # Gamma circuit breaker (every loop iteration, not only on ticks)
            metrics = portfolio.calculate_portfolio_metrics()
            await _check_gamma_breaker(
                metrics, executor, risk, portfolio, tracker,
                breach_state_ref={"active": gamma_breach_active},
            )

            # Dashboard sync
            await _write_dashboard_metrics(
                metrics, executor, risk, portfolio, tracker,
            )

        except Exception as e:
            logger.error(f"⚠️ Main Loop Error: {e}", exc_info=True)
            await asyncio.sleep(1)


# ============================================================
#                       HELPERS
# ============================================================
def _today_local_key() -> str:
    return datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")


def _build_tick(data: Dict) -> Optional[OptionTick]:
    try:
        tick = OptionTick(
            symbol=data["symbol"],
            strike=data["strike"],
            expiration=data["expiration"],
            option_type=data["option_type"],
            bid=data["bid"], ask=data["ask"], mid=data["mid"],
            bid_size=data.get("bid_size", 0),
            ask_size=data.get("ask_size", 0),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            underlying_price=data["underlying_price"],
            implied_volatility=data["implied_volatility"],
        )
        g = data.get("greeks")
        if g:
            tick.greeks = GreeksResult(
                price=g.get("price", tick.mid),
                delta=g.get("delta", 0.0),
                gamma=g.get("gamma", 0.0),
                theta=g.get("theta", 0.0),
                vega=g.get("vega", 0.0),
                rho=g.get("rho", 0.0),
            )
        return tick
    except Exception as e:
        logger.warning(f"_build_tick: failed to parse tick payload: {e}")
        return None


# Per-symbol throttle for the IV-vs-RV diagnostic log (1 line every 60s)
_DIAG_LAST_LOG: Dict[str, float] = {}


async def _process_tick(tick: OptionTick, strategy: VolatilityArbitrage,
                         executor: IBKRExecutionEngine, risk: RiskManager,
                         portfolio: Portfolio, tracker: TradeTracker,
                         todays_ticker_set: Set[str]):
    """Per-tick: priority is EXITS first, then ENTRIES."""

    # ---- DIAGNOSTIC: throttled IV vs RV gap log ----
    underlying = tick.symbol.rstrip("0123456789CP")
    rv = strategy.target_rv_map.get(underlying, 0.0)
    last_diag = _DIAG_LAST_LOG.get(tick.symbol, 0.0)
    if time.time() - last_diag > 60 and tick.implied_volatility > 0:
        gap = rv - tick.implied_volatility
        will_fire = "BUY" if gap > strategy.threshold else "HOLD"
        logger.info(
            f"📐 [{tick.symbol}] IV={tick.implied_volatility:.2%} RV={rv:.2%} "
            f"gap={gap:+.2%} (need >+{strategy.threshold:.2%} for BUY) → {will_fire}"
        )
        _DIAG_LAST_LOG[tick.symbol] = time.time()

    # ---- EXITS: fundamental, TP, SL ----
    active = tracker.get_active(tick.symbol)
    if active is not None:
        # 1. Fundamental exit (IV converged to RV)
        if strategy.is_fundamental_exit(tick):
            ok = await executor.execute_exit_market(active, reason="FUND")
            if ok:
                todays_ticker_set.add(tick.symbol)
            return

        # 2. Stop loss: mid <= entry * 0.75 (-25%)
        if tick.mid > 0 and tick.mid <= active.entry_price * 0.75:
            ok = await executor.execute_exit_market(active, reason="STOP")
            if ok:
                todays_ticker_set.add(tick.symbol)
            return

        # 3. Take profit: only place a TP if we don't already have one
        if active.tp_order_id is None and tick.mid > 0:
            await executor.place_tp(active, current_mid=tick.mid)

    # ---- ENTRIES (long-only, market-open gated inside execute_entry) ----
    sig = strategy.generate_signal(tick)
    if sig == "BUY_TO_OPEN":
        greeks_dict = {}
        if tick.greeks:
            greeks_dict = {
                "delta": tick.greeks.delta, "gamma": tick.greeks.gamma,
                "vega": tick.greeks.vega, "theta": tick.greeks.theta,
            }
        new_trade = await executor.execute_entry(
            symbol=tick.symbol, signal=sig, mid_price=tick.mid,
            tick_greeks=greeks_dict, tick_iv=tick.implied_volatility,
            underlying_price=tick.underlying_price,
        )
        if new_trade is not None:
            todays_ticker_set.add(tick.symbol)


async def _check_gamma_breaker(metrics, executor: IBKRExecutionEngine,
                                 risk: RiskManager, portfolio: Portfolio,
                                 tracker: TradeTracker, breach_state_ref: dict):
    """Sum dollar gamma across the portfolio; alert on breach (1/sec rate-limited)."""
    summary = await executor.refresh_account(max_age_s=10.0)
    nlv = summary.get("NetLiquidation", 0.0)
    if nlv <= 0:
        return

    # Build positions list — qty/multiplier come from tracker (authoritative),
    # delta/gamma/underlying_price come from the latest streaming tick.
    positions = []
    for t in tracker.list_active():
        spec = t.contract_spec
        tk = portfolio.positions.get(t.symbol)
        delta = gamma = 0.0
        u_price = 0.0
        if tk and tk.greeks:
            delta = tk.greeks.delta
            gamma = tk.greeks.gamma
            u_price = tk.underlying_price or 0.0
        positions.append({
            "delta": delta, "gamma": gamma,
            "qty": t.qty, "multiplier": spec.get("multiplier", 100.0),
            "underlying_price": u_price,
        })

    d_gamma = risk.dollar_gamma(positions)
    util_pct, breached = risk.gamma_utilization(d_gamma, nlv)
    if breached:
        msg = (f"⚠️ *GAMMA BREACH* — ${d_gamma:,.0f} ({util_pct:.1f}% of "
               f"{GAMMA_LIMIT_PCT*100:.0f}% NLV limit). New entries halted.")
        # Rate-limited Telegram (1 per second token bucket)
        sent = executor.send_telegram(msg)
        if sent:
            logger.warning(msg.replace("\n", " | "))
        breach_state_ref["active"] = True
    else:
        if breach_state_ref.get("active"):
            logger.info(f"✅ Gamma utilization back within limit ({util_pct:.1f}%).")
        breach_state_ref["active"] = False


async def _write_dashboard_metrics(metrics, executor: IBKRExecutionEngine,
                                     risk: RiskManager, portfolio: Portfolio,
                                     tracker: TradeTracker):
    """Update data/metrics.json + greeks.csv for the dashboard."""
    summary = await executor.refresh_account(max_age_s=15.0)
    nlv = summary.get("NetLiquidation", 0.0)
    daily_pnl = summary.get("DailyPnL", 0.0)
    settled = summary.get("SettledCash", 0.0)

    # Dollar gamma from tracker active trades + live portfolio
    positions_dg = []
    for t in tracker.list_active():
        sym = t.symbol
        tk = portfolio.positions.get(sym)
        d = g = 0.0
        u = 0.0
        if tk and tk.greeks:
            d, g = tk.greeks.delta, tk.greeks.gamma
            u = tk.underlying_price or 0.0
        positions_dg.append({
            "delta": d, "gamma": g, "qty": t.qty,
            "multiplier": t.contract_spec.get("multiplier", 100.0),
            "underlying_price": u,
        })
    d_gamma = risk.dollar_gamma(positions_dg)
    d_delta = risk.dollar_delta(positions_dg)
    util_pct, breached = risk.gamma_utilization(d_gamma, nlv)

    out = metrics.to_dict()
    out.update({
        "last_update": datetime.now().isoformat(),
        "settled_cash": settled,
        "net_liquidation": nlv,
        "daily_pnl": daily_pnl,
        "dollar_delta": d_delta,
        "dollar_gamma": d_gamma,
        "gamma_limit_pct": GAMMA_LIMIT_PCT,
        "gamma_utilization_pct": util_pct,
        "gamma_breached": breached,
        "active_trades": len(tracker.list_active()),
    })
    try:
        with open(os.path.join(DATA_DIR, 'metrics.json'), 'w') as f:
            json.dump(out, f)
    except Exception as e:
        logger.warning(f"metrics.json write failed: {e}")

    greeks_df = portfolio.get_greeks_dataframe()
    if not greeks_df.empty:
        try:
            greeks_df.to_csv(os.path.join(DATA_DIR, 'greeks.csv'), index=False)
        except Exception as e:
            logger.warning(f"greeks.csv write failed: {e}")


# ============================================================
#                  PERIODIC BACKGROUND TASKS
# ============================================================
async def market_open_routine(executor: IBKRExecutionEngine, risk: RiskManager,
                                portfolio: Portfolio, tracker: TradeTracker):
    """
    At 09:30 EST exactly, force a portfolio-level delta hedge BEFORE any
    new alpha entries are processed (to handle overnight gap risk).
    """
    last_fired = ""
    while True:
        try:
            now_est = datetime.now(timezone(timedelta(hours=-5)))
            # Fire once per day at 09:30-09:35 window
            today_key = now_est.strftime("%Y-%m-%d")
            if (now_est.weekday() < 5 and  # Mon–Fri
                    dtime(9, 30) <= now_est.time() <= dtime(9, 35) and
                    last_fired != today_key):
                last_fired = today_key
                logger.info("🌅 09:30 EST market-open routine: forcing delta hedge check")
                await _hedge_now(executor, risk, portfolio, tracker)
        except Exception as e:
            logger.warning(f"market_open_routine error: {e}")
        await asyncio.sleep(30)


async def _hedge_now(executor: IBKRExecutionEngine, risk: RiskManager,
                       portfolio: Portfolio, tracker: TradeTracker):
    if not tracker.can_hedge():
        logger.info("hedge_now: cooldown active, skipping")
        return
    # Build positions list
    positions = []
    largest = None
    largest_notional = 0.0
    for t in tracker.list_active():
        tk = portfolio.positions.get(t.symbol)
        d = g = 0.0
        u = 0.0
        if tk and tk.greeks:
            d, g = tk.greeks.delta, tk.greeks.gamma
            u = tk.underlying_price or 0.0
        mult = t.contract_spec.get("multiplier", 100.0)
        notional = abs(t.qty * mult * u)
        if notional > largest_notional:
            largest_notional = notional
            largest = t.contract_spec.get("underlying")
        positions.append({
            "delta": d, "gamma": g, "qty": t.qty,
            "multiplier": mult, "underlying_price": u,
        })
    d_delta = risk.dollar_delta(positions)
    if abs(d_delta) < 50:
        logger.info(f"hedge_now: dollar_delta={d_delta:.2f} within +/-50 band, no hedge")
        return
    hedge = await risk.select_hedge_instrument(fallback_symbol=largest)
    if hedge is None:
        logger.warning("hedge_now: no hedge instrument available")
        return
    shares = await risk.compute_hedge_qty(d_delta, hedge)
    if shares <= 0:
        return
    side = "SELL" if d_delta > 0 else "BUY"   # opposite direction to neutralize
    await executor.hedge_with(hedge, shares, side, reason="delta")


async def eod_summary_routine(executor: IBKRExecutionEngine, tracker: TradeTracker):
    """At 16:05 EST, send the EOD Telegram summary."""
    last_fired = ""
    while True:
        try:
            now_est = datetime.now(timezone(timedelta(hours=-5)))
            today_key = now_est.strftime("%Y-%m-%d")
            if (now_est.weekday() < 5 and
                    dtime(16, 5) <= now_est.time() <= dtime(16, 10) and
                    last_fired != today_key):
                last_fired = today_key
                # Gather today's traded tickers from the monthly CSV
                tickers = _scan_todays_tickers(now_est)
                await executor.send_eod_summary(tickers)
                logger.info(f"📊 EOD summary dispatched ({len(tickers)} symbols)")
        except Exception as e:
            logger.warning(f"eod_summary_routine error: {e}")
        await asyncio.sleep(30)


def _scan_todays_tickers(now_est: datetime) -> List[str]:
    """Read this month's history CSV, return today's distinct symbols."""
    import csv as _csv
    path = os.path.join(DATA_DIR, f"trade_history_{now_est.strftime('%Y_%m')}.csv")
    if not os.path.exists(path):
        return []
    today_prefix = now_est.astimezone(LOCAL_TZ).strftime("%Y-%m-%d")
    out: List[str] = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                if row.get("timestamp_local", "").startswith(today_prefix):
                    out.append(row.get("symbol", ""))
    except Exception as e:
        logger.warning(f"_scan_todays_tickers: {e}")
    return [s for s in out if s]


async def repeg_loop(executor: IBKRExecutionEngine, portfolio: Portfolio, tracker: TradeTracker):
    """Every minute, check active trades' TP orders and re-peg if >15 min old."""
    while True:
        try:
            for active in tracker.list_active():
                if not tracker.needs_repeg(active.symbol):
                    continue
                tk = portfolio.positions.get(active.symbol)
                if not tk or tk.mid <= 0:
                    continue
                # Cancel the old TP, place a new one at current mid (or +20%, whichever higher)
                if active.tp_order_id is not None:
                    try:
                        for o in executor.ib.openOrders():
                            if o.orderId == active.tp_order_id:
                                executor.ib.cancelOrder(o)
                                logger.info(f"🔄 re-peg: cancelled stale TP {active.tp_order_id} for {active.symbol}")
                                break
                    except Exception as e:
                        logger.warning(f"re-peg cancel failed for {active.symbol}: {e}")
                    tracker.clear_pending(active.tp_order_id)
                # Reset the timer regardless of whether the new order succeeds
                active.tp_order_id = None
                await executor.place_tp(active, current_mid=tk.mid)
        except Exception as e:
            logger.warning(f"repeg_loop error: {e}")
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
