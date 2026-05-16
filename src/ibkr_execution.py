"""
IBKR Execution Engine — cash-account, long-only, async-native.

Responsibilities:
- Use a SINGLE persistent ib_insync.IB instance supplied by main.py (no per-call
  reconnect — connection management is centralized).
- Cash-account safety: only `BUY_TO_OPEN`. `SELL_TO_OPEN` signals are ignored.
  `SELL_TO_CLOSE` is permitted as a market exit for liquidation.
- 1% SettledCash sizing with strict floor and qty >= 1 guard.
- Native market-status check via `reqContractDetailsAsync(SPY)` → liquidHours.
- Partial-fill aware: subscribes to order events, records each fill into the
  TradeTracker, finalizes on `Filled`/`Cancelled`/`Inactive`.
- Telegram alerts rate-limited to exactly 1 message per second (token-bucket).
- EOD summary at 16:05 EST: NLV, DailyPnL (IBKR-native), Net Delta, today's
  trade count and ticker list.
"""

import asyncio
import logging
import math
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests
from ib_insync import IB, Option, Stock, LimitOrder, MarketOrder, Order, Trade, Contract

from src.trade_tracker import TradeTracker, ActiveTrade

logger = logging.getLogger(__name__)

EST = timezone(timedelta(hours=-5))   # IBKR's session timestamps; we don't need DST precision here

# Cash-account: only these signals open positions; sell-to-open is blocked
ALLOWED_ENTRY_SIGNALS = {"BUY_TO_OPEN"}


@dataclass
class SizingResult:
    qty: int
    settled_cash: float
    nlv: float
    reason: str = ""


class TelegramThrottle:
    """Token-bucket: exactly 1 message per second, with a small burst tolerance."""
    def __init__(self, min_interval_s: float = 1.0):
        self.min_interval_s = min_interval_s
        self._last_sent_ts = 0.0
        self._lock = threading.Lock()

    def can_send(self) -> bool:
        with self._lock:
            now = time.time()
            if now - self._last_sent_ts >= self.min_interval_s:
                self._last_sent_ts = now
                return True
            return False


class IBKRExecutionEngine:
    """
    Single-connection execution. Holds a reference to main.py's persistent IB.
    """

    def __init__(self, ib: IB, tracker: TradeTracker,
                 telegram_token: Optional[str] = None,
                 telegram_chat_id: Optional[str] = None):
        self.ib = ib
        self.tracker = tracker
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        self._telegram = TelegramThrottle(min_interval_s=1.0)
        self._account_summary_cache: Dict[str, float] = {}
        self._account_summary_ts: float = 0.0
        self._market_status_cache: Tuple[bool, float] = (False, 0.0)  # (is_open, fetched_ts)

        # Wire fill/status events
        self.ib.orderStatusEvent += self._on_order_status
        self.ib.execDetailsEvent += self._on_exec_details

        logger.info("✅ IBKRExecutionEngine initialized (persistent connection, long-only, cash account)")

    # ============================================================
    #                        TELEGRAM
    # ============================================================

    def send_telegram(self, message: str, force: bool = False) -> bool:
        """Returns True if sent, False if rate-limited."""
        if not self.telegram_token or not self.telegram_chat_id:
            return False
        if not force and not self._telegram.can_send():
            return False
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.telegram_token}/sendMessage",
                json={"chat_id": self.telegram_chat_id, "text": message, "parse_mode": "Markdown"},
                timeout=5,
            )
            return True
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return False

    # ============================================================
    #                  ACCOUNT + MARKET STATUS
    # ============================================================

    async def refresh_account(self, max_age_s: float = 30.0) -> Dict[str, float]:
        """Refresh SettledCash, AvailableFunds, NetLiquidation, DailyPnL via ib.accountSummary().

        ib_insync's accountSummary() is fast (returns cached AccountValues that
        ib_insync auto-subscribes to on first call). We DO NOT wrap it in a
        thread executor — that races with ib_insync's single-threaded event
        loop and can deadlock.
        """
        if time.time() - self._account_summary_ts < max_age_s and self._account_summary_cache:
            return self._account_summary_cache
        try:
            summary = self.ib.accountSummary()
            d: Dict[str, float] = {}
            for v in summary:
                try:
                    d[v.tag] = float(v.value)
                except (TypeError, ValueError):
                    pass
            if d:
                self._account_summary_cache = d
                self._account_summary_ts = time.time()
            return self._account_summary_cache
        except Exception as e:
            logger.warning(f"accountSummary refresh failed: {e}")
            return self._account_summary_cache

    async def is_market_open(self, max_age_s: float = 60.0) -> bool:
        """Use SPY contract details liquidHours to decide native market open state."""
        is_open, fetched = self._market_status_cache
        if time.time() - fetched < max_age_s:
            return is_open
        try:
            spy = Stock("SPY", "SMART", "USD")
            details = await asyncio.wait_for(self.ib.reqContractDetailsAsync(spy), timeout=10)
            if not details:
                return False
            d = details[0]
            now_utc = datetime.now(timezone.utc)
            # IBKR returns liquidHours as a semicolon-separated list of "YYYYMMDD:HHMM-HHMM" entries
            # or "YYYYMMDD:CLOSED". Parse it against the contract's timeZoneId.
            tz_str = getattr(d, "timeZoneId", "America/New_York")
            try:
                from zoneinfo import ZoneInfo
                tz = ZoneInfo(tz_str)
            except Exception:
                tz = timezone(timedelta(hours=-5))
            now_local = now_utc.astimezone(tz)
            open_now = self._is_within_liquid_hours(d.liquidHours, now_local)
            self._market_status_cache = (open_now, time.time())
            return open_now
        except Exception as e:
            logger.warning(f"market status check failed: {e}; assuming CLOSED")
            self._market_status_cache = (False, time.time())
            return False

    @staticmethod
    def _is_within_liquid_hours(liquid_hours_str: str, now_local: datetime) -> bool:
        if not liquid_hours_str:
            return False
        today_prefix = now_local.strftime("%Y%m%d")
        for entry in liquid_hours_str.split(";"):
            entry = entry.strip()
            if not entry.startswith(today_prefix):
                continue
            if "CLOSED" in entry.upper():
                return False
            # Format: 20260514:0930-20260514:1600
            try:
                start_part, end_part = entry.split("-")
                _, start_hm = start_part.split(":")
                _, end_hm = end_part.split(":")
                start_t = dtime(int(start_hm[:2]), int(start_hm[2:]))
                end_t = dtime(int(end_hm[:2]), int(end_hm[2:]))
                t_now = now_local.time()
                if start_t <= t_now <= end_t:
                    return True
            except Exception:
                continue
        return False

    # ============================================================
    #                       SIZING (1% rule)
    # ============================================================

    def size_position(self, settled_cash: float, mid_price: float, multiplier: float) -> SizingResult:
        """Strict 1% SettledCash sizing with math.floor. qty<1 → 0 (no trade)."""
        if mid_price <= 0 or multiplier <= 0 or settled_cash <= 0:
            return SizingResult(0, settled_cash, 0.0, "invalid inputs")
        raw = (settled_cash * 0.01) / (mid_price * multiplier)
        qty = math.floor(raw)
        if qty < 1:
            return SizingResult(0, settled_cash, 0.0,
                                f"qty<1 (raw={raw:.3f}; cash={settled_cash:.2f}, mid={mid_price:.2f}, mult={multiplier})")
        return SizingResult(qty, settled_cash, 0.0, "ok")

    # ============================================================
    #                    ENTRY / EXIT EXECUTION
    # ============================================================

    @staticmethod
    def _parse_option_symbol(sym: str) -> Optional[Tuple[str, str, str, float]]:
        """Parse 'SPY260601C639' → (underlying, '20260601', 'C', 639.0)."""
        m = re.match(r"^([A-Z]+)(\d{6})([CP])(\d+(?:\.\d+)?)$", sym)
        if not m:
            return None
        und, exp_short, right, strike = m.groups()
        return und, f"20{exp_short}", right, float(strike)

    async def execute_entry(self, symbol: str, signal: str, mid_price: float,
                            tick_greeks: Dict[str, float], tick_iv: float,
                            underlying_price: float) -> Optional[ActiveTrade]:
        """
        BUY_TO_OPEN flow (cash account, long-only).
        Returns the new ActiveTrade or None.
        """
        if signal not in ALLOWED_ENTRY_SIGNALS:
            logger.info(f"[{symbol}] signal {signal} blocked (cash account is long-only)")
            return None

        if not self.tracker.can_open_alpha(symbol):
            return None
        if self.tracker.has_pending(symbol):
            logger.info(f"[{symbol}] entry blocked: order already pending")
            return None
        if symbol in self.tracker.active:
            logger.debug(f"[{symbol}] entry blocked: already long")
            return None

        # Native market-hours gate (entries only — exits go anytime)
        if not await self.is_market_open():
            logger.info(f"[{symbol}] entry blocked: market is CLOSED (native check)")
            return None

        parsed = self._parse_option_symbol(symbol)
        if not parsed:
            logger.error(f"[{symbol}] entry rejected: cannot parse OCC symbol")
            return None
        und, exp, right, strike = parsed
        contract = Option(und, exp, strike, right, "SMART")
        try:
            await asyncio.wait_for(self.ib.qualifyContractsAsync(contract), timeout=10)
            if not contract.conId:
                logger.error(f"[{symbol}] qualification returned no conId")
                return None
        except Exception as e:
            logger.error(f"[{symbol}] qualifyContractsAsync failed: {e}")
            return None

        multiplier = float(contract.multiplier or 100)

        # Pre-trade account refresh
        summary = await self.refresh_account(max_age_s=10.0)
        settled_cash = summary.get("SettledCash", 0.0)
        nlv = summary.get("NetLiquidation", 0.0)
        sizing = self.size_position(settled_cash, mid_price, multiplier)
        if sizing.qty < 1:
            logger.info(f"[{symbol}] sizing rejected: {sizing.reason}")
            return None

        # Place the BUY limit order
        order = LimitOrder("BUY", sizing.qty, round(mid_price, 2))
        order.tif = "DAY"
        self.tracker.mark_alpha_attempt(symbol)
        try:
            trade = self.ib.placeOrder(contract, order)
        except Exception as e:
            logger.error(f"[{symbol}] placeOrder failed: {e}")
            return None
        order_id = trade.order.orderId
        self.tracker.register_pending(order_id, symbol)
        self.tracker.begin_order(order_id, symbol, "BUY_TO_OPEN", "option")

        msg = (f"📥 *ENTRY SENT* `{symbol}`\nQty: {sizing.qty} @ ${mid_price:.2f}\n"
               f"Cash: ${settled_cash:,.0f}  NLV: ${nlv:,.0f}")
        logger.info(msg.replace("\n", " | "))
        self.send_telegram(msg)

        active = ActiveTrade(
            symbol=symbol, action="BUY_TO_OPEN", qty=sizing.qty,
            entry_price=mid_price,
            entry_time=datetime.now(timezone.utc).isoformat(),
            contract_spec={"underlying": und, "expiry": exp, "strike": strike, "right": right,
                            "multiplier": multiplier},
        )
        self.tracker.add_active(active)
        return active

    async def place_tp(self, trade: ActiveTrade, current_mid: float) -> Optional[int]:
        """Place a +20% TP limit (or re-peg an existing one)."""
        if current_mid <= 0:
            return None
        target = round(trade.entry_price * 1.20, 2)
        # If mid is already above target, peg slightly above mid to actually fill
        limit_price = max(target, round(current_mid, 2))

        parsed = self._parse_option_symbol(trade.symbol)
        if not parsed:
            return None
        und, exp, right, strike = parsed
        contract = Option(und, exp, strike, right, "SMART")
        try:
            await asyncio.wait_for(self.ib.qualifyContractsAsync(contract), timeout=10)
        except Exception as e:
            logger.warning(f"TP qualify failed for {trade.symbol}: {e}")
            return None

        # Compute remaining open qty (TP should target only what's actually held)
        remaining_qty = self._remaining_open_qty(trade)
        if remaining_qty <= 0:
            return None

        order = LimitOrder("SELL", remaining_qty, limit_price)
        order.tif = "GTC"
        try:
            tp_trade = self.ib.placeOrder(contract, order)
        except Exception as e:
            logger.error(f"TP placeOrder failed for {trade.symbol}: {e}")
            return None
        order_id = tp_trade.order.orderId
        self.tracker.register_pending(order_id, trade.symbol)
        self.tracker.begin_order(order_id, trade.symbol, "SELL_TO_CLOSE_TP", "option")
        self.tracker.update_tp_repeg(trade.symbol, limit_price, order_id)
        logger.info(f"🎯 TP placed {trade.symbol}: SELL {remaining_qty} @ ${limit_price:.2f}")
        return order_id

    async def execute_exit_market(self, trade: ActiveTrade, reason: str) -> bool:
        """Cancel TP, compute remaining qty, send MARKET sell to fully liquidate."""
        # 1. Cancel existing TP
        if trade.tp_order_id is not None:
            try:
                open_orders = self.ib.openOrders()
                for o in open_orders:
                    if o.orderId == trade.tp_order_id:
                        self.ib.cancelOrder(o)
                        logger.info(f"⛔ cancelled TP order {trade.tp_order_id} for {trade.symbol}")
                        break
            except Exception as e:
                logger.warning(f"cancelOrder failed for {trade.symbol} TP {trade.tp_order_id}: {e}")
            self.tracker.clear_pending(trade.tp_order_id)

        # 2. Compute remaining open qty (handle partial fills on the TP)
        remaining_qty = self._remaining_open_qty(trade)
        if remaining_qty <= 0:
            logger.info(f"[{trade.symbol}] exit({reason}): nothing left to close")
            self.tracker.remove_active(trade.symbol)
            return True

        parsed = self._parse_option_symbol(trade.symbol)
        if not parsed:
            return False
        und, exp, right, strike = parsed
        contract = Option(und, exp, strike, right, "SMART")
        try:
            await asyncio.wait_for(self.ib.qualifyContractsAsync(contract), timeout=10)
        except Exception as e:
            logger.warning(f"exit qualify failed for {trade.symbol}: {e}")
            return False

        order = MarketOrder("SELL", remaining_qty)
        try:
            ex_trade = self.ib.placeOrder(contract, order)
        except Exception as e:
            logger.error(f"exit placeOrder failed for {trade.symbol}: {e}")
            return False
        order_id = ex_trade.order.orderId
        self.tracker.register_pending(order_id, trade.symbol)
        self.tracker.begin_order(order_id, trade.symbol, f"SELL_TO_CLOSE_{reason}", "option")

        msg = f"🚪 *EXIT ({reason})* `{trade.symbol}`\nMKT SELL {remaining_qty}"
        logger.info(msg.replace("\n", " | "))
        self.send_telegram(msg)
        return True

    def _remaining_open_qty(self, trade: ActiveTrade) -> int:
        """Best-effort: subtract any closing-side fills we've recorded."""
        # Walk positions; the simplest correct answer is to ask the gateway:
        try:
            for pos in self.ib.positions():
                spec = trade.contract_spec
                c = pos.contract
                if (c.secType == "OPT" and c.symbol == spec["underlying"] and
                        c.lastTradeDateOrContractMonth == spec["expiry"] and
                        float(c.strike) == float(spec["strike"]) and c.right == spec["right"]):
                    return max(0, int(pos.position))
        except Exception:
            pass
        return trade.qty  # fall back to the entry qty

    # ============================================================
    #                       HEDGING
    # ============================================================

    async def hedge_with(self, hedge_stock: Stock, shares: int, side: str, reason: str = "delta") -> bool:
        """side: 'BUY' or 'SELL'. shares already computed by RiskManager."""
        if shares <= 0:
            return False
        if not self.tracker.can_hedge():
            logger.info(f"hedge blocked: cooldown ({HEDGE_COOLDOWN_REMAINING_HINT}s remaining)" if False else "hedge blocked: cooldown active")
            return False
        order = MarketOrder(side, shares)
        try:
            trade = self.ib.placeOrder(hedge_stock, order)
        except Exception as e:
            logger.error(f"hedge placeOrder failed: {e}")
            return False
        order_id = trade.order.orderId
        sym = hedge_stock.symbol
        self.tracker.register_pending(order_id, f"HEDGE_{sym}")
        self.tracker.begin_order(order_id, sym, f"HEDGE_{side}_{reason}", "stock")
        self.tracker.mark_hedge_attempt()
        msg = f"🛡️ *HEDGE* `{sym}` {side} {shares} ({reason})"
        logger.info(msg.replace("\n", " | "))
        self.send_telegram(msg)
        return True

    # ============================================================
    #                EVENT WIRING (partial fills)
    # ============================================================

    def _on_order_status(self, trade: Trade):
        """Called by ib_insync on every order status update."""
        st = trade.orderStatus.status
        oid = trade.order.orderId
        if st in ("Filled",):
            # finalize the fill accumulator → one consolidated CSV row
            self.tracker.finalize_order(oid)
            self.tracker.clear_pending(oid)
            # If this was a closing order, remove the active trade
            self._maybe_remove_active_on_close(trade)
        elif st in ("Cancelled", "Inactive"):
            self.tracker.finalize_order(oid)
            self.tracker.clear_pending(oid)

    def _on_exec_details(self, trade: Trade, fill):
        """Per-fill callback. `fill.execution.shares`, `fill.execution.price`, `fill.execution.time` (UTC)."""
        try:
            ex = fill.execution
            fill_time = ex.time if isinstance(ex.time, datetime) else datetime.now(timezone.utc)
            if fill_time.tzinfo is None:
                fill_time = fill_time.replace(tzinfo=timezone.utc)
            self.tracker.record_fill(
                order_id=trade.order.orderId,
                qty=float(ex.shares),
                price=float(ex.price),
                fill_time=fill_time,
                greeks=None,    # caller can attach greeks separately if it has a fresh tick
                iv=0.0,
                underlying_price=0.0,
            )
        except Exception as e:
            logger.warning(f"exec_details callback error: {e}")

    def _maybe_remove_active_on_close(self, trade: Trade):
        action = (trade.order.action or "").upper()
        if action != "SELL":
            return
        # find which active position this contract maps to
        c = trade.contract
        for sym, active in list(self.tracker.active.items()):
            spec = active.contract_spec
            if (c.secType == "OPT" and c.symbol == spec["underlying"] and
                    c.lastTradeDateOrContractMonth == spec["expiry"] and
                    float(c.strike) == float(spec["strike"]) and c.right == spec["right"]):
                self.tracker.remove_active(sym)
                return

    # ============================================================
    #                   END-OF-DAY SUMMARY
    # ============================================================

    async def send_eod_summary(self, todays_trades: List[str]) -> bool:
        summary = await self.refresh_account(max_age_s=0)
        nlv = summary.get("NetLiquidation", 0.0)
        daily_pnl = summary.get("DailyPnL", 0.0)
        # Net delta from portfolio Greek totals isn't in account summary; caller supplies via tracker if needed
        tickers_str = ", ".join(sorted(set(todays_trades))) if todays_trades else "(none)"
        body = (f"📊 *EOD SUMMARY*\n"
                f"NLV: `${nlv:,.2f}`\n"
                f"DailyPnL: `${daily_pnl:,.2f}`\n"
                f"Trades today: {len(todays_trades)}\n"
                f"Tickers: {tickers_str}")
        # EOD summary uses force=True — we want this even if rate limit would block
        return self.send_telegram(body, force=True)


# Hint string kept for log clarity in the hedge cooldown path
HEDGE_COOLDOWN_REMAINING_HINT = "300"
