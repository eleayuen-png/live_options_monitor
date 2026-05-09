import logging
import requests
import re
import time
import asyncio
import random
from ib_insync import IB, Option, Stock, LimitOrder, MarketOrder

logger = logging.getLogger(__name__)

class IBKRExecutionEngine:
    """
    Handles live order routing to IBKR TWS.
    Upgraded with Isolated Connections (to prevent deadlocks) 
    and Order Cooldowns (to prevent API flooding).
    """
    def __init__(self, host='host.docker.internal', port=7497, client_id=None, telegram_token=None, telegram_chat_id=None):
        self.host = host
        self.port = port
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        self.is_simulation = False
        
        # Track the last time we placed an order for a symbol to prevent flooding
        self.last_order_time = {} 
        self.cooldown_seconds = 60.0 # Wait 60 seconds before sending another order for the same contract
        
        logger.info("✅ IBKR Execution Engine Initialized (Using Isolated Connections)")

    def send_telegram_alert(self, message: str):
        """Sends a push notification via Telegram."""
        if not self.telegram_token or not self.telegram_chat_id:
            return
            
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            "chat_id": self.telegram_chat_id,
            "text": f"🤖 *Quant OMS Alert*\n\n{message}",
            "parse_mode": "Markdown"
        }
        try:
            requests.post(url, json=payload)
        except Exception as e:
            logger.error(f"Telegram notification failed: {e}")

    def execute_option_signal(self, symbol: str, signal: str, qty: int = 1, limit_price: float = None):
        if signal == "HOLD":
            return
            
        if limit_price is None or limit_price <= 0:
            return
            
        # --- ANTI-FLOOD COOLDOWN CHECK ---
        now = time.time()
        if symbol in self.last_order_time:
            if (now - self.last_order_time[symbol]) < self.cooldown_seconds:
                # Still in cooldown, ignore the signal
                return
        
        logger.info(f"🚨 SIGNAL: {signal} {qty} contract(s) of {symbol} @ Limit ${limit_price:.2f}")
        
        # --- ISOLATED CONNECTION TO PREVENT DEADLOCKS ---
        isolated_ib = IB()
        client_id = random.randint(10000, 19999) 
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Connect temporarily just to route the order
            isolated_ib.connect(self.host, self.port, clientId=client_id, timeout=5)
            
            match = re.match(r"^([A-Z]+)(\d{6})([CP])(\d+(?:\.\d+)?)$", symbol)
            if not match:
                logger.error(f"Could not parse symbol {symbol} for IBKR")
                return
                
            und, exp_short, right, strike = match.groups()
            exp_full = f"20{exp_short}"
            
            contract = Option(und, exp_full, float(strike), right, 'SMART')
            
            # This is where it used to deadlock! Now it is safe.
            isolated_ib.qualifyContracts(contract)

            # --- 1. NEW: CONTRACT VALIDATION CHECK ---
            if not contract.conId:
                logger.error(f"❌ Cannot execute {symbol}: IBKR says this contract does not exist (Error 200).")
                return
            # -----------------------------------------

            action = "BUY" if signal == "BUY_TO_OPEN" else "SELL"
            order = LimitOrder(action, qty, round(limit_price, 2))
            
            trade = isolated_ib.placeOrder(contract, order)
            
            # Give IBKR network 1 second to register the order before disconnecting
            isolated_ib.sleep(1.0)
            
            # --- 2. NEW: FINAL STATUS CHECK ---
            final_status = trade.orderStatus.status
            if final_status in ['Cancelled', 'Inactive']:
                logger.error(f"❌ Order for {symbol} was rejected/cancelled by IBKR.")
                return
            # ----------------------------------
            
            # Mark the time to trigger the cooldown
            self.last_order_time[symbol] = now
            
            success_msg = f"✅ *IBKR ARBITRAGE EXECUTED!*\n\n*Action:* {action}\n*Qty:* {qty}\n*Contract:* `{symbol}`\n*Limit Price:* `${limit_price:.2f}`\n*Status:* `{final_status}`"
            logger.info(success_msg)
            
            if not self.is_simulation:
                self.send_telegram_alert(success_msg)
                
        except Exception as e:
            logger.error(f"❌ Failed to execute IBKR option order for {symbol}: {e}")
        finally:
            isolated_ib.disconnect()

    def hedge_delta(self, underlying_symbol: str, current_delta: float, threshold: float = 50.0):
        if abs(current_delta) < threshold:
            return 
            
        qty = int(abs(current_delta))
        action = "SELL" if current_delta > 0 else "BUY"
        
        # Check cooldown for hedging to prevent spamming stock orders
        hedge_key = f"HEDGE_{underlying_symbol}"
        now = time.time()
        if hedge_key in self.last_order_time:
            if (now - self.last_order_time[hedge_key]) < self.cooldown_seconds:
                return
                
        logger.warning(f"Delta Risk Exceeded ({current_delta:.2f})! Initiating IBKR Hedge...")
        
        isolated_ib = IB()
        client_id = random.randint(20000, 29999) 
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            isolated_ib.connect(self.host, self.port, clientId=client_id, timeout=5)
            
            contract = Stock(underlying_symbol, 'SMART', 'USD')
            isolated_ib.qualifyContracts(contract)
            
            order = MarketOrder(action, qty)
            trade = isolated_ib.placeOrder(contract, order)
            
            isolated_ib.sleep(1.0)
            self.last_order_time[hedge_key] = now
            
            success_msg = f"🛡️ *IBKR DELTA HEDGED!*\n\n*Action:* {action}\n*Qty:* {qty} shares\n*Underlying:* `{underlying_symbol}`\n*Current Delta was:* {current_delta:.2f}\n*Status:* `{trade.orderStatus.status}`"
            logger.info(success_msg)
            
            if not self.is_simulation:
                self.send_telegram_alert(success_msg)
                
        except Exception as e:
            logger.error(f"❌ Failed to hedge delta via IBKR: {e}")
        finally:
            isolated_ib.disconnect()