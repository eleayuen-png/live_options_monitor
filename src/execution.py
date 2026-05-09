"""
Order Management System (OMS)

Connects to the Alpaca Trading API to execute trades and manage portfolio risk.
"""

import logging
import requests
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest # <-- ADDED LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

logger = logging.getLogger(__name__)

class ExecutionEngine:
    """
    Handles live order routing to Alpaca Paper Trading.
    """
    def __init__(self, api_key: str, secret_key: str, telegram_token: str = None, telegram_chat_id: str = None):
        # paper=True is CRITICAL to ensure you don't use real money!
        self.client = TradingClient(api_key, secret_key, paper=True)
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        self.is_simulation = False # <-- ADD THIS FLAG
        
        # Verify connection
        try:
            
            acct = self.client.get_account()
            logger.info(f"Execution Engine Connected! Buying Power: ${acct.buying_power}")
        except Exception as e:
            logger.error(f"Failed to connect to Alpaca Trading: {e}")

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
        """
        Executes an options trade using Limit Orders to prevent massive slippage.
        """
        if signal == "HOLD":
            return
            
        # Safety check: Never send a market order for an option in this strategy
        if limit_price is None or limit_price <= 0:
            logger.error(f"Rejected {signal} for {symbol}: No limit price provided.")
            return
            
        logger.info(f"🚨 VOLATILITY ARBITRAGE SIGNAL: {signal} {qty} contract(s) of {symbol} @ Limit ${limit_price:.2f}")
        
        # Map our alpha strategy signals to Alpaca Order Sides
        if signal == "BUY_TO_OPEN":
            side = OrderSide.BUY
        elif signal == "SELL_TO_OPEN":
            side = OrderSide.SELL
        else:
            logger.error(f"Unknown signal received: {signal}")
            return
            
        try:
            # --- NEW: LIMIT ORDER LOGIC ---
            # Using Limit Orders pegged to the mid-price prevents paying the full spread
            order_data = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=TimeInForce.DAY,
                limit_price=round(limit_price, 2)
            )
            # Submit the options order to Alpaca's trading servers
            order = self.client.submit_order(order_data=order_data)
            
            # Include Order Status in Alert (Point 4 from Audit)
            success_msg = f"✅ *ARBITRAGE EXECUTED!*\n\n*Action:* {side.name}\n*Qty:* {qty}\n*Contract:* `{symbol}`\n*Limit Price:* `${limit_price:.2f}`\n*Status:* `{order.status.name}`\n*Order ID:* `{order.id}`"
            logger.info(success_msg)
            
            if not self.is_simulation: # <-- ONLY SEND IF NOT IN SIMULATION
                self.send_telegram_alert(success_msg)
            
        except Exception as e:
            logger.error(f"❌ Failed to execute option order for {symbol}: {e}")
            if not self.is_simulation: # <-- ONLY SEND IF NOT IN SIMULATION
                self.send_telegram_alert(f"❌ *ORDER FAILED*\n\n*Contract:* `{symbol}`\n*Reason:* {e}")

    def hedge_delta(self, underlying_symbol: str, current_delta: float, threshold: float = 50.0):
        """
        Automatically buys or sells the underlying stock to neutralize portfolio Delta.
        
        Args:
            underlying_symbol: The stock to trade (e.g., 'SPY')
            current_delta: The net delta exposure of your options portfolio
            threshold: How much delta to tolerate before hedging (e.g., 50 shares)
        """
        if abs(current_delta) < threshold:
            return # Delta is within acceptable risk limits
            
        # We need to do the OPPOSITE of our current delta to hedge
        qty = int(abs(current_delta))
        side = OrderSide.SELL if current_delta > 0 else OrderSide.BUY
        
        logger.warning(f"Delta Risk Exceeded ({current_delta:.2f})! Initiating Hedge...")
        
        try:
            order_data = MarketOrderRequest(
                symbol=underlying_symbol,
                qty=qty,
                side=side,
                time_in_force=TimeInForce.GTC
            )
            order = self.client.submit_order(order_data=order_data)
            
            success_msg = f"🛡️ *DELTA HEDGED!*\n\n*Action:* {side.name}\n*Qty:* {qty} shares\n*Underlying:* `{underlying_symbol}`\n*Current Delta was:* {current_delta:.2f}\n*Order ID:* `{order.id}`"
            logger.info(success_msg)
            
            if not self.is_simulation: # <-- ONLY SEND IF NOT IN SIMULATION
                self.send_telegram_alert(success_msg)
            
        except Exception as e:
            logger.error(f"❌ Failed to hedge delta: {e}")