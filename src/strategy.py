"""
Options Strategy & Alpha Engine - IBKR Native Edition

Features:
- Dynamic Realized Volatility Calculation via IBKR Historical Data (Isolated Connection)
- Statistical Arbitrage (IV vs RV) Signal Generation
"""

import logging
import asyncio
import random
import numpy as np
import pandas as pd
from typing import List, Dict
from ib_insync import IB, Stock, util

from src.streamer import OptionTick

logger = logging.getLogger(__name__)

class DynamicVolatilityEngine:
    """
    Downloads historical data via IBKR and calculates rolling Realized Volatility (RV).
    """
    # --- CRITICAL FIX: Updated default host and port to use the new IB Gateway ---
    def __init__(self, host='ib-gateway', port=4002):
        self.host = host
        self.port = port
        self.rv_cache: Dict[str, float] = {}

    def calculate_30d_rv(self, symbols: List[str]) -> Dict[str, float]:
        """
        Synchronous fetch using an ISOLATED, temporary IBKR connection.
        """
        logger.info(f"Downloading IBKR historical data for: {symbols}")
        
        isolated_ib = IB()
        client_id = random.randint(5000, 9999) 
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # --- Connect using the dynamic variables (ib-gateway:4002) ---
            isolated_ib.connect(self.host, self.port, clientId=client_id, timeout=10)
            logger.info(f"🔌 Isolated Strategy connection established (Client ID: {client_id})")
        except Exception as e:
            logger.error(f"❌ Isolated Strategy connection failed: {e}")
            return {sym: 0.20 for sym in symbols} 

        try:
            for symbol in symbols:
                isolated_ib.sleep(1.0) # Pacing
                
                success = False
                for attempt in range(3):
                    try:
                        contract = Stock(symbol, 'SMART', 'USD')
                        isolated_ib.qualifyContracts(contract)
                        
                        if not contract.conId:
                            logger.warning(f"⚠️ Could not qualify {symbol}.")
                            isolated_ib.sleep(2.0)
                            continue

                        bars = isolated_ib.reqHistoricalData(
                            contract,
                            endDateTime='',
                            durationStr='45 D',
                            barSizeSetting='1 day',
                            whatToShow='ADJUSTED_LAST',
                            useRTH=True,
                            formatDate=1
                        )
                        
                        if not bars:
                            logger.warning(f"⚠️ No historical data for {symbol} on attempt {attempt+1}")
                            isolated_ib.sleep(2.0)
                            continue

                        df = util.df(bars)
                        df['log_ret'] = np.log(df['close'] / df['close'].shift(1))
                        annualized_rv = df['log_ret'].std() * np.sqrt(252)
                        
                        self.rv_cache[symbol] = float(annualized_rv)
                        logger.info(f"✅ [{symbol}] IBKR Calculated 30-Day RV: {annualized_rv:.2%}")
                        success = True
                        break
                        
                    except Exception as e:
                        logger.warning(f"⏳ Timeout/Error fetching IBKR history for {symbol} (Attempt {attempt+1}/3): {e}")
                        isolated_ib.sleep(3.0)
                
                if not success:
                    logger.error(f"🛑 Failed to fetch data for {symbol} after 3 attempts. Defaulting to 20% RV.")
                    self.rv_cache[symbol] = 0.20
        finally:
            isolated_ib.disconnect()
            logger.info("🔌 Isolated Strategy connection closed. Handing back to Dashboard.")
            
        return self.rv_cache

class VolatilityArbitrage:
    def __init__(self, rv_engine: DynamicVolatilityEngine, symbols: List[str], threshold: float = 0.05):
        self.rv_engine = rv_engine
        self.threshold = threshold
        self.symbols = symbols
        self.target_rv_map = {}

    def initialize(self):
        """Synchronous initialization to compute RVs."""
        self.target_rv_map = self.rv_engine.calculate_30d_rv(self.symbols)

    def generate_signal(self, tick: OptionTick) -> str:
        if not tick or tick.implied_volatility <= 0:
            return "HOLD"
            
        if tick.bid <= 0 or tick.ask <= 0:
            return "HOLD"
            
        spread = tick.ask - tick.bid
        if spread / tick.mid > 0.20:
            return "HOLD"

        underlying = tick.symbol.rstrip("0123456789CP")
        target_rv = self.target_rv_map.get(underlying, 0.20)

        if tick.implied_volatility > target_rv + self.threshold:
            return "SELL_TO_OPEN"
        elif tick.implied_volatility < target_rv - self.threshold:
            return "BUY_TO_OPEN"
            
        return "HOLD"