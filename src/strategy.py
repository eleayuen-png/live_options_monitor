"""
Options Strategy & Alpha Engine
Final Stable Version - Fixed Imports and Client ID logic.
"""

import logging
import asyncio
import numpy as np
import pandas as pd
from typing import List, Dict
from ib_insync import IB, Stock, util

# Use relative-style import or ensure sys.path in main.py covers this
try:
    from src.streamer import OptionTick
except ImportError:
    # Fallback if running as standalone
    from streamer import OptionTick

logger = logging.getLogger(__name__)

class DynamicVolatilityEngine:
    def __init__(self, host='ib-gateway', port=4002, on_connect=None):
        self.host = host
        self.port = port
        self.on_connect = on_connect
        self.rv_cache: Dict[str, float] = {}

    async def calculate_30d_rv_async(self, symbols: List[str]) -> Dict[str, float]:
        """Fetch historical data using a high Client ID to avoid TWS conflicts."""
        ib = IB()
        try:
            logger.info(f"🔌 Connecting to IBKR for historical data...")
            # Use ID 400 for strategy data fetching
            await ib.connectAsync(self.host, self.port, clientId=400, timeout=30)
            if self.on_connect is not None:
                self.on_connect(ib)
            
            for symbol in symbols:
                try:
                    contract = Stock(symbol, 'SMART', 'USD')
                    await ib.qualifyContractsAsync(contract)
                    
                    bars = await ib.reqHistoricalDataAsync(
                        contract, endDateTime='', durationStr='45 D',
                        barSizeSetting='1 day', whatToShow='ADJUSTED_LAST',
                        useRTH=True, formatDate=1
                    )
                    
                    if bars:
                        df = util.df(bars)
                        df['log_ret'] = np.log(df['close'] / df['close'].shift(1))
                        rv = df['log_ret'].std() * np.sqrt(252)
                        self.rv_cache[symbol] = float(rv)
                        logger.info(f"✅ [{symbol}] RV Calculated: {rv:.2%}")
                    else:
                        self.rv_cache[symbol] = 0.20
                        
                    await asyncio.sleep(1) 
                except Exception as e:
                    logger.warning(f"Error fetching {symbol}: {e}")
                    self.rv_cache[symbol] = 0.20
        except Exception as e:
            logger.error(f"Strategy Connection failed: {e}")
        finally:
            if ib.isConnected():
                ib.disconnect()
                await asyncio.sleep(2) # Let the socket clear
        return self.rv_cache

class VolatilityArbitrage:
    def __init__(self, rv_engine: DynamicVolatilityEngine, symbols: List[str], threshold: float = 0.05):
        self.rv_engine = rv_engine
        self.threshold = threshold
        self.symbols = symbols
        self.target_rv_map = {}

    async def initialize_async(self):
        # Retry-with-backoff: a stale clientId=400 slot or mid-restart gateway
        # can leave the RV cache full of defaults (0.20). Without retry, the
        # whole run silently runs on those defaults and no signals fire.
        delay = 5
        result = {}
        for attempt in range(5):
            result = await self.rv_engine.calculate_30d_rv_async(self.symbols)
            if any(v != 0.20 for v in result.values()):
                self.target_rv_map = result
                return
            logger.warning(f"RV fetch attempt {attempt+1}/5 returned only defaults — retry in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)
        logger.error("Strategy failed to fetch real RV data after 5 attempts — running with 20% defaults")
        self.target_rv_map = result

    def generate_signal(self, tick: OptionTick) -> str:
        """
        Long-only cash-account strategy:
          - BUY_TO_OPEN when RV - IV > threshold (option is cheap vs realized vol)
          - SELL_TO_OPEN is NEVER emitted (cash account cannot short)
          - HOLD otherwise

        Caller is responsible for separately checking the "fundamental exit"
        condition via `is_fundamental_exit(tick)` on every tick.
        """
        if not tick or tick.implied_volatility <= 0:
            return "HOLD"
        underlying = tick.symbol.rstrip("0123456789CP")
        target_rv = self.target_rv_map.get(underlying, 0.20)

        # Long-only: enter when realized > implied + threshold (IV undervalued)
        if target_rv - tick.implied_volatility > self.threshold:
            return "BUY_TO_OPEN"
        return "HOLD"

    def is_fundamental_exit(self, tick: OptionTick, tolerance: float = 0.01) -> bool:
        """
        Triggered when IV converges to RV (within tolerance). The vol-arb
        opportunity has played out — flatten the position regardless of P&L.
        """
        if not tick or tick.implied_volatility <= 0:
            return False
        underlying = tick.symbol.rstrip("0123456789CP")
        target_rv = self.target_rv_map.get(underlying, 0.20)
        return abs(tick.implied_volatility - target_rv) <= tolerance