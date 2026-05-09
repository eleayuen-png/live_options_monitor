"""
Alpaca Options WebSocket Streamer

Connects to Alpaca's Data API WebSocket to stream live options quotes,
parses the data, calculates Greeks, and sends updates via thread-safe queue.

Uses asyncio for non-blocking I/O and can be run in a background thread
alongside the Streamlit dashboard.
"""

import asyncio
import json
import logging
import os
from typing import Dict, List, Optional, Any, NamedTuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from collections import deque
import traceback

import aiohttp
from dotenv import load_dotenv

from src.pricer import OptionPricer, GreeksResult


logger = logging.getLogger(__name__)
load_dotenv()


@dataclass
class OptionTick:
    """Single option tick update."""
    symbol: str
    strike: float
    expiration: str
    option_type: str  # "call" or "put"
    bid: float
    ask: float
    mid: float  # (bid + ask) / 2
    bid_size: int
    ask_size: int
    timestamp: datetime
    greeks: Optional[GreeksResult] = None
    underlying_price: float = 0.0
    implied_volatility: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "symbol": self.symbol,
            "strike": self.strike,
            "expiration": self.expiration,
            "option_type": self.option_type,
            "bid": self.bid,
            "ask": self.ask,
            "mid": self.mid,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "timestamp": self.timestamp.isoformat(),
            "underlying_price": self.underlying_price,
            "implied_volatility": self.implied_volatility,
            "greeks": self.greeks.to_dict() if self.greeks else None,
        }


class AlpacaStreamer:
    """
    Alpaca options data WebSocket streamer with automatic reconnection,
    Greek calculation, and queue-based updates for dashboard consumption.
    """
    
    # Alpaca WebSocket endpoints
    WS_DATA_URL = "wss://stream.data.alpaca.markets/v1beta1/options"
    REST_API_URL = "https://api.alpaca.markets"
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        max_options: int = 100,
        max_reconnect_attempts: int = 5,
        reconnect_delay: int = 5,
    ):
        """
        Initialize Alpaca streamer.
        
        Args:
            api_key: Alpaca API key (uses env var if None)
            secret_key: Alpaca secret key (uses env var if None)
            max_options: Maximum options to track simultaneously
            max_reconnect_attempts: Max WebSocket reconnection tries
            reconnect_delay: Seconds to wait between reconnection attempts
        """
        self.api_key = api_key or os.getenv("ALPACA_API_KEY")
        self.secret_key = secret_key or os.getenv("ALPACA_SECRET_KEY")
        
        if not self.api_key or not self.secret_key:
            raise ValueError(
                "Alpaca credentials not found. Set ALPACA_API_KEY and "
                "ALPACA_SECRET_KEY in .env or pass them as arguments."
            )
        
        self.max_options = max_options
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_delay = reconnect_delay
        
        self.pricer = OptionPricer(risk_free_rate=0.05)
        
        # Data storage
        self.active_options: Dict[str, OptionTick] = {}
        self.underlying_prices: Dict[str, float] = {}
        self.option_history: Dict[str, deque] = {}  # Symbol -> deque of OptionTick
        self.max_history_points = 1000  # Keep last 1000 ticks per symbol
        
        # Connection state
        self.ws = None
        self.session = None
        self.is_connected = False
        self.reconnect_count = 0
        
        # Headers for Alpaca API
        self.headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
        }
    
    async def connect_and_stream(
        self,
        watchlist: Optional[List[str]] = None,
        update_queue: Optional[asyncio.Queue] = None,
    ) -> None:
        """
        Main streaming loop. Connects to Alpaca WebSocket and streams options data.
        
        Args:
            watchlist: List of option symbols to watch (e.g., ["AAPL", "TSLA"])
            update_queue: Asyncio queue to put tick updates into
            
        Raises:
            Exception: If connection fails after max reconnection attempts
        """
        if not watchlist:
            logger.warning("No watchlist provided; using empty list")
            watchlist = []
        
        while self.reconnect_count < self.max_reconnect_attempts:
            try:
                logger.info(f"Connecting to Alpaca WebSocket (attempt {self.reconnect_count + 1})")
                
                self.session = aiohttp.ClientSession()
                
                async with self.session.ws_connect(
                    self.WS_DATA_URL,
                    headers=self.headers,
                ) as ws:
                    self.ws = ws
                    self.is_connected = True
                    self.reconnect_count = 0
                    logger.info("Connected to Alpaca WebSocket")
                    
                    # Subscribe to symbols
                    for symbol in watchlist:
                        await self._subscribe(symbol)
                    
                    # Main message loop
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                tick = self._parse_alpaca_message(data)
                                
                                if tick:
                                    # Calculate Greeks
                                    tick = await self._calculate_greeks(tick)
                                    
                                    # Store tick
                                    self.active_options[tick.symbol] = tick
                                    
                                    # Maintain history (for charting)
                                    if tick.symbol not in self.option_history:
                                        self.option_history[tick.symbol] = deque(
                                            maxlen=self.max_history_points
                                        )
                                    self.option_history[tick.symbol].append(tick)
                                    
                                    # Send to dashboard queue
                                    if update_queue:
                                        try:
                                            update_queue.put_nowait({
                                                "type": "tick_update",
                                                "data": tick.to_dict(),
                                            })
                                        except asyncio.QueueFull:
                                            logger.warning("Update queue full; dropping tick")
                            
                            except json.JSONDecodeError as e:
                                logger.error(f"Failed to parse message: {e}")
                            except Exception as e:
                                logger.error(f"Error processing tick: {e}\n{traceback.format_exc()}")
                        
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"WebSocket error: {ws.exception()}")
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            logger.info("WebSocket closed")
                            break
            
            except asyncio.CancelledError:
                logger.info("Stream cancelled")
                break
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}\n{traceback.format_exc()}")
                self.reconnect_count += 1
                
                if self.reconnect_count < self.max_reconnect_attempts:
                    logger.info(f"Reconnecting in {self.reconnect_delay}s...")
                    await asyncio.sleep(self.reconnect_delay)
            finally:
                self.is_connected = False
                if self.session:
                    await self.session.close()
        
        logger.error(f"Max reconnection attempts ({self.max_reconnect_attempts}) reached")
    
    async def _subscribe(self, symbol: str) -> None:
        """Subscribe to option symbol on WebSocket."""
        if not self.ws:
            logger.error("WebSocket not connected; cannot subscribe")
            return
        
        subscribe_msg = {
            "action": "subscribe",
            "trades": [symbol],
            "quotes": [symbol],
        }
        
        try:
            await self.ws.send_json(subscribe_msg)
            logger.info(f"Subscribed to {symbol}")
        except Exception as e:
            logger.error(f"Failed to subscribe to {symbol}: {e}")
    
    def _parse_alpaca_message(self, data: Dict[str, Any]) -> Optional[OptionTick]:
        """
        Parse Alpaca WebSocket message into OptionTick.
        
        Expected format (varies by subscription type):
        - trades: {"T": "t", "x": "cboe", "S": "SPY230519C400", "p": 10.5, "s": 10, "t": "..."}
        - quotes: {"T": "q", "ax": "P", "ap": 10.5, "as": 5, "bx": "P", "bp": 10.4, "bs": 10, ...}
        """
        try:
            msg_type = data.get("T")
            
            if msg_type == "q":  # Quote
                symbol = data.get("S")
                bid = data.get("bp")
                ask = data.get("ap")
                bid_size = data.get("bs", 0)
                ask_size = data.get("as", 0)
            elif msg_type == "t":  # Trade
                symbol = data.get("S")
                price = data.get("p")
                bid = price
                ask = price
                bid_size = data.get("s", 0)
                ask_size = 0
            else:
                return None
            
            if not symbol or not bid or not ask:
                return None
            
            # Parse option symbol: e.g., "SPY230519C400" -> strike=400, exp=2023-05-19, type=call
            symbol_data = self._parse_option_symbol(symbol)
            if not symbol_data:
                return None
            
            strike, expiration, opt_type = symbol_data
            underlying_symbol = symbol.rstrip("0123456789CP")
            
            tick = OptionTick(
                symbol=symbol,
                strike=strike,
                expiration=expiration,
                option_type=opt_type,
                bid=bid,
                ask=ask,
                mid=(bid + ask) / 2,
                bid_size=bid_size,
                ask_size=ask_size,
                timestamp=datetime.now(timezone.utc),
                underlying_price=self.underlying_prices.get(underlying_symbol, 0.0),
                implied_volatility=0.0,  # Will be set from data if available
            )
            
            return tick
        
        except Exception as e:
            logger.error(f"Error parsing Alpaca message: {e}")
            return None
    
    @staticmethod
    def _parse_option_symbol(symbol: str) -> Optional[tuple]:
        """
        Parse OCC option symbol format.
        
        Format: UNDERLYING + YYMMDD + C/P + STRIKE
        Example: SPY230519C400
        - Underlying: SPY
        - Date: 2023-05-19
        - Type: Call
        - Strike: 400
        
        Returns: (strike, expiration_str, option_type) or None
        """
        try:
            # Find where the option type (C or P) is
            cp_idx = -1
            for i, char in enumerate(symbol):
                if char in ("C", "P"):
                    cp_idx = i
                    break
            
            if cp_idx == -1 or cp_idx < 7:
                return None
            
            # Extract components
            underlying = symbol[:cp_idx-6]
            date_str = symbol[cp_idx-6:cp_idx]  # YYMMDD
            opt_type = symbol[cp_idx]  # C or P
            strike_str = symbol[cp_idx+1:]
            
            if not strike_str or not date_str:
                return None
            
            # Parse date (YYMMDD -> YYYY-MM-DD)
            yy = int(date_str[:2])
            mm = int(date_str[2:4])
            dd = int(date_str[4:6])
            yyyy = 2000 + yy if yy <= 30 else 1900 + yy  # Cutoff at 2030
            expiration = f"{yyyy:04d}-{mm:02d}-{dd:02d}"
            
            # Parse strike (may have decimal: "40000" -> 400.00, "4050" -> 40.50)
            strike = float(strike_str) / 100.0
            
            return (strike, expiration, "call" if opt_type == "C" else "put")
        
        except (ValueError, IndexError):
            return None
    
    async def _calculate_greeks(self, tick: OptionTick) -> OptionTick:
        """Calculate Greeks for an option tick."""
        try:
            if tick.underlying_price <= 0 or tick.implied_volatility <= 0:
                # Use mid as fallback price for Greeks
                return tick
            
            # Days to expiration
            exp_date = datetime.strptime(tick.expiration, "%Y-%m-%d").date()
            today = datetime.now(timezone.utc).date()
            days_to_exp = max((exp_date - today).days, 1)
            time_to_exp = days_to_exp / 365.0
            
            # Back-calculate Implied Volatility using the mid price
            iv = self.pricer.implied_volatility(
                target_price=tick.mid,
                spot_price=tick.underlying_price,
                strike_price=tick.strike,
                time_to_expiry=time_to_exp,
                option_type=tick.option_type
            )
            tick.implied_volatility = iv
            
            # Now calculate accurate Greeks
            greeks = self.pricer.calculate_greeks(
                spot_price=tick.underlying_price,
                strike_price=tick.strike,
                time_to_expiry=time_to_exp,
                volatility=iv,
                option_type=tick.option_type,
            )
            
            tick.greeks = greeks
            return tick
        
        except Exception as e:
            logger.error(f"Error calculating Greeks for {tick.symbol}: {e}")
            return tick
    
    async def set_underlying_price(self, symbol: str, price: float) -> None:
        """Update underlying asset price for Greeks calculation."""
        self.underlying_prices[symbol] = price
    
    async def set_implied_volatility(self, symbol: str, iv: float) -> None:
        """Update implied volatility for option symbol."""
        if symbol in self.active_options:
            self.active_options[symbol].implied_volatility = iv
    
    def get_active_options(self) -> List[OptionTick]:
        """Get list of all currently active options."""
        return list(self.active_options.values())
    
    def get_option_history(self, symbol: str, limit: int = 100) -> List[OptionTick]:
        """Get historical ticks for a symbol."""
        if symbol not in self.option_history:
            return []
        
        history = self.option_history[symbol]
        return list(history)[-limit:] if limit else list(history)
    
    async def close(self) -> None:
        """Close WebSocket connection and cleanup."""
        logger.info("Closing Alpaca streamer")
        
        if self.ws:
            await self.ws.close()
        
        if self.session:
            await self.session.close()
        
        self.is_connected = False


async def run_streamer_background(
    api_key: str,
    secret_key: str,
    watchlist: List[str],
    update_queue: asyncio.Queue,
    max_options: int = 100,
) -> None:
    """
    Run the Alpaca streamer as a background coroutine.
    
    Designed to be called from a background thread with its own event loop.
    
    Args:
        api_key: Alpaca API key
        secret_key: Alpaca secret key
        watchlist: List of tickers to watch
        update_queue: Asyncio queue for tick updates
        max_options: Max options to track
    """
    streamer = AlpacaStreamer(
        api_key=api_key,
        secret_key=secret_key,
        max_options=max_options,
    )
    
    try:
        await streamer.connect_and_stream(watchlist, update_queue)
    finally:
        await streamer.close()


if __name__ == "__main__":
    # Example: Run streamer standalone
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        queue = asyncio.Queue(maxsize=100)
        await run_streamer_background(
            api_key=os.getenv("ALPACA_API_KEY"),
            secret_key=os.getenv("ALPACA_SECRET_KEY"),
            watchlist=["AAPL", "TSLA"],
            update_queue=queue,
        )
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down")
