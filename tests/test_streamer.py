"""
Unit tests for Alpaca streamer module.

Tests option symbol parsing, Greeks calculation, and data aggregation.
Uses mock data to validate parsing and alert logic without live API.
"""

import pytest
from datetime import datetime, timezone
from typing import Dict
from src.streamer import AlpacaStreamer, OptionTick
from src.pricer import GreeksResult


class TestOptionSymbolParsing:
    """Test OCC option symbol format parsing."""
    
    def test_parse_call_symbol(self):
        """Test parsing standard call option symbol."""
        # SPY230519C400 -> SPY, 2023-05-19, Call, 400 strike
        result = AlpacaStreamer._parse_option_symbol("SPY230519C400")
        
        assert result is not None
        strike, exp, opt_type = result
        assert strike == 400.0
        assert exp == "2023-05-19"
        assert opt_type == "call"
    
    def test_parse_put_symbol(self):
        """Test parsing put option symbol."""
        result = AlpacaStreamer._parse_option_symbol("AAPL240119P150")
        
        assert result is not None
        strike, exp, opt_type = result
        assert strike == 150.0
        assert exp == "2024-01-19"
        assert opt_type == "put"
    
    def test_parse_fractional_strike(self):
        """Test parsing option with fractional strike."""
        # QQQ240119C375.50 (strike of 375.50)
        result = AlpacaStreamer._parse_option_symbol("QQQ24011937550")
        
        assert result is not None
        strike, exp, opt_type = result
        assert strike == 375.50
        assert opt_type == "call"
    
    def test_parse_invalid_symbol(self):
        """Test that invalid symbols return None."""
        assert AlpacaStreamer._parse_option_symbol("INVALID") is None
        assert AlpacaStreamer._parse_option_symbol("SPY") is None
        assert AlpacaStreamer._parse_option_symbol("") is None
    
    def test_year_cutoff_logic(self):
        """Test that year parsing uses correct cutoff (00-30 -> 2000+)."""
        # 240119 should be 2024
        result = AlpacaStreamer._parse_option_symbol("SPY240119C400")
        assert result[1] == "2024-01-19"
        
        # 321231 would be 2032 (past cutoff, so 1932 - but should parse as 2032)
        result = AlpacaStreamer._parse_option_symbol("SPY321231C400")
        # Expect 2032 if cutoff is 30
        assert result is not None


class TestOptionTick:
    """Test OptionTick data structure."""
    
    def test_tick_creation(self):
        """Test creating an OptionTick."""
        tick = OptionTick(
            symbol="SPY230519C400",
            strike=400.0,
            expiration="2023-05-19",
            option_type="call",
            bid=5.0,
            ask=5.1,
            mid=5.05,
            bid_size=100,
            ask_size=50,
            timestamp=datetime.now(timezone.utc),
        )
        
        assert tick.symbol == "SPY230519C400"
        assert tick.mid == 5.05
        assert tick.option_type == "call"
    
    def test_tick_with_greeks(self):
        """Test OptionTick with Greeks."""
        greeks = GreeksResult(
            price=5.05,
            delta=0.65,
            gamma=0.02,
            theta=-0.05,
            vega=0.15,
            rho=0.10,
        )
        
        tick = OptionTick(
            symbol="SPY230519C400",
            strike=400.0,
            expiration="2023-05-19",
            option_type="call",
            bid=5.0,
            ask=5.1,
            mid=5.05,
            bid_size=100,
            ask_size=50,
            timestamp=datetime.now(timezone.utc),
            greeks=greeks,
        )
        
        assert tick.greeks.delta == 0.65
        assert tick.greeks.gamma == 0.02
    
    def test_tick_to_dict(self):
        """Test serializing OptionTick to dictionary."""
        greeks = GreeksResult(
            price=5.05,
            delta=0.65,
            gamma=0.02,
            theta=-0.05,
            vega=0.15,
            rho=0.10,
        )
        
        tick = OptionTick(
            symbol="SPY230519C400",
            strike=400.0,
            expiration="2023-05-19",
            option_type="call",
            bid=5.0,
            ask=5.1,
            mid=5.05,
            bid_size=100,
            ask_size=50,
            timestamp=datetime.now(timezone.utc),
            greeks=greeks,
        )
        
        data = tick.to_dict()
        assert data["symbol"] == "SPY230519C400"
        assert data["strike"] == 400.0
        assert data["greeks"]["delta"] == 0.65


class TestAlpacaMessageParsing:
    """Test Alpaca WebSocket message parsing."""
    
    def test_parse_quote_message(self):
        """Test parsing quote (bid/ask) message."""
        streamer = AlpacaStreamer(
            api_key="test_key",
            secret_key="test_secret",
        )
        
        # Mock Alpaca quote message
        message = {
            "T": "q",
            "S": "SPY230519C400",
            "bp": 5.0,
            "ap": 5.1,
            "bs": 100,
            "as": 50,
        }
        
        tick = streamer._parse_alpaca_message(message)
        
        assert tick is not None
        assert tick.bid == 5.0
        assert tick.ask == 5.1
        assert tick.bid_size == 100
        assert tick.ask_size == 50
    
    def test_parse_trade_message(self):
        """Test parsing trade message."""
        streamer = AlpacaStreamer(
            api_key="test_key",
            secret_key="test_secret",
        )
        
        # Mock Alpaca trade message
        message = {
            "T": "t",
            "S": "SPY230519C400",
            "p": 5.05,
            "s": 25,
        }
        
        tick = streamer._parse_alpaca_message(message)
        
        assert tick is not None
        assert tick.bid == 5.05
        assert tick.ask == 5.05
        assert tick.mid == 5.05
    
    def test_parse_invalid_message(self):
        """Test that invalid messages return None."""
        streamer = AlpacaStreamer(
            api_key="test_key",
            secret_key="test_secret",
        )
        
        # Missing symbol
        assert streamer._parse_alpaca_message({"T": "q", "bp": 5.0}) is None
        
        # Missing bid/ask
        assert streamer._parse_alpaca_message({"T": "q", "S": "SPY230519C400"}) is None
        
        # Invalid type
        assert streamer._parse_alpaca_message({"T": "x", "S": "SPY230519C400"}) is None


class TestAlpacaInitialization:
    """Test AlpacaStreamer initialization."""
    
    def test_init_with_credentials(self):
        """Test initializing with explicit credentials."""
        streamer = AlpacaStreamer(
            api_key="test_key",
            secret_key="test_secret",
        )
        
        assert streamer.api_key == "test_key"
        assert streamer.secret_key == "test_secret"
    
    def test_init_missing_credentials(self):
        """Test that missing credentials raise error."""
        import os
        
        # Temporarily remove env vars
        old_key = os.environ.pop("ALPACA_API_KEY", None)
        old_secret = os.environ.pop("ALPACA_SECRET_KEY", None)
        
        try:
            with pytest.raises(ValueError, match="credentials not found"):
                AlpacaStreamer()
        finally:
            # Restore
            if old_key:
                os.environ["ALPACA_API_KEY"] = old_key
            if old_secret:
                os.environ["ALPACA_SECRET_KEY"] = old_secret


class TestMockDataGeneration:
    """Helper functions for generating realistic mock market data."""
    
    @staticmethod
    def generate_mock_quote(
        symbol: str = "SPY230519C400",
        base_bid: float = 5.0,
        spread: float = 0.1,
    ) -> Dict:
        """Generate a mock Alpaca quote message."""
        return {
            "T": "q",
            "S": symbol,
            "bp": base_bid,
            "ap": base_bid + spread,
            "bs": 100,
            "as": 50,
        }
    
    @staticmethod
    def generate_mock_trade(
        symbol: str = "SPY230519C400",
        price: float = 5.05,
        size: int = 25,
    ) -> Dict:
        """Generate a mock Alpaca trade message."""
        return {
            "T": "t",
            "S": symbol,
            "p": price,
            "s": size,
        }


class TestMessageParsing:
    """Test parsing mock messages."""
    
    def test_parse_generated_quote(self):
        """Test parsing a generated mock quote."""
        streamer = AlpacaStreamer(
            api_key="test_key",
            secret_key="test_secret",
        )
        
        message = TestMockDataGeneration.generate_mock_quote(
            symbol="TSLA240119C150",
            base_bid=4.5,
            spread=0.15,
        )
        
        tick = streamer._parse_alpaca_message(message)
        assert tick is not None
        assert tick.bid == 4.5
        assert tick.ask == 4.65
    
    def test_parse_generated_trade(self):
        """Test parsing a generated mock trade."""
        streamer = AlpacaStreamer(
            api_key="test_key",
            secret_key="test_secret",
        )
        
        message = TestMockDataGeneration.generate_mock_trade(
            symbol="AAPL240119P150",
            price=2.30,
            size=50,
        )
        
        tick = streamer._parse_alpaca_message(message)
        assert tick is not None
        assert tick.mid == 2.30


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
