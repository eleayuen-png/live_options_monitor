"""
Unit tests for Black-Scholes pricer module.

Tests validate against known benchmark values and edge cases.
"""

import pytest
import numpy as np
from src.pricer import OptionPricer, price_call, price_put, greeks_call, GreeksResult


class TestOptionPricer:
    """Test suite for OptionPricer class."""
    
    @pytest.fixture
    def pricer(self):
        """Create a pricer instance with default 5% risk-free rate."""
        return OptionPricer(risk_free_rate=0.05)
    
    def test_atm_call_price(self, pricer):
        """
        Test at-the-money (ATM) call pricing.
        
        For ATM call with 1 year to expiry and 20% vol:
        - Spot = Strike = 100
        - T = 1 year
        - r = 5%
        - σ = 20%
        Expected: ~10.45 (approximate benchmark)
        """
        price = pricer.price_option(
            spot_price=100,
            strike_price=100,
            time_to_expiry=1.0,
            volatility=0.20,
            option_type="call"
        )
        assert 10.0 < price < 11.0, f"ATM call price {price} outside expected range"
    
    def test_atm_put_price(self, pricer):
        """
        Test at-the-money put pricing.
        
        For ATM put with 1 year to expiry and 20% vol:
        Expected: ~5.57 (approximate benchmark)
        """
        price = pricer.price_option(
            spot_price=100,
            strike_price=100,
            time_to_expiry=1.0,
            volatility=0.20,
            option_type="put"
        )
        assert 5.0 < price < 6.0, f"ATM put price {price} outside expected range"
    
    def test_call_put_parity(self, pricer):
        """
        Test put-call parity: C - P = S - K*e^(-r*T)
        
        For the same spot, strike, expiry, and vol:
        call_price - put_price ≈ spot - strike * discount_factor
        """
        S, K, T, sigma = 100, 100, 1.0, 0.20
        
        call_price = pricer.price_option(S, K, T, sigma, "call")
        put_price = pricer.price_option(S, K, T, sigma, "put")
        
        discount_factor = np.exp(-0.05 * T)
        parity_lhs = call_price - put_price
        parity_rhs = S - K * discount_factor
        
        assert abs(parity_lhs - parity_rhs) < 1e-8, \
            f"Put-call parity violated: {parity_lhs} ≠ {parity_rhs}"
    
    def test_call_delta_range(self, pricer):
        """Test that call delta is between 0 and 1."""
        greeks = pricer.calculate_greeks(100, 100, 1.0, 0.20, "call")
        assert 0 <= greeks.delta <= 1, f"Call delta {greeks.delta} outside [0, 1]"
    
    def test_put_delta_range(self, pricer):
        """Test that put delta is between -1 and 0."""
        greeks = pricer.calculate_greeks(100, 100, 1.0, 0.20, "put")
        assert -1 <= greeks.delta <= 0, f"Put delta {greeks.delta} outside [-1, 0]"
    
    def test_deep_itm_call_delta(self, pricer):
        """Test that deep ITM call has delta close to 1."""
        greeks = pricer.calculate_greeks(150, 100, 1.0, 0.20, "call")
        assert greeks.delta > 0.95, f"Deep ITM call delta too low: {greeks.delta}"
    
    def test_deep_otm_call_delta(self, pricer):
        """Test that deep OTM call has delta close to 0."""
        greeks = pricer.calculate_greeks(50, 100, 1.0, 0.20, "call")
        assert greeks.delta < 0.05, f"Deep OTM call delta too high: {greeks.delta}"
    
    def test_gamma_always_positive(self, pricer):
        """Test that gamma is always positive for both calls and puts."""
        call_greeks = pricer.calculate_greeks(100, 100, 1.0, 0.20, "call")
        put_greeks = pricer.calculate_greeks(100, 100, 1.0, 0.20, "put")
        
        assert call_greeks.gamma > 0, f"Call gamma should be positive: {call_greeks.gamma}"
        assert put_greeks.gamma > 0, f"Put gamma should be positive: {put_greeks.gamma}"
    
    def test_gamma_same_for_call_put(self, pricer):
        """Test that gamma is identical for call and put (same underlying)."""
        call_greeks = pricer.calculate_greeks(100, 100, 1.0, 0.20, "call")
        put_greeks = pricer.calculate_greeks(100, 100, 1.0, 0.20, "put")
        
        assert abs(call_greeks.gamma - put_greeks.gamma) < 1e-10, \
            "Call and put gamma should be identical"
    
    def test_vega_always_positive(self, pricer):
        """Test that vega is always positive (for both calls and puts)."""
        call_greeks = pricer.calculate_greeks(100, 100, 1.0, 0.20, "call")
        put_greeks = pricer.calculate_greeks(100, 100, 1.0, 0.20, "put")
        
        assert call_greeks.vega > 0, f"Call vega should be positive: {call_greeks.vega}"
        assert put_greeks.vega > 0, f"Put vega should be positive: {put_greeks.vega}"
    
    def test_theta_sign_long_call(self, pricer):
        """Test that theta is negative for long calls (time decay hurts longs)."""
        greeks = pricer.calculate_greeks(100, 100, 0.01, 0.20, "call")  # Near expiry
        assert greeks.theta < 0, f"Long call should have negative theta: {greeks.theta}"
    
    def test_theta_sign_long_put(self, pricer):
        """Test that theta is negative for long puts (time decay hurts longs)."""
        greeks = pricer.calculate_greeks(100, 100, 0.01, 0.20, "put")
        assert greeks.theta < 0, f"Long put should have negative theta: {greeks.theta}"
    
    def test_rho_positive_call(self, pricer):
        """Test that rho is positive for calls (rising rates increase call value)."""
        greeks = pricer.calculate_greeks(100, 100, 1.0, 0.20, "call")
        assert greeks.rho > 0, f"Call rho should be positive: {greeks.rho}"
    
    def test_rho_negative_put(self, pricer):
        """Test that rho is negative for puts (rising rates decrease put value)."""
        greeks = pricer.calculate_greeks(100, 100, 1.0, 0.20, "put")
        assert greeks.rho < 0, f"Put rho should be negative: {greeks.rho}"
    
    def test_vectorized_batch_calculation(self, pricer):
        """Test batch Greeks calculation with arrays."""
        spots = np.array([90, 100, 110])
        strikes = np.array([100, 100, 100])
        times = np.array([1.0, 1.0, 1.0])
        vols = np.array([0.20, 0.20, 0.20])
        
        batch_greeks = pricer.calculate_greeks_batch(spots, strikes, times, vols, "call")
        
        # Check that we get arrays of correct length
        assert len(batch_greeks["delta"]) == 3
        assert len(batch_greeks["gamma"]) == 3
        
        # Check that delta increases with spot price (for calls)
        assert batch_greeks["delta"][0] < batch_greeks["delta"][1] < batch_greeks["delta"][2]
    
    def test_input_validation_negative_spot(self, pricer):
        """Test that negative spot price raises ValueError."""
        with pytest.raises(ValueError, match="Spot price must be positive"):
            pricer.price_option(-100, 100, 1.0, 0.20, "call")
    
    def test_input_validation_negative_strike(self, pricer):
        """Test that negative strike price raises ValueError."""
        with pytest.raises(ValueError, match="Strike price must be positive"):
            pricer.price_option(100, -100, 1.0, 0.20, "call")
    
    def test_input_validation_negative_time(self, pricer):
        """Test that negative time to expiry raises ValueError."""
        with pytest.raises(ValueError, match="Time to expiry must be positive"):
            pricer.price_option(100, 100, -1.0, 0.20, "call")
    
    def test_input_validation_negative_vol(self, pricer):
        """Test that negative volatility raises ValueError."""
        with pytest.raises(ValueError, match="Volatility must be positive"):
            pricer.price_option(100, 100, 1.0, -0.20, "call")
    
    def test_invalid_option_type(self, pricer):
        """Test that invalid option type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid option_type"):
            pricer.price_option(100, 100, 1.0, 0.20, "straddle")


class TestConvenienceFunctions:
    """Test convenience wrapper functions."""
    
    def test_price_call_convenience(self):
        """Test price_call() convenience function."""
        price = price_call(100, 100, 1.0, 0.20)
        assert 10.0 < price < 11.0
    
    def test_price_put_convenience(self):
        """Test price_put() convenience function."""
        price = price_put(100, 100, 1.0, 0.20)
        assert 5.0 < price < 6.0
    
    def test_greeks_call_convenience(self):
        """Test greeks_call() convenience function."""
        greeks = greeks_call(100, 100, 1.0, 0.20)
        assert isinstance(greeks, GreeksResult)
        assert 0 <= greeks.delta <= 1
        assert greeks.gamma > 0


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    @pytest.fixture
    def pricer(self):
        return OptionPricer(risk_free_rate=0.05)
    
    def test_very_short_expiry(self, pricer):
        """Test option with 1 day to expiry."""
        greeks = pricer.calculate_greeks(100, 100, 1/365, 0.20, "call")
        assert isinstance(greeks.delta, float)
        assert not np.isnan(greeks.delta)
    
    def test_very_long_expiry(self, pricer):
        """Test option with 10 years to expiry."""
        greeks = pricer.calculate_greeks(100, 100, 10.0, 0.20, "call")
        assert isinstance(greeks.delta, float)
        assert not np.isnan(greeks.delta)
    
    def test_high_volatility(self, pricer):
        """Test with 200% annualized volatility."""
        # Should still compute but may warn
        greeks = pricer.calculate_greeks(100, 100, 1.0, 2.0, "call")
        assert isinstance(greeks.delta, float)
        assert not np.isnan(greeks.delta)
    
    def test_low_volatility(self, pricer):
        """Test with 0.1% volatility."""
        greeks = pricer.calculate_greeks(100, 100, 1.0, 0.001, "call")
        assert isinstance(greeks.delta, float)
        # Near-zero vol should make call price close to intrinsic value
        intrinsic = max(100 - 100, 0)
        assert greeks.price > intrinsic


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
