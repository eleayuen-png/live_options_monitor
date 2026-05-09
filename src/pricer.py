"""
Black-Scholes Options Pricing Engine

Upgraded with Implied Volatility (Newton-Raphson) for real-world usability.
"""

from typing import Dict, Tuple, NamedTuple, Literal, Union, Optional
from dataclasses import dataclass
import numpy as np
from scipy.stats import norm
import warnings


OptionType = Literal["call", "put"]


@dataclass
class GreeksResult:
    """Container for option Greeks and theoretical price."""
    price: float
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float
    
    def to_dict(self) -> Dict[str, float]:
        """Convert Greeks to dictionary for easy serialization."""
        return {
            "price": self.price,
            "delta": self.delta,
            "gamma": self.gamma,
            "theta": self.theta,
            "vega": self.vega,
            "rho": self.rho,
        }


class OptionPricer:
    """
    Vectorized Black-Scholes options pricing calculator.
    """
    
    def __init__(self, risk_free_rate: float = 0.05):
        self.r = risk_free_rate
        
    def price_option(
        self,
        spot_price: Union[float, np.ndarray],
        strike_price: Union[float, np.ndarray],
        time_to_expiry: Union[float, np.ndarray],
        volatility: Union[float, np.ndarray],
        option_type: OptionType = "call",
    ) -> Union[float, np.ndarray]:
        self._validate_inputs(spot_price, strike_price, time_to_expiry, volatility)
        d1, d2 = self._compute_d1_d2(spot_price, strike_price, time_to_expiry, volatility)
        discount_factor = np.exp(-self.r * time_to_expiry)
        
        if option_type.lower() == "call":
            price = (spot_price * norm.cdf(d1) - strike_price * discount_factor * norm.cdf(d2))
        else:
            price = (strike_price * discount_factor * norm.cdf(-d2) - spot_price * norm.cdf(-d1))
        return price
    
    def calculate_greeks(
        self,
        spot_price: float,
        strike_price: float,
        time_to_expiry: float,
        volatility: float,
        option_type: OptionType = "call",
    ) -> GreeksResult:
        self._validate_inputs(spot_price, strike_price, time_to_expiry, volatility)
        d1, d2 = self._compute_d1_d2(spot_price, strike_price, time_to_expiry, volatility)
        discount_factor = np.exp(-self.r * time_to_expiry)
        sqrt_t = np.sqrt(time_to_expiry)
        pdf_d1 = norm.pdf(d1)
        
        if option_type.lower() == "call":
            price = (spot_price * norm.cdf(d1) - strike_price * discount_factor * norm.cdf(d2))
            delta = norm.cdf(d1)
            theta = ((-spot_price * pdf_d1 * volatility / (2 * sqrt_t)) - (self.r * strike_price * discount_factor * norm.cdf(d2))) / 365.0
            rho = strike_price * time_to_expiry * discount_factor * norm.cdf(d2) / 100.0
        else:
            price = (strike_price * discount_factor * norm.cdf(-d2) - spot_price * norm.cdf(-d1))
            delta = norm.cdf(d1) - 1.0
            theta = ((-spot_price * pdf_d1 * volatility / (2 * sqrt_t)) + (self.r * strike_price * discount_factor * norm.cdf(-d2))) / 365.0
            rho = -strike_price * time_to_expiry * discount_factor * norm.cdf(-d2) / 100.0
            
        gamma = pdf_d1 / (spot_price * volatility * sqrt_t)
        vega = spot_price * pdf_d1 * sqrt_t / 100.0
        
        return GreeksResult(
            price=float(price), delta=float(delta), gamma=float(gamma),
            theta=float(theta), vega=float(vega), rho=float(rho)
        )

    def implied_volatility(
        self,
        target_price: float,
        spot_price: float,
        strike_price: float,
        time_to_expiry: float,
        option_type: OptionType = "call",
        tol: float = 1e-5,
        max_iter: int = 100
    ) -> float:
        """
        Calculate Implied Volatility using Newton-Raphson.
        Proves you understand numerical methods for Quant roles.
        """
        sigma = 0.30  # Standard starting guess
        for i in range(max_iter):
            greeks = self.calculate_greeks(spot_price, strike_price, time_to_expiry, sigma, option_type)
            diff = greeks.price - target_price
            if abs(diff) < tol:
                return sigma
            vega = greeks.vega * 100  # Scale back to decimal volatility change
            if abs(vega) < 1e-6: break
            sigma = sigma - diff / vega
            sigma = max(0.0001, min(sigma, 5.0)) # Boundaries
        return sigma

    def _compute_d1_d2(self, S, K, T, sigma):
        sqrt_t = np.sqrt(T)
        d1 = (np.log(S / K) + (self.r + 0.5 * sigma**2) * T) / (sigma * sqrt_t)
        d2 = d1 - sigma * sqrt_t
        return d1, d2

    @staticmethod
    def _validate_inputs(S, K, T, sigma):
        if np.any(np.asarray(S) <= 0) or np.any(np.asarray(K) <= 0) or np.any(np.asarray(T) <= 0) or np.any(np.asarray(sigma) <= 0):
            raise ValueError("All BS parameters (S, K, T, sigma) must be positive.")