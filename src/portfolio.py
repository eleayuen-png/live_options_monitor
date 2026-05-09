"""
Portfolio Aggregation & Risk Management

Aggregates individual option positions into portfolio-level Greeks,
provides bucketing strategies (by strike/expiration), and detects
significant market moves for alerting.
"""

import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np
import pandas as pd

from src.streamer import OptionTick

logger = logging.getLogger(__name__)


@dataclass
class PortfolioMetrics:
    """Aggregated portfolio-level risk metrics."""
    total_delta: float = 0.0
    total_gamma: float = 0.0
    total_theta: float = 0.0
    total_vega: float = 0.0
    total_rho: float = 0.0
    
    # Directional breakdowns
    long_delta: float = 0.0
    short_delta: float = 0.0
    
    num_options: int = 0
    num_calls: int = 0
    num_puts: int = 0
    
    total_notional_value: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "total_delta": self.total_delta,
            "total_gamma": self.total_gamma,
            "total_theta": self.total_theta,
            "total_vega": self.total_vega,
            "total_rho": self.total_rho,
            "long_delta": self.long_delta,
            "short_delta": self.short_delta,
            "num_options": self.num_options,
            "num_calls": self.num_calls,
            "num_puts": self.num_puts,
            "total_notional_value": self.total_notional_value,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class GreekAlert:
    """Alert for significant Greek changes."""
    symbol: str
    greek_name: str
    old_value: float
    new_value: float
    change_pct: float
    threshold: float
    timestamp: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "greek_name": self.greek_name,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "change_pct": self.change_pct,
            "threshold": self.threshold,
            "timestamp": self.timestamp.isoformat(),
        }


class Portfolio:
    """
    Aggregates individual option positions and provides portfolio-level analytics.
    Bucketing strategy: First by strike, then by expiration.
    """
    
    def __init__(
        self,
        delta_alert_threshold: float = 0.15,
        gamma_alert_threshold: float = 0.02,
        theta_alert_threshold: float = 0.10,
        vega_alert_threshold: float = 0.20,
    ):
        self.delta_alert_threshold = delta_alert_threshold
        self.gamma_alert_threshold = gamma_alert_threshold
        self.theta_alert_threshold = theta_alert_threshold
        self.vega_alert_threshold = vega_alert_threshold
        
        self.positions: Dict[str, OptionTick] = {}
        self.previous_greeks: Dict[str, Dict[str, float]] = {}
        
        self.alerts: List[GreekAlert] = []
        self.max_alerts = 1000
        
        self.metrics_history: List[PortfolioMetrics] = []
        self.max_history = 1000
    
    def update_position(self, tick: OptionTick) -> Optional[GreekAlert]:
        """Update portfolio with new option tick and check for alerts."""
        alert = None
        
        # Check for significant Greek changes
        if tick.symbol in self.positions and tick.greeks:
            alert = self._check_for_alert(tick)
        
        # Store position
        self.positions[tick.symbol] = tick
        
        # Store Greeks for next comparison
        if tick.greeks:
            self.previous_greeks[tick.symbol] = {
                "delta": tick.greeks.delta,
                "gamma": tick.greeks.gamma,
                "theta": tick.greeks.theta,
                "vega": tick.greeks.vega,
                "rho": tick.greeks.rho,
            }
        
        return alert
    
    def _check_for_alert(self, tick: OptionTick) -> Optional[GreekAlert]:
        """Check if Greeks have changed significantly enough to trigger alert."""
        if not tick.greeks or tick.symbol not in self.previous_greeks:
            return None
        
        prev = self.previous_greeks[tick.symbol]
        curr = tick.greeks
        
        greeks_to_check = [
            ("delta", curr.delta, prev["delta"], self.delta_alert_threshold),
            ("gamma", curr.gamma, prev["gamma"], self.gamma_alert_threshold),
            ("theta", curr.theta, prev["theta"], self.theta_alert_threshold),
            ("vega", curr.vega, prev["vega"], self.vega_alert_threshold),
        ]
        
        for greek_name, new_val, old_val, threshold in greeks_to_check:
            if old_val == 0:
                pct_change = 1.0 if new_val != 0 else 0.0
            else:
                pct_change = abs((new_val - old_val) / old_val)
            
            if pct_change > threshold:
                alert = GreekAlert(
                    symbol=tick.symbol,
                    greek_name=greek_name,
                    old_value=old_val,
                    new_value=new_val,
                    change_pct=pct_change,
                    threshold=threshold,
                    timestamp=datetime.now(),
                )
                
                logger.warning(f"Alert for {tick.symbol}: {greek_name} changed {pct_change:.1%}")
                self.alerts.append(alert)
                
                if len(self.alerts) > self.max_alerts:
                    self.alerts.pop(0)
                
                return alert
        
        return None
    
    def calculate_portfolio_metrics(self) -> PortfolioMetrics:
        """Calculate aggregated portfolio Greeks and risk metrics."""
        metrics = PortfolioMetrics()
        
        if not self.positions:
            self.metrics_history.append(metrics)
            if len(self.metrics_history) > self.max_history:
                self.metrics_history.pop(0)
            return metrics
        
        for symbol, tick in self.positions.items():
            if not tick.greeks:
                continue
            
            metrics.total_delta += tick.greeks.delta
            metrics.total_gamma += tick.greeks.gamma
            metrics.total_theta += tick.greeks.theta
            metrics.total_vega += tick.greeks.vega
            metrics.total_rho += tick.greeks.rho
            
            metrics.num_options += 1
            metrics.total_notional_value += tick.mid * 100
            
            if tick.option_type == "call":
                metrics.num_calls += 1
            else:
                metrics.num_puts += 1
            
            if tick.greeks.delta > 0:
                metrics.long_delta += tick.greeks.delta
            else:
                metrics.short_delta += tick.greeks.delta
        
        metrics.timestamp = datetime.now()
        self.metrics_history.append(metrics)
        if len(self.metrics_history) > self.max_history:
            self.metrics_history.pop(0)
        
        return metrics
    
    def get_greeks_by_strike_and_expiration(self) -> Dict[float, Dict[str, Dict[str, float]]]:
        """Aggregate Greeks grouped by strike THEN expiration (2D matrix) for Heatmap."""
        by_strike_exp: Dict[float, Dict[str, Dict[str, float]]] = defaultdict(
            lambda: defaultdict(
                lambda: {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "rho": 0.0, "count": 0}
            )
        )
        
        for tick in self.positions.values():
            if not tick.greeks:
                continue
            
            strike = tick.strike
            exp = tick.expiration
            
            by_strike_exp[strike][exp]["delta"] += tick.greeks.delta
            by_strike_exp[strike][exp]["gamma"] += tick.greeks.gamma
            by_strike_exp[strike][exp]["theta"] += tick.greeks.theta
            by_strike_exp[strike][exp]["vega"] += tick.greeks.vega
            by_strike_exp[strike][exp]["rho"] += tick.greeks.rho
            by_strike_exp[strike][exp]["count"] += 1
        
        result = {}
        for strike, exp_dict in by_strike_exp.items():
            result[strike] = dict(exp_dict)
        
        return dict(sorted(result.items()))
    
    def get_alerts_since(self, minutes_ago: int = 60) -> List[GreekAlert]:
        """Get all alerts from the last N minutes."""
        cutoff_time = datetime.now() - timedelta(minutes=minutes_ago)
        return [a for a in self.alerts if a.timestamp >= cutoff_time]
    
    def get_historical_delta(self, limit: int = 100) -> pd.DataFrame:
        """Get historical portfolio delta as DataFrame."""
        if not self.metrics_history:
            return pd.DataFrame()
        
        data = []
        for metrics in self.metrics_history[-limit:]:
            data.append({
                "timestamp": metrics.timestamp,
                "delta": metrics.total_delta,
                "gamma": metrics.total_gamma,
                "theta": metrics.total_theta,
                "vega": metrics.total_vega,
            })
        
        return pd.DataFrame(data)
    
    def get_greeks_dataframe(self) -> pd.DataFrame:
        """Get all positions as DataFrame for easy analysis."""
        data = []
        
        for symbol, tick in self.positions.items():
            if not tick.greeks:
                continue
            
            data.append({
                "symbol": symbol,
                "strike": tick.strike,
                "expiration": tick.expiration,
                "type": tick.option_type,
                "bid": tick.bid,
                "ask": tick.ask,
                "mid": tick.mid,
                "delta": tick.greeks.delta,
                "gamma": tick.greeks.gamma,
                "theta": tick.greeks.theta,
                "vega": tick.greeks.vega,
                "rho": tick.greeks.rho,
                "price": tick.greeks.price,
                "iv": tick.implied_volatility,
            })
        
        # CRITICAL BUG FIX: Ensure columns exist even when empty
        if not data:
            return pd.DataFrame(columns=[
                "symbol", "strike", "expiration", "type", "bid", "ask", 
                "mid", "delta", "gamma", "theta", "vega", "rho", "price", "iv"
            ])
            
        return pd.DataFrame(data).sort_values(["strike", "expiration"])