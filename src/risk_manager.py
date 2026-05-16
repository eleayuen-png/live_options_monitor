"""
Risk Manager — dollar-delta/gamma calculations, hedging hierarchy, multiplier cache.

The portfolio's Greek totals are summed *per-contract*. To convert to dollar
exposure we need:
    Option Exposure  = greek_value * qty * contract_multiplier
    Dollar Delta     = sum(Option_Delta_per_contract * qty * multiplier * underlying_price)
    Dollar Gamma     = sum(Option_Gamma_per_contract * qty * multiplier * underlying_price^2)

`contract.multiplier` is fetched once via `ib.reqContractDetailsAsync` and cached
so the per-tick math doesn't hit the API.
"""

import asyncio
import logging
import math
from typing import Dict, List, Optional, Tuple

from ib_insync import IB, Stock, Option, Contract

logger = logging.getLogger(__name__)

# Hierarchy of hedging instruments
HEDGE_HIERARCHY = ["SPY", "QQQ", "IWM"]

# Gamma circuit-breaker threshold as a fraction of NLV
GAMMA_LIMIT_PCT = 0.05  # 5% of NLV


class RiskManager:
    def __init__(self, ib: IB):
        self.ib = ib
        self._multiplier_cache: Dict[str, float] = {}
        self._underlying_price_cache: Dict[str, float] = {}

    # ---------- multiplier cache ----------

    async def get_multiplier(self, contract: Contract) -> float:
        """Resolve contract.multiplier once and cache it. Options default to 100."""
        key = self._cache_key(contract)
        if key in self._multiplier_cache:
            return self._multiplier_cache[key]

        # If ib_insync already populated it (qualifyContracts often does), use that
        mult_raw = getattr(contract, "multiplier", None)
        if mult_raw:
            try:
                mult = float(mult_raw)
                self._multiplier_cache[key] = mult
                return mult
            except (TypeError, ValueError):
                pass

        # Otherwise ask the gateway
        try:
            details = await asyncio.wait_for(
                self.ib.reqContractDetailsAsync(contract), timeout=5
            )
            if details and getattr(details[0].contract, "multiplier", None):
                mult = float(details[0].contract.multiplier)
            else:
                mult = 100.0  # equity option default
            self._multiplier_cache[key] = mult
            return mult
        except Exception as e:
            logger.warning(f"multiplier lookup failed for {key}: {e}; defaulting to 100")
            self._multiplier_cache[key] = 100.0
            return 100.0

    @staticmethod
    def _cache_key(contract: Contract) -> str:
        if isinstance(contract, Option):
            return f"OPT:{contract.symbol}:{contract.lastTradeDateOrContractMonth}:{contract.strike}:{contract.right}"
        if isinstance(contract, Stock):
            return f"STK:{contract.symbol}"
        return f"{contract.secType}:{contract.symbol}"

    # ---------- dollar exposure math ----------

    def dollar_delta(self, positions: List[dict]) -> float:
        """
        positions = [{"delta": float, "qty": int, "multiplier": float, "underlying_price": float}, ...]
        """
        total = 0.0
        for p in positions:
            total += p["delta"] * p["qty"] * p["multiplier"] * p["underlying_price"]
        return total

    def dollar_gamma(self, positions: List[dict]) -> float:
        total = 0.0
        for p in positions:
            total += p["gamma"] * p["qty"] * p["multiplier"] * (p["underlying_price"] ** 2)
        return total

    def gamma_utilization(self, dollar_gamma_val: float, nlv: float) -> Tuple[float, bool]:
        """Return (utilization_pct, breached) where breached = utilization > 100%."""
        if nlv <= 0:
            return 0.0, False
        limit = nlv * GAMMA_LIMIT_PCT
        util = abs(dollar_gamma_val) / limit if limit > 0 else 0.0
        return util * 100.0, util > 1.0

    # ---------- hedge hierarchy ----------

    async def select_hedge_instrument(self, fallback_symbol: Optional[str] = None) -> Optional[Stock]:
        """
        Walk SPY → QQQ → IWM. If none qualify (extremely unlikely), fall back
        to the underlying of the largest position (caller supplies symbol).
        """
        for sym in HEDGE_HIERARCHY:
            try:
                stock = Stock(sym, "SMART", "USD")
                qualified = await asyncio.wait_for(self.ib.qualifyContractsAsync(stock), timeout=5)
                if qualified and qualified[0].conId:
                    return qualified[0]
            except Exception as e:
                logger.warning(f"hedge instrument {sym} unavailable: {e}")
                continue

        if fallback_symbol:
            try:
                stock = Stock(fallback_symbol, "SMART", "USD")
                qualified = await asyncio.wait_for(self.ib.qualifyContractsAsync(stock), timeout=5)
                if qualified and qualified[0].conId:
                    logger.warning(f"Falling back to largest-position underlying for hedge: {fallback_symbol}")
                    return qualified[0]
            except Exception:
                pass
        return None

    async def compute_hedge_qty(self, dollar_delta_val: float, hedge_stock: Stock) -> int:
        """Number of shares of `hedge_stock` to bring portfolio dollar-delta toward zero."""
        try:
            [ticker] = await asyncio.wait_for(self.ib.reqTickersAsync(hedge_stock), timeout=5)
            price = ticker.marketPrice()
            if not price or price != price or price <= 0:
                logger.warning(f"hedge instrument {hedge_stock.symbol} has invalid market price; aborting hedge")
                return 0
        except Exception as e:
            logger.warning(f"Failed to fetch hedge price for {hedge_stock.symbol}: {e}")
            return 0
        # Divide dollar-delta by hedge price; round toward zero so we end inside the boundary
        shares = math.floor(abs(dollar_delta_val) / price)
        # Sign of action determined later by caller; here we just return magnitude
        return shares
