"""
Trade Tracker — persistence, cooldowns, partial-fill consolidation, monthly CSV history.

Responsibilities:
- Persist active option positions to `data/trades.json` for crash recovery.
- Append every trade to `data/trade_history_YYYY_MM.csv` (monthly rollover).
- Consolidate intraday partial fills into ONE row using VWAP for price and the
  LAST fill's Greeks/IV/timestamp. Fills spanning multiple days are logged as
  separate daily rows.
- Enforce trade cooldowns: 300 s per option-contract, 300 s portfolio-wide for hedge orders.
- Track pending orders so we never double-fire on the same contract.
- Provide a 15-minute "re-peg" check for unfilled TP limit orders.
"""

import csv
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

ALPHA_COOLDOWN_S = 300.0   # per-contract cooldown for new option orders
HEDGE_COOLDOWN_S = 300.0   # portfolio-wide cooldown for stock hedge orders
REPEG_INTERVAL_S = 15 * 60  # 15 minutes between TP limit re-pegs


@dataclass
class FillAccumulator:
    """Accumulates partial fills for a single order until consolidation."""
    order_id: int
    symbol: str
    action: str                 # "BUY" / "SELL"
    contract_kind: str          # "option" / "stock"
    fills: List[Dict[str, Any]] = field(default_factory=list)   # [{qty, price, time}]
    last_greeks: Dict[str, float] = field(default_factory=dict)
    last_iv: float = 0.0
    last_underlying_price: float = 0.0
    first_fill_day: Optional[str] = None   # YYYY-MM-DD in local TZ for daily rollover

    def add_fill(self, qty: float, price: float, fill_time: datetime,
                 greeks: Optional[Dict[str, float]] = None,
                 iv: float = 0.0, underlying_price: float = 0.0) -> bool:
        """Returns True if this fill triggered a day-rollover (caller should flush prior fills)."""
        day_str = fill_time.strftime("%Y-%m-%d")
        rolled = False
        if self.first_fill_day is None:
            self.first_fill_day = day_str
        elif self.first_fill_day != day_str:
            rolled = True
        self.fills.append({"qty": float(qty), "price": float(price), "time": fill_time.isoformat()})
        if greeks:
            self.last_greeks = greeks
        if iv:
            self.last_iv = float(iv)
        if underlying_price:
            self.last_underlying_price = float(underlying_price)
        return rolled

    def vwap_price(self) -> float:
        total_qty = sum(f["qty"] for f in self.fills)
        if total_qty == 0:
            return 0.0
        return sum(f["qty"] * f["price"] for f in self.fills) / total_qty

    def total_qty(self) -> float:
        return sum(f["qty"] for f in self.fills)

    def last_fill_time(self) -> datetime:
        if not self.fills:
            return datetime.now(timezone.utc)
        return datetime.fromisoformat(self.fills[-1]["time"])

    def reset_to_today(self):
        """Reset accumulator state for the next day (multi-day spanning trades)."""
        self.fills = []
        self.first_fill_day = None


@dataclass
class ActiveTrade:
    """A position currently held, with TP order tracking."""
    symbol: str
    action: str                 # entry action: "BUY_TO_OPEN"
    qty: int
    entry_price: float          # VWAP of entry fills
    entry_time: str             # ISO timestamp
    contract_spec: Dict[str, Any]   # underlying, strike, expiry, right, multiplier
    tp_order_id: Optional[int] = None
    tp_limit_price: Optional[float] = None
    tp_last_repeg_ts: Optional[float] = None   # epoch seconds


class TradeTracker:
    """Single source of truth for trade state and history."""

    def __init__(self, data_dir: str, local_tz_offset_hours: int = 8):
        self.data_dir = data_dir
        self.trades_path = os.path.join(data_dir, "trades.json")
        self.local_tz_offset_hours = local_tz_offset_hours
        self._lock = threading.RLock()

        self.active: Dict[str, ActiveTrade] = {}
        self.last_order_ts: Dict[str, float] = {}     # contract symbol → epoch
        self.last_hedge_ts: float = 0.0
        self.pending_order_ids: Dict[int, str] = {}   # orderId → contract symbol
        self._fill_accumulators: Dict[int, FillAccumulator] = {}

        os.makedirs(data_dir, exist_ok=True)
        self._load()

    # ---------- persistence ----------

    def _load(self):
        if not os.path.exists(self.trades_path):
            return
        try:
            with open(self.trades_path, "r") as f:
                state = json.load(f)
            for sym, t in state.get("active", {}).items():
                self.active[sym] = ActiveTrade(**t)
            self.last_order_ts = state.get("last_order_ts", {})
            self.last_hedge_ts = state.get("last_hedge_ts", 0.0)
            logger.info(f"TradeTracker loaded {len(self.active)} active trades from {self.trades_path}")
        except Exception as e:
            logger.error(f"Failed to load trades.json (continuing fresh): {e}")

    def _save(self):
        try:
            tmp = self.trades_path + ".tmp"
            with open(tmp, "w") as f:
                json.dump({
                    "active": {s: asdict(t) for s, t in self.active.items()},
                    "last_order_ts": self.last_order_ts,
                    "last_hedge_ts": self.last_hedge_ts,
                }, f, indent=2)
            os.replace(tmp, self.trades_path)
        except Exception as e:
            logger.error(f"Failed to save trades.json: {e}")

    # ---------- cooldowns ----------

    def can_open_alpha(self, contract_symbol: str) -> bool:
        with self._lock:
            last = self.last_order_ts.get(contract_symbol, 0.0)
            ok = (time.time() - last) >= ALPHA_COOLDOWN_S
            return ok

    def can_hedge(self) -> bool:
        with self._lock:
            return (time.time() - self.last_hedge_ts) >= HEDGE_COOLDOWN_S

    def mark_alpha_attempt(self, contract_symbol: str):
        with self._lock:
            self.last_order_ts[contract_symbol] = time.time()
            self._save()

    def mark_hedge_attempt(self):
        with self._lock:
            self.last_hedge_ts = time.time()
            self._save()

    # ---------- pending order tracking ----------

    def has_pending(self, contract_symbol: str) -> bool:
        with self._lock:
            return contract_symbol in self.pending_order_ids.values()

    def register_pending(self, order_id: int, contract_symbol: str):
        with self._lock:
            self.pending_order_ids[order_id] = contract_symbol

    def clear_pending(self, order_id: int):
        with self._lock:
            self.pending_order_ids.pop(order_id, None)

    # ---------- active trades ----------

    def add_active(self, trade: ActiveTrade):
        with self._lock:
            self.active[trade.symbol] = trade
            self._save()

    def remove_active(self, contract_symbol: str):
        with self._lock:
            self.active.pop(contract_symbol, None)
            self._save()

    def get_active(self, contract_symbol: str) -> Optional[ActiveTrade]:
        return self.active.get(contract_symbol)

    def list_active(self) -> List[ActiveTrade]:
        return list(self.active.values())

    def update_tp_repeg(self, contract_symbol: str, new_limit: float, new_order_id: int):
        with self._lock:
            t = self.active.get(contract_symbol)
            if t is None:
                return
            t.tp_order_id = new_order_id
            t.tp_limit_price = new_limit
            t.tp_last_repeg_ts = time.time()
            self._save()

    def needs_repeg(self, contract_symbol: str) -> bool:
        t = self.active.get(contract_symbol)
        if t is None or t.tp_order_id is None:
            return False
        last = t.tp_last_repeg_ts or 0.0
        return (time.time() - last) >= REPEG_INTERVAL_S

    # ---------- partial fill consolidation ----------

    def begin_order(self, order_id: int, contract_symbol: str, action: str,
                    contract_kind: str = "option"):
        with self._lock:
            self._fill_accumulators[order_id] = FillAccumulator(
                order_id=order_id, symbol=contract_symbol,
                action=action, contract_kind=contract_kind,
            )

    def record_fill(self, order_id: int, qty: float, price: float, fill_time: datetime,
                    greeks: Optional[Dict[str, float]] = None,
                    iv: float = 0.0, underlying_price: float = 0.0):
        """Add one partial fill. If it crosses a day boundary, flush prior fills first."""
        with self._lock:
            acc = self._fill_accumulators.get(order_id)
            if acc is None:
                logger.warning(f"record_fill: no accumulator for order {order_id}")
                return
            rolled = acc.add_fill(qty, price, fill_time, greeks, iv, underlying_price)
            if rolled:
                # write the previous day's chunk, then start fresh for the new day
                # NB: rolled means the NEW fill is on a new day; flush the prior fills first
                pending_fill = acc.fills.pop()
                self._flush_consolidated_row(acc)
                acc.reset_to_today()
                acc.add_fill(qty=pending_fill["qty"], price=pending_fill["price"],
                             fill_time=datetime.fromisoformat(pending_fill["time"]),
                             greeks=greeks, iv=iv, underlying_price=underlying_price)

    def finalize_order(self, order_id: int):
        """Order is fully filled or cancelled — write consolidated row and drop the accumulator."""
        with self._lock:
            acc = self._fill_accumulators.pop(order_id, None)
            if acc is None or not acc.fills:
                return
            self._flush_consolidated_row(acc)

    def _flush_consolidated_row(self, acc: FillAccumulator):
        """Write one CSV row representing this consolidated set of fills."""
        if not acc.fills:
            return
        last_time = acc.last_fill_time()
        local_time = last_time.astimezone(timezone(timedelta(hours=self.local_tz_offset_hours)))
        row = {
            "timestamp_local": local_time.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_utc": last_time.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "action": acc.action,
            "symbol": acc.symbol,
            "contract_kind": acc.contract_kind,
            "qty": acc.total_qty(),
            "vwap_price": round(acc.vwap_price(), 4),
            "delta": acc.last_greeks.get("delta", 0.0) * acc.total_qty(),
            "gamma": acc.last_greeks.get("gamma", 0.0) * acc.total_qty(),
            "vega": acc.last_greeks.get("vega", 0.0) * acc.total_qty(),
            "iv": acc.last_iv,
            "underlying_price": acc.last_underlying_price,
            "num_fills": len(acc.fills),
        }
        self._append_history(local_time, row)

    def _history_path_for(self, dt_local: datetime) -> str:
        return os.path.join(self.data_dir, f"trade_history_{dt_local.strftime('%Y_%m')}.csv")

    def _append_history(self, dt_local: datetime, row: Dict[str, Any]):
        path = self._history_path_for(dt_local)
        is_new = not os.path.exists(path)
        try:
            with open(path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                if is_new:
                    writer.writeheader()
                writer.writerow(row)
            logger.info(f"📒 trade row appended → {path}: {row['action']} {row['qty']} {row['symbol']} @ {row['vwap_price']}")
        except Exception as e:
            logger.error(f"Failed to append trade history: {e}")
