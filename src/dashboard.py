"""
Streamlit Live Options Dashboard - IBKR Autonomous OMS Edition
"""

import sys
import os
import asyncio

# --- CRITICAL FIX: ASYNCIO PATCH MUST BE AT THE VERY TOP ---
# ib_insync (via eventkit) grabs the event loop at the exact moment of import.
# We must create the loop for Streamlit's thread BEFORE importing any IBKR modules.
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())
# -----------------------------------------------------------

import logging
import queue
import threading
import time
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo 

# Fix ModuleNotFoundError
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv

# --- IBKR NATIVE IMPORTS ---
# Now it is safe to import these because the event loop exists!
from src.streamer import OptionTick 
from src.ibkr_streamer import run_ibkr_streamer_background
from src.portfolio import Portfolio
from src.pricer import GreeksResult, OptionPricer
from src.strategy import VolatilityArbitrage, DynamicVolatilityEngine
from src.ibkr_execution import IBKRExecutionEngine

# Asyncio Patch for Windows/IBKR
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Quant Delta Monitor", layout="wide", page_icon="📊")

def is_us_market_open() -> bool:
    ny_time = datetime.now(ZoneInfo("America/New_York"))
    if ny_time.weekday() > 4: return False
    market_open = ny_time.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = ny_time.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= ny_time <= market_close

def init_session_state():
    if "portfolio" not in st.session_state:
        st.session_state.portfolio = Portfolio()
    if "update_queue" not in st.session_state:
        st.session_state.update_queue = queue.Queue()
    if "streamer_active" not in st.session_state:
        st.session_state.streamer_active = False
        st.session_state.selected_mode = None
        st.session_state.running_mode = None
        
    if "executor" not in st.session_state:
        # Initialize IBKR Execution Engine (Client ID 1)
        st.session_state.executor = IBKRExecutionEngine(
            host='ib-gateway', # <-- 改為 Docker 內的 Gateway
            port=4002,         # <-- 改為 Paper Trading 的 Port
            client_id=1,
            telegram_token=os.getenv("TELEGRAM_TOKEN", ""),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "")
        )

def process_queue():
    updates = 0
    while not st.session_state.update_queue.empty():
        try:
            update = st.session_state.update_queue.get_nowait()
            if update["type"] == "tick_update":
                data = update["data"]
                tick = OptionTick(
                    symbol=data["symbol"], strike=data["strike"], expiration=data["expiration"],
                    option_type=data["option_type"], bid=data["bid"], ask=data["ask"],
                    mid=data["mid"], bid_size=data["bid_size"], ask_size=data["ask_size"],
                    timestamp=datetime.fromisoformat(data["timestamp"]),
                    underlying_price=data["underlying_price"], implied_volatility=data["implied_volatility"]
                )
                if data.get("greeks"):
                    g = data["greeks"]
                    tick.greeks = GreeksResult(price=g["price"], delta=g["delta"], gamma=g["gamma"], theta=g["theta"], vega=g["vega"], rho=g["rho"])
                
                st.session_state.portfolio.update_position(tick)
                
                if "strategy" in st.session_state:
                    signal = st.session_state.strategy.generate_signal(tick)
                    if signal != "HOLD":
                        st.session_state.executor.execute_option_signal(symbol=tick.symbol, signal=signal, qty=1, limit_price=tick.mid)
            updates += 1
        except queue.Empty: break
            
    metrics = st.session_state.portfolio.calculate_portfolio_metrics()
    if abs(metrics.total_delta) > 50.0:
        st.session_state.executor.hedge_delta("SPY", metrics.total_delta, threshold=50.0)
    return updates

def start_live_streamer(watchlist, target_queue, stop_event):
    """Spawns the IBKR WebSocket on a separate thread."""
    def thread_target():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        async_queue = asyncio.Queue()
        
        async def queue_bridge():
            while not stop_event.is_set():
                try: target_queue.put(await asyncio.wait_for(async_queue.get(), timeout=1.0))
                except asyncio.TimeoutError: continue
                except Exception: break
                    
        loop.create_task(queue_bridge())
        try:
            # RUN THE IBKR NATIVE STREAMER (Client ID 2)
            loop.run_until_complete(run_ibkr_streamer_background(watchlist, async_queue, client_id=2))
        except Exception as e:
            logger.error(f"IBKR Streamer died: {e}")

    threading.Thread(target=thread_target, daemon=True).start()

def start_mock_streamer(watchlist, target_queue, stop_event):
    def thread_target():
        pricer = OptionPricer()
        base_prices = {sym: random.uniform(100, 500) for sym in watchlist}
        while not stop_event.is_set():
            for symbol in watchlist:
                base_prices[symbol] += random.uniform(-0.5, 0.5) 
                base = base_prices[symbol]
                strike = round(base / 5) * 5
                exp = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
                for opt_type in ["call", "put"]:
                    iv = random.uniform(0.15, 0.25)
                    greeks = pricer.calculate_greeks(base, float(strike), 30/365, iv, opt_type)
                    target_queue.put({"type": "tick_update", "data": {
                        "symbol": f"{symbol}{exp.replace('-', '')[2:]}{opt_type[0].upper()}{int(strike)}",
                        "strike": float(strike), "expiration": exp, "option_type": opt_type,
                        "bid": 2.0, "ask": 2.1, "mid": 2.05, "bid_size": 10, "ask_size": 10,
                        "timestamp": datetime.now().isoformat(), "underlying_price": base, "implied_volatility": iv,
                        "greeks": greeks.to_dict()
                    }})
            time.sleep(2)
    threading.Thread(target=thread_target, daemon=True).start()

init_session_state()

st.sidebar.title("🎮 IBKR Control Panel")
universe_input = st.sidebar.text_area("Tickers to Monitor", value="SPY, QQQ, NVDA, TSLA, GLD, TLT, USO")
watchlist = [s.strip().upper() for s in universe_input.split(",") if s.strip()]

data_mode = st.sidebar.radio("📡 Data Source", ["Auto Switch (Market Hours)", "Live Market (IBKR)", "Simulation (24/7)"])

col1, col2 = st.sidebar.columns(2)
if col1.button("▶️ Start", disabled=st.session_state.get('streamer_active', False)):
    st.session_state.streamer_active = True
    st.session_state.stop_event = threading.Event()
    st.session_state.selected_mode = data_mode
    
    try:
        with st.spinner("Calculating 30-Day Realized Volatility..."):
            # --- CRITICAL FIX ---
            # 絕對不要在這裡傳入 st.session_state.executor.ib
            # 因為策略引擎現在會自己建立獨立的背景連線！
            rv_engine = DynamicVolatilityEngine()
            
            st.session_state.strategy = VolatilityArbitrage(rv_engine=rv_engine, symbols=watchlist)
            
            # 呼叫初始化函數，讓它去 IBKR 下載歷史數據
            if hasattr(st.session_state.strategy, 'initialize'):
                st.session_state.strategy.initialize()
                
        st.sidebar.success("Historical RVs Cached!")
    except Exception as e:
        st.sidebar.error(f"Failed to calculate RV: {e}")
        st.session_state.streamer_active = False # 如果失敗，把按鈕狀態重置
        
    # Determine the actual mode to launch right now
        target_mode = "Live Market (IBKR)" if is_us_market_open() else "Simulation (24/7)"
    else:
        target_mode = data_mode
        
    st.session_state.running_mode = target_mode
    
    # 2. 啟動正確的數據流
    if target_mode == "Live Market (IBKR)":
        st.session_state.executor.is_simulation = False
        start_live_streamer(watchlist, st.session_state.update_queue, st.session_state.stop_event)
        st.sidebar.success("Connecting to IBKR WebSocket...")
    else:
        st.session_state.executor.is_simulation = True
        start_mock_streamer(watchlist, st.session_state.update_queue, st.session_state.stop_event)
        st.sidebar.success("Started Simulation...")
    st.rerun()

if col2.button("⏹️ Stop", disabled=not st.session_state.streamer_active):
    if "stop_event" in st.session_state: st.session_state.stop_event.set()
    st.session_state.streamer_active = False
    st.session_state.portfolio = Portfolio() 
    while not st.session_state.update_queue.empty(): st.session_state.update_queue.get_nowait()
    st.rerun()

st.sidebar.divider()
live_update = st.sidebar.checkbox("Live Refresh UI", value=True)

st.title("📊 Institutional OMS & Risk Monitor")

if st.session_state.streamer_active:
    if st.session_state.running_mode == "Live Market (IBKR)":
        st.success("🟢 LIVE: Connected to IBKR TWS OPRA Options Exchange.")
    else:
        st.info("🟡 SIMULATION: Market is closed or simulation forced.")

process_queue()

metrics = st.session_state.portfolio.calculate_portfolio_metrics()
cols = st.columns(4)
cols[0].metric("Net Portfolio Delta", f"{metrics.total_delta:.2f}")
cols[1].metric("Portfolio Gamma", f"{metrics.total_gamma:.4f}")
cols[2].metric("Daily Theta Decay", f"${metrics.total_theta * 100:.2f}")
cols[3].metric("Active Positions", metrics.num_options)

st.subheader("🌐 Real-Time Volatility Surface (3D)")
greeks_df = st.session_state.portfolio.get_greeks_dataframe()

if not greeks_df.empty and len(greeks_df) > 3:
    fig_3d = go.Figure(data=[go.Mesh3d(x=greeks_df['strike'], y=pd.to_datetime(greeks_df['expiration']).astype(int) / 10**9, z=greeks_df['iv'], intensity=greeks_df['iv'], colorscale='Viridis', opacity=0.8)])
    fig_3d.update_layout(scene=dict(xaxis_title='Strike', yaxis_title='Expiry', zaxis_title='IV'), height=500, margin=dict(l=0, r=0, b=0, t=0))
    st.plotly_chart(fig_3d, use_container_width=True)
else:
    st.info("💡 Waiting for IBKR Data stream... (Ensure TWS is open)")

st.subheader("📋 Active Options Stream")
if not greeks_df.empty:
    st.dataframe(greeks_df, use_container_width=True, hide_index=True)

if live_update and st.session_state.streamer_active:
    time.sleep(2)
    st.rerun()