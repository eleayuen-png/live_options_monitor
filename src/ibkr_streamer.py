import asyncio
import logging
from datetime import datetime
from ib_insync import IB, Stock, Option

logger = logging.getLogger(__name__)

# --- 將 host 與 port 的預設值改為 ib-gateway 與 4002 ---
async def run_ibkr_streamer_background(watchlist: list, target_queue: asyncio.Queue, host='ib-gateway', port=4002, client_id=2):
    """
    Connects to IBKR TWS, dynamically requests Option Chains, 
    finds At-The-Money (ATM) options for the nearest expiration, 
    and streams live OPRA market data.
    """
    ib = IB()
    try:
        await ib.connectAsync(host, port, clientId=client_id)
        logger.info("✅ IBKR Market Data Streamer Connected!")
        
        # 1 = Live, 3 = Delayed (Force Live for Arbitrage)
        ib.reqMarketDataType(1) 
        
        active_contracts = []
        
        logger.info("🔍 Scanning IBKR for dynamic ATM options...")
        for symbol in watchlist:
            try:
                stock = Stock(symbol, 'SMART', 'USD')
                await ib.qualifyContractsAsync(stock)
                
                # 1. 取得底層股票現價
                [ticker] = await ib.reqTickersAsync(stock)
                spot_price = ticker.marketPrice()
                if spot_price != spot_price or spot_price == 0: 
                    logger.warning(f"⚠️ No spot price for {symbol}. Skipping.")
                    continue
                
                # 2. 動態獲取選擇權鏈 (Option Chain Parameters)
                chains = await ib.reqSecDefOptParamsAsync(stock.symbol, '', stock.secType, stock.conId)
                if not chains:
                    logger.warning(f"⚠️ No option chain found for {symbol}.")
                    continue
                    
                # 選擇 SMART 交易所的選擇權鏈
                chain = next((c for c in chains if c.exchange == 'SMART'), chains[0])
                
                # 3. 尋找最近的未來到期日 (Nearest Expiration)
                today_str = datetime.now().strftime('%Y%m%d')
                valid_expirations = sorted([exp for exp in chain.expirations if exp >= today_str])
                if not valid_expirations:
                    continue
                nearest_exp = valid_expirations[0] # 取最靠近今天的到期日
                
                # 4. 尋找最平價的履約價 (ATM Strike)
                strikes = sorted(chain.strikes)
                atm_strike = min(strikes, key=lambda x: abs(x - spot_price))
                
                # 5. 建立合約
                call = Option(symbol, nearest_exp, atm_strike, 'C', 'SMART')
                put = Option(symbol, nearest_exp, atm_strike, 'P', 'SMART')
                active_contracts.extend([call, put])
                
                logger.info(f"🎯 [{symbol}] Found ATM Strike: ${atm_strike} for Expiry: {nearest_exp}")
                
            except Exception as e:
                logger.error(f"Failed to build contracts for {symbol}: {e}")

        if not active_contracts:
            logger.error("❌ No valid options contracts built. Halting streamer.")
            return

        # 驗證所有建立的合約
        await ib.qualifyContractsAsync(*active_contracts)
        logger.info(f"📡 Subscribing to {len(active_contracts)} live option streams...")

        # 請求市場數據
        tickers = [ib.reqMktData(contract, '', False, False) for contract in active_contracts]

        # 無窮迴圈，持續將跳動的 Ticks 塞入 Queue 中給 Dashboard
        while True:
            await asyncio.sleep(0.5) 
            
            for ticker in tickers:
                if ticker.bid != ticker.bid or ticker.ask != ticker.ask or ticker.bid <= 0:
                    continue # 略過尚未取得報價的 Tick
                    
                mid = (ticker.bid + ticker.ask) / 2.0
                opt_type = "call" if ticker.contract.right == "C" else "put"
                exp_format = f"{ticker.contract.lastTradeDateOrContractMonth[:4]}-{ticker.contract.lastTradeDateOrContractMonth[4:6]}-{ticker.contract.lastTradeDateOrContractMonth[6:]}"
                
                tick = {
                    "type": "tick_update", 
                    "data": {
                        "symbol": f"{ticker.contract.symbol}{ticker.contract.lastTradeDateOrContractMonth[2:]}{ticker.contract.right}{int(ticker.contract.strike)}",
                        "strike": float(ticker.contract.strike),
                        "expiration": exp_format,
                        "option_type": opt_type,
                        "bid": float(ticker.bid),
                        "ask": float(ticker.ask),
                        "mid": float(mid),
                        "bid_size": int(ticker.bidSize) if ticker.bidSize else 0,
                        "ask_size": int(ticker.askSize) if ticker.askSize else 0,
                        "timestamp": datetime.now().isoformat(),
                        "underlying_price": ticker.modelGreeks.undPrice if ticker.modelGreeks else spot_price,
                        "implied_volatility": ticker.modelGreeks.impliedVol if ticker.modelGreeks else 0.20,
                        "greeks": {
                            "price": mid,
                            "delta": ticker.modelGreeks.delta if ticker.modelGreeks else 0.0,
                            "gamma": ticker.modelGreeks.gamma if ticker.modelGreeks else 0.0,
                            "theta": ticker.modelGreeks.theta if ticker.modelGreeks else 0.0,
                            "vega": ticker.modelGreeks.vega if ticker.modelGreeks else 0.0,
                            "rho": 0.0
                        }
                    }
                }
                await target_queue.put(tick)

    except Exception as e:
        logger.error(f"IBKR Streamer crashed: {e}")
    finally:
        ib.disconnect()