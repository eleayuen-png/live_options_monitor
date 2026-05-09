# Quick Start Guide

## 5-Minute Setup

### 1. Install Dependencies
```bash
cd live_options_monitor
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Add Alpaca Credentials
```bash
# Create .env file with your Alpaca API keys
cat > .env << EOF
ALPACA_API_KEY=your_key_here
ALPACA_SECRET_KEY=your_secret_here
EOF
```

**Get your API key:**
1. Sign up: https://app.alpaca.markets
2. Go to Dashboard → API Keys
3. Copy your key and secret

### 3. Run Dashboard
```bash
streamlit run src/dashboard.py
```

Your browser will open at `http://localhost:8501`

## Dashboard Quick Reference

| Feature | What It Does |
|---------|-------------|
| **Watchlist** | Enter stock tickers (AAPL, TSLA, SPY) |
| **Start Streamer** | Begin monitoring options for those symbols |
| **Portfolio Greeks** | See total Delta, Gamma, Theta, Vega, Rho |
| **Alerts** | Real-time notifications for 15%+ Delta moves |
| **Live Options** | Bid/Ask prices, Greeks per contract |
| **Greeks Heatmap** | Visual matrix: Strike × Expiration |
| **Historical Charts** | Delta/Theta trends over 5-min to 2-hour window |

## Example Workflow

### Step 1: Start Monitoring
```
1. In sidebar, enter: AAPL
2. Click "▶️ Start Streamer"
3. Wait 5-10 seconds for Alpaca to stream options data
```

### Step 2: Watch Real-Time Updates
```
- Portfolio Greeks update as market moves
- Red alerts appear when Greeks change significantly
- Historical chart builds up data points
```

### Step 3: Analyze Risk
```
- Click on any Strike to see Call/Put breakdown
- Check expiration bucketing
- Use heatmap to spot concentration risk
```

## Common Tasks

### Monitor Multiple Underlyings
```
Sidebar Watchlist:
AAPL
TSLA
SPY
QQQ

Then start streamer
```

### View Last 1 Hour of History
```
Sidebar → Select "60 minutes" from dropdown
Charts will show 60-minute window
```

### Check for Alerts
```
Look for red boxes at top of dashboard
Each shows:
- Option symbol
- Which Greek changed
- Percentage change
- Time of alert
```

### Export Data
```python
# In Python script:
from src.portfolio import Portfolio
portfolio = Portfolio()
df = portfolio.get_greeks_dataframe()
df.to_csv('greeks_snapshot.csv')
```

## Architecture at a Glance

```
You (UI)
    ↓
Streamlit Dashboard (main thread)
    ↓
    ├─→ Start Button
    │       ↓
    │   Creates background thread
    │
    └─→ Updates from Queue
            ↑
            │
    Background Thread (async WebSocket)
            │
            ├─→ Connects to Alpaca
            ├─→ Gets bid/ask quotes
            ├─→ Calculates Greeks
            ├─→ Puts in Queue
            └─→ Repeats 10-100x per second
```

## Troubleshooting

### "Credentials not found" Error
```
Make sure .env file exists with:
ALPACA_API_KEY = your_actual_key
ALPACA_SECRET_KEY = your_actual_secret
```

### Streamer Won't Start
```
1. Check API keys are valid
2. Check internet connection
3. Alpaca API might be down (try: alpaca.markets/status)
```

### No Options Appearing
```
1. Check that watchlist symbols are valid (e.g., AAPL, not APPLE)
2. Options may not exist for that symbol (try large-cap: SPY, TSLA)
3. Market may be closed (options only stream during market hours)
```

### Dashboard Showing "0 Positions"
```
- Streamer takes 10-30 seconds to connect and stream data
- Wait and click "🔄 Refresh" manually
- Or check "Auto-refresh" for continuous updates
```

## Performance Tips

- **100 positions**: No lag, smooth updates
- **500+ positions**: May slow down; consider filtering by strike/expiration
- **Historical charts**: Keep to <2 hours for best performance
- **Auto-refresh**: Disable for faster manual control

## Next Steps

### 1. Customize Alert Thresholds
Edit `.env`:
```
DELTA_ALERT_THRESHOLD=0.20  # 20% change
GAMMA_ALERT_THRESHOLD=0.05  # 5% change
```

### 2. Analyze Strategy
```python
# Get current portfolio state
portfolio = st.session_state.portfolio
metrics = portfolio.calculate_portfolio_metrics()
print(f"Delta: {metrics.total_delta}")  # Net directional exposure
print(f"Gamma: {metrics.total_gamma}")  # Convexity risk
print(f"Theta: {metrics.total_theta}")  # Time decay per day
```

### 3. Build Alerts Integration
```python
# Hook into GreekAlert system
recent_alerts = portfolio.get_alerts_since(60)  # Last 60 minutes
for alert in recent_alerts:
    print(f"{alert.symbol}: {alert.greek_name} +{alert.change_pct:.1%}")
    # Could send to Slack, email, SMS, etc.
```

## Running Tests

```bash
# All tests
pytest tests/ -v

# Just pricer (Black-Scholes math)
pytest tests/test_pricer.py -v

# Just streamer (symbol parsing, mock data)
pytest tests/test_streamer.py -v

# With coverage report
pytest tests/ --cov=src --cov-report=html
```

## Key Equations

### Black-Scholes Call Price
```
C = S₀ × N(d₁) - K × e^(-rT) × N(d₂)

where:
d₁ = [ln(S/K) + (r + σ²/2)T] / (σ√T)
d₂ = d₁ - σ√T
```

### Delta (Directional Risk)
```
Call: Δ = N(d₁)        (0 to +1)
Put:  Δ = N(d₁) - 1    (-1 to 0)
```

### Gamma (Convexity)
```
Γ = N'(d₁) / (S × σ × √T)
(Same for calls and puts; always positive)
```

### Theta (Daily Decay)
```
Call: Θ = -S × N'(d₁) × σ/(2√T) - r × K × e^(-rT) × N(d₂)
Put:  Θ = -S × N'(d₁) × σ/(2√T) + r × K × e^(-rT) × N(-d₂)
(Per day, so divide by 365)
```

### Vega (Volatility Sensitivity)
```
ν = S × N'(d₁) × √T
(Per 1% change in volatility)
```

## Useful Links

- **Alpaca Docs**: https://docs.alpaca.markets
- **Black-Scholes**: https://en.wikipedia.org/wiki/Black–Scholes_model
- **Streamlit Docs**: https://docs.streamlit.io/
- **Plotly Charts**: https://plotly.com/python/

## Support & Contributing

Found a bug? Want to add features?
- Check test coverage: `pytest tests/ --cov=src`
- Verify Greeks against benchmarks: See `tests/test_pricer.py`
- Symbol parsing working? See `tests/test_streamer.py`

---

**Happy monitoring! 📊**
