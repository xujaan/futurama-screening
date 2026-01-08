# Bybit Quant Trading Bot v8.0

An institutional-grade, modular cryptocurrency trading bot designed for the Bybit Exchange. This bot leverages a hybrid analysis engine combining geometric pattern recognition, quantitative metrics (RVOL, OBI, VPIN), and derivative market data (CVD, Open Interest, Funding Rates) to identify high-probability setups.

It features a robust production architecture with PostgreSQL persistence, real-time Discord dashboards, and systemd integration.

## 🚀 Key Features

### 🧠 Advanced Analysis Engine
* **Geometric Pattern Recognition:** Automatically detects Double Tops/Bottoms, Bull/Bear Flags, Ascending Triangles, and Rectangles.
* **Market Context Awareness:**
  * **BTC Bias Filter:** Uses Daily EMA 13/21 crosses to determine trend direction.
  * **Trap Detection:** Identifies "Dead Cat Bounces" and "Bullish Exhaustion" (RSI > 75) to prevent bad entries.
* **Divergence Logic:** Scans for Regular and Hidden divergences on Stochastic RSI and CVD (Cumulative Volume Delta).

### 🛡️ Risk Management & Quant Logic
* **Dynamic Risk:Reward:** Calculates Entry, Stop Loss, and Take Profits (TP1-3) using Swing High/Low Fibonacci retracements.
  * *Enforces a minimum Risk:Reward ratio (Configurable, default 1:3).*
* **Fakeout Protection:** Rejects valid patterns if Relative Volume (RVOL) is below threshold (default 2.0).
* **Derivative Filters:**
  * Skips Longs if Funding Rate is overheated (> 0.02%).
  * Analyzes Basis (Spot vs. Perp premium).

### ⚙️ Production Infrastructure
* **Modular Architecture:** Split logic for maintainability (`technicals`, `derivatives`, `quant`, `patterns`).
* **Database Persistence:** Uses **PostgreSQL** to store trade history, prevent duplicate signals, and manage state.
* **Discord Integration:**
  * **Rich Alerts:** Sends chart screenshots with TP targets, detailed scoring explanations, and market bias.
  * **Live Dashboard:** Auto-updating message showing active PnL and open positions.
* **DevOps Ready:** Includes `systemd` service files and daily auto-restart scripts (Cron) for memory management.

---

## 📂 Directory Structure

```text
/bybit_bot_v8
├── config.json             # Main configuration (Strategy, API, Webhooks)
├── .env                    # Environment variables (DB Creds, Secrets)
├── main.py                 # Core Orchestrator
├── requirements.txt        # Python Dependencies
├── /modules
│   ├── config_loader.py    # Environment handling (Prod/Test)
│   ├── database.py         # Connection pooling & Schema migration
│   ├── technicals.py       # Indicators & Divergence logic
│   ├── derivatives.py      # Funding, Basis, CVD analysis
│   ├── quant.py            # RVOL, OBI, Fakeout logic
│   ├── patterns.py         # Pattern recognition algorithms
│   └── discord_bot.py      # Chart generation & Alerting
└── /deploy
    ├── bot.service         # Systemd service file
    └── restart_bot.sh      # Cron restart script
```
## 🛠️ Installation & Setup

### 1. Prerequisites
* **Python 3.8+**
* **PostgreSQL Database**
* **Bybit API Keys** (Permissions: Read-Write for Orders/Positions)
* **Discord Webhook URLs**

### 2. Clone & Install
```bash
# Clone the repository
git clone [https://github.com/yourusername/bybit_bot_v8.git](https://github.com/yourusername/bybit_bot_v8.git)
cd bybit_bot_v8

# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```
### 3. Database Setup
The bot requires a PostgreSQL database to store trade history and manage state.

**1. Access the PostgreSQL prompt:**
```bash
sudo -u postgres psql
```
### 4. Configuration
Create a `config.json` file in the root directory. This controls your strategy, risk settings, and API keys.

**Copy and paste this template:**
```json
{
    "api": {
        "discord_webhook": "[https://discord.com/api/webhooks/YOUR_WEBHOOK_URL](https://discord.com/api/webhooks/YOUR_WEBHOOK_URL)",
        "discord_live_webhook": "[https://discord.com/api/webhooks/YOUR_LIVE_WEBHOOK_URL](https://discord.com/api/webhooks/YOUR_LIVE_WEBHOOK_URL)",
        "discord_dashboard_webhook": "[https://discord.com/api/webhooks/YOUR_DASHBOARD_WEBHOOK_URL](https://discord.com/api/webhooks/YOUR_DASHBOARD_WEBHOOK_URL)",
        "bybit_key": "YOUR_BYBIT_API_KEY",
        "bybit_secret": "YOUR_BYBIT_API_SECRET",
        "discord_server_id": "YOUR_DISCORD_SERVER_ID",
        "discord_role_id": "YOUR_DISCORD_ROLE_ID_TO_MENTION"
    },
    "database": {
        "host": "localhost",
        "database": "bybit_bot",
        "user": "postgres",
        "password": "PrinceOfRed78@!",
        "port": "5432"
    },
    "system": {
        "timezone": "Asia/Jakarta",
        "max_threads": 20,
        "check_interval_hours": 1,
        "timeframes": ["1h", "4h", "1d", "1w"]
    },
    "setup": {
        "fib_entry_start": 0.5,
        "fib_entry_end": 0.618,
        "fib_sl": 0.27,
        "fib_tp_1": 1.0,
        "fib_tp_2": 1.618,
        "fib_tp_3": 2.618
    },
    "strategy": {
        "min_tech_score": 5,
        "min_quant_score": 3,
        "risk_reward_min": 3.0
    },
    "indicators": {
        "min_rvol": 2.0
    },
    "patterns": {
        "double_top": true,
        "double_bottom": true,
        "bull_flag": true,
        "bear_flag": true,
        "ascending_triangle": true,
        "bullish_rectangle": true
    },
    "pattern_signals": {
        "double_bottom": "Long",
        "double_top": "Short",
        "bull_flag": "Long",
        "bear_flag": "Short",
        "ascending_triangle": "Long",
        "bullish_rectangle": "Long"
    }
}```

## 🚀 Deployment (Linux/Systemd)

### 1. Setup Service
This allows the bot to run in the background and automatically restart if it crashes or the server reboots.

```bash
# Copy the service file to the systemd directory
sudo cp deploy/bot.service /etc/systemd/system/bybit_bot.service

# Reload the systemd daemon to recognize the new service
sudo systemctl daemon-reload

# Enable the service to start on boot
sudo systemctl enable bybit_bot

# Start the bot immediately
sudo systemctl start bybit_bot

# Check the status to ensure it's running
sudo systemctl status bybit_bot
```
### 2. Setup Daily Restart (Cron)
To keep the bot fresh and clear memory, set up a daily restart at 00:00.

Bash

# Open the crontab editor
crontab -e

# Add the following line at the bottom of the file:
0 0 * * * /opt/bybit_bot_v8/deploy/restart_bot.sh

## 📊 Logic Overview

### Scoring System
The bot assigns a score to every potential trade. A setup must meet the `min_tech_score` (default 5) to trigger.

* **Base Score:** 3 points (Valid Pattern Found).
* **Technicals:** +/- 2 for Stochastic Divergence.
* **Quant:** +1 for Nuclear RVOL (>5.0), +1 for OBI Imbalance.
* **Derivatives:** +1 for Cool Funding, +/- 2 for CVD Divergence.
* **Context:** +1 if setup aligns with BTC Daily Bias.

### Trade Lifecycle
1.  **Scan:** Multithreaded scan of top 400 pairs on Bybit.
2.  **Filter:** Checks BTC Bias, "ST" tags, and Fakeout logic.
3.  **Validate:** Calculates Risk:Reward. If < 3.0, trade is dropped.
4.  **Alert:** Sends chart and details to Discord.
5.  **Track:** Inserts into DB as `Waiting Entry`.
6.  **Monitor:** `run_fast_update` runs every minute to check if Entry/TP/SL is hit.
7.  **Update:** Updates Discord Dashboard in real-time.

---

## ⚠️ Disclaimer
This software is for educational purposes only. Cryptocurrency trading involves significant risk. The authors are not responsible for any financial losses incurred while using this bot. Use at your own risk.