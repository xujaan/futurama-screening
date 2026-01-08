Bybit Quant Trading Bot v8.0
An institutional-grade, modular cryptocurrency trading bot designed for the Bybit Exchange. This bot leverages a hybrid analysis engine combining geometric pattern recognition, quantitative metrics (RVOL, OBI, VPIN), and derivative market data (CVD, Open Interest, Funding Rates) to identify high-probability setups.

It features a robust production architecture with PostgreSQL persistence, real-time Discord dashboards, and systemd integration.

🚀 Key Features
🧠 Advanced Analysis Engine
Geometric Pattern Recognition: Automatically detects Double Tops/Bottoms, Bull/Bear Flags, Ascending Triangles, and Rectangles.

Market Context Awareness:

BTC Bias Filter: Uses Daily EMA 13/21 crosses to determine trend direction.

Trap Detection: Identifies "Dead Cat Bounces" and "Bullish Exhaustion" (RSI > 75) to prevent bad entries.

Divergence Logic: Scans for Regular and Hidden divergences on Stochastic RSI and CVD (Cumulative Volume Delta).

🛡️ Risk Management & Quant Logic
Dynamic Risk:Reward: Calculates Entry, Stop Loss, and Take Profits (TP1-3) using Swing High/Low Fibonacci retracements.

Enforces a minimum Risk:Reward ratio (Configurable, default 1:3).

Fakeout Protection: Rejects valid patterns if Relative Volume (RVOL) is below threshold (default 2.0).

Derivative Filters:

Skips Longs if Funding Rate is overheated (> 0.02%).

Analyzes Basis (Spot vs. Perp premium).

⚙️ Production Infrastructure
Modular Architecture: Split logic for maintainability (technicals, derivatives, quant, patterns).

Database Persistence: Uses PostgreSQL to store trade history, prevent duplicate signals, and manage state.

Discord Integration:

Rich Alerts: Sends chart screenshots with TP targets, detailed scoring explanations, and market bias.

Live Dashboard: Auto-updating message showing active PnL and open positions.

DevOps Ready: Includes systemd service files and daily auto-restart scripts (Cron) for memory management.

📂 Directory Structure
Plaintext

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
🛠️ Installation & Setup
1. Prerequisites
Python 3.8+

PostgreSQL Database

Bybit API Keys (Read/Trade permissions)

Discord Webhooks

2. Clone & Install
Bash

git clone https://github.com/yourusername/bybit_bot_v8.git
cd bybit_bot_v8
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
3. Database Setup
Create a PostgreSQL database (the bot handles table creation automatically).

SQL

CREATE DATABASE bybit_bot;
CREATE USER postgres WITH PASSWORD 'YourPassword';
GRANT ALL PRIVILEGES ON DATABASE bybit_bot TO postgres;
4. Configuration
Create a config.json file in the root directory:

JSON

{
    "api": {
        "discord_webhook": "https://discord.com/api/webhooks/...",
        "discord_live_webhook": "https://discord.com/api/webhooks/...",
        "discord_dashboard_webhook": "https://discord.com/api/webhooks/...",
        "bybit_key": "YOUR_BYBIT_API_KEY",
        "bybit_secret": "YOUR_BYBIT_SECRET",
        "discord_role_id": "123456789"
    },
    "database": {
        "host": "localhost",
        "database": "bybit_bot",
        "user": "postgres",
        "password": "YourPassword",
        "port": "5432"
    },
    "system": {
        "timezone": "Asia/Jakarta",
        "max_threads": 20,
        "check_interval_hours": 1,
        "timeframes": ["1h", "4h", "1d"]
    },
    "strategy": {
        "min_tech_score": 5,
        "risk_reward_min": 3.0
    }
    // ... (See full config example in documentation)
}
Create a .env file (optional, for environment overriding):

Ini, TOML

BOT_ENV=production
🚀 Deployment (Linux/Systemd)
1. Setup Service
Copy the service file to systemd:

Bash

sudo cp deploy/bot.service /etc/systemd/system/bybit_bot.service
sudo systemctl daemon-reload
sudo systemctl enable bybit_bot
sudo systemctl start bybit_bot
2. Setup Daily Restart (Cron)
To keep the bot fresh and clear memory, setup a daily restart at 00:00.

Bash

crontab -e
# Add the following line:
0 0 * * * /opt/bybit_bot_v8/deploy/restart_bot.sh
📊 Logic Overview
Scoring System
The bot assigns a score to every potential trade. A setup must meet the min_tech_score (default 5) to trigger.

Base Score: 3 points (Valid Pattern Found).

Technicals: +/- 2 for Stochastic Divergence.

Quant: +1 for Nuclear RVOL (>5.0), +1 for OBI Imbalance.

Derivatives: +1 for Cool Funding, +/- 2 for CVD Divergence.

Context: +1 if setup aligns with BTC Daily Bias.

Trade Lifecycle
Scan: Multithreaded scan of top 400 pairs on Bybit.

Filter: Checks BTC Bias, "ST" tags, and Fakeout logic.

Validate: Calculates Risk:Reward. If < 3.0, trade is dropped.

Alert: Sends chart and details to Discord.

Track: Inserts into DB as Waiting Entry.

Monitor: run_fast_update runs every minute to check if Entry/TP/SL is hit.

Update: Updates Discord Dashboard in real-time.

⚠️ Disclaimer
This software is for educational purposes only. Cryptocurrency trading involves significant risk. The authors are not responsible for any financial losses incurred while using this bot. Use at your own risk.