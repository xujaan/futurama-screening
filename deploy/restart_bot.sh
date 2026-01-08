#!/bin/bash
# Logic 10: Daily Restart
sudo systemctl restart bybit_bot
echo "Bot Restarted at $(date)" >> /var/log/bot_restart.log