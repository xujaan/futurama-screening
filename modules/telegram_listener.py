import time
import requests
import threading
from modules.database import set_risk_config, get_risk_config
from modules.config_loader import CONFIG

class TelegramListener:
    def __init__(self, exchange=None):
        self.token = CONFIG['api'].get('telegram_bot_token')
        self.offset = 0
        self.running = False
        self.exchange = exchange
        
    def start(self):
        if not self.token: return
        self.running = True
        self.thread = threading.Thread(target=self.poll, daemon=True)
        self.thread.start()
        print("🤖 Telegram Command Listener Started.")
        
    def stop(self):
        self.running = False
        
    def poll(self):
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        while self.running:
            try:
                r = requests.get(url, params={'offset': self.offset, 'timeout': 10}, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    for update in data.get('result', []):
                        self.offset = update['update_id'] + 1
                        if 'callback_query' in update:
                            self.handle_callback(update['callback_query'])
                            continue
                            
                        msg = update.get('message', {})
                        text = msg.get('text', '')
                        chat_id = msg.get('chat', {}).get('id')
                        
                        if text and chat_id:
                            self.handle_command(text, chat_id)
            except: pass
            time.sleep(2)
            
    def handle_callback(self, callback_query):
        from modules.database import get_conn, release_conn, get_risk_config, get_dict_cursor
        callback_id = callback_query.get('id')
        data = callback_query.get('data', '')
        msg = callback_query.get('message', {})
        chat_id = msg.get('chat', {}).get('id')
        
        reply = ""
        if data.startswith('trade_'):
            symbol = data[6:]
            if not self.exchange:
                reply = "❌ Exchange is not initialized."
            else:
                conn = get_conn()
                try:
                    cur = get_dict_cursor(conn)
                    cur.execute("SELECT * FROM trades WHERE symbol = %s AND status = 'Waiting Entry' ORDER BY created_at DESC LIMIT 1", (symbol,))
                    trade = cur.fetchone()
                    
                    if trade:
                        from modules.execution import execute_entry
                        res = {
                            'Symbol': trade['symbol'],
                            'Side': trade['side'],
                            'Entry': float(trade['entry_price']),
                            'SL': float(trade['sl_price'])
                        }
                        
                        risk_cfg = get_risk_config()
                        active_pos_count = 0
                        try:
                            positions = self.exchange.fetch_positions()
                            active_pos_count = len([p for p in positions if float(p['contracts']) > 0])
                        except Exception as e: print("Gagal fetch pos limit:", e)
                        
                        if active_pos_count < risk_cfg.get('max_concurrent_trades', 2):
                            success = execute_entry(self.exchange, res)
                            if success:
                                reply = f"✅ Trade LIMIT order for {symbol} placed successfully!"
                            else:
                                reply = f"❌ Failed to place order for {symbol}."
                        else:
                            reply = f"❌ Trade limit reached ({active_pos_count}/{risk_cfg.get('max_concurrent_trades', 2)})"
                    else:
                        reply = f"❌ No 'Waiting Entry' found for {symbol}. (Maybe already processed)"
                except Exception as e:
                    reply = f"❌ DB Error: {e}"
                finally:
                    release_conn(conn)
                    
        if reply and chat_id:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            requests.post(url, json={'chat_id': chat_id, 'text': reply, 'parse_mode': 'HTML'})
            
        ans_url = f"https://api.telegram.org/bot{self.token}/answerCallbackQuery"
        requests.post(ans_url, json={'callback_query_id': callback_id})

    def handle_command(self, text, chat_id):
        parts = text.split()
        cmd = parts[0].lower()
        
        reply = ""
        if cmd == '/setcapital' and len(parts) > 1:
            try:
                val = float(parts[1])
                if set_risk_config('total_trading_capital_usdt', val):
                    reply = f"✅ Modal Total Trading berhasil diatur ke: **${val}**"
            except: reply = "❌ Format salah. Contoh: /setcapital 10"
            
        elif cmd == '/setkuota' and len(parts) > 1:
            try:
                val = int(parts[1])
                if set_risk_config('max_concurrent_trades', val):
                    reply = f"✅ Maksimal Koin Bersamaan berhasil diatur ke: **{val}** koin"
            except: reply = "❌ Format salah. Contoh: /setkuota 2"
            
        elif cmd == '/autotrade' and len(parts) > 1:
            val = parts[1].lower()
            if val in ['on', 'off']:
                if set_risk_config('auto_trade', val):
                    reply = f"✅ Auto Trade otomatis **{'DIHIDUPKAN' if val == 'on' else 'DIMATIKAN'}**"
            else: reply = "❌ Format salah. Contoh: /autotrade on"
            
        elif cmd == '/statusrisk':
            cfg = get_risk_config()
            reply = f"📊 **STATUS RISK & MODAL** 📊\n\n"
            reply += f"🤖 Auto Trade: **{'ON' if cfg['auto_trade'] else 'OFF'}**\n"
            reply += f"💰 Modal Total: **${cfg['total_trading_capital_usdt']}**\n"
            reply += f"🛑 Limit Koin Bersamaan: **{cfg['max_concurrent_trades']}** koin"
            
        if reply:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            requests.post(url, json={'chat_id': chat_id, 'text': reply, 'parse_mode': 'Markdown'})
