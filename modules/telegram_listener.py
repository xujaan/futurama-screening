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
        
        try:
            url = f"https://api.telegram.org/bot{self.token}/setMyCommands"
            commands = [
                {"command": "status", "description": "Tampilkan PnL Live & tombol tutup posisi"},
                {"command": "live", "description": "Lihat rekap trade dari Database"},
                {"command": "scan", "description": "Paksa bot melakukan screening pasar saat ini juga"},
                {"command": "reset", "description": "Hapus semua data riwayat screening di DB"},
                {"command": "autotrade", "description": "ON/OFF fitur Auto Trade"},
                {"command": "setcapital", "description": "Atur modal total trading"},
                {"command": "setkuota", "description": "Atur batas jumlah koin berjalan"},
                {"command": "statusrisk", "description": "Cek setelan Autotrade, Modal, Kuota"}
            ]
            requests.post(url, json={"commands": commands}, timeout=5)
        except Exception as e:
            print(f"Failed to register TG commands: {e}")
            
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
                            'SL': float(trade['sl_price']),
                            'TP3': float(trade['tp3']) if trade.get('tp3') else None,
                            'Total_Score': trade.get('tech_score', 0) + trade.get('smc_score', 0) + trade.get('quant_score', 0) + trade.get('deriv_score', 0)
                        }
                        
                        risk_cfg = get_risk_config()
                        active_pos_count = 0
                        try:
                            positions = self.exchange.fetch_positions()
                            active_pos_count = len([p for p in positions if float(p['contracts']) > 0])
                        except Exception as e: print("Gagal fetch pos limit:", e)
                        
                        if active_pos_count < risk_cfg.get('max_concurrent_trades', 2):
                            result = execute_entry(self.exchange, res)
                            if result:
                                def fmt_price(p): return f"{p:.8f}".rstrip('0').rstrip('.') if p < 1 else f"{p:.4f}"
                                reply = (
                                    f"✅ <b>TRADE LIMIT SUCCESS!</b>\n\n"
                                    f"🪙 <b>Symbol:</b> <code>{result['symbol']}</code>\n"
                                    f"🧭 <b>Mode:</b> <code>{result['side']}</code>\n"
                                    f"🎯 <b>Entry:</b> <code>{fmt_price(result['entry_price'])}</code>\n"
                                    f"📦 <b>Quantity:</b> <code>{result['qty']}</code>\n"
                                    f"🔩 <b>Leverage:</b> <code>{result['leverage']}x</code>\n"
                                    f"💵 <b>Margin Used:</b> <code>${result['margin']:.2f}</code>\n"
                                    f"💰 <b>Total Capital:</b> <code>${result['total_cap']:.2f}</code>\n"
                                    f"🛑 <b>Stop Loss:</b> <code>{fmt_price(result['sl'])}</code>\n"
                                    f"🩸 <b>Est. Liq Price:</b> <code>{fmt_price(result['liq_price'])}</code>\n"
                                    f"🛒 <b>Order ID:</b> <code>{result['order_id']}</code>"
                                )
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
                    
        elif data.startswith('endtrade_'):
            symbol = data.split('_', 1)[1]
            if not self.exchange:
                reply = "❌ Exchange is not initialized."
            else:
                from modules.execution import close_position
                from modules.database import get_conn, release_conn, get_dict_cursor
                success, msg = close_position(self.exchange, symbol)
                if success:
                    reply = f"✅ {msg}"
                    # Update DB
                    conn = get_conn()
                    try:
                        cur = get_dict_cursor(conn)
                        cur.execute("UPDATE trades SET status = 'Closed (Manual)' WHERE symbol = %s AND status NOT LIKE '%Closed%'", (symbol,))
                        conn.commit()
                    except Exception as e:
                        print(f"Error updating DB on manual close: {e}")
                    finally:
                        release_conn(conn)
                else:
                    reply = f"❌ {msg}"
                    
        elif data == 'confirmreset_true':
            from modules.database import get_conn, release_conn
            conn = get_conn()
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM trades")
                conn.commit()
                reply = "✅ **SUKSES!** Seluruh data screening dan histori posisi lama di database telah berhasil dibersihkan."
            except Exception as e:
                reply = f"❌ Gagal mereset database: {e}"
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
            
        elif cmd == '/live':
            from modules.database import get_conn, release_conn, get_dict_cursor
            conn = get_conn()
            lines = []
            try:
                cur = get_dict_cursor(conn)
                cur.execute("SELECT symbol, side, status, entry_hit_at, created_at FROM trades WHERE status NOT LIKE '%Closed%' ORDER BY created_at DESC")
                trades = cur.fetchall()
                def fmt_time(t_val):
                    if hasattr(t_val, 'strftime'): return t_val.strftime('%H:%M')
                    if isinstance(t_val, str) and len(t_val) >= 16: return t_val[11:16]
                    return str(t_val)
                lines = [f"`{fmt_time(t['entry_hit_at'] or t['created_at'])}` {'🟢' if 'Active' in t['status'] else '⏳'} **{t['symbol']}** ({t['side']}): {t['status']}" for t in trades]
            except Exception as e:
                reply = f"❌ Error fetching DB live status: {e}"
            finally:
                release_conn(conn)
            
            if lines:
                text_lines = "\n".join(lines)
                reply = "<b>📊 LIVE DASHBOARD (DB)</b>\n\n" + text_lines
            elif not reply:
                reply = "<b>📊 LIVE DASHBOARD (DB)</b>\n\n⚪ Tidak ada trade aktif/pending di Database."
            
        elif cmd == '/scan':
            reply = "🔍 **Memulai Scanning Pasar Manual...**\n\n*Bot sedang menyapu ratusan koin. Biarkan ia bekerja, jika ada sinyal profit, akan segera masuk ke sini!*"
            import threading
            def run_manual_scan():
                import main
                try:
                    main.scan()
                except Exception as e:
                    print(f"Manual scan error: {e}")
            threading.Thread(target=run_manual_scan, daemon=True).start()
            
        elif cmd == '/reset':
            keyboard = [[{"text": "⚠️ YA, HAPUS SEMUA DATA", "callback_data": "confirmreset_true"}]]
            import json
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            requests.post(url, json={
                'chat_id': chat_id, 
                'text': "Apakah Anda amat yakin ingin **menghapus SELURUH histori trade & antrean screening** di database?\n\n*Tindakan ini tidak bisa dibatalkan.*", 
                'parse_mode': 'Markdown',
                'reply_markup': json.dumps({"inline_keyboard": keyboard})
            })
            return
            
        elif cmd == '/status':
            if not self.exchange:
                reply = "❌ Exchange is not initialized."
            else:
                try:
                    positions = self.exchange.fetch_positions()
                    active_pos = [p for p in positions if float(p.get('contracts', 0)) > 0]
                    
                    if not active_pos:
                        reply = "⚪ Tidak ada posisi yang aktif saat ini."
                    else:
                        reply = "🟢 **DAFTAR POSISI AKTIF** 🟢\n\n"
                        keyboard = []
                        for p in active_pos:
                            sym = p['symbol']
                            side = p['side'].upper()
                            qty = p['contracts']
                            pnl = p.get('unrealizedPnl', 0)
                            if pnl is None: pnl = 0
                            icon = "🟩" if float(pnl) > 0 else "🟥"
                            
                            reply += f"{icon} **{sym}** ({side})\n"
                            reply += f"   • Qty: `{qty}`\n"
                            reply += f"   • Entry: `{p.get('entryPrice', 0)}`\n"
                            reply += f"   • UPL: `${float(pnl):.2f}`\n\n"
                            
                            keyboard.append([{"text": f"🛑 End {sym}", "callback_data": f"endtrade_{sym}"}])
                            
                        import json
                        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                        requests.post(url, json={
                            'chat_id': chat_id, 
                            'text': reply, 
                            'parse_mode': 'Markdown',
                            'reply_markup': json.dumps({"inline_keyboard": keyboard})
                        })
                        return # Reply handled
                except Exception as e:
                    reply = f"❌ Gagal mengambil status posisi: {e}"
            
        if reply:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            requests.post(url, json={'chat_id': chat_id, 'text': reply, 'parse_mode': 'Markdown'})
