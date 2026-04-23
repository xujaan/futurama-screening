import time
import threading
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from modules.database import set_risk_config, get_risk_config, set_active_cex, get_active_cex
from modules.config_loader import CONFIG
import telegramify_markdown

class TelegramListener:
    def __init__(self, exchange=None):
        self.token = CONFIG['api'].get('telegram_bot_token')
        self.exchange = exchange
        self.running = False
        
        if self.token:
            self.bot = telebot.TeleBot(self.token)
            self._register_handlers()
        else:
            self.bot = None
            
    def _register_handlers(self):
        
        # --- COMMANDS ---
        
        @self.bot.message_handler(commands=['cex'])
        def cmd_cex(message):
            parts = message.text.split()
            if len(parts) > 1:
                val = parts[1].lower()
                if val in ['binance', 'bitget', 'bybit']:
                    if set_active_cex(val):
                        import main
                        main.SCAN_ABORT_FLAG = True
                        from modules.exchange_manager import get_current_exchange
                        self.exchange = get_current_exchange(force_reload=True) 
                        reply = f"✅ **Platform Switched Successfully**\nBot is now scanning and trading entirely on **{val.upper()}**.\n*(Note: Make sure your keys are mapped in config.json)*"
                    else:
                        reply = "❌ Failed to update active CEX in DB."
                else:
                    reply = "❌ Invalid platform. Provide `bybit`, `binance`, or `bitget`."
            else:
                reply = "❌ Format error. Example: `/cex binance`"
            self.safesend(message.chat.id, reply)

        @self.bot.message_handler(commands=['setcapital'])
        def cmd_setcapital(message):
            parts = message.text.split()
            if len(parts) > 1:
                try:
                    val = float(parts[1])
                    if set_risk_config('total_trading_capital_usdt', val):
                        reply = f"✅ Trading Capital Set To: **${val}**"
                except: reply = "❌ Format error. Example: `/setcapital 10`"
            else: reply = "❌ Format error. Example: `/setcapital 10`"
            self.safesend(message.chat.id, reply)

        @self.bot.message_handler(commands=['setquota'])
        def cmd_setquota(message):
            parts = message.text.split()
            if len(parts) > 1:
                try:
                    val = int(parts[1])
                    if set_risk_config('max_concurrent_trades', val):
                        reply = f"✅ Maximum Concurrent Pair Set To: **{val}** pairs"
                except: reply = "❌ Format error. Example: `/setquota 2`"
            else: reply = "❌ Format error. Example: `/setquota 2`"
            self.safesend(message.chat.id, reply)

        @self.bot.message_handler(commands=['autotrade'])
        def cmd_autotrade(message):
            parts = message.text.split()
            if len(parts) > 1:
                val = parts[1].lower()
                if val in ['on', 'off']:
                    if set_risk_config('auto_trade', val):
                        reply = f"✅ Auto Trade Mode **{'ENABLED' if val == 'on' else 'DISABLED'}**"
                else: reply = "❌ Format error. Example: `/autotrade on`"
            else: reply = "❌ Format error. Example: `/autotrade on`"
            self.safesend(message.chat.id, reply)

        @self.bot.message_handler(commands=['statusrisk'])
        def cmd_statusrisk(message):
            cfg = get_risk_config()
            reply = f"📊 **RISK MANAGER STATUS** 📊\n\n"
            reply += f"🏢 Current Node: **{get_active_cex().upper()}**\n"
            reply += f"🤖 Auto Trade: **{'ON' if cfg['auto_trade'] else 'OFF'}**\n"
            reply += f"💰 Trading Pool: **${cfg['total_trading_capital_usdt']}**\n"
            reply += f"🛑 Slot Ceiling: **{cfg['max_concurrent_trades']}** active pairs"
            self.safesend(message.chat.id, reply)

        @self.bot.message_handler(commands=['live'])
        def cmd_live(message):
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
                lines = [f"[{fmt_time(t['entry_hit_at'] or t['created_at'])}] {'🟢' if 'Active' in t['status'] else '⏳'} {t['symbol'].split(':')[0]} ({t['side']}): {t['status']}" for t in trades]
            except Exception as e: reply = f"❌ Error fetching DB: {e}"
            finally: release_conn(conn)
            
            if lines: 
                block = "\n".join(lines)
                reply = f"**📊 LIVE DASHBOARD (DB)**\n\n```text\n{block}\n```"
            else:
                reply = "**📊 LIVE DASHBOARD (DB)**\n\n```text\n⚪ No active or pending trades mapped.\n```"
            self.safesend(message.chat.id, reply)

        @self.bot.message_handler(commands=['scan'])
        def cmd_scan(message):
            import main
            if main.AUTOSCAN_ENABLED:
                main.AUTOSCAN_ENABLED = False
                main.SCAN_ABORT_FLAG = True
                sent_msg = self.safesend_sync(message.chat.id, "⚠️ **Autoscan Disabled & Aborted.** Firing manual scan in 2 seconds...")
                time.sleep(2)
            else:
                sent_msg = self.safesend_sync(message.chat.id, "⏳ Firing manual algorithm scan cycle...")
            main.SCAN_ABORT_FLAG = False
            
            def prog_cb(text):
                if sent_msg:
                    try:
                        self.bot.edit_message_text(
                            chat_id=message.chat.id, 
                            message_id=sent_msg.message_id, 
                            text=telegramify_markdown.markdownify(text), 
                            parse_mode='MarkdownV2'
                        )
                    except: pass
            
            def run_manual_scan():
                try: main.scan(prog_cb)
                except Exception as e: prog_cb(f"❌ System Fault: {e}")
            threading.Thread(target=run_manual_scan, daemon=True).start()

        @self.bot.message_handler(commands=['start'])
        def cmd_start(message):
            import main
            if main.AUTOSCAN_ENABLED:
                self.safesend(message.chat.id, "⚠️ **Autoscan is already running.**")
            else:
                main.AUTOSCAN_ENABLED = True
                main.SCAN_ABORT_FLAG = False
                self.safesend(message.chat.id, "✅ **Autoscan STARTED.**\nBot will now scan continuously automatically.\nType /stop to halt it.")

        @self.bot.message_handler(commands=['stop'])
        def cmd_stop(message):
            import main
            main.AUTOSCAN_ENABLED = False
            main.SCAN_ABORT_FLAG = True
            self.safesend(message.chat.id, "🛑 **Autoscan STOPPED & Abort Signal Sent.** Any active scans will halt instantly.")
            
        @self.bot.message_handler(commands=['autoscan'])
        def cmd_autoscan(message):
            import main
            parts = message.text.split()
            if len(parts) > 1:
                val = parts[1].lower()
                if val == 'on':
                    cmd_start(message)
                elif val == 'off':
                    cmd_stop(message)
                else:
                    self.safesend(message.chat.id, "❌ Usage: /autoscan on|off")
            else:
                state = "ON" if main.AUTOSCAN_ENABLED else "OFF"
                self.safesend(message.chat.id, f"🔄 Autoscan is currently **{state}**.\nUsage: /autoscan on|off")

        @self.bot.message_handler(commands=['pending'])
        def cmd_pending(message):
            if not self.exchange: reply = "❌ Exchange architecture empty."
            else:
                try:
                    open_orders = self.exchange.fetch_open_orders()
                    if not open_orders: reply = f"⚪ No active limit queues on {get_active_cex().title()}"
                    else:
                        block = ""
                        for o in open_orders:
                            sym = o['symbol'].split(':')[0]
                            side = o['side'].upper()
                            qty = o['amount']
                            price = o['price']
                            block += f"{sym} ({side})\n"
                            block += f" ├ Size: {qty}\n └ Bid : {price}\n\n"
                            if len(block) > 3500:
                                block += "...(Truncated)...\n"
                                break
                        reply = f"⏳ **BROKER QUEUE ({get_active_cex().title()})** ⏳\n\n```text\n{block}\n```"
                except Exception as e: reply = f"❌ Fetch limits failed: {e}"
            self.safesend(message.chat.id, reply)

        @self.bot.message_handler(commands=['reset'])
        def cmd_reset(message):
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("⚠️ PROCEED WIPE", callback_data="confirmreset_true"))
            text = "⚠️ **WARNING:** This purely purges old screening histories.\n\nYour Active Auto Trades will **NOT** be erased."
            self.bot.send_message(message.chat.id, telegramify_markdown.markdownify(text), parse_mode='MarkdownV2', reply_markup=markup)

        @self.bot.message_handler(commands=['balance'])
        def cmd_balance(message):
            if not self.exchange: reply = "❌ Exchange engine disjointed."
            else:
                try:
                    active_cex = get_active_cex().title()
                    b = self.exchange.fetch_balance()
                    total = float(b['total'].get('USDT', 0))
                    free = float(b['free'].get('USDT', 0))
                    used = float(b['used'].get('USDT', 0))
                    reply = f"🏦 **{active_cex} UNIFIED WALLET (USDT)** 🏦\n\n"
                    reply += f"💵 **Total Equity:** `${total:.2f}`\n"
                    reply += f"🛡️ **Margin Used:** `${used:.2f}`\n"
                    reply += f"🟢 **Available:** `${free:.2f}`\n"
                except Exception as e: reply = f"❌ Fetch Balance failed: {e}"
            self.safesend(message.chat.id, reply)

        @self.bot.message_handler(commands=['log'])
        def cmd_log(message):
            from modules.database import get_conn, release_conn, get_dict_cursor
            conn = get_conn()
            try:
                cur = get_dict_cursor(conn)
                cur.execute("SELECT type, message, created_at FROM system_logs ORDER BY created_at DESC LIMIT 10")
                logs = cur.fetchall()
                if not logs: reply = "⚪ No system logs mapped yet."
                else:
                    block = ""
                    for lg in logs:
                        dt = str(lg['created_at'])[11:19]
                        block += f"[{dt}] {lg['type']}\n > {lg['message']}\n"
                    reply = f"📜 **SYSTEM LOGS (Last 10)**\n\n```text\n{block}\n```"
            except Exception as e: reply = f"❌ Fetch logs failed: {e}"
            finally: release_conn(conn)
            self.safesend(message.chat.id, reply)

        @self.bot.message_handler(commands=['fav'])
        def cmd_fav(message):
            from modules.database import get_conn, release_conn, get_dict_cursor
            conn = get_conn()
            try:
                cur = get_dict_cursor(conn)
                cur.execute("SELECT symbol, side, timeframe, pattern, entry_price FROM favorites_list ORDER BY added_at DESC LIMIT 15")
                favs = cur.fetchall()
                if not favs: reply = "⚪ You have no favorites saved."
                else:
                    block = ""
                    for f in favs:
                        block += f"⭐ {f['symbol']} ({f['side']})\n"
                        block += f" ├ TF: {f['timeframe']} [{f['pattern']}]\n └ Entry: {f['entry_price']}\n\n"
                    reply = f"🌟 **SAVED FAVORITES** 🌟\n\n```text\n{block}\n```"
            except Exception as e: reply = f"❌ Fetch favorites failed: {e}"
            finally: release_conn(conn)
            self.safesend(message.chat.id, reply)

        @self.bot.message_handler(commands=['status'])
        def cmd_status(message):
            if not self.exchange: reply = "❌ Exchange engine disjointed."
            else:
                try:
                    positions = self.exchange.fetch_positions()
                    active_pos = [p for p in positions if float(p.get('contracts', 0)) > 0]
                    if not active_pos:
                        reply = f"⚪ Zero exposure on {get_active_cex().title()}"
                        self.safesend(message.chat.id, reply)
                    else:
                        reply = f"🟢 **MARKET POSITIONS ({get_active_cex().title()})** 🟢\n\n"
                        markup = InlineKeyboardMarkup()
                        for p in active_pos:
                            sym = p['symbol']
                            side = p['side'].upper()
                            qty = float(p.get('contracts', 0))
                            pnl = float(p.get('unrealizedPnl', 0) or 0)
                            entry_price = float(p.get('entryPrice', 1))
                            
                            # Coba ambil real margin dari info
                            margin_usd = float(p.get('initialMargin', 0) or 0)
                            if margin_usd == 0 and 'info' in p:
                                margin_usd = float(p['info'].get('positionMargin', 0) or 0)
                            
                            # Fallback
                            if margin_usd == 0 and qty > 0 and entry_price > 0:
                                margin_usd = (qty * entry_price) / 25 # Assume 25x
                                
                            pct = (pnl / margin_usd * 100) if margin_usd > 0 else 0
                                
                            icon = "🟩" if pnl > 0 else "🟥"
                            reply += f"{icon} **{sym}** (`{side}`)\n"
                            reply += f"   • Size: `{qty}`\n"
                            reply += f"   • Margin: `${margin_usd:.2f}`\n"
                            reply += f"   • B. Entry: `{entry_price}`\n"
                            reply += f"   • Est uNL: `${pnl:.2f} ({pct:.2f}%)`\n\n"
                            markup.add(InlineKeyboardButton(f"🛑 Kill {sym}", callback_data=f"endtrade_{sym}"))
                        
                        self.bot.send_message(message.chat.id, telegramify_markdown.markdownify(reply), parse_mode='MarkdownV2', reply_markup=markup)
                except Exception as e: 
                    self.safesend(message.chat.id, f"❌ Socket link fault: {e}")

        # --- CALLBACKS ---
        
        @self.bot.callback_query_handler(func=lambda call: call.data.startswith('trade_'))
        def call_trade(call):
            symbol = call.data[6:]
            reply = ""
            if not self.exchange: reply = "❌ Exchange is not initialized."
            else:
                from modules.database import get_conn, release_conn, get_dict_cursor, get_risk_config
                conn = get_conn()
                try:
                    cur = get_dict_cursor(conn)
                    cur.execute("SELECT * FROM trades WHERE symbol = ? AND status = 'Waiting Entry' ORDER BY created_at DESC LIMIT 1", (symbol,))
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
                            active_pos_count = len([p for p in positions if float(p.get('contracts', 0)) > 0])
                        except: pass
                        
                        if active_pos_count < risk_cfg.get('max_concurrent_trades', 2):
                            result = execute_entry(self.exchange, res)
                            if result:
                                def fmt_price(p): return f"{p:.8f}".rstrip('0').rstrip('.') if p < 1 else f"{p:.4f}"
                                reply = (
                                    f"✅ **TRADE LIMIT SUCCESS!**\n\n"
                                    f"🪙 **Symbol:** `{result['symbol']}`\n"
                                    f"🧭 **Mode:** `{result['side']}`\n"
                                    f"🎯 **Entry:** `{fmt_price(result['entry_price'])}`\n"
                                    f"📦 **Quantity:** `{result['qty']}`\n"
                                    f"🔩 **Leverage:** `{result['leverage']}x`\n"
                                    f"💵 **Margin Used:** `${result['margin']:.2f}`\n"
                                    f"🛑 **Stop Loss:** `{fmt_price(result['sl'])}`\n"
                                    f"🛒 **Order ID:** `{result['order_id']}`"
                                )
                                try:
                                    cur.execute("""
                                        INSERT INTO active_trades (signal_id, symbol, side, entry_price, sl_price, tp1, tp2, tp3, quantity, leverage, order_id, status)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
                                    """, (trade['id'], result['symbol'], trade['side'], result['entry_price'], result['sl'], trade['tp1'], trade['tp2'], trade['tp3'], result['qty'], result['leverage'], result['order_id']))
                                    conn.commit()
                                except Exception as e: print(f"Active trades insert err: {e}")
                            else: reply = f"❌ Failed to place order for {symbol}."
                        else: reply = f"❌ Trade limit reached ({active_pos_count}/{risk_cfg.get('max_concurrent_trades', 2)})"
                    else: reply = f"❌ No 'Waiting Entry' found for {symbol}."
                except Exception as e: reply = f"❌ DB Error: {e}"
                finally: release_conn(conn)
            
            if reply: self.safesend(call.message.chat.id, reply)
            self.bot.answer_callback_query(call.id)

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith('endtrade_'))
        def call_endtrade(call):
            symbol = call.data.split('_', 1)[1]
            reply = ""
            if not self.exchange: reply = "❌ Exchange is not initialized."
            else:
                from modules.execution import close_position
                from modules.database import get_conn, release_conn, get_dict_cursor, log_action
                success, msg_response = close_position(self.exchange, symbol)
                if success:
                    reply = f"✅ **{msg_response}**"
                    conn = get_conn()
                    try:
                        cur = get_dict_cursor(conn)
                        cur.execute("UPDATE trades SET status = 'Closed (Manual)' WHERE symbol = ? AND status NOT LIKE '%Closed%'", (symbol,))
                        conn.commit()
                        log_action('MANUAL_CLOSE', f"User manually closed position for {symbol}")
                    except: pass
                    finally: release_conn(conn)
                else: 
                    log_action('MANUAL_CLOSE_ERROR', f"Failed to close {symbol}: {msg_response}")
                    reply = f"❌ **{msg_response}**"
            
            if reply: self.safesend(call.message.chat.id, reply)
            self.bot.answer_callback_query(call.id)

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith('fav_'))
        def call_fav(call):
            symbol = call.data[4:]
            from modules.database import get_conn, release_conn, get_dict_cursor
            conn = get_conn()
            reply = ""
            try:
                cur = get_dict_cursor(conn)
                cur.execute("SELECT * FROM trades WHERE symbol = ? ORDER BY created_at DESC LIMIT 1", (symbol,))
                trade = cur.fetchone()
                if trade:
                    cur.execute("INSERT INTO favorites_list (symbol, side, timeframe, pattern, entry_price) VALUES (?, ?, ?, ?, ?)", (trade['symbol'], trade['side'], trade['timeframe'], trade['pattern'], trade['entry_price']))
                    conn.commit()
                    reply = f"⭐ Pinned **{symbol}** ({trade['side']}) to your Favorites! Use /fav to view."
                else: reply = f"❌ Cannot find recent screening for {symbol}."
            except Exception as e: reply = f"❌ DB Error: {e}"
            finally: release_conn(conn)
            
            if reply: self.safesend(call.message.chat.id, reply)
            self.bot.answer_callback_query(call.id)

        @self.bot.callback_query_handler(func=lambda call: call.data == 'confirmreset_true')
        def call_confirmreset(call):
            from modules.database import get_conn, release_conn, log_action
            conn = get_conn()
            reply = ""
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM trades")
                conn.commit()
                log_action('SCREENING_RESET', 'User wiped the screening histories.')
                reply = "✅ **SUCCESS!** Screening histories have been wiped from DB. Active Trades remain untouched."
            except Exception as e: reply = f"❌ Failed to wipe db: {e}"
            finally: release_conn(conn)
            
            if reply: self.safesend(call.message.chat.id, reply)
            self.bot.answer_callback_query(call.id)
            
    def safesend(self, chat_id, text):
        if not text: return
        try:
            self.bot.send_message(chat_id, telegramify_markdown.markdownify(text), parse_mode='MarkdownV2')
        except Exception as e: print(f"Telebot Error: {e}")
        
    def safesend_sync(self, chat_id, text):
        if not text: return None
        try:
            return self.bot.send_message(chat_id, telegramify_markdown.markdownify(text), parse_mode='MarkdownV2')
        except Exception as e: 
            print(f"Telebot Error: {e}")
            return None

    def start(self):
        if not self.bot: return
        self.running = True
        
        try:
            commands = [
                telebot.types.BotCommand("status", "Show live positions & fast close actions"),
                telebot.types.BotCommand("balance", "Check unified balance & exposure"),
                telebot.types.BotCommand("live", "Show DB live dashboard & pending signals"),
                telebot.types.BotCommand("pending", "Retrieve limit orders queue in Exchange"),
                telebot.types.BotCommand("start", "Start Continuous Auto-Scan loop"),
                telebot.types.BotCommand("autoscan", "Toggle Auto-Scan ON/OFF"),
                telebot.types.BotCommand("scan", "Force manual market scan instantly"),
                telebot.types.BotCommand("stop", "Abort any active screening sequence"),
                telebot.types.BotCommand("fav", "View favorite saved signals"),
                telebot.types.BotCommand("log", "View system activity logs"),
                telebot.types.BotCommand("reset", "Erase screening histories from database"),
                telebot.types.BotCommand("autotrade", "Toggle Autotrade ON/OFF"),
                telebot.types.BotCommand("setcapital", "Set trading equity config"),
                telebot.types.BotCommand("setquota", "Set maximum allowed open pairs"),
                telebot.types.BotCommand("statusrisk", "Check configuration defaults"),
                telebot.types.BotCommand("cex", "Switch Active CEX [binance/bitget/bybit]")
            ]
            self.bot.set_my_commands(commands)
        except Exception as e:
            print(f"Failed to register TG commands via telebot: {e}")
            
        self.thread = threading.Thread(target=self.poll, daemon=True)
        self.thread.start()
        print("🤖 Telegram Command Listener Started (Telebot).")
        
    def stop(self):
        self.running = False
        if self.bot:
            self.bot.stop_polling()
        
    def poll(self):
        while self.running:
            try:
                self.bot.polling(non_stop=True, timeout=60, skip_pending=True)
            except Exception as e:
                print(f"Telebot polling error: {e}")
                time.sleep(3)
