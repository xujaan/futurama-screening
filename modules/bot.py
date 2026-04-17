import requests, json, os, pytz, pandas as pd, numpy as np
import matplotlib
matplotlib.use('Agg')
import mplfinance as mpf
from scipy.signal import argrelextrema
from datetime import datetime
from modules.config_loader import CONFIG
from modules.database import get_conn, release_conn, get_dict_cursor

def get_now(): return datetime.now(pytz.timezone(CONFIG['system']['timezone']))
def format_price(value): return "{:.8f}".format(float(value)).rstrip('0').rstrip('.') if float(value) < 1 else "{:.2f}".format(float(value))

def generate_chart(df, symbol, pattern, timeframe):
    filename = f"chart_{symbol.replace('/','')}_{timeframe}.png"
    try:
        plot_df = df.iloc[-100:].copy()
        if 'timestamp' in plot_df.columns: plot_df.set_index('timestamp', inplace=True)
        plot_df.index = pd.to_datetime(plot_df.index)

        n=3
        min_idx = argrelextrema(plot_df['low'].values, np.less_equal, order=n)[0]
        max_idx = argrelextrema(plot_df['high'].values, np.greater_equal, order=n)[0]
        
        peak_dates, peak_vals = plot_df.index[max_idx], plot_df['high'].iloc[max_idx].values
        valley_dates, valley_vals = plot_df.index[min_idx], plot_df['low'].iloc[min_idx].values
        
        lines, colors = [], []
        def add_line(dates, vals, color):
            if len(dates) >= 2: lines.append([(str(dates[-2]), float(vals[-2])), (str(dates[-1]), float(vals[-1]))]); colors.append(color)

        if pattern in ['ascending_triangle', 'bullish_rectangle', 'double_top', 'bear_flag', 'descending_triangle']: add_line(peak_dates, peak_vals, 'red')
        if pattern in ['descending_triangle', 'bullish_rectangle', 'double_bottom', 'bull_flag', 'ascending_triangle']: add_line(valley_dates, valley_vals, 'green')

        mc = mpf.make_marketcolors(up='#2ebd85', down='#f6465d', edge='inherit', wick='inherit', volume='in')
        s = mpf.make_mpf_style(base_mpf_style='nightclouds', marketcolors=mc)
        apds = []
        if 'EMA_Fast' in plot_df.columns: apds.append(mpf.make_addplot(plot_df['EMA_Fast'], color='cyan', width=1))
        
        ratios, vol_panel = (3, 1), 1
        if 'MACD_h' in plot_df.columns:
            cols = ['#2ebd85' if v >= 0 else '#f6465d' for v in plot_df['MACD_h']]
            apds.append(mpf.make_addplot(plot_df['MACD_h'], type='bar', panel=1, color=cols, ylabel='MACD'))
            ratios, vol_panel = (3, 1, 1), 2

        kwargs = dict(type='candle', style=s, addplot=apds, title=f"\n{symbol} ({timeframe}) - {pattern}", figsize=(12, 8), panel_ratios=ratios, volume=True, volume_panel=vol_panel, savefig=dict(fname=filename, dpi=100, bbox_inches='tight'))
        if lines: kwargs['alines'] = dict(alines=lines, colors=colors, linewidths=1.5, alpha=0.7)
        mpf.plot(plot_df, **kwargs)
        return filename
    except Exception as e: print(f"Chart Error: {e}"); return None

def send_telegram_alert(data, image_path=None):
    token = CONFIG['api'].get('telegram_bot_token')
    chat_id = CONFIG['api'].get('telegram_chat_id')
    if not token or not chat_id: return False
    
    emoji = "🚀" if data['Side'] == 'Long' else "🔻"
    current_price = data['df']['close'].iloc[-1]
    ts = get_now().strftime('%Y-%m-%d %H:%M:%S')
    
    text = f"<b>{emoji} SIGNAL: {data['Symbol']} ({data['Pattern']})</b>\n"
    text += f"<b>{data['Side']}</b> | <b>{data['Timeframe']}</b>\n"
    text += f"🕒 <code>{ts}</code>\n\n"
    text += f"💵 <b>Current:</b> <code>{format_price(current_price)}</code>\n"
    text += f"🎯 <b>Entry:</b> <code>{format_price(data['Entry'])}</code>\n"
    text += f"🛑 <b>Stop:</b> <code>{format_price(data['SL'])}</code>\n"
    text += f"💰 <b>RR:</b> 1:{data.get('RR', 0.0)}\n\n"
    text += f"🏁 <b>Targets:</b>\n"
    text += f"TP1: <code>{format_price(data['TP1'])}</code>\n"
    text += f"TP2: <code>{format_price(data['TP2'])}</code>\n"
    text += f"TP3: <code>{format_price(data['TP3'])}</code>\n\n"
    text += f"🏆 <b>Scores:</b>\nTech: <code>{data.get('Tech_Score',0)}</code> | SMC: <code>{data.get('SMC_Score',0)}</code> | Quant: <code>{data.get('Quant_Score',0)}</code> | Deriv: <code>{data.get('Deriv_Score',0)}</code>\n"
    text += f"🧠 <b>Bias:</b> {data.get('BTC_Bias', '-')}"
    
    url = f"https://api.telegram.org/bot{token}/"
    
    reply_markup = None
    try:
        from modules.database import get_risk_config
        cfg = get_risk_config()
        if not cfg.get('auto_trade', False):
            reply_markup = {
                "inline_keyboard": [
                    [{"text": f"⚡ Start Trade {data['Symbol']}", "callback_data": f"trade_{data['Symbol']}"}]
                ]
            }
    except Exception as e:
        print(f"Error fetching risk config for tg button: {e}")
        
    try:
        if image_path and os.path.exists(image_path):
            data_payload = {'chat_id': chat_id, 'caption': text, 'parse_mode': 'HTML'}
            if reply_markup:
                data_payload['reply_markup'] = json.dumps(reply_markup)
            with open(image_path, 'rb') as f:
                requests.post(url + 'sendPhoto', data=data_payload, files={'photo': f})
        else:
            json_payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'}
            if reply_markup:
                json_payload['reply_markup'] = reply_markup
            requests.post(url + 'sendMessage', json=json_payload)
        return True
    except Exception as e:
        print(f"Telegram Alert Error: {e}")
        return False

def update_telegram_dashboard(lines_text):
    token = CONFIG['api'].get('telegram_bot_token')
    chat_id = CONFIG['api'].get('telegram_chat_id')
    if not token or not chat_id: return
    
    content = "<b>📊 LIVE DASHBOARD</b>\n\n" + lines_text
    url = f"https://api.telegram.org/bot{token}/"
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value_text FROM bot_state WHERE key_name = 'telegram_dashboard_msg_id'")
        row = cur.fetchone()
        msg_id = row[0] if row else None
        
        success = False
        if msg_id:
            r = requests.post(url + 'editMessageText', json={'chat_id': chat_id, 'message_id': msg_id, 'text': content, 'parse_mode': 'HTML'})
            if r.status_code == 200: success = True
            
        if not success:
            r = requests.post(url + 'sendMessage', json={'chat_id': chat_id, 'text': content, 'parse_mode': 'HTML'})
            if r.status_code == 200:
                new_id = r.json().get('result', {}).get('message_id')
                if new_id:
                    cur.execute("INSERT INTO bot_state (key_name, value_text) VALUES ('telegram_dashboard_msg_id', %s) ON CONFLICT (key_name) DO UPDATE SET value_text = EXCLUDED.value_text", (str(new_id),))
                    conn.commit()
    except Exception as e:
        print(f"TG Dashboard Error: {e}")
    finally: release_conn(conn)

def send_alert(data):
    webhook = CONFIG['api'].get('discord_webhook')
    tg_token = CONFIG['api'].get('telegram_bot_token')
    
    if not webhook and not tg_token: return False
    
    symbol = data['Symbol']
    
    # 1. Generate Chart
    image_path = None
    try:
        image_path = generate_chart(data['df'], symbol, data['Pattern'], data['Timeframe'])
    except Exception as e:
        print(f"❌ Chart Error: {e}")

    # 2. Telegram Send
    tg_sent = False
    if tg_token:
        tg_sent = send_telegram_alert(data, image_path)

    # 3. Discord Send
    discord_sent = False
    msg_id = None
    channel_id = None
    
    if webhook:
        try:
            is_long = data['Side'] == 'Long'
            color = 0x00ff00 if is_long else 0xff0000
            emoji = "🚀" if is_long else "🔻"
            
            rvol = data['df']['RVOL'].iloc[-1]
            rvol_txt = "⚡ Explosive" if rvol > 3.0 else ("🔥 Strong" if rvol > 2.0 else "🌊 Normal")
            obi_val = data.get('OBI', 0.0)
            obi_icon = "🟢" if obi_val > 0 else ("🔴" if obi_val < 0 else "⚪")
            
            quant_block = f"**RVOL:** `{rvol:.1f}x` ({rvol_txt})\n**Z-Score:** `{data.get('Z_Score', 0):.2f}σ`\n**ζ-Field:** `{data.get('Zeta_Score', 0):.1f}` / 100\n**OBI:** `{obi_val:.2f}` {obi_icon}"

            fund_rate = data['df'].get('funding', pd.Series([0])).iloc[-1]
            if isinstance(fund_rate, pd.Series): fund_rate = fund_rate.iloc[-1]
            fund_pct = fund_rate * 100
            fund_icon = "🔴" if fund_pct > 0.01 else "🟢"
            fund_txt = "Hot" if fund_pct > 0.01 else "Cool"
            basis_pct = data.get('Basis', 0) * 100
            deriv_block = f"**Funding:** `{fund_pct:.4f}%` {fund_icon} ({fund_txt})\n**Basis:** `{basis_pct:.4f}%`\n**Bias:** {data.get('Deriv_Reasons', 'Neutral')}"

            smc_reasons_str = str(data.get('SMC_Reasons', ''))
            smc_txt = "None"
            if "Order Block" in smc_reasons_str:
                smc_txt = "🟢 Demand Zone" if "Bullish" in smc_reasons_str else "🔴 Supply Zone"
            elif "Structure" in smc_reasons_str:
                smc_txt = "📈 Higher Low" if "Higher Low" in smc_reasons_str else "📉 Lower High"
            elif data.get('SMC_Score', 0) > 0:
                smc_txt = "✅ Confluence Found"

            scores_txt = f"Tech: `{data.get('Tech_Score',0)}` | SMC: `{data.get('SMC_Score',0)}` | Quant: `{data.get('Quant_Score',0)}` | Deriv: `{data.get('Deriv_Score',0)}`"
            analysis_txt = f"**Tech:** {data.get('Tech_Reasons', '-')}\n**SMC:** {smc_reasons_str if smc_reasons_str else '-'}\n**Quant:** {data.get('Quant_Reasons', '-')}"
            legend_txt = "• **Z-Score:** `>3.0`=Nuclear | **ζ-Field:** `>70`=High Prob\n• **OBI:** `>0.3`=Bullish Book | **Funding:** `>0.01%`=Expensive"

            embed = {
                "title": f"{emoji} SIGNAL: {symbol} ({data['Pattern']})",
                "description": f"**{data['Side']}** | **{data['Timeframe']}**",
                "color": color,
                "fields": [
                    {"name": "🎯 Entry", "value": f"`{format_price(data['Entry'])}`", "inline": True},
                    {"name": "🛑 Stop", "value": f"`{format_price(data['SL'])}`", "inline": True},
                    {"name": "💰 Rewards", "value": f"RR: **1:{data.get('RR', 0.0)}**", "inline": True},
                    {"name": "🏁 Targets", "value": f"TP1: `{format_price(data['TP1'])}`\nTP2: `{format_price(data['TP2'])}`\nTP3: `{format_price(data['TP3'])}`", "inline": False},
                    {"name": "📊 Technicals", "value": f"**Pattern:** {data['Pattern']}\n**Trend:** {emoji} {data['Side']}\n**SMC:** {smc_txt}", "inline": False},
                    {"name": "🧮 Quant Models", "value": quant_block, "inline": True},
                    {"name": "⛽ Derivatives", "value": deriv_block, "inline": True},
                    {"name": "🏆 Scores", "value": scores_txt, "inline": False},
                    {"name": "📝 Detailed Analysis", "value": analysis_txt, "inline": False},
                    {"name": "ℹ️ Metrics Guide", "value": legend_txt, "inline": False},
                    {"name": "🧠 Context", "value": f"Bias: **{data.get('BTC_Bias','')}**", "inline": False}
                ],
                "footer": {"text": f"V8 Bot | {get_now().strftime('%Y-%m-%d %H:%M:%S')}"}
            }
            
            payload = {"content": "", "embeds": [embed]}
            if image_path and os.path.exists(image_path):
                with open(image_path, 'rb') as f:
                    r = requests.post(webhook, data={'payload_json': json.dumps(payload)}, files={'file': f}, params={"wait": "true"})
            else:
                r = requests.post(webhook, json=payload, params={"wait": "true"})
                
            if r.status_code in [200, 201]:
                discord_sent = True
                msg_id = r.json().get('id')
                channel_id = r.json().get('channel_id')
        except Exception as e:
            print(f"Discord Alert Error: {e}")

    # Remove image
    if image_path and os.path.exists(image_path): os.remove(image_path)

    # 4. Save to DB
    if discord_sent or tg_sent:
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO trades (symbol, side, timeframe, pattern, entry_price, sl_price, tp1, tp2, tp3, reason, 
                tech_score, quant_score, deriv_score, smc_score, basis, btc_bias, z_score, zeta_score, obi, 
                tech_reasons, quant_reasons, deriv_reasons, smc_reasons, message_id, channel_id, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Waiting Entry')
            """, (symbol, data['Side'], data['Timeframe'], data['Pattern'], data['Entry'], data['SL'], data['TP1'], 
                  data['TP2'], data['TP3'], data.get('Reason',''), data.get('Tech_Score',0), data.get('Quant_Score',0), data.get('Deriv_Score',0), 
                  data.get('SMC_Score',0), data.get('Basis',0), data.get('BTC_Bias',''), data.get('Z_Score',0), data.get('Zeta_Score',0), data.get('OBI',0), 
                  data.get('Tech_Reasons',''), data.get('Quant_Reasons',''), data.get('Deriv_Reasons',''), str(data.get('SMC_Reasons','')),
                  msg_id, channel_id))
            conn.commit()
            release_conn(conn)
            return True
        except Exception as e:
            print(f"DB Insert Error after alert: {e}")
            
    return False

def update_status_dashboard():
    conn = get_conn()
    lines = []
    try:
        cur = get_dict_cursor(conn)
        cur.execute("SELECT symbol, side, status, entry_hit_at, created_at FROM trades WHERE status NOT LIKE '%Closed%' ORDER BY created_at DESC")
        trades = cur.fetchall()
        lines = [f"`{(t['entry_hit_at'] or t['created_at']).strftime('%H:%M')}` {'🟢' if 'Active' in t['status'] else '⏳'} **{t['symbol']}** ({t['side']}): {t['status']}" for t in trades]
    except Exception as e:
        print(f"Failed to fetch trades for dashboard: {e}")
    finally: release_conn(conn)
    
    text_lines = "\n".join(lines) if lines else "No active trades."
    
    # 1. Update Telegram Dashboard (Disabled here per user request, moved to /live command)
    # update_telegram_dashboard(text_lines)
    
    # 2. Update Discord Dashboard
    webhook = CONFIG['api'].get('discord_dashboard_webhook')
    if webhook:
        content = "**📊 LIVE DASHBOARD**\n" + text_lines
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT value_text FROM bot_state WHERE key_name = 'dashboard_msg_id'")
            row = cur.fetchone()
            msg_id = row[0] if row else None
            
            if msg_id: requests.patch(f"{webhook}/messages/{msg_id}", json={"content": content})
            else:
                r = requests.post(webhook, json={"content": content}, params={"wait": "true"})
                if r.status_code in [200, 201]:
                    new_id = r.json().get('id')
                    cur.execute("INSERT INTO bot_state (key_name, value_text) VALUES ('dashboard_msg_id', %s) ON CONFLICT (key_name) DO UPDATE SET value_text = EXCLUDED.value_text", (str(new_id),))
                    conn.commit()
        except: pass
        finally: release_conn(conn)

def send_scan_completion(count, duration, bias):
    # Telegram
    tg_token = CONFIG['api'].get('telegram_bot_token')
    tg_chat = CONFIG['api'].get('telegram_chat_id')
    if tg_token and tg_chat:
        icon = "🟢" if "Bullish" in bias else ("🔴" if "Bearish" in bias else "⚪")
        text = f"🔭 <b>Scan Cycle Complete</b>\n\n⏱️ <b>Duration:</b> <code>{duration:.2f}s</code>\n📶 <b>Signals:</b> <code>{count}</code>\n📊 <b>Bias:</b> {icon} <b>{bias}</b>"
        url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
        try: requests.post(url, json={'chat_id': tg_chat, 'text': text, 'parse_mode': 'HTML'})
        except: pass

    # Discord
    webhook = CONFIG['api'].get('discord_webhook')
    if webhook:
        color = 0x00ff00 if "Bullish" in bias else (0xff0000 if "Bearish" in bias else 0x808080)
        embed = {"title": "🔭 Scan Cycle Complete", "color": color, "fields": [{"name": "⏱️ Duration", "value": f"`{duration:.2f}s`", "inline": True}, {"name": "📶 Signals", "value": f"`{count}`", "inline": True}, {"name": "📊 Bias", "value": f"**{bias}**", "inline": True}]}
        try: requests.post(webhook, json={"embeds": [embed]})
        except: pass

def run_fast_update(exchange=None):
    update_status_dashboard()
    
    if exchange and CONFIG.get('risk_management', {}).get('auto_trade', False):
        try:
            import modules.execution as execution
            conn = get_conn()
            cur = get_dict_cursor(conn)
            cur.execute("SELECT * FROM trades WHERE status = 'Waiting Entry'")
            waiting_trades = cur.fetchall()
            
            if waiting_trades:
                positions = exchange.fetch_positions()
                pos_map = {p['symbol']: p for p in positions if float(p['contracts']) > 0}
                
                for t in waiting_trades:
                    sym = t['symbol']
                    if sym in pos_map:
                        pos = pos_map[sym]
                        pos_side = pos['side']
                        total_qty = float(pos['contracts'])
                        
                        execution.place_layered_tps(exchange, sym, pos_side, float(t['tp1']), float(t['tp2']), float(t['tp3']), total_qty)
                        
                        cur.execute("UPDATE trades SET status = 'Active (TP Set)' WHERE id = %s", (t['id'],))
                conn.commit()
        except Exception as e:
            print("AutoTrade TP Error:", e)
        finally:
            if 'conn' in locals() and conn:
                release_conn(conn)