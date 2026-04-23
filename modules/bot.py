"""
Tujuan: Memformat payload sinyal dan mengirimkan alert/notifikasi via Telegram.
Caller: main.py
Dependensi: requests, matplotlib, pandas, database, config_loader
Main Functions: send_telegram_alert(), update_telegram_dashboard(), send_alert(), run_fast_update()
Side Effects: Mengirim POST HTTP ke Telegram API. Writes image temporary ke disk.
"""

import requests, json, os, pytz, pandas as pd, numpy as np
import matplotlib
matplotlib.use('Agg')
import mplfinance as mpf
from scipy.signal import argrelextrema
from datetime import datetime
from modules.config_loader import CONFIG
import telegramify_markdown
from modules.database import get_conn, release_conn, get_dict_cursor, get_active_cex

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
    if not token or not chat_id: return False, None
    
    emoji = "🚀" if data['Side'] == 'Long' else "🔻"
    current_price = data['df']['close'].iloc[-1]
    ts = get_now().strftime('%Y-%m-%d %H:%M:%S')
    active_cex = get_active_cex().upper()
    
    rvol = data['df']['RVOL'].iloc[-1]
    rvol_txt = "Explosive" if rvol > 3.0 else ("Strong" if rvol > 2.0 else "Normal")
    
    fund_rate = data['df'].get('funding', pd.Series([0])).iloc[-1]
    if isinstance(fund_rate, pd.Series): fund_rate = fund_rate.iloc[-1]
    fund_pct = fund_rate * 100
    
    smc_reasons_str = str(data.get('SMC_Reasons', ''))
    smc_txt = "None"
    if "Order Block" in smc_reasons_str: smc_txt = "Bullish Demand" if "Bullish" in smc_reasons_str else "Bearish Supply"
    elif data.get('SMC_Score', 0) > 0: smc_txt = "Confluence Found"
    
    sym_no_slash = data['Symbol'].replace('/', '').replace(':', '')
    cex_url = f"https://www.bybit.com/trade/usdt/{sym_no_slash}"
    if active_cex == 'BINANCE': cex_url = f"https://www.binance.com/en/futures/{sym_no_slash}"
    elif active_cex == 'BITGET': cex_url = f"https://www.bitget.com/futures/usdt/{sym_no_slash}"
    
    text = f"**{emoji} SIGNAL: {data['Symbol']}**\n"
    text += f"🏢 CEX: **[{active_cex}]({cex_url})**\n"
    text += f"🧭 **{data['Side']}** | **{data['Timeframe']}** | {data['Pattern']}\n"
    text += f"🕒 `{ts}`\n\n"
    text += f"💵 **Current:** `{format_price(current_price)}`\n"
    text += f"🎯 **Entry:** `{format_price(data['Entry'])}`\n"
    text += f"🛑 **Stop Loss:** `{format_price(data['SL'])}`\n"
    text += f"💰 **Risk/Reward:** 1:{data.get('RR', 0.0)}\n\n"
    text += f"🏁 **Targets:**\n"
    text += f"TP1: `{format_price(data['TP1'])}`\n"
    text += f"TP2: `{format_price(data['TP2'])}`\n"
    text += f"TP3: `{format_price(data['TP3'])}`\n\n"
    
    text += f"🧮 **Quant & Derivs:**\n"
    text += f"• RVOL: `{rvol:.1f}x` ({rvol_txt})\n"
    text += f"• Z-Score: `{data.get('Z_Score', 0):.2f}σ`\n"
    text += f"• OBI: `{data.get('OBI', 0.0):.2f}`\n"
    text += f"• Funding: `{fund_pct:.4f}%`\n\n"
    
    total_score = data.get('Tech_Score', 0) + data.get('SMC_Score', 0) + data.get('Quant_Score', 0) + data.get('Deriv_Score', 0)
    text += f"🏆 **Scores (Total: {total_score}):**\nTech: `{data.get('Tech_Score',0)}` | SMC: `{data.get('SMC_Score',0)}` | Quant: `{data.get('Quant_Score',0)}` | Deriv: `{data.get('Deriv_Score',0)}`\n"
    text += f"🧠 **BTC Bias:** {data.get('BTC_Bias', '-')}"
    
    url = f"https://api.telegram.org/bot{token}/"
    
    reply_markup = None
    try:
        from modules.database import get_risk_config
        cfg = get_risk_config()
        
        reply_markup = {"inline_keyboard": []}
        btn_row = []
        if not cfg.get('auto_trade', False):
            btn_row.append({"text": f"⚡ Start Trade", "callback_data": f"trade_{data['Symbol']}"})
        
        btn_row.append({"text": f"⭐ Favorite", "callback_data": f"fav_{data['Symbol']}"})
        reply_markup["inline_keyboard"].append(btn_row)
    except: pass
        
    msg_id = None
    try:
        if image_path and os.path.exists(image_path):
            data_payload = {'chat_id': chat_id, 'caption': telegramify_markdown.markdownify(text), 'parse_mode': 'MarkdownV2'}
            if reply_markup: data_payload['reply_markup'] = json.dumps(reply_markup)
            with open(image_path, 'rb') as f:
                r = requests.post(url + 'sendPhoto', data=data_payload, files={'photo': f})
        else:
            json_payload = {'chat_id': chat_id, 'text': telegramify_markdown.markdownify(text), 'parse_mode': 'MarkdownV2'}
            if reply_markup: json_payload['reply_markup'] = reply_markup
            r = requests.post(url + 'sendMessage', json=json_payload)
            
        if r.status_code == 200:
            msg_id = r.json().get('result', {}).get('message_id')
        return True, msg_id
    except Exception as e:
        print(f"Telegram Alert Error: {e}")
        return False, None

def update_telegram_dashboard(lines_text):
    token = CONFIG['api'].get('telegram_bot_token')
    chat_id = CONFIG['api'].get('telegram_chat_id')
    if not token or not chat_id: return
    
    content = "**📊 LIVE DASHBOARD**\n\n```text\n" + lines_text + "\n```"
    url = f"https://api.telegram.org/bot{token}/"
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value_text FROM bot_state WHERE key_name = 'telegram_dashboard_msg_id'")
        row = cur.fetchone()
        msg_id = row['value_text'] if row and isinstance(row, dict) else (row[0] if row else None)
        
        success = False
        if msg_id:
            r = requests.post(url + 'editMessageText', json={'chat_id': chat_id, 'message_id': msg_id, 'text': telegramify_markdown.markdownify(content), 'parse_mode': 'MarkdownV2'})
            if r.status_code == 200: success = True
            
        if not success:
            r = requests.post(url + 'sendMessage', json={'chat_id': chat_id, 'text': telegramify_markdown.markdownify(content), 'parse_mode': 'MarkdownV2'})
            if r.status_code == 200:
                new_id = r.json().get('result', {}).get('message_id')
                if new_id:
                    cur.execute("INSERT INTO bot_state (key_name, value_text) VALUES ('telegram_dashboard_msg_id', ?) ON CONFLICT (key_name) DO UPDATE SET value_text = excluded.value_text", (str(new_id),))
                    conn.commit()
    except Exception as e:
        print(f"TG Dashboard Error: {e}")
    finally: release_conn(conn)

def send_alert(data):
    tg_token = CONFIG['api'].get('telegram_bot_token')
    if not tg_token: return False
    
    symbol = data['Symbol']
    
    image_path = None
    try: image_path = generate_chart(data['df'], symbol, data['Pattern'], data['Timeframe'])
    except Exception as e: print(f"❌ Chart Error: {e}")

    tg_sent, msg_id = send_telegram_alert(data, image_path)

    if image_path and os.path.exists(image_path): os.remove(image_path)

    if tg_sent:
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO trades (symbol, side, timeframe, pattern, entry_price, sl_price, tp1, tp2, tp3, reason, 
                tech_score, quant_score, deriv_score, smc_score, basis, btc_bias, z_score, zeta_score, obi, 
                tech_reasons, quant_reasons, deriv_reasons, smc_reasons, message_id, status, natr)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Waiting Entry', ?)
            """, (symbol, data['Side'], data['Timeframe'], data['Pattern'], data['Entry'], data['SL'], data['TP1'], 
                  data['TP2'], data['TP3'], data.get('Reason',''), data.get('Tech_Score',0), data.get('Quant_Score',0), data.get('Deriv_Score',0), 
                  data.get('SMC_Score',0), data.get('Basis',0), data.get('BTC_Bias',''), data.get('Z_Score',0), data.get('Zeta_Score',0), data.get('OBI',0), 
                  data.get('Tech_Reasons',''), data.get('Quant_Reasons',''), data.get('Deriv_Reasons',''), str(data.get('SMC_Reasons','')),
                  msg_id, data.get('NATR', 0.0)))
            conn.commit()
            release_conn(conn)
            return True
        except Exception as e:
            print(f"DB Insert Error after alert: {e}")
            
    return False

def update_status_dashboard():
    # Only updates Telegram Dashboard now
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
        lines = [f"[{fmt_time(t['entry_hit_at'] or t['created_at'])}] {'🟢' if 'Active' in t['status'] else '⏳'} {t['symbol']} ({t['side']}): {t['status']}" for t in trades]
    except Exception as e: pass
    finally: release_conn(conn)
    
    text_lines = "\n".join(lines) if lines else "No active trades."
    # update_telegram_dashboard(text_lines) # Disabled per user preference

def send_scan_completion(count, duration, bias, dispatched_signals=None):
    tg_token = CONFIG['api'].get('telegram_bot_token')
    tg_chat = CONFIG['api'].get('telegram_chat_id')
    active_cex = get_active_cex().upper()
    if tg_token and tg_chat:
        icon = "🟢" if "Bullish" in bias else ("🔴" if "Bearish" in bias else "⚪")
        text = f"🔭 **Scan Cycle Complete ({active_cex})**\n\n⏱️ **Duration:** `{duration:.2f}s`\n📶 **Signals Found:** `{count}`\n📊 **Global Bias:** {icon} **{bias}**"
        
        if dispatched_signals and len(dispatched_signals) > 0:
            sorted_signals = sorted(dispatched_signals, key=lambda x: x.get('Total_Score', 0), reverse=True)
            text += "\n\n🏆 **Signal Leaderboard:**\n"
            
            for idx, sig in enumerate(sorted_signals):
                emoji = "🚀" if sig['Side'] == 'Long' else "🔻"
                score = sig.get('Total_Score', 0)
                text += f"{idx+1}. {emoji} **{sig['Symbol']}** ({sig['Timeframe']}) \\- Score: `{score}`\n"
                
        url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
        try: requests.post(url, json={'chat_id': tg_chat, 'text': telegramify_markdown.markdownify(text), 'parse_mode': 'MarkdownV2'})
        except: pass

def run_fast_update(exchange=None):
    update_status_dashboard()