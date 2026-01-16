import requests
import json
import os
import mplfinance as mpf
import pytz
import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
from datetime import datetime
from psycopg2.extras import RealDictCursor
from modules.config_loader import CONFIG
from modules.database import get_conn, release_conn

def get_now():
    return datetime.now(pytz.timezone(CONFIG['system']['timezone']))

def format_price(value):
    try:
        val = float(value)
        if val < 1: return "{:.8f}".format(val).rstrip('0').rstrip('.')
        return "{:.2f}".format(val)
    except: return "0"

def generate_chart(df, symbol, pattern, timeframe):
    filename = f"chart_{symbol.replace('/','')}_{timeframe}.png"
    try:
        # Create a copy and ensure datetime index
        plot_df = df.iloc[-100:].copy()
        if 'timestamp' in plot_df.columns:
            plot_df.set_index('timestamp', inplace=True)
        plot_df.index = pd.to_datetime(plot_df.index)

        # 1. Find Peaks/Valleys
        n = 3
        min_idx = argrelextrema(plot_df['low'].values, np.less_equal, order=n)[0]
        max_idx = argrelextrema(plot_df['high'].values, np.greater_equal, order=n)[0]

        # 2. Extract Data
        peak_dates = plot_df.index[max_idx]
        peak_vals = plot_df['high'].iloc[max_idx].values
        valley_dates = plot_df.index[min_idx]
        valley_vals = plot_df['low'].iloc[min_idx].values

        lines = []
        line_colors = []

        # Helper to safely add lines
        def add_line(dates, vals, color):
            if len(dates) >= 2:
                d1 = str(dates[-2])
                p1 = float(vals[-2])
                d2 = str(dates[-1])
                p2 = float(vals[-1])
                lines.append([(d1, p1), (d2, p2)])
                line_colors.append(color)

        # 3. Define Lines Based on Pattern
        if pattern in ['ascending_triangle', 'bullish_rectangle', 'double_top', 'bear_flag', 'descending_triangle']:
            add_line(peak_dates, peak_vals, 'red') # Resistance

        if pattern in ['descending_triangle', 'bullish_rectangle', 'double_bottom', 'bull_flag', 'ascending_triangle']:
            add_line(valley_dates, valley_vals, 'green') # Support

        # 4. Setup Chart Panels & Ratios
        mc = mpf.make_marketcolors(up='#2ebd85', down='#f6465d', edge='inherit', wick='inherit', volume='in')
        s  = mpf.make_mpf_style(base_mpf_style='nightclouds', marketcolors=mc)
        
        apds = []
        if 'EMA_Fast' in plot_df.columns: 
            apds.append(mpf.make_addplot(plot_df['EMA_Fast'], color='cyan', width=1))
        
        has_macd = 'MACD_h' in plot_df.columns
        
        # --- CRITICAL FIX FOR PANEL COUNT ---
        if has_macd:
            # Panel 0: Price, Panel 1: MACD, Panel 2: Volume
            colors = ['#2ebd85' if v >= 0 else '#f6465d' for v in plot_df['MACD_h']]
            apds.append(mpf.make_addplot(plot_df['MACD_h'], type='bar', panel=1, color=colors, ylabel='MACD'))
            
            ratios = (3, 1, 1)
            vol_panel = 2  # Explicitly push Volume to Panel 2
        else:
            # Panel 0: Price, Panel 1: Volume
            ratios = (3, 1)
            vol_panel = 1  # Volume is the next available panel

        kwargs = dict(
            type='candle', style=s, addplot=apds, 
            title=f"\n{symbol} ({timeframe}) - {pattern}",
            figsize=(12, 8),
            panel_ratios=ratios,
            volume=True,
            volume_panel=vol_panel, # <--- Fix applied here
            savefig=dict(fname=filename, dpi=100, bbox_inches='tight')
        )
        
        if lines:
            kwargs['alines'] = dict(alines=lines, colors=line_colors, linewidths=1.5, alpha=0.7)
            
        mpf.plot(plot_df, **kwargs)
        return filename
    except Exception as e: 
        print(f"Chart Error: {e}")
        return None


def send_scan_completion(signal_count, duration, btc_bias):
    """
    Sends a summary message when the scan finishes.
    Mentions the configured role ID.
    """
    webhook = CONFIG['api']['discord_webhook'] # Use dashboard webhook for logs
    if not webhook: return
    
    role_id = CONFIG['api'].get('discord_role_id')
    mention = f"<@&{role_id}>" if role_id else ""
    
    # Determine color based on Bias
    color = 0x808080 # Grey
    if "Bullish" in btc_bias: color = 0x00ff00
    elif "Bearish" in btc_bias: color = 0xff0000
    
    embed = {
        "title": "🔭 Scan Cycle Complete",
        "description": f"**Analysis finished for all timeframes.**",
        "color": color,
        "fields": [
            {"name": "⏱️ Duration", "value": f"`{duration:.2f}s`", "inline": True},
            {"name": "📶 Signals Found", "value": f"`{signal_count}`", "inline": True},
            {"name": "📊 Market Bias", "value": f"**{btc_bias}**", "inline": True}
        ],
        "footer": {"text": f"V8 Bot | {get_now().strftime('%H:%M:%S')}"}
    }
    
    try:
        # Send the embed + mention string
        payload = {
            "content": f"{mention} Scan finished.", 
            "embeds": [embed]
        }
        requests.post(webhook, json=payload)
    except Exception as e:
        print(f"Scan Completion Alert Error: {e}")


def send_alert(data):
    webhook = CONFIG['api']['discord_webhook']
    if not webhook: return

    symbol = data['Symbol']
    image_path = generate_chart(data['df'], symbol, data['Pattern'], data['Timeframe'])
    
    is_long = data['Side'] == 'Long'
    color = 0x00ff00 if is_long else 0xff0000
    emoji = "🚀" if is_long else "🔻"
    trend_icon = "🟢" if is_long else "🔴"
    
    fund_val = data['df']['funding'].iloc[-1] if 'funding' in data['df'] else 0
    fund_emoji = "🔴" if abs(fund_val) > 0.01 else "🟢"
    fund_txt = "Hot" if abs(fund_val) > 0.01 else "Cool"
    
    rvol = data['df']['RVOL'].iloc[-1]
    rvol_txt = "⚡ Explosive" if rvol > 3.0 else ("🔥 Strong" if rvol > 2.0 else "🌊 Normal")
    
    # OBI Formatting
    obi_val = data['OBI']
    obi_icon = "🟢" if obi_val > 0 else "🔴"
    # Text Blocks
    tech_block = f"**Pattern:** {data['Pattern']}\n**Trend:** {trend_icon} {data['Side']} Trend\n**MACD:** {data.get('MACD_Signal', 'Expand')} 🟢"
    deriv_block = f"**Fund:** {fund_emoji} {fund_txt} `{fund_val*100:.3f}%` | Basis: `{data['Basis']*100:.3f}%`\n**Flow:** Accumulating 🟢"
    quant_block = (
        f"**RVOL:** `{rvol:.1f}x` ({rvol_txt})\n"
        f"**Z-Score:** `{data['Z_Score']:.2f}σ`\n"
        f"**ζ-Field:** `{data['Zeta_Score']:.1f}` / 100\n"
        f"**OBI:** `{obi_val:.2f}` {obi_icon}"    
    )
    
    # NEW: SMC Text
    smc_txt = "None"
    if "In Bullish OB" in data['Tech_Reasons']: smc_txt = "🟢 Demand Zone"
    elif "In Bearish OB" in data['Tech_Reasons']: smc_txt = "🔴 Supply Zone"
    elif "Higher Low" in data['Tech_Reasons']: smc_txt = "📈 Higher Low (Dip)"
    elif "Lower High" in data['Tech_Reasons']: smc_txt = "📉 Lower High (Rally)"
    
    explanations = f"**Tech:** {data.get('Tech_Reasons', '-')}\n**Quant:** {data.get('Quant_Reasons', '-')}\n**Deriv:** {data.get('Deriv_Reasons', '-')}"

    embed = {
        "title": f"{emoji} SIGNAL: {symbol} ({data['Pattern']})",
        "description": f"**{data['Side']}** | **{data['Timeframe']}**",
        "color": color,
        "fields": [
            {"name": "🎯 Entry", "value": f"`{format_price(data['Entry'])}`", "inline": True},
            {"name": "🛑 Stop", "value": f"`{format_price(data['SL'])}`", "inline": True},
            {"name": "💰 Rewards", "value": f"RR (TP3): **1:{data.get('RR', 0.0)}**", "inline": True},
            {"name": "🏁 Targets", "value": f"TP1: `{format_price(data['TP1'])}`\nTP2: `{format_price(data['TP2'])}`\nTP3: `{format_price(data['TP3'])}`", "inline": False},
            {"name": "📊 Technicals & SMC", "value": f"{tech_block}\n**SMC:** {smc_txt}", "inline": False},
            {"name": "⛽ Derivatives", "value": deriv_block, "inline": False},
            {"name": "🧮 Quant", "value": quant_block, "inline": False},
            {"name": "🏆 Scores", "value": f"Tech: `{data['Tech_Score']}` | Quant: `{data['Quant_Score']}` | Deriv: `{data['Deriv_Score']}`", "inline": False},
            {"name": "📝 Analysis", "value": explanations, "inline": False},
            {"name": "🧠 Context", "value": f"Bias: **{data['BTC_Bias']}**", "inline": False}
        ],
        "footer": {"text": f"V8 Bot | {get_now().strftime('%Y-%m-%d %H:%M:%S')}"}
    }

    try:
        payload = {"content": "", "embeds": [embed]}
        if image_path:
            with open(image_path, 'rb') as f:
                r = requests.post(webhook, data={'payload_json': json.dumps(payload)}, files={'file': f}, params={"wait": "true"})
        else:
            r = requests.post(webhook, json=payload, params={"wait": "true"})
            
        msg_id, ch_id = (r.json().get('id'), r.json().get('channel_id')) if r.status_code in [200, 201] else (None, None)
        
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO trades (symbol, side, timeframe, pattern, entry_price, sl_price, tp1, tp2, tp3, reason, 
            tech_score, quant_score, deriv_score, smc_score, basis, btc_bias, message_id, channel_id, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Waiting Entry')
        """, (symbol, data['Side'], data['Timeframe'], data['Pattern'], data['Entry'], data['SL'], data['TP1'], 
              data['TP2'], data['TP3'], data['Reason'], data['Tech_Score'], data['Quant_Score'], data['Deriv_Score'], 
              data['SMC_Score'], data['Basis'], data['BTC_Bias'], msg_id, ch_id))
        conn.commit()
        release_conn(conn)
    except Exception as e: print(e)
    finally:
        if image_path and os.path.exists(image_path): os.remove(image_path)

def update_status_dashboard():
    webhook = CONFIG['api']['discord_dashboard_webhook']
    if not webhook: return
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT symbol, side, status, entry_hit_at, created_at FROM trades WHERE status NOT LIKE '%Closed%' ORDER BY created_at DESC")
        trades = cur.fetchall()
        lines = [f"`{(t['entry_hit_at'] or t['created_at']).strftime('%H:%M')}` {'🟢' if 'Active' in t['status'] else '⏳'} **{t['symbol']}** ({t['side']}): {t['status']}" for t in trades]
        content = "**📊 LIVE DASHBOARD**\n" + ("\n".join(lines) if lines else "No active trades.")
        
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

def run_fast_update():
    update_status_dashboard()