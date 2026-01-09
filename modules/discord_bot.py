import requests
import json
import os
import mplfinance as mpf
import pytz
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
        plot_df = df.iloc[-80:].copy()
        mc = mpf.make_marketcolors(up='#2ebd85', down='#f6465d', edge='inherit', wick='inherit', volume='in')
        s  = mpf.make_mpf_style(base_mpf_style='nightclouds', marketcolors=mc)
        apds = []
        if 'EMA_Fast' in plot_df.columns: apds.append(mpf.make_addplot(plot_df['EMA_Fast'], color='cyan', width=1))
        if 'MACD_h' in plot_df.columns:
            colors = ['#2ebd85' if v >= 0 else '#f6465d' for v in plot_df['MACD_h']]
            apds.append(mpf.make_addplot(plot_df['MACD_h'], type='bar', panel=1, color=colors, ylabel='MACD'))
        
        mpf.plot(plot_df, type='candle', style=s, addplot=apds, title=f"\n{symbol} ({timeframe}) - {pattern}", 
                 figsize=(12, 6), panel_ratios=(3,1), savefig=dict(fname=filename, dpi=100, bbox_inches='tight'), 
                 volume=False, closefig=True)
        return filename
    except: return None

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
    
    # Text Blocks
    tech_block = f"**Pattern:** {data['Pattern']}\n**Trend:** {trend_icon} {data['Side']} Trend\n**MACD:** {data.get('MACD_Signal', 'Expand')} 🟢"
    deriv_block = f"**Fund:** {fund_emoji} {fund_txt} `{fund_val*100:.3f}%` | Basis: `{data['Basis']*100:.3f}%`\n**Flow:** Accumulating 🟢"
    quant_block = (
        f"**RVOL:** `{rvol:.1f}x` ({rvol_txt})\n"
        f"**Z-Score:** `{data['Z_Score']:.2f}σ`\n"
        f"**OBI:** `0.31`"
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