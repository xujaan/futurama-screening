import ccxt
import time
import schedule
import random
import os
import pandas as pd
import pandas_ta as ta
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from modules.config_loader import CONFIG
from modules.database import init_db
from modules.technicals import get_technicals, detect_divergence
from modules.quant import calculate_metrics, check_fakeout
from modules.derivatives import analyze_derivatives
from modules.smc import analyze_smc
from modules.patterns import find_pattern
from modules.discord_bot import send_alert, update_status_dashboard, run_fast_update

exchange = ccxt.bybit({'apiKey': CONFIG['api']['bybit_key'], 'secret': CONFIG['api']['bybit_secret'], 'options': {'defaultType': 'swap'}})

def get_btc_bias():
    try:
        bars = exchange.fetch_ohlcv('BTC/USDT', '1d', limit=100)
        if not bars: return "Sideways"
        df = pd.DataFrame(bars, columns=['t','o','h','l','c','v'])
        df['ema13'] = ta.ema(df['c'], length=13)
        df['ema21'] = ta.ema(df['c'], length=21)
        df['rsi'] = ta.rsi(df['c'], length=14)
        curr = df.iloc[-1]
        
        bias = "Bullish" if curr['ema13'] > curr['ema21'] else "Bearish"
        if bias == "Bullish" and curr['rsi'] > 75: return "Neutral (Overbought)"
        if bias == "Bearish" and curr['c'] > curr['ema13'] and curr['rsi'] < 60: return "Bearish (Dead Cat)"
        return bias
    except: return "Sideways"

def calculate_rr(entry, sl, tp3):
    if entry <= 0 or sl <= 0 or tp3 <= 0: return 0.0
    risk = abs(entry - sl)
    return round(abs(tp3 - entry) / risk, 2) if risk > 0 else 0.0

def analyze_ticker(symbol, timeframe, btc_bias, seen_symbols):
    if symbol in seen_symbols: return None
    try:
        ticker_info = exchange.fetch_ticker(symbol)
        if "ST" in ticker_info.get('info', {}).get('symbol', ''): return None
        
        min_candles = CONFIG['system'].get('min_candles_analysis', 200)
        bars = exchange.fetch_ohlcv(symbol, timeframe, limit=min_candles + 50)
        if len(bars) < min_candles: return None
        
        df = pd.DataFrame(bars, columns=['timestamp','open','high','low','close','volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Technicals
        df = get_technicals(df)
        pattern = find_pattern(df)
        if not pattern: return None
        side = CONFIG['pattern_signals'].get(pattern)
        
        # SMC
        valid_smc, smc_score, smc_reasons = analyze_smc(df, side)
        if not valid_smc: return None
        
        # Quant & Deriv (UPDATED UNPACKING)
        df, basis, z_score, zeta_score, obi, quant_score, quant_reasons = calculate_metrics(df, ticker_info)
        
        valid_deriv, deriv_score, deriv_reasons = analyze_derivatives(df, ticker_info, side)
        if not valid_deriv: return None
        
        div_score, div_msg = detect_divergence(df)
        tech_score = 3 + div_score + smc_score
        tech_reasons = [f"Pattern: {pattern}", div_msg] + smc_reasons
        
        # Bias
        if "Bearish" in btc_bias and side == "Long": return None
        if "Bullish" in btc_bias and side == "Short": return None
        
        valid_fo, fo_msg = check_fakeout(df, CONFIG['indicators']['min_rvol'])
        if not valid_fo: return None
        if tech_score < CONFIG['strategy']['min_tech_score']: return None

        # Setup
        s = CONFIG['setup']
        swing_high = df['high'].iloc[-100:].max()
        swing_low = df['low'].iloc[-100:].min()
        rng = swing_high - swing_low
        
        if side == 'Long':
            entry = (swing_high - (rng * s['fib_entry_start']) + swing_high - (rng * s['fib_entry_end'])) / 2
            sl = swing_low - (rng * s['fib_sl'])
            tp1, tp2, tp3 = swing_low + rng, swing_low + (rng*1.618), swing_low + (rng*2.618)
        else:
            entry = (swing_low + (rng * s['fib_entry_start']) + swing_low + (rng * s['fib_entry_end'])) / 2
            sl = swing_high + (rng * s['fib_sl'])
            tp1, tp2, tp3 = swing_high - rng, swing_high - (rng*1.618), swing_high - (rng*2.618)
            
        rr = calculate_rr(entry, sl, tp3)
        if rr < CONFIG['strategy'].get('risk_reward_min', 3.0): return None
        
        df['funding'] = float(ticker_info.get('info', {}).get('fundingRate', 0))
        seen_symbols.append(symbol)
        
        return {
            "Symbol": symbol, 
            "Side": side, 
            "Timeframe": timeframe, 
            "Pattern": pattern,
            "Entry": float(entry),       # <--- Cast to float
            "SL": float(sl),             # <--- Cast to float
            "TP1": float(tp1),           # <--- Cast to float
            "TP2": float(tp2),           # <--- Cast to float
            "TP3": float(tp3),           # <--- Cast to float
            "RR": float(rr),             # <--- Cast to float
            "Tech_Score": int(tech_score),   # <--- Cast to int
            "Quant_Score": int(quant_score), # <--- Cast to int
            "Deriv_Score": int(deriv_score), # <--- Cast to int
            "SMC_Score": int(smc_score),     # <--- Cast to int
            "Basis": float(basis),           # <--- Cast to float
            "Z_Score": float(z_score),       # <--- Cast to float
            "Zeta_Score": float(zeta_score), # <--- Cast to float
            "OBI": float(obi),               # <--- Cast to float
            "BTC_Bias": btc_bias, 
            "Reason": pattern,
            "Tech_Reasons": ", ".join(tech_reasons),
            "Quant_Reasons": ", ".join(quant_reasons),
            "Deriv_Reasons": ", ".join(deriv_reasons), 
            "df": df
        }
    except: return None

def scan():
    print(f"\n[{pd.Timestamp.now()}] 🔭 Scanning... Mode: {os.getenv('BOT_ENV', 'PROD')}")
    btc_bias = get_btc_bias()
    print(f"📊 BTC Bias: {btc_bias}")
    seen_symbols = []
    
    mkts = exchange.load_markets()
    syms = [s for s in mkts if mkts[s].get('swap') and mkts[s]['quote'] == 'USDT' and mkts[s].get('active')][:400]
    random.shuffle(syms)
    
    for tf in reversed(CONFIG['system']['timeframes']):
        with ThreadPoolExecutor(max_workers=CONFIG['system']['max_threads']) as ex:
            futures = [ex.submit(analyze_ticker, s, tf, btc_bias, seen_symbols) for s in syms]
            for f in as_completed(futures):
                res = f.result()
                if res: send_alert(res)

if __name__ == "__main__":
    init_db()
    scan()
    schedule.every(CONFIG['system']['check_interval_hours']).hours.do(scan)
    schedule.every(1).minutes.do(run_fast_update)
    print("🚀 Bot Started.")
    while True: schedule.run_pending(); time.sleep(1)