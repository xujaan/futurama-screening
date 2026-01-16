import ccxt
import time
import schedule
import random
import os
import pandas as pd
import pandas_ta as ta
import numpy as np
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from modules.config_loader import CONFIG
from modules.database import init_db, get_active_signals
from modules.technicals import get_technicals, detect_divergence
from modules.quant import calculate_metrics, check_fakeout
from modules.derivatives import analyze_derivatives
from modules.smc import analyze_smc
from modules.patterns import find_pattern
from modules.discord_bot import send_alert, update_status_dashboard, run_fast_update, send_scan_completion

exchange = ccxt.bybit({'apiKey': CONFIG['api']['bybit_key'], 'secret': CONFIG['api']['bybit_secret'], 'options': {'defaultType': 'swap'}})

def get_btc_bias():
    try:
        bars = exchange.fetch_ohlcv('BTC/USDT', '1d', limit=100)
        if not bars: return "Sideways"
        df = pd.DataFrame(bars, columns=['t','o','h','l','c','v'])
        df['ema13'] = ta.ema(df['c'], length=13)
        df['ema21'] = ta.ema(df['c'], length=21)
        curr = df.iloc[-1]
        return "Bullish" if curr['ema13'] > curr['ema21'] else "Bearish"
    except: return "Sideways"

def calculate_rr(entry, sl, tp3):
    if entry <= 0 or sl <= 0 or tp3 <= 0: return 0.0
    risk = abs(entry - sl)
    return round(abs(tp3 - entry) / risk, 2) if risk > 0 else 0.0

def analyze_ticker(symbol, timeframe, btc_bias, active_signals):
    # 1. DUPLICATE CHECK
    if (symbol, timeframe) in active_signals: return None
    
    try:
        ticker_info = exchange.fetch_ticker(symbol)
        if "ST" in ticker_info.get('info', {}).get('symbol', ''): return None
        
        min_candles = CONFIG['system'].get('min_candles_analysis', 150)
        bars = exchange.fetch_ohlcv(symbol, timeframe, limit=min_candles + 50)
        if not bars or len(bars) < min_candles: return None
            
        df = pd.DataFrame(bars, columns=['timestamp','open','high','low','close','volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # 2. Technicals & Pattern
        df = get_technicals(df)
        pattern = find_pattern(df)
        if not pattern: return None
        side = CONFIG['pattern_signals'].get(pattern)
        
        # 3. SMC Analysis (Optional Filter)
        valid_smc, smc_score, smc_reasons = analyze_smc(df, side)
        # if not valid_smc: return None  # Un-comment for strict SMC

        # 4. Quant & Deriv Metrics
        df, basis, z_score, zeta_score, obi, quant_score, quant_reasons = calculate_metrics(df, ticker_info)
        valid_deriv, deriv_score, deriv_reasons = analyze_derivatives(df, ticker_info, side)
        if not valid_deriv: return None
        
        # 5. Scores & Bias
        div_score, div_msg = detect_divergence(df)
        tech_score = 3 + div_score
        tech_reasons = [f"Pattern: {pattern}", div_msg] + smc_reasons

        total_score = tech_score + smc_score + quant_score + deriv_score
        
        if "Bearish" in btc_bias and side == "Long": return None
        if "Bullish" in btc_bias and side == "Short": return None
        
        valid_fo, fo_msg = check_fakeout(df, CONFIG['indicators']['min_rvol'])
        if not valid_fo: return None

        if tech_score < CONFIG['strategy']['min_tech_score']: return None

        # 6. Setup Calculation
        s = CONFIG['setup']
        swing_high = df['high'].iloc[-50:].max()
        swing_low = df['low'].iloc[-50:].min()
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
        if rr < CONFIG['strategy'].get('risk_reward_min', 2.0): return None
        
        df['funding'] = float(ticker_info.get('info', {}).get('fundingRate', 0))
        
        # 7. Return Data (Type Casted)
        return {
            "Symbol": symbol, "Side": side, "Timeframe": timeframe, "Pattern": pattern,
            "Entry": float(entry), "SL": float(sl), "TP1": float(tp1), "TP2": float(tp2), "TP3": float(tp3), "RR": float(rr),
            "Tech_Score": int(tech_score), "Quant_Score": int(quant_score), 
            "Deriv_Score": int(deriv_score), "SMC_Score": int(smc_score),
            "Basis": float(basis), "Z_Score": float(z_score), "Zeta_Score": float(zeta_score), "OBI": float(obi),
            "BTC_Bias": btc_bias, "Reason": pattern, 
            "Tech_Reasons": ", ".join(tech_reasons),
            "Quant_Reasons": ", ".join(quant_reasons),
            "SMC_Reasons": ", ".join([r for r in smc_reasons if r]), # <--- NEW FIELD
            "Deriv_Reasons": ", ".join(deriv_reasons), "df": df
        }
    except: return None

def scan():
    start_time = time.time()
    print(f"\n[{pd.Timestamp.now()}] 🔭 Scanning... Mode: {os.getenv('BOT_ENV', 'PROD')}")
    btc_bias = get_btc_bias()
    print(f"📊 BTC Bias: {btc_bias}")
    
    active_signals = get_active_signals()
    print(f"🛡️ Active Signals Ignored: {len(active_signals)}")
    signal_count = 0 
    
    try:
        mkts = exchange.load_markets()
        
        # 🚫 LIST OF STABLECOINS TO IGNORE (As Base Currency)
        STABLECOINS = ['USDC', 'USDT', 'DAI', 'FDUSD', 'USDD', 'USDE', 'TUSD', 'BUSD', 'PYUSD', 'USDS', 'EUR', 'USD']

        # Filter Logic:
        # 1. Must be a Swap (Perpetual)
        # 2. Quote currency must be USDT
        # 3. Must be Active (trading enabled)
        # 4. Base currency MUST NOT be a stablecoin
        syms = [
            s for s in mkts 
            if mkts[s].get('swap') 
            and mkts[s]['quote'] == 'USDT' 
            and mkts[s].get('active')
            and mkts[s]['base'] not in STABLECOINS # <--- STABLECOIN FILTER
        ]
        
        random.shuffle(syms) 
        
        print(f"🔍 Scanning {len(syms)} valid pairs (Stables removed)...")

        for tf in reversed(CONFIG['system']['timeframes']):
            with ThreadPoolExecutor(max_workers=CONFIG['system']['max_threads']) as ex:
                futures = [ex.submit(analyze_ticker, s, tf, btc_bias, active_signals) for s in syms]
                for f in as_completed(futures):
                    res = f.result()
                    if res: 
                        success = send_alert(res)
                        if success: signal_count += 1
                        
    except Exception as e: print(f"Scan Error: {e}")
    finally:
        duration = time.time() - start_time
        print(f"✅ Scan Finished in {duration:.2f}s. Signals: {signal_count}")
        send_scan_completion(signal_count, duration, btc_bias)

if __name__ == "__main__":
    init_db()
    scan()
    schedule.every(CONFIG['system']['check_interval_hours']).hours.do(scan)
    schedule.every(1).minutes.do(run_fast_update)
    print("🚀 Bot Started.")
    while True: schedule.run_pending(); time.sleep(1)