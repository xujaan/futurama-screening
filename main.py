import matplotlib
matplotlib.use('Agg')
import ccxt
import time
import schedule
import random
import os
import pandas as pd
import pandas_ta_classic as ta
import numpy as np
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from modules.config_loader import CONFIG
from modules.database import init_db, get_active_signals
from modules.technicals import get_technicals, detect_divergence, check_volatility_squeeze, detect_regime
from modules.quant import calculate_metrics, check_fakeout
from modules.derivatives import analyze_derivatives
from modules.smc import analyze_smc
from modules.patterns import find_pattern
from modules.bot import send_alert, update_status_dashboard, run_fast_update, send_scan_completion

from modules.exchange_manager import get_current_exchange

exchange = get_current_exchange()
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

def analyze_ticker(symbol, timeframe, btc_bias, active_signals, macro_cache):
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
        if smc_score < CONFIG['strategy'].get('min_smc_score', 0):
            # print(f"❌ {symbol} rejected: SMC Score too low ({smc_score})")
            return None
        # if not valid_smc: return None  # Un-comment for strict SMC

        # 4. Quant & Deriv Metrics
        df, basis, z_score, zeta_score, obi, quant_score, quant_reasons = calculate_metrics(df, ticker_info)
        valid_deriv, deriv_score, deriv_reasons = analyze_derivatives(df, ticker_info, side)
        if not valid_deriv: return None

        if deriv_score < CONFIG['strategy'].get('min_deriv_score', 0):
            # print(f"❌ {symbol} rejected: Deriv Score too low ({deriv_score})")
            return None
        
        # 5. Scores & Bias
        div_score, div_msg = detect_divergence(df)
        tech_score = 3 + div_score
        
        regime = detect_regime(df)
        is_squeezing, squeeze_firing = check_volatility_squeeze(df)
        
        # -- Phase B: MTC Logic --
        if timeframe in ['1w', '1d', '4h']:
            macro_cache[symbol] = regime
        else:
            macro_regime = macro_cache.get(symbol)
            if macro_regime == "Trending Bull" and side == "Short": return None
            if macro_regime == "Trending Bear" and side == "Long": return None
            if macro_regime: tech_reasons.append(f"MTC Aligned")
            
        tech_reasons = [f"Pattern: {pattern}", div_msg] + smc_reasons
        if squeeze_firing: 
            tech_score += 2
            tech_reasons.append("💥 Squeeze Firing")
        elif is_squeezing: 
            tech_score += 1
            tech_reasons.append("🗜️ Squeeze ON")
            
        if regime == "Trending Bull":
            if side == "Long": tech_score += 1
            else: return None # Strict trend alignment
        elif regime == "Trending Bear":
            if side == "Short": tech_score += 1
            else: return None # Strict trend alignment

        tech_reasons.append(f"Regime: {regime}")
        
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
        natr_val = df['NATR_14'].iloc[-1] if 'NATR_14' in df.columns else 0.0
        return {
            "Symbol": symbol, "Side": side, "Timeframe": timeframe, "Pattern": pattern,
            "Entry": float(entry), "SL": float(sl), "TP1": float(tp1), "TP2": float(tp2), "TP3": float(tp3), "RR": float(rr),
            "Tech_Score": int(tech_score), "Quant_Score": int(quant_score), 
            "Deriv_Score": int(deriv_score), "SMC_Score": int(smc_score),
            "Basis": float(basis), "Z_Score": float(z_score), "Zeta_Score": float(zeta_score), "OBI": float(obi),
            "NATR": float(natr_val),
            "BTC_Bias": btc_bias, "Reason": pattern, 
            "Tech_Reasons": ", ".join(tech_reasons),
            "Quant_Reasons": ", ".join(quant_reasons),
            "SMC_Reasons": ", ".join([r for r in smc_reasons if r]), # <--- NEW FIELD
            "Deriv_Reasons": ", ".join(deriv_reasons), "df": df
        }
    except: return None

def scan(progress_callback=None):
    start_time = time.time()
    msg = f"\n[{pd.Timestamp.now()}] 🔭 Scanning... Mode: {os.getenv('BOT_ENV', 'PROD')}"
    print(msg)
    
    global exchange
    exchange = get_current_exchange(force_reload=True)
    platform = exchange.id if exchange else "Unknown"
    print(f"🏢 Active CEX: {platform.upper()}")
    
    btc_bias = get_btc_bias()
    print(f"📊 BTC Bias: {btc_bias}")
    
    if progress_callback:
        progress_callback(f"🔍 **Mulai Scanning**\nBias BTC: `{btc_bias}`\nMengambil daftar market Bybit...")
        
    active_signals = get_active_signals()
    print(f"🛡️ Active Signals Ignored: {len(active_signals)}")
    signal_count = 0 
    
    try:
        if progress_callback: progress_callback(f"🔍 **Memfilter Koin**\nMenyingkirkan koin mati/stablecoin...")
        mkts = exchange.load_markets()
        
        # 🚫 LIST OF STABLECOINS TO IGNORE (As Base Currency)
        STABLECOINS = ['USDC', 'USDT', 'DAI', 'FDUSD', 'USDD', 'USDE', 'TUSD', 'BUSD', 'PYUSD', 'USDS', 'EUR', 'USD']

        # Filter Logic:
        # 1. Must be a Swap (Perpetual)
        # 2. Quote currency must be USDT
        # 3. Must be Active (trading enabled)
        # Base currency MUST NOT be a stablecoin
        # Different exchanges might report quote differently
        syms = [
            s for s in mkts 
            if mkts[s].get('swap') or mkts[s].get('future') or mkts[s].get('linear')
            and mkts[s].get('quote') == 'USDT' 
            and mkts[s].get('active', True)
            and mkts[s].get('base') not in STABLECOINS
        ]
        
        random.shuffle(syms) 
        
        c = len(syms)
        print(f"🔍 Scanning {c} valid pairs (Stables removed)...")

        tfs = CONFIG['system']['timeframes']
        macro_cache = {} # MTC Phase cache
        
        for i, tf in enumerate(reversed(tfs)):
            if progress_callback: progress_callback(f"⏳ **Menganalisa Timeframe {tf}** ({i+1}/{len(tfs)})\nMemindai {c} koin...")
            scan_results = []
            for s in syms:
                try:
                    res = analyze_ticker(s, tf, btc_bias, active_signals, macro_cache)
                    if res: scan_results.append(res)
                except Exception as e:
                    print(f"Error on {s}: {e}")
            
            # Sort by total score
            for res in scan_results:
                res['Total_Score'] = res.get('Tech_Score', 0) + res.get('SMC_Score', 0) + res.get('Quant_Score', 0) + res.get('Deriv_Score', 0)
            scan_results.sort(key=lambda x: x['Total_Score'], reverse=True)
            
            from modules.database import get_risk_config
            risk_cfg = get_risk_config()
            active_pos_count = 0
            if risk_cfg.get('auto_trade', False):
                try:
                    positions = exchange.fetch_positions()
                    active_pos_count = len([p for p in positions if float(p['contracts']) > 0])
                except Exception as e: print("Gagal fetch pos limit:", e)
                
            for res in scan_results:
                success = send_alert(res)
                if success: 
                    signal_count += 1
                    if risk_cfg.get('auto_trade', False):
                        if active_pos_count < risk_cfg.get('max_concurrent_trades', 2):
                            import modules.execution as execution
                            if execution.execute_entry(exchange, res):
                                active_pos_count += 1
                        else:
                            print(f"⏩ Melewati {res['Symbol']} (Kuota penuh: {active_pos_count}/{risk_cfg.get('max_concurrent_trades', 2)})")
                        
    except Exception as e: 
        print(f"Scan Error: {e}")
        if progress_callback: progress_callback(f"❌ **Error saat scanning:** \n`{str(e)}`")
    finally:
        duration = time.time() - start_time
        print(f"✅ Scan Finished in {duration:.2f}s. Signals: {signal_count}")
        send_scan_completion(signal_count, duration, btc_bias)
        if progress_callback: progress_callback(f"✅ **Scanning Selesai dalam {duration:.1f} detik!**\nDitemukan **{signal_count}** sinyal valid dikirim.")

if __name__ == "__main__":
    init_db()
    
    from modules.telegram_listener import TelegramListener
    tg_listener = TelegramListener(exchange=exchange)
    tg_listener.start()
    
    scan()
    schedule.every(CONFIG['system']['check_interval_hours']).hours.do(scan)
    schedule.every(1).minutes.do(run_fast_update, exchange=exchange)
    print("🚀 Bot Started.")
    while True: schedule.run_pending(); time.sleep(1)