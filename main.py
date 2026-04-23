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
SCAN_ABORT_FLAG = False
AUTOSCAN_ENABLED = False
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
        # Fetching extra 200 bars as warmup padding for SMA_200 before dropna wipes them
        bars = exchange.fetch_ohlcv(symbol, timeframe, limit=min_candles + 200)
        if not bars or len(bars) < min_candles: return None
            
        df = pd.DataFrame(bars, columns=['timestamp','open','high','low','close','volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # 2. Technicals & Pattern
        df = get_technicals(df)
        pattern = find_pattern(df)
        side = None
        if pattern: 
            side = CONFIG['pattern_signals'].get(pattern)
        
        # 3. SMC Analysis (Optional Filter or Fallback)
        if side:
            valid_smc, smc_score, smc_reasons = analyze_smc(df, side)
        else:
            # If no classical pattern, we test SMC pure zones
            _, long_score, long_reas = analyze_smc(df, "Long")
            _, short_score, short_reas = analyze_smc(df, "Short")
            if long_score > short_score and long_score > 0:
                side, pattern, smc_score, smc_reasons = "Long", "SMC Zone", long_score, long_reas
            elif short_score > long_score and short_score > 0:
                side, pattern, smc_score, smc_reasons = "Short", "SMC Zone", short_score, short_reas
            else:
                return None
                
        if smc_score < CONFIG['strategy'].get('min_smc_score', 0):
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
        tech_score = 3 + div_score # Base score 3. Will pass natively, but drop to 2 (fail) if explicitly counter-trend!
        
        regime = detect_regime(df)
        is_squeezing, squeeze_firing = check_volatility_squeeze(df)
        
        # -- Phase B: MTC Logic --
        if timeframe in ['1w', '1d', '4h']:
            macro_cache[symbol] = regime
        else:
            macro_regime = macro_cache.get(symbol)
            if macro_regime == "Trending Bull" and side == "Short": tech_score -= 1 # Counter macro trend
            if macro_regime == "Trending Bear" and side == "Long": tech_score -= 1
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
            else: tech_score -= 1 # Counter micro trend
        elif regime == "Trending Bear":
            if side == "Short": tech_score += 1
            else: tech_score -= 1 # Counter micro trend

        tech_reasons.append(f"Regime: {regime}")
        
        total_score = tech_score + smc_score + quant_score + deriv_score
        
        if "Bearish" in btc_bias and side == "Long": tech_score -= 1
        if "Bullish" in btc_bias and side == "Short": tech_score -= 1
        
        valid_fo, fo_msg = check_fakeout(df, CONFIG['indicators']['min_rvol'])
        if not valid_fo: 
            quant_score -= 1
        else:
            if fo_msg: quant_reasons.append(fo_msg)

        if tech_score < CONFIG['strategy']['min_tech_score']: return None
        if quant_score < CONFIG['strategy'].get('min_quant_score', 0): return None

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
    global SCAN_ABORT_FLAG
    SCAN_ABORT_FLAG = False
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
        progress_callback(f"🔍 **Initiating Scan**\nBTC Bias: `{btc_bias}`\nFetching markets from {platform.upper()}...")
        
    active_signals = get_active_signals()
    print(f"🛡️ Active Signals Ignored: {len(active_signals)}")
    signal_count = 0 
    all_dispatched = []
    
    try:
        if progress_callback: progress_callback(f"🔍 **Filtering Markets**\nStripping inactive/stablecoins...")
        mkts = exchange.load_markets()
        
        # 🚫 LIST OF STABLECOINS TO IGNORE (As Base Currency)
        STABLECOINS = ['USDC', 'USDT', 'DAI', 'FDUSD', 'USDD', 'USDE', 'TUSD', 'BUSD', 'PYUSD', 'USDS', 'EUR', 'USD']

        # Filter Logic:
        # 1. Must be a Perpetual Swap (type == 'swap' and swap == True)
        # 2. Quote currency must be USDT
        # 3. Must be Active (trading enabled)
        # 4. Base currency MUST NOT be a stablecoin
        syms = [
            s for s in mkts 
            if mkts[s].get('swap') == True
            and mkts[s].get('type') == 'swap'
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
            if SCAN_ABORT_FLAG: break
            scan_results = []
            for s_idx, s in enumerate(syms):
                if SCAN_ABORT_FLAG: break
                
                # Real-time progress update every 20 pairs (~3 seconds) to prevent TG rate limit
                if s_idx % 20 == 0 and progress_callback:
                    progress_callback(f"⏳ **Analyzing Timeframe {tf}** ({i+1}/{len(tfs)})\nScanning: {s_idx}/{c} pairs... (`{s}`)")
                    
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
                if SCAN_ABORT_FLAG: break
                success = send_alert(res)
                if success: 
                    signal_count += 1
                    all_dispatched.append(res)
                    if risk_cfg.get('auto_trade', False):
                        if active_pos_count < risk_cfg.get('max_concurrent_trades', 2):
                            import modules.execution as execution
                            if execution.execute_entry(exchange, res):
                                active_pos_count += 1
                        else:
                            print(f"⏩ Skipped {res['Symbol']} (Quota full: {active_pos_count}/{risk_cfg.get('max_concurrent_trades', 2)})")
                        
    except Exception as e: 
        print(f"Scan Error: {e}")
        if progress_callback: progress_callback(f"❌ **Scan Fault:** \n`{str(e)}`")
    finally:
        duration = time.time() - start_time
        if SCAN_ABORT_FLAG:
            print("🛑 Scan Aborted by User.")
            if progress_callback: progress_callback(f"🛑 **Scan Aborted.**")
        else:
            print(f"✅ Scan Finished in {duration:.2f}s. Signals: {signal_count}")
            send_scan_completion(signal_count, duration, btc_bias, all_dispatched)
            if progress_callback: progress_callback(f"✅ **Scan Completed in {duration:.1f}s!**\nDispatched **{signal_count}** valid signals.")

if __name__ == "__main__":
    init_db()
    
    from modules.telegram_listener import TelegramListener
    tg_listener = TelegramListener(exchange=exchange)
    tg_listener.start()
    
    print("🚀 Bot Started. type /start to start autoscan.")
    while True: 
        if AUTOSCAN_ENABLED:
            try:
                print("🔄 Running AutoScan cycle...")
                SCAN_ABORT_FLAG = False
                scan()
                
                if AUTOSCAN_ENABLED and not SCAN_ABORT_FLAG:
                    interval = CONFIG['system'].get('check_interval_hours', 1)
                    print(f"💤 Scan finished. Sleeping for {interval} hours.")
                    sleep_secs = int(interval * 3600)
                    for _ in range(sleep_secs):
                        if not AUTOSCAN_ENABLED or SCAN_ABORT_FLAG: break
                        time.sleep(1)
            except Exception as e:
                print(f"❌ Autoscan Error: {e}")
                time.sleep(60)
        else:
            time.sleep(1)