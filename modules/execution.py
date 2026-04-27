"""
Tujuan: Eksekusi order ke bursa (Binance, Bitget, Bybit) dengan manajemen leverage dinamis dan adaptive SL.
Caller: main.py, auto_trades.py
Dependensi: ccxt, modules.database, modules.config_loader, modules.technicals
Main Functions: execute_entry(), place_layered_tps(), close_position()
Side Effects: REST API Call ke bursa (Create/Cancel Order), Database Read (Risk Config).
"""
import ccxt
import time
import math
import pandas as pd
from modules.technicals import calculate_atr, find_swing_low, find_swing_high, calculate_dynamic_sl

def set_leverage(exchange, symbol, lev):
    try:
        exchange.set_leverage(lev, symbol)
    except Exception as e:
        if 'not modified' not in str(e).lower() and 'same' not in str(e).lower():
            print(f"Leverage Warning for {symbol}: {e}")

def execute_entry(exchange, res):
    from modules.database import get_risk_config
    from modules.config_loader import CONFIG
    risk_cfg = get_risk_config()
    
    symbol = res['Symbol']
    side = 'buy' if res['Side'] == 'Long' else 'sell'
    entry_price = float(res['Entry'])
    sl = float(res['SL'])
    tf = res.get('Timeframe')
    
    # --- Strategy Routing ---
    strategy = 'NORMAL'
    grid_max = 1
    tp1 = res.get('TP1')
    tp2 = res.get('TP2')
    tp3 = res.get('TP3')

    if tf in ['15m', '1h']:
        strategy = 'SCALPING'
        s_cfg = CONFIG.get('scalping_setup', {'tp_percentage': 1.5, 'sl_percentage': 1.0})
        tp_pct = s_cfg['tp_percentage'] / 100
        
        # --- ADAPTIVE SL LOGIC (ATR & SWING LOW) ---
        try:
            # 1. Fetch data if not provided in res
            df_local = res.get('df')
            if df_local is None:
                bars = exchange.fetch_ohlcv(symbol, tf, limit=60)
                df_local = pd.DataFrame(bars, columns=['timestamp','open','high','low','close','volume'])
            
            # 2. Calculate Adaptive SL using dynamic function
            sl, s_val, atr = calculate_dynamic_sl(df_local, side, entry_price, lookback=30)
            
            # 3. Apply Fallback check (Max 3% from entry)
            if side == 'buy':
                tp1 = entry_price * (1 + tp_pct)
                max_sl = entry_price * 0.97 
                sl = max(sl, max_sl)
                logger_msg = f"DEBUG [Scalping]: {symbol} Side: Long, Entry: {entry_price}, SwingLow: {s_val}, ATR: {atr}, Final SL: {sl}"
            else:
                tp1 = entry_price * (1 - tp_pct)
                max_sl = entry_price * 1.03
                sl = min(sl, max_sl)
                logger_msg = f"DEBUG [Scalping]: {symbol} Side: Short, Entry: {entry_price}, SwingHigh: {s_val}, ATR: {atr}, Final SL: {sl}"
            
            print(logger_msg)
                
        except Exception as e:
            print(f"⚠️ Error calculating adaptive SL for {symbol}: {e}. Falling back to fixed SL.")
            sl_pct = s_cfg['sl_percentage'] / 100
            if side == 'buy':
                tp1 = entry_price * (1 + tp_pct)
                sl = entry_price * (1 - sl_pct)
            else:
                tp1 = entry_price * (1 - tp_pct)
                sl = entry_price * (1 + sl_pct)
        
        tp2, tp3 = None, None
    elif tf in ['4h', '1d', '1w']:
        strategy = 'GRID'
        grid_max = CONFIG.get('grid_setup', {}).get('max_layers', 4)

    try:
        market = exchange.market(symbol)
    except Exception as e:
        print(f"Gagal memuat market {symbol}: {e}")
        return False
        
    total_cap = risk_cfg['total_trading_capital_usdt']
    max_trades = risk_cfg['max_concurrent_trades']
    
    total_score = res.get('Total_Score', 6)
    if total_score >= 9: alloc_scale = 1.0
    elif total_score >= 7: alloc_scale = 0.75
    else: alloc_scale = 0.50
        
    margin_per_trade = (total_cap / max_trades) * alloc_scale
    
    # 🌟 Kalkulator Jarak SL ke Leverage Dinamis
    sl_dist_pct = abs(entry_price - sl) / entry_price
    if sl_dist_pct == 0: sl_dist_pct = 0.01 
    dynamic_lev = math.floor(0.90 / sl_dist_pct) 
    leverage = min(dynamic_lev, risk_cfg['max_leverage_limit'])
    leverage = max(1, int(leverage))

    set_leverage(exchange, symbol, leverage)
    
    pos_usd = margin_per_trade * leverage
    raw_qty = pos_usd / entry_price
    
    qty_str = exchange.amount_to_precision(symbol, raw_qty)
    qty = float(qty_str)
    price_str = exchange.price_to_precision(symbol, entry_price)
    
    if qty <= 0:
        print(f"❌ Order {symbol} failed: Qty too small")
        return False
        
    print(f"🚀 (Manual/Auto) {strategy} Order {symbol} | Entry: {price_str}")
    
    params = {'stopLoss': float(sl)}
    if tp1: params['takeProfit'] = float(tp1)
        
    try:
        order = exchange.create_order(symbol, 'limit', side, qty_str, price_str, params)
        print(f"✅ Entry Sukses! ID: {order.get('id')}")
        
        if strategy == 'GRID' and order.get('id'):
            # Place Subsequent Layers
            g_cfg = CONFIG.get('grid_setup', {'price_step_percentage': 2.5, 'martingale_multiplier': 2.0})
            price_step = g_cfg['price_step_percentage'] / 100
            multiplier = g_cfg['martingale_multiplier']
            curr_p, curr_q = entry_price, qty
            for i in range(2, grid_max + 1):
                curr_p = curr_p * (1 - price_step) if side == 'buy' else curr_p * (1 + price_step)
                curr_q = curr_q * multiplier
                p_s = exchange.price_to_precision(symbol, curr_p)
                q_s = exchange.amount_to_precision(symbol, curr_q)
                try: exchange.create_order(symbol, 'limit', side, q_s, p_s, {'reduceOnly': False})
                except: pass

        mmr = 0.005
        liq_price = entry_price * (1 - 1/leverage + mmr) if side == 'buy' else entry_price * (1 + 1/leverage - mmr)
            
        return {
            "success": True,
            "order_id": order.get('id', 'N/A'),
            "symbol": symbol,
            "side": "LONG" if side == 'buy' else "SHORT",
            "margin": margin_per_trade,
            "leverage": leverage,
            "qty": qty,
            "entry_price": entry_price,
            "sl": sl,
            "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "liq_price": liq_price,
            "strategy": strategy,
            "grid_max": grid_max
        }
    except Exception as e:
        print(f"❌ Gagal eksekusi {symbol}: {e}")
        return False

def place_layered_tps(exchange, symbol, pos_side, tp1, tp2, tp3, total_qty):
    side = 'sell' if pos_side.lower() == 'long' else 'buy'
    
    q1 = exchange.amount_to_precision(symbol, float(total_qty) * 0.33)
    q2 = exchange.amount_to_precision(symbol, float(total_qty) * 0.33)
    q3 = exchange.amount_to_precision(symbol, float(total_qty) - float(q1) - float(q2))
    
    tps = [(tp1, q1), (tp2, q2), (tp3, q3)]
    
    print(f"🎯 Menerbitkan 3 Lapis Take Profit untuk {symbol}")
    for tp_price, qty in tps:
        if float(qty) <= 0: continue
        price_str = exchange.price_to_precision(symbol, tp_price)
        params = {'reduceOnly': True}
        try:
            exchange.create_order(symbol, 'limit', side, qty, price_str, params)
            print(f"   ✅ TP Limit Order di harga {price_str} (Qty: {qty}) terbawa")
        except Exception as e:
            print(f"   ❌ Gagal pasang TP di {price_str}: {e}")

def close_position(exchange, symbol):
    try:
        positions = exchange.fetch_positions([symbol])
        pos = next((p for p in positions if float(p.get('contracts', 0)) > 0), None)
        
        if not pos:
            return False, f"No active position found for {symbol}"
            
        pos_side = pos['side'].lower()
        qty = float(pos['contracts'])
        target_side = 'sell' if pos_side == 'long' else 'buy'
        qty_str = exchange.amount_to_precision(symbol, qty)
        
        print(f"🛑 Closing Position {symbol} | Market {target_side} | Qty: {qty_str}")
        exchange.create_order(symbol, 'market', target_side, qty_str, params={'reduceOnly': True})
        exchange.cancel_all_orders(symbol)
        
        return True, f"Closed {symbol} successfully."
    except Exception as e:
        err_msg = f"Failed to close {symbol}: {e}"
        print(f"❌ {err_msg}")
        return False, err_msg
