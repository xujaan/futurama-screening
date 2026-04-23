import ccxt
import time
import math

def set_leverage(exchange, symbol, lev):
    try:
        exchange.set_leverage(lev, symbol)
    except Exception as e:
        if 'not modified' not in str(e).lower() and 'same' not in str(e).lower():
            print(f"Leverage Warning for {symbol}: {e}")

def execute_entry(exchange, res):
    from modules.database import get_risk_config
    risk_cfg = get_risk_config()
    
    symbol = res['Symbol']
    side = 'buy' if res['Side'] == 'Long' else 'sell'
    entry_price = float(res['Entry'])
    sl = float(res['SL'])
    
    try:
        market = exchange.market(symbol)
    except Exception as e:
        print(f"Gagal memuat market {symbol}: {e}")
        return False
        
    total_cap = risk_cfg['total_trading_capital_usdt']
    max_trades = risk_cfg['max_concurrent_trades']
    
    total_score = res.get('Total_Score', 6)
    if total_score >= 9:
        alloc_scale = 1.0
    elif total_score >= 7:
        alloc_scale = 0.75
    else:
        alloc_scale = 0.50
        
    margin_per_trade = (total_cap / max_trades) * alloc_scale
    
    # 🌟 Kalkulator Jarak SL ke Leverage Dinamis
    sl_dist_pct = abs(entry_price - sl) / entry_price
    if sl_dist_pct == 0: sl_dist_pct = 0.01 # Pencegah error
    dynamic_lev = math.floor(0.90 / sl_dist_pct) # Buffer keamanan 90%
    leverage = min(dynamic_lev, risk_cfg['max_leverage_limit'])
    leverage = max(1, int(leverage))

    set_leverage(exchange, symbol, leverage)
    
    pos_usd = margin_per_trade * leverage
    raw_qty = pos_usd / entry_price
    
    qty_str = exchange.amount_to_precision(symbol, raw_qty)
    qty = float(qty_str)
    price_str = exchange.price_to_precision(symbol, entry_price)
    
    if qty <= 0:
        print(f"❌ Order {symbol} failed: Qty is zero or too small")
        return False
        
    print(f"🚀 (Auto-Trade) LIMIT Order {symbol} | Mgn: ${margin_per_trade:.2f} | Lev: {leverage}x | Qty: {qty} | Entry: {price_str}")
    
    params = {
        'stopLoss': float(sl)
    }
    
    tp3 = res.get('TP3')
    tp1 = res.get('TP1')
    # if tp3:
    #     params['takeProfit'] = float(tp3)
    if tp1:
        params['takeProfit'] = float(tp1)
        
    try:
        order = exchange.create_order(symbol, 'limit', side, qty_str, price_str, params)
        print(f"✅ LIMIT Order Entry Sukses! ID: {order.get('id')}")
        
        mmr = 0.005
        if side == 'buy':
            liq_price = entry_price * (1 - 1/leverage + mmr)
        else:
            liq_price = entry_price * (1 + 1/leverage - mmr)
            
        return {
            "success": True,
            "order_id": order.get('id', 'N/A'),
            "symbol": symbol,
            "side": side.upper(),
            "margin": margin_per_trade,
            "total_cap": total_cap,
            "leverage": leverage,
            "qty": qty,
            "entry_price": entry_price,
            "sl": sl,
            "liq_price": liq_price
        }
    except Exception as e:
        print(f"❌ Gagal mengeksekusi order {symbol}: {e}")
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
