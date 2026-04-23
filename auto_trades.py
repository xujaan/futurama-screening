"""
Tujuan: Mengotomatisasi siklus hidup trading (Entry, Limit TPs, SL to Breakeven, Close) untuk koin-koin yang disaring.
Caller: Standalone script (Background Worker).
Dependensi: ccxt, pybit, sqlite3, schedule.
Main Functions: ingest_fresh_signals(), check_pending_orders(), poll_positions(), check_missed_tps()
Side Effects: Write/Read dari SQLite. Eksekusi orders ke bursa (Binance/Bitget/Bybit).
"""

import ccxt
import time
import schedule
import threading
import logging
import math
from datetime import datetime
from pybit.unified_trading import WebSocket
from modules.config_loader import CONFIG
from modules.database import get_conn, release_conn, get_active_cex
from modules.exchange_manager import get_current_exchange

TARGET_LEVERAGE = 25    
RISK_PERCENT = 0.01           
MAX_POSITIONS = 40            
TP_SPLIT = [0.30, 0.30, 0.40] 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("AutoTrader")

# Globals to hold active connection states
active_engine = {
    'platform': None,
    'exchange': None,
    'ws': None
}

def sync_active_exchange():
    """Reloads CEX instance safely if switched dynamically in DB."""
    current_cex = get_active_cex()
    
    if active_engine['platform'] != current_cex:
        logger.info(f"🔄 CEX Switch Detected! Loading {current_cex.upper()}")
        active_engine['platform'] = current_cex
        active_engine['exchange'] = get_current_exchange(force_reload=True)
        
        # Close old WS if it exists
        if active_engine['ws']:
            try: active_engine['ws'].close()
            except: pass
            active_engine['ws'] = None
            
        # If bybit, init optimized WebSocket
        if current_cex == 'bybit':
            try:
                keys = CONFIG['api'].get('bybit', {})
                ws = WebSocket(
                    testnet=False,
                    channel_type="private",
                    api_key=keys.get('key', ''),
                    api_secret=keys.get('secret', ''),
                )
                ws.execution_stream(callback=on_execution_update)
                ws.position_stream(callback=on_position_update)
                active_engine['ws'] = ws
                logger.info("🔌 Bybit WebSocket Connected.")
            except Exception as e:
                logger.error(f"Failed to connect Bybit WS: {e}")
                
def init_execution_db():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS active_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INT,
                symbol VARCHAR(20),
                side VARCHAR(10),
                entry_price DECIMAL,
                sl_price DECIMAL,
                tp1 DECIMAL,
                tp2 DECIMAL,
                tp3 DECIMAL,
                quantity DECIMAL,
                leverage INT,
                order_id VARCHAR(50),
                status VARCHAR(20) DEFAULT 'PENDING',
                pnl DECIMAL DEFAULT 0,
                is_sl_moved BOOLEAN DEFAULT FALSE,
                trailing_active BOOLEAN DEFAULT FALSE,
                trailing_stop_price DECIMAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_reports (
                report_date DATE PRIMARY KEY,
                total_pnl DECIMAL DEFAULT 0,
                win_rate DECIMAL DEFAULT 0,
                total_wins INT DEFAULT 0,
                total_losses INT DEFAULT 0,
                total_trades INT DEFAULT 0,
                best_trade_symbol VARCHAR(20),
                best_trade_pnl DECIMAL,
                worst_trade_symbol VARCHAR(20),
                worst_trade_pnl DECIMAL,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        logger.info("✅ Execution DB Synced.")
    except Exception as e:
        logger.error(f"❌ DB Init Error: {e}")
    finally:
        release_conn(conn)

def place_split_tps(symbol, side, total_qty, tp1, tp2, tp3):
    try:
        side_str = str(side).lower()
        tp_side = 'sell' if side_str in ['buy', 'long'] else 'buy'
        
        exchange = active_engine['exchange']
        if not exchange: return False
        
        q1 = float(exchange.amount_to_precision(symbol, total_qty * TP_SPLIT[0]))
        q2 = float(exchange.amount_to_precision(symbol, total_qty * TP_SPLIT[1]))
        
        # We NO LONGER limit TP3! We let it trail!
        params = {'reduceOnly': True}
        
        logger.info(f"⚡ Placing TPs for {symbol} ({tp_side.upper()}): {q1} | {q2} | Trailing 40%")
        
        exchange.create_order(symbol, 'limit', tp_side, q1, float(tp1), params)
        exchange.create_order(symbol, 'limit', tp_side, q2, float(tp2), params)
        
        return True
    except Exception as e:
        logger.error(f"⚠️ TP Fail {symbol}: {e}")
        return False

# --- BYBIT WEBSOCKET HANDLERS ---
def on_execution_update(message):
    try:
        data = message.get('data', [])
        for exec_item in data:
            if exec_item.get('execType') == 'Trade':
                conn = get_conn()
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT id, tp1, tp2, tp3 FROM active_trades WHERE symbol = ? AND status = 'OPEN'", (exec_item['symbol'],))
                    row = cur.fetchone()
                    if row:
                        t_id, tp1, tp2, tp3 = row
                        pos = active_engine['exchange'].fetch_position(exec_item['symbol'])
                        size = float(pos['contracts'])
                        if size > 0:
                            if place_split_tps(exec_item['symbol'], exec_item['side'], size, tp1, tp2, tp3):
                                cur.execute("UPDATE active_trades SET status = 'OPEN_TPS_SET', updated_at = datetime('now') WHERE id = ?", (t_id,))
                                conn.commit()
                except: pass
                finally: release_conn(conn)
    except: pass

def on_position_update(message):
    try:
        data = message.get('data', [])
        for pos in data:
            symbol = pos['symbol']
            size = float(pos['size'])
            mark_price = float(pos['markPrice'])
            side = pos['side']
            
            conn = get_conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT id, entry_price, tp1, is_sl_moved, status FROM active_trades WHERE symbol = ? AND status = 'OPEN_TPS_SET'", (symbol,))
                row = cur.fetchone()
                
                if row:
                    t_id, entry, tp1, sl_moved, status = row
                    
                    if size == 0:
                        logger.info(f"🏁 {symbol} Pos Closed (WS).")
                        time.sleep(1)
                        try:
                            trades = active_engine['exchange'].fetch_my_trades(symbol, limit=1)
                            real_pnl = float(trades[0]['info'].get('closedPnl', 0)) if trades else 0
                            cur.execute("UPDATE active_trades SET status = 'CLOSED', pnl = ?, updated_at = datetime('now') WHERE id = ?", (real_pnl, t_id))
                        except:
                            cur.execute("UPDATE active_trades SET status = 'CLOSED', updated_at = datetime('now') WHERE id = ?", (t_id,))
                        conn.commit()
                        return

                    hit_tp1 = (side == 'Buy' and mark_price >= float(tp1)) or (side == 'Sell' and mark_price <= float(tp1))
                    if hit_tp1 and not sl_moved:
                        logger.info(f"♻️ {symbol} TP1 Hit. BEP...")
                        try:
                            active_engine['exchange'].set_position_stop_loss(symbol, float(entry), side.lower())
                            cur.execute("UPDATE active_trades SET is_sl_moved = 1 WHERE id = ?", (t_id,))
                            conn.commit()
                        except: pass
            except: pass
            finally: release_conn(conn)
    except: pass


# --- CCXT POLLING ENGINE (For Binance/Bitget) ---
def ccxt_poll_positions():
    """Fallback Poller for CEXs that don't have WebSocket implemented here."""
    exchange = active_engine['exchange']
    if not exchange or active_engine['platform'] == 'bybit': return # Bybit uses WS
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # 1. Manage Entries waiting for TPs ('OPEN')
        cur.execute("SELECT id, symbol, side, tp1, tp2, tp3 FROM active_trades WHERE status = 'OPEN'")
        open_trades = cur.fetchall()
        for t in open_trades:
            t_id, sym, side_str, tp1, tp2, tp3 = t
            try:
                pos = exchange.fetch_position(sym)
                size = float(pos.get('contracts', 0))
                if size > 0:
                    if place_split_tps(sym, side_str, size, tp1, tp2, tp3):
                        cur.execute("UPDATE active_trades SET status = 'OPEN_TPS_SET', updated_at = datetime('now') WHERE id = ?", (t_id,))
            except Exception as e:
                logger.error(f"Poll Entry Error {sym}: {e}")
                
        # 2. Manage 'OPEN_TPS_SET' for PnL, SL move, and Trailing Stop
        cur.execute("SELECT t.id, t.symbol, t.side, t.entry_price, t.tp1, t.tp2, t.is_sl_moved, t.trailing_active, t.trailing_stop_price, s.natr FROM active_trades t LEFT JOIN trades s ON t.signal_id = s.id WHERE t.status = 'OPEN_TPS_SET'")
        active_tps = cur.fetchall()
        for t in active_tps:
            t_id, sym, side_str, entry, tp1, tp2, sl_moved, trail_active, trail_stop, natr_val = t
            try:
                pos = exchange.fetch_position(sym)
                size = float(pos.get('contracts', 0))
                
                # Check Closure
                if size <= 0:
                    try:
                        trades = exchange.fetch_my_trades(sym, limit=1)
                        pnl = sum([float(tr['info'].get('realizedPnl', 0)) for tr in trades])
                    except: pnl = 0
                    cur.execute("UPDATE active_trades SET status = 'CLOSED', pnl = ?, updated_at = datetime('now') WHERE id = ?", (pnl, t_id))
                    logger.info(f"🏁 {sym} Pos Closed (Poll).")
                    continue
                
                mark = float(pos.get('markPrice', 0))
                
                # Check BEP Move (TP1 Hit)
                hit_tp1 = (side_str == 'Long' and mark >= float(tp1)) or (side_str == 'Short' and mark <= float(tp1))
                if hit_tp1 and not sl_moved:
                    logger.info(f"♻️ {sym} TP1 Hit. Modifying SL to Break Even...")
                    try:
                        exchange.create_order(sym, 'stopMarket', 'sell' if side_str == 'Long' else 'buy', size, params={'stopPrice': float(entry), 'reduceOnly': True})
                        cur.execute("UPDATE active_trades SET is_sl_moved = 1 WHERE id = ?", (t_id,))
                    except: pass 
                    
                # Check Trailing Activation (TP2 Hit)
                hit_tp2 = (side_str == 'Long' and mark >= float(tp2)) or (side_str == 'Short' and mark <= float(tp2))
                if hit_tp2 and not trail_active:
                    logger.info(f"🚀 {sym} TP2 Hit! Activating Chandelier Trailing Stop...")
                    cur.execute("UPDATE active_trades SET trailing_active = 1, trailing_stop_price = ? WHERE id = ?", (float(entry), t_id))
                    trail_active = True
                    trail_stop = float(entry)
                    
                # Trailing Logic Execution
                if trail_active:
                    atr_buffer = float(natr_val if natr_val else (mark * 0.02)) * 2 # 2x ATR buffer or 4% default
                    if side_str == 'Long':
                        new_trail = mark - atr_buffer
                        if not trail_stop or new_trail > float(trail_stop):
                            cur.execute("UPDATE active_trades SET trailing_stop_price = ? WHERE id = ?", (new_trail, t_id))
                            try: exchange.create_order(sym, 'stopMarket', 'sell', size, params={'stopPrice': new_trail, 'reduceOnly': True})
                            except: pass
                    else:
                        new_trail = mark + atr_buffer
                        if not trail_stop or new_trail < float(trail_stop):
                            cur.execute("UPDATE active_trades SET trailing_stop_price = ? WHERE id = ?", (new_trail, t_id))
                            try: exchange.create_order(sym, 'stopMarket', 'buy', size, params={'stopPrice': new_trail, 'reduceOnly': True})
                            except: pass
            except: pass
            
        conn.commit()
    except Exception as e:
        logger.error(f"Poller Error: {e}")
    finally:
        release_conn(conn)


# --- GENERAL LOGIC ---
def ingest_fresh_signals():
    exchange = active_engine['exchange']
    if not exchange: return
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM active_trades WHERE status IN ('OPEN', 'OPEN_TPS_SET')")
        current_active = cur.fetchone()[0]
        if current_active >= MAX_POSITIONS: return

        balance = exchange.fetch_balance()
        total_equity = float(balance['total'].get('USDT', 0))
        markets = exchange.load_markets()

        query = """
            SELECT t.id, t.symbol, t.side, t.entry_price, t.sl_price, t.tp1, t.tp2, t.tp3, t.natr
            FROM trades t
            LEFT JOIN active_trades a ON t.id = a.signal_id
            WHERE t.status = 'Waiting Entry'
            AND t.created_at >= datetime('now', '-12 hours')
            AND a.id IS NULL
        """
        cur.execute(query)
        signals = cur.fetchall()
        
        for sig in signals:
            if current_active >= MAX_POSITIONS: break
            sig_id, sym, side, entry, sl, tp1, tp2, tp3, natr = sig
            entry, sl = float(entry), float(sl)
            
            market = markets.get(sym)
            max_lev = 25
            if market and 'limits' in market and 'leverage' in market['limits']:
                try: max_lev = float(market['limits']['leverage'].get('max', 25))
                except: pass
            
            final_leverage = min(TARGET_LEVERAGE, int(max_lev))
            margin_cost = total_equity * RISK_PERCENT
            
            # --- ATR Volatility Risk Sizing ---
            natr_val = float(natr) if natr else 0.0
            multiplier = 1.0
            if natr_val > 15.0: multiplier = 0.5     # Extreme Meme Volatility (Half Risk)
            elif natr_val > 8.0: multiplier = 0.75   # High Volatility (Trim Risk)
            
            position_value = (margin_cost * final_leverage) * multiplier
            qty_coins = position_value / entry
            
            # Most CEX require minimal mapping (~$5-$6 value)
            if position_value < 6.0: continue

            cur.execute("""
                INSERT INTO active_trades (signal_id, symbol, side, entry_price, sl_price, tp1, tp2, tp3, quantity, leverage, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
            """, (sig_id, sym, side, entry, sl, tp1, tp2, tp3, qty_coins, final_leverage))
            
            logger.info(f"📥 Signal Ingested: {sym} | Lev: {final_leverage}x | Cost: ${margin_cost:.2f}")
            current_active += 1
            
        conn.commit()
    except Exception as e: logger.error(f"Ingest Error: {e}")
    finally: release_conn(conn)

def execute_pending_orders():
    exchange = active_engine['exchange']
    if not exchange: return
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, symbol, side, entry_price, sl_price, tp3, quantity, leverage FROM active_trades WHERE status = 'PENDING'")
        orders = cur.fetchall()
        if not orders: return 

        for order in orders:
            oid, sym, side, entry, sl, tp3_val, qty, lev = order
            try:
                try: exchange.set_leverage(int(lev), sym)
                except: pass

                ticker = exchange.fetch_ticker(sym)
                current_price = float(ticker['last'])
                entry = float(entry)
                
                is_better_price = (side == 'Long' and current_price <= entry) or (side == 'Short' and current_price >= entry)
                type_side = 'buy' if side == 'Long' else 'sell'
                
                params = {'stopLoss': float(sl)}
                if tp3_val:
                    params['takeProfit'] = float(tp3_val)
                    
                qty = float(exchange.amount_to_precision(sym, qty))
                
                if is_better_price:
                    res = exchange.create_order(sym, 'market', type_side, qty, None, params)
                else:
                    res = exchange.create_order(sym, 'limit', type_side, entry, qty, params)
                
                if res and 'id' in res:
                    cur.execute("UPDATE active_trades SET order_id = ?, status = 'OPEN' WHERE id = ?", (res['id'], oid))
                    conn.commit()
                    logger.info(f"✅ Order Placed for {sym} (ID: {res['id']})")
            except Exception as e:
                logger.error(f"❌ Execution Failed {sym}: {e}")
                cur.execute("UPDATE active_trades SET status = 'FAILED' WHERE id = ?", (oid,))
                conn.commit()
    except Exception as e: logger.error(f"Exec Loop Error: {e}")
    finally: release_conn(conn)

def check_missed_tps():
    exchange = active_engine['exchange']
    if not exchange: return
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, symbol, side, order_id, tp1, tp2, tp3 FROM active_trades WHERE status = 'OPEN' AND order_id IS NOT NULL")
        stuck_trades = cur.fetchall()
        for trade in stuck_trades:
            t_id, sym, side, oid, tp1, tp2, tp3 = trade
            try:
                order_status = None
                try:
                    order = exchange.fetch_order(oid, sym)
                    order_status = order['status']
                except: pass

                if order_status == 'closed':
                    pos = exchange.fetch_position(sym)
                    size = float(pos.get('contracts', 0))
                    if size > 0:
                        if place_split_tps(sym, side, size, tp1, tp2, tp3):
                            cur.execute("UPDATE active_trades SET status = 'OPEN_TPS_SET', updated_at = datetime('now') WHERE id = ?", (t_id,))
                            conn.commit()
                            logger.info(f"✅ TP Recovered for {sym}")
                elif order_status in ['canceled', 'rejected']:
                    cur.execute("UPDATE active_trades SET status = 'CANCELLED' WHERE id = ?", (t_id,))
                    conn.commit()
            except: pass
    except: pass
    finally: release_conn(conn)

if __name__ == "__main__":
    logger.info("🟢 Starting Multi-CEX Auto-Trader...")
    init_execution_db()
    
    schedule.every(3).seconds.do(sync_active_exchange)
    schedule.every(10).seconds.do(ccxt_poll_positions) # Fallback for Binance/Bitget
    schedule.every(1).minutes.do(ingest_fresh_signals)      
    schedule.every(5).seconds.do(execute_pending_orders)    
    schedule.every(20).seconds.do(check_missed_tps)         
    
    logger.info(f"🚀 Bot is LIVE. Monitoring {MAX_POSITIONS} Max Positions.")
    while True:
        schedule.run_pending()
        time.sleep(1)