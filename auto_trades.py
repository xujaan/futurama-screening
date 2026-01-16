import ccxt
import time
import schedule
import threading
import logging
from datetime import datetime
from pybit.unified_trading import WebSocket
from modules.config_loader import CONFIG
from modules.database import get_conn, release_conn

# --- ⚙️ CONFIGURATION ---
TARGET_LEVERAGE = 25    
RISK_PERCENT = 0.01          # Risk 1% of Equity per trade
MAX_POSITIONS = 20           # Max Concurrent OPEN positions
TP_SPLIT = [0.30, 0.30, 0.40] # 30% TP1, 30% TP2, 40% TP3

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AutoTrader")

# REST API Connection (CCXT)
exchange = ccxt.bybit({
    'apiKey': CONFIG['api']['bybit_key'],
    'secret': CONFIG['api']['bybit_secret'],
    'options': {'defaultType': 'swap', 'adjustForTimeDifference': True}
})

# ---------------------------------------------------------
# 🛠️ DATABASE INITIALIZATION (Self-Healing)
# ---------------------------------------------------------
def init_execution_db():
    """Creates execution tables if they don't exist."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # 1. Active Trades Table (Isolated Execution)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS active_trades (
                id SERIAL PRIMARY KEY,
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 2. Daily Reports Table
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
        logger.info("✅ Execution Database Tables Sync Complete.")
    except Exception as e:
        logger.error(f"❌ DB Init Error: {e}")
    finally:
        release_conn(conn)

# ---------------------------------------------------------
# ⚡ WEBSOCKET EVENT HANDLERS (Real-Time Brain)
# ---------------------------------------------------------
def place_split_tps(symbol, side, total_qty, tp1, tp2, tp3):
    """
    Called instantly via WebSocket when Entry is Filled.
    Places 3 Reduce-Only Limit orders.
    """
    try:
        # Determine TP side (Opposite of Entry)
        tp_side = 'sell' if side.lower() == 'buy' else 'buy'
        
        # Calculate Splits
        q1 = float(exchange.amount_to_precision(symbol, total_qty * TP_SPLIT[0]))
        q2 = float(exchange.amount_to_precision(symbol, total_qty * TP_SPLIT[1]))
        q3 = float(exchange.amount_to_precision(symbol, total_qty * TP_SPLIT[2]))
        
        # Adjust remainder to ensure 100% close
        current_sum = q1 + q2 + q3
        if current_sum != total_qty:
            diff = total_qty - current_sum
            q3 += diff
            q3 = float(exchange.amount_to_precision(symbol, q3))

        params = {'reduceOnly': True}
        
        # Batch execution could be used here, but sequential is fine for now
        exchange.create_order(symbol, 'limit', tp_side, q1, float(tp1), params)
        exchange.create_order(symbol, 'limit', tp_side, q2, float(tp2), params)
        exchange.create_order(symbol, 'limit', tp_side, q3, float(tp3), params)
        
        logger.info(f"⚡ TPs Placed for {symbol}: {q1}@{tp1} | {q2}@{tp2} | {q3}@{tp3}")
        return True
    except Exception as e:
        logger.error(f"⚠️ TP Placement Failed {symbol}: {e}")
        return False

def on_execution_update(message):
    """
    WebSocket Callback: Listens for Order Fills ('Trade').
    Triggers TP placement logic.
    """
    try:
        data = message.get('data', [])
        for exec_item in data:
            symbol = exec_item['symbol']
            side = exec_item['side']
            exec_type = exec_item.get('execType') 
            
            # Filter: Only care about 'Trade' (Fills)
            if exec_type == 'Trade':
                conn = get_conn()
                try:
                    cur = conn.cursor()
                    
                    # Check if we have an OPEN trade waiting for TPs
                    cur.execute("""
                        SELECT id, tp1, tp2, tp3 
                        FROM active_trades 
                        WHERE symbol = %s AND status = 'OPEN'
                    """, (symbol,))
                    row = cur.fetchone()
                    
                    if row:
                        t_id, tp1, tp2, tp3 = row
                        logger.info(f"⚡ WS: Entry Filled for {symbol}! Placing TPs...")
                        
                        # Double check position size from REST to be accurate
                        pos = exchange.fetch_position(symbol)
                        current_size = float(pos['contracts'])
                        
                        if current_size > 0:
                            success = place_split_tps(symbol, side, current_size, tp1, tp2, tp3)
                            if success:
                                cur.execute("UPDATE active_trades SET status = 'OPEN_TPS_SET', updated_at = NOW() WHERE id = %s", (t_id,))
                                conn.commit()
                except Exception as e:
                    logger.error(f"WS Exec Logic Error: {e}")
                finally:
                    release_conn(conn)
    except Exception as e:
        logger.error(f"WS Payload Error: {e}")

def on_position_update(message):
    """
    WebSocket Callback: Listens for PnL/Price updates.
    Handles BEP moves and Close detection.
    """
    try:
        data = message.get('data', [])
        for pos in data:
            symbol = pos['symbol']
            size = float(pos['size'])
            mark_price = float(pos['markPrice'])
            side = pos['side'] # 'Buy' or 'Sell'
            
            conn = get_conn()
            try:
                cur = conn.cursor()
                
                # Fetch trade info
                cur.execute("""
                    SELECT id, entry_price, tp1, is_sl_moved, status 
                    FROM active_trades 
                    WHERE symbol = %s AND status = 'OPEN_TPS_SET'
                """, (symbol,))
                row = cur.fetchone()
                
                if row:
                    t_id, entry, tp1, sl_moved, status = row
                    
                    # 1. POSITION CLOSED CHECK (Size -> 0)
                    if size == 0:
                        logger.info(f"🏁 WS: {symbol} Position Closed. Fetching PnL...")
                        time.sleep(1) # Allow Bybit backend to settle PnL
                        try:
                            trades = exchange.fetch_my_trades(symbol, limit=1)
                            real_pnl = float(trades[0]['info'].get('closedPnl', 0)) if trades else 0
                            cur.execute("UPDATE active_trades SET status = 'CLOSED', pnl = %s, updated_at = NOW() WHERE id = %s", (real_pnl, t_id))
                        except:
                            cur.execute("UPDATE active_trades SET status = 'CLOSED', updated_at = NOW() WHERE id = %s", (t_id,))
                        conn.commit()
                        return

                    # 2. BREAKEVEN LOGIC (Hit TP1 -> Move SL)
                    # For Long: Mark >= TP1. For Short: Mark <= TP1
                    hit_tp1 = (side == 'Buy' and mark_price >= float(tp1)) or \
                              (side == 'Sell' and mark_price <= float(tp1))
                    
                    if hit_tp1 and not sl_moved:
                        logger.info(f"♻️ WS: {symbol} hit TP1. Moving SL to Entry...")
                        try:
                            exchange.set_position_stop_loss(symbol, float(entry), side.lower())
                            cur.execute("UPDATE active_trades SET is_sl_moved = TRUE WHERE id = %s", (t_id,))
                            conn.commit()
                        except Exception as sl_err:
                            logger.error(f"⚠️ Failed to move SL for {symbol}: {sl_err}")

            except Exception as e:
                # logger.error(f"WS Pos Error: {e}") # Silent fail on DB locks
                pass
            finally:
                release_conn(conn)
    except: pass

# ---------------------------------------------------------
# 📥 SIGNAL INGESTION (Polling)
# ---------------------------------------------------------
def ingest_fresh_signals():
    """Reads 'Waiting Entry' from scanner table, applies Risk/Lev, inserts to active_trades."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # 1. Check Max Positions (OPEN only)
        cur.execute("SELECT COUNT(*) FROM active_trades WHERE status IN ('OPEN', 'OPEN_TPS_SET')")
        current_active = cur.fetchone()[0]
        
        if current_active >= MAX_POSITIONS:
            # logger.info(f"🚫 Max positions ({current_active}) reached.")
            return

        # 2. Fetch Data needed for calc
        try:
            balance = exchange.fetch_balance()
            total_equity = float(balance['total']['USDT'])
            markets = exchange.load_markets()
        except Exception as e:
            logger.error(f"API Fetch Error: {e}")
            return

        # 3. Get New Signals
        query = """
            SELECT t.id, t.symbol, t.side, t.entry_price, t.sl_price, t.tp1, t.tp2, t.tp3
            FROM trades t
            LEFT JOIN active_trades a ON t.id = a.signal_id
            WHERE t.status = 'Waiting Entry'
            AND t.created_at >= NOW() - INTERVAL '24 hours'
            AND a.id IS NULL
        """
        cur.execute(query)
        signals = cur.fetchall()
        
        for sig in signals:
            if current_active >= MAX_POSITIONS: break
            
            sig_id, sym, side, entry, sl, tp1, tp2, tp3 = sig
            entry, sl = float(entry), float(sl)
            
            # A. Dynamic Leverage (Target 25x or Max)
            market = markets.get(sym)
            max_lev = 25
            if market and 'limits' in market:
                limit_lev = market['limits']['leverage']['max']
                if limit_lev: max_lev = float(limit_lev)
            
            final_leverage = min(TARGET_LEVERAGE, int(max_lev))
            
            # B. Risk Calc (1% of Equity)
            # Qty = Risk ($) / |Entry - SL|
            risk_amt = total_equity * RISK_PERCENT
            price_diff = abs(entry - sl)
            
            if price_diff == 0: continue
            
            qty_coins = risk_amt / price_diff
            
            # Check Min Notional (Approx $6 for Bybit)
            notional = qty_coins * entry
            if notional < 6.0:
                logger.warning(f"⚠️ Signal {sym} skipped: Position size ${notional:.2f} too small.")
                continue

            # C. Insert PENDING Trade
            cur.execute("""
                INSERT INTO active_trades (signal_id, symbol, side, entry_price, sl_price, tp1, tp2, tp3, quantity, leverage, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING')
            """, (sig_id, sym, side, entry, sl, tp1, tp2, tp3, qty_coins, final_leverage))
            
            logger.info(f"📥 Signal Ingested: {sym} | Lev: {final_leverage}x | Size: ${notional:.1f}")
            current_active += 1
            
        conn.commit()
    except Exception as e:
        logger.error(f"Ingest Error: {e}")
    finally:
        release_conn(conn)

# ---------------------------------------------------------
# 🚀 SMART EXECUTION (With Debugging)
# ---------------------------------------------------------
def execute_pending_orders():
    conn = get_conn()
    try:
        cur = conn.cursor()
        # Fetch pending orders
        cur.execute("SELECT id, symbol, side, entry_price, sl_price, quantity, leverage FROM active_trades WHERE status = 'PENDING'")
        orders = cur.fetchall()
        
        if not orders:
            # print("💤 No pending orders to execute.") # Uncomment to verify loop is running
            return

        logger.info(f"🔎 Found {len(orders)} PENDING orders. Processing...")

        for order in orders:
            oid, sym, side, entry, sl, qty, lev = order
            logger.info(f"👉 Processing {sym}: {side} | Qty: {qty} | Entry: {entry}")

            try:
                # 1. Set Leverage
                try: 
                    exchange.set_leverage(int(lev), sym)
                    logger.info(f"   ✅ Leverage set to {lev}x for {sym}")
                except Exception as e: 
                    # Leverage often fails if already set, which is fine.
                    logger.warning(f"   ⚠️ Leverage set skipped/failed: {e}")

                # 2. Check LIVE Price
                ticker = exchange.fetch_ticker(sym)
                current_price = float(ticker['last'])
                entry = float(entry)
                
                # Logic: Is the price better than our entry?
                is_better_price = (side == 'Long' and current_price <= entry) or \
                                  (side == 'Short' and current_price >= entry)

                type_side = 'buy' if side == 'Long' else 'sell'
                params = {'stopLoss': float(sl)}
                
                # Precision Handling (Crucial for execution success)
                qty = float(exchange.amount_to_precision(sym, qty))
                
                logger.info(f"   📊 Price Check: Market ${current_price} vs Entry ${entry}")

                res = None
                
                # 3. Execution Decision
                if is_better_price:
                    logger.info(f"   ⚡ ACTION: MARKET ORDER (Price is better)")
                    res = exchange.create_order(sym, 'market', type_side, qty, None, params)
                else:
                    logger.info(f"   ⏳ ACTION: LIMIT ORDER (Waiting for price)")
                    res = exchange.create_order(sym, 'limit', type_side, entry, qty, params)
                
                # 4. Verification & DB Update
                if res and 'id' in res:
                    order_id = res['id']
                    order_status = res['status'] # 'open', 'closed' (filled)
                    
                    cur.execute("UPDATE active_trades SET order_id = %s, status = 'OPEN' WHERE id = %s", (order_id, oid))
                    conn.commit()
                    
                    logger.info(f"   ✅ SUCCESS: Order Placed! ID: {order_id} | Status: {order_status}")
                    logger.info(f"   📝 DB Updated for Trade ID {oid}")
                else:
                    logger.error(f"   ❌ FAILED: API returned no ID. Response: {res}")

            except Exception as e:
                logger.error(f"   ❌ CRITICAL EXECUTION ERROR {sym}: {e}")
                # Optional: Mark failed so it doesn't retry infinitely?
                # cur.execute("UPDATE active_trades SET status = 'FAILED' WHERE id = %s", (oid,))
                # conn.commit()
                
    except Exception as e:
        logger.error(f"Global Exec Loop Error: {e}")
    finally:
        release_conn(conn)

# ---------------------------------------------------------
# 📊 DAILY REPORTING
# ---------------------------------------------------------
def generate_daily_report():
    logger.info("📊 Generating Daily Report...")
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                COUNT(*),
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END),
                SUM(pnl),
                MAX(pnl),
                MIN(pnl)
            FROM active_trades 
            WHERE status = 'CLOSED' 
            AND updated_at >= NOW() - INTERVAL '24 hours'
        """)
        row = cur.fetchone()
        
        if row and row[0] > 0:
            total, wins, losses, pnl, best, worst = row
            pnl = pnl if pnl else 0
            win_rate = (wins/total)*100
            
            # Fetch symbols
            cur.execute("SELECT symbol FROM active_trades WHERE pnl = %s LIMIT 1", (best,))
            b_sym = cur.fetchone(); best_sym = b_sym[0] if b_sym else "-"
            
            cur.execute("SELECT symbol FROM active_trades WHERE pnl = %s LIMIT 1", (worst,))
            w_sym = cur.fetchone(); worst_sym = w_sym[0] if w_sym else "-"
            
            cur.execute("""
                INSERT INTO daily_reports (report_date, total_pnl, win_rate, total_wins, total_losses, total_trades, best_trade_symbol, best_trade_pnl, worst_trade_symbol, worst_trade_pnl)
                VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (report_date) DO NOTHING
            """, (pnl, win_rate, wins, losses, total, best_sym, best, worst_sym, worst))
            conn.commit()
            logger.info(f"✅ Report Generated: ${pnl:.2f} ({wins}W/{losses}L)")
            
    except Exception as e:
        logger.error(f"Report Error: {e}")
    finally:
        release_conn(conn)

# ---------------------------------------------------------
# 🏁 MAIN
# ---------------------------------------------------------
if __name__ == "__main__":
    logger.info("🟢 Starting Auto-Trader (Hybrid Architecture)...")
    
    # 1. Init DB
    init_execution_db()
    
    # 2. Start WebSocket (Background Thread)
    ws = WebSocket(
        testnet=False,
        channel_type="private",
        api_key=CONFIG['api']['bybit_key'],
        api_secret=CONFIG['api']['bybit_secret'],
    )
    ws.execution_stream(callback=on_execution_update)
    ws.position_stream(callback=on_position_update)
    logger.info("🔌 WebSocket Connected.")
    
    # 3. Schedule Jobs (Foreground)
    schedule.every(1).minutes.do(ingest_fresh_signals)
    schedule.every(10).seconds.do(execute_pending_orders) # Fast polling for pending
    schedule.every().day.at("00:00").do(generate_daily_report)
    
    logger.info("🚀 Bot is LIVE. Press Ctrl+C to stop.")
    
    while True:
        schedule.run_pending()
        time.sleep(1)