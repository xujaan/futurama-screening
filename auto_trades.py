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
import pandas as pd
from modules.technicals import (
    calculate_dynamic_sl,
    calculate_trade_progress,
    detect_momentum_loss,
    detect_rejection_signal,
    is_long_side,
)
from datetime import datetime
from pybit.unified_trading import WebSocket
from modules.config_loader import CONFIG
from modules.database import get_conn, release_conn, get_active_cex, init_execution_db as sync_execution_db
from modules.exchange_manager import get_current_exchange

TARGET_LEVERAGE = 25    
RISK_PERCENT = 0.01           
MAX_POSITIONS = 40            
TP_SPLIT = [0.30, 0.30, 0.40] 
ADAPTIVE_DEFAULTS = {
    'enabled': True,
    'check_interval_seconds': 30,
    'use_origin_timeframe': True,
    'candle_fetch_limit': 60,
    'bep_trigger_ratio_15m': 0.30,
    'bep_trigger_ratio_1h': 0.45,
    'profit_lock_ratio_1': 0.25,
    'profit_lock_ratio_2': 0.50,
    'profit_lock_ratio_3': 0.75,
    'tp_shrink_on_rejection': True,
    'allow_partial_close': True,
    'partial_close_ratio': 0.50,
    'max_stagnant_candles_15m': 4,
    'max_stagnant_candles_1h': 3,
    'stagnation_progress_threshold': 0.20,
    'tp_shrink_factor_15m': 0.88,
    'tp_shrink_factor_1h': 0.92,
    'level1_sl_buffer_ratio_15m': 0.15,
    'level1_sl_buffer_ratio_1h': 0.10,
    'level3_lock_ratio_15m': 0.35,
    'level3_lock_ratio_1h': 0.25,
    'early_exit_peak_threshold': 0.60,
    'early_exit_fallback_progress': 0.35,
    'min_sl_update_interval_seconds': 90,
    'min_tp_update_interval_seconds': 180,
    'min_partial_close_interval_seconds': 300,
    'min_early_exit_interval_seconds': 300,
    'min_sl_change_pct': 0.0015,
    'min_tp_change_pct': 0.0025,
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("AutoTrader")

# Globals to hold active connection states
active_engine = {
    'platform': None,
    'exchange': None,
    'ws': None
}

def get_adaptive_cfg():
    cfg = ADAPTIVE_DEFAULTS.copy()
    cfg.update(CONFIG.get('adaptive_management', {}))
    return cfg

def get_tf_profile(timeframe, cfg=None):
    cfg = cfg or get_adaptive_cfg()
    tf = str(timeframe or '15m')
    if tf == '15m':
        return {
            'level1_ratio': float(cfg.get('profit_lock_ratio_1', 0.25)),
            'level2_ratio': float(cfg.get('profit_lock_ratio_2', 0.50)),
            'level3_ratio': float(cfg.get('profit_lock_ratio_3', 0.75)),
            'bep_ratio': float(cfg.get('bep_trigger_ratio_15m', 0.30)),
            'max_stagnant_candles': int(cfg.get('max_stagnant_candles_15m', 4)),
            'level1_sl_buffer_ratio': float(cfg.get('level1_sl_buffer_ratio_15m', 0.15)),
            'level3_lock_ratio': float(cfg.get('level3_lock_ratio_15m', 0.35)),
            'tp_shrink_factor': float(cfg.get('tp_shrink_factor_15m', 0.88)),
        }
    return {
        'level1_ratio': max(0.30, float(cfg.get('profit_lock_ratio_1', 0.25))),
        'level2_ratio': max(0.60, float(cfg.get('profit_lock_ratio_2', 0.50))),
        'level3_ratio': max(0.80, float(cfg.get('profit_lock_ratio_3', 0.75))),
        'bep_ratio': float(cfg.get('bep_trigger_ratio_1h', 0.45)),
        'max_stagnant_candles': int(cfg.get('max_stagnant_candles_1h', 3)),
        'level1_sl_buffer_ratio': float(cfg.get('level1_sl_buffer_ratio_1h', 0.10)),
        'level3_lock_ratio': float(cfg.get('level3_lock_ratio_1h', 0.25)),
        'tp_shrink_factor': float(cfg.get('tp_shrink_factor_1h', 0.92)),
    }

def timeframe_to_minutes(timeframe):
    tf = str(timeframe or '').lower()
    if tf.endswith('m'):
        return int(tf[:-1] or 0)
    if tf.endswith('h'):
        return int(tf[:-1] or 0) * 60
    if tf.endswith('d'):
        return int(tf[:-1] or 0) * 1440
    if tf.endswith('w'):
        return int(tf[:-1] or 0) * 10080
    return 15

def parse_db_timestamp(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(str(value), fmt)
        except Exception:
            continue
    return None

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
    try:
        result = sync_execution_db()
        logger.info(
            "✅ Execution DB Synced. backfill=%s manual_sync=%s",
            result.get('backfilled_origin_timeframe', 0),
            result.get('synced_manual_closures', 0),
        )
    except Exception as e:
        logger.error(f"❌ DB Init Error: {e}")

def place_split_tps(symbol, side, total_qty, tp1, tp2, tp3, strategy='NORMAL'):
    try:
        side_str = str(side).lower()
        tp_side = 'sell' if side_str in ['buy', 'long'] else 'buy'
        
        exchange = active_engine['exchange']
        if not exchange: return False
        
        params = {'reduceOnly': True}
        
        if strategy in ['SCALPING', 'GRID']:
            if tp1 is None: return False
            q_str = exchange.amount_to_precision(symbol, total_qty)
            p_str = exchange.price_to_precision(symbol, tp1)
            logger.info(f"⚡ Placing Single TP ({strategy}) for {symbol}: {q_str} @ {p_str}")
            exchange.create_order(symbol, 'limit', tp_side, q_str, p_str, params)
            return True

        if tp1 is None or tp2 is None: return False
        q1 = float(exchange.amount_to_precision(symbol, total_qty * TP_SPLIT[0]))
        q2 = float(exchange.amount_to_precision(symbol, total_qty * TP_SPLIT[1]))
        
        logger.info(f"⚡ Placing Split TPs for {symbol} ({tp_side.upper()}): {q1} | {q2} | Trailing 40%")
        
        exchange.create_order(symbol, 'limit', tp_side, q1, float(tp1), params)
        exchange.create_order(symbol, 'limit', tp_side, q2, float(tp2), params)
        
        return True
    except Exception as e:
        logger.error(f"⚠️ TP Fail {symbol}: {e}")
        return False

def fetch_management_candles(exchange, symbol, timeframe, limit=60):
    bars = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def _is_reduce_only_order(order):
    value = order.get('reduceOnly')
    if value is None:
        value = order.get('info', {}).get('reduceOnly')
    return str(value).lower() in ['true', '1'] or value is True

def _get_order_stop_price(order):
    return order.get('stopPrice') or order.get('triggerPrice') or order.get('info', {}).get('stopPrice')

def cancel_reduce_only_orders(symbol, order_side=None, only_stop=False, only_limit=False):
    exchange = active_engine['exchange']
    if not exchange:
        return 0
    cancelled = 0
    try:
        open_orders = exchange.fetch_open_orders(symbol)
        for oo in open_orders:
            if order_side and str(oo.get('side', '')).lower() != str(order_side).lower():
                continue
            if not _is_reduce_only_order(oo):
                continue
            stop_price = _get_order_stop_price(oo)
            is_stop = bool(stop_price) or 'stop' in str(oo.get('type', '')).lower()
            if only_stop and not is_stop:
                continue
            if only_limit and is_stop:
                continue
            exchange.cancel_order(oo['id'], symbol)
            cancelled += 1
    except Exception as e:
        logger.error(f"Cancel reduce-only orders failed for {symbol}: {e}")
    return cancelled

def update_stop_loss_on_exchange(symbol, side, size, new_sl):
    exchange = active_engine['exchange']
    if not exchange or size <= 0:
        return False
    try:
        if active_engine['platform'] == 'bybit':
            exchange.set_position_stop_loss(symbol, float(new_sl), str(side).lower())
        else:
            cancel_reduce_only_orders(symbol, only_stop=True)
            exit_side = 'sell' if is_long_side(side) else 'buy'
            exchange.create_order(symbol, 'stopMarket', exit_side, size, params={'stopPrice': float(new_sl), 'reduceOnly': True})
        return True
    except Exception as e:
        logger.error(f"Failed SL update for {symbol}: {e}")
        return False

def update_take_profit_on_exchange(symbol, side, size, new_tp):
    exchange = active_engine['exchange']
    if not exchange or size <= 0 or not new_tp:
        return False
    try:
        exit_side = 'sell' if is_long_side(side) else 'buy'
        cancel_reduce_only_orders(symbol, order_side=exit_side, only_limit=True)
        qty_str = exchange.amount_to_precision(symbol, size)
        price_str = exchange.price_to_precision(symbol, float(new_tp))
        exchange.create_order(symbol, 'limit', exit_side, qty_str, price_str, {'reduceOnly': True})
        return True
    except Exception as e:
        logger.error(f"Failed TP update for {symbol}: {e}")
        return False

def execute_partial_close(symbol, side, size, close_ratio, note):
    exchange = active_engine['exchange']
    if not exchange or size <= 0:
        return False, 0.0
    try:
        close_qty = float(exchange.amount_to_precision(symbol, float(size) * float(close_ratio)))
        if close_qty <= 0 or close_qty >= float(size):
            return False, 0.0
        exit_side = 'sell' if is_long_side(side) else 'buy'
        exchange.create_order(symbol, 'market', exit_side, close_qty, params={'reduceOnly': True})
        logger.info(f"⚖️ Partial close {symbol} qty={close_qty} reason={note}")
        return True, close_qty
    except Exception as e:
        logger.error(f"Partial close failed for {symbol}: {e}")
        return False, 0.0

def execute_early_exit(symbol, side, size, note):
    exchange = active_engine['exchange']
    if not exchange or size <= 0:
        return False
    try:
        exit_side = 'sell' if is_long_side(side) else 'buy'
        qty_str = exchange.amount_to_precision(symbol, size)
        exchange.create_order(symbol, 'market', exit_side, qty_str, params={'reduceOnly': True})
        logger.info(f"🛑 Early exit {symbol} qty={qty_str} reason={note}")
        return True
    except Exception as e:
        logger.error(f"Early exit failed for {symbol}: {e}")
        return False

def detect_stagnation(trade_row, progress_ratio, origin_tf, cfg):
    created_at = parse_db_timestamp(trade_row.get('created_at'))
    if not created_at:
        return False, None
    elapsed_min = (datetime.utcnow() - created_at).total_seconds() / 60.0
    tf_mins = max(1, timeframe_to_minutes(origin_tf))
    candles_open = elapsed_min / tf_mins
    profile = get_tf_profile(origin_tf, cfg)
    if candles_open >= profile['max_stagnant_candles'] and progress_ratio < float(cfg.get('stagnation_progress_threshold', 0.20)):
        return True, f"stagnant_{origin_tf}_{candles_open:.1f}c"
    return False, None

def pct_change(old_value, new_value):
    try:
        old_value = float(old_value)
        new_value = float(new_value)
        if old_value == 0:
            return 1.0
        return abs(new_value - old_value) / abs(old_value)
    except Exception:
        return 0.0

def seconds_since(ts_value):
    dt = parse_db_timestamp(ts_value)
    if not dt:
        return None
    return (datetime.utcnow() - dt).total_seconds()

def action_allowed(last_action_at, action_type, cfg):
    elapsed = seconds_since(last_action_at)
    if elapsed is None:
        return True
    key_map = {
        'sl_update': 'min_sl_update_interval_seconds',
        'tp_update': 'min_tp_update_interval_seconds',
        'partial_close': 'min_partial_close_interval_seconds',
        'early_exit': 'min_early_exit_interval_seconds',
    }
    required = float(cfg.get(key_map.get(action_type, ''), 0))
    return elapsed >= required

def maybe_raise_profit_lock(sym, side_str, entry, tp1, current_sl, mark, size, trade_row, cfg, progress_ratio):
    profile = get_tf_profile(trade_row.get('origin_timeframe') or '15m', cfg)
    locked_level = int(trade_row.get('locked_profit_level') or 0)
    risk_dist = abs(float(entry) - float(current_sl))
    target_sl = None
    target_level = locked_level
    note = None

    if progress_ratio >= profile['level3_ratio'] and locked_level < 3:
        gain = abs(float(tp1) - float(entry))
        protected = gain * profile['level3_lock_ratio']
        target_sl = float(entry) + protected if is_long_side(side_str) else float(entry) - protected
        target_level = 3
        note = "profit_lock_level_3"
    elif progress_ratio >= profile['level2_ratio'] and locked_level < 2:
        gain = abs(float(tp1) - float(entry))
        protected = gain * 0.05
        target_sl = float(entry) + protected if is_long_side(side_str) else float(entry) - protected
        target_level = 2
        note = "profit_lock_level_2"
    elif progress_ratio >= profile['level1_ratio'] and locked_level < 1:
        buffer = risk_dist * profile['level1_sl_buffer_ratio']
        target_sl = float(entry) - buffer if is_long_side(side_str) else float(entry) + buffer
        target_level = 1
        note = "profit_lock_level_1"

    if target_sl is None:
        return locked_level, None

    better_sl = target_sl > float(current_sl) if is_long_side(side_str) else target_sl < float(current_sl)
    if better_sl and update_stop_loss_on_exchange(sym, side_str, size, target_sl):
        logger.info(f"🔒 {sym} progress={progress_ratio:.2f} level={target_level} action=move_sl reason={note}")
        return target_level, (target_sl, note)
    return target_level, None

# --- BYBIT WEBSOCKET HANDLERS ---
def on_execution_update(message):
    try:
        data = message.get('data', [])
        for exec_item in data:
            if exec_item.get('execType') == 'Trade':
                conn = get_conn()
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT id, tp1, tp2, tp3, strategy FROM active_trades WHERE symbol = ? AND status = 'OPEN'", (exec_item['symbol'],))
                    row = cur.fetchone()
                    if row:
                        t_id, tp1, tp2, tp3, strategy = row
                        pos = active_engine['exchange'].fetch_position(exec_item['symbol'])
                        size = float(pos['contracts'])
                        if size > 0:
                            if place_split_tps(exec_item['symbol'], exec_item['side'], size, tp1, tp2, tp3, strategy=strategy):
                                cur.execute("UPDATE active_trades SET status = 'OPEN_TPS_SET', updated_at = datetime('now') WHERE id = ?", (t_id,))
                                conn.commit()
                except Exception as e:
                    logger.error(f"Execution update handler error for {exec_item.get('symbol', '?')}: {e}")
                finally: release_conn(conn)
    except Exception as e:
        logger.error(f"Execution stream error: {e}")

def on_position_update(message):
    try:
        data = message.get('data', [])
        for pos_data in data:
            symbol = pos_data['symbol']
            size = float(pos_data['size'])
            mark_price = float(pos_data['markPrice'])
            side = pos_data['side']
            
            conn = get_conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT id, entry_price, tp1, is_sl_moved, status, strategy, quantity, avg_entry_price, origin_timeframe FROM active_trades WHERE symbol = ? AND status = 'OPEN_TPS_SET'", (symbol,))
                row = cur.fetchone()
                
                if row:
                    t_id, entry, tp1, sl_moved, status, strategy, recorded_qty, avg_entry, origin_tf = row
                    
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

                    # --- GRID LAYER MONITORING (Bybit WS) ---
                    if strategy == 'GRID' and size > float(recorded_qty) + 0.00001:
                        # fetch AEP from message or API
                        new_avg = float(pos_data.get('entryPrice', avg_entry))
                        g_cfg = CONFIG.get('grid_setup', {'take_profit_percentage': 1.5})
                        new_tp = new_avg * (1 + g_cfg['take_profit_percentage']/100) if side == 'Buy' else new_avg * (1 - g_cfg['take_profit_percentage']/100)
                        
                        logger.info(f"📈 {symbol} Grid Layer Filled (WS). New AEP: {new_avg}")
                        
                        try:
                            # Cancel old TP
                            exchange = active_engine['exchange']
                            open_orders = exchange.fetch_open_orders(symbol)
                            for oo in open_orders:
                                if oo.get('reduceOnly') == True:
                                    exchange.cancel_order(oo['id'], symbol)
                            
                            place_split_tps(symbol, side.lower(), size, new_tp, None, None, strategy='GRID')
                        except: pass
                        
                        cur.execute("UPDATE active_trades SET quantity = ?, avg_entry_price = ?, tp1 = ?, grid_layer = grid_layer + 1 WHERE id = ?", (size, new_avg, new_tp, t_id))
                        conn.commit()
                        recorded_qty, tp1 = size, new_tp

                    if strategy != 'GRID':
                        # --- BEP Trigger based on timeframe-specific adaptive ratio ---
                        if tp1 and not sl_moved:
                            dist_to_tp = abs(float(tp1) - entry)
                            profile = get_tf_profile(origin_tf or '15m', get_adaptive_cfg())
                            bep_ratio = profile['bep_ratio']
                            bep_threshold = entry + (dist_to_tp * bep_ratio) if side.lower() in ['buy', 'long'] else entry - (dist_to_tp * bep_ratio)
                            
                            is_bep_reached = (side.lower() in ['buy', 'long'] and mark_price >= bep_threshold) or \
                                             (side.lower() in ['sell', 'short'] and mark_price <= bep_threshold)
                            
                            if is_bep_reached:
                                logger.info(f"♻️ {symbol} 50% to TP1 reached ({mark_price}). Moving SL to BEP ({entry})")
                                try:
                                    active_engine['exchange'].set_position_stop_loss(symbol, float(entry), side.lower())
                                    cur.execute("UPDATE active_trades SET is_sl_moved = 1 WHERE id = ?", (t_id,))
                                    conn.commit()
                                except Exception as e:
                                    logger.error(f"Failed to set BEP for {symbol}: {e}")
            except Exception as e:
                logger.error(f"Position update handler error for {symbol}: {e}")
            finally: release_conn(conn)
    except Exception as e:
        logger.error(f"Position stream error: {e}")

def ccxt_poll_positions():
    exchange = active_engine['exchange']
    if not exchange or active_engine['platform'] == 'bybit': return # Bybit uses WS
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        # --- CCXT POLLING ENGINE (For Binance/Bitget) ---
        # 1. Manage Entries waiting for TPs ('OPEN')
        cur.execute("SELECT id, symbol, side, tp1, tp2, tp3, strategy, quantity FROM active_trades WHERE status = 'OPEN'")
        open_trades = cur.fetchall()
        for t in open_trades:
            t_id, sym, side_str, tp1, tp2, tp3, strategy, recorded_qty = t
            try:
                pos = exchange.fetch_position(sym)
                size = float(pos.get('contracts', 0))
                if size > 0:
                    if place_split_tps(sym, side_str, size, tp1, tp2, tp3, strategy=strategy):
                        cur.execute("UPDATE active_trades SET status = 'OPEN_TPS_SET', updated_at = datetime('now') WHERE id = ?", (t_id,))
                        conn.commit()
            except Exception as e:
                logger.error(f"Poll Entry Error {sym}: {e}")
                
        # 2. Manage 'OPEN_TPS_SET' for PnL, SL move, and Trailing Stop
        cur.execute("""
            SELECT t.id, t.symbol, t.side, t.entry_price, t.tp1, t.tp2, t.is_sl_moved, 
                   t.trailing_active, t.trailing_stop_price, s.natr, t.strategy, 
                   t.quantity, t.avg_entry_price, t.origin_timeframe
            FROM active_trades t 
            LEFT JOIN trades s ON t.signal_id = s.id 
            WHERE t.status = 'OPEN_TPS_SET'
        """)
        active_tps = cur.fetchall()
        for t in active_tps:
            t_id, sym, side_str, entry, tp1, tp2, sl_moved, trail_active, trail_stop, natr_val, strategy, recorded_qty, avg_entry, origin_tf = t
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
                    conn.commit()
                    logger.info(f"🏁 {sym} Pos Closed (Poll).")
                    continue

                # --- GRID LAYER MONITORING ---
                if strategy == 'GRID' and size > float(recorded_qty) + 0.00001:
                    new_avg = float(pos.get('entryPrice', avg_entry))
                    g_cfg = CONFIG.get('grid_setup', {'take_profit_percentage': 1.5})
                    tp_pct = g_cfg['take_profit_percentage'] / 100
                    new_tp = new_avg * (1 + tp_pct) if side_str.lower() in ['long', 'buy'] else new_avg * (1 - tp_pct)
                    
                    logger.info(f"📈 {sym} Grid Layer Filled! New AEP: {new_avg} | New TP: {new_tp}")
                    
                    # Update TP on exchange (Cancel old reduce-only orders first)
                    try:
                        open_orders = exchange.fetch_open_orders(sym)
                        for oo in open_orders:
                            if oo['side'].lower() == ('sell' if side_str.lower() in ['long', 'buy'] else 'buy') and oo.get('reduceOnly') == True:
                                exchange.cancel_order(oo['id'], sym)
                        
                        place_split_tps(sym, side_str, size, new_tp, None, None, strategy='GRID')
                    except Exception as e:
                        logger.error(f"Failed to update GRID TP for {sym}: {e}")
                    
                    cur.execute("UPDATE active_trades SET quantity = ?, avg_entry_price = ?, tp1 = ?, grid_layer = grid_layer + 1 WHERE id = ?", (size, new_avg, new_tp, t_id))
                    conn.commit()
                    recorded_qty, avg_entry, tp1 = size, new_avg, new_tp
                
                mark = float(pos.get('markPrice', 0))
                
                # Check BEP Move using timeframe-specific adaptive ratio
                if strategy != 'GRID':
                    if tp1 and not sl_moved:
                        dist_to_tp = abs(float(tp1) - entry)
                        profile = get_tf_profile(origin_tf or '15m', get_adaptive_cfg())
                        bep_threshold = entry + (dist_to_tp * profile['bep_ratio']) if side_str.lower() in ['long', 'buy'] else entry - (dist_to_tp * profile['bep_ratio'])
                        
                        is_bep_reached = (side_str.lower() in ['long', 'buy'] and mark >= bep_threshold) or \
                                         (side_str.lower() in ['short', 'sell'] and mark <= bep_threshold)
                                         
                        if is_bep_reached:
                            logger.info(f"♻️ {sym} 50% to TP1 reached ({mark}). Modifying SL to BEP ({entry})")
                            try:
                                if update_stop_loss_on_exchange(sym, side_str, size, float(entry)):
                                    cur.execute("""
                                        UPDATE active_trades
                                        SET is_sl_moved = 1, sl_price = ?, last_sl_update_at = datetime('now'),
                                            last_management_note = 'bep_move', updated_at = datetime('now')
                                        WHERE id = ?
                                    """, (float(entry), t_id))
                                    conn.commit()
                                else:
                                    raise RuntimeError("exchange SL update returned False")
                            except Exception as e:
                                logger.error(f"Failed to set BEP for {sym}: {e}")
                    
                # Check Trailing Activation (TP2 Hit) - Only for NORMAL
                if strategy == 'NORMAL':
                    hit_tp2 = (side_str == 'Long' and mark >= float(tp2)) or (side_str == 'Short' and mark <= float(tp2))
                    if hit_tp2 and not trail_active:
                        logger.info(f"🚀 {sym} TP2 Hit! Activating Chandelier Trailing Stop...")
                        cur.execute("UPDATE active_trades SET trailing_active = 1, trailing_stop_price = ? WHERE id = ?", (float(entry), t_id))
                        trail_active = True
                        trail_stop = float(entry)
                    
                # Trailing Logic Execution
                    if strategy == 'NORMAL' and trail_active:
                        atr_buffer = float(natr_val if natr_val else (mark * 0.02)) * 2 # 2x ATR buffer or 4% default
                        if side_str == 'Long':
                            new_trail = mark - atr_buffer
                            if not trail_stop or new_trail > float(trail_stop):
                                if update_stop_loss_on_exchange(sym, side_str, size, new_trail):
                                    cur.execute("""
                                        UPDATE active_trades
                                        SET trailing_stop_price = ?, sl_price = ?, last_sl_update_at = datetime('now'),
                                            last_management_note = 'trailing_stop_update', updated_at = datetime('now')
                                        WHERE id = ?
                                    """, (new_trail, new_trail, t_id))
                        else:
                            new_trail = mark + atr_buffer
                            if not trail_stop or new_trail < float(trail_stop):
                                if update_stop_loss_on_exchange(sym, side_str, size, new_trail):
                                    cur.execute("""
                                        UPDATE active_trades
                                        SET trailing_stop_price = ?, sl_price = ?, last_sl_update_at = datetime('now'),
                                            last_management_note = 'trailing_stop_update', updated_at = datetime('now')
                                        WHERE id = ?
                                    """, (new_trail, new_trail, t_id))
            except Exception as e:
                logger.error(f"CCXT poll position error for {sym}: {e}")
            
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
            SELECT t.id, t.symbol, t.side, t.entry_price, t.sl_price, t.tp1, t.tp2, t.tp3, t.natr, t.timeframe
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
            sig_id, sym, side, entry, sl, tp1, tp2, tp3, natr, tf = sig
            entry, sl = float(entry), float(sl)

            # --- Strategy Routing ---
            strategy = 'NORMAL'
            grid_max = 1
            if tf in ['15m', '1h']:
                strategy = 'SCALPING'
                s_cfg = CONFIG.get('scalping_setup', {'tp_percentage': 1.5, 'sl_percentage': 1.0})
                tp_pct = s_cfg['tp_percentage'] / 100
                sl_pct = s_cfg['sl_percentage'] / 100
                if side == 'Long':
                    tp1 = entry * (1 + tp_pct)
                    sl = entry * (1 - sl_pct)
                else:
                    tp1 = entry * (1 - tp_pct)
                    sl = entry * (1 + sl_pct)
                tp2, tp3 = None, None # Scalping uses single TP
            elif tf in ['4h', '1d', '1w']:
                strategy = 'GRID'
                grid_max = CONFIG.get('grid_setup', {}).get('max_layers', 4)
                g_cfg = CONFIG.get('grid_setup', {'take_profit_percentage': 1.5})
                tp_pct = g_cfg['take_profit_percentage'] / 100
                if side == 'Long':
                    tp1 = entry * (1 + tp_pct)
                else:
                    tp1 = entry * (1 - tp_pct)
                tp2, tp3 = None, None
            
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
                INSERT INTO active_trades (
                    signal_id, symbol, side, entry_price, sl_price, tp1, tp2, tp3,
                    quantity, leverage, status, strategy, grid_max_layers, avg_entry_price,
                    origin_timeframe, management_state, progress_ratio, peak_price,
                    peak_progress_ratio, locked_profit_level
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?, 'LIVE_MONITORING', 0, ?, 0, 0)
            """, (sig_id, sym, side, entry, sl, tp1, tp2, tp3, qty_coins, final_leverage, strategy, grid_max, entry, tf, entry))
            
            logger.info(f"📥 Signal Ingested: {sym} ({strategy}) | TF: {tf} | Lev: {final_leverage}x")
            current_active += 1
            
        conn.commit()
    except Exception as e: logger.error(f"Ingest Error: {e}")
    finally: release_conn(conn)

def execute_scalping_trade(exchange, oid, sym, side, entry, sl, tp1_val, qty, lev):
    conn = get_conn()
    try:
        cur = conn.cursor()
        try: exchange.set_leverage(int(lev), sym)
        except: pass

        ticker = exchange.fetch_ticker(sym)
        current_price = float(ticker['last'])
        
        is_better_price = (side == 'Long' and current_price <= entry) or (side == 'Short' and current_price >= entry)
        type_side = 'buy' if side == 'Long' else 'sell'
        
        params = {'stopLoss': float(sl)}
        if tp1_val:
            params['takeProfit'] = float(tp1_val)
            
        qty_str = exchange.amount_to_precision(sym, qty)
        
        if is_better_price:
            res = exchange.create_order(sym, 'market', type_side, qty_str, None, params)
        else:
            res = exchange.create_order(sym, 'limit', type_side, qty_str, entry, params)
        
        if res and 'id' in res:
            cur.execute("UPDATE active_trades SET order_id = ?, status = 'OPEN', updated_at = datetime('now') WHERE id = ?", (res['id'], oid))
            conn.commit()
            logger.info(f"✅ Scalping Order Placed for {sym}")
            return True
    except Exception as e:
        logger.error(f"❌ Scalping Execution Failed {sym}: {e}")
        cur.execute("UPDATE active_trades SET status = 'FAILED' WHERE id = ?", (oid,))
        conn.commit()
    finally: release_conn(conn)
    return False

def execute_grid_trade(exchange, oid, sym, side, entry, sl, tp1_val, qty, lev, grid_max):
    conn = get_conn()
    try:
        cur = conn.cursor()
        try: exchange.set_leverage(int(lev), sym)
        except: pass

        ticker = exchange.fetch_ticker(sym)
        current_price = float(ticker['last'])
        
        is_better_price = (side == 'Long' and current_price <= entry) or (side == 'Short' and current_price >= entry)
        type_side = 'buy' if side == 'Long' else 'sell'
        
        params = {'stopLoss': float(sl)}
        if tp1_val:
            params['takeProfit'] = float(tp1_val)
            
        qty_str = exchange.amount_to_precision(sym, qty)
        
        # Layer 1
        if is_better_price:
            res = exchange.create_order(sym, 'market', type_side, qty_str, None, params)
        else:
            res = exchange.create_order(sym, 'limit', type_side, qty_str, entry, params)
        
        if res and 'id' in res:
            cur.execute("UPDATE active_trades SET order_id = ?, status = 'OPEN', updated_at = datetime('now') WHERE id = ?", (res['id'], oid))
            conn.commit()
            logger.info(f"✅ Grid Layer 1 Placed for {sym}")

            # Subsequent Layers
            g_cfg = CONFIG.get('grid_setup', {'price_step_percentage': 2.5, 'martingale_multiplier': 2.0})
            price_step = g_cfg['price_step_percentage'] / 100
            multiplier = g_cfg['martingale_multiplier']
            
            curr_p, curr_q = entry, qty
            for i in range(2, grid_max + 1):
                curr_p = curr_p * (1 - price_step) if side == 'Long' else curr_p * (1 + price_step)
                curr_q = curr_q * multiplier
                
                p_s = exchange.price_to_precision(sym, curr_p)
                q_s = exchange.amount_to_precision(sym, curr_q)
                try:
                    exchange.create_order(sym, 'limit', type_side, q_s, p_s, {'reduceOnly': False})
                    logger.info(f"   🕸️ Layer {i} set at {p_s} (Qty: {q_s})")
                except Exception as ex:
                    logger.error(f"   ❌ Layer {i} Fail: {ex}")
            return True
    except Exception as e:
        logger.error(f"❌ Grid Execution Failed {sym}: {e}")
        cur.execute("UPDATE active_trades SET status = 'FAILED' WHERE id = ?", (oid,))
        conn.commit()
    finally: release_conn(conn)
    return False

def execute_pending_orders():
    exchange = active_engine['exchange']
    if not exchange: return
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, symbol, side, entry_price, sl_price, tp1, quantity, leverage, strategy, grid_max_layers FROM active_trades WHERE status = 'PENDING'")
        orders = cur.fetchall()
        if not orders: return 

        for order in orders:
            oid, sym, side, entry, sl, tp1_val, qty, lev, strategy, grid_max = order
            
            if strategy == 'SCALPING':
                execute_scalping_trade(exchange, oid, sym, side, entry, sl, tp1_val, qty, lev)
            elif strategy == 'GRID':
                execute_grid_trade(exchange, oid, sym, side, entry, sl, tp1_val, qty, lev, grid_max)
            else:
                # NORMAL Fallback
                try:
                    try: exchange.set_leverage(int(lev), sym)
                    except: pass

                    ticker = exchange.fetch_ticker(sym)
                    current_price = float(ticker['last'])
                    
                    is_better_price = (side == 'Long' and current_price <= entry) or (side == 'Short' and current_price >= entry)
                    type_side = 'buy' if side == 'Long' else 'sell'
                    
                    params = {'stopLoss': float(sl)}
                    if tp1_val: params['takeProfit'] = float(tp1_val)
                        
                    qty_str = exchange.amount_to_precision(sym, qty)
                    
                    if is_better_price:
                        res = exchange.create_order(sym, 'market', type_side, qty_str, None, params)
                    else:
                        res = exchange.create_order(sym, 'limit', type_side, qty_str, entry, params)
                    
                    if res and 'id' in res:
                        cur.execute("UPDATE active_trades SET order_id = ?, status = 'OPEN', updated_at = datetime('now') WHERE id = ?", (res['id'], oid))
                        conn.commit()
                        logger.info(f"✅ Normal Order Placed for {sym}")
                except Exception as e:
                    logger.error(f"❌ Normal Execution Failed {sym}: {e}")
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
            except Exception as e:
                logger.error(f"Missed TP recovery error for {sym}: {e}")
    except Exception as e:
        logger.error(f"Missed TP loop error: {e}")
    finally: release_conn(conn)

def run_adaptive_trade_management():
    exchange = active_engine['exchange']
    cfg = get_adaptive_cfg()
    if not exchange or not cfg.get('enabled', True):
        return

    logger.info("🔄 Running adaptive trade management...")
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, symbol, side, entry_price, sl_price, tp1, quantity, strategy, status,
                   origin_timeframe, progress_ratio, peak_price, peak_progress_ratio,
                   locked_profit_level, partial_tp_done, early_exit_done, created_at,
                   last_management_note, last_sl_update_at, last_tp_update_at,
                   last_action_type, last_action_at
            FROM active_trades
            WHERE status IN ('OPEN', 'OPEN_TPS_SET') AND strategy = 'SCALPING'
        """)
        active_scalps = cur.fetchall()

        for trade in active_scalps:
            try:
                (
                    t_id, sym, side, entry, current_sl, tp1, recorded_qty, strategy, status,
                    origin_tf, stored_progress, peak_price, peak_progress, locked_level,
                    partial_done, early_exit_done, created_at, last_note, last_sl_update_at,
                    last_tp_update_at, last_action_type, last_action_at
                ) = trade

                pos = exchange.fetch_position(sym)
                size = float(pos.get('contracts', 0))
                if size <= 0:
                    continue

                origin_tf = origin_tf or '15m'
                profile = get_tf_profile(origin_tf, cfg)
                mark = float(pos.get('markPrice') or pos.get('lastPrice') or pos.get('entryPrice') or entry)

                df = fetch_management_candles(exchange, sym, origin_tf, limit=int(cfg.get('candle_fetch_limit', 60)))
                progress_ratio = calculate_trade_progress(entry, mark, tp1, side) if tp1 else 0.0
                peak_price = float(peak_price) if peak_price else float(entry)
                peak_progress = float(peak_progress) if peak_progress else float(stored_progress or 0.0)
                current_sl = float(current_sl)
                entry = float(entry)

                if is_long_side(side):
                    peak_price = max(peak_price, mark)
                else:
                    peak_price = min(peak_price, mark)
                peak_progress = max(float(peak_progress), float(progress_ratio))
                loop_action_taken = False

                cur.execute("""
                    UPDATE active_trades
                    SET progress_ratio = ?, peak_price = ?, peak_progress_ratio = ?,
                        last_candle_check_at = datetime('now'), updated_at = datetime('now')
                    WHERE id = ?
                """, (progress_ratio, peak_price, peak_progress, t_id))

                new_level, sl_action = maybe_raise_profit_lock(
                    sym, side, entry, tp1, current_sl, mark, size,
                    {'origin_timeframe': origin_tf, 'locked_profit_level': locked_level},
                    cfg, progress_ratio
                )
                if sl_action:
                    new_sl, note = sl_action
                    sl_delta = pct_change(current_sl, new_sl)
                    if action_allowed(last_sl_update_at, 'sl_update', cfg) and sl_delta >= float(cfg.get('min_sl_change_pct', 0.0015)):
                        current_sl = float(new_sl)
                        locked_level = new_level
                        cur.execute("""
                            UPDATE active_trades
                            SET sl_price = ?, locked_profit_level = ?, is_sl_moved = 1,
                                last_sl_update_at = datetime('now'), last_management_note = ?,
                                last_action_type = 'sl_update', last_action_at = datetime('now'),
                                updated_at = datetime('now')
                            WHERE id = ?
                        """, (current_sl, locked_level, note, t_id))
                        last_sl_update_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                        last_action_at = last_sl_update_at
                        last_action_type = 'sl_update'
                        loop_action_taken = True
                elif new_level > int(locked_level or 0):
                    locked_level = new_level
                    cur.execute("""
                        UPDATE active_trades
                        SET locked_profit_level = ?, last_management_note = ?,
                            updated_at = datetime('now')
                        WHERE id = ?
                    """, (locked_level, f"profit_lock_level_{locked_level}_ack", t_id))

                adaptive_sl, swing_val, atr_val = calculate_dynamic_sl(df, side, entry, lookback=20)
                better_dynamic_sl = adaptive_sl > current_sl if is_long_side(side) else adaptive_sl < current_sl
                if better_dynamic_sl and locked_level >= 1 and not loop_action_taken:
                    sl_delta = pct_change(current_sl, adaptive_sl)
                    sl_cooldown_ok = action_allowed(last_sl_update_at, 'sl_update', cfg)
                    sl_change_ok = sl_delta >= float(cfg.get('min_sl_change_pct', 0.0015))
                    if not sl_cooldown_ok or not sl_change_ok:
                        better_dynamic_sl = False
                if better_dynamic_sl and locked_level >= 1 and not loop_action_taken:
                    if update_stop_loss_on_exchange(sym, side, size, adaptive_sl):
                        current_sl = float(adaptive_sl)
                        note = f"structure_sl_update atr={atr_val:.6f} swing={swing_val:.6f}"
                        cur.execute("""
                            UPDATE active_trades
                            SET sl_price = ?, is_sl_moved = 1, last_sl_update_at = datetime('now'),
                                last_management_note = ?, last_action_type = 'sl_update',
                                last_action_at = datetime('now'), updated_at = datetime('now')
                            WHERE id = ?
                        """, (current_sl, note, t_id))
                        last_sl_update_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                        last_action_at = last_sl_update_at
                        last_action_type = 'sl_update'
                        loop_action_taken = True

                rejection, rejection_reason = detect_rejection_signal(df, side)
                momentum_loss, momentum_reason = detect_momentum_loss(df, side)
                stagnant, stagnant_reason = detect_stagnation(
                    {'created_at': created_at}, progress_ratio, origin_tf, cfg
                )

                if (
                    cfg.get('tp_shrink_on_rejection', True) and tp1 and locked_level >= 2 and
                    (rejection or stagnant) and not str(last_note or '').startswith('tp_shrink') and
                    not loop_action_taken
                ):
                    current_tp = float(tp1)
                    distance = abs(current_tp - entry)
                    reduced_distance = distance * profile['tp_shrink_factor']
                    new_tp = entry + reduced_distance if is_long_side(side) else entry - reduced_distance
                    tp_change = pct_change(current_tp, new_tp)
                    if (
                        action_allowed(last_tp_update_at, 'tp_update', cfg) and
                        tp_change >= float(cfg.get('min_tp_change_pct', 0.0025)) and
                        update_take_profit_on_exchange(sym, side, size, new_tp)
                    ):
                        note = f"tp_shrink_{rejection_reason or stagnant_reason or 'adaptive'}"
                        cur.execute("""
                            UPDATE active_trades
                            SET tp1 = ?, last_tp_update_at = datetime('now'),
                                last_management_note = ?, last_action_type = 'tp_update',
                                last_action_at = datetime('now'), updated_at = datetime('now')
                            WHERE id = ?
                        """, (new_tp, note, t_id))
                        tp1 = new_tp
                        last_tp_update_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                        last_action_at = last_tp_update_at
                        last_action_type = 'tp_update'
                        loop_action_taken = True
                        logger.info(f"🎯 {sym} progress={progress_ratio:.2f} action=shrink_tp reason={note}")

                if (
                    cfg.get('allow_partial_close', True) and not partial_done and locked_level >= 2 and
                    progress_ratio >= profile['level3_ratio'] and (rejection or momentum_loss) and
                    not loop_action_taken and action_allowed(last_action_at, 'partial_close', cfg)
                ):
                    note = rejection_reason or momentum_reason or 'adaptive_partial_close'
                    ok, closed_qty = execute_partial_close(sym, side, size, float(cfg.get('partial_close_ratio', 0.50)), note)
                    if ok:
                        remaining_qty = max(float(size) - float(closed_qty), 0.0)
                        partial_done = True
                        cur.execute("""
                            UPDATE active_trades
                            SET partial_tp_done = 1, quantity = ?, last_management_note = ?,
                                last_action_type = 'partial_close', last_action_at = datetime('now'),
                                updated_at = datetime('now')
                            WHERE id = ?
                        """, (remaining_qty, f"partial_close_{note}", t_id))
                        last_action_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                        last_action_type = 'partial_close'
                        loop_action_taken = True

                should_early_exit = (
                    not early_exit_done and peak_progress >= float(cfg.get('early_exit_peak_threshold', 0.60)) and
                    progress_ratio <= float(cfg.get('early_exit_fallback_progress', 0.35)) and
                    (rejection or momentum_loss)
                )
                if not should_early_exit and not early_exit_done and stagnant and progress_ratio <= 0:
                    should_early_exit = True

                if should_early_exit and not loop_action_taken and action_allowed(last_action_at, 'early_exit', cfg):
                    note = rejection_reason or momentum_reason or stagnant_reason or 'adaptive_early_exit'
                    if execute_early_exit(sym, side, size, note):
                        cur.execute("""
                            UPDATE active_trades
                            SET early_exit_done = 1, last_management_note = ?,
                                last_action_type = 'early_exit', last_action_at = datetime('now'),
                                updated_at = datetime('now')
                            WHERE id = ?
                        """, (f"early_exit_{note}", t_id))
                        logger.info(f"🛑 {sym} progress={progress_ratio:.2f} peak={peak_progress:.2f} action=early_exit reason={note}")

            except Exception as e:
                logger.error(f"⚠️ Adaptive management error for trade {trade[0] if trade else '?'}: {e}")
        conn.commit()
    except Exception as e:
        logger.error(f"Adaptive management loop error: {e}")
    finally:
        release_conn(conn)

def run_periodic_sl_update():
    """
    Backward-compatible wrapper. Adaptive manager supersedes the old SL-only updater.
    """
    run_adaptive_trade_management()

if __name__ == "__main__":
    logger.info("🟢 Starting Multi-CEX Auto-Trader...")
    init_execution_db()
    
    schedule.every(3).seconds.do(sync_active_exchange)
    schedule.every(10).seconds.do(ccxt_poll_positions) # Fallback for Binance/Bitget
    schedule.every(int(get_adaptive_cfg().get('check_interval_seconds', 30))).seconds.do(run_adaptive_trade_management)
    schedule.every(1).minutes.do(ingest_fresh_signals)      
    schedule.every(5).seconds.do(execute_pending_orders)    
    schedule.every(20).seconds.do(check_missed_tps)         
    
    logger.info(f"🚀 Bot is LIVE. Monitoring {MAX_POSITIONS} Max Positions.")
    while True:
        schedule.run_pending()
        time.sleep(1)
