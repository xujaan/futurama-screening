"""
Tujuan: Menyediakan indikator teknikal dan perhitungan pola harga (ATR, Swing Low/High, Divergence, Regime).
Caller: main.py, modules.execution.py, modules.smc.py
Dependensi: pandas, pandas_ta_classic, scipy
Main Functions: calculate_atr(), find_swing_low(), get_technicals(), detect_regime()
Side Effects: None (Purely data calculation)
"""
import pandas_ta_classic as ta
import numpy as np
import pandas as pd
from scipy.signal import argrelextrema

def is_long_side(side):
    return str(side).lower() in ['long', 'buy']

def calculate_atr(df, length=14):
    """
    Menghitung Average True Range (ATR) murni.
    """
    atr = ta.atr(df['high'], df['low'], df['close'], length=length)
    if atr is not None:
        return atr.iloc[-1]
    return 0.0

def find_swing_low(df, lookback=50):
    """
    Mencari harga terendah dalam rentang lookback terakhir.
    """
    return df['low'].iloc[-lookback:].min()

def find_swing_high(df, lookback=50):
    """
    Mencari harga tertinggi dalam rentang lookback terakhir.
    """
    return df['high'].iloc[-lookback:].max()

def calculate_dynamic_sl(df, side, entry_price, lookback=20, atr_length=14, atr_mult=1.5):
    """
    Menghitung SL dinamis berdasarkan Swing Low/High dan ATR.
    """
    atr = calculate_atr(df, length=atr_length)
    if side.lower() in ['long', 'buy']:
        s_low = find_swing_low(df, lookback=lookback)
        # Ambil nilai yang lebih rendah antara swing low dan entry - buffer ATR
        sl_final = min(s_low, entry_price - (atr_mult * atr))
        return sl_final, s_low, atr
    else:
        s_high = find_swing_high(df, lookback=lookback)
        # Ambil nilai yang lebih tinggi antara swing high dan entry + buffer ATR
        sl_final = max(s_high, entry_price + (atr_mult * atr))
        return sl_final, s_high, atr

def calculate_trade_progress(entry_price, current_price, tp1_price, side):
    """
    Returns progress toward TP1.
    < 0 means drawdown, 0..1 means on the way, >= 1 means TP1 reached/passed.
    """
    try:
        entry_price = float(entry_price)
        current_price = float(current_price)
        tp1_price = float(tp1_price)
        if entry_price <= 0 or tp1_price <= 0:
            return 0.0

        if is_long_side(side):
            journey = tp1_price - entry_price
            move = current_price - entry_price
        else:
            journey = entry_price - tp1_price
            move = entry_price - current_price

        if journey == 0:
            return 0.0
        return move / journey
    except Exception:
        return 0.0

def detect_rejection_signal(df, side):
    """
    Lightweight rejection detector meant for trade management, not signal generation.
    """
    try:
        if df is None or len(df) < 3:
            return False, None

        last = df.iloc[-1]
        prev = df.iloc[-2]
        price_range = max(float(last['high']) - float(last['low']), 1e-9)
        body = abs(float(last['close']) - float(last['open']))
        upper_wick = float(last['high']) - max(float(last['open']), float(last['close']))
        lower_wick = min(float(last['open']), float(last['close'])) - float(last['low'])
        prev_mid = (float(prev['open']) + float(prev['close'])) / 2

        if is_long_side(side):
            if upper_wick >= body * 1.5 and float(last['close']) < float(last['open']):
                return True, "upper_wick_rejection"
            if float(last['high']) > float(prev['high']) and float(last['close']) < prev_mid:
                return True, "failed_breakout_rejection"
            if upper_wick / price_range >= 0.45 and float(last['close']) <= float(prev['close']):
                return True, "weak_close_after_push"
        else:
            if lower_wick >= body * 1.5 and float(last['close']) > float(last['open']):
                return True, "lower_wick_rejection"
            if float(last['low']) < float(prev['low']) and float(last['close']) > prev_mid:
                return True, "failed_breakdown_rejection"
            if lower_wick / price_range >= 0.45 and float(last['close']) >= float(prev['close']):
                return True, "weak_close_after_dump"
        return False, None
    except Exception:
        return False, None

def detect_momentum_loss(df, side):
    """
    Basic momentum fade detector using the last three candles.
    """
    try:
        if df is None or len(df) < 4:
            return False, None

        last = df.iloc[-1]
        prev = df.iloc[-2]
        prev2 = df.iloc[-3]

        if is_long_side(side):
            lower_highs = float(last['high']) <= float(prev['high']) <= float(prev2['high'])
            lower_closes = float(last['close']) < float(prev['close']) < float(prev2['close'])
            if lower_highs and lower_closes:
                return True, "bullish_momentum_loss"
        else:
            higher_lows = float(last['low']) >= float(prev['low']) >= float(prev2['low'])
            higher_closes = float(last['close']) > float(prev['close']) > float(prev2['close'])
            if higher_lows and higher_closes:
                return True, "bearish_momentum_loss"
        return False, None
    except Exception:
        return False, None

def detect_divergence(df):
    score = 0
    reasons = []
    close = df['close'].values
    k = df['stoch_rsi_k'].values
    
    high_idx = argrelextrema(close, np.greater, order=3)[0]
    low_idx = argrelextrema(close, np.less, order=3)[0]

    if len(high_idx) >= 2:
        if close[high_idx[-1]] > close[high_idx[-2]] and k[high_idx[-1]] < k[high_idx[-2]]:
            score -= 2; reasons.append("Bear Div")
    
    if len(low_idx) >= 2:
        if close[low_idx[-1]] < close[low_idx[-2]] and k[low_idx[-1]] > k[low_idx[-2]]:
            score += 2; reasons.append("Bull Div")
            
    return score, ", ".join(reasons)

def get_technicals(df):
    df['EMA_Fast'] = ta.ema(df['close'], length=13)
    df['EMA_Slow'] = ta.ema(df['close'], length=21)
    stoch = ta.stochrsi(df['close'], length=14, k=3, d=3)
    if stoch is not None:
        df['stoch_rsi_k'] = stoch[stoch.columns[0]]
        df['stoch_rsi_d'] = stoch[stoch.columns[1]]
    
    macd = ta.macd(df['close'])
    if macd is not None:
        df['MACD_h'] = macd[macd.columns[1]]

    vol_sma = ta.sma(df['volume'], length=20)
    if vol_sma is not None: df['RVOL'] = df['volume'] / vol_sma
    else: df['RVOL'] = 0.0
        
    # Squeeze & Regime Indicators
    df['SMA_50'] = ta.sma(df['close'], length=50) if len(df) >= 50 else np.nan
    df['SMA_200'] = ta.sma(df['close'], length=200) if len(df) >= 200 else np.nan
    
    adx_res = ta.adx(df['high'], df['low'], df['close'], length=14)
    if adx_res is not None: df['ADX_14'] = adx_res['ADX_14']
    else: df['ADX_14'] = 0
    
    # Bollinger Bands
    bbands = ta.bbands(df['close'], length=20, std=2.0)
    if bbands is not None:
        df['BBL_20_2.0'] = bbands[bbands.columns[0]]
        df['BBU_20_2.0'] = bbands[bbands.columns[2]]
        
    # Keltner Channels
    kc = ta.kc(df['high'], df['low'], df['close'], length=20, scalar=1.5)
    if kc is not None:
        df['KCLe_20_1.5'] = kc[kc.columns[0]]
        df['KCUe_20_1.5'] = kc[kc.columns[2]]
        
    atr = ta.atr(df['high'], df['low'], df['close'], length=14)
    if atr is not None: df['ATR_14'] = atr
    else: df['ATR_14'] = 0.0

    # Normalized ATR (NATR) for Volatility Sizing
    natr = ta.natr(df['high'], df['low'], df['close'], length=14)
    if natr is not None: df['NATR_14'] = natr
    else: df['NATR_14'] = 0.0
        
    df.dropna(inplace=True)
    return df

def check_volatility_squeeze(df):
    """
    TTM Squeeze concepts: Bollinger Bands completely enclosed inside Keltner Channels.
    Returns: Is_Squeezing (bool), and Squeeze_Firing (bool)
    """
    try:
        bbl = df['BBL_20_2.0'].iloc[-1]
        bbu = df['BBU_20_2.0'].iloc[-1]
        kcl = df['KCLe_20_1.5'].iloc[-1]
        kcu = df['KCUe_20_1.5'].iloc[-1]
        
        # Squeeze is ON if BB bounds are inside KC bounds
        squeeze_on = (bbl > kcl) and (bbu < kcu)
        
        # Squeeze FIRING: Was ON previously, but now OFF (Bands expanding rapidly)
        prev_bbl = df['BBL_20_2.0'].iloc[-2]
        prev_bbu = df['BBU_20_2.0'].iloc[-2]
        prev_kcl = df['KCLe_20_1.5'].iloc[-2]
        prev_kcu = df['KCUe_20_1.5'].iloc[-2]
        was_squeezing = (prev_bbl > prev_kcl) and (prev_bbu < prev_kcu)
        
        squeeze_firing = was_squeezing and not squeeze_on
        
        return squeeze_on, squeeze_firing
    except: return False, False

def detect_regime(df):
    """
    Market Regime classification based on ADX (trend strength) and SMA hierarchy.
    Regimes: Trending Bull, Trending Bear, Choppy/Sideways
    """
    try:
        close = df['close'].iloc[-1]
        sma50 = df['SMA_50'].iloc[-1]
        sma200 = df['SMA_200'].iloc[-1]
        adx = df['ADX_14'].iloc[-1]
        
        is_trending = adx > 25
        
        if is_trending:
            if close > sma50 > sma200:
                return "Trending Bull"
            elif close < sma50 < sma200:
                return "Trending Bear"
            else:
                return "Volatile Expansion"
        else:
            return "Choppy / Sideways"
    except: return "Unknown"
