import pandas_ta_classic as ta
import numpy as np
from scipy.signal import argrelextrema

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